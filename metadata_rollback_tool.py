#!/usr/bin/env python3

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path

from db_manager import DB_PATH, get_connection, exclusive_transaction


def resolve_build(conn, title=None, version=None, build_id=None):
    if build_id is not None:
        row = conn.execute(
            """
            SELECT b.id, b.vn_id, v.series_id, v.title, b.version
            FROM builds b
            JOIN visual_novels v ON v.id = b.vn_id
            WHERE b.id = ?
            """,
            (build_id,),
        ).fetchone()
        if not row:
            raise ValueError(f"No build found for build_id={build_id}")
        return row

    if not title or not version:
        raise ValueError("Provide either --build-id or both --title and --version")

    row = conn.execute(
        """
        SELECT b.id, b.vn_id, v.series_id, v.title, b.version
        FROM builds b
        JOIN visual_novels v ON v.id = b.vn_id
        WHERE v.title = ? AND b.version = ?
        """,
        (title, version),
    ).fetchone()

    if not row:
        raise ValueError(f"No build found for title='{title}' version='{version}'")

    return row


def get_version_rows(conn, build_id):
    return conn.execute(
        """
        SELECT
            mv.id,
            mv.version_number,
            mv.created_at,
            mv.is_current,
            mv.change_note,
            mv.metadata_hash
        FROM metadata_versions mv
        WHERE mv.build_id = ?
        ORDER BY mv.version_number DESC, mv.id DESC
        """,
        (build_id,),
    ).fetchall()


def get_archive_rows(conn, build_id):
    return conn.execute(
        """
        SELECT
            a.id,
            a.sha256,
            a.archived_at,
            a.created_at
        FROM archives a
        WHERE a.build_id = ?
        ORDER BY a.created_at DESC, a.id DESC
        """,
        (build_id,),
    ).fetchall()


def backup_database(backup_dir):
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    backup_path = Path(backup_dir) / f"archive_backup_{ts}.db"
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(DB_PATH, backup_path)
    return backup_path


def prune_orphaned_rows(conn):
    metadata_deleted = conn.execute(
        """
        DELETE FROM metadata_objects
        WHERE hash NOT IN (SELECT metadata_hash FROM metadata_versions)
        """
    ).rowcount

    tags_deleted = conn.execute(
        """
        DELETE FROM tags
        WHERE id NOT IN (SELECT tag_id FROM vn_tags)
        """
    ).rowcount

    series_deleted = conn.execute(
        """
        DELETE FROM series
        WHERE id NOT IN (
            SELECT DISTINCT series_id FROM visual_novels WHERE series_id IS NOT NULL
        )
        """
    ).rowcount

    return {
        "metadata_objects": metadata_deleted,
        "tags": tags_deleted,
        "series": series_deleted,
    }


def preview_undo_build_create(conn, build_row, delete_empty_vn):
    build_id = build_row["id"]
    vn_id = build_row["vn_id"]

    preview = {
        "build_id": build_id,
        "vn_id": vn_id,
        "would_delete": {
            "builds": 1,
            "archives": conn.execute(
                "SELECT COUNT(*) FROM archives WHERE build_id = ?",
                (build_id,),
            ).fetchone()[0],
            "build_target_platforms": conn.execute(
                "SELECT COUNT(*) FROM build_target_platforms WHERE build_id = ?",
                (build_id,),
            ).fetchone()[0],
            "metadata_versions": conn.execute(
                "SELECT COUNT(*) FROM metadata_versions WHERE build_id = ?",
                (build_id,),
            ).fetchone()[0],
        },
        "would_delete_vn": False,
        "note": "",
    }

    remaining_builds_after = conn.execute(
        "SELECT COUNT(*) FROM builds WHERE vn_id = ? AND id != ?",
        (vn_id, build_id),
    ).fetchone()[0]

    if delete_empty_vn and remaining_builds_after == 0:
        preview["would_delete_vn"] = True
        preview["note"] = "VN row would be deleted because no builds would remain."
    elif remaining_builds_after > 0:
        preview["note"] = "VN row would be kept because other builds exist."
    else:
        preview["note"] = "VN row would be kept because --keep-empty-vn was used."

    return preview


def export_metadata_json(conn, build_row, version_row, out_dir):
    blob = conn.execute(
        "SELECT metadata_json FROM metadata_objects WHERE hash = ?",
        (version_row["metadata_hash"],),
    ).fetchone()

    if not blob or not blob["metadata_json"]:
        raise ValueError("No metadata JSON found for selected version")

    parsed = json.loads(blob["metadata_json"])
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    base_name = f"{build_row['title']}_v{build_row['version']}_meta_v{version_row['version_number']}_{ts}.json"
    safe_name = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in base_name)

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / safe_name

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(parsed, f, ensure_ascii=False, indent=2)

    return json_path


def list_versions(args):
    with get_connection() as conn:
        build_row = resolve_build(conn, args.title, args.version, args.build_id)
        rows = get_version_rows(conn, build_row["id"])

    if not rows:
        print("No metadata versions found for that build.")
        return

    print(f"Build {build_row['id']}: {build_row['title']} (version {build_row['version']})")
    for row in rows:
        current = "*" if row["is_current"] else " "
        note = (row["change_note"] or "").strip()
        note_display = f" - {note}" if note else ""
        print(
            f"{current} v{row['version_number']:<4} id={row['id']:<4} "
            f"hash={row['metadata_hash'][:12]}... created={row['created_at']}{note_display}"
        )


def rollback(args):
    with get_connection() as conn:
        build_row = resolve_build(conn, args.title, args.version, args.build_id)
        rows = get_version_rows(conn, build_row["id"])

        if len(rows) < 2 and args.to_version is None:
            raise ValueError("Need at least 2 versions to rollback to previous")

        current_row = next((r for r in rows if r["is_current"]), None)
        if not current_row:
            raise ValueError("No current metadata version is marked for this build")

        if args.to_version is None:
            target_row = next((r for r in rows if r["version_number"] < current_row["version_number"]), None)
            if not target_row:
                raise ValueError("No earlier metadata version available to rollback")
        else:
            target_row = next((r for r in rows if r["version_number"] == args.to_version), None)
            if not target_row:
                raise ValueError(f"Version v{args.to_version} not found for this build")

        if target_row["id"] == current_row["id"]:
            raise ValueError("Target version is already current")

        backup_path = backup_database(args.backup_dir) if args.backup else None

        with exclusive_transaction(conn):
            conn.execute("UPDATE metadata_versions SET is_current = 0 WHERE build_id = ?", (build_row["id"],))
            conn.execute("UPDATE metadata_versions SET is_current = 1 WHERE id = ?", (target_row["id"],))

        print(
            f"Rolled back build {build_row['id']} ({build_row['title']} v{build_row['version']}) "
            f"from metadata v{current_row['version_number']} to v{target_row['version_number']}."
        )
        if backup_path:
            print(f"Database backup created: {backup_path}")
        if args.export_dir:
            json_path = export_metadata_json(conn, build_row, target_row, args.export_dir)
            print(f"Exported rolled-back metadata JSON: {json_path}")


def delete_version(args):
    with get_connection() as conn:
        build_row = resolve_build(conn, args.title, args.version, args.build_id)
        rows = get_version_rows(conn, build_row["id"])

        if not rows:
            raise ValueError("No metadata versions found for that build")

        if args.latest:
            target_row = rows[0]
        else:
            target_row = next((r for r in rows if r["version_number"] == args.target_version), None)
            if not target_row:
                raise ValueError(f"Version v{args.target_version} not found for this build")

        if target_row["is_current"] and len(rows) == 1:
            raise ValueError("Cannot delete the only metadata version for this build")

        backup_path = backup_database(args.backup_dir) if args.backup else None

        with exclusive_transaction(conn):
            if target_row["is_current"]:
                replacement = next((r for r in rows if r["id"] != target_row["id"]), None)
                if not replacement:
                    raise ValueError("Could not find replacement current version")
                conn.execute("UPDATE metadata_versions SET is_current = 0 WHERE build_id = ?", (build_row["id"],))
                conn.execute("UPDATE metadata_versions SET is_current = 1 WHERE id = ?", (replacement["id"],))

            conn.execute("DELETE FROM metadata_versions WHERE id = ?", (target_row["id"],))
            prune_stats = prune_orphaned_rows(conn)

        print(
            f"Deleted metadata version v{target_row['version_number']} for "
            f"build {build_row['id']} ({build_row['title']} v{build_row['version']})."
        )
        print(f"Cleanup: {prune_stats}")
        if backup_path:
            print(f"Database backup created: {backup_path}")


def undo_latest_entry(args):
    """Undo the newest metadata+archive entry on an existing build.

    Intended for cases where an update to an existing build (same build_id) created
    a new metadata version and archive row, and you want to remove that latest pair
    without deleting the build itself.
    """
    with get_connection() as conn:
        build_row = resolve_build(conn, args.title, args.version, args.build_id)
        version_rows = get_version_rows(conn, build_row["id"])
        archive_rows = get_archive_rows(conn, build_row["id"])

        if len(version_rows) < 2:
            raise ValueError(
                "Cannot undo latest entry: this build has fewer than 2 metadata versions. "
                "Use rollback/delete-version as appropriate."
            )

        if len(archive_rows) < 2:
            raise ValueError(
                "Cannot undo latest entry: deleting the only archive row would trigger build deletion. "
                "Use undo-build-create if you intend to remove the full build."
            )

        target_version = version_rows[0]
        replacement_version = next((r for r in version_rows if r["id"] != target_version["id"]), None)
        if not replacement_version:
            raise ValueError("Could not find replacement metadata version for current pointer")

        target_archive = archive_rows[0]

        backup_path = backup_database(args.backup_dir) if args.backup else None

        with exclusive_transaction(conn):
            if target_version["is_current"]:
                conn.execute(
                    "UPDATE metadata_versions SET is_current = 0 WHERE build_id = ?",
                    (build_row["id"],),
                )
                conn.execute(
                    "UPDATE metadata_versions SET is_current = 1 WHERE id = ?",
                    (replacement_version["id"],),
                )

            conn.execute("DELETE FROM metadata_versions WHERE id = ?", (target_version["id"],))
            conn.execute("DELETE FROM archives WHERE id = ?", (target_archive["id"],))
            prune_stats = prune_orphaned_rows(conn)

        print(
            f"Undid latest entry for build {build_row['id']} ({build_row['title']} v{build_row['version']}): "
            f"removed metadata v{target_version['version_number']} and archive id={target_archive['id']} "
            f"({target_archive['sha256'][:12]}...)."
        )
        print(f"Current metadata version is now v{replacement_version['version_number']}.")
        print(f"Cleanup: {prune_stats}")
        if backup_path:
            print(f"Database backup created: {backup_path}")


def undo_build_create(args):
    with get_connection() as conn:
        build_row = resolve_build(conn, args.title, args.version, args.build_id)

        if args.dry_run:
            preview = preview_undo_build_create(conn, build_row, args.delete_empty_vn)
            print("DRY RUN: no database changes were made.")
            print(
                f"Target build: {build_row['id']} ({build_row['title']} v{build_row['version']})"
            )
            print(f"Would delete rows: {preview['would_delete']}")
            print(f"Would delete VN row: {preview['would_delete_vn']}")
            print(preview["note"])
            return

        backup_path = backup_database(args.backup_dir) if args.backup else None

        with exclusive_transaction(conn):
            conn.execute("DELETE FROM builds WHERE id = ?", (build_row["id"],))

            vn_deleted = False
            remaining_builds = conn.execute(
                "SELECT COUNT(*) FROM builds WHERE vn_id = ?",
                (build_row["vn_id"],),
            ).fetchone()[0]
            if args.delete_empty_vn and remaining_builds == 0:
                conn.execute("DELETE FROM visual_novels WHERE id = ?", (build_row["vn_id"],))
                vn_deleted = True

            prune_stats = prune_orphaned_rows(conn)

        print(
            f"Removed build {build_row['id']} ({build_row['title']} v{build_row['version']}) "
            "and cascaded build-linked metadata/archive rows."
        )
        if vn_deleted:
            print("Removed now-empty visual_novels row for this title.")
        else:
            print("Visual novel row was retained (other builds still exist, or --keep-empty-vn was used).")
        print(f"Cleanup: {prune_stats}")
        if backup_path:
            print(f"Database backup created: {backup_path}")


def build_parser():
    parser = argparse.ArgumentParser(
        description=(
            "Manage metadata/build history in archive.db: list, rollback, delete version(s), undo latest update entry, or fully undo a build create."
        )
    )
    parser.add_argument("--title", help="Visual novel title (used with --version)")
    parser.add_argument("--version", help="Build version text (used with --title)")
    parser.add_argument("--build-id", type=int, help="Build ID (alternative to --title/--version)")

    sub = parser.add_subparsers(dest="command", required=True)

    list_cmd = sub.add_parser("list", help="List metadata versions for a build")
    list_cmd.set_defaults(func=list_versions)

    rollback_cmd = sub.add_parser("rollback", help="Rollback current pointer to an older metadata version")
    rollback_cmd.add_argument("--to-version", type=int, help="Explicit metadata version to mark current")
    rollback_cmd.add_argument("--backup", action="store_true", help="Create backup before rollback")
    rollback_cmd.add_argument("--backup-dir", default="db_backups", help="Backup directory")
    rollback_cmd.add_argument("--export-dir", default="metadata_exports", help="Export target JSON directory")
    rollback_cmd.set_defaults(func=rollback)

    delete_cmd = sub.add_parser("delete-version", help="Delete metadata version(s) for a build")
    delete_target = delete_cmd.add_mutually_exclusive_group(required=True)
    delete_target.add_argument("--target-version", type=int, help="metadata_versions.version_number")
    delete_target.add_argument("--latest", action="store_true", help="Delete newest metadata version for this build")
    delete_cmd.add_argument("--backup", action="store_true", help="Create backup before deletion")
    delete_cmd.add_argument("--backup-dir", default="db_backups", help="Backup directory")
    delete_cmd.set_defaults(func=delete_version)

    undo_latest_cmd = sub.add_parser(
        "undo-latest-entry",
        help="Undo newest metadata+archive entry for an existing build without deleting the build",
    )
    undo_latest_cmd.add_argument("--backup", action="store_true", help="Create backup before undo")
    undo_latest_cmd.add_argument("--backup-dir", default="db_backups", help="Backup directory")
    undo_latest_cmd.set_defaults(func=undo_latest_entry)

    undo_cmd = sub.add_parser(
        "undo-build-create",
        help="Hard undo a created build entry (closest to 'as if that create never happened')",
    )
    undo_cmd.add_argument("--backup", action="store_true", help="Create backup before undo")
    undo_cmd.add_argument("--backup-dir", default="db_backups", help="Backup directory")
    undo_cmd.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview rows that would be affected without applying changes",
    )
    undo_cmd.add_argument(
        "--keep-empty-vn",
        action="store_true",
        help="Keep visual_novels row even if no builds remain",
    )
    undo_cmd.set_defaults(func=undo_build_create, delete_empty_vn=True)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    if getattr(args, "keep_empty_vn", False):
        args.delete_empty_vn = False
    args.func(args)


if __name__ == "__main__":
    main()
