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
            SELECT b.id, v.title, b.version
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
        SELECT b.id, v.title, b.version
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


def backup_database(backup_dir):
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    backup_path = Path(backup_dir) / f"archive_backup_{ts}.db"
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(DB_PATH, backup_path)
    return backup_path


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
    safe_name = "".join(ch if ch.isalnum() or ch in "-_\." else "_" for ch in base_name)

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

        backup_path = None
        if args.backup:
            backup_path = backup_database(args.backup_dir)

        with exclusive_transaction(conn):
            conn.execute(
                "UPDATE metadata_versions SET is_current = 0 WHERE build_id = ?",
                (build_row["id"],),
            )
            conn.execute(
                "UPDATE metadata_versions SET is_current = 1 WHERE id = ?",
                (target_row["id"],),
            )

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

        target_row = next((r for r in rows if r["version_number"] == args.target_version), None)
        if not target_row:
            raise ValueError(f"Version v{args.target_version} not found for this build")

        current_row = next((r for r in rows if r["is_current"]), None)

        if target_row["is_current"] and len(rows) == 1:
            raise ValueError("Cannot delete the only metadata version for this build")

        backup_path = None
        if args.backup:
            backup_path = backup_database(args.backup_dir)

        with exclusive_transaction(conn):
            if target_row["is_current"]:
                replacement = next((r for r in rows if r["id"] != target_row["id"]), None)
                if not replacement:
                    raise ValueError("Could not find replacement current version")
                conn.execute(
                    "UPDATE metadata_versions SET is_current = 0 WHERE build_id = ?",
                    (build_row["id"],),
                )
                conn.execute(
                    "UPDATE metadata_versions SET is_current = 1 WHERE id = ?",
                    (replacement["id"],),
                )

            conn.execute("DELETE FROM metadata_versions WHERE id = ?", (target_row["id"],))

            conn.execute(
                """
                DELETE FROM metadata_objects
                WHERE hash = ?
                  AND hash NOT IN (SELECT metadata_hash FROM metadata_versions)
                """,
                (target_row["metadata_hash"],),
            )

        print(
            f"Deleted metadata version v{target_row['version_number']} for "
            f"build {build_row['id']} ({build_row['title']} v{build_row['version']})."
        )
        if target_row["is_current"] and current_row:
            print("Current pointer was reassigned to the next available version.")
        if backup_path:
            print(f"Database backup created: {backup_path}")


def build_parser():
    parser = argparse.ArgumentParser(
        description=(
            "List, rollback, and optionally delete metadata_versions entries in archive.db."
        )
    )
    parser.add_argument("--title", help="Visual novel title (used with --version)")
    parser.add_argument("--version", help="Build version text (used with --title)")
    parser.add_argument("--build-id", type=int, help="Build ID (alternative to --title/--version)")

    sub = parser.add_subparsers(dest="command", required=True)

    list_cmd = sub.add_parser("list", help="List metadata versions for a build")
    list_cmd.set_defaults(func=list_versions)

    rollback_cmd = sub.add_parser("rollback", help="Rollback current pointer to an older metadata version")
    rollback_cmd.add_argument(
        "--to-version",
        type=int,
        help="Explicit metadata version number to mark as current (default: previous)",
    )
    rollback_cmd.add_argument(
        "--backup",
        action="store_true",
        help="Create a timestamped archive.db backup before rollback",
    )
    rollback_cmd.add_argument(
        "--backup-dir",
        default="db_backups",
        help="Directory for backup files (default: db_backups)",
    )
    rollback_cmd.add_argument(
        "--export-dir",
        default="metadata_exports",
        help="Export selected metadata version JSON to this directory",
    )
    rollback_cmd.set_defaults(func=rollback)

    delete_cmd = sub.add_parser(
        "delete-version",
        help="Delete one metadata version row for a build (history-destructive)",
    )
    delete_cmd.add_argument(
        "--target-version",
        type=int,
        required=True,
        help="Metadata version_number to delete",
    )
    delete_cmd.add_argument(
        "--backup",
        action="store_true",
        help="Create a timestamped archive.db backup before deletion",
    )
    delete_cmd.add_argument(
        "--backup-dir",
        default="db_backups",
        help="Directory for backup files (default: db_backups)",
    )
    delete_cmd.set_defaults(func=delete_version)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
