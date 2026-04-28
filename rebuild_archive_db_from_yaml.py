#!/usr/bin/env python3
"""Recreate archive.db by processing YAML metadata files in a folder tree."""

import argparse
import logging
import shutil
import sys
from pathlib import Path

log = logging.getLogger(__name__)


def find_yaml_files(root: Path):
    return sorted(
        [
            p for p in root.rglob("*.yaml")
            if p.is_file()
            and p.name.lower() != "backblaze_config.yaml"
            and "metadata_templates" not in p.parts
        ]
    )


def load_metadata_documents(yaml_module, path: Path):
    with path.open("r", encoding="utf-8") as handle:
        docs = [doc for doc in yaml_module.safe_load_all(handle) if doc is not None]

    out = []
    for idx, doc in enumerate(docs, start=1):
        if not isinstance(doc, dict):
            log.warning("Skipping %s document #%d: expected mapping, got %s", path, idx, type(doc).__name__)
            continue
        out.append(doc)
    return out


def _snapshot_cloud_tracking(db_path: Path) -> tuple[list, list]:
    """Dump cloud_archive and cloud_sidecar rows before the DB is wiped.

    Returns (cloud_archive_rows, cloud_sidecar_rows) where each element is a
    list of (sha256, file_size, storage_path) tuples.  Returns empty lists if
    the tables do not exist or the DB cannot be read.
    """
    import sqlite3 as _sqlite3

    cloud_archive_rows: list[tuple] = []
    cloud_sidecar_rows: list[tuple] = []
    try:
        conn = _sqlite3.connect(str(db_path))
        conn.row_factory = _sqlite3.Row
        existing_tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        if "cloud_archive" in existing_tables:
            cloud_archive_rows = [
                (r["sha256"], r["file_size"], r["storage_path"])
                for r in conn.execute(
                    "SELECT sha256, file_size, storage_path FROM cloud_archive"
                ).fetchall()
            ]
        if "cloud_sidecar" in existing_tables:
            cloud_sidecar_rows = [
                (r["sha256"], r["file_size"], r["storage_path"])
                for r in conn.execute(
                    "SELECT sha256, file_size, storage_path FROM cloud_sidecar"
                ).fetchall()
            ]
        conn.close()
    except Exception as exc:  # pragma: no cover
        log.warning("Could not snapshot cloud tracking tables (data may be lost): %s", exc)
    return cloud_archive_rows, cloud_sidecar_rows


def _restore_cloud_tracking(
    cloud_archive_rows: list[tuple],
    cloud_sidecar_rows: list[tuple],
) -> None:
    """Reinsert snapshotted cloud tracking rows into the freshly rebuilt DB."""
    if not cloud_archive_rows and not cloud_sidecar_rows:
        return
    import db_manager

    with db_manager.get_connection() as conn:
        if cloud_archive_rows:
            conn.executemany(
                "INSERT OR IGNORE INTO cloud_archive (sha256, file_size, storage_path)"
                " VALUES (?, ?, ?)",
                cloud_archive_rows,
            )
        if cloud_sidecar_rows:
            conn.executemany(
                "INSERT OR IGNORE INTO cloud_sidecar (sha256, file_size, storage_path)"
                " VALUES (?, ?, ?)",
                cloud_sidecar_rows,
            )
        conn.commit()
    log.info(
        "Restored cloud tracking: %d cloud_archive row(s), %d cloud_sidecar row(s).",
        len(cloud_archive_rows),
        len(cloud_sidecar_rows),
    )


def rebuild_database(source_dir: Path, db_path: Path, backup_dir: Path | None = None):
    try:
        import yaml
    except ImportError as exc:
        raise RuntimeError("Missing dependency 'PyYAML'. Install with: pip install pyyaml") from exc

    import db_manager

    db_manager.DB_PATH = str(db_path)

    from vn_archiver import insert_visual_novel

    # Snapshot cloud tracking data before the DB is erased so it can be
    # restored afterwards.  This prevents every file appearing as "not
    # uploaded" after a rebuild.
    cloud_archive_rows, cloud_sidecar_rows = (
        _snapshot_cloud_tracking(db_path) if db_path.exists() else ([], [])
    )
    if cloud_archive_rows or cloud_sidecar_rows:
        log.info(
            "Snapshotted %d cloud_archive and %d cloud_sidecar row(s) for restoration.",
            len(cloud_archive_rows),
            len(cloud_sidecar_rows),
        )

    if db_path.exists():
        if backup_dir is not None:
            backup_dir.mkdir(parents=True, exist_ok=True)
            backup_path = backup_dir / f"{db_path.stem}.pre_rebuild{db_path.suffix}"
            shutil.copy2(db_path, backup_path)
            log.info("Backed up existing DB to: %s", backup_path)
        db_path.unlink()
        wal_path = db_path.with_suffix(db_path.suffix + "-wal")
        shm_path = db_path.with_suffix(db_path.suffix + "-shm")
        if wal_path.exists():
            wal_path.unlink()
        if shm_path.exists():
            shm_path.unlink()

    db_manager.initialize_database(reset=True)
    _restore_cloud_tracking(cloud_archive_rows, cloud_sidecar_rows)

    yaml_files = find_yaml_files(source_dir)
    if not yaml_files:
        log.warning("No YAML files found under: %s", source_dir)
        return 0, 0

    file_count = 0
    metadata_count = 0

    for yaml_path in yaml_files:
        docs = load_metadata_documents(yaml, yaml_path)
        if not docs:
            continue

        file_count += 1
        for doc in docs:
            if not doc.get("title"):
                log.warning("Skipping metadata without title in %s", yaml_path)
                continue
            try:
                insert_visual_novel(doc)
                metadata_count += 1
                log.info("Processed metadata from %s", yaml_path)
            except Exception as exc:
                log.error("Failed to process metadata from %s: %s", yaml_path, exc, exc_info=True)

    return file_count, metadata_count


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description="Recreate archive.db by processing all YAML metadata files in a folder."
    )
    parser.add_argument(
        "--source-dir",
        default=".",
        help="Folder to scan recursively for YAML files (default: current directory).",
    )
    parser.add_argument(
        "--db-path",
        default="archive.db",
        help="Path to archive.db file to recreate (default: archive.db).",
    )
    parser.add_argument(
        "--backup-dir",
        default="db_backups",
        help="Directory to store a pre-rebuild backup if DB already exists (default: db_backups).",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Do not make a pre-rebuild backup of an existing DB.",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv or sys.argv[1:])

    source_dir = Path(args.source_dir).resolve()
    db_path = Path(args.db_path).resolve()
    backup_dir = None if args.no_backup else Path(args.backup_dir).resolve()

    if not source_dir.exists() or not source_dir.is_dir():
        log.error("Source directory does not exist: %s", source_dir)
        return 2

    try:
        file_count, metadata_count = rebuild_database(source_dir, db_path, backup_dir=backup_dir)
    except Exception as exc:
        log.error("Rebuild failed: %s", exc, exc_info=True)
        return 1

    log.info(
        "Rebuilt %s from %d metadata document(s) across %d YAML file(s).",
        db_path, metadata_count, file_count,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
