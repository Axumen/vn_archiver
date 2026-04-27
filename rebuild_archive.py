#!/usr/bin/env python3
"""Convenience wrapper for rebuilding from metadata mirror folders."""

import argparse
import logging
import sys
from pathlib import Path

from rebuild_archive_db_from_yaml import rebuild_database

log = logging.getLogger(__name__)


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description="Rebuild archive DB from a collection of metadata YAML files."
    )
    parser.add_argument(
        "--source-dir",
        default="rebuild_metadata",
        help="Folder to scan recursively for metadata YAML files (default: rebuild_metadata).",
    )
    parser.add_argument(
        "--db-path",
        default="rebuild_metadata/archive_rebuild.db",
        help="Output DB path to rebuild (default: rebuild_metadata/archive_rebuild.db).",
    )
    parser.add_argument(
        "--backup-dir",
        default="db_backups",
        help="Directory to store pre-rebuild backup if DB exists (default: db_backups).",
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
        file_count, metadata_count = rebuild_database(
            source_dir=source_dir,
            db_path=db_path,
            backup_dir=backup_dir,
        )
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
