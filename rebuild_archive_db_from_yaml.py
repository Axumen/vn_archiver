#!/usr/bin/env python3
"""Recreate archive.db by processing YAML metadata files in a folder tree."""

import argparse
import shutil
import sys
from pathlib import Path


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
            print(f"[WARN] Skipping {path} document #{idx}: expected mapping, got {type(doc).__name__}")
            continue
        out.append(doc)
    return out


def resync_canon_relationships(metadata_docs, get_connection, sync_canon_relationship):
    """Second-pass canon sync so parent/child links resolve regardless of YAML ordering."""
    rel_docs = [
        doc for doc in metadata_docs
        if isinstance(doc, dict)
        and str(doc.get("title", "")).strip()
        and (doc.get("parent_vn_title") or doc.get("relationship_type"))
    ]

    if not rel_docs:
        return 0

    rel_count = 0
    with get_connection() as conn:
        for doc in rel_docs:
            title = str(doc.get("title", "")).strip()
            vn_row = conn.execute(
                "SELECT id FROM visual_novels WHERE title = ?",
                (title,)
            ).fetchone()
            if not vn_row:
                print(f"[WARN] Could not find VN row during canon resync for title: {title}")
                continue

            sync_canon_relationship(conn, vn_row["id"], doc)
            rel_count += 1

    return rel_count


def rebuild_database(source_dir: Path, db_path: Path, backup_dir: Path | None = None):
    try:
        import yaml
    except ImportError as exc:
        raise RuntimeError("Missing dependency 'PyYAML'. Install with: pip install pyyaml") from exc

    import db_manager

    db_manager.DB_PATH = str(db_path)

    from vn_archiver import insert_visual_novel, is_artifact_metadata, sync_canon_relationship
    from db_manager import get_connection

    if db_path.exists():
        if backup_dir is not None:
            backup_dir.mkdir(parents=True, exist_ok=True)
            backup_path = backup_dir / f"{db_path.stem}.pre_rebuild{db_path.suffix}"
            shutil.copy2(db_path, backup_path)
            print(f"[INFO] Backed up existing DB to: {backup_path}")
        db_path.unlink()
        wal_path = db_path.with_suffix(db_path.suffix + "-wal")
        shm_path = db_path.with_suffix(db_path.suffix + "-shm")
        if wal_path.exists():
            wal_path.unlink()
        if shm_path.exists():
            shm_path.unlink()

    db_manager.initialize_database()

    yaml_files = find_yaml_files(source_dir)
    if not yaml_files:
        print(f"[WARN] No YAML files found under: {source_dir}")
        return 0, 0, 0

    file_count = 0
    metadata_count = 0
    all_docs = []
    build_docs = []
    artifact_docs = []

    for yaml_path in yaml_files:
        docs = load_metadata_documents(yaml, yaml_path)
        if not docs:
            continue

        file_count += 1
        for doc in docs:
            if not doc.get("title"):
                print(f"[WARN] Skipping metadata without title in {yaml_path}")
                continue
            if is_artifact_metadata(doc):
                artifact_docs.append((yaml_path, doc))
            else:
                build_docs.append((yaml_path, doc))

    for yaml_path, doc in build_docs:
        insert_visual_novel(doc)
        all_docs.append(doc)
        metadata_count += 1
        print(f"[OK] Processed build metadata from {yaml_path}")

    pending_artifacts = list(artifact_docs)
    max_passes = max(1, len(pending_artifacts))
    for _ in range(max_passes):
        if not pending_artifacts:
            break
        deferred = []
        progress = False
        for yaml_path, doc in pending_artifacts:
            try:
                insert_visual_novel(doc)
            except ValueError as exc:
                deferred.append((yaml_path, doc))
                print(f"[WARN] Deferred artifact metadata from {yaml_path}: {exc}")
                continue
            all_docs.append(doc)
            metadata_count += 1
            progress = True
            print(f"[OK] Processed artifact metadata from {yaml_path}")

        pending_artifacts = deferred
        if not progress:
            break

    if pending_artifacts:
        unresolved_sources = ", ".join(str(path) for path, _ in pending_artifacts[:5])
        if len(pending_artifacts) > 5:
            unresolved_sources += ", ..."
        raise RuntimeError(
            "Artifact metadata rebuild failed: mirror should contain successfully processable build/artifact metadata, "
            f"but {len(pending_artifacts)} artifact document(s) were unresolved. Sources: {unresolved_sources}"
        )

    relationship_count = resync_canon_relationships(all_docs, get_connection, sync_canon_relationship)
    if relationship_count:
        print(f"[OK] Re-synced canon relationships for {relationship_count} metadata document(s).")

    return file_count, metadata_count, relationship_count


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
        print(f"[ERROR] Source directory does not exist: {source_dir}")
        return 2

    try:
        file_count, metadata_count, relationship_count = rebuild_database(source_dir, db_path, backup_dir=backup_dir)
    except Exception as exc:
        print(f"[ERROR] Rebuild failed: {exc}")
        return 1

    print(
        f"[DONE] Rebuilt {db_path} from {metadata_count} metadata document(s) "
        f"across {file_count} YAML file(s); canon resync checked {relationship_count} relationship document(s)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
