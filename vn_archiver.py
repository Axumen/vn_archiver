#!/usr/bin/env python3

import json
import os

from colorama import Fore

from db_manager import get_connection, exclusive_transaction
from domain_layer import VisualNovelDomainService
from ingestion_repository import VnIngestionRepository
from metadata_validation import validate_metadata_contract
from staging import stage_ingested_files_for_upload
from template_service import (
    detect_latest_metadata_template_version,
    load_metadata_template,
)
from utils import (
    normalize_version_for_sort,
    normalize_metadata_fields,
    CATEGORY_ALL_FIELDS,
    sha256_file,
)


# ==============================
# DATABASE
# ==============================

def get_latest_metadata_for_title(title):
    """Fetch metadata blob for the highest version build of a VN title, if present."""
    if not title:
        return {}

    normalized_title = str(title).strip()
    if not normalized_title:
        return {}

    with get_connection() as conn:
        rows = conn.execute(
            '''
            SELECT
                r.version AS release_version,
                r.release_id AS release_id,
                rev.revision_id AS revision_id,
                rev.raw_json AS metadata_json
            FROM title t
            JOIN release r ON r.title_id = t.title_id
            JOIN revision rev ON rev.release_id = r.release_id AND rev.is_current = 1
            WHERE TRIM(t.title) = TRIM(?) COLLATE NOCASE
            ''',
            (normalized_title,),
        ).fetchall()

    if not rows:
        return {}

    latest_row = max(
        rows,
        key=lambda row: (
            normalize_version_for_sort(row["release_version"]),
            int(row["release_id"] or 0),
            int(row["revision_id"] or 0),
        ),
    )

    if not latest_row["metadata_json"]:
        return {}

    try:
        parsed = json.loads(latest_row["metadata_json"])
        return parsed if isinstance(parsed, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


def collect_archives_for_db(metadata):
    archives_to_process = []

    top_level_sha = metadata.get("sha256")

    if top_level_sha:
        archives_to_process.append(
            {
                "sha256": top_level_sha,
                "size_bytes": metadata.get("size_bytes") or None,
                "filename": metadata.get("original_filename"),
                "artifact_type": metadata.get("artifact_type"),
            }
        )

    if "archives" in metadata and isinstance(metadata["archives"], list):
        for archive in metadata["archives"]:
            if isinstance(archive, dict) and archive.get("sha256"):
                archives_to_process.append(
                    {
                        "sha256": archive.get("sha256"),
                        "size_bytes": archive.get("size_bytes") or None,
                        "filename": archive.get("filename"),
                        "artifact_type": archive.get("artifact_type"),
                    }
                )

    if not top_level_sha and archives_to_process:
        top_level_sha = archives_to_process[0].get("sha256")

    return archives_to_process, top_level_sha


def insert_visual_novel(metadata):
    """Insert or update normalized metadata into the SQLite database."""
    metadata = normalize_metadata_fields(metadata)

    raw_text = metadata.pop("_raw_text", None)
    source_file = metadata.pop("_source_file", None)

    metadata_version = int(metadata.get("metadata_version") or detect_latest_metadata_template_version())
    template = load_metadata_template(metadata_version)
    validate_metadata_contract(metadata, template, CATEGORY_ALL_FIELDS)

    with get_connection() as conn:
        repository = VnIngestionRepository(conn)
        domain_service = VisualNovelDomainService(
            conn,
            repository=repository,
            collect_archives_for_db=collect_archives_for_db,
        )

        ingest_payload = dict(metadata)
        if raw_text is not None:
            ingest_payload["_raw_text"] = raw_text
        if source_file is not None:
            ingest_payload["_source_file"] = source_file

        with exclusive_transaction(conn):
            result = domain_service.ingest(ingest_payload)
        return result


def finalize_archive_creation(metadata, archives_data):
    """Shared finalization flow for prompted and pre-filled metadata runs."""
    if not archives_data:
        raise ValueError(
            "A release requires at least one archive file. "
            "Please provide a primary file before creating a release."
        )

    result = insert_visual_novel(metadata)
    if not result:
        print(Fore.RED + "Failed to insert visual novel into database.")
        return

    staged_paths, staged_meta_path = stage_ingested_files_for_upload(
        metadata,
        archives_data,
        result.metadata_version_number,
    )

    if staged_paths:
        for staged_path in staged_paths:
            print(Fore.GREEN + f"Staged archive for upload: {staged_path}")
    if staged_meta_path:
        print(Fore.GREEN + f"Staged metadata sidecar: {staged_meta_path}")


def create_archive_from_metadata_file(archive_paths, metadata, raw_text=None, source_file=None):
    """Create archive pipeline from an existing metadata YAML payload."""
    archives_data = []
    for path in archive_paths:
        print(f"Calculating SHA-256 for: {os.path.basename(path)}...")
        sha256 = sha256_file(path)
        file_size = os.path.getsize(path)
        archives_data.append(
            {
                "original_path": path,
                "filename": os.path.basename(path),
                "size_bytes": file_size,
                "sha256": sha256,
            }
        )

    prepared = dict(metadata or {})
    prepared.setdefault("metadata_version", detect_latest_metadata_template_version())
    if raw_text is not None:
        prepared["_raw_text"] = raw_text
    if source_file is not None:
        prepared["_source_file"] = source_file
    if archives_data:
        prepared["archives"] = [
            {
                "filename": a["filename"],
                "size_bytes": a["size_bytes"],
                "sha256": a["sha256"],
            }
            for a in archives_data
        ]

    finalize_archive_creation(prepared, archives_data)
