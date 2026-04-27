from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from db_manager import get_connection, exclusive_transaction
from domain_layer import VisualNovelDomainService
from ingestion_repository import VnIngestionRepository
from staging import stage_ingested_files_for_upload, stage_metadata_yaml_for_upload
from utils import sha256_file
from vn_archiver import insert_visual_novel


@dataclass
class AttachedFileResult:
    release_id: int
    file_id: int
    file_sha256: str
    file_size_bytes: int
    staged_archives: list
    file_sidecar_path: Path | None


@dataclass
class IncomingPairIngestionResult:
    release_id: int
    release_metadata_revision: int | None
    file_id: int
    file_sha256: str
    file_size_bytes: int
    release_sidecar_path: Path | None
    staged_archives: list
    file_sidecar_path: Path | None


def attach_file_to_release_pipeline(
    archive_path,
    release_id,
    file_metadata,
    *,
    release_id_for_staging=None,
):
    archive_name = Path(archive_path).name
    file_sha = sha256_file(archive_path)
    file_size = Path(archive_path).stat().st_size
    archived_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    metadata_payload = dict(file_metadata or {})
    metadata_payload["archives"] = [
        {"filename": archive_name, "sha256": file_sha, "size_bytes": file_size}
    ]

    with get_connection() as conn:
        repo = VnIngestionRepository(conn)
        domain_service = VisualNovelDomainService(
            conn,
            repository=repo,
            collect_archives_for_db=lambda _: ([], None),
        )
        with exclusive_transaction(conn):
            file_id = domain_service.attach_file_to_release(
                release_id=release_id,
                metadata={
                    **metadata_payload,
                    "archived_at": archived_at,
                    "artifact_type": metadata_payload.get("artifact_type"),
                },
                archive_data={
                    "sha256": file_sha,
                    "filename": archive_name,
                    "size_bytes": file_size,
                },
            )

    staged_archives, file_sidecar_path = stage_ingested_files_for_upload(
        metadata_payload,
        [{"original_path": archive_path, "filename": archive_name, "sha256": file_sha}],
        metadata_version_number=int(metadata_payload.get("metadata_version") or 1),
        release_id=release_id_for_staging or release_id,
    )

    return AttachedFileResult(
        release_id=release_id,
        file_id=file_id,
        file_sha256=file_sha,
        file_size_bytes=file_size,
        staged_archives=staged_archives,
        file_sidecar_path=file_sidecar_path,
    )


def ingest_incoming_pair(
    archive_path,
    release_metadata,
    file_metadata,
    *,
    raw_metadata_text=None,
    source_file=None,
):
    release_payload = dict(release_metadata or {})
    release_payload.pop("archives", None)
    if raw_metadata_text is not None:
        release_payload["_raw_text"] = raw_metadata_text
    if source_file is not None:
        release_payload["_source_file"] = source_file

    release_result = insert_visual_novel(release_payload)
    release_id = release_result.release_id
    release_revision = release_result.metadata_version_number

    file_result = attach_file_to_release_pipeline(
        archive_path,
        release_id,
        file_metadata,
        release_id_for_staging=release_id,
    )

    release_sidecar_path = stage_metadata_yaml_for_upload(
        release_payload,
        release_revision,
        sha256=file_result.file_sha256,
        release_id=release_id,
    )

    return IncomingPairIngestionResult(
        release_id=release_id,
        release_metadata_revision=release_revision,
        file_id=file_result.file_id,
        file_sha256=file_result.file_sha256,
        file_size_bytes=file_result.file_size_bytes,
        release_sidecar_path=release_sidecar_path,
        staged_archives=file_result.staged_archives,
        file_sidecar_path=file_result.file_sidecar_path,
    )
