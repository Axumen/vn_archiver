from dataclasses import dataclass
from typing import Protocol
from utils import normalize_language_value, normalize_version_value


@dataclass(frozen=True)
class Release:
    """
    Release-centric distributable aggregate.

    A Release represents a distributable release. Version is only a descriptor
    attached to Release and does not define identity on its own.
    """

    release_id: int
    title_id: int
    version: "Version"
    release_type: str | None = None
    release_status: str | None = None
    file_count: int = 1

    def __post_init__(self):
        if self.file_count < 1:
            raise ValueError("A Release must have at least one file.")


@dataclass(frozen=True)
class Title:
    canonical_title: str
    developer: str | None = None
    publisher: str | None = None


@dataclass(frozen=True)
class Version:
    """
    Descriptor for release labeling and ordering.

    Version is descriptive metadata, not an identity root.
    """

    version_string: str
    normalized_version: str | None = None


@dataclass(frozen=True)
class IngestionResult:
    title_id: int
    release_id: int
    metadata_version_number: int | None = None
    release: Release | None = None
    title: Title | None = None

    def __post_init__(self):
        if self.release is not None:
            if self.release.title_id != self.title_id:
                raise ValueError("Release must belong to exactly one Title.")
            if self.release.release_id != self.release_id:
                raise ValueError("Release identity mismatch in ingestion result.")


class IngestionRepository(Protocol):
    def get_or_create_title(self, metadata): ...

    def get_or_create_release(self, title_id, metadata): ...

    def create_file_link(self, release_id, metadata, archive_data): ...

    def create_metadata_raw(self, raw_payload, file_id, release_id=None): ...

    def create_file_attachment_metadata(self, release_id, file_id, metadata_dict): ...


class VisualNovelDomainService:
    """
    Domain-layer orchestration for VN archiving.

    This service centralizes the file -> Release -> Title flow so callers do
    not need to coordinate low-level SQL-oriented helper functions directly.
    """

    def __init__(
        self,
        conn,
        repository: IngestionRepository,
        *,
        collect_archives_for_db,
    ):
        self.conn = conn
        self.repository = repository
        self.collect_archives_for_db = collect_archives_for_db

    def _build_domain_graph(self, metadata, archives_to_process, *, release_id=None, title_id=None):
        if release_id is None or title_id is None:
            raise ValueError("Release and Title IDs must be resolved before domain graph creation.")

        title_obj = Title(
            canonical_title=metadata["title"],
            developer=metadata.get("developer"),
            publisher=metadata.get("publisher"),
        )
        version = Version(
            version_string=metadata.get("version", "unknown"),
            normalized_version=metadata.get("normalized_version"),
        )
        release = Release(
            release_id=release_id,
            title_id=title_id,
            version=version,
            release_type=metadata.get("release_type"),
            release_status=metadata.get("release_status"),
            file_count=max(1, len(archives_to_process)),
        )
        return release, title_obj

    def _prepare_resolution_metadata(self, metadata):
        """Stage 6 split: route Title vs Release metadata to the correct columns/entities."""
        resolved = dict(metadata)

        creator = resolved.get("creator")
        if creator and not resolved.get("developer"):
            resolved["developer"] = creator

        normalized_version = normalize_version_value(resolved.get("version") or resolved.get("version_string"))
        if normalized_version:
            resolved["version"] = normalized_version
            resolved["normalized_version"] = normalized_version.lower()

        normalized_language = normalize_language_value(resolved.get("language"))
        if normalized_language:
            resolved["language"] = normalized_language

        return resolved

    def ingest(self, metadata):
        if not metadata.get("title"):
            raise ValueError("Title is required.")

        archives_to_process, _ = self.collect_archives_for_db(metadata)

        resolved_metadata = self._prepare_resolution_metadata(metadata)
        title_id = self.repository.get_or_create_title(resolved_metadata)
        release_id = self.repository.get_or_create_release(title_id, resolved_metadata)

        if archives_to_process:
            seen = set()
            for archive in archives_to_process:
                sha = archive.get("sha256")
                if not sha:
                    continue
                if sha in seen:
                    raise ValueError(f"Duplicate file sha256 in ingest payload: {sha}")
                seen.add(sha)

        created_file_ids = []
        for archive_data in archives_to_process:
            sha = archive_data.get("sha256")
            path = archive_data.get("filepath") or archive_data.get("filename")
            if not sha or not path:
                continue
            file_id = self.repository.create_file_link(release_id, resolved_metadata, archive_data)
            if file_id is not None:
                created_file_ids.append(file_id)

        metadata_version_number = None
        raw_payload = dict(resolved_metadata)
        raw_payload.pop("_raw_text", None)
        raw_payload.pop("_source_file", None)
        primary_file_id = created_file_ids[0] if created_file_ids else None
        if raw_payload:
            metadata_version_number = self.repository.create_metadata_raw(
                raw_payload,
                primary_file_id,
                release_id=release_id,
            )

        release, title_obj = self._build_domain_graph(
            resolved_metadata,
            archives_to_process,
            release_id=release_id,
            title_id=title_id,
        )
        return IngestionResult(
            title_id=title_id,
            release_id=release_id,
            metadata_version_number=metadata_version_number,
            release=release,
            title=title_obj,
        )

    def attach_file_to_release(self, *, release_id, metadata, archive_data):
        """Attach a file to an existing release and snapshot file-level metadata."""
        file_id = self.repository.create_file_link(release_id, metadata, archive_data)
        if file_id is None:
            raise ValueError("Could not attach file to release: missing file identifier.")
        self.repository.create_file_attachment_metadata(release_id, file_id, metadata)
        return file_id
