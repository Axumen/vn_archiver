from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class Build:
    """
    Build-centric release aggregate.

    A Build represents a distributable release. Version is only a descriptor
    attached to Build and does not define identity on its own.
    """

    build_id: int
    vn_id: int
    version: "Version"
    release_type: str | None = None
    release_status: str | None = None
    file_count: int = 1

    def __post_init__(self):
        if self.file_count < 1:
            raise ValueError("A Build must have at least one file.")


@dataclass(frozen=True)
class VN:
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
    vn_id: int
    build_id: int
    metadata_version_number: int | None = None
    build: Build | None = None
    vn: VN | None = None

    def __post_init__(self):
        if self.build is not None:
            if self.build.vn_id != self.vn_id:
                raise ValueError("Build must belong to exactly one VN.")
            if self.build.build_id != self.build_id:
                raise ValueError("Build identity mismatch in ingestion result.")


class IngestionRepository(Protocol):
    def get_or_create_vn(self, metadata): ...

    def get_or_create_build(self, vn_id, metadata): ...

    def create_file_link(self, build_id, metadata, archive_data): ...

    def create_metadata_raw(self, raw_payload, file_id, build_id=None): ...


class VisualNovelDomainService:
    """
    Domain-layer orchestration for VN archiving.

    This service centralizes the file -> Build -> VN flow so callers do
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

    def _build_domain_graph(self, metadata, archives_to_process, *, build_id=None, vn_id=None):
        if build_id is None or vn_id is None:
            raise ValueError("Build and VN IDs must be resolved before domain graph creation.")

        vn = VN(
            canonical_title=metadata["title"],
            developer=metadata.get("developer"),
            publisher=metadata.get("publisher"),
        )
        version = Version(
            version_string=metadata.get("version", "unknown"),
            normalized_version=metadata.get("normalized_version"),
        )
        build = Build(
            build_id=build_id,
            vn_id=vn_id,
            version=version,
            release_type=metadata.get("release_type"),
            release_status=metadata.get("release_status"),
            file_count=max(1, len(archives_to_process)),
        )
        return build, vn

    @staticmethod
    def normalize_version(version_value):
        """Normalize user-provided version labels (e.g. v1.0 -> 1.0)."""
        version_text = str(version_value or "").strip()
        if not version_text:
            return ""
        if version_text.lower().startswith("v") and len(version_text) > 1:
            return version_text[1:].strip()
        return version_text

    @staticmethod
    def normalize_language(language_value):
        """Normalize language labels to stable ingest keys."""
        language_text = str(language_value or "").strip()
        if not language_text:
            return ""
        if language_text.isalpha() and len(language_text) <= 3:
            return language_text.upper()
        return language_text.lower()

    def _prepare_resolution_metadata(self, metadata):
        """Stage 6 split: route VN vs Build metadata to the correct columns/entities."""
        resolved = dict(metadata)

        creator = resolved.get("creator")
        if creator and not resolved.get("developer"):
            resolved["developer"] = creator

        normalized_version = self.normalize_version(resolved.get("version") or resolved.get("version_string"))
        if normalized_version:
            resolved["version"] = normalized_version
            resolved["normalized_version"] = normalized_version.lower()

        normalized_language = self.normalize_language(resolved.get("language"))
        if normalized_language:
            resolved["language"] = normalized_language

        return resolved

    def ingest(self, metadata):
        if not metadata.get("title"):
            raise ValueError("Title is required.")

        archives_to_process, _ = self.collect_archives_for_db(metadata)

        resolved_metadata = self._prepare_resolution_metadata(metadata)
        vn_id = self.repository.get_or_create_vn(resolved_metadata)
        build_id = self.repository.get_or_create_build(vn_id, resolved_metadata)

        candidate_sha256 = None
        if archives_to_process:
            seen = set()
            for archive in archives_to_process:
                sha = archive.get("sha256")
                if not sha:
                    continue
                if sha in seen:
                    raise ValueError(f"Duplicate file sha256 in ingest payload: {sha}")
                seen.add(sha)
            candidate_sha256 = archives_to_process[0].get("sha256")
        if not candidate_sha256:
            candidate_sha256 = metadata.get("sha256")

        created_file_ids = []
        for archive_data in archives_to_process:
            sha = archive_data.get("sha256")
            path = archive_data.get("filepath") or archive_data.get("filename")
            if not sha or not path:
                continue
            file_id = self.repository.create_file_link(build_id, resolved_metadata, archive_data)
            if file_id is not None:
                created_file_ids.append(file_id)

        metadata_version_number = None
        raw_payload = dict(resolved_metadata)
        raw_payload.pop("_raw_text", None)
        raw_payload.pop("_source_file", None)
        primary_file_id = created_file_ids[0] if created_file_ids else None
        if raw_payload and primary_file_id is not None:
            metadata_version_number = self.repository.create_metadata_raw(
                raw_payload,
                primary_file_id,
                build_id=build_id,
            )

        build, vn = self._build_domain_graph(
            resolved_metadata,
            archives_to_process,
            build_id=build_id,
            vn_id=vn_id,
        )
        return IngestionResult(
            vn_id=vn_id,
            build_id=build_id,
            metadata_version_number=metadata_version_number,
            build=build,
            vn=vn,
        )
