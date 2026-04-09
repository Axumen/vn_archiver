import sqlite3
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
    artifact_count: int = 1

    def __post_init__(self):
        if self.artifact_count < 1:
            raise ValueError("A Build must have at least one Artifact.")


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
class Artifact:
    """
    File-carrying artifact linked to a Build.

    Files are modeled as artifacts linked to builds through this object.
    """

    file_sha256: str
    build_id: int
    artifact_type: str | None = None
    platform: str | None = None
    source_url: str | None = None

    def __post_init__(self):
        if not self.file_sha256:
            raise ValueError("Artifact sha256 is required.")


@dataclass(frozen=True)
class IngestionResult:
    vn_id: int
    build_id: int
    artifact: Artifact | None = None
    build: Build | None = None
    vn: VN | None = None
    artifact_status: str | None = None

    def __post_init__(self):
        if self.build is not None and self.artifact is not None:
            if self.artifact.build_id != self.build.build_id:
                raise ValueError("Artifact must belong to the returned Build.")
        if self.build is not None:
            if self.build.vn_id != self.vn_id:
                raise ValueError("Build must belong to exactly one VN.")
            if self.build.build_id != self.build_id:
                raise ValueError("Build identity mismatch in ingestion result.")


class IngestionRepository(Protocol):
    def resolve_existing_build_for_artifact(self, metadata): ...

    def upsert_vn_and_build(self, metadata): ...

    def create_artifact(self, build_id, metadata, archive_data): ...

    def create_metadata_raw(self, raw_text, source_file, artifact_id): ...


class VisualNovelDomainService:
    """
    Domain-layer orchestration for VN archiving.

    This service centralizes the file -> Artifact -> Build -> VN flow so callers do
    not need to coordinate low-level SQL-oriented helper functions directly.
    """

    def __init__(
        self,
        conn,
        repository: IngestionRepository,
        *,
        is_artifact_metadata,
        collect_archives_for_db,
        process_archives_for_build,
    ):
        self.conn = conn
        self.repository = repository
        self.is_artifact_metadata = is_artifact_metadata
        self.collect_archives_for_db = collect_archives_for_db
        self.process_archives_for_build = process_archives_for_build

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
            artifact_count=max(1, len(archives_to_process)),
        )
        primary_archive = archives_to_process[0] if archives_to_process else {}
        file_sha256 = primary_archive.get("sha256") or metadata.get("sha256")
        if not file_sha256:
            raise ValueError("A Build must have at least one Artifact sha256.")
        artifact = Artifact(
            file_sha256=file_sha256,
            build_id=build_id,
            artifact_type=metadata.get("artifact_type"),
            platform=metadata.get("platform"),
            source_url=metadata.get("url"),
        )
        return artifact, build, vn

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

        if self.is_artifact_metadata(resolved_metadata):
            vn_id, build_id = self.repository.resolve_existing_build_for_artifact(resolved_metadata)
        else:
            vn_id, build_id = self.repository.upsert_vn_and_build(resolved_metadata)

        candidate_sha256 = None
        if archives_to_process:
            seen = set()
            for archive in archives_to_process:
                sha = archive.get("sha256")
                if not sha:
                    continue
                if sha in seen:
                    raise ValueError(f"Duplicate artifact sha256 in ingest payload: {sha}")
                seen.add(sha)
            candidate_sha256 = archives_to_process[0].get("sha256")
        if not candidate_sha256:
            candidate_sha256 = metadata.get("sha256")

        created_artifact_ids = []
        for archive_data in archives_to_process:
            sha = archive_data.get("sha256")
            path = archive_data.get("filepath") or archive_data.get("filename")
            if not sha or not path:
                continue
            artifact_id = self.repository.create_artifact(build_id, resolved_metadata, archive_data)
            if artifact_id is not None:
                created_artifact_ids.append(artifact_id)

        raw_text = metadata.get("_raw_text")
        source_file = metadata.get("_source_file")
        primary_artifact_id = created_artifact_ids[0] if created_artifact_ids else None
        if raw_text and primary_artifact_id is not None:
            self.repository.create_metadata_raw(raw_text, source_file, primary_artifact_id)

        self.process_archives_for_build(
            self.conn,
            build_id,
            resolved_metadata,
            vn_id,
            archives_to_process,
        )
        artifact, build, vn = self._build_domain_graph(
            resolved_metadata,
            archives_to_process,
            build_id=build_id,
            vn_id=vn_id,
        )
        return IngestionResult(
            vn_id=vn_id,
            build_id=build_id,
            artifact=artifact,
            build=build,
            vn=vn,
            artifact_status="classified",
        )
