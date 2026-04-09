from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class Build:
    """
    Build-centric release aggregate.

    A Build represents a distributable release. Version is only a descriptor
    attached to Build and does not define identity on its own.
    """

    build_id: int | None
    vn_id: int | None
    version: "Version"
    release_type: str | None = None
    release_status: str | None = None


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

    file_sha256: str | None
    build: Build
    artifact_type: str | None = None
    platform: str | None = None
    source_url: str | None = None


@dataclass(frozen=True)
class IngestionResult:
    vn_id: int
    build_id: int
    artifact: Artifact | None = None
    build: Build | None = None
    vn: VN | None = None


class IngestionRepository(Protocol):
    def resolve_existing_build_for_artifact(self, metadata): ...

    def upsert_vn_and_build(self, metadata): ...


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
        )
        primary_archive = archives_to_process[0] if archives_to_process else {}
        file_sha256 = primary_archive.get("sha256") or metadata.get("sha256")
        artifact = Artifact(
            file_sha256=file_sha256,
            build=build,
            artifact_type=metadata.get("artifact_type"),
            platform=metadata.get("platform"),
            source_url=metadata.get("url"),
        )
        return artifact, build, vn

    def ingest(self, metadata):
        if not metadata.get("title"):
            raise ValueError("Title is required.")

        archives_to_process, _ = self.collect_archives_for_db(metadata)

        if self.is_artifact_metadata(metadata):
            vn_id, build_id = self.repository.resolve_existing_build_for_artifact(metadata)
        else:
            vn_id, build_id = self.repository.upsert_vn_and_build(metadata)

        self.process_archives_for_build(
            self.conn,
            build_id,
            metadata,
            vn_id,
            archives_to_process,
        )
        artifact, build, vn = self._build_domain_graph(
            metadata,
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
        )
