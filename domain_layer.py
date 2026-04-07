from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class IngestionResult:
    vn_id: int
    build_id: int


class IngestionRepository(Protocol):
    def resolve_existing_build_for_artifact(self, metadata): ...

    def upsert_vn_and_build(self, metadata): ...


class VisualNovelDomainService:
    """
    Domain-layer orchestration for VN archiving.

    This service centralizes the VN -> Version -> Artifact flow so callers do
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

    def ingest(self, metadata):
        if not metadata.get("title"):
            raise ValueError("Title is required.")

        if self.is_artifact_metadata(metadata):
            vn_id, build_id = self.repository.resolve_existing_build_for_artifact(metadata)
        else:
            vn_id, build_id = self.repository.upsert_vn_and_build(metadata)

        archives_to_process, _ = self.collect_archives_for_db(metadata)
        self.process_archives_for_build(
            self.conn,
            build_id,
            metadata,
            vn_id,
            archives_to_process,
        )
        return IngestionResult(vn_id=vn_id, build_id=build_id)
