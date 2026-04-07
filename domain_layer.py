from dataclasses import dataclass


@dataclass(frozen=True)
class IngestionResult:
    vn_id: int
    build_id: int


class VisualNovelDomainService:
    """
    Domain-layer orchestration for VN archiving.

    This service centralizes the VN -> Version -> Artifact flow so callers do
    not need to coordinate low-level SQL-oriented helper functions directly.
    """

    def __init__(
        self,
        conn,
        *,
        is_artifact_metadata,
        upsert_series,
        upsert_visual_novel_record,
        sync_vn_tags,
        sync_canon_relationship,
        upsert_build_record,
        sync_build_target_platforms,
        resolve_existing_build_for_artifact,
        collect_archives_for_db,
        process_archives_for_build,
    ):
        self.conn = conn
        self.is_artifact_metadata = is_artifact_metadata
        self.upsert_series = upsert_series
        self.upsert_visual_novel_record = upsert_visual_novel_record
        self.sync_vn_tags = sync_vn_tags
        self.sync_canon_relationship = sync_canon_relationship
        self.upsert_build_record = upsert_build_record
        self.sync_build_target_platforms = sync_build_target_platforms
        self.resolve_existing_build_for_artifact = resolve_existing_build_for_artifact
        self.collect_archives_for_db = collect_archives_for_db
        self.process_archives_for_build = process_archives_for_build

    def ingest(self, metadata):
        if not metadata.get("title"):
            raise ValueError("Title is required.")

        if self.is_artifact_metadata(metadata):
            vn_id, build_id = self.resolve_existing_build_for_artifact(self.conn, metadata)
        else:
            vn_id, build_id = self._upsert_vn_and_build(metadata)

        archives_to_process, _ = self.collect_archives_for_db(metadata)
        self.process_archives_for_build(
            self.conn,
            build_id,
            metadata,
            vn_id,
            archives_to_process,
        )
        return IngestionResult(vn_id=vn_id, build_id=build_id)

    def _upsert_vn_and_build(self, metadata):
        series_id = self.upsert_series(self.conn, metadata)
        vn_id = self.upsert_visual_novel_record(self.conn, metadata, series_id)
        self.sync_vn_tags(self.conn, vn_id, metadata)
        self.sync_canon_relationship(self.conn, vn_id, metadata)

        build_id = self.upsert_build_record(self.conn, vn_id, metadata)
        self.sync_build_target_platforms(self.conn, build_id, metadata)
        return vn_id, build_id
