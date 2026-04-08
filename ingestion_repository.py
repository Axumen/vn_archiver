class VnIngestionRepository:
    """
    Transitional repository for VN ingestion SQL workflows.

    This wraps existing ingestion helper operations behind a repository
    interface so domain services can depend on a smaller persistence surface.
    """

    def __init__(
        self,
        conn,
        *,
        upsert_series,
        upsert_visual_novel_record,
        sync_vn_tags,
        sync_canon_relationship,
        upsert_build_record,
        sync_build_target_platforms,
        resolve_existing_build_for_artifact,
    ):
        self.conn = conn
        self._upsert_series = upsert_series
        self._upsert_visual_novel_record = upsert_visual_novel_record
        self._sync_vn_tags = sync_vn_tags
        self._sync_canon_relationship = sync_canon_relationship
        self._upsert_build_record = upsert_build_record
        self._sync_build_target_platforms = sync_build_target_platforms
        self._resolve_existing_build_for_artifact = resolve_existing_build_for_artifact

    def resolve_existing_build_for_artifact(self, metadata):
        return self._resolve_existing_build_for_artifact(self.conn, metadata)

    def upsert_vn_and_build(self, metadata):
        series_id = self._upsert_series(self.conn, metadata)
        vn_id = self._upsert_visual_novel_record(self.conn, metadata, series_id)
        self._sync_vn_tags(self.conn, vn_id, metadata)
        self._sync_canon_relationship(self.conn, vn_id, metadata)

        build_id = self._upsert_build_record(self.conn, vn_id, metadata)
        self._sync_build_target_platforms(self.conn, build_id, metadata)
        return vn_id, build_id
