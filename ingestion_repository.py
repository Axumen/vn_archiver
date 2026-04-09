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
        sync_build_relations,
        resolve_existing_build_for_artifact,
        create_artifact_record,
    ):
        self.conn = conn
        self._upsert_series = upsert_series
        self._upsert_visual_novel_record = upsert_visual_novel_record
        self._sync_vn_tags = sync_vn_tags
        self._sync_canon_relationship = sync_canon_relationship
        self._upsert_build_record = upsert_build_record
        self._sync_build_target_platforms = sync_build_target_platforms
        self._sync_build_relations = sync_build_relations
        self._resolve_existing_build_for_artifact = resolve_existing_build_for_artifact
        self._create_artifact_record = create_artifact_record

    def resolve_existing_build_for_artifact(self, metadata):
        return self._resolve_existing_build_for_artifact(self.conn, metadata)

    def get_or_create_vn(self, metadata):
        series_id = self._upsert_series(self.conn, metadata)
        vn_id = self._upsert_visual_novel_record(self.conn, metadata, series_id)
        self._sync_vn_tags(self.conn, vn_id, metadata)
        self._sync_canon_relationship(self.conn, vn_id, metadata)
        return vn_id

    def _build_lookup_filters(self, metadata):
        version_value = (metadata.get("normalized_version") or metadata.get("version") or "").strip()
        language = metadata.get("language")
        release_type = metadata.get("release_type") or metadata.get("build_type")
        platform = metadata.get("platform")
        return version_value, language, release_type, platform

    def find_build(self, vn_id, metadata):
        version_value, language, release_type, platform = self._build_lookup_filters(metadata)
        if not version_value:
            return None

        build_columns = {
            row[1]
            for row in self.conn.execute("PRAGMA table_info(builds)").fetchall()
        }
        version_column = "normalized_version" if "normalized_version" in build_columns else (
            "version" if "version" in build_columns else "version_string"
        )

        where_clauses = ["vn_id = ?", f"{version_column} = ?"]
        params = [vn_id, version_value]

        if "language" in build_columns:
            where_clauses.append("COALESCE(language, '') = COALESCE(?, '')")
            params.append(language)
        if "release_type" in build_columns:
            where_clauses.append("COALESCE(release_type, '') = COALESCE(?, '')")
            params.append(release_type)
        elif "build_type" in build_columns:
            where_clauses.append("COALESCE(build_type, '') = COALESCE(?, '')")
            params.append(release_type)
        if "platform" in build_columns:
            where_clauses.append("COALESCE(platform, '') = COALESCE(?, '')")
            params.append(platform)

        row = self.conn.execute(
            f"SELECT id FROM builds WHERE {' AND '.join(where_clauses)} ORDER BY id DESC LIMIT 1",
            tuple(params),
        ).fetchone()
        return row["id"] if row else None

    def create_build(self, vn_id, metadata):
        build_id = self._upsert_build_record(self.conn, vn_id, metadata)
        self._sync_build_target_platforms(self.conn, build_id, metadata)
        self._sync_build_relations(self.conn, build_id, metadata)
        return build_id

    def get_or_create_build(self, vn_id, metadata):
        existing = self.find_build(vn_id, metadata)
        if existing:
            return existing
        return self.create_build(vn_id, metadata)

    def upsert_vn_and_build(self, metadata):
        vn_id = self.get_or_create_vn(metadata)
        build_id = self.get_or_create_build(vn_id, metadata)
        return vn_id, build_id

    def create_artifact(self, build_id, metadata, archive_data):
        return self._create_artifact_record(self.conn, build_id, metadata, archive_data)

    def create_metadata_raw(self, raw_text, source_file, artifact_id):
        self.conn.execute(
            "INSERT INTO metadata_raw (raw_text, source_file, artifact_id) VALUES (?, ?, ?)",
            (raw_text, source_file, artifact_id),
        )
