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
        title = str(metadata.get("title") or "").strip()
        if not title:
            raise ValueError("Title is required for VN resolution.")

        existing = self.conn.execute(
            "SELECT id FROM vn WHERE TRIM(title) = TRIM(?) COLLATE NOCASE LIMIT 1",
            (title,),
        ).fetchone()
        if existing:
            return existing["id"]

        self.conn.execute(
            "INSERT INTO vn (title) VALUES (?)",
            (title,),
        )
        return self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    def _build_lookup_filters(self, metadata):
        version_value = str(metadata.get("version") or metadata.get("version_string") or "").strip()
        language = metadata.get("language")
        release_type = metadata.get("release_type")
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
        version_column = "version_string" if "version_string" in build_columns else (
            "version" if "version" in build_columns else "normalized_version"
        )

        where_clauses = ["vn_id = ?", f"{version_column} = ?"]
        params = [vn_id, version_value]

        if "language" in build_columns:
            where_clauses.append("COALESCE(language, '') = COALESCE(?, '')")
            params.append(language)
        if "release_type" in build_columns:
            where_clauses.append("COALESCE(release_type, '') = COALESCE(?, '')")
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
        version_value, language, release_type, platform = self._build_lookup_filters(metadata)
        if not version_value:
            version_value = "1.0"

        build_columns = {
            row[1]
            for row in self.conn.execute("PRAGMA table_info(builds)").fetchall()
        }
        version_column = "version_string" if "version_string" in build_columns else (
            "version" if "version" in build_columns else "normalized_version"
        )

        columns = ["vn_id", version_column]
        values = [vn_id, version_value]

        for optional_col, optional_val in (
            ("language", language),
            ("release_type", release_type),
            ("platform", platform),
        ):
            if optional_col in build_columns:
                columns.append(optional_col)
                values.append(optional_val)

        placeholders = ", ".join(["?"] * len(columns))
        self.conn.execute(
            f"INSERT INTO builds ({', '.join(columns)}) VALUES ({placeholders})",
            tuple(values),
        )
        return self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]

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
