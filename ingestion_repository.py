class VnIngestionRepository:
    """Repository adapter for VN/build/file ingestion across schema variants.

    Supports both:
    - legacy tables (`vn`, `builds`, `artifacts`)
    - new domain tables (`vn`, `build`, `file`, `build_file`)
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

        self._resolve_schema()

    def _table_exists(self, table_name):
        row = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        ).fetchone()
        return row is not None

    def _table_columns(self, table_name):
        if not self._table_exists(table_name):
            return set()
        rows = self.conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {row[1] for row in rows}

    def _resolve_schema(self):
        self.vn_table = "vn"
        vn_columns = self._table_columns(self.vn_table)
        self.vn_id_column = "vn_id" if "vn_id" in vn_columns else "id"

        self.build_table = "build" if self._table_exists("build") else "builds"
        build_columns = self._table_columns(self.build_table)
        self.build_id_column = "build_id" if "build_id" in build_columns else "id"
        self.build_version_column = "version" if "version" in build_columns else "version_string"
        self.build_platform_column = "target_platform" if "target_platform" in build_columns else "platform"

        self.has_artifacts_table = self._table_exists("artifacts")
        self.has_file_link_tables = self._table_exists("file") and self._table_exists("build_file")

    def resolve_existing_build_for_artifact(self, metadata):
        return self._resolve_existing_build_for_artifact(self.conn, metadata)

    def get_or_create_vn(self, metadata):
        title = str(metadata.get("title") or "").strip()
        if not title:
            raise ValueError("Title is required for VN resolution.")

        existing = self.conn.execute(
            f"SELECT {self.vn_id_column} FROM {self.vn_table} WHERE TRIM(title) = TRIM(?) COLLATE NOCASE LIMIT 1",
            (title,),
        ).fetchone()
        if existing:
            return existing[self.vn_id_column]

        self.conn.execute(
            f"INSERT INTO {self.vn_table} (title) VALUES (?)",
            (title,),
        )
        return self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    def _build_lookup_filters(self, metadata):
        version_value = str(metadata.get("version") or "").strip()
        language = metadata.get("language")
        release_type = metadata.get("release_type")
        platform = metadata.get("platform") or metadata.get("target_platform")
        return version_value, language, release_type, platform

    def find_build(self, vn_id, metadata):
        version_value, language, release_type, platform = self._build_lookup_filters(metadata)
        if not version_value:
            return None

        columns = self._table_columns(self.build_table)
        where_clauses = ["vn_id = ?", f"{self.build_version_column} = ?"]
        params = [vn_id, version_value]

        if "language" in columns:
            where_clauses.append("COALESCE(language, '') = COALESCE(?, '')")
            params.append(language)
        if "release_type" in columns:
            where_clauses.append("COALESCE(release_type, '') = COALESCE(?, '')")
            params.append(release_type)
        if self.build_platform_column in columns:
            where_clauses.append(
                f"COALESCE({self.build_platform_column}, '') = COALESCE(?, '')"
            )
            params.append(platform)

        row = self.conn.execute(
            f"SELECT {self.build_id_column} FROM {self.build_table} WHERE {' AND '.join(where_clauses)} ORDER BY {self.build_id_column} DESC LIMIT 1",
            tuple(params),
        ).fetchone()
        return row[self.build_id_column] if row else None

    def create_build(self, vn_id, metadata):
        version_value, language, release_type, platform = self._build_lookup_filters(metadata)
        if not version_value:
            version_value = "1.0"

        columns = self._table_columns(self.build_table)
        insert_columns = ["vn_id", self.build_version_column]
        values = [vn_id, version_value]

        if "language" in columns:
            insert_columns.append("language")
            values.append(language)
        if "release_type" in columns:
            insert_columns.append("release_type")
            values.append(release_type)
        if self.build_platform_column in columns:
            insert_columns.append(self.build_platform_column)
            values.append(platform)

        placeholders = ", ".join(["?"] * len(insert_columns))
        self.conn.execute(
            f"INSERT INTO {self.build_table} ({', '.join(insert_columns)}) VALUES ({placeholders})",
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

    def _create_artifact_in_legacy_table(self, build_id, metadata, archive_data):
        artifact_sha = archive_data.get("sha256")
        if not artifact_sha:
            return None

        artifact_path = archive_data.get("filepath") or archive_data.get("filename") or metadata.get("original_filename")
        artifact_type = str(metadata.get("artifact_type") or "game_archive").strip().lower() or "game_archive"

        existing = self.conn.execute(
            """
            SELECT id
            FROM artifacts
            WHERE sha256 = ?
              AND COALESCE(build_id, -1) = COALESCE(?, -1)
            LIMIT 1
            """,
            (artifact_sha, build_id),
        ).fetchone()
        if existing:
            return existing["id"]

        self.conn.execute(
            "INSERT INTO artifacts (build_id, sha256, path, type) VALUES (?, ?, ?, ?)",
            (build_id, artifact_sha, artifact_path or artifact_sha, artifact_type),
        )
        return self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    def _create_artifact_in_file_tables(self, build_id, metadata, archive_data):
        artifact_sha = archive_data.get("sha256")
        if not artifact_sha:
            return None

        filename = archive_data.get("filepath") or archive_data.get("filename") or metadata.get("original_filename")

        file_row = self.conn.execute(
            "SELECT file_id FROM file WHERE sha256 = ? LIMIT 1",
            (artifact_sha,),
        ).fetchone()
        if file_row:
            file_id = file_row["file_id"]
        else:
            self.conn.execute(
                "INSERT INTO file (sha256, filename) VALUES (?, ?)",
                (artifact_sha, filename),
            )
            file_id = self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        link_row = self.conn.execute(
            "SELECT 1 FROM build_file WHERE build_id = ? AND file_id = ? LIMIT 1",
            (build_id, file_id),
        ).fetchone()
        if not link_row:
            self.conn.execute(
                "INSERT INTO build_file (build_id, file_id, original_filename, archived_at) VALUES (?, ?, ?, ?)",
                (build_id, file_id, filename, metadata.get("archived_at")),
            )

        return file_id

    def create_artifact(self, build_id, metadata, archive_data):
        if self.has_artifacts_table:
            return self._create_artifact_in_legacy_table(build_id, metadata, archive_data)
        if self.has_file_link_tables:
            return self._create_artifact_in_file_tables(build_id, metadata, archive_data)
        raise RuntimeError("No supported artifact/file persistence tables found in current schema.")

    def create_metadata_raw(self, raw_text, source_file, artifact_id):
        if not self._table_exists("metadata_raw"):
            return
        self.conn.execute(
            "INSERT INTO metadata_raw (raw_text, source_file, artifact_id) VALUES (?, ?, ?)",
            (raw_text, source_file, artifact_id),
        )
