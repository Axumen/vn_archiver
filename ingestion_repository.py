class VnIngestionRepository:
    """Repository adapter for VN/build/file ingestion across schema variants.

    Supports both:
    - legacy build tables (`vn`, `builds`)
    - new domain tables (`vn`, `build`, `file`, `build_file`, `tags`, `vn_tags`)
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

        self.has_file_link_tables = self._table_exists("file") and self._table_exists("build_file")

    @staticmethod
    def _normalize_text_value(value):
        if value is None:
            return None
        if isinstance(value, str):
            normalized = value.strip()
            return normalized or None
        if isinstance(value, list):
            parts = [str(item).strip() for item in value if str(item).strip()]
            return ", ".join(parts) if parts else None
        return str(value).strip() or None

    @staticmethod
    def _normalize_translator_value(value):
        if value is None:
            return None
        if isinstance(value, str):
            normalized = value.strip()
            return normalized or None
        if isinstance(value, list):
            parts = [str(item).strip() for item in value if str(item).strip()]
            return ", ".join(parts) if parts else None
        if isinstance(value, dict):
            normalized_chunks = []
            for language_key, translators in value.items():
                key = str(language_key).strip()
                if not key:
                    continue
                if isinstance(translators, list):
                    names = [str(name).strip() for name in translators if str(name).strip()]
                else:
                    single = str(translators).strip()
                    names = [single] if single else []
                if names:
                    normalized_chunks.append(f"{key}: {', '.join(names)}")
            return " | ".join(normalized_chunks) if normalized_chunks else None
        return str(value).strip() or None

    @staticmethod
    def _normalize_tag_list(value):
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item).strip().lower() for item in value if str(item).strip()]
        if isinstance(value, str):
            return [part.strip().lower() for part in value.split(",") if part.strip()]
        normalized = str(value).strip().lower()
        return [normalized] if normalized else []

    def _sync_vn_tags_if_supported(self, vn_id, tags_value):
        if not self._table_exists("tags") or not self._table_exists("vn_tags"):
            return

        tags = self._normalize_tag_list(tags_value)
        self.conn.execute("DELETE FROM vn_tags WHERE vn_id = ?", (vn_id,))
        for tag in tags:
            tag_row = self.conn.execute("SELECT tag_id FROM tags WHERE name = ? LIMIT 1", (tag,)).fetchone()
            if tag_row:
                tag_id = int(tag_row["tag_id"])
            else:
                self.conn.execute("INSERT INTO tags (name) VALUES (?)", (tag,))
                tag_id = int(self.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            self.conn.execute(
                "INSERT OR IGNORE INTO vn_tags (vn_id, tag_id) VALUES (?, ?)",
                (vn_id, tag_id),
            )

    def resolve_existing_build_for_artifact(self, metadata):
        return self._resolve_existing_build_for_artifact(self.conn, metadata)

    def get_or_create_vn(self, metadata):
        title = str(metadata.get("title") or "").strip()
        if not title:
            raise ValueError("Title is required for VN resolution.")

        vn_columns = self._table_columns(self.vn_table)
        vn_updatable_columns = [
            "series",
            "series_description",
            "aliases",
            "developer",
            "publisher",
            "release_status",
            "content_rating",
            "content_mode",
            "content_type",
            "description",
            "source",
            "tags",
            "original_release_date",
        ]

        vn_values = {}
        for column_name in vn_updatable_columns:
            if column_name not in vn_columns:
                continue
            if column_name not in metadata:
                continue
            vn_values[column_name] = self._normalize_text_value(metadata.get(column_name))

        existing = self.conn.execute(
            f"SELECT {self.vn_id_column} FROM {self.vn_table} WHERE TRIM(title) = TRIM(?) COLLATE NOCASE LIMIT 1",
            (title,),
        ).fetchone()
        if existing:
            vn_id = existing[self.vn_id_column]
            if vn_values:
                assignments = ", ".join(f"{column} = ?" for column in vn_values)
                self.conn.execute(
                    f"UPDATE {self.vn_table} SET {assignments} WHERE {self.vn_id_column} = ?",
                    tuple(vn_values.values()) + (vn_id,),
                )
            self._sync_vn_tags_if_supported(vn_id, metadata.get("tags"))
            return vn_id

        insert_columns = ["title"]
        insert_values = [title]
        for column_name, value in vn_values.items():
            insert_columns.append(column_name)
            insert_values.append(value)

        placeholders = ", ".join(["?"] * len(insert_columns))
        self.conn.execute(
            f"INSERT INTO {self.vn_table} ({', '.join(insert_columns)}) VALUES ({placeholders})",
            tuple(insert_values),
        )
        vn_id = self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        self._sync_vn_tags_if_supported(vn_id, metadata.get("tags"))
        return vn_id

    def _build_lookup_filters(self, metadata):
        version_value = str(metadata.get("version") or "").strip()
        language = metadata.get("language")
        build_type = metadata.get("build_type") or metadata.get("release_type")
        platform = metadata.get("platform") or metadata.get("target_platform")
        return version_value, language, build_type, platform

    def find_build(self, vn_id, metadata):
        version_value, language, build_type, platform = self._build_lookup_filters(metadata)
        if not version_value:
            return None

        columns = self._table_columns(self.build_table)
        where_clauses = ["vn_id = ?", f"{self.build_version_column} = ?"]
        params = [vn_id, version_value]

        if "language" in columns:
            where_clauses.append("COALESCE(language, '') = COALESCE(?, '')")
            params.append(language)
        if "build_type" in columns:
            where_clauses.append("COALESCE(build_type, '') = COALESCE(?, '')")
            params.append(build_type)
        elif "release_type" in columns:
            where_clauses.append("COALESCE(release_type, '') = COALESCE(?, '')")
            params.append(build_type)
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
        version_value, language, build_type, platform = self._build_lookup_filters(metadata)
        if not version_value:
            version_value = "1.0"

        columns = self._table_columns(self.build_table)
        insert_columns = ["vn_id", self.build_version_column]
        values = [vn_id, version_value]

        if "language" in columns:
            insert_columns.append("language")
            values.append(language)
        if "build_type" in columns:
            insert_columns.append("build_type")
            values.append(build_type)
        elif "release_type" in columns:
            insert_columns.append("release_type")
            values.append(build_type)
        if self.build_platform_column in columns:
            insert_columns.append(self.build_platform_column)
            values.append(platform)

        build_column_to_metadata = {
            "distribution_model": "distribution_model",
            "distribution_platform": "distribution_platform",
            "translator": "translator",
            "edition": "edition",
            "release_date": "release_date",
            "engine": "engine",
            "engine_version": "engine_version",
            "notes": "notes",
            "change_note": "change_note",
        }

        for build_column, metadata_key in build_column_to_metadata.items():
            if build_column not in columns:
                continue
            if metadata_key not in metadata:
                continue
            if build_column == "translator":
                normalized_value = self._normalize_translator_value(metadata.get(metadata_key))
            else:
                normalized_value = self._normalize_text_value(metadata.get(metadata_key))
            insert_columns.append(build_column)
            values.append(normalized_value)

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
