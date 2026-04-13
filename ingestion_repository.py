import hashlib
import json
from datetime import datetime, timezone


class VnIngestionRepository:
    """Repository adapter for VN/build/file ingestion on the canonical schema.

    Strictly supports the canonical domain schema:
    - `vn`, `build`, `file`, `build_file`
    - enrichments: `tags`, `vn_tags`, `developers`, `vn_developers`,
      `publishers`, `vn_publishers`, `languages`, `build_languages`,
      `metadata_raw_versions`
    """

    BUILD_METADATA_COLUMN_MAP = {
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

    def __init__(self, conn):
        self.conn = conn
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
        if not self._table_exists(self.vn_table):
            raise RuntimeError("New schema required: missing 'vn' table.")
        vn_columns = self._table_columns(self.vn_table)
        if "vn_id" not in vn_columns:
            raise RuntimeError("New schema required: 'vn.vn_id' column is missing.")
        self.vn_id_column = "vn_id"

        self.build_table = "build"
        if not self._table_exists(self.build_table):
            raise RuntimeError("New schema required: missing 'build' table.")
        build_columns = self._table_columns(self.build_table)
        if "build_id" not in build_columns or "version" not in build_columns:
            raise RuntimeError("New schema required: 'build.build_id' and 'build.version' columns are missing.")
        self.build_id_column = "build_id"
        self.build_version_column = "version"
        self.build_platform_column = "target_platform"

        required_tables = (
            "file",
            "build_file",
            "build_file_metadata",
            "tags",
            "vn_tags",
            "developers",
            "vn_developers",
            "publishers",
            "vn_publishers",
            "languages",
            "build_languages",
            "metadata_raw_versions",
        )
        missing_tables = [name for name in required_tables if not self._table_exists(name)]
        if missing_tables:
            raise RuntimeError(
                f"New schema required: missing canonical table(s): {', '.join(missing_tables)}."
            )

        required_build_columns = (
            "language",
            "build_type",
            "target_platform",
            "distribution_model",
            "distribution_platform",
            "translator",
            "edition",
            "release_date",
            "engine",
            "engine_version",
            "notes",
            "change_note",
        )
        missing_build_columns = [name for name in required_build_columns if name not in build_columns]
        if missing_build_columns:
            raise RuntimeError(
                f"New schema required: missing canonical build column(s): {', '.join(missing_build_columns)}."
            )

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

    def _sync_vn_tags_tables(self, vn_id, tags_value):
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

    def _sync_vn_people_tables(
        self,
        *,
        vn_id,
        raw_value,
        dictionary_table,
        dictionary_id_column,
        dictionary_name_column,
        join_table,
        join_foreign_id_column,
    ):
        values = self._normalize_tag_list(raw_value)
        self.conn.execute(f"DELETE FROM {join_table} WHERE vn_id = ?", (vn_id,))
        for name in values:
            row = self.conn.execute(
                f"SELECT {dictionary_id_column} FROM {dictionary_table} WHERE {dictionary_name_column} = ? LIMIT 1",
                (name,),
            ).fetchone()
            if row:
                foreign_id = int(row[dictionary_id_column])
            else:
                self.conn.execute(
                    f"INSERT INTO {dictionary_table} ({dictionary_name_column}) VALUES (?)",
                    (name,),
                )
                foreign_id = int(self.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            self.conn.execute(
                f"INSERT OR IGNORE INTO {join_table} (vn_id, {join_foreign_id_column}) VALUES (?, ?)",
                (vn_id, foreign_id),
            )

    def _sync_build_languages_tables(self, build_id, language_value):
        values = self._normalize_tag_list(language_value)
        self.conn.execute("DELETE FROM build_languages WHERE build_id = ?", (build_id,))
        for code in values:
            row = self.conn.execute(
                "SELECT language_id FROM languages WHERE code = ? LIMIT 1",
                (code,),
            ).fetchone()
            if row:
                language_id = int(row["language_id"])
            else:
                self.conn.execute("INSERT INTO languages (code) VALUES (?)", (code,))
                language_id = int(self.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            self.conn.execute(
                "INSERT OR IGNORE INTO build_languages (build_id, language_id) VALUES (?, ?)",
                (build_id, language_id),
            )



    def get_or_create_vn(self, metadata):
        title = str(metadata.get("title") or "").strip()
        if not title:
            raise ValueError("Title is required for VN resolution.")

        vn_columns = self._table_columns(self.vn_table)
        vn_updatable_columns = [
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
            self._sync_vn_tags_tables(vn_id, metadata.get("tags"))
            self._sync_vn_people_tables(
                vn_id=vn_id,
                raw_value=metadata.get("developer"),
                dictionary_table="developers",
                dictionary_id_column="developer_id",
                dictionary_name_column="name",
                join_table="vn_developers",
                join_foreign_id_column="developer_id",
            )
            self._sync_vn_people_tables(
                vn_id=vn_id,
                raw_value=metadata.get("publisher"),
                dictionary_table="publishers",
                dictionary_id_column="publisher_id",
                dictionary_name_column="name",
                join_table="vn_publishers",
                join_foreign_id_column="publisher_id",
            )
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
        self._sync_vn_tags_tables(vn_id, metadata.get("tags"))
        self._sync_vn_people_tables(
            vn_id=vn_id,
            raw_value=metadata.get("developer"),
            dictionary_table="developers",
            dictionary_id_column="developer_id",
            dictionary_name_column="name",
            join_table="vn_developers",
            join_foreign_id_column="developer_id",
        )
        self._sync_vn_people_tables(
            vn_id=vn_id,
            raw_value=metadata.get("publisher"),
            dictionary_table="publishers",
            dictionary_id_column="publisher_id",
            dictionary_name_column="name",
            join_table="vn_publishers",
            join_foreign_id_column="publisher_id",
        )
        return vn_id

    def _build_lookup_filters(self, metadata):
        version_value = str(metadata.get("version") or "").strip()
        language = self._normalize_text_value(metadata.get("language"))
        build_type = self._normalize_text_value(metadata.get("build_type"))
        platform = self._normalize_text_value(metadata.get("target_platform"))
        return version_value, language, build_type, platform

    def find_build(self, vn_id, metadata):
        version_value, language, build_type, platform = self._build_lookup_filters(metadata)
        if not version_value:
            return None

        where_clauses = ["vn_id = ?", f"{self.build_version_column} = ?"]
        params = [vn_id, version_value]

        where_clauses.append("COALESCE(language, '') = COALESCE(?, '')")
        params.append(language)
        where_clauses.append("COALESCE(build_type, '') = COALESCE(?, '')")
        params.append(build_type)
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

        insert_columns = ["vn_id", self.build_version_column]
        values = [vn_id, version_value]

        insert_columns.append("language")
        values.append(language)
        insert_columns.append("build_type")
        values.append(build_type)
        insert_columns.append(self.build_platform_column)
        values.append(platform)

        for build_column, metadata_key in self.BUILD_METADATA_COLUMN_MAP.items():
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
        build_id = self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        self._sync_build_languages_tables(build_id, metadata.get("language"))
        return build_id

    def get_or_create_build(self, vn_id, metadata):
        existing = self.find_build(vn_id, metadata)
        if existing:
            self._sync_build_languages_tables(existing, metadata.get("language"))
            return existing
        return self.create_build(vn_id, metadata)

    def upsert_vn_and_build(self, metadata):
        vn_id = self.get_or_create_vn(metadata)
        build_id = self.get_or_create_build(vn_id, metadata)
        return vn_id, build_id

    def _create_file_in_tables(self, build_id, metadata, archive_data):
        archive_data = archive_data or {}
        artifact_sha = self._normalize_text_value(archive_data.get("sha256"))
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
            size_bytes = archive_data.get("size_bytes")
            first_seen_at = archive_data.get("first_seen_at")
            mime_type = archive_data.get("mime_type")
            self.conn.execute(
                "INSERT INTO file (sha256, filename, size_bytes, first_seen_at, mime_type) VALUES (?, ?, ?, ?, ?)",
                (artifact_sha, filename, size_bytes, first_seen_at, mime_type),
            )
            file_id = self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        link_row = self.conn.execute(
            "SELECT 1 FROM build_file WHERE build_id = ? AND file_id = ? LIMIT 1",
            (build_id, file_id),
        ).fetchone()
        if not link_row:
            artifact_type = self._normalize_text_value(metadata.get("artifact_type"))
            self.conn.execute(
                "INSERT INTO build_file (build_id, file_id, original_filename, artifact_type, archived_at) VALUES (?, ?, ?, ?, ?)",
                (build_id, file_id, filename, artifact_type, metadata.get("archived_at")),
            )

        return file_id

    def create_file_link(self, build_id, metadata, archive_data):
        if self.has_file_link_tables:
            return self._create_file_in_tables(build_id, metadata, archive_data)
        raise RuntimeError("No supported file persistence tables found in current schema.")

    def create_metadata_raw(self, raw_payload, file_id, build_id=None):
        if self._table_exists("metadata_raw_versions"):
            if build_id is None:
                return

            if isinstance(raw_payload, dict):
                payload = {k: v for k, v in raw_payload.items() if not str(k).startswith("_")}
                raw_json_value = json.dumps(payload, ensure_ascii=False, sort_keys=True)
            else:
                raw_json_value = json.dumps({"raw_value": str(raw_payload or "")}, ensure_ascii=False, sort_keys=True)

            raw_sha256 = hashlib.sha256(raw_json_value.encode("utf-8")).hexdigest()
            created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

            # Mark all previous versions as not current
            self.conn.execute(
                "UPDATE metadata_raw_versions SET is_current = 0 WHERE build_id = ? AND is_current = 1",
                (build_id,),
            )

            next_version = self.conn.execute(
                "SELECT COALESCE(MAX(version_number), 0) + 1 FROM metadata_raw_versions WHERE build_id = ?",
                (build_id,),
            ).fetchone()[0]

            self.conn.execute(
                """
                INSERT INTO metadata_raw_versions (
                    build_id, file_id, raw_json, raw_sha256, version_number, is_current, created_at
                ) VALUES (?, ?, ?, ?, ?, 1, ?)
                """,
                (build_id, file_id, raw_json_value, raw_sha256, next_version, created_at),
            )
            return next_version

    def create_file_attachment_metadata(self, build_id, file_id, metadata_dict):
        """Record a metadata snapshot at the time a file is attached to a build.

        This writes to ``build_file_metadata`` — a provenance table distinct from
        ``metadata_raw_versions`` (which tracks the build-level metadata version chain).
        """
        created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        raw_json = json.dumps(metadata_dict, ensure_ascii=False, sort_keys=True)

        self.conn.execute(
            """
            INSERT INTO build_file_metadata (
                build_id, file_id, metadata_version, title, version,
                build_type, normalized_version, distribution_platform, platform,
                language, edition,
                release_date, source_url, notes, change_note, raw_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                build_id,
                file_id,
                int(metadata_dict.get("metadata_version") or 1),
                str(metadata_dict.get("title") or ""),
                str(metadata_dict.get("version") or ""),
                str(metadata_dict.get("build_type") or ""),
                str(metadata_dict.get("normalized_version") or ""),
                str(metadata_dict.get("distribution_platform") or ""),
                str(metadata_dict.get("platform") or ""),
                str(metadata_dict.get("language") or ""),
                str(metadata_dict.get("edition") or ""),
                str(metadata_dict.get("release_date") or ""),
                str(metadata_dict.get("source_url") or ""),
                str(metadata_dict.get("notes") or ""),
                str(metadata_dict.get("change_note") or ""),
                raw_json,
                created_at,
            ),
        )
