import hashlib
import json
from datetime import datetime, timezone
from utils import (
    normalize_csv_list,
    normalize_text_value,
    normalize_translator_value,
    normalize_version_value,
)


class VnIngestionRepository:
    """Repository adapter for Title/Release/File ingestion on the canonical schema.

    Strictly supports the canonical domain schema:
    - `title`, `release`, `file`, `release_file`
    - enrichments: `tag`, `title_tag`, `developer`, `title_developer`,
      `publisher`, `title_publisher`, `language`, `release_language`,
      `revision`
    """

    RELEASE_METADATA_COLUMN_MAP = {
        "language": "language",
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
        self.title_table = "title"
        if not self._table_exists(self.title_table):
            raise RuntimeError("New schema required: missing 'title' table.")
        title_columns = self._table_columns(self.title_table)
        if "title_id" not in title_columns:
            raise RuntimeError("New schema required: 'title.title_id' column is missing.")
        self.title_id_column = "title_id"

        self.release_table = "release"
        if not self._table_exists(self.release_table):
            raise RuntimeError("New schema required: missing 'release' table.")
        release_columns = self._table_columns(self.release_table)
        if "release_id" not in release_columns or "version" not in release_columns:
            raise RuntimeError("New schema required: 'release.release_id' and 'release.version' columns are missing.")
        self.release_id_column = "release_id"
        self.release_version_column = "version"
        self.release_platform_column = "target_platform"
        self.release_has_normalized_version = "normalized_version" in release_columns

        required_tables = (
            "file",
            "release_file",
            "file_snapshot",
            "tag",
            "title_tag",
            "developer",
            "title_developer",
            "publisher",
            "title_publisher",
            "language",
            "release_language",
            "revision",
        )
        missing_tables = [name for name in required_tables if not self._table_exists(name)]
        if missing_tables:
            raise RuntimeError(
                f"New schema required: missing canonical table(s): {', '.join(missing_tables)}."
            )

        required_release_columns = (
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
        missing_release_columns = [name for name in required_release_columns if name not in release_columns]
        if missing_release_columns:
            raise RuntimeError(
                f"New schema required: missing canonical release column(s): {', '.join(missing_release_columns)}."
            )

        self.has_file_link_tables = self._table_exists("file") and self._table_exists("release_file")

    def _sync_title_tags_tables(self, title_id, tags_value):
        tags = normalize_csv_list(tags_value, lowercase=True)
        self.conn.execute("DELETE FROM title_tag WHERE title_id = ?", (title_id,))
        for tag_name in tags:
            tag_row = self.conn.execute("SELECT tag_id FROM tag WHERE name = ? LIMIT 1", (tag_name,)).fetchone()
            if tag_row:
                tag_id = int(tag_row["tag_id"])
            else:
                self.conn.execute("INSERT INTO tag (name) VALUES (?)", (tag_name,))
                tag_id = int(self.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            self.conn.execute(
                "INSERT OR IGNORE INTO title_tag (title_id, tag_id) VALUES (?, ?)",
                (title_id, tag_id),
            )

    def _sync_title_people_tables(
        self,
        *,
        title_id,
        raw_value,
        dictionary_table,
        dictionary_id_column,
        dictionary_name_column,
        join_table,
        join_foreign_id_column,
    ):
        values = normalize_csv_list(raw_value, lowercase=True)
        self.conn.execute(f"DELETE FROM {join_table} WHERE title_id = ?", (title_id,))
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
                f"INSERT OR IGNORE INTO {join_table} (title_id, {join_foreign_id_column}) VALUES (?, ?)",
                (title_id, foreign_id),
            )

    def _sync_release_languages_tables(self, release_id, language_value):
        values = normalize_csv_list(language_value, lowercase=True)
        self.conn.execute("DELETE FROM release_language WHERE release_id = ?", (release_id,))
        for code in values:
            row = self.conn.execute(
                "SELECT language_id FROM language WHERE code = ? LIMIT 1",
                (code,),
            ).fetchone()
            if row:
                language_id = int(row["language_id"])
            else:
                self.conn.execute("INSERT INTO language (code) VALUES (?)", (code,))
                language_id = int(self.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            self.conn.execute(
                "INSERT OR IGNORE INTO release_language (release_id, language_id) VALUES (?, ?)",
                (release_id, language_id),
            )



    def get_or_create_series(self, metadata):
        series_name = normalize_text_value(metadata.get("series"))
        if not series_name:
            return None
            
        row = self.conn.execute(
            "SELECT series_id FROM series WHERE TRIM(name) = TRIM(?) COLLATE NOCASE LIMIT 1",
            (series_name,),
        ).fetchone()
        
        description = normalize_text_value(metadata.get("series_description"))
        
        if row:
            series_id = int(row["series_id"])
            if description:
                self.conn.execute(
                    "UPDATE series SET description = ? WHERE series_id = ?",
                    (description, series_id),
                )
            return series_id
            
        self.conn.execute(
            "INSERT INTO series (name, description) VALUES (?, ?)",
            (series_name, description),
        )
        return int(self.conn.execute("SELECT last_insert_rowid()").fetchone()[0])

    def get_or_create_title(self, metadata):
        title = str(metadata.get("title") or "").strip()
        if not title:
            raise ValueError("Title is required for Title resolution.")

        title_columns = self._table_columns(self.title_table)
        title_updatable_columns = [
            "aliases",
            "release_status",
            "content_rating",
            "content_mode",
            "content_type",
            "description",
            "source",
            "original_release_date",
        ]

        title_values = {}
        for column_name in title_updatable_columns:
            if column_name not in title_columns:
                continue
            if column_name not in metadata:
                continue
            title_values[column_name] = normalize_text_value(metadata.get(column_name))

        if "series_id" in title_columns:
            series_id = self.get_or_create_series(metadata)
            if series_id is not None:
                title_values["series_id"] = series_id

        existing = self.conn.execute(
            f"SELECT {self.title_id_column} FROM {self.title_table} WHERE TRIM(title) = TRIM(?) COLLATE NOCASE LIMIT 1",
            (title,),
        ).fetchone()
        if existing:
            title_id = existing[self.title_id_column]
            if title_values:
                assignments = ", ".join(f"{column} = ?" for column in title_values)
                self.conn.execute(
                    f"UPDATE {self.title_table} SET {assignments} WHERE {self.title_id_column} = ?",
                    tuple(title_values.values()) + (title_id,),
                )
            self._sync_title_tags_tables(title_id, metadata.get("tags"))
            self._sync_title_people_tables(
                title_id=title_id,
                raw_value=metadata.get("developer"),
                dictionary_table="developer",
                dictionary_id_column="developer_id",
                dictionary_name_column="name",
                join_table="title_developer",
                join_foreign_id_column="developer_id",
            )
            self._sync_title_people_tables(
                title_id=title_id,
                raw_value=metadata.get("publisher"),
                dictionary_table="publisher",
                dictionary_id_column="publisher_id",
                dictionary_name_column="name",
                join_table="title_publisher",
                join_foreign_id_column="publisher_id",
            )
            return title_id

        insert_columns = ["title"]
        insert_values = [title]
        for column_name, value in title_values.items():
            insert_columns.append(column_name)
            insert_values.append(value)

        placeholders = ", ".join(["?"] * len(insert_columns))
        self.conn.execute(
            f"INSERT INTO {self.title_table} ({', '.join(insert_columns)}) VALUES ({placeholders})",
            tuple(insert_values),
        )
        title_id = self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        self._sync_title_tags_tables(title_id, metadata.get("tags"))
        self._sync_title_people_tables(
            title_id=title_id,
            raw_value=metadata.get("developer"),
            dictionary_table="developer",
            dictionary_id_column="developer_id",
            dictionary_name_column="name",
            join_table="title_developer",
            join_foreign_id_column="developer_id",
        )
        self._sync_title_people_tables(
            title_id=title_id,
            raw_value=metadata.get("publisher"),
            dictionary_table="publisher",
            dictionary_id_column="publisher_id",
            dictionary_name_column="name",
            join_table="title_publisher",
            join_foreign_id_column="publisher_id",
        )
        return title_id

    def _release_lookup_filters(self, metadata):
        version_value = normalize_version_value(metadata.get("version"))
        language = normalize_text_value(metadata.get("language"))
        edition = normalize_text_value(metadata.get("edition"))
        distribution_platform = normalize_text_value(metadata.get("distribution_platform"))
        return version_value, language, edition, distribution_platform

    def find_release(self, title_id, metadata):
        version_value, language, edition, distribution_platform = self._release_lookup_filters(metadata)
        if not version_value:
            return None

        normalized_version_expr = "normalized_version" if self.release_has_normalized_version else "lower(trim(version))"
        where_clauses = ["title_id = ?", f"{normalized_version_expr} = lower(trim(?))"]
        params = [title_id, version_value]

        where_clauses.append("COALESCE(language, '') = COALESCE(?, '')")
        params.append(language)
        where_clauses.append("COALESCE(edition, '') = COALESCE(?, '')")
        params.append(edition)
        where_clauses.append("COALESCE(distribution_platform, '') = COALESCE(?, '')")
        params.append(distribution_platform)

        row = self.conn.execute(
            f"SELECT {self.release_id_column} FROM {self.release_table} WHERE {' AND '.join(where_clauses)} ORDER BY {self.release_id_column} DESC LIMIT 1",
            tuple(params),
        ).fetchone()
        return row[self.release_id_column] if row else None

    def create_release(self, title_id, metadata):
        version_value, language, _, _ = self._release_lookup_filters(metadata)
        if not version_value:
            version_value = "1.0"

        insert_columns = ["title_id", self.release_version_column]
        values = [title_id, version_value]

        insert_columns.append("language")
        values.append(language)

        for release_column, metadata_key in self.RELEASE_METADATA_COLUMN_MAP.items():
            if metadata_key not in metadata:
                continue
            if release_column == "translator":
                normalized_value = normalize_translator_value(metadata.get(metadata_key), dict_format="inline")
            else:
                normalized_value = normalize_text_value(metadata.get(metadata_key))
            insert_columns.append(release_column)
            values.append(normalized_value)

        placeholders = ", ".join(["?"] * len(insert_columns))
        self.conn.execute(
            f"INSERT INTO {self.release_table} ({', '.join(insert_columns)}) VALUES ({placeholders})",
            tuple(values),
        )
        release_id = self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        self._sync_release_languages_tables(release_id, metadata.get("language"))
        return release_id

    def get_or_create_release(self, title_id, metadata):
        existing = self.find_release(title_id, metadata)
        if existing:
            self._sync_release_languages_tables(existing, metadata.get("language"))
            return existing
        return self.create_release(title_id, metadata)

    def upsert_title_and_release(self, metadata):
        title_id = self.get_or_create_title(metadata)
        release_id = self.get_or_create_release(title_id, metadata)
        return title_id, release_id

    def _get_file_size_from_disk(self, file_path):
        import os
        if file_path and os.path.exists(file_path):
            return os.path.getsize(file_path)
        return None

    def _create_file_in_tables(self, release_id, metadata, archive_data):
        archive_data = archive_data or {}
        artifact_sha = normalize_text_value(archive_data.get("sha256"))
        if not artifact_sha:
            return None

        filename = archive_data.get("filepath") or archive_data.get("filename") or metadata.get("original_filename")

        file_row = self.conn.execute(
            "SELECT file_id FROM file WHERE sha256 = ? LIMIT 1",
            (artifact_sha,),
        ).fetchone()

        if file_row:
            file_id = file_row["file_id"]
            size_bytes = archive_data.get("size_bytes")
            if size_bytes:
                self.conn.execute(
                    "UPDATE file SET size_bytes = ? WHERE file_id = ? AND (size_bytes IS NULL OR size_bytes = 0)",
                    (size_bytes, file_id)
                )
        else:
            size_bytes = archive_data.get("size_bytes")
            if not size_bytes:
                path_to_check = archive_data.get("filepath") or archive_data.get("original_path") or archive_data.get("filename")
                size_bytes = self._get_file_size_from_disk(path_to_check)
            self.conn.execute(
                "INSERT INTO file (sha256, filename, size_bytes) VALUES (?, ?, ?)",
                (artifact_sha, filename, size_bytes),
            )
            file_id = self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        link_row = self.conn.execute(
            "SELECT 1 FROM release_file WHERE release_id = ? AND file_id = ? LIMIT 1",
            (release_id, file_id),
        ).fetchone()
        artifact_type = normalize_text_value(metadata.get("artifact_type"))
        if not artifact_type:
            artifact_type = normalize_text_value(archive_data.get("artifact_type"))

        if not link_row:
            archived_at = metadata.get("archived_at")
            if not archived_at:
                archived_at = archive_data.get("archived_at")
            if not archived_at:
                archived_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

            self.conn.execute(
                "INSERT INTO release_file (release_id, file_id, original_filename, artifact_type, archived_at) VALUES (?, ?, ?, ?, ?)",
                (release_id, file_id, filename, artifact_type, archived_at),
            )
        else:
            if artifact_type is not None:
                self.conn.execute(
                    "UPDATE release_file SET artifact_type = ? WHERE release_id = ? AND file_id = ? AND (artifact_type IS NULL OR artifact_type = '')",
                    (artifact_type, release_id, file_id)
                )

        return file_id

    def create_file_link(self, release_id, metadata, archive_data):
        if self.has_file_link_tables:
            return self._create_file_in_tables(release_id, metadata, archive_data)
        raise RuntimeError("No supported file persistence tables found in current schema.")

    def create_metadata_raw(self, raw_payload, file_id, release_id=None):
        if self._table_exists("revision"):
            if release_id is None:
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
                "UPDATE revision SET is_current = 0 WHERE release_id = ? AND is_current = 1",
                (release_id,),
            )

            next_version = self.conn.execute(
                "SELECT COALESCE(MAX(version_number), 0) + 1 FROM revision WHERE release_id = ?",
                (release_id,),
            ).fetchone()[0]

            self.conn.execute(
                """
                INSERT INTO revision (
                    release_id, file_id, raw_json, raw_sha256, version_number, is_current, created_at
                ) VALUES (?, ?, ?, ?, ?, 1, ?)
                """,
                (release_id, file_id, raw_json_value, raw_sha256, next_version, created_at),
            )
            return next_version

    def create_file_attachment_metadata(self, release_id, file_id, metadata_dict):
        """Record a metadata snapshot at the time a file is attached to a release.

        This writes to ``file_snapshot`` — a provenance table distinct from
        ``revision`` (which tracks the release-level metadata version chain).
        """
        created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        raw_json = json.dumps(metadata_dict, ensure_ascii=False, sort_keys=True)

        self.conn.execute(
            """
            INSERT INTO file_snapshot (
                release_id, file_id, metadata_version, title, version,
                build_type, normalized_version, distribution_platform, platform,
                language, edition,
                release_date, source_url, notes, change_note, raw_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                release_id,
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
