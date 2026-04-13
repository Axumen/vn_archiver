"""Domain schema derived from domain_logic.md using metadata_v1.yaml fields.

Core principles applied:
- Title -> Release -> File hierarchy
- Release as primary release unit
- Explicit relation table for link semantics

Constraint applied:
- Table columns are based only on fields available in metadata/metadata_v1.yaml
  (including archives[].* keys).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    source_field: str
    required: bool = False


@dataclass(frozen=True)
class TableSpec:
    name: str
    columns: tuple[ColumnSpec, ...]


SERIES_TABLE = TableSpec(
    name="series",
    columns=(
        ColumnSpec("series_id", "series_id", required=True),
        ColumnSpec("name", "series", required=True),
        ColumnSpec("description", "series_description"),
    ),
)

TITLE_TABLE = TableSpec(
    name="title",
    columns=(
        ColumnSpec("title_id", "title_id", required=True),
        ColumnSpec("title", "title", required=True),
        ColumnSpec("series_id", "series_id"),
        ColumnSpec("aliases", "aliases"),
        ColumnSpec("developer", "developer"),
        ColumnSpec("publisher", "publisher"),
        ColumnSpec("release_status", "release_status"),
        ColumnSpec("content_rating", "content_rating"),
        ColumnSpec("content_mode", "content_mode"),
        ColumnSpec("content_type", "content_type"),
        ColumnSpec("description", "description"),
        ColumnSpec("source", "source"),
        ColumnSpec("tags", "tags"),
        ColumnSpec("original_release_date", "original_release_date"),
    ),
)

RELEASE_TABLE = TableSpec(
    name="release",
    columns=(
        ColumnSpec("release_id", "release_id", required=True),
        ColumnSpec("title_id", "title_id", required=True),
        ColumnSpec("version", "version", required=True),
        ColumnSpec("build_type", "build_type"),
        ColumnSpec("distribution_model", "distribution_model"),
        ColumnSpec("distribution_platform", "distribution_platform"),
        ColumnSpec("language", "language"),
        ColumnSpec("translator", "translator"),
        ColumnSpec("edition", "edition"),
        ColumnSpec("release_date", "release_date"),
        ColumnSpec("engine", "engine"),
        ColumnSpec("engine_version", "engine_version"),
        ColumnSpec("target_platform", "target_platform"),
        ColumnSpec("notes", "notes"),
        ColumnSpec("change_note", "change_note"),
    ),
)

FILE_TABLE = TableSpec(
    name="file",
    columns=(
        ColumnSpec("file_id", "file_id", required=True),
        ColumnSpec("sha256", "archives.sha256", required=True),
        ColumnSpec("size_bytes", "size_bytes"),
        ColumnSpec("filename", "archives.filename"),
    ),
)

FILE_SNAPSHOT_TABLE = TableSpec(
    name="file_snapshot",
    columns=(
        ColumnSpec("metadata_id", "metadata_id", required=True),
        ColumnSpec("release_id", "release_id", required=True),
        ColumnSpec("file_id", "file_id", required=True),
        ColumnSpec("metadata_version", "metadata_version", required=True),
        ColumnSpec("title", "title"),
        ColumnSpec("version", "version"),
        ColumnSpec("build_type", "build_type"),
        ColumnSpec("normalized_version", "normalized_version"),
        ColumnSpec("distribution_platform", "distribution_platform"),
        ColumnSpec("platform", "platform"),
        ColumnSpec("language", "language"),
        ColumnSpec("edition", "edition"),
        ColumnSpec("release_date", "release_date"),
        ColumnSpec("source_url", "source_url"),
        ColumnSpec("notes", "notes"),
        ColumnSpec("change_note", "change_note"),
        ColumnSpec("raw_json", "metadata_json", required=True),
        ColumnSpec("created_at", "created_at", required=True),
    ),
)


DOMAIN_TABLES = (
    SERIES_TABLE,
    TITLE_TABLE,
    RELEASE_TABLE,
    FILE_TABLE,
    FILE_SNAPSHOT_TABLE,
)


def table_names() -> list[str]:
    return [table.name for table in DOMAIN_TABLES]
