"""Pure utility functions for the VN Archiver.

This module contains hash computation, string formatting, and version
sorting helpers that carry no domain knowledge.  Every other module may
depend on utils — utils depends on nothing inside the project.
"""

import hashlib
import json
import re
from datetime import date, datetime


def sha256_file(filepath):
    """Return the hex SHA-256 digest of a file."""
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def sha1_file(filepath):
    """Return the hex SHA-1 digest of a file."""
    sha1 = hashlib.sha1()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha1.update(chunk)
    return sha1.hexdigest()


def safe_json_serialize(obj):
    """Helper to serialize datetime objects to strings for JSON dumping."""
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def slugify_component(value, fallback):
    """
    Slugify using:
    - lowercase
    - hyphen as word separator
    - alphanumeric only
    - collapse duplicates
    """
    text = str(value or "").strip().lower()
    if not text:
        return fallback

    normalized = []
    last_was_hyphen = False

    for char in text:
        if char.isalnum():
            normalized.append(char)
            last_was_hyphen = False
        else:
            if not last_was_hyphen:
                normalized.append("-")
                last_was_hyphen = True

    slug = "".join(normalized).strip("-")
    return slug or fallback


def format_uploaded_component(value, fallback):
    """Format a metadata value for use in directory/display names."""
    text = str(value or "").replace("_", " ").strip()
    text = " ".join(text.split())
    return text or fallback


def normalize_version_for_sort(version_text):
    """Convert versions like '1.10.2' into sortable tuples with text fallback."""
    text = str(version_text or "").strip()
    if not text:
        return (0,)

    cleaned = re.sub(r"[^0-9A-Za-z\.\-_]", "", text)
    tokens = re.split(r"[\.\-_]+", cleaned)
    sortable = []
    for tok in tokens:
        if tok.isdigit():
            sortable.append((0, int(tok)))
        else:
            sortable.append((1, tok.lower()))
    return tuple(sortable)


def determine_latest_version(versions):
    """Return the highest version string from a list using semantic ordering."""
    valid_versions = [str(v).strip() for v in versions if str(v).strip()]
    if not valid_versions:
        return "unknown"
    return max(valid_versions, key=normalize_version_for_sort)


def normalize_text_value(value):
    """Normalize free-form values into optional trimmed text."""
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip()
        return normalized or None
    if isinstance(value, list):
        parts = [str(item).strip() for item in value if str(item).strip()]
        return ", ".join(parts) if parts else None
    fallback = str(value).strip()
    return fallback or None


def normalize_csv_list(value, *, lowercase=False, unique=False, sort_values=False):
    """Normalize CSV text/list values into cleaned list items."""
    if value is None:
        return []
    if isinstance(value, str):
        items = [part.strip() for part in value.split(",") if part.strip()]
    elif isinstance(value, list):
        items = [str(item).strip() for item in value if str(item).strip()]
    else:
        single = str(value).strip()
        items = [single] if single else []

    if lowercase:
        items = [item.lower() for item in items]
    if unique:
        items = list(dict.fromkeys(items))
    if sort_values:
        items = sorted(items)
    return items


def normalize_text_list_value(value):
    """Normalize text-or-list values into comma-separated text."""
    values = normalize_csv_list(value)
    return ", ".join(values) if values else None


def normalize_translator_value(value, *, dict_format="json"):
    """Normalize translator metadata into a storable TEXT value."""
    if value is None:
        return None

    if isinstance(value, str):
        normalized = value.strip()
        return normalized or None

    if isinstance(value, list):
        flattened = [str(item).strip() for item in value if str(item).strip()]
        if not flattened:
            return None
        if dict_format == "inline":
            return ", ".join(flattened)
        return json.dumps(flattened, ensure_ascii=False)

    if isinstance(value, dict):
        normalized_map = {}
        normalized_chunks = []
        for language, translators in value.items():
            language_key = str(language or "").strip()
            if not language_key:
                continue
            names = normalize_csv_list(translators)
            if not names:
                continue
            normalized_map[language_key] = names
            normalized_chunks.append(f"{language_key}: {', '.join(names)}")

        if not normalized_map:
            return None
        if dict_format == "inline":
            return " | ".join(normalized_chunks)
        return json.dumps(normalized_map, ensure_ascii=False)

    fallback = str(value).strip()
    return fallback or None


def normalize_version_value(value):
    """Normalize user-provided version labels (e.g. v1.0 -> 1.0)."""
    version_text = str(value or "").strip()
    if not version_text:
        return ""
    if version_text.lower().startswith("v") and len(version_text) > 1:
        version_text = version_text[1:].strip()
    return version_text


def normalize_language_value(value):
    """Normalize language labels to stable ingest keys."""
    language_text = str(value or "").strip()
    if not language_text:
        return ""
    if language_text.isalpha() and len(language_text) <= 3:
        return language_text.upper()
    return language_text.lower()


def normalize_metadata_list(metadata, field_name):
    """Normalize a metadata field into a list of values."""
    return normalize_csv_list((metadata or {}).get(field_name))


CSV_TO_TEXT_FIELDS = {
    "developer",
    "publisher",
    "language",
    "content_rating",
    "content_mode",
    "target_platform",
}

CSV_TO_LIST_FIELDS = {
    "aliases",
    "tags",
}

PASSTHROUGH_FIELDS = {
    "metadata_version",
    "title",
    "version",
    "normalized_version",
    "series",
    "series_description",
    "release_status",
    "description",
    "source",
    "source_url",
    "release_type",
    "distribution_model",
    "distribution_platform",
    "platform",
    "translator",
    "edition",
    "original_release_date",
    "release_date",
    "engine",
    "engine_version",
    "parent_vn_title",
    "relationship_type",
    "build_relations",
    "notes",
    "change_note",
    "content_type",
    "archives",
    "sha256",
    "size_bytes",
    "original_filename",
    "artifact_type",
    "archived_at",
    "_raw_text",
    "_source_file",
}

CATEGORY_ALL_FIELDS = CSV_TO_TEXT_FIELDS | CSV_TO_LIST_FIELDS | PASSTHROUGH_FIELDS


def validate_metadata_field_categories(metadata):
    """Warn about unknown metadata keys and validate category overlap."""
    overlap = (
        (CSV_TO_TEXT_FIELDS & CSV_TO_LIST_FIELDS)
        | (CSV_TO_TEXT_FIELDS & PASSTHROUGH_FIELDS)
        | (CSV_TO_LIST_FIELDS & PASSTHROUGH_FIELDS)
    )
    if overlap:
        raise ValueError(f"Metadata category overlap detected: {sorted(overlap)}")

    unknown_fields = sorted(set(metadata.keys()) - CATEGORY_ALL_FIELDS)
    if unknown_fields:
        print(f"[WARN] Unknown metadata fields (no explicit category): {', '.join(unknown_fields)}")


def normalize_metadata_fields(metadata):
    """Normalize metadata values according to explicit field categories."""
    if not isinstance(metadata, dict):
        return metadata

    normalized = dict(metadata)
    validate_metadata_field_categories(normalized)

    for field in CSV_TO_TEXT_FIELDS:
        if field in normalized:
            normalized[field] = normalize_text_list_value(normalized.get(field))

    for field in CSV_TO_LIST_FIELDS:
        if field in normalized:
            normalized[field] = normalize_csv_list(normalized.get(field))

    return normalized


def table_exists(conn, table_name):
    """Check whether a table exists in the connected SQLite database."""
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None
