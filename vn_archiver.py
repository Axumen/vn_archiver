#!/usr/bin/env python3

import hashlib
import json
import os
import re
import shutil
import sys
import time
import yaml
from tqdm import tqdm
from colorama import Fore
from datetime import date, datetime
from pathlib import Path
from b2sdk.v2 import InMemoryAccountInfo, B2Api
from db_manager import get_connection

# ==============================
# CONFIGURATION
# ==============================

INCOMING_DIR = "incoming"
UPLOADING_DIR = "uploading"
VN_ARCHIVE_DIR = "vn archive"
METADATA_TEMPLATE_DIR = Path("metadata")
DEFAULT_METADATA_VERSION = 1
B2_CONFIG_FILE = "backblaze_config.yaml"

B2_KEY_ID = None
B2_APPLICATION_KEY = None
B2_BUCKET_NAME = None

SUGGESTED_TAGS = [
    "romance", "drama", "comedy", "slice-of-life",
    "mystery", "horror", "sci-fi", "fantasy",
    "school", "adult", "nakige", "utsuge"
]

SUGGESTED_CONTENT_TYPE = [
    "main_story", "story_expansion", "seasonal_event",
    "april_fools", "side_story", "non_canon_special"
]

SUGGESTED_ARTIFACT_TYPE = [
    "game_archive",
    "patch",
    "instructions",
    "readme",
    "manual",
    "soundtrack",
    "bonus",
    "checksum",
]

AUTO_METADATA_FIELDS = {
    "original_filename": lambda zip_path: os.path.basename(zip_path),
    "file_size_bytes": lambda zip_path: os.path.getsize(zip_path),
    "sha256": lambda zip_path: sha256_file(zip_path),
    "archived_at": lambda _: datetime.utcnow().isoformat() + "Z",
    # Legacy identification fields maintained in nested archive metadata.
    "archive.filename": lambda zip_path: os.path.basename(zip_path),
    "archive.sha256": lambda zip_path: sha256_file(zip_path),
    "archive.file_size": lambda zip_path: os.path.getsize(zip_path),
}


# ==============================
# UTILITY
# ==============================

def ensure_directories():
    Path(INCOMING_DIR).mkdir(exist_ok=True)
    Path(UPLOADING_DIR).mkdir(exist_ok=True)
    Path(VN_ARCHIVE_DIR).mkdir(exist_ok=True)


def sha256_file(filepath):
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def sha1_file(filepath):
    sha1 = hashlib.sha1()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha1.update(chunk)
    return sha1.hexdigest()


def _extract_remote_hashes(file_info_obj):
    """Best-effort extraction of remote object hashes from B2 file metadata."""
    file_info_map = getattr(file_info_obj, "file_info", None) or {}
    remote_sha1 = (
        getattr(file_info_obj, "content_sha1", None)
        or file_info_map.get("large_file_sha1")
        or file_info_map.get("src_sha1")
    )
    remote_sha256 = file_info_map.get("src_sha256")
    return (
        str(remote_sha1).strip().lower() if remote_sha1 else None,
        str(remote_sha256).strip().lower() if remote_sha256 else None,
    )


def verify_remote_upload_integrity(
    remote_info,
    local_size,
    local_sha1,
    local_sha256,
    label,
):
    """Verify cloud object integrity using size + cloud-available hash metadata."""
    remote_size = getattr(remote_info, "size", None)
    if remote_size is not None and int(remote_size) != int(local_size):
        print(
            Fore.RED
            + f"Post-upload verification failed for {label}: remote size {remote_size} does not match local size {local_size}."
        )
        return False

    remote_sha1, remote_sha256 = _extract_remote_hashes(remote_info)
    if remote_sha256:
        if remote_sha256 != local_sha256:
            print(
                Fore.RED
                + f"Post-upload verification failed for {label}: remote SHA-256 does not match local SHA-256."
            )
            return False
        print(Fore.GREEN + f"Verified {label} integrity via remote SHA-256.")
        return True

    if remote_sha1:
        if remote_sha1 != local_sha1:
            print(
                Fore.RED
                + f"Post-upload verification failed for {label}: remote SHA-1 does not match local SHA-1."
            )
            return False
        print(Fore.GREEN + f"Verified {label} integrity via remote SHA-1 (B2-compatible fallback).")
        return True

    print(Fore.YELLOW + f"Remote hash unavailable for {label}; verified size only.")
    return True


def get_metadata_template_path(version=DEFAULT_METADATA_VERSION):
    return METADATA_TEMPLATE_DIR / f"metadata_v{version}.yaml"


def get_available_metadata_template_versions():
    if not METADATA_TEMPLATE_DIR.exists():
        return []

    versions = []
    for template_path in METADATA_TEMPLATE_DIR.glob("metadata_v*.yaml"):
        stem = template_path.stem
        try:
            version = int(stem.split("_v", 1)[1])
            versions.append(version)
        except (IndexError, ValueError):
            continue

    return sorted(set(versions))


def safe_json_serialize(obj):
    """Helper to serialize datetime objects to strings for JSON dumping."""
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def detect_latest_metadata_template_version():
    versions = get_available_metadata_template_versions()
    if versions:
        return versions[-1]
    return DEFAULT_METADATA_VERSION


def load_metadata_template(version=None):
    if version is None:
        version = detect_latest_metadata_template_version()

    template_path = get_metadata_template_path(version)

    if not template_path.exists():
        raise FileNotFoundError(
            f"Metadata template not found for version {version}: {template_path}"
        )

    with open(template_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def set_nested_value(target, dotted_key, value):
    parts = dotted_key.split(".")
    current = target

    for part in parts[:-1]:
        if part not in current or not isinstance(current[part], dict):
            current[part] = {}
        current = current[part]

    current[parts[-1]] = value


def get_nested_value(target, dotted_key):
    current = target
    for part in dotted_key.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def resolve_prompt_fields(template):
    """
    Returns metadata keys that should be prompted from the template format.

    Supported structures:
    1) {required: [...], optional: [...]}  # current template format
    2) {fields: ["a", "b"]}
    3) {fields: {a: ..., b: ...}}
    """

    fields = []

    required_fields = template.get("required") or []
    optional_fields = template.get("optional") or []

    if required_fields or optional_fields:
        fields.extend(required_fields)
        fields.extend(optional_fields)

    structured_fields = template.get("fields")
    if isinstance(structured_fields, list):
        fields.extend(structured_fields)
    elif isinstance(structured_fields, dict):
        fields.extend(structured_fields.keys())

    deduplicated = []
    seen = set()
    for field in fields:
        if not isinstance(field, str):
            continue
        if field in seen:
            continue
        if field in AUTO_METADATA_FIELDS:
            continue
        seen.add(field)
        deduplicated.append(field)

    return deduplicated


def load_b2_config(config_path=B2_CONFIG_FILE):
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Backblaze config file not found: {config_path}. "
            "Create it from the project template."
        )

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    key_id = config.get("key_id")
    application_key = config.get("application_key")
    bucket_name = config.get("bucket_name")
    dry_run = config.get("dry_run", True)

    missing_fields = [
        field_name
        for field_name, field_value in (
            ("key_id", key_id),
            ("application_key", application_key),
            ("bucket_name", bucket_name),
        )
        if not field_value
    ]

    if missing_fields:
        missing = ", ".join(missing_fields)
        raise ValueError(f"Missing Backblaze config field(s): {missing}")

    return key_id, application_key, bucket_name, bool(dry_run)


def prompt_field(field_name, current_value):
    # Existing content_type suggestion logic...
    if field_name == "content_type":
        print("\nSuggested content_type:")
        print(", ".join(SUGGESTED_CONTENT_TYPE))
    elif field_name == "artifact_type":
        print("\nSuggested artifact_type:")
        print(", ".join(SUGGESTED_ARTIFACT_TYPE))

    value = input(f"{field_name} [{current_value}]: ").strip()
    return value if value else current_value


def prompt_tags():
    print("\nSuggested Tags:")
    print(", ".join(SUGGESTED_TAGS))
    user_input = input("Tags (comma separated, blank allowed): ").strip()
    if not user_input:
        return []
    return [t.strip() for t in user_input.split(",")]


def create_metadata(zip_path):
    template = load_metadata_template()
    metadata_version = template.get(
        "metadata_version",
        detect_latest_metadata_template_version()
    )
    prompt_fields = [field for field in resolve_prompt_fields(template) if field != "artifact_type"]

    metadata = {"metadata_version": metadata_version}

    print("\nFill Metadata (Press ENTER to leave blank):\n")

    for key in prompt_fields:
        if key == "tags":
            metadata[key] = prompt_tags()
        else:
            metadata[key] = prompt_field(key, "")

    # Automatic fields
    for key, value_factory in AUTO_METADATA_FIELDS.items():
        value = value_factory(zip_path)
        if "." in key:
            set_nested_value(metadata, key, value)
        else:
            metadata[key] = value

    return metadata


# ==============================
# DATABASE
# ==============================

def sha_exists(build_id, sha256):
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id FROM archives WHERE build_id = ? AND sha256 = ?",
            (build_id, sha256)
        ).fetchone()
        return row is not None

def get_metadata_value(metadata, key, fallback=None):
    value = metadata.get(key)
    if value is not None:
        return value

    nested = get_nested_value(metadata, key)
    if nested is not None:
        return nested

    return fallback


def normalize_metadata_list(metadata, field_name):
    values = metadata.get(field_name) or []
    if isinstance(values, str):
        values = [item.strip() for item in values.split(',') if item.strip()]
    return values


def normalize_text_list_value(value):
    """Normalize text-or-list metadata fields into a comma-separated string."""
    if value is None:
        return None

    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        parts = [part.strip() for part in normalized.split(',') if part.strip()]
        return ", ".join(parts) if parts else None

    if isinstance(value, list):
        parts = [str(item).strip() for item in value if str(item).strip()]
        return ", ".join(parts) if parts else None

    fallback = str(value).strip()
    return fallback or None


CSV_TO_TEXT_FIELDS = {
    "developer",
    "publisher",
    "language",
    "content_rating",
    "content_mode",
}

CSV_TO_LIST_FIELDS = {
    "aliases",
    "tags",
    "target_platform",
}

PASSTHROUGH_FIELDS = {
    "metadata_version",
    "title",
    "version",
    "series",
    "series_description",
    "release_status",
    "description",
    "source",
    "build_type",
    "distribution_model",
    "distribution_platform",
    "translator",
    "edition",
    "original_release_date",
    "release_date",
    "engine",
    "engine_version",
    "parent_vn_title",
    "relationship_type",
    "notes",
    "change_note",
    "artifact_type",
    "archives",
    "archive",
    "sha256",
    "file_size_bytes",
    "original_filename",
    "archived_at",
}


CATEGORY_ALL_FIELDS = CSV_TO_TEXT_FIELDS | CSV_TO_LIST_FIELDS | PASSTHROUGH_FIELDS


def validate_metadata_field_categories(metadata):
    """Warn about unknown metadata keys and validate category overlap."""
    overlap = (CSV_TO_TEXT_FIELDS & CSV_TO_LIST_FIELDS) | (CSV_TO_TEXT_FIELDS & PASSTHROUGH_FIELDS) | (CSV_TO_LIST_FIELDS & PASSTHROUGH_FIELDS)
    if overlap:
        raise ValueError(f"Metadata category overlap detected: {sorted(overlap)}")

    unknown_fields = sorted(set(metadata.keys()) - CATEGORY_ALL_FIELDS)
    if unknown_fields:
        print(Fore.YELLOW + f"[WARN] Unknown metadata fields (no explicit category): {', '.join(unknown_fields)}")


def normalize_metadata_fields(metadata):
    """Normalize metadata values according to explicit field categories.

    - CSV_TO_TEXT_FIELDS: accepts comma-separated string or YAML list, stored as text.
    - CSV_TO_LIST_FIELDS: accepts comma-separated string or YAML list, stored as list.
    - PASSTHROUGH_FIELDS: preserved as-is.
    """
    if not isinstance(metadata, dict):
        return metadata

    normalized = dict(metadata)
    validate_metadata_field_categories(normalized)

    for field in CSV_TO_TEXT_FIELDS:
        if field in normalized:
            normalized[field] = normalize_text_list_value(normalized.get(field))

    for field in CSV_TO_LIST_FIELDS:
        if field in normalized:
            field_value = normalized.get(field)
            if isinstance(field_value, str):
                normalized[field] = [item.strip() for item in field_value.split(',') if item.strip()]
            elif isinstance(field_value, list):
                normalized[field] = [str(item).strip() for item in field_value if str(item).strip()]

    return normalized


def normalize_translator_value(value):
    """Normalize translator metadata into a storable TEXT value.

    Supports:
    - plain string: "Group A"
    - list: ["Person A", "Person B"]
    - dict keyed by language:
      {"english": ["Person A", "Person B"], "spanish": "Person C"}
    """
    if value is None:
        return None

    if isinstance(value, str):
        normalized = value.strip()
        return normalized or None

    if isinstance(value, list):
        flattened = [str(item).strip() for item in value if str(item).strip()]
        return json.dumps(flattened, ensure_ascii=False) if flattened else None

    if isinstance(value, dict):
        normalized_map = {}
        for language, translators in value.items():
            if not language:
                continue
            language_key = str(language).strip()
            if not language_key:
                continue

            if isinstance(translators, list):
                names = [str(name).strip() for name in translators if str(name).strip()]
            else:
                single = str(translators).strip()
                names = [single] if single else []

            if names:
                normalized_map[language_key] = names

        return json.dumps(normalized_map, ensure_ascii=False) if normalized_map else None

    fallback = str(value).strip()
    return fallback or None


def get_latest_metadata_for_title(title):
    """Fetch the current metadata blob for an existing VN title, if present."""
    if not title:
        return {}
    normalized_title = str(title).strip()
    if not normalized_title:
        return {}

    with get_connection() as conn:
        row = conn.execute(
            '''
            SELECT mo.metadata_json
            FROM visual_novels v
            JOIN builds b ON b.vn_id = v.id
            JOIN metadata_versions mv ON mv.build_id = b.id AND mv.is_current = 1
            JOIN metadata_objects mo ON mo.hash = mv.metadata_hash
            WHERE TRIM(v.title) = TRIM(?) COLLATE NOCASE
            ORDER BY b.created_at DESC, b.id DESC, mv.created_at DESC, mv.id DESC
            LIMIT 1
            ''',
            (normalized_title,)
        ).fetchone()

    if not row or not row['metadata_json']:
        return {}

    try:
        parsed = json.loads(row['metadata_json'])
        return parsed if isinstance(parsed, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


def upsert_series(conn, metadata):
    if not metadata.get('series'):
        return None

    series_name = metadata['series'].strip()
    series_description = get_metadata_value(metadata, 'series_description')
    series_row = conn.execute(
        'SELECT id, description FROM series WHERE name = ?',
        (series_name,)
    ).fetchone()

    if series_row:
        if series_description is not None and series_description != series_row['description']:
            conn.execute(
                'UPDATE series SET description = ? WHERE id = ?',
                (series_description, series_row['id'])
            )
        return series_row['id']

    conn.execute(
        'INSERT INTO series (name, description) VALUES (?, ?)',
        (series_name, series_description)
    )
    return conn.execute('SELECT last_insert_rowid()').fetchone()[0]


def upsert_visual_novel_record(conn, metadata, series_id):
    aliases = normalize_metadata_list(metadata, 'aliases')
    title = metadata['title']

    vn_exists = conn.execute(
        '''
        SELECT id, developer, publisher, description,
               release_status, content_rating, source
        FROM visual_novels
        WHERE title = ?
        ''',
        (title,)
    ).fetchone()

    slug = slugify_component(title, 'unknown-title')

    def effective_vn(field_name):
        if field_name in metadata:
            incoming_value = metadata.get(field_name)
            if field_name == 'description' and vn_exists and vn_exists['description'] and incoming_value:
                # Keep work-level synopsis stable across builds unless no description exists yet.
                return vn_exists['description']
            return incoming_value
        return vn_exists[field_name] if vn_exists else None

    if vn_exists:
        vn_id = vn_exists['id']
        conn.execute('''
            UPDATE visual_novels SET
                series_id = ?, canonical_slug = ?, aliases = ?,
                developer = ?, publisher = ?, description = ?, release_status = ?,
                content_rating = ?, source = ?
            WHERE id = ?
        ''', (
            series_id,
            slug,
            json.dumps(aliases),
            normalize_text_list_value(effective_vn('developer')),
            normalize_text_list_value(effective_vn('publisher')),
            effective_vn('description'),
            effective_vn('release_status'),
            normalize_text_list_value(effective_vn('content_rating')),
            effective_vn('source'),
            vn_id
        ))
        return vn_id

    conn.execute('''
        INSERT INTO visual_novels (
            series_id, title, canonical_slug, aliases,
            developer, publisher, description, release_status, content_rating, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        series_id,
        title,
        slug,
        json.dumps(aliases),
        normalize_text_list_value(metadata.get('developer')),
        normalize_text_list_value(metadata.get('publisher')),
        metadata.get('description'),
        metadata.get('release_status'),
        normalize_text_list_value(metadata.get('content_rating')),
        metadata.get('source')
    ))
    return conn.execute('SELECT last_insert_rowid()').fetchone()[0]


def sync_vn_tags(conn, vn_id, metadata):
    tags = normalize_metadata_list(metadata, 'tags')

    conn.execute('DELETE FROM vn_tags WHERE vn_id = ?', (vn_id,))
    for tag in tags:
        tag_id = conn.execute('SELECT id FROM tags WHERE name = ?', (tag,)).fetchone()
        if not tag_id:
            conn.execute('INSERT INTO tags (name) VALUES (?)', (tag,))
            tag_id = {'id': conn.execute('SELECT last_insert_rowid()').fetchone()[0]}

        conn.execute('INSERT INTO vn_tags (vn_id, tag_id) VALUES (?, ?)', (vn_id, tag_id['id']))


def sync_visual_novel_upload_status(conn, vn_id):
    """Set visual_novels.status based on the newest main build status."""
    main_build_row = conn.execute(
        """
        SELECT status
        FROM builds
        WHERE vn_id = ?
          AND (
              build_type IS NULL
              OR TRIM(build_type) = ''
              OR LOWER(build_type) IN ('full', 'standalone')
          )
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (vn_id,)
    ).fetchone()

    if main_build_row is None:
        # Fallback for libraries that only contain non-main build types.
        main_build_row = conn.execute(
            """
            SELECT status
            FROM builds
            WHERE vn_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (vn_id,)
        ).fetchone()

    target_status = 'uploaded' if (main_build_row and main_build_row['status'] == 'uploaded') else 'local'
    conn.execute('UPDATE visual_novels SET status = ? WHERE id = ?', (target_status, vn_id))


def upsert_build_record(conn, vn_id, metadata):
    build_version = metadata.get('version', '1.0')
    build_language = normalize_text_list_value(metadata.get('language'))
    build_type = metadata.get('build_type')
    build_edition = metadata.get('edition')
    build_distribution_platform = metadata.get('distribution_platform')
    build_exists = conn.execute(
        '''
        SELECT id, build_type, distribution_model, distribution_platform,
               language, translator, edition, original_release_date, release_date, engine,
               engine_version, source
        FROM builds
        WHERE vn_id = ? AND version = ?
          AND COALESCE(language, '') = COALESCE(?, '')
          AND COALESCE(build_type, '') = COALESCE(?, '')
          AND COALESCE(edition, '') = COALESCE(?, '')
          AND COALESCE(distribution_platform, '') = COALESCE(?, '')
        ''',
        (vn_id, build_version, build_language, build_type, build_edition, build_distribution_platform)
    ).fetchone()

    existing = build_exists if build_exists else {}

    def effective(field_name):
        if field_name in metadata:
            return metadata.get(field_name)
        return existing[field_name] if build_exists else None

    values = (
        effective('build_type'),
        effective('distribution_model'),
        effective('distribution_platform'),
        normalize_text_list_value(effective('language')),
        normalize_translator_value(effective('translator')),
        effective('edition'),
        effective('original_release_date'),
        effective('release_date'),
        effective('engine'),
        effective('engine_version'),
        effective('source'),
    )

    if build_exists:
        build_id = build_exists['id']
        conn.execute('''
            UPDATE builds SET
                build_type = ?, distribution_model = ?, distribution_platform = ?,
                language = ?, translator = ?, edition = ?, original_release_date = ?, release_date = ?,
                engine = ?, engine_version = ?, source = ?
            WHERE id = ?
        ''', values + (build_id,))
        return build_id

    conn.execute('''
        INSERT INTO builds (
            vn_id, version, build_type, distribution_model,
            distribution_platform, language, translator, edition,
            original_release_date, release_date, engine, engine_version, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        vn_id,
        build_version,
        *values
    ))
    return conn.execute('SELECT last_insert_rowid()').fetchone()[0]


def sync_canon_relationship(conn, vn_id, metadata):
    parent_title = (metadata.get('parent_vn_title') or '').strip()
    relationship_type = (metadata.get('relationship_type') or '').strip()

    # Keep behavior explicit: only write relationship rows when both values are provided.
    conn.execute('DELETE FROM canon_relationships WHERE child_vn_id = ?', (vn_id,))

    if not parent_title or not relationship_type:
        return

    parent_row = conn.execute(
        'SELECT id FROM visual_novels WHERE title = ?',
        (parent_title,)
    ).fetchone()

    if not parent_row:
        print(Fore.YELLOW + f"[DEBUG] Parent VN '{parent_title}' not found; skipping canon_relationship insert.")
        return

    conn.execute(
        '''
        INSERT INTO canon_relationships (parent_vn_id, child_vn_id, relationship_type)
        VALUES (?, ?, ?)
        ''',
        (parent_row['id'], vn_id, relationship_type)
    )


def sync_build_target_platforms(conn, build_id, metadata):
    target_platforms = normalize_metadata_list(metadata, 'target_platform')

    conn.execute('DELETE FROM build_target_platforms WHERE build_id = ?', (build_id,))
    for platform in target_platforms:
        platform_id = conn.execute('SELECT id FROM target_platforms WHERE name = ?', (platform,)).fetchone()
        if not platform_id:
            conn.execute('INSERT INTO target_platforms (name) VALUES (?)', (platform,))
            platform_id = {'id': conn.execute('SELECT last_insert_rowid()').fetchone()[0]}

        conn.execute(
            'INSERT INTO build_target_platforms (build_id, platform_id) VALUES (?, ?)',
            (build_id, platform_id['id'])
        )


def collect_archives_for_db(metadata):
    archives_to_process = []

    top_level_sha = metadata.get('sha256')
    if not top_level_sha and 'archive' in metadata and isinstance(metadata['archive'], dict):
        top_level_sha = metadata['archive'].get('sha256')

    if top_level_sha:
        archives_to_process.append({
            'sha256': top_level_sha,
            'file_size': metadata.get('file_size_bytes', 0),
            'filename': metadata.get('original_filename') or get_nested_value(metadata, 'archive.filename'),
            'is_primary': 1,
        })

    if 'archives' in metadata and isinstance(metadata['archives'], list):
        for idx, archive in enumerate(metadata['archives']):
            if isinstance(archive, dict) and archive.get('sha256'):
                archives_to_process.append({
                    'sha256': archive.get('sha256'),
                    'file_size': archive.get('file_size_bytes', 0),
                    'filename': archive.get('filename'),
                    'is_primary': 1 if not top_level_sha and idx == 0 else 0,
                })

    if not top_level_sha and archives_to_process:
        top_level_sha = archives_to_process[0].get('sha256')

    return archives_to_process, top_level_sha


def upsert_artifact_record(conn, build_id, metadata, archive_data):
    artifact_sha256 = archive_data.get('sha256')
    if not artifact_sha256:
        return

    artifact_type = str(metadata.get('artifact_type') or '').strip().lower() or "game_archive"
    filename = archive_data.get('filename') or metadata.get('original_filename')
    notes = metadata.get('notes')
    release_date = metadata.get('release_date')
    is_primary = 1 if archive_data.get('is_primary') else 0

    existing_row = conn.execute(
        '''
        SELECT artifact_id
        FROM artifacts
        WHERE build_id = ? AND sha256 = ?
        ''',
        (build_id, artifact_sha256)
    ).fetchone()

    if existing_row:
        conn.execute(
            '''
            UPDATE artifacts
            SET artifact_type = COALESCE(NULLIF(?, ''), artifact_type),
                filename = COALESCE(?, filename),
                is_primary = CASE WHEN ? = 1 THEN 1 ELSE is_primary END,
                release_date = COALESCE(?, release_date),
                notes = COALESCE(?, notes)
            WHERE artifact_id = ?
            ''',
            (artifact_type, filename, is_primary, release_date, notes, existing_row['artifact_id'])
        )
        return existing_row['artifact_id']

    conn.execute(
        '''
        INSERT INTO artifacts (
            build_id, artifact_type, filename, sha256, is_primary, base_artifact_id, release_date, notes
        ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?)
        ''',
        (build_id, artifact_type, filename, artifact_sha256, is_primary, release_date, notes)
    )
    return conn.execute('SELECT last_insert_rowid()').fetchone()[0]


def finalize_metadata_objects(conn, metadata, vn_id, build_id):
    try:
        schema_version = int(metadata.get('metadata_version') or 1)
    except (ValueError, TypeError):
        schema_version = 1

    # Keep hash canonical for dedup/version comparisons, but preserve human-friendly
    # metadata field order when storing metadata_json for history/export readability.
    canonical_json = json.dumps(
        metadata,
        default=safe_json_serialize,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":")
    )
    stored_metadata = order_metadata_for_yaml(metadata)
    stored_metadata_json = json.dumps(
        stored_metadata,
        default=safe_json_serialize,
        ensure_ascii=False,
        separators=(",", ":")
    )
    metadata_hash = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()

    conn.execute('''
        INSERT OR IGNORE INTO metadata_objects (hash, schema_version, metadata_json)
        VALUES (?, ?, ?)
    ''', (metadata_hash, schema_version, stored_metadata_json))

    current_row = conn.execute(
        'SELECT id, metadata_hash FROM metadata_versions WHERE build_id = ? AND is_current = 1',
        (build_id,)
    ).fetchone()

    if current_row and current_row['metadata_hash'] == metadata_hash:
        print(Fore.MAGENTA + f'[DEBUG] Metadata version unchanged for build {build_id}; current pointer retained.')
        return

    parent_version_id = current_row['id'] if current_row else None
    next_version_number = conn.execute(
        'SELECT COALESCE(MAX(version_number), 0) + 1 FROM metadata_versions WHERE build_id = ?',
        (build_id,)
    ).fetchone()[0]

    conn.execute(
        'UPDATE metadata_versions SET is_current = 0 WHERE build_id = ? AND is_current = 1',
        (build_id,)
    )

    conn.execute('''
        INSERT INTO metadata_versions (
            vn_id, build_id, metadata_hash, parent_version_id, version_number, change_note, is_current
        ) VALUES (?, ?, ?, ?, ?, ?, 1)
    ''', (
        vn_id,
        build_id,
        metadata_hash,
        parent_version_id,
        next_version_number,
        metadata.get('change_note')
    ))

    print(Fore.GREEN + f'[DEBUG] Metadata version v{next_version_number} recorded for build {build_id}.')


def finalize_artifact_metadata_objects(conn, metadata, artifact_id):
    try:
        schema_version = int(metadata.get('metadata_version') or 1)
    except (ValueError, TypeError):
        schema_version = 1

    canonical_json = json.dumps(
        metadata,
        default=safe_json_serialize,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":")
    )
    stored_metadata = order_metadata_for_yaml(metadata)
    stored_metadata_json = json.dumps(
        stored_metadata,
        default=safe_json_serialize,
        ensure_ascii=False,
        separators=(",", ":")
    )
    metadata_hash = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()

    conn.execute(
        '''
        INSERT OR IGNORE INTO artifact_metadata_objects (hash, schema_version, metadata_json)
        VALUES (?, ?, ?)
        ''',
        (metadata_hash, schema_version, stored_metadata_json)
    )

    current_row = conn.execute(
        'SELECT id, metadata_hash FROM artifact_metadata_versions WHERE artifact_id = ? AND is_current = 1',
        (artifact_id,)
    ).fetchone()

    if current_row and current_row['metadata_hash'] == metadata_hash:
        print(Fore.MAGENTA + f'[DEBUG] Artifact metadata unchanged for artifact {artifact_id}; current pointer retained.')
        return

    parent_version_id = current_row['id'] if current_row else None
    next_version_number = conn.execute(
        'SELECT COALESCE(MAX(version_number), 0) + 1 FROM artifact_metadata_versions WHERE artifact_id = ?',
        (artifact_id,)
    ).fetchone()[0]

    conn.execute(
        'UPDATE artifact_metadata_versions SET is_current = 0 WHERE artifact_id = ? AND is_current = 1',
        (artifact_id,)
    )

    conn.execute(
        '''
        INSERT INTO artifact_metadata_versions (
            artifact_id, metadata_hash, parent_version_id, version_number, change_note, is_current
        ) VALUES (?, ?, ?, ?, ?, 1)
        ''',
        (artifact_id, metadata_hash, parent_version_id, next_version_number, metadata.get('change_note'))
    )

    print(Fore.GREEN + f'[DEBUG] Artifact metadata version v{next_version_number} recorded for artifact {artifact_id}.')


def process_archives_for_build(conn, build_id, metadata, vn_id, archives_to_process):
    print(Fore.CYAN + f'\n[DEBUG] Found {len(archives_to_process)} archive(s) to process for DB.')
    artifact_ids = []

    for arch_data in archives_to_process:
        sha256 = arch_data.get('sha256')
        file_size = arch_data.get('file_size', 0)

        if not sha256:
            print(Fore.RED + '[DEBUG] Skipping archive insertion - missing SHA256.')
            continue

        artifact_id = upsert_artifact_record(conn, build_id, metadata, arch_data)

        archive_exists = conn.execute(
            'SELECT id FROM archives WHERE build_id = ? AND sha256 = ?',
            (build_id, sha256)
        ).fetchone()

        if not archive_exists:
            print(Fore.YELLOW + f'[DEBUG] Executing SQL INSERT INTO archives for {sha256[:8]}...')
            try:
                conn.execute('''
                    INSERT INTO archives (
                        build_id, sha256, file_size_bytes,
                        metadata_json, metadata_version
                    ) VALUES (?, ?, ?, ?, ?)
                ''', (
                    build_id,
                    sha256,
                    file_size,
                    json.dumps(metadata),
                    metadata.get('metadata_version', 1)
                ))
                print(Fore.GREEN + f'[DEBUG] Successfully queued archive {sha256[:8]} for commit.')
            except Exception as ex:
                print(Fore.RED + f'[CRITICAL] SQL Archive Insert Failed: {ex}')
            continue

        print(Fore.MAGENTA + f'[DEBUG] Archive {sha256[:8]} is already in DB. Skipping insert.')

    try:
        if is_artifact_metadata(metadata):
            for artifact_id in sorted(set(artifact_ids)):
                finalize_artifact_metadata_objects(conn, metadata, artifact_id)
        else:
            finalize_metadata_objects(conn, metadata, vn_id, build_id)
    except Exception as e:
        print(Fore.RED + f'[CRITICAL] Final DB commit failed: {e}')
        raise e

    try:
        conn.commit()
        print(Fore.GREEN + '[DEBUG] Database transaction committed successfully!')
    except Exception as e:
        print(Fore.RED + f'[CRITICAL] SQLite Commit Failed: {e}')
        raise e

    return vn_id

    return None


def insert_visual_novel(metadata):
    '''
    Inserts or updates the normalized metadata into the SQLite database.
    '''

    metadata = normalize_metadata_fields(metadata)

    with get_connection() as conn:
        if not metadata.get('title'):
            raise ValueError('Title is required.')

        series_id = upsert_series(conn, metadata)
        vn_id = upsert_visual_novel_record(conn, metadata, series_id)

        sync_vn_tags(conn, vn_id, metadata)

        build_id = upsert_build_record(conn, vn_id, metadata)
        sync_build_target_platforms(conn, build_id, metadata)
        sync_canon_relationship(conn, vn_id, metadata)

        archives_to_process, _ = collect_archives_for_db(metadata)

        early_return_vn_id = process_archives_for_build(
            conn,
            build_id,
            metadata,
            vn_id,
            archives_to_process
        )
        if early_return_vn_id:
            return early_return_vn_id

        return vn_id

        # ==========================================================
        # RETURN VALUE (For new inserts and metadata-only operations)
        # ==========================================================
        # If we reached this point, the VN/build metadata was inserted or
        # updated successfully even when no archive row existed yet.
        return vn_id


# ==============================
# BACKBLAZE
# ==============================
def get_b2_api():
    key_id, application_key, _, _ = load_b2_config()

    info = InMemoryAccountInfo()
    b2_api = B2Api(info)
    b2_api.authorize_account(
        "production",
        key_id,
        application_key
    )
    return b2_api


def upload_to_b2(filepath, remote_folder=None):
    """
    Upload file to Backblaze.
    dry_run in config prevents any real upload.
    """

    _, _, bucket_name, dry_run = load_b2_config()

    if not os.path.exists(filepath):
        raise Exception("File does not exist for upload.")

    filename = os.path.basename(filepath)

    if remote_folder:
        remote_name = f"{remote_folder}/{filename}"
    else:
        remote_name = filename

    # ---------------------------
    # DRY RUN (SAFE MODE)
    # ---------------------------
    if dry_run:
        print("\n[DRY RUN ENABLED]")
        print(f"Would upload:")
        print(f"  Local file : {filepath}")
        print(f"  Bucket     : {bucket_name}")
        print(f"  Remote path: {remote_name}")
        print("No upload performed.\n")
        return False

    # ---------------------------
    # CONFIRMATION (EXTRA SAFETY)
    # ---------------------------
    confirm = input(f"Upload '{remote_name}' to Backblaze? (yes/no): ").strip().lower()
    if confirm != "yes":
        print("Upload cancelled.")
        return False

    # ---------------------------
    # REAL UPLOAD
    # ---------------------------
    b2_api = get_b2_api()
    bucket = b2_api.get_bucket_by_name(bucket_name)

    file_size = os.path.getsize(filepath)

    class UploadProgressListener:
        """Advanced console progress display for Backblaze uploads."""

        def __init__(self, total_bytes):
            self.total_bytes = total_bytes
            self.start_time = time.time()
            self.last_update_time = 0

        def set_total_bytes(self, total_bytes):
            self.total_bytes = total_bytes

        def bytes_completed(self, byte_count):
            if self.total_bytes <= 0:
                return

            now = time.time()

            # Limit redraw frequency to avoid flicker
            if now - self.last_update_time < 0.1 and byte_count < self.total_bytes:
                return

            self.last_update_time = now

            percent = (byte_count / self.total_bytes) * 100
            elapsed = now - self.start_time
            speed = byte_count / elapsed if elapsed > 0 else 0

            bar_length = 30
            filled = int((byte_count / self.total_bytes) * bar_length)
            bar = "#" * filled + "-" * (bar_length - filled)

            sys.stdout.write(
                f"\rUploading: [{bar}] "
                f"{percent:6.2f}% "
                f"{byte_count / 1024 / 1024:8.2f}MB / {self.total_bytes / 1024 / 1024:8.2f}MB "
                f"{speed / 1024 / 1024:6.2f} MB/s"
            )
            sys.stdout.flush()

        def close(self):
            sys.stdout.write("\n")
            sys.stdout.flush()

    progress_listener = UploadProgressListener(file_size)

    bucket.upload_local_file(
        local_file=filepath,
        file_name=remote_name,
        progress_listener=progress_listener
    )

    progress_listener.close()

    print(f"Uploaded to Backblaze: {remote_name}")
    return True


def create_archive_only(archive_paths=None, metadata_version=DEFAULT_METADATA_VERSION):
    if archive_paths is None:
        archive_paths = []
    elif isinstance(archive_paths, str):
        archive_paths = [archive_paths]


    if archive_paths:
        print(f"\nProcessing {len(archive_paths)} file(s)...")

    # -------------------------------------------------------------------
    # 1. Gather data for all archives
    # -------------------------------------------------------------------
    archives_data = []
    for path in archive_paths:
        print(f"Calculating SHA-256 for: {os.path.basename(path)}...")
        sha256 = sha256_file(path)
        file_size = os.path.getsize(path)

        archives_data.append({
            "original_path": path,
            "filename": os.path.basename(path),
            "file_size_bytes": file_size,
            "sha256": sha256
        })

    # -------------------------------------------------------------------
    # 2. Prepare metadata (Beautiful TUI Prompts)
    # -------------------------------------------------------------------
    base_template = load_metadata_template(metadata_version)
    prompt_fields = [field for field in resolve_prompt_fields(base_template) if field != "artifact_type"]

    metadata = {"metadata_version": metadata_version}
    defaults = {}

    FIELD_SUGGESTIONS = {
        "release_status": ["ongoing", "completed", "hiatus", "cancelled", "abandoned"],
        "distribution_model": ["free", "paid", "freemium", "donationware", "subscription", "patron_only"],
        "build_type": ["full", "demo", "trial", "alpha", "beta", "release-candidate", "patch", "dlc", "standalone"],
        "language": ["japanese", "english", "chinese-simplified", "chinese-traditional", "korean", "spanish", "german",
                     "french", "russian", "multi-language"],
        "distribution_platform": ["steam", "itch.io", "dlsite", "fanza", "gumroad", "patreon", "booth",
                                  "self-distributed", "other"],
        "content_rating": ["all-ages", "teen", "mature", "18+", "unrated"],
        "content_mode": ["sfw", "nsfw", "selectable", "patchable", "mixed", "unknown"],
        "target_platform": ["windows", "linux", "mac", "android", "web", "ios", "switch"],
        "content_type": ["main_story", "story_expansion", "seasonal_event", "april_fools", "side_story",
                         "non_canon_special"],
        "artifact_type": SUGGESTED_ARTIFACT_TYPE,
        "tags": [
            "romance", "drama", "comedy", "slice-of-life", "mystery", "horror", "sci-fi",
            "fantasy", "psychological", "thriller", "action", "historical", "supernatural",
            "nakige", "utsuge", "nukige", "moege", "dark", "wholesome", "tragic", "bittersweet",
            "school", "modern", "adult"
        ]
    }

    def normalize_list(val):
        if not val: return None
        return sorted(set([v.strip() for v in val.split(",") if v.strip()]))

    print(Fore.MAGENTA + "\nFill Metadata (Press ENTER to skip fields)\n")
    print(Fore.CYAN + "Tip: when a [default] is shown, press ENTER to keep it, or type '-' to clear it.")

    for field in prompt_fields:
        if field in ("tags", "target_platform", "aliases", "developer", "publisher"):
            suggestions = FIELD_SUGGESTIONS.get(field) or []
            if suggestions:
                print(Fore.CYAN + f"Suggested {field}: " + ", ".join(suggestions))

            default_val = defaults.get(field)
            if isinstance(default_val, list):
                default_items = [str(v).strip() for v in default_val if str(v).strip()]
            elif field in ("developer", "publisher") and isinstance(default_val, str):
                default_items = [v.strip() for v in default_val.split(',') if v.strip()]
            else:
                default_items = []

            default_display = ", ".join(default_items) if default_items else ""
            prompt = f"{field} (comma separated)"
            if default_display:
                prompt += f" [{default_display}]"
            raw_val = input(Fore.YELLOW + f"{prompt}: ").strip()

            if raw_val == "-":
                metadata[field] = []
            elif raw_val:
                metadata[field] = normalize_list(raw_val)
            elif default_items:
                metadata[field] = default_items

        else:
            suggestions = FIELD_SUGGESTIONS.get(field) or []
            if suggestions:
                print(Fore.CYAN + f"Suggested {field}: " + ", ".join(suggestions))
            default_val = defaults.get(field)
            prompt = f"{field}"
            if default_val not in (None, ""):
                prompt += f" [{default_val}]"
            raw_val = input(Fore.YELLOW + f"{prompt}: ").strip()

            if raw_val == "-":
                metadata[field] = ""
            elif raw_val:
                metadata[field] = raw_val
                if field == "title":
                    defaults = get_latest_metadata_for_title(raw_val)
                    if defaults:
                        print(Fore.GREEN + f"Loaded defaults from latest metadata for '{raw_val}'.")
                        latest_known_version = defaults.get('version')
                        if latest_known_version not in (None, ""):
                            print(
                                Fore.CYAN
                                + f"Latest known version is '{latest_known_version}'. "
                                "Press ENTER on version to reuse it, or type a new version."
                            )
                        defaults.pop('archives', None)
                        defaults.pop('metadata_version', None)
            elif default_val not in (None, ""):
                metadata[field] = default_val

    # -------------------------------------------------------------------
    # 3. Inject the multi-archive data
    # -------------------------------------------------------------------
    if archives_data:
        archives_list = []
        for a in archives_data:
            archives_list.append({
                "filename": a.get("filename"),
                "file_size_bytes": a.get("file_size_bytes"),
                "sha256": a.get("sha256")
            })
        metadata["archives"] = archives_list

    finalize_archive_creation(metadata, archives_data)


def finalize_archive_creation(metadata, archives_data):
    """Shared finalization flow for prompted and pre-filled metadata runs."""
    vn_id = insert_visual_novel(metadata)
    if not vn_id:
        print(Fore.RED + "Failed to insert visual novel into database.")
        return

    build_id = None
    build_language = normalize_text_list_value(metadata.get('language'))
    build_type = metadata.get('build_type')
    build_edition = metadata.get('edition')
    build_distribution_platform = metadata.get('distribution_platform')
    with get_connection() as conn:
        build_row = conn.execute(
            '''
            SELECT id FROM builds
            WHERE vn_id = ? AND version = ?
              AND COALESCE(language, '') = COALESCE(?, '')
              AND COALESCE(build_type, '') = COALESCE(?, '')
              AND COALESCE(edition, '') = COALESCE(?, '')
              AND COALESCE(distribution_platform, '') = COALESCE(?, '')
            ''',
            (vn_id, metadata.get('version'), build_language, build_type, build_edition, build_distribution_platform)
        ).fetchone()
        if build_row:
            build_id = build_row['id']

    metadata_version_number = get_current_metadata_version_number(vn_id=vn_id, build_id=build_id)
    if is_artifact_metadata(metadata):
        with get_connection() as conn:
            first_sha = archives_data[0].get("sha256") if archives_data else None
            artifact_id = resolve_artifact_id_for_metadata(conn, build_id, metadata, fallback_sha=first_sha)
        metadata_version_number = get_current_artifact_metadata_version_number(artifact_id)

    if archives_data:
        uploaded_dest_dir = os.path.join(UPLOADING_DIR)
        os.makedirs(uploaded_dest_dir, exist_ok=True)

        print(Fore.CYAN + f"\nMoving files to upload queue directory: {uploaded_dest_dir}...")

        staged_meta_path = stage_metadata_yaml_for_upload(
            metadata,
            metadata_version_number,
            target_dir=uploaded_dest_dir
        )
        print(Fore.GREEN + f"Staged metadata for upload: {staged_meta_path}")

        latest_meta_path = stage_metadata_yaml_for_upload(metadata, metadata_version_number)
        print(Fore.GREEN + f"Staged metadata in latest upload folder: {latest_meta_path}")

        # Move archives into uploading queue and mirror source assets into versioned vn archive/
        vn_archive_version_dir = get_vn_archive_version_dir(metadata)
        metadata_copy_name = Path(staged_meta_path).name
        shutil.copy2(staged_meta_path, vn_archive_version_dir / metadata_copy_name)

        for arch in archives_data:
            original_ext = os.path.splitext(arch["filename"])[1].lower() or ".zip"
            recommended_name = build_recommended_archive_name(
                metadata,
                arch.get("sha256"),
                ext=original_ext
            )

            original_copy = vn_archive_version_dir / arch["filename"]
            shutil.copy2(arch["original_path"], original_copy)

            archive_stem = Path(arch["filename"]).stem
            source_folder = Path(arch["original_path"]).parent / archive_stem
            target_folder = vn_archive_version_dir / archive_stem
            if source_folder.is_dir():
                if target_folder.exists():
                    shutil.rmtree(target_folder)
                shutil.move(str(source_folder), str(target_folder))

            dest_file = os.path.join(uploaded_dest_dir, recommended_name)
            shutil.move(arch["original_path"], dest_file)

            print(Fore.GREEN + f"Moved to uploading and mirrored to VN archive: {arch['filename']}")

        print(Fore.GREEN + f"\nUpload queue prepared at: {uploaded_dest_dir}")
        print(Fore.GREEN + f"VN archive updated at: {vn_archive_version_dir}")
        print(Fore.GREEN + "Archive processing complete!")
    else:
        staged_meta_path = stage_metadata_yaml_for_upload(metadata, metadata_version_number)
        print(Fore.GREEN + f"\nMetadata staged for upload: {staged_meta_path}")
        print(Fore.GREEN + "Metadata creation complete!")


def create_archive_from_metadata_file(archive_paths, metadata):
    """Create archive pipeline from existing metadata.yaml without prompts."""
    archives_data = []
    for path in archive_paths:
        print(f"Calculating SHA-256 for: {os.path.basename(path)}...")
        sha256 = sha256_file(path)
        file_size = os.path.getsize(path)
        archives_data.append({
            "original_path": path,
            "filename": os.path.basename(path),
            "file_size_bytes": file_size,
            "sha256": sha256
        })

    prepared = dict(metadata or {})
    prepared.setdefault("metadata_version", detect_latest_metadata_template_version())
    if archives_data:
        prepared["archives"] = [
            {
                "filename": a["filename"],
                "file_size_bytes": a["file_size_bytes"],
                "sha256": a["sha256"]
            }
            for a in archives_data
        ]

    finalize_archive_creation(prepared, archives_data)


def format_uploaded_component(value, fallback):
    text = str(value or "").replace("_", " ").strip()
    text = " ".join(text.split())
    return text or fallback


def get_current_metadata_version_number(vn_id=None, build_id=None):
    """Return active metadata_versions.version_number for a build (or fallback VN scope)."""
    if not build_id and not vn_id:
        return 1

    with get_connection() as conn:
        if build_id:
            row = conn.execute(
                'SELECT version_number FROM metadata_versions WHERE build_id = ? AND is_current = 1',
                (build_id,)
            ).fetchone()
        else:
            row = conn.execute(
                'SELECT version_number FROM metadata_versions WHERE vn_id = ? AND is_current = 1 ORDER BY created_at DESC, id DESC LIMIT 1',
                (vn_id,)
            ).fetchone()

    return int(row['version_number']) if row and row['version_number'] is not None else 1


def get_current_artifact_metadata_version_number(artifact_id):
    if not artifact_id:
        return 1

    with get_connection() as conn:
        row = conn.execute(
            'SELECT version_number FROM artifact_metadata_versions WHERE artifact_id = ? AND is_current = 1',
            (artifact_id,)
        ).fetchone()

    return int(row['version_number']) if row and row['version_number'] is not None else 1


def resolve_artifact_id_for_metadata(conn, build_id, metadata, fallback_sha=None):
    if not build_id:
        return None

    sha_candidates = []
    if fallback_sha:
        sha_candidates.append(str(fallback_sha).strip().lower())

    top_sha = str(metadata.get('sha256') or '').strip().lower()
    if top_sha:
        sha_candidates.append(top_sha)

    archives = metadata.get('archives')
    if isinstance(archives, list):
        for archive in archives:
            if isinstance(archive, dict) and archive.get('sha256'):
                sha_candidates.append(str(archive.get('sha256')).strip().lower())

    for sha in sha_candidates:
        if not sha:
            continue
        row = conn.execute(
            'SELECT artifact_id FROM artifacts WHERE build_id = ? AND sha256 = ?',
            (build_id, sha)
        ).fetchone()
        if row:
            return row['artifact_id']

    return None


def build_recommended_archive_name(metadata, sha256, ext='.zip'):
    title_slug = slugify_component(metadata.get('title'), 'unknown')
    version_slug = slugify_component(metadata.get('version'), 'unknown')
    short_hash = (sha256 or 'nohash')[:8]
    safe_ext = ext if ext.startswith('.') else f'.{ext}'
    return f"{title_slug}_{version_slug}_{short_hash}{safe_ext}"


def build_recommended_metadata_name(metadata, sha256, metadata_version_number):
    title_slug = slugify_component(metadata.get('title'), 'unknown')
    version_slug = slugify_component(metadata.get('version'), 'unknown')
    short_hash = (sha256 or 'nohash')[:8]
    return f"{title_slug}_{version_slug}_{short_hash}_meta_v{metadata_version_number}.yaml"


def order_metadata_for_yaml(metadata):
    """Return metadata ordered exactly by metadata template field order."""
    if not isinstance(metadata, dict):
        return metadata

    try:
        template_version = int(metadata.get('metadata_version') or DEFAULT_METADATA_VERSION)
    except (ValueError, TypeError):
        template_version = DEFAULT_METADATA_VERSION

    try:
        template = load_metadata_template(template_version)
    except FileNotFoundError:
        # Keep quick/sidecar processing resilient when a metadata file references
        # a template version that is not currently available on disk.
        print(
            Fore.YELLOW
            + f"Metadata template v{template_version} not found; preserving existing field order."
        )
        return dict(metadata)
    if not isinstance(template, dict):
        return dict(metadata)

    ordered = {}

    template_field_order = ['metadata_version']

    required_fields = template.get('required')
    if isinstance(required_fields, list):
        template_field_order.extend(
            field for field in required_fields if isinstance(field, str)
        )

    optional_fields = template.get('optional')
    if isinstance(optional_fields, list):
        template_field_order.extend(
            field for field in optional_fields if isinstance(field, str)
        )

    if 'archives' in template and 'archives' not in template_field_order:
        template_field_order.append('archives')

    for key in template_field_order:
        if key in metadata:
            ordered[key] = metadata[key]

    for key, value in metadata.items():
        if key not in ordered:
            ordered[key] = value

    return ordered


def stage_metadata_yaml_for_upload(metadata, metadata_version_number, target_dir=None):
    """Create a metadata.yaml copy and stage it in uploading/ with recommended naming."""
    meta_sha = metadata.get('sha256') or get_nested_value(metadata, 'archive.sha256')
    if not meta_sha and isinstance(metadata.get('archives'), list) and metadata['archives']:
        first_arch = metadata['archives'][0]
        if isinstance(first_arch, dict):
            meta_sha = first_arch.get('sha256')

    final_name = build_recommended_metadata_name(metadata, meta_sha, metadata_version_number)

    if target_dir is None:
        target_dir = get_uploading_latest_dir(metadata)
    target_dir = Path(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    temp_meta_path = target_dir / 'metadata.yaml'
    ordered_metadata = order_metadata_for_yaml(metadata)
    with open(temp_meta_path, 'w', encoding='utf-8') as handle:
        yaml.dump(ordered_metadata, handle, sort_keys=False, allow_unicode=True)

    final_path = target_dir / final_name
    if final_path.exists():
        final_path.unlink()
    temp_meta_path.rename(final_path)
    return final_path


def get_uploading_latest_dir(metadata):
    # Keep upload queue flat (no title/version folder structure required).
    return Path(UPLOADING_DIR)


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
    valid_versions = [str(v).strip() for v in versions if str(v).strip()]
    if not valid_versions:
        return "unknown"
    return max(valid_versions, key=normalize_version_for_sort)


def get_vn_archive_version_dir(metadata):
    title = format_uploaded_component(metadata.get("title"), "Unknown Title")
    current_version = format_uploaded_component(metadata.get("version"), "unknown")

    title_root = Path(VN_ARCHIVE_DIR)
    title_root.mkdir(parents=True, exist_ok=True)

    sibling_versions = [current_version]
    existing_title_parent = None
    for entry in title_root.iterdir():
        if not entry.is_dir():
            continue
        prefix = f"{title} "
        if not entry.name.startswith(prefix):
            continue
        existing_title_parent = entry
        parent_version = entry.name[len(prefix):].strip()
        if parent_version:
            sibling_versions.append(parent_version)
        for child in entry.iterdir():
            if child.is_dir() and child.name:
                sibling_versions.append(child.name)
        break

    latest_version = determine_latest_version(sibling_versions)
    target_parent = title_root / f"{title} {latest_version}"

    if existing_title_parent and existing_title_parent != target_parent:
        if target_parent.exists():
            for child in existing_title_parent.iterdir():
                destination = target_parent / child.name
                if destination.exists():
                    if destination.is_dir():
                        shutil.rmtree(destination)
                    else:
                        destination.unlink(missing_ok=True)
                shutil.move(str(child), str(destination))
            existing_title_parent.rmdir()
        else:
            existing_title_parent.rename(target_parent)

    target_parent.mkdir(parents=True, exist_ok=True)
    target_version_dir = target_parent / current_version
    target_version_dir.mkdir(parents=True, exist_ok=True)
    return target_version_dir


def get_b2_bucket():
    # 1. Check if the config file exists
    if not os.path.exists(B2_CONFIG_FILE):
        print(Fore.RED + f"Config file '{B2_CONFIG_FILE}' not found.")
        print(Fore.YELLOW + "Creating a blank template. Please fill it out and try again.")

        template = {
            "b2_key_id": "YOUR_KEY_ID_HERE",
            "b2_application_key": "YOUR_APPLICATION_KEY_HERE",
            "b2_bucket_name": "YOUR_BUCKET_NAME_HERE"
        }
        with open(B2_CONFIG_FILE, "w", encoding="utf-8") as f:
            yaml.dump(template, f, default_flow_style=False, sort_keys=False)

        return None, None

    # 2. Read credentials from the YAML file
    try:
        with open(B2_CONFIG_FILE, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
    except Exception as e:
        print(Fore.RED + f"Failed to read '{B2_CONFIG_FILE}': {e}")
        return None, None

    b2_key_id = config.get("b2_key_id")
    b2_application_key = config.get("b2_application_key")
    b2_bucket_name = config.get("b2_bucket_name")

    # 3. Block upload if credentials are still the default placeholders
    if not b2_key_id or not b2_application_key or not b2_bucket_name or b2_key_id == "YOUR_KEY_ID_HERE":
        print(Fore.RED + f"Credentials missing. Please edit '{B2_CONFIG_FILE}' with your actual B2 keys.")
        return None, None

    # 4. Authenticate with Backblaze
    info = InMemoryAccountInfo()
    api = B2Api(info)
    try:
        api.authorize_account("production", b2_key_id, b2_application_key)
        bucket = api.get_bucket_by_name(b2_bucket_name)
        return api, bucket
    except Exception as e:
        print(Fore.RED + f"Failed to authorize Backblaze B2: {e}")
        return None, None


def upload_archive(file_path):
    if not os.path.exists(file_path):
        print(Fore.RED + f"File not found: {file_path}")
        return False

    print(Fore.CYAN + f"\nAnalyzing {os.path.basename(file_path)}...")

    # -------------------------------------------------------------------
    # 1. Read metadata only from queued sidecar file
    # -------------------------------------------------------------------
    metadata = None
    metadata_source = None
    selected_sidecar = None

    archive_stem = Path(file_path).stem
    sidecar_dir = Path(file_path).parent
    sidecar_pattern = re.compile(rf"^{re.escape(archive_stem)}_meta_v\d+\.ya?ml$", re.IGNORECASE)
    sidecar_candidates = [
        candidate for candidate in sidecar_dir.iterdir()
        if candidate.is_file() and sidecar_pattern.match(candidate.name)
    ]

    def sidecar_sort_key(path_obj):
        match = re.search(r"_meta_v(\d+)\.ya?ml$", path_obj.name)
        numeric_version = int(match.group(1)) if match else -1
        return (numeric_version, path_obj.stat().st_mtime, path_obj.name)

    sidecar_candidates.sort(key=sidecar_sort_key)

    if sidecar_candidates:
        selected_sidecar = sidecar_candidates[-1]
        try:
            with open(selected_sidecar, 'r', encoding='utf-8') as handle:
                metadata = yaml.safe_load(handle)
                metadata_source = str(selected_sidecar)
        except Exception as e:
            print(Fore.RED + f"Upload Blocked: Failed to read sidecar metadata file '{selected_sidecar.name}': {e}")
            return False

    if not isinstance(metadata, dict):
        print(Fore.RED + "Upload Blocked: Could not find valid metadata sidecar file.")
        print(Fore.YELLOW + "Expected '<archive_name>_meta_vN.yaml' next to the archive in uploading/.")
        return False

    metadata = normalize_metadata_fields(metadata)

    print(Fore.CYAN + f"Metadata source: sidecar file ({metadata_source})")

    revision_match = re.search(r"_meta_v(\d+)\.ya?ml$", Path(metadata_source).name)
    requested_metadata_revision = int(revision_match.group(1)) if revision_match else None

    title = str(metadata.get("title", "")).strip()
    version = str(metadata.get("version", "")).strip()
    language = normalize_text_list_value(metadata.get("language")) or ""
    build_type = str(metadata.get("build_type", "")).strip()
    edition = str(metadata.get("edition", "")).strip()
    distribution_platform = str(metadata.get("distribution_platform", "")).strip()
    is_artifact_content = str(metadata.get("content_type", "")).strip().lower() == "artifact"

    if not title:
        print(Fore.RED + "Upload Blocked: metadata sidecar is missing 'title'.")
        return False

    # -------------------------------------------------------------------
    # 2. Block upload if it wasn't inserted into the Database
    # -------------------------------------------------------------------
    vn_id = None
    build_id = None
    with get_connection() as conn:
        vn_row = conn.execute("SELECT id FROM visual_novels WHERE title = ?", (title,)).fetchone()
        if not vn_row:
            print(Fore.RED + f"Upload Blocked: Visual Novel '{title}' does not exist in the database.")
            print(Fore.YELLOW + "Please run '(1) Create Metadata' to register it before uploading.")
            return False

        vn_id = vn_row[0]

        if version:
            build_row = conn.execute(
                """
                SELECT id, version FROM builds
                WHERE vn_id = ? AND version = ?
                  AND COALESCE(language, '') = COALESCE(?, '')
                  AND COALESCE(build_type, '') = COALESCE(?, '')
                  AND COALESCE(edition, '') = COALESCE(?, '')
                  AND COALESCE(distribution_platform, '') = COALESCE(?, '')
                """,
                (vn_id, version, language, build_type, edition, distribution_platform)
            ).fetchone()
            if not build_row:
                lang_label = language if language else "default"
                edition_label = edition if edition else "default"
                print(Fore.RED + f"Upload Blocked: Version '{version}' (language={lang_label}, edition={edition_label}) for '{title}' does not exist in the database.")
                print(Fore.YELLOW + "Please run '(1) Create Metadata' to register this build before uploading.")
                return False
        else:
            build_row = conn.execute(
                "SELECT id, version FROM builds WHERE vn_id = ? ORDER BY created_at DESC, id DESC LIMIT 1",
                (vn_id,)
            ).fetchone()
            if not build_row:
                print(Fore.RED + f"Upload Blocked: Visual Novel '{title}' has no builds in the database.")
                print(Fore.YELLOW + "Please run '(1) Create Metadata' to register a build before uploading.")
                return False
            version = str(build_row["version"]).strip()
            print(Fore.YELLOW + f"No version supplied in sidecar metadata; using latest DB build version: {version}")

        build_id = build_row["id"]

    # -------------------------------------------------------------------
    # 3. Validate sidecar metadata revision against DB metadata history
    # -------------------------------------------------------------------
    with get_connection() as conn:
        if is_artifact_metadata(metadata):
            artifact_id = resolve_artifact_id_for_metadata(conn, build_id, metadata)
            if not artifact_id:
                print(Fore.RED + f"Upload Blocked: Could not resolve artifact row for build {build_id} from sidecar metadata sha256.")
                return False
            if requested_metadata_revision is not None:
                metadata_row = conn.execute(
                    "SELECT metadata_hash, version_number FROM artifact_metadata_versions WHERE artifact_id = ? AND version_number = ?",
                    (artifact_id, requested_metadata_revision)
                ).fetchone()
            else:
                metadata_row = conn.execute(
                    "SELECT metadata_hash, version_number FROM artifact_metadata_versions WHERE artifact_id = ? AND is_current = 1",
                    (artifact_id,)
                ).fetchone()
        else:
            if requested_metadata_revision is not None:
                metadata_row = conn.execute(
                    "SELECT metadata_hash, version_number FROM metadata_versions WHERE build_id = ? AND version_number = ?",
                    (build_id, requested_metadata_revision)
                ).fetchone()
            else:
                metadata_row = conn.execute(
                    "SELECT metadata_hash, version_number FROM metadata_versions WHERE build_id = ? AND is_current = 1",
                    (build_id,)
                ).fetchone()

    if not metadata_row:
        if requested_metadata_revision is not None:
            print(Fore.RED + f"Upload Blocked: {'Artifact' if is_artifact_metadata(metadata) else 'Build'} {build_id} has no metadata version v{requested_metadata_revision} in database.")
        else:
            print(Fore.RED + f"Upload Blocked: {'Artifact' if is_artifact_metadata(metadata) else 'Build'} {build_id} has no current metadata version in database.")
        print(Fore.YELLOW + "Please run '(1) Create Metadata' or update metadata before uploading.")
        return False

    db_metadata_hash = metadata_row["metadata_hash"]
    db_version_number = metadata_row["version_number"]

    canonical_metadata_json = json.dumps(
        metadata,
        default=safe_json_serialize,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":")
    )
    sidecar_metadata_hash = hashlib.sha256(canonical_metadata_json.encode("utf-8")).hexdigest()
    if sidecar_metadata_hash != db_metadata_hash:
        print(Fore.RED + "Upload Blocked: Sidecar metadata does not match metadata stored in database for this revision.")
        print(Fore.YELLOW + f"DB metadata hash : {db_metadata_hash}")
        print(Fore.YELLOW + f"Sidecar hash     : {sidecar_metadata_hash}")
        print(Fore.YELLOW + "Regenerate/stage metadata so the sidecar matches the intended build metadata revision.")
        return False

    # -------------------------------------------------------------------
    # 4. Formulate cloud naming paths & hashes
    # -------------------------------------------------------------------
    title_slug = slugify_component(title, "unknown")
    version_slug = slugify_component(version, "unknown")

    print(Fore.CYAN + "Calculating archive SHA-256 for cloud verification...")
    archive_sha256 = sha256_file(file_path)

    ext = os.path.splitext(file_path)[1].lower()
    # Standardized naming for VN archives (title + build version + hash)
    file_name = build_recommended_archive_name(metadata, archive_sha256, ext=ext)
    metadata_file_name = Path(metadata_source).name

    cloud_path = f"archives/{title_slug}/vn-{vn_id:05d}/{version_slug}/{file_name}"
    metadata_cloud_path = f"metadata/{title_slug}/vn-{vn_id:05d}/{version_slug}/{metadata_file_name}"

    if db_version_number > 1:
        parent_metadata_cloud_path = re.sub(r"_meta_v\d+(\.ya?ml)$", f"_meta_v{db_version_number - 1}\\1", metadata_cloud_path)
        with get_connection() as conn:
            parent_uploaded_row = conn.execute(
                "SELECT 1 FROM metadata_file_objects WHERE storage_path = ?",
                (parent_metadata_cloud_path,)
            ).fetchone()
        if not parent_uploaded_row:
            print(Fore.RED + f"Upload Blocked: Parent metadata revision v{db_version_number - 1} is not uploaded yet.")
            print(Fore.YELLOW + f"Expected parent path: {parent_metadata_cloud_path}")
            return False

    print(Fore.GREEN + f"Database verification passed (VN ID: {vn_id}, metadata v{db_version_number})")

    # Ensure queued local file uses the same recommended naming scheme
    current_name = os.path.basename(file_path)
    if current_name != file_name:
        renamed_local_path = os.path.join(os.path.dirname(file_path), file_name)
        if os.path.exists(renamed_local_path):
            os.remove(renamed_local_path)
        os.rename(file_path, renamed_local_path)
        file_path = renamed_local_path
        print(Fore.CYAN + f"Renamed queued archive to: {file_name}")

    file_size = os.path.getsize(file_path)

    # -------------------------------------------------------------------
    # 5. CAS Deduplication Check (archive object only)
    # -------------------------------------------------------------------
    with get_connection() as conn:
        existing_obj = conn.execute(
            "SELECT storage_path FROM archive_objects WHERE sha256 = ?",
            (archive_sha256,)
        ).fetchone()

    archive_needs_upload = existing_obj is None
    if not archive_needs_upload:
        existing_cloud_path = existing_obj["storage_path"]
        print(Fore.GREEN + f"\n[DEDUPLICATION MATCH] Archive already exists in cloud!")
        print(Fore.CYAN + f"Existing Path: {existing_cloud_path}")
        print(Fore.YELLOW + "Skipping archive upload. Linking database records...")

        with get_connection() as conn:
            conn.execute(
                """
                UPDATE archives
                SET status = 'uploaded',
                    uploaded_at = COALESCE(uploaded_at, CURRENT_TIMESTAMP)
                WHERE build_id = ? AND sha256 = ?
                """,
                (build_id, archive_sha256)
            )
            conn.execute("UPDATE builds SET status = ?, archive_object_sha256 = ? WHERE id = ?", ("uploaded", archive_sha256, build_id))
            sync_visual_novel_upload_status(conn, vn_id)

    # -------------------------------------------------------------------
    # 6. Backblaze B2 Authentication via Config
    # -------------------------------------------------------------------
    try:
        key_id, app_key, bucket_name, dry_run = load_b2_config()

        info = InMemoryAccountInfo()
        api = B2Api(info)
        api.authorize_account("production", key_id, app_key)
        bucket = api.get_bucket_by_name(bucket_name)
    except Exception as e:
        print(Fore.RED + f"B2 Authentication failed: {e}")
        return False

    # -------------------------------------------------------------------
    # 7. Upload archive object (if not deduplicated)
    # -------------------------------------------------------------------
    if dry_run:
        if archive_needs_upload:
            print(Fore.YELLOW + f"[DRY RUN] Would upload archive {file_name} to: {cloud_path}")
        else:
            print(Fore.YELLOW + f"[DRY RUN] Archive already deduplicated at: {cloud_path}")
        print(Fore.YELLOW + f"[DRY RUN] Would upload metadata {metadata_file_name} to: {metadata_cloud_path}")
        return True

    if archive_needs_upload:
        archive_sha1 = sha1_file(file_path)
        print(Fore.CYAN + f"\nUploading Archive: {file_name}")
        print(Fore.CYAN + f"Destination      : {cloud_path}")

        with tqdm(total=file_size, unit='B', unit_scale=True, desc="Progress", colour="green") as pbar:
            class TqdmProgressListener:
                def set_total_bytes(self, total_bytes):
                    pass

                def bytes_completed(self, byte_count):
                    pbar.update(byte_count - pbar.n)

                def close(self):
                    pass

            try:
                bucket.upload_local_file(
                    local_file=str(file_path),
                    file_name=cloud_path,
                    file_infos={
                        "src_sha256": archive_sha256,
                        "src_sha1": archive_sha1,
                    },
                    progress_listener=TqdmProgressListener()
                )
            except Exception as e:
                print(Fore.RED + f"\nUpload failed for {file_name}: {e}")
                return False

        print(Fore.GREEN + "\nArchive upload complete!")

        try:
            uploaded_info = bucket.get_file_info_by_name(cloud_path)
        except Exception as e:
            print(Fore.RED + f"Post-upload verification failed for {cloud_path}: {e}")
            return False

        if not verify_remote_upload_integrity(
            remote_info=uploaded_info,
            local_size=file_size,
            local_sha1=archive_sha1,
            local_sha256=archive_sha256,
            label=f"archive {cloud_path}",
        ):
            return False

        print(Fore.GREEN + f"Verified remote archive object: {cloud_path}")

        with get_connection() as conn:
            try:
                conn.execute(
                    '''
                    INSERT OR IGNORE INTO archive_objects (sha256, file_size, storage_path)
                    VALUES (?, ?, ?)
                    ''',
                    (archive_sha256, file_size, cloud_path)
                )

                conn.execute(
                    """
                    UPDATE archives
                    SET status = 'uploaded',
                        uploaded_at = COALESCE(uploaded_at, CURRENT_TIMESTAMP)
                    WHERE build_id = ? AND sha256 = ?
                    """,
                    (build_id, archive_sha256)
                )

                conn.execute("UPDATE builds SET status = ?, archive_object_sha256 = ? WHERE id = ?", ("uploaded", archive_sha256, build_id))
                sync_visual_novel_upload_status(conn, vn_id)
            except Exception as e:
                print(Fore.RED + f"Database update failed after upload verification: {e}")
                return False

    # -------------------------------------------------------------------
    # 8. Upload metadata sidecar object (with CAS dedup + DB record)
    # -------------------------------------------------------------------
    metadata_sha256 = sha256_file(selected_sidecar)
    metadata_local_size = os.path.getsize(selected_sidecar)

    with get_connection() as conn:
        existing_meta_obj = conn.execute(
            "SELECT storage_path FROM metadata_file_objects WHERE sha256 = ?",
            (metadata_sha256,)
        ).fetchone()

    metadata_needs_upload = existing_meta_obj is None
    if not metadata_needs_upload:
        existing_meta_path = existing_meta_obj["storage_path"]
        print(Fore.GREEN + f"\n[DEDUPLICATION MATCH] Metadata sidecar already exists in cloud!")
        print(Fore.CYAN + f"Existing Path: {existing_meta_path}")

    if metadata_needs_upload:
        metadata_sha1 = sha1_file(selected_sidecar)
        print(Fore.CYAN + f"\nUploading Metadata: {metadata_file_name}")
        print(Fore.CYAN + f"Destination       : {metadata_cloud_path}")
        try:
            bucket.upload_local_file(
                local_file=str(selected_sidecar),
                file_name=metadata_cloud_path,
                file_infos={
                    "src_sha256": metadata_sha256,
                    "src_sha1": metadata_sha1,
                },
            )
        except Exception as e:
            print(Fore.RED + f"Upload failed for metadata sidecar {metadata_file_name}: {e}")
            return False

        try:
            metadata_info = bucket.get_file_info_by_name(metadata_cloud_path)
        except Exception as e:
            print(Fore.RED + f"Post-upload verification failed for metadata {metadata_cloud_path}: {e}")
            return False

        if not verify_remote_upload_integrity(
            remote_info=metadata_info,
            local_size=metadata_local_size,
            local_sha1=metadata_sha1,
            local_sha256=metadata_sha256,
            label=f"metadata {metadata_cloud_path}",
        ):
            return False

        print(Fore.GREEN + f"Metadata upload complete: {metadata_cloud_path}")

    with get_connection() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO metadata_file_objects (sha256, file_size, storage_path) VALUES (?, ?, ?)",
            (metadata_sha256, metadata_local_size, metadata_cloud_path)
        )

    return True


def upload_metadata_sidecar(sidecar_path):
    """Upload a metadata sidecar file independently of archive upload."""
    if not os.path.exists(sidecar_path):
        print(Fore.RED + f"Metadata sidecar not found: {sidecar_path}")
        return False

    sidecar_file = Path(sidecar_path)
    if not re.search(r"_meta_v\d+\.ya?ml$", sidecar_file.name):
        print(Fore.RED + "Upload Blocked: Metadata sidecar filename must follow '<archive_name>_meta_vN.yaml'.")
        return False

    try:
        with open(sidecar_file, 'r', encoding='utf-8') as handle:
            metadata = yaml.safe_load(handle)
    except Exception as e:
        print(Fore.RED + f"Upload Blocked: Failed to read metadata sidecar '{sidecar_file.name}': {e}")
        return False

    if not isinstance(metadata, dict):
        print(Fore.RED + "Upload Blocked: Metadata sidecar is not a valid YAML mapping.")
        return False

    metadata = normalize_metadata_fields(metadata)

    revision_match = re.search(r"_meta_v(\d+)\.ya?ml$", sidecar_file.name)
    requested_metadata_revision = int(revision_match.group(1)) if revision_match else None

    title = str(metadata.get("title", "")).strip()
    version = str(metadata.get("version", "")).strip()
    language = normalize_text_list_value(metadata.get("language")) or ""
    build_type = str(metadata.get("build_type", "")).strip()
    edition = str(metadata.get("edition", "")).strip()
    distribution_platform = str(metadata.get("distribution_platform", "")).strip()
    is_artifact_content = str(metadata.get("content_type", "")).strip().lower() == "artifact"

    if not title:
        print(Fore.RED + "Upload Blocked: metadata sidecar is missing 'title'.")
        return False

    vn_id = None
    build_id = None
    with get_connection() as conn:
        vn_row = conn.execute("SELECT id FROM visual_novels WHERE title = ?", (title,)).fetchone()
        if not vn_row:
            print(Fore.RED + f"Upload Blocked: Visual Novel '{title}' does not exist in the database.")
            return False

        vn_id = vn_row[0]

        if version:
            build_row = conn.execute(
                """
                SELECT id, version FROM builds
                WHERE vn_id = ? AND version = ?
                  AND COALESCE(language, '') = COALESCE(?, '')
                  AND COALESCE(build_type, '') = COALESCE(?, '')
                  AND COALESCE(edition, '') = COALESCE(?, '')
                  AND COALESCE(distribution_platform, '') = COALESCE(?, '')
                """,
                (vn_id, version, language, build_type, edition, distribution_platform)
            ).fetchone()
            if not build_row:
                print(Fore.RED + f"Upload Blocked: Version '{version}' for '{title}' does not exist in the database.")
                return False
        else:
            build_row = conn.execute(
                "SELECT id, version FROM builds WHERE vn_id = ? ORDER BY created_at DESC, id DESC LIMIT 1",
                (vn_id,)
            ).fetchone()
            if not build_row:
                print(Fore.RED + f"Upload Blocked: Visual Novel '{title}' has no builds in the database.")
                return False
            version = str(build_row["version"]).strip()
            print(Fore.YELLOW + f"No version supplied in sidecar metadata; using latest DB build version: {version}")

        build_id = build_row["id"]

    with get_connection() as conn:
        if is_artifact_metadata(metadata):
            artifact_id = resolve_artifact_id_for_metadata(conn, build_id, metadata)
            if not artifact_id:
                print(Fore.RED + f"Upload Blocked: Could not resolve artifact row for build {build_id} from sidecar metadata sha256.")
                return False
            if requested_metadata_revision is not None:
                metadata_row = conn.execute(
                    "SELECT metadata_hash, version_number FROM artifact_metadata_versions WHERE artifact_id = ? AND version_number = ?",
                    (artifact_id, requested_metadata_revision)
                ).fetchone()
            else:
                metadata_row = conn.execute(
                    "SELECT metadata_hash, version_number FROM artifact_metadata_versions WHERE artifact_id = ? AND is_current = 1",
                    (artifact_id,)
                ).fetchone()
        else:
            if requested_metadata_revision is not None:
                metadata_row = conn.execute(
                    "SELECT metadata_hash, version_number FROM metadata_versions WHERE build_id = ? AND version_number = ?",
                    (build_id, requested_metadata_revision)
                ).fetchone()
            else:
                metadata_row = conn.execute(
                    "SELECT metadata_hash, version_number FROM metadata_versions WHERE build_id = ? AND is_current = 1",
                    (build_id,)
                ).fetchone()

    if not metadata_row:
        print(Fore.RED + f"Upload Blocked: No matching metadata version found in database for build {build_id}.")
        return False

    db_metadata_hash = metadata_row["metadata_hash"]
    db_version_number = metadata_row["version_number"]
    canonical_metadata_json = json.dumps(
        metadata,
        default=safe_json_serialize,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":")
    )
    sidecar_metadata_hash = hashlib.sha256(canonical_metadata_json.encode("utf-8")).hexdigest()
    if sidecar_metadata_hash != db_metadata_hash:
        print(Fore.RED + "Upload Blocked: Sidecar metadata does not match metadata stored in database for this revision.")
        print(Fore.YELLOW + f"DB metadata hash : {db_metadata_hash}")
        print(Fore.YELLOW + f"Sidecar hash     : {sidecar_metadata_hash}")
        return False

    title_slug = slugify_component(title, "unknown")
    version_slug = slugify_component(version, "unknown")
    metadata_file_name = sidecar_file.name
    metadata_cloud_path = f"metadata/{title_slug}/vn-{vn_id:05d}/{version_slug}/{metadata_file_name}"

    if db_version_number > 1:
        parent_metadata_cloud_path = re.sub(r"_meta_v\d+(\.ya?ml)$", f"_meta_v{db_version_number - 1}\\1", metadata_cloud_path)
        with get_connection() as conn:
            parent_uploaded_row = conn.execute(
                "SELECT 1 FROM metadata_file_objects WHERE storage_path = ?",
                (parent_metadata_cloud_path,)
            ).fetchone()
        if not parent_uploaded_row:
            print(Fore.RED + f"Upload Blocked: Parent metadata revision v{db_version_number - 1} is not uploaded yet.")
            print(Fore.YELLOW + f"Expected parent path: {parent_metadata_cloud_path}")
            return False

    metadata_sha256 = sha256_file(sidecar_file)
    metadata_local_size = os.path.getsize(sidecar_file)

    with get_connection() as conn:
        existing_meta_obj = conn.execute(
            "SELECT storage_path FROM metadata_file_objects WHERE sha256 = ?",
            (metadata_sha256,)
        ).fetchone()

    metadata_needs_upload = existing_meta_obj is None

    try:
        key_id, app_key, bucket_name, dry_run = load_b2_config()
        info = InMemoryAccountInfo()
        api = B2Api(info)
        api.authorize_account("production", key_id, app_key)
        bucket = api.get_bucket_by_name(bucket_name)
    except Exception as e:
        print(Fore.RED + f"B2 Authentication failed: {e}")
        return False

    if dry_run:
        if metadata_needs_upload:
            print(Fore.YELLOW + f"[DRY RUN] Would upload metadata {metadata_file_name} to: {metadata_cloud_path}")
        else:
            print(Fore.YELLOW + f"[DRY RUN] Metadata already deduplicated at: {metadata_cloud_path}")
        return True

    if metadata_needs_upload:
        metadata_sha1 = sha1_file(sidecar_file)
        print(Fore.CYAN + f"\nUploading Metadata: {metadata_file_name}")
        print(Fore.CYAN + f"Destination       : {metadata_cloud_path}")
        try:
            bucket.upload_local_file(
                local_file=str(sidecar_file),
                file_name=metadata_cloud_path,
                file_infos={
                    "src_sha256": metadata_sha256,
                    "src_sha1": metadata_sha1,
                },
            )
        except Exception as e:
            print(Fore.RED + f"Upload failed for metadata sidecar {metadata_file_name}: {e}")
            return False

        try:
            metadata_info = bucket.get_file_info_by_name(metadata_cloud_path)
        except Exception as e:
            print(Fore.RED + f"Post-upload verification failed for metadata {metadata_cloud_path}: {e}")
            return False

        if not verify_remote_upload_integrity(
            remote_info=metadata_info,
            local_size=metadata_local_size,
            local_sha1=metadata_sha1,
            local_sha256=metadata_sha256,
            label=f"metadata {metadata_cloud_path}",
        ):
            return False

    with get_connection() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO metadata_file_objects (sha256, file_size, storage_path) VALUES (?, ?, ?)",
            (metadata_sha256, metadata_local_size, metadata_cloud_path)
        )

    print(Fore.GREEN + f"Metadata upload complete: {metadata_cloud_path}")
    return True


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
