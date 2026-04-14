#!/usr/bin/env python3

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import yaml
from tqdm import tqdm
from colorama import Fore
from datetime import date, datetime
from pathlib import Path
from db_manager import get_connection
from domain_layer import VisualNovelDomainService
from ingestion_repository import VnIngestionRepository
from metadata_validation import validate_metadata_contract

# ==============================
# CONFIGURATION
# ==============================

INCOMING_DIR = "incoming"
UPLOADING_DIR = "uploading"
VN_ARCHIVE_DIR = "vn archive"
REBUILD_METADATA_DIR = "rebuild_metadata"
METADATA_TEMPLATE_DIR = Path("metadata")
DEFAULT_METADATA_VERSION = 1
SUGGESTED_TAGS = [
    "romance", "drama", "comedy", "slice-of-life",
    "mystery", "horror", "sci-fi", "fantasy",
    "school", "adult", "nakige", "utsuge"
]


METADATA_LIST_FIELDS = {"tags", "target_platform", "aliases", "developer", "publisher"}

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
    "content_type": ["main_story", "story_expansion", "seasonal_event", "april_fools", "side_story", "non_canon_special"],
    "target_platform": ["windows", "linux", "mac", "android", "web", "ios", "switch"],
    "tags": [
        "romance", "drama", "comedy", "slice-of-life", "mystery", "horror", "sci-fi",
        "fantasy", "psychological", "thriller", "action", "historical", "supernatural",
        "nakige", "utsuge", "nukige", "moege", "dark", "wholesome", "tragic", "bittersweet",
        "school", "modern", "adult"
    ],
}

AUTO_METADATA_FIELDS = {
    "original_filename": lambda zip_path: os.path.basename(zip_path),
    "size_bytes": lambda zip_path: os.path.getsize(zip_path),
    "sha256": lambda zip_path: sha256_file(zip_path),
    "archived_at": lambda _: datetime.utcnow().isoformat() + "Z",
}


# ==============================
# UTILITY
# ==============================

def ensure_directories():
    Path(INCOMING_DIR).mkdir(exist_ok=True)
    Path(UPLOADING_DIR).mkdir(exist_ok=True)
    Path(VN_ARCHIVE_DIR).mkdir(exist_ok=True)
    Path(REBUILD_METADATA_DIR).mkdir(exist_ok=True)


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


def _table_exists(conn, table_name):
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _column_exists(conn, table_name, column_name):
    try:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    except Exception:
        return False
    for row in rows:
        normalized = tuple(row)
        if len(normalized) > 1 and normalized[1] == column_name:
            return True
    return False


def get_metadata_template_path(version=DEFAULT_METADATA_VERSION):
    return METADATA_TEMPLATE_DIR / f"metadata_v{version}.yaml"


def get_file_metadata_template_path(version=DEFAULT_METADATA_VERSION):
    return METADATA_TEMPLATE_DIR / f"metadata_file_v{version}.yaml"


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


def load_file_metadata_template(version=None):
    """Load the file-level metadata template (metadata_file_v*.yaml).

    This template defines the fields prompted when attaching a file to an
    existing build, as opposed to the build-level template used for full
    build/VN ingestion.
    """
    if version is None:
        version = detect_latest_metadata_template_version()

    template_path = get_file_metadata_template_path(version)

    if not template_path.exists():
        raise FileNotFoundError(
            f"File metadata template not found for version {version}: {template_path}"
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


def resolve_prompt_fields(template):
    """
    Returns metadata keys that should be prompted from the template format.

    Supported structures:
    1) {required: [...], optional: [...]}  # current template format
    2) {fields: ["a", "b"]}
    3) {fields: {a: ..., b: ...}}
    """

    required_fields, optional_fields = resolve_prompt_field_groups(template)
    return required_fields + optional_fields


def resolve_prompt_field_groups(template):
    required_fields = template.get("required") or []
    optional_fields = template.get("optional") or []

    structured_fields = template.get("fields")
    if isinstance(structured_fields, list):
        optional_fields = [*optional_fields, *structured_fields]
    elif isinstance(structured_fields, dict):
        optional_fields = [*optional_fields, *structured_fields.keys()]

    def deduplicate(fields, seen):
        output = []
        for field in fields:
            if not isinstance(field, str):
                continue
            if field in seen:
                continue
            if field in AUTO_METADATA_FIELDS:
                continue
            seen.add(field)
            output.append(field)
        return output

    seen = set()
    dedup_required = deduplicate(required_fields, seen)
    dedup_optional = deduplicate(optional_fields, seen)
    return dedup_required, dedup_optional


def prompt_field(field_name, current_value):
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
    required_fields, optional_fields = resolve_prompt_field_groups(template)

    metadata = {"metadata_version": metadata_version}

    print("\nFill Metadata\n")
    print("Required fields (for valid builds):")
    for key in required_fields:
        if key == "tags":
            metadata[key] = prompt_tags()
        else:
            metadata[key] = prompt_field(key, "")

    print("\nOptional fields:")
    for key in optional_fields:
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

def get_metadata_value(metadata, key, fallback=None):
    value = metadata.get(key)
    if value is not None:
        return value

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
    "build_type",
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

    """Fetch metadata blob for the highest version build of a VN title, if present."""

    if not title:

        return {}

    normalized_title = str(title).strip()

    if not normalized_title:

        return {}



    with get_connection() as conn:

        rows = conn.execute(

            '''

            SELECT

                r.version AS release_version,

                r.release_id AS release_id,

                rev.revision_id AS revision_id,

                rev.raw_json AS metadata_json

            FROM title t

            JOIN release r ON r.title_id = t.title_id

            JOIN revision rev ON rev.release_id = r.release_id AND rev.is_current = 1

            WHERE TRIM(t.title) = TRIM(?) COLLATE NOCASE

            ''',

            (normalized_title,)

        ).fetchall()



    if not rows:

        return {}



    latest_row = max(

        rows,

        key=lambda row: (

            normalize_version_for_sort(row["release_version"]),

            int(row["release_id"] or 0),

            int(row["revision_id"] or 0),

        )

    )



    if not latest_row['metadata_json']:

        return {}



    try:

        parsed = json.loads(latest_row['metadata_json'])

        return parsed if isinstance(parsed, dict) else {}

    except (json.JSONDecodeError, TypeError):

        return {}





def collect_archives_for_db(metadata):
    archives_to_process = []

    top_level_sha = metadata.get('sha256')

    if top_level_sha:
        archives_to_process.append({
            'sha256': top_level_sha,
            'size_bytes': metadata.get('size_bytes') or None,
            'filename': metadata.get('original_filename'),
            'artifact_type': metadata.get('artifact_type'),
        })

    if 'archives' in metadata and isinstance(metadata['archives'], list):
        for archive in metadata['archives']:
            if isinstance(archive, dict) and archive.get('sha256'):
                archives_to_process.append({
                    'sha256': archive.get('sha256'),
                    'size_bytes': archive.get('size_bytes') or None,
                    'filename': archive.get('filename'),
                    'artifact_type': archive.get('artifact_type'),
                })

    if not top_level_sha and archives_to_process:
        top_level_sha = archives_to_process[0].get('sha256')

    return archives_to_process, top_level_sha



def insert_visual_novel(metadata):

    '''

    Inserts or updates the normalized metadata into the SQLite database.

    '''



    metadata = normalize_metadata_fields(metadata)

    raw_text = metadata.pop("_raw_text", None)

    source_file = metadata.pop("_source_file", None)

    metadata_version = int(metadata.get("metadata_version") or detect_latest_metadata_template_version())

    template = load_metadata_template(metadata_version)

    validate_metadata_contract(metadata, template, CATEGORY_ALL_FIELDS)



    with get_connection() as conn:

        repository = VnIngestionRepository(conn)

        domain_service = VisualNovelDomainService(

            conn,

            repository=repository,

            collect_archives_for_db=collect_archives_for_db,

        )

        ingest_payload = dict(metadata)

        if raw_text is not None:

            ingest_payload["_raw_text"] = raw_text

        if source_file is not None:

            ingest_payload["_source_file"] = source_file



        result = domain_service.ingest(ingest_payload)



        return result





def open_metadata_in_editor_with_defaults(initial_metadata):
    """Open metadata YAML in an editor, then parse and return it."""
    editor_candidates = []
    configured_editor = os.environ.get("VN_ARCHIVER_EDITOR") or os.environ.get("EDITOR")
    if configured_editor:
        editor_candidates.append(configured_editor)
    editor_candidates.extend(["notepad", "nano", "vi"])

    with tempfile.NamedTemporaryFile("w+", suffix=".yaml", delete=False, encoding="utf-8") as tmp:
        temp_path = tmp.name
        yaml.safe_dump(initial_metadata, tmp, sort_keys=False, allow_unicode=True)

    selected_editor = None
    for editor in editor_candidates:
        command_name = editor.split()[0]
        if shutil.which(command_name):
            selected_editor = editor
            break

    if not selected_editor:
        os.remove(temp_path)
        raise RuntimeError("No supported editor found. Install notepad/nano/vi or set VN_ARCHIVER_EDITOR.")

    print(Fore.CYAN + f"Opening metadata in editor: {selected_editor}")
    subprocess.run(f'{selected_editor} "{temp_path}"', shell=True, check=True)

    try:
        with open(temp_path, "r", encoding="utf-8") as f:
            parsed = yaml.safe_load(f) or {}
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)

    if not isinstance(parsed, dict):
        raise ValueError("Edited metadata must be a YAML object.")

    return parsed


def _is_empty_metadata_value(value):
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, list):
        return len(value) == 0
    return False


def create_archive_only(
    archive_paths=None,
    metadata_version=DEFAULT_METADATA_VERSION,
    metadata_input_mode="prompt",
):
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
            "size_bytes": file_size,
            "sha256": sha256
        })

    # -------------------------------------------------------------------
    # 2. Prepare metadata (Prompt or Editor)
    # -------------------------------------------------------------------
    base_template = load_metadata_template(metadata_version)
    required_fields, optional_fields = resolve_prompt_field_groups(base_template)
    prompt_fields = required_fields + optional_fields

    metadata = {"metadata_version": metadata_version}
    defaults = {}

    def normalize_list(val):
        if not val:
            return None
        return sorted(set([v.strip() for v in val.split(",") if v.strip()]))

    if metadata_input_mode == "editor":
        template_defaults = base_template.get("defaults", {}) if isinstance(base_template.get("defaults"), dict) else {}
        metadata_editor_seed = {"metadata_version": metadata_version}

        preselected_title = input(
            Fore.YELLOW + "title (used to preload defaults before editor opens): "
        ).strip()
        if preselected_title:
            defaults = get_latest_metadata_for_title(preselected_title) or {}
            defaults.pop("archives", None)
            defaults.pop("metadata_version", None)
            if defaults:
                print(Fore.GREEN + f"Loaded defaults from latest metadata for '{preselected_title}' before editor launch.")

        for field in prompt_fields:
            default_value = template_defaults.get(field)
            if field in defaults and not _is_empty_metadata_value(defaults.get(field)):
                default_value = defaults.get(field)
            if default_value is None and field in METADATA_LIST_FIELDS:
                default_value = []
            if default_value is None:
                default_value = ""
            if field == "title" and preselected_title:
                default_value = preselected_title
            metadata_editor_seed[field] = default_value

        edited_metadata = None
        while True:
            try:
                edited_metadata = open_metadata_in_editor_with_defaults(metadata_editor_seed)
            except (RuntimeError, ValueError, subprocess.CalledProcessError) as exc:
                print(Fore.RED + f"Editor metadata mode failed: {exc}")
                retry_choice = input(
                    Fore.YELLOW + "Retry opening editor? [y/N]: "
                ).strip().lower()
                if retry_choice in ("y", "yes"):
                    continue
                return

            confirm_choice = input(
                Fore.YELLOW
                + "Use edited metadata? [Y]es / [E]dit again / [C]ancel: "
            ).strip().lower()
            if confirm_choice in ("", "y", "yes"):
                break
            if confirm_choice in ("e", "edit", "edit again"):
                continue
            if confirm_choice in ("c", "cancel", "n", "no"):
                print(Fore.YELLOW + "Metadata editor flow cancelled.")
                return
            print(Fore.YELLOW + "Invalid choice. Re-opening editor.")

        title_value = str(edited_metadata.get("title", "")).strip()
        if title_value:
            defaults = get_latest_metadata_for_title(title_value) or {}
            defaults.pop("archives", None)
            defaults.pop("metadata_version", None)
            if defaults:
                print(Fore.GREEN + f"Loaded defaults from latest metadata for '{title_value}'.")

        for field in prompt_fields:
            default_value = metadata_editor_seed.get(field)
            user_value = edited_metadata.get(field, default_value)
            if _is_empty_metadata_value(user_value):
                user_value = defaults.get(field, default_value)

            if field in METADATA_LIST_FIELDS and isinstance(user_value, str):
                user_value = normalize_list(user_value) or []

            if not _is_empty_metadata_value(user_value):
                metadata[field] = user_value
    else:
        print(Fore.MAGENTA + "\nFill Metadata (Press ENTER to skip optional fields)\n")
        print(Fore.CYAN + "Tip: when a [default] is shown, press ENTER to keep it, or type '-' to clear it.")

        print(Fore.GREEN + "\nRequired fields for a valid build:")
        for field in required_fields:
            default_val = defaults.get(field)
            prompt = f"{field} (required)"
            if default_val not in (None, ""):
                prompt += f" [{default_val}]"
            raw_val = input(Fore.YELLOW + f"{prompt}: ").strip()
            if raw_val:
                metadata[field] = raw_val
            elif default_val not in (None, ""):
                metadata[field] = default_val

        print(Fore.CYAN + "\nOptional fields and suggestions:")
        for field in optional_fields:
            if field in METADATA_LIST_FIELDS:
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
                "size_bytes": a.get("size_bytes"),
                "sha256": a.get("sha256")
            })
        metadata["archives"] = archives_list

    finalize_archive_creation(metadata, archives_data)


def finalize_archive_creation(metadata, archives_data):
    """Shared finalization flow for prompted and pre-filled metadata runs."""
    result = insert_visual_novel(metadata)
    if not result:
        print(Fore.RED + "Failed to insert visual novel into database.")
        return


def create_archive_from_metadata_file(archive_paths, metadata, raw_text=None, source_file=None):
    """Create archive pipeline from existing metadata.yaml without prompts."""
    archives_data = []
    for path in archive_paths:
        print(f"Calculating SHA-256 for: {os.path.basename(path)}...")
        sha256 = sha256_file(path)
        file_size = os.path.getsize(path)
        archives_data.append({
            "original_path": path,
            "filename": os.path.basename(path),
            "size_bytes": file_size,
            "sha256": sha256
        })

    prepared = dict(metadata or {})
    prepared.setdefault("metadata_version", detect_latest_metadata_template_version())
    if raw_text is not None:
        prepared["_raw_text"] = raw_text
    if source_file is not None:
        prepared["_source_file"] = source_file
    if archives_data:
        prepared["archives"] = [
            {
                "filename": a["filename"],
                "size_bytes": a["size_bytes"],
                "sha256": a["sha256"]
            }
            for a in archives_data
        ]

    finalize_archive_creation(prepared, archives_data)


def format_uploaded_component(value, fallback):
    text = str(value or "").replace("_", " ").strip()
    text = " ".join(text.split())
    return text or fallback




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
    metadata_for_staging = dict(metadata or {})
    metadata_for_staging.pop("_raw_text", None)
    metadata_for_staging.pop("_source_file", None)

    meta_sha = metadata_for_staging.get('sha256')
    if not meta_sha and isinstance(metadata_for_staging.get('archives'), list) and metadata_for_staging['archives']:
        first_arch = metadata_for_staging['archives'][0]
        if isinstance(first_arch, dict):
            meta_sha = first_arch.get('sha256')

    final_name = build_recommended_metadata_name(metadata_for_staging, meta_sha, metadata_version_number)

    if target_dir is None:
        target_dir = get_uploading_latest_dir(metadata)
    target_dir = Path(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    temp_meta_path = target_dir / 'metadata.yaml'
    ordered_metadata = order_metadata_for_yaml(metadata_for_staging)
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


def mirror_metadata_for_rebuild(staged_meta_path, archives_data, release_id):
    """Mirror staged sidecar metadata into rebuild_metadata/ with archive-id-prefixed names."""
    metadata_dir = Path(REBUILD_METADATA_DIR)
    metadata_dir.mkdir(parents=True, exist_ok=True)

    if not release_id:
        print(Fore.YELLOW + "[WARN] Rebuild metadata mirror skipped: missing release ID.")
        return []

    archive_id_by_sha = {}
    with get_connection() as conn:
        if _table_exists(conn, "file") and _table_exists(conn, "release_file"):
            rows = conn.execute(
                """
                SELECT f.file_id AS id, f.sha256 AS sha256
                FROM release_file rf
                JOIN file f ON f.file_id = rf.file_id
                WHERE rf.release_id = ?
                """,
                (release_id,),
            ).fetchall()
            for row in rows:
                archive_id_by_sha[str(row["sha256"]).strip().lower()] = int(row["id"])
        else:
            print(Fore.YELLOW + "[WARN] Rebuild metadata mirror skipped: missing file/release_file tables.")
            return []

    staged_name = Path(staged_meta_path).name
    mirrored_paths = []
    for archive in archives_data or []:
        archive_sha = str(archive.get("sha256") or "").strip().lower()
        archive_id = archive_id_by_sha.get(archive_sha)
        if not archive_id:
            print(Fore.YELLOW + f"[WARN] Could not resolve archive ID for metadata mirror ({archive_sha[:8]}...).")
            continue

        mirrored_path = metadata_dir / f"{archive_id}_{staged_name}"
        shutil.copy2(staged_meta_path, mirrored_path)
        mirrored_paths.append(mirrored_path)

    if mirrored_paths:
        print(Fore.GREEN + f"Mirrored metadata copies for rebuild: {len(mirrored_paths)} file(s).")
    return mirrored_paths


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
