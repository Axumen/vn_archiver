#!/usr/bin/env python3

import json
import os
import yaml
from colorama import Fore
from datetime import datetime, timezone
from pathlib import Path
from db_manager import get_connection
from domain_layer import VisualNovelDomainService
from ingestion_repository import VnIngestionRepository
from metadata_validation import validate_metadata_contract
from utils import (
    sha256_file,
    normalize_version_for_sort,
    normalize_metadata_fields,
    CATEGORY_ALL_FIELDS,
)
from staging import (
    stage_metadata_yaml_for_upload as _stage_metadata_yaml,
    stage_ingested_files_for_upload as _stage_ingested_files,
)

# ==============================
# CONFIGURATION
# ==============================

# Path constants are now defined in staging.py and re-imported above.
METADATA_TEMPLATE_DIR = Path("metadata")
DEFAULT_METADATA_VERSION = 1

AUTO_METADATA_FIELDS = {
    "original_filename": lambda zip_path: os.path.basename(zip_path),
    "size_bytes": lambda zip_path: os.path.getsize(zip_path),
    "sha256": lambda zip_path: sha256_file(zip_path),  # from utils
    "archived_at": lambda _: datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
}


# ==============================
# TEMPLATE RESOLUTION
# ==============================

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


# ==============================
# DATABASE
# ==============================

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


def stage_ingested_files_for_upload(metadata, archives_data, metadata_version_number=None, release_id=None):
    """Convenience wrapper that passes :func:`order_metadata_for_yaml` to staging."""
    return _stage_ingested_files(
        metadata, archives_data, metadata_version_number,
        release_id=release_id,
        order_fn=order_metadata_for_yaml,
    )


def stage_metadata_yaml_for_upload(metadata, metadata_version_number, sha256=None, release_id=None, target_dir=None):
    """Convenience wrapper that passes :func:`order_metadata_for_yaml` to staging."""
    return _stage_metadata_yaml(
        metadata, metadata_version_number, sha256=sha256, release_id=release_id, target_dir=target_dir,
        order_fn=order_metadata_for_yaml,
    )


def finalize_archive_creation(metadata, archives_data):
    """Shared finalization flow for prompted and pre-filled metadata runs.
    
    Raises
    ------
    ValueError
        If no archive files are provided. A release must have at least one
        primary file attached — this is a file-exists-basis archiving system.
    """
    if not archives_data:
        raise ValueError(
            "A release requires at least one archive file. "
            "Please provide a primary file before creating a release."
        )
    result = insert_visual_novel(metadata)
    if not result:
        print(Fore.RED + "Failed to insert visual novel into database.")
        return

    staged_paths, staged_meta_path = stage_ingested_files_for_upload(
        metadata,
        archives_data,
        result.metadata_version_number,
    )
    if staged_paths:
        for staged_path in staged_paths:
            print(Fore.GREEN + f"Staged archive for upload: {staged_path}")
    if staged_meta_path:
        print(Fore.GREEN + f"Staged metadata sidecar: {staged_meta_path}")


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
        if key == 'archives':
            continue
        if key in metadata:
            ordered[key] = metadata[key]

    for key, value in metadata.items():
        if key not in ordered and key != 'archives':
            ordered[key] = value

    if 'archives' in metadata:
        ordered['archives'] = metadata['archives']

    return ordered
