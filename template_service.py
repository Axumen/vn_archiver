import os
from datetime import datetime, timezone
from pathlib import Path

import yaml

from utils import sha256_file

METADATA_TEMPLATE_DIR = Path("metadata")
DEFAULT_METADATA_VERSION = 1

AUTO_METADATA_FIELDS = {
    "original_filename": lambda zip_path: os.path.basename(zip_path),
    "size_bytes": lambda zip_path: os.path.getsize(zip_path),
    "sha256": lambda zip_path: sha256_file(zip_path),
    "archived_at": lambda _: datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
}


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


def order_metadata_for_yaml(metadata):
    if not isinstance(metadata, dict):
        return metadata

    try:
        template_version = int(metadata.get("metadata_version") or DEFAULT_METADATA_VERSION)
    except (ValueError, TypeError):
        template_version = DEFAULT_METADATA_VERSION

    try:
        template = load_metadata_template(template_version)
    except FileNotFoundError:
        return dict(metadata)

    if not isinstance(template, dict):
        return dict(metadata)

    ordered = {}

    template_field_order = ["metadata_version"]

    required_fields = template.get("required")
    if isinstance(required_fields, list):
        template_field_order.extend(
            field for field in required_fields if isinstance(field, str)
        )

    optional_fields = template.get("optional")
    if isinstance(optional_fields, list):
        template_field_order.extend(
            field for field in optional_fields if isinstance(field, str)
        )

    if "archives" in template and "archives" not in template_field_order:
        template_field_order.append("archives")

    for key in template_field_order:
        if key == "archives":
            continue
        if key in metadata:
            ordered[key] = metadata[key]

    for key, value in metadata.items():
        if key not in ordered and key != "archives":
            ordered[key] = value

    if "archives" in metadata:
        ordered["archives"] = metadata["archives"]

    return ordered
