#!/usr/bin/env python3

import os
import zipfile
import hashlib
import shutil
import sys
import yaml
import json
from datetime import datetime
from pathlib import Path
from b2sdk.v2 import InMemoryAccountInfo, B2Api
from tools.db_manager import get_connection

# ==============================
# CONFIGURATION
# ==============================

INCOMING_DIR = "incoming"
PROCESSED_DIR = "processed"
UPLOADED_DIR = "uploaded"
METADATA_TEMPLATE_DIR = Path("metadata_templates")
DEFAULT_METADATA_VERSION = 1
B2_CONFIG_FILE = "backblaze_config.yml"

B2_KEY_ID = None
B2_APPLICATION_KEY = None
B2_BUCKET_NAME = None

SUGGESTED_TAGS = [
    "romance", "drama", "comedy", "slice-of-life",
    "mystery", "horror", "sci-fi", "fantasy",
    "school", "adult", "nakige", "utsuge"
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
    Path(PROCESSED_DIR).mkdir(exist_ok=True)
    Path(UPLOADED_DIR).mkdir(exist_ok=True)


def sha256_file(filepath):
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def get_metadata_template_path(version=DEFAULT_METADATA_VERSION):
    return METADATA_TEMPLATE_DIR / f"metadata_v{version}.yml"


def load_metadata_template(version=DEFAULT_METADATA_VERSION):
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
    metadata_version = template.get("metadata_version", DEFAULT_METADATA_VERSION)
    prompt_fields = resolve_prompt_fields(template)

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


def create_archive(original_zip, metadata_dict, output_path):

    # Ensure metadata_version exists
    metadata_dict.setdefault("metadata_version", DEFAULT_METADATA_VERSION)

    temp_metadata_path = "metadata.yml"

    with open(temp_metadata_path, "w", encoding="utf-8") as f:
        yaml.dump(metadata_dict, f, sort_keys=False, allow_unicode=True)

    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_STORED) as archive:
        archive.write(original_zip, arcname=os.path.basename(original_zip))
        archive.write(temp_metadata_path, arcname="metadata.yml")

    os.remove(temp_metadata_path)

# ==============================
# DATABASE
# ==============================

def sha_exists(sha256):
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id FROM visual_novels WHERE sha256 = ?",
            (sha256,)
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


def insert_visual_novel(metadata, archive_path):

    with get_connection() as conn:

        metadata_json = json.dumps(metadata, ensure_ascii=False)

        cursor = conn.execute(
            """
            INSERT INTO visual_novels
            (title, developer, engine, language, release_date,
             version, sha256, file_size, archive_path, status,
             metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                metadata.get("title"),
                metadata.get("developer"),
                metadata.get("engine"),
                metadata.get("language"),
                metadata.get("release_date"),
                metadata.get("version"),
                get_metadata_value(metadata, "sha256", get_metadata_value(metadata, "archive.sha256")),
                get_metadata_value(metadata, "file_size_bytes", get_metadata_value(metadata, "archive.file_size")),
                archive_path,
                "archived",
                metadata_json
            )
        )

        # Format vn_id with leading zeros (e.g., 000123)
        vn_id = f"{cursor.lastrowid:06d}"

        # ---- Normalize Tags ----
        tags = metadata.get("tags") or []

        if not isinstance(tags, list):
            tags = [tags]

        for tag in tags:
            if not tag:
                continue

            conn.execute(
                "INSERT OR IGNORE INTO tags (name) VALUES (?)",
                (tag,)
            )

            tag_row = conn.execute(
                "SELECT id FROM tags WHERE name = ?",
                (tag,)
            ).fetchone()

            if tag_row:
                conn.execute(
                    "INSERT OR IGNORE INTO vn_tags (vn_id, tag_id) VALUES (?, ?)",
                    (vn_id, tag_row["id"])
                )

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
        """Simple console progress display for Backblaze uploads."""

        def __init__(self, total_bytes):
            self.total_bytes = total_bytes
            self.last_percent = -1

        def set_total_bytes(self, total_bytes):
            self.total_bytes = total_bytes

        def bytes_completed(self, byte_count):
            if self.total_bytes <= 0:
                return

            percent = int((byte_count / self.total_bytes) * 100)
            if percent == self.last_percent:
                return

            self.last_percent = percent
            bar_length = 30
            filled = int((byte_count / self.total_bytes) * bar_length)
            bar = "#" * filled + "-" * (bar_length - filled)
            sys.stdout.write(f"\rUploading: [{bar}] {percent:3d}%")
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


def move_uploaded_archive(filepath, metadata):
    """Move uploaded archive out of processed into uploaded/<title>/<version>/."""
    if not os.path.exists(filepath):
        raise Exception("Archive not found for post-upload move.")

    def sanitize(value):
        return str(value).strip().replace(" ", "_")

    title = sanitize(metadata.get("title") or "Unknown_Title")
    build_version = sanitize(metadata.get("version") or "unknown")

    target_dir = Path(UPLOADED_DIR) / title / build_version
    target_dir.mkdir(parents=True, exist_ok=True)

    destination = target_dir / Path(filepath).name

    if destination.exists():
        raise Exception(f"Destination already exists: {destination}")

    shutil.move(filepath, destination)

    return str(destination)

# ==============================
# ARCHIVE CREATION ONLY
# ==============================

def create_archive_only(filename, metadata):
    ensure_directories()

    full_path = os.path.join(INCOMING_DIR, filename)

    if not os.path.exists(full_path):
        raise Exception("File not found.")

    metadata["original_filename"] = os.path.basename(full_path)
    metadata["file_size_bytes"] = os.path.getsize(full_path)
    metadata["sha256"] = sha256_file(full_path)
    metadata["archived_at"] = datetime.utcnow().isoformat() + "Z"
    set_nested_value(metadata, "archive.filename", metadata["original_filename"])
    set_nested_value(metadata, "archive.sha256", metadata["sha256"])
    set_nested_value(metadata, "archive.file_size", metadata["file_size_bytes"])

    # Prevent duplicate ingestion
    if sha_exists(metadata["sha256"]):
        raise Exception("Archive already exists in database (duplicate SHA256).")

    # ---- Step 1: Temporary archive (before vn_id exists) ----
    temp_name = filename.replace(".zip", "_archive_temp.zip")
    temp_path = os.path.join(PROCESSED_DIR, temp_name)

    create_archive(full_path, metadata, temp_path)

    # ---- Step 2: Insert into DB to get vn_id ----
    vn_id = insert_visual_novel(metadata, temp_path)

    # ---- Step 3: Build structured final name ----
    def sanitize(value):
        return str(value).strip().replace(" ", "_")

    title = sanitize(metadata.get("title") or "Unknown_Title")
    build_version = sanitize(metadata.get("version") or "unknown")

    final_name = f"{title}_build_{build_version}.zip"
    final_path = os.path.join(PROCESSED_DIR, final_name)

    # ---- Step 4: Rename archive ----
    if os.path.exists(final_path):
        raise Exception("Archive with same title and build already exists.")

    os.rename(temp_path, final_path)

    # ---- Step 5: Update DB archive_path ----
    with get_connection() as conn:
        conn.execute(
            "UPDATE visual_novels SET archive_path = ? WHERE id = ?",
            (final_path, int(vn_id))
        )

    # ---- Step 6: Move original ZIP into processed ----
    shutil.move(full_path, os.path.join(PROCESSED_DIR, filename))

    return final_path

# ==============================
# STRUCTURED ARCHIVE UPLOAD
# ==============================

def upload_archive(filepath, metadata=None, vn_id=None):
    """
    Structured upload using:
    archives/<Title>/vn_<id>/build_<version>/

    Backward compatible:
    If metadata or vn_id is missing, falls back to simple archives/ upload.
    """

    if not os.path.exists(filepath):
        raise Exception("Archive not found.")

    # ---------------------------
    # Fallback mode (old behavior)
    # ---------------------------
    if metadata is None or vn_id is None:
        return upload_to_b2(filepath, remote_folder="archives")

    title = metadata.get("title") or "Unknown_Title"
    title_folder = title.strip().replace(" ", "_")

    build_version = metadata.get("version") or "unknown"

    remote_folder = (
        f"archives/"
        f"{title_folder}/"
        f"vn_{vn_id}/"
        f"build_{build_version}"
    )

    return upload_to_b2(filepath, remote_folder=remote_folder)
