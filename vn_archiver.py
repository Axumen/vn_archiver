#!/usr/bin/env python3

import os
import zipfile
import hashlib
import shutil
import yaml
import json
from datetime import datetime
from pathlib import Path
from b2sdk.v2 import InMemoryAccountInfo, B2Api
from tools.db_manager import get_connection

# ==============================
# SAFETY
# ==============================

DRY_RUN = True  # Set to False when ready to upload for real

# ==============================
# CONFIGURATION
# ==============================

INCOMING_DIR = "incoming"
PROCESSED_DIR = "processed"
METADATA_TEMPLATE = "metadata.yaml"
B2_CONFIG_FILE = "backblaze_config.yaml"

B2_KEY_ID = None
B2_APPLICATION_KEY = None
B2_BUCKET_NAME = None

SUGGESTED_TAGS = [
    "romance", "drama", "comedy", "slice-of-life",
    "mystery", "horror", "sci-fi", "fantasy",
    "school", "adult", "nakige", "utsuge"
]

# ==============================
# UTILITY
# ==============================

def ensure_directories():
    Path(INCOMING_DIR).mkdir(exist_ok=True)
    Path(PROCESSED_DIR).mkdir(exist_ok=True)


def sha256_file(filepath):
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def load_metadata_template():
    with open(METADATA_TEMPLATE, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


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

    return key_id, application_key, bucket_name


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

    print("\nFill Metadata (Press ENTER to leave blank):\n")

    for key in template.keys():
        if key in ["original_filename", "file_size_bytes", "sha256", "archived_at"]:
            continue

        if key == "tags":
            template[key] = prompt_tags()
        else:
            template[key] = prompt_field(key, template.get(key, ""))

    # Automatic fields
    template["original_filename"] = os.path.basename(zip_path)
    template["file_size_bytes"] = os.path.getsize(zip_path)
    template["sha256"] = sha256_file(zip_path)
    template["archived_at"] = datetime.utcnow().isoformat() + "Z"

    return template


def create_archive(original_zip, metadata_dict, output_path):

    # Ensure metadata_version exists
    metadata_dict.setdefault("metadata_version", 1)

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
                metadata.get("sha256"),
                metadata.get("file_size_bytes"),
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
    key_id, application_key, _ = load_b2_config()

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
    DRY_RUN prevents any real upload.
    """

    _, _, bucket_name = load_b2_config()

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
    if DRY_RUN:
        print("\n[DRY RUN ENABLED]")
        print(f"Would upload:")
        print(f"  Local file : {filepath}")
        print(f"  Bucket     : {bucket_name}")
        print(f"  Remote path: {remote_name}")
        print("No upload performed.\n")
        return

    # ---------------------------
    # CONFIRMATION (EXTRA SAFETY)
    # ---------------------------
    confirm = input(f"Upload '{remote_name}' to Backblaze? (yes/no): ").strip().lower()
    if confirm != "yes":
        print("Upload cancelled.")
        return

    # ---------------------------
    # REAL UPLOAD
    # ---------------------------
    b2_api = get_b2_api()
    bucket = b2_api.get_bucket_by_name(bucket_name)

    bucket.upload_local_file(
        local_file=filepath,
        file_name=remote_name
    )

    print(f"Uploaded to Backblaze: {remote_name}")

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
        upload_to_b2(filepath, remote_folder="archives")
        return

    title = metadata.get("title") or "Unknown_Title"
    title_folder = title.strip().replace(" ", "_")

    build_version = metadata.get("version") or "unknown"

    remote_folder = (
        f"archives/"
        f"{title_folder}/"
        f"vn_{vn_id}/"
        f"build_{build_version}"
    )

    upload_to_b2(filepath, remote_folder=remote_folder)
