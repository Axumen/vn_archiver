#!/usr/bin/env python3

import os
import zipfile
import hashlib
import shutil
import yaml
from datetime import datetime
from pathlib import Path
from b2sdk.v2 import InMemoryAccountInfo, B2Api

# ==============================
# CONFIGURATION
# ==============================

INCOMING_DIR = "incoming"
PROCESSED_DIR = "processed"
METADATA_TEMPLATE = "metadata.yaml"

B2_KEY_ID = "YOUR_KEY_ID"
B2_APPLICATION_KEY = "YOUR_APPLICATION_KEY"
B2_BUCKET_NAME = "YOUR_BUCKET_NAME"

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
    temp_metadata_path = "metadata.yml"

    with open(temp_metadata_path, "w", encoding="utf-8") as f:
        yaml.dump(metadata_dict, f, sort_keys=False, allow_unicode=True)

    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.write(original_zip, arcname=os.path.basename(original_zip))
        archive.write(temp_metadata_path, arcname="metadata.yml")

    os.remove(temp_metadata_path)


# ==============================
# BACKBLAZE
# ==============================

def get_b2_api():
    info = InMemoryAccountInfo()
    b2_api = B2Api(info)
    b2_api.authorize_account(
        "production",
        B2_KEY_ID,
        B2_APPLICATION_KEY
    )
    return b2_api


def upload_to_b2(filepath):
    b2_api = get_b2_api()
    bucket = b2_api.get_bucket_by_name(B2_BUCKET_NAME)
    filename = os.path.basename(filepath)

    bucket.upload_local_file(
        local_file=filepath,
        file_name=filename
    )

    print(f"Uploaded to Backblaze: {filename}")


# ==============================
# ARCHIVE CREATION ONLY
# ==============================

def create_archive_only(filename, metadata):
    ensure_directories()

    full_path = os.path.join(INCOMING_DIR, filename)

    if not os.path.exists(full_path):
        raise Exception("File not found.")

    # Auto fields
    metadata["original_filename"] = os.path.basename(full_path)
    metadata["file_size_bytes"] = os.path.getsize(full_path)
    metadata["sha256"] = sha256_file(full_path)
    metadata["archived_at"] = datetime.utcnow().isoformat() + "Z"

    final_name = filename.replace(".zip", "_archive.zip")
    final_path = os.path.join(PROCESSED_DIR, final_name)

    create_archive(full_path, metadata, final_path)

    shutil.move(full_path, os.path.join(PROCESSED_DIR, filename))

    return final_path


# ==============================
# UPLOAD SEPARATE
# ==============================

def upload_archive(filepath):
    if not os.path.exists(filepath):
        raise Exception("Archive not found.")

    upload_to_b2(filepath)