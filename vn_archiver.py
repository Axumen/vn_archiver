#!/usr/bin/env python3

import os
import zipfile
import hashlib
import shutil
import sys
import yaml
import json
import tempfile
import time
import json
import hashlib
from datetime import datetime
from pathlib import Path
from b2sdk.v2 import InMemoryAccountInfo, B2Api
from db_manager import get_connection, exclusive_transaction

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

SUGGESTED_CONTENT_TYPE = [
    "main_story", "story_expansion", "seasonal_event",
    "april_fools", "side_story", "non_canon_special"
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


def get_available_metadata_template_versions():
    if not METADATA_TEMPLATE_DIR.exists():
        return []

    versions = []
    for template_path in METADATA_TEMPLATE_DIR.glob("metadata_v*.yml"):
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
    if field_name == "content_type":
        print("\nSuggested content_type:")
        print(", ".join(SUGGESTED_CONTENT_TYPE))

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
    metadata_dict.setdefault(
        "metadata_version",
        detect_latest_metadata_template_version()
    )

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


def insert_visual_novel(metadata):
    conn = get_connection()
    try:
        # Wrap everything in an exclusive transaction to prevent race conditions
        with exclusive_transaction(conn):

            title = metadata.get("title")
            if not title:
                raise Exception("Title is required.")

            # -------------------------------------------------
            # 1️⃣ SERIES NORMALIZATION
            # -------------------------------------------------
            series_name = metadata.get("series")
            series_id = None

            if series_name:
                conn.execute("INSERT OR IGNORE INTO series (name) VALUES (?)", (series_name,))
                series_row = conn.execute("SELECT id FROM series WHERE name = ?", (series_name,)).fetchone()
                if series_row:
                    series_id = series_row["id"]

            # -------------------------------------------------
            # 2️⃣ INSERT OR FETCH VISUAL NOVEL
            # -------------------------------------------------
            existing_row = conn.execute("SELECT id FROM visual_novels WHERE title = ?", (title,)).fetchone()
            is_new_work = False

            # Format aliases as JSON array
            aliases_raw = metadata.get("aliases", [])
            if isinstance(aliases_raw, str):
                aliases_raw = [a.strip() for a in aliases_raw.split(",") if a.strip()]
            aliases_json = json.dumps(aliases_raw, ensure_ascii=False) if aliases_raw else None

            if existing_row:
                vn_id = existing_row["id"]
                # UPDATE existing VN details with any edits
                conn.execute(
                    """
                    UPDATE visual_novels 
                    SET aliases = ?, developer = ?, publisher = ?, release_status = ?, content_rating = ?
                    WHERE id = ?
                    """,
                    (aliases_json, metadata.get("developer"), metadata.get("publisher"),
                     metadata.get("release_status"), metadata.get("content_rating"), vn_id)
                )
            else:
                is_new_work = True

                cursor = conn.execute(
                    """
                    INSERT INTO visual_novels
                    (series_id, title, aliases, developer, publisher, release_status, content_rating)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        series_id,
                        title,
                        aliases_json,
                        metadata.get("developer"),
                        metadata.get("publisher"),  # NEW FIELD
                        metadata.get("release_status"),
                        metadata.get("content_rating"),
                    )
                )
                vn_id = cursor.lastrowid

            # -------------------------------------------------
            # 3️⃣ INSERT OR FETCH BUILD (FIXED BUG)
            # -------------------------------------------------
            build_version = metadata.get("version")
            existing_build = conn.execute(
                "SELECT id FROM builds WHERE vn_id = ? AND version = ?",
                (vn_id, build_version)
            ).fetchone()

            if existing_build:
                build_id = existing_build["id"]
                # UPDATE existing build details with any edits
                conn.execute(
                    """
                    UPDATE builds
                    SET build_type = ?, distribution_model = ?, distribution_platform = ?,
                        language = ?, translator = ?, edition = ?, release_date = ?,
                        engine = ?, engine_version = ?, base_archive_sha256 = ?
                    WHERE id = ?
                    """,
                    (
                        metadata.get("build_type"), metadata.get("distribution_model"),
                        metadata.get("distribution_platform"), metadata.get("language"),
                        metadata.get("translator"), metadata.get("edition"),
                        metadata.get("release_date"), metadata.get("engine"),
                        metadata.get("engine_version"), metadata.get("base_archive_sha256"),
                        build_id
                    )
                )
            else:
                build_cursor = conn.execute(
                    """
                    INSERT INTO builds
                    (vn_id, version, build_type, distribution_model, distribution_platform,
                     language, translator, edition, release_date, engine, engine_version, base_archive_sha256)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        vn_id,
                        build_version,
                        metadata.get("build_type"),
                        metadata.get("distribution_model"),
                        metadata.get("distribution_platform"),
                        metadata.get("language"),
                        metadata.get("translator"),
                        metadata.get("edition"),
                        metadata.get("release_date"),
                        metadata.get("engine"),
                        metadata.get("engine_version"),
                        metadata.get("base_archive_sha256"),
                    )
                )
                build_id = build_cursor.lastrowid

            # -------------------------------------------------
            # 4️⃣ PLATFORM NORMALIZATION
            # -------------------------------------------------
            raw_platforms = metadata.get("target_platform", [])
            if isinstance(raw_platforms, str):
                raw_platforms = [p.strip() for p in raw_platforms.split(",") if p.strip()]
            elif not isinstance(raw_platforms, list):
                raw_platforms = [raw_platforms]

            for platform in raw_platforms:
                if not platform: continue
                normalized = str(platform).strip().lower()
                if not normalized: continue
                conn.execute("INSERT OR IGNORE INTO target_platforms (name) VALUES (?)", (normalized,))
                platform_row = conn.execute("SELECT id FROM target_platforms WHERE name = ?", (normalized,)).fetchone()
                if platform_row:
                    conn.execute("INSERT OR IGNORE INTO build_target_platforms (build_id, platform_id) VALUES (?, ?)",
                                 (build_id, platform_row["id"]))

                    # -------------------------------------------------
                    # 5️⃣ INSERT ARCHIVES
                    # -------------------------------------------------
                    archives_to_process = []

                    # 1. Check for a single top-level archive definition
                    top_level_sha = get_metadata_value(metadata, "sha256",
                                                       get_metadata_value(metadata, "archive.sha256"))
                    if top_level_sha:
                        archives_to_process.append({
                            "sha256": top_level_sha,
                            "file_size_bytes": get_metadata_value(metadata, "file_size_bytes",
                                                                  get_metadata_value(metadata,
                                                                                     "archive.file_size") or 0)
                        })

                    # 2. Check for multi-archive list (from the YAML template)
                    multi_archives = metadata.get("archives", [])
                    if isinstance(multi_archives, list):
                        for arch in multi_archives:
                            # Make sure the array item is actually a dictionary and has a SHA
                            if isinstance(arch, dict) and arch.get("sha256"):
                                archives_to_process.append({
                                    "sha256": arch.get("sha256"),
                                    "file_size_bytes": arch.get("file_size_bytes", 0)
                                })

                    # 3. Insert all gathered archives safely
                    for arch_data in archives_to_process:
                        sha256 = arch_data["sha256"]
                        file_size = arch_data["file_size_bytes"]

                        archive_exists = conn.execute(
                            "SELECT id FROM archives WHERE build_id = ? AND sha256 = ?",
                            (build_id, sha256)
                        ).fetchone()

                        if not archive_exists:
                            conn.execute(
                                """
                                INSERT INTO archives
                                (build_id, sha256, file_size_bytes, metadata_json, metadata_version)
                                VALUES (?, ?, ?, ?, ?)
                                """,
                                (
                                    build_id,
                                    sha256,
                                    file_size,
                                    json.dumps(metadata, ensure_ascii=False),
                                    metadata.get("metadata_version", 1),
                                )
                            )

            # -------------------------------------------------
            # 6️⃣ TAG NORMALIZATION
            # -------------------------------------------------
            tags = metadata.get("tags") or []
            if isinstance(tags, str):
                tags = [t.strip() for t in tags.split(",") if t.strip()]
            elif not isinstance(tags, list):
                tags = [tags]

            for tag in tags:
                if not tag: continue
                conn.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (tag,))
                tag_row = conn.execute("SELECT id FROM tags WHERE name = ?", (tag,)).fetchone()
                if tag_row:
                    conn.execute("INSERT OR IGNORE INTO vn_tags (vn_id, tag_id) VALUES (?, ?)", (vn_id, tag_row["id"]))

            # -------------------------------------------------
            # 7️⃣ CANON RELATIONSHIPS
            # -------------------------------------------------
            if is_new_work:
                parent_title = metadata.get("parent_vn_title")
                relationship_type = metadata.get("relationship_type")
                if parent_title and relationship_type:
                    parent_row = conn.execute("SELECT id FROM visual_novels WHERE title = ?",
                                              (parent_title,)).fetchone()
                    if parent_row:
                        conn.execute(
                            "INSERT OR IGNORE INTO canon_relationships (parent_vn_id, child_vn_id, relationship_type) VALUES (?, ?, ?)",
                            (parent_row["id"], vn_id, relationship_type))

            # -------------------------------------------------
            # 8️⃣ METADATA VERSIONING (NEW)
            # -------------------------------------------------
            # Generate a consistent string to hash
            canonical_json = json.dumps(metadata, sort_keys=True, ensure_ascii=False)
            metadata_hash = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
            schema_version = metadata.get("metadata_version", 1)

            # Insert immutable blob into metadata_objects
            conn.execute(
                """
                INSERT OR IGNORE INTO metadata_objects (hash, schema_version, metadata_json)
                VALUES (?, ?, ?)
                """,
                (metadata_hash, schema_version, canonical_json)
            )

            # Check if this exact metadata hash is already the current version
            current_ver = conn.execute(
                "SELECT metadata_hash FROM metadata_versions WHERE vn_id = ? AND is_current = 1",
                (vn_id,)
            ).fetchone()

            if not current_ver or current_ver["metadata_hash"] != metadata_hash:
                # Turn off the old active version
                conn.execute("UPDATE metadata_versions SET is_current = 0 WHERE vn_id = ?", (vn_id,))

                # Fetch the next sequential version number
                cursor = conn.execute("SELECT MAX(version_number) FROM metadata_versions WHERE vn_id = ?", (vn_id,))
                current_max = cursor.fetchone()[0]
                next_version = (current_max or 0) + 1

                # Insert the new version history
                conn.execute(
                    """
                    INSERT INTO metadata_versions (vn_id, metadata_hash, version_number, is_current)
                    VALUES (?, ?, ?, 1)
                    """,
                    (vn_id, metadata_hash, next_version)
                )

        return vn_id
    finally:
        conn.close()

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
                f"{byte_count/1024/1024:8.2f}MB / {self.total_bytes/1024/1024:8.2f}MB "
                f"{speed/1024/1024:6.2f} MB/s"
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


def move_uploaded_archive(filepath, metadata):
    if not os.path.exists(filepath):
        raise Exception("Archive not found for post-upload move.")

    target_dir = get_uploaded_latest_dir(metadata)
    ensure_clean_directory(target_dir)

    return move_file_to_uploaded_dir(filepath, target_dir)

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

    temp_name = filename.replace(".zip", "_archive_temp.zip")
    temp_path = os.path.join(PROCESSED_DIR, temp_name)

    create_archive(full_path, metadata, temp_path)

    # Insert into DB
    vn_id = insert_visual_novel(metadata)

    # Slug-safe naming
    # ==============================
    # Structured filename naming
    # ==============================

    title_slug = slugify_component(metadata.get("title"), "unknown")
    build_slug = slugify_component(metadata.get("version"), "unknown")
    sha8 = str(metadata.get("sha256", ""))[:8] or "unknown"

    final_name = (
        f"{title_slug}_"
        f"{build_slug}_"
        f"{sha8}.archive.zip"
    )

    final_path = os.path.join(PROCESSED_DIR, final_name)

    if os.path.exists(final_path):
        raise Exception("Archive with same title and build already exists.")

    os.rename(temp_path, final_path)

    original_processed_path = os.path.join(PROCESSED_DIR, filename)
    shutil.move(full_path, original_processed_path)

    # NOW RETURNS vn_id (required by tui.py)
    return final_path, original_processed_path, vn_id
    
def move_original_to_uploaded_local(original_filepath, metadata):
    """Move original zip to uploaded/<title>/Latest Version/ using local naming only."""
    if not os.path.exists(original_filepath):
        raise Exception("Original file not found for local move.")

    target_dir = get_uploaded_latest_dir(metadata)
    ensure_clean_directory(target_dir)

    title = format_uploaded_component(metadata.get("title"), "Unknown Title")
    build_version = format_uploaded_component(metadata.get("version"), "unknown")
    cleaned_name = f"{title} {build_version}.zip"

    return move_file_to_uploaded_dir(original_filepath, target_dir, cleaned_name)


def move_processed_metadata_to_uploaded(metadata_filepath, metadata):
    """Move processed metadata YAML to uploaded/<title>/Latest Version/."""
    if not os.path.exists(metadata_filepath):
        raise Exception("Metadata file not found for post-upload move.")

    target_dir = get_uploaded_latest_dir(metadata)
    return move_file_to_uploaded_dir(metadata_filepath, target_dir)


def format_uploaded_component(value, fallback):
    text = str(value or "").replace("_", " ").strip()
    text = " ".join(text.split())
    return text or fallback


def get_uploaded_latest_dir(metadata):
    title = format_uploaded_component(metadata.get("title"), "Unknown Title")
    return Path(UPLOADED_DIR) / title / "Latest Version"


def ensure_clean_directory(target_dir):
    target_dir.mkdir(parents=True, exist_ok=True)
    for entry in target_dir.iterdir():
        if entry.is_dir():
            shutil.rmtree(entry)
        else:
            entry.unlink(missing_ok=True)


def move_file_to_uploaded_dir(source_filepath, target_dir, destination_name=None):
    target_dir.mkdir(parents=True, exist_ok=True)
    resolved_name = destination_name or Path(source_filepath).name
    destination = target_dir / resolved_name

    if destination.exists():
        destination.unlink()

    shutil.move(source_filepath, destination)
    return str(destination)

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

    title_folder = slugify_component(metadata.get("title"), "unknown_title")
    build_version = slugify_component(metadata.get("version"), "unknown")

    remote_folder = (
        f"archives/"
        f"{title_folder}/"
        f"vn_{vn_id}/"
        f"build_{build_version}"
    )

    return upload_to_b2(filepath, remote_folder=remote_folder)
    
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

def upload_metadata_sidecar(metadata, vn_id):
    """
    Upload metadata as a sidecar artifact to a consolidated metadata namespace:
    metadata/<title>/vn_<id>/build_<version>/v<schema>/<title>__build_<version>__sha_<sha8>.yml
    """

    if not isinstance(metadata, dict):
        raise ValueError("Metadata sidecar upload requires a metadata dictionary.")

    metadata_version = metadata.get("metadata_version")
    if metadata_version in (None, ""):
        metadata_version = detect_latest_metadata_template_version()

    title_slug = slugify_component(metadata.get("title"), "unknown_title")
    build_slug = slugify_component(metadata.get("version"), "unknown")

    sha256 = get_metadata_value(
        metadata,
        "sha256",
        get_metadata_value(metadata, "archive.sha256")
    )
    sha_prefix = str(sha256 or "unknown")[:8]

    raw_platform = metadata.get("target_platform")
    if isinstance(raw_platform, list) and raw_platform:
        platform_value = raw_platform[0]
    else:
        platform_value = raw_platform

    platform_slug = slugify_component(platform_value, "unknown")
    build_type_slug = slugify_component(metadata.get("build_type"), "unknown")

    filename = (
        f"{title_slug}_"
        f"{build_slug}_"
        f"{sha_prefix}_meta.yml"
    )
    remote_folder = (
        f"metadata/"
        f"{title_slug}/"
        f"vn_{vn_id}/"
        f"build_{build_slug}/"
        f"v{metadata_version}"
    )

    with tempfile.NamedTemporaryFile("w", suffix=".yml", encoding="utf-8", delete=False) as handle:
        temp_path = handle.name
        yaml.dump(metadata, handle, sort_keys=False, allow_unicode=True)

    final_temp_path = Path(temp_path)

    try:
        local_sidecar_path = final_temp_path.with_name(filename)
        final_temp_path.rename(local_sidecar_path)
        return upload_to_b2(str(local_sidecar_path), remote_folder=remote_folder)
    finally:
        if final_temp_path.exists():
            final_temp_path.unlink(missing_ok=True)
        local_sidecar_path = final_temp_path.with_name(filename)
        if local_sidecar_path.exists():
            local_sidecar_path.unlink(missing_ok=True)
