#!/usr/bin/env python3

import os
import re
import zipfile
import shutil
import sys
import yaml
import tempfile
import time
import json
import hashlib
from tqdm import tqdm
from colorama import Fore
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

    temp_metadata_path = "metadata.yaml"

    with open(temp_metadata_path, "w", encoding="utf-8") as f:
        yaml.dump(metadata_dict, f, sort_keys=False, allow_unicode=True)

    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_STORED) as archive:
        archive.write(original_zip, arcname=os.path.basename(original_zip))
        archive.write(temp_metadata_path, arcname="metadata.yaml")

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

def create_archive_only(archive_paths=None, metadata_version=DEFAULT_METADATA_VERSION):
    from colorama import Fore, Style
    import zipfile

    if archive_paths is None:
        archive_paths = []
    elif isinstance(archive_paths, str):
        archive_paths = [archive_paths]

    if not os.path.exists(PROCESSED_DIR):
        os.makedirs(PROCESSED_DIR)

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
    prompt_fields = resolve_prompt_fields(base_template)

    metadata = {"metadata_version": metadata_version}

    FIELD_SUGGESTIONS = {
        "release_status": ["ongoing", "completed", "hiatus", "cancelled", "abandoned"],
        "distribution_model": ["free", "paid", "freemium", "donationware", "subscription", "patron_only"],
        "build_type": ["full", "demo", "trial", "alpha", "beta", "release-candidate", "patch", "dlc", "seasonal",
                       "side-story"],
        "language": ["japanese", "english", "chinese-simplified", "chinese-traditional", "korean", "spanish", "german",
                     "french", "russian", "multi-language"],
        "distribution_platform": ["steam", "itch.io", "dlsite", "fanza", "gumroad", "patreon", "booth",
                                  "self-distributed", "other"],
        "content_rating": ["all-ages", "teen", "mature", "18+", "unrated"],
        "target_platform": ["windows", "linux", "mac", "android", "web", "ios", "switch"],
        "content_type": ["main_story", "story_expansion", "seasonal_event", "april_fools", "side_story",
                         "non_canon_special"],
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

    for field in prompt_fields:
        if field in ("tags", "target_platform", "aliases"):
            suggestions = FIELD_SUGGESTIONS.get(field) or []
            if suggestions:
                print(Fore.CYAN + f"Suggested {field}:")
                print(", ".join(suggestions))
            value = input(Fore.YELLOW + f"{field} (comma separated): " + Style.RESET_ALL).strip()
            metadata[field] = normalize_list(value)
        else:
            suggestions = FIELD_SUGGESTIONS.get(field)
            if suggestions:
                print(Fore.CYAN + f"Suggested {field}:")
                print(", ".join(suggestions))
            value = input(Fore.YELLOW + f"{field}: " + Style.RESET_ALL).strip()
            metadata[field] = value if value else None

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

    # -------------------------------------------------------------------
    # 4. Insert into Database
    # -------------------------------------------------------------------
    vn_id = insert_visual_novel(metadata)
    if not vn_id:
        print(Fore.RED + "Failed to insert visual novel into database.")
        return

        # -------------------------------------------------------------------
        # 5 & 6. Repackage ZIP & Update Dynamic Parent Folder
        # -------------------------------------------------------------------

        # Grab the exact strings the user typed
    proper_title = str(metadata.get("title", "Unknown Title"))
    proper_version = str(metadata.get("version", "Unknown Version"))

    # Safely remove invalid Windows/Linux file characters but KEEP spaces and casing
    safe_title = re.sub(r'[\\/*?:"<>|]', "", proper_title).strip()
    safe_version = re.sub(r'[\\/*?:"<>|]', "", proper_version).strip()

    if archives_data:
        # 1. Package the master zip file
        bundle_filename = f"{safe_title} {safe_version}.zip"
        bundle_path = os.path.join(PROCESSED_DIR, bundle_filename)

        print(Fore.CYAN + f"\nPackaging files into {bundle_filename}...")

        with zipfile.ZipFile(bundle_path, 'w', zipfile.ZIP_DEFLATED) as bundle_zip:
            yaml_str = yaml.dump(metadata, sort_keys=False, allow_unicode=True)
            bundle_zip.writestr("metadata.yaml", yaml_str)

            for arch in archives_data:
                bundle_zip.write(arch["original_path"], arch["filename"])
                os.remove(arch["original_path"])

        print(Fore.GREEN + f"Bundle successfully created at: {bundle_path}")

        # 2. Bulletproof Parent Folder Renaming Logic
        new_parent_name = f"{safe_title} {safe_version}"
        new_parent_path = os.path.join(UPLOADED_DIR, new_parent_name)

        if os.path.exists(UPLOADED_DIR):
            for existing_folder in os.listdir(UPLOADED_DIR):
                old_parent_path = os.path.join(UPLOADED_DIR, existing_folder)

                # Check if it's a directory and starts with "Title " (with a space)
                if os.path.isdir(old_parent_path) and existing_folder.startswith(safe_title + " "):

                    # Deduce what the old version string was based on the folder name
                    possible_old_version = existing_folder[len(safe_title) + 1:].strip()

                    # Verify it's actually the matching game by checking if that version subfolder exists inside it
                    if os.path.isdir(os.path.join(old_parent_path, possible_old_version)):
                        if existing_folder != new_parent_name:
                            print(
                                Fore.YELLOW + f"Updating parent folder: '{existing_folder}' -> '{new_parent_name}'")
                            os.rename(old_parent_path, new_parent_path)
                        break

        # 3. Create the final subfolder and move the zip
        uploaded_dest_dir = os.path.join(new_parent_path, safe_version)
        os.makedirs(uploaded_dest_dir, exist_ok=True)

        uploaded_dest_path = os.path.join(uploaded_dest_dir, bundle_filename)
        shutil.copy2(bundle_path, uploaded_dest_path)

        print(Fore.GREEN + f"Bundle copied to: {uploaded_dest_path}")
        print(Fore.GREEN + "Archive processing complete!")

    else:
        # Option 1 Fallback (No physical files selected)
        # Safely names it strictly metadata.yaml inside the processed folder
        meta_filename = "metadata.yaml"
        meta_path = os.path.join(PROCESSED_DIR, meta_filename)

        with open(meta_path, "w", encoding="utf-8") as f:
            yaml.dump(metadata, f, sort_keys=False, allow_unicode=True)

        print(Fore.GREEN + f"\nMetadata saved to: {meta_path}")
        print(Fore.GREEN + "Metadata creation complete!")

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
    from colorama import Fore
    import zipfile
    import yaml
    import os
    from b2sdk.v2 import InMemoryAccountInfo, B2Api
    from tqdm import tqdm

    if not os.path.exists(file_path):
        print(Fore.RED + f"File not found: {file_path}")
        return False

    print(Fore.CYAN + f"\nAnalyzing {os.path.basename(file_path)}...")

    # -------------------------------------------------------------------
    # 1. Read metadata.yaml from inside the master bundle
    # -------------------------------------------------------------------
    try:
        with zipfile.ZipFile(file_path, 'r') as z:
            if 'metadata.yaml' not in z.namelist():
                print(Fore.RED + "Upload Blocked: No 'metadata.yaml' found inside the zip.")
                print(Fore.YELLOW + "This does not appear to be a valid processed bundle.")
                return False

            with z.open('metadata.yaml') as f:
                yaml_content = f.read().decode('utf-8')
                metadata = yaml.safe_load(yaml_content)
    except zipfile.BadZipFile:
        print(Fore.RED + "Upload Blocked: File is not a valid zip archive.")
        return False

    # Strictly convert to string to prevent YAML from reading "1.0" as a float
    title = str(metadata.get("title", ""))
    version = str(metadata.get("version", ""))

    if not title or not version:
        print(Fore.RED + "Upload Blocked: 'metadata.yaml' is missing 'title' or 'version'.")
        return False

    # -------------------------------------------------------------------
    # 2. Block upload if it wasn't inserted into the Database
    # -------------------------------------------------------------------
    vn_id = None
    build_id = None
    with get_connection() as conn:
        # Check if the VN exists
        vn_row = conn.execute("SELECT id FROM visual_novels WHERE title = ?", (title,)).fetchone()
        if not vn_row:
            print(Fore.RED + f"Upload Blocked: Visual Novel '{title}' does not exist in the database.")
            print(Fore.YELLOW + "Please run '(3) Process Archive' to register it before uploading.")
            return False

        vn_id = vn_row[0]

        # Check if this specific version build exists
        build_row = conn.execute("SELECT id FROM builds WHERE vn_id = ? AND version = ?", (vn_id, version)).fetchone()
        if not build_row:
            print(Fore.RED + f"Upload Blocked: Version '{version}' for '{title}' does not exist in the database.")
            print(Fore.YELLOW + "Please run '(3) Process Archive' to register this build before uploading.")
            return False

        build_id = build_row[0]

    # -------------------------------------------------------------------
    # 3. Formulate the Strict Cloud Naming Scheme
    # -------------------------------------------------------------------
    title_slug = slugify_component(title, "unknown")
    version_slug = slugify_component(version, "unknown")

    print(Fore.CYAN + "Calculating outer bundle SHA-256 for cloud verification...")
    bundle_sha256 = sha256_file(file_path)
    short_hash = bundle_sha256[:8]

    ext = os.path.splitext(file_path)[1].lower()

    # Cloud Object Key: archives / [title-slug] / vn-[id] / [version-slug] / [title-slug]_[version-slug]_[hash].zip
    cloud_filename = f"{title_slug}_{version_slug}_{short_hash}{ext}"
    cloud_path = f"archives/{title_slug}/vn-{vn_id:05d}/{version_slug}/{cloud_filename}"

    print(Fore.GREEN + f"Database verification passed (VN ID: {vn_id})")
    print(Fore.CYAN + f"Target Cloud Path: {cloud_path}")

    # -------------------------------------------------------------------
    # 4. Backblaze B2 Authentication via Config
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
    # 5. Actual Upload with Progress Bar
    # -------------------------------------------------------------------
    if dry_run:
        print(Fore.YELLOW + f"[DRY RUN] Would upload to: {cloud_path}")
        return True

    print(Fore.YELLOW + f"Uploading to Backblaze B2. This may take a while...")
    file_size = os.path.getsize(file_path)

    with tqdm(total=file_size, unit='B', unit_scale=True, desc="Progress", colour="green") as pbar:
        # b2sdk expects exactly these three methods
        class TqdmProgressListener:
            def set_total_bytes(self, total_bytes):
                pass

            def bytes_completed(self, byte_count):
                # Update progress by calculating the delta
                pbar.update(byte_count - pbar.n)

            def close(self):
                pass

        try:
            bucket.upload_local_file(
                local_file=str(file_path),
                file_name=cloud_path,
                progress_listener=TqdmProgressListener()
            )
        except Exception as e:
            print(Fore.RED + f"\nUpload failed: {e}")
            return False

    print(Fore.GREEN + "\nUpload Complete!")

    # Safely attempt to update status (ignores error if your table doesn't have a status column)
    with get_connection() as conn:
        try:
            conn.execute("UPDATE builds SET status = ? WHERE id = ?", ("uploaded", build_id))
            conn.execute("UPDATE visual_novels SET status = ? WHERE id = ?", ("uploaded", vn_id))
        except Exception:
            pass

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

def upload_metadata_sidecar(metadata, vn_id):
    """
    Upload metadata as a sidecar artifact to a consolidated metadata namespace:
    metadata/<title>/vn_<id>/build_<version>/v<schema>/<title>__build_<version>__sha_<sha8>.yaml
    """

    if not isinstance(metadata, dict):
        raise ValueError("Metadata sidecar upload requires a metadata dictionary.")

    metadata_version = metadata.get("metadata_version")
    if metadata_version in (None, ""):
        metadata_version = detect_latest_metadata_template_version()

    title_slug = slugify_component(metadata.get("title"), "unknown_title")
    build_slug = slugify_component(metadata.get("version"), "unknown")

    # 1. Try to get top-level SHA (legacy)
    sha256 = get_metadata_value(metadata, "sha256", get_metadata_value(metadata, "archive.sha256"))

    # 2. Fallback to the first archive in the multi-archive list (new schema)
    if not sha256 and metadata.get("archives") and isinstance(metadata["archives"], list) and len(
            metadata["archives"]) > 0:
        sha256 = metadata["archives"][0].get("sha256")

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
        f"{sha_prefix}_meta.yaml"
    )
    remote_folder = (
        f"metadata/"
        f"{title_slug}/"
        f"vn_{vn_id}/"
        f"build_{build_slug}/"
        f"v{metadata_version}"
    )

    with tempfile.NamedTemporaryFile("w", suffix=".yaml", encoding="utf-8", delete=False) as handle:
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
