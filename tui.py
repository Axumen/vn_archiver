#!/usr/bin/env python3

import os
import yaml
import shutil
import json
from tools.db_manager import initialize_database
from pathlib import Path
from colorama import init, Fore, Style
from vn_archiver import (
    create_archive_only,
    upload_archive,
    INCOMING_DIR,
    PROCESSED_DIR,
    sha256_file,
    load_metadata_template
)


init(autoreset=True)

# =============================
# SUGGESTED VALUES (Normalized)
# =============================

SUGGESTED_RELEASE_STATUS = [
    "ongoing", "completed", "hiatus", "cancelled", "abandoned"
]

SUGGESTED_DISTRIBUTION_MODEL = [
    "free", "paid", "freemium", "donationware", "subscription", "patron_only"
]

SUGGESTED_BUILD_TYPE = [
    "full", "demo", "trial", "alpha", "beta", "release-candidate", "patch", "dlc"
]

SUGGESTED_LANGUAGE = [
    "japanese", "english", "chinese-simplified", "chinese-traditional",
    "korean", "spanish", "german", "french", "russian", "multi-language"
]

SUGGESTED_DISTRIBUTION_PLATFORM = [
    "steam", "itch.io", "dlsite", "fanza", "gumroad",
    "patreon", "booth", "self-distributed", "other"
]

SUGGESTED_CONTENT_RATING = [
    "all-ages", "teen", "mature", "18+", "unrated"
]

SUGGESTED_TARGET_PLATFORM = [
    "windows", "linux", "mac", "android", "web", "ios", "switch"
]

# =============================
# HELPERS
# =============================



def header():
    width = shutil.get_terminal_size().columns
    title = "VN ARCHIVER SYSTEM"

    line = "─" * width
    centered_title = title.center(width)

    print()
    print(Fore.CYAN + line)
    print(Style.BRIGHT + Fore.WHITE + centered_title)
    print(Fore.CYAN + line + "\n")



def list_zips():
    return [f for f in os.listdir(INCOMING_DIR)
            if f.endswith(".zip")]


def list_metadata():
    return [f for f in os.listdir(INCOMING_DIR)
            if f.endswith(".yaml")]


def list_processed_archives():
    return [f for f in os.listdir(PROCESSED_DIR)
            if f.endswith(".zip")]

def normalize_value(value):
    return value.strip() if value else None


def normalize_list(value):
    if not value:
        return None
    return sorted(set([v.strip() for v in value.split(",") if v.strip()]))
    
def show_file_info(filename):
    path = Path(INCOMING_DIR) / filename
    size = path.stat().st_size
    hash_val = sha256_file(path)

    print(Fore.GREEN + f"File: {filename}")
    print(Fore.GREEN + f"Size: {size} bytes")
    print(Fore.GREEN + f"SHA256: {hash_val}\n")


def choose_from_list(items, title):
    if not items:
        print(Fore.RED + "Nothing available.\n")
        return None

    print(Fore.BLUE + f"\n{title}")
    for i, item in enumerate(items, 1):
        print(f"{i}) {item}")

    try:
        selection = int(input(Fore.YELLOW + "\nSelect number: "))
        return items[selection - 1]
    except (ValueError, IndexError):
        print(Fore.RED + "Invalid selection.\n")
        return None


# =============================
# METADATA CREATION
# =============================

def create_metadata_only():
    
    zips = list_zips()
    filename = choose_from_list(zips, "Select VN to create metadata for")
    if not filename:
        return

    show_file_info(filename)

    metadata = {}
    metadata["metadata_version"] = 1    

    print(Fore.MAGENTA + "Fill Metadata (Press ENTER to skip fields)\n")

    def ask_with_suggestions(field, suggestions):
        print(Fore.CYAN + f"Suggested {field}:")
        print(", ".join(suggestions))
        value = input(Fore.YELLOW + f"{field}: ").strip()
        return normalize_value(value)

    # Basic free fields
    metadata["developer"] = normalize_value(input(Fore.YELLOW + "developer: ").strip())
    metadata["title"] = normalize_value(input(Fore.YELLOW + "title: ").strip())
    metadata["version"] = normalize_value(input(Fore.YELLOW + "version: ").strip())

    # Structured fields with suggestions
    metadata["release_status"] = ask_with_suggestions(
        "release_status", SUGGESTED_RELEASE_STATUS
    )

    metadata["distribution_model"] = ask_with_suggestions(
        "distribution_model", SUGGESTED_DISTRIBUTION_MODEL
    )

    metadata["build_type"] = ask_with_suggestions(
        "build_type", SUGGESTED_BUILD_TYPE
    )

    metadata["release_date"] = normalize_value(input(Fore.YELLOW + "release_date (YYYY-MM-DD): ").strip())

    # Engine (NO suggestion enforcement per your request)
    metadata["engine"] = normalize_value(input(Fore.YELLOW + "engine: ").strip())

    metadata["engine_version"] = normalize_value(input(Fore.YELLOW + "engine_version: ").strip())

    metadata["language"] = ask_with_suggestions(
        "language", SUGGESTED_LANGUAGE
    )

    metadata["distribution_platform"] = ask_with_suggestions(
        "distribution_platform", SUGGESTED_DISTRIBUTION_PLATFORM
    )

    metadata["content_rating"] = ask_with_suggestions(
        "content_rating", SUGGESTED_CONTENT_RATING
    )

    metadata["source"] = normalize_value(input(Fore.YELLOW + "source URL: ").strip())
    metadata["notes"] = normalize_value(input(Fore.YELLOW + "notes: ").strip())

    # Target platform (comma separated normalized list)
    print(Fore.CYAN + "Suggested target_platform:")
    print(", ".join(SUGGESTED_TARGET_PLATFORM))
    value = input(Fore.YELLOW + "target_platform (comma separated): ").strip()
    metadata["target_platform"] = normalize_list(value)

    # Tags (still manual, only suggestions shown)
    print(Fore.CYAN + "Suggested tags:")
    print("romance, drama, comedy, slice-of-life, mystery, horror, sci-fi, fantasy, psychological, thriller, action, historical, supernatural, nakige, utsuge, nukige, moege, dark, wholesome, tragic, bittersweet, school, modern, adult")
    value = input(Fore.YELLOW + "tags (comma separated): ").strip()
    metadata["tags"] = normalize_list(value)

    metadata_path = Path(INCOMING_DIR) / (Path(filename).stem + ".yaml")

    with open(metadata_path, "w", encoding="utf-8") as f:
        yaml.dump(metadata, f, sort_keys=False)

    print(Fore.GREEN + f"\nMetadata created: {metadata_path.name}\n")


# =============================
# METADATA EDITING
# =============================

def edit_metadata_only():
    metadata_files = list_metadata()
    filename = choose_from_list(metadata_files, "Select metadata file to edit")
    if not filename:
        return

    path = Path(INCOMING_DIR) / filename

    with open(path, "r", encoding="utf-8") as f:
        metadata = yaml.safe_load(f) or {}

    print(Fore.MAGENTA + "\nPress ENTER to keep current value.\n")

    for key, value in metadata.items():
        new_value = input(Fore.YELLOW + f"{key} [{value}]: ").strip()
        if new_value:
            if key in ["tags", "target_platform"]:
                metadata[key] = [x.strip() for x in new_value.split(",")]
            else:
                metadata[key] = new_value

    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(metadata, f, sort_keys=False)

    print(Fore.GREEN + "\nMetadata updated.\n")


# =============================
# PROCESS ARCHIVE
# =============================

def process_archive():
    zips = list_zips()
    zip_filename = choose_from_list(zips, "Select VN ZIP to process")
    if not zip_filename:
        return

    metadata_files = list_metadata()
    metadata_filename = choose_from_list(metadata_files, "Select metadata YAML to use")
    if not metadata_filename:
        return

    zip_base = Path(zip_filename).stem
    metadata_base = Path(metadata_filename).stem

    zip_path = Path(INCOMING_DIR) / zip_filename
    metadata_path = Path(INCOMING_DIR) / metadata_filename

    print(Fore.CYAN + "\n=== PROCESSING ARCHIVE ===\n")

    # 🔎 Check filename match
    if zip_base != metadata_base:
        print(Fore.YELLOW + "WARNING: ZIP and metadata filenames do not match.")
        print(Fore.YELLOW + f"ZIP: {zip_filename}")
        print(Fore.YELLOW + f"Metadata: {metadata_filename}")
        confirm = input(Fore.RED + "Are you sure you want to continue? (y/N): ").strip().lower()
        if confirm != "y":
            print(Fore.RED + "\nProcess cancelled.\n")
            return

    # Step 1: Load metadata
    print(Fore.BLUE + "Loading metadata...", end=" ")
    try:
        with open(metadata_path, "r", encoding="utf-8") as f:
            metadata = yaml.safe_load(f)
            
            from tools.db_manager import get_connection

            # ---- Enforce metadata versioning ----
            build_version = metadata.get("version")
            title = metadata.get("title")

            with get_connection() as conn:
                row = conn.execute(
                    """
                    SELECT metadata_json FROM visual_novels
                    WHERE title = ? AND version = ?
                    ORDER BY id DESC LIMIT 1
                    """,
                    (title, build_version)
                ).fetchone()

            if row:
                try:
                    existing_metadata = json.loads(row["metadata_json"])
                    old_version = existing_metadata.get("metadata_version", 1)
                    metadata["metadata_version"] = old_version + 1
                except Exception:
                    metadata["metadata_version"] = 1
            else:
                metadata["metadata_version"] = 1
                
    except Exception as e:
        print(Fore.RED + "FAILED")
        print(Fore.RED + f"Error reading metadata: {e}\n")
        return

    if not metadata:
        print(Fore.RED + "FAILED")
        print(Fore.RED + "Metadata file is empty.\n")
        return

    print(Fore.GREEN + "OK")

    # Step 2: Creating archive
    print(Fore.BLUE + "Creating archive (hashing + packaging)...", end=" ")
    try:
        archive_path = create_archive_only(zip_filename, metadata)
    except Exception as e:
        print(Fore.RED + "FAILED")
        print(Fore.RED + f"Archive creation failed: {e}\n")
        return

    print(Fore.GREEN + "DONE")

    # Step 3: Move metadata (with overwrite option)
    print(Fore.BLUE + "Moving metadata to processed folder...", end=" ")

    # ---- Structured metadata naming ----
    from tools.db_manager import get_connection

    with get_connection() as conn:
        row = conn.execute(
            "SELECT id FROM visual_novels WHERE sha256 = ?",
            (metadata["sha256"],)
        ).fetchone()

    if not row:
        print(Fore.RED + "Could not determine vn_id for metadata naming.\n")
        return

    def sanitize(value):
        return str(value).strip().replace(" ", "_")

    title = sanitize(metadata.get("title") or "Unknown_Title")
    build_version = sanitize(metadata.get("version") or "unknown")
    meta_version = metadata.get("metadata_version", 1)

    new_meta_name = f"{title}_build_{build_version}_meta_v{meta_version}.yml"
    destination_path = Path(PROCESSED_DIR) / new_meta_name

    try:
        with open(destination_path, "w", encoding="utf-8") as f:
            yaml.dump(metadata, f, sort_keys=False, allow_unicode=True)

        metadata_path.unlink()

    except Exception as e:
        print(Fore.RED + "FAILED")
        print(Fore.RED + f"Metadata move failed: {e}\n")
        return


# =============================
# UPLOAD
# =============================

def upload_archives():
    archives = list_processed_archives()
    filename = choose_from_list(archives, "Select archive to upload")
    if not filename:
        return

    archive_path = os.path.join(PROCESSED_DIR, filename)

    # ---- Load metadata + vn_id from DB ----
    from tools.db_manager import get_connection

    with get_connection() as conn:
        row = conn.execute(
            "SELECT id, metadata_json FROM visual_novels WHERE archive_path = ?",
            (archive_path,)
        ).fetchone()

    if not row:
        print(Fore.RED + "Archive not found in database.\n")
        return

    vn_id = f"{row['id']:06d}"

    raw_metadata = row["metadata_json"]

    if not raw_metadata:
        print(Fore.RED + "Metadata is empty in database.\n")
        return

    try:
        if isinstance(raw_metadata, str):
            metadata = json.loads(raw_metadata)
        else:
            metadata = raw_metadata
    except Exception as e:
        print(Fore.RED + f"Failed to parse metadata from database.")
        print(Fore.RED + f"Raw value: {raw_metadata}")
        print(Fore.RED + f"Error: {e}\n")
        return

    # ---- Call structured upload ----
    upload_archive(archive_path, metadata, vn_id)

    print(Fore.GREEN + "Upload complete.\n")


# =============================
# MAIN MENU
# =============================

def main():
    
    initialize_database()
    
    while True:
        header()

        print(Fore.MAGENTA + "1) Create Metadata")
        print(Fore.MAGENTA + "2) Edit Metadata")
        print(Fore.MAGENTA + "3) Process Archive")
        print(Fore.MAGENTA + "4) Upload Archive")
        print(Fore.MAGENTA + "5) Quit\n")

        choice = input(Fore.YELLOW + "Select option: ").strip()

        if choice == "1":
            create_metadata_only()
        elif choice == "2":
            edit_metadata_only()
        elif choice == "3":
            process_archive()
        elif choice == "4":
            upload_archives()
        elif choice == "5":
            print(Fore.CYAN + "\nGoodbye.\n")
            break
        else:
            print(Fore.RED + "Invalid option.\n")


if __name__ == "__main__":
    main()