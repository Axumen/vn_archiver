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
    move_uploaded_archive,
    INCOMING_DIR,
    PROCESSED_DIR,
    sha256_file,
    load_metadata_template,
    resolve_prompt_fields,
    get_available_metadata_template_versions,
    detect_latest_metadata_template_version,
)


init(autoreset=True)


SELECTED_METADATA_TEMPLATE_VERSION = None


SELECTED_METADATA_TEMPLATE_VERSION = None

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


def get_active_metadata_template_version():
    if SELECTED_METADATA_TEMPLATE_VERSION is not None:
        return SELECTED_METADATA_TEMPLATE_VERSION
    return detect_latest_metadata_template_version()


def configure_metadata_template_version():
    global SELECTED_METADATA_TEMPLATE_VERSION

    versions = get_available_metadata_template_versions()
    if not versions:
        print(Fore.RED + "No metadata templates found in metadata_templates/.\n")
        return

    print(Fore.CYAN + "\nAvailable metadata template versions:")
    for version in versions:
        tag = " (latest)" if version == versions[-1] else ""
        print(Fore.CYAN + f"- v{version}{tag}")

    selected = input(Fore.YELLOW + "\nSelect metadata template version number: ").strip()
    try:
        selected_version = int(selected)
    except ValueError:
        print(Fore.RED + "Invalid version selection.\n")
        return

    if selected_version not in versions:
        print(Fore.RED + f"Template v{selected_version} not found.\n")
        return

    template = load_metadata_template(selected_version)
    fields = resolve_prompt_fields(template)

    print(Fore.BLUE + f"\nTemplate preview for v{selected_version}:")
    print(Fore.BLUE + f"metadata_version: {template.get('metadata_version', selected_version)}")

    required = template.get("required") or []
    optional = template.get("optional") or []

    if required:
        print(Fore.GREEN + "Required fields:")
        for field in required:
            print(Fore.GREEN + f"  - {field}")

    if optional:
        print(Fore.GREEN + "Optional fields:")
        for field in optional:
            print(Fore.GREEN + f"  - {field}")

    if not required and not optional:
        print(Fore.GREEN + "Prompt fields:")
        for field in fields:
            print(Fore.GREEN + f"  - {field}")

    confirm = input(Fore.YELLOW + f"\nUse metadata template v{selected_version}? [y/N]: ").strip().lower()
    if confirm in ("y", "yes"):
        SELECTED_METADATA_TEMPLATE_VERSION = selected_version
        print(Fore.GREEN + f"Metadata template v{selected_version} is now active.\n")
    else:
        print(Fore.YELLOW + "No changes made to active metadata template.\n")


def get_active_metadata_template_version():
    if SELECTED_METADATA_TEMPLATE_VERSION is not None:
        return SELECTED_METADATA_TEMPLATE_VERSION
    return detect_latest_metadata_template_version()


def configure_metadata_template_version():
    global SELECTED_METADATA_TEMPLATE_VERSION

    versions = get_available_metadata_template_versions()
    if not versions:
        print(Fore.RED + "No metadata templates found in metadata_templates/.\n")
        return

    print(Fore.CYAN + "\nAvailable metadata template versions:")
    for version in versions:
        tag = " (latest)" if version == versions[-1] else ""
        print(Fore.CYAN + f"- v{version}{tag}")

    selected = input(Fore.YELLOW + "\nSelect metadata template version number: ").strip()
    try:
        selected_version = int(selected)
    except ValueError:
        print(Fore.RED + "Invalid version selection.\n")
        return

    if selected_version not in versions:
        print(Fore.RED + f"Template v{selected_version} not found.\n")
        return

    template = load_metadata_template(selected_version)
    fields = resolve_prompt_fields(template)

    print(Fore.BLUE + f"\nTemplate preview for v{selected_version}:")
    print(Fore.BLUE + f"metadata_version: {template.get('metadata_version', selected_version)}")

    required = template.get("required") or []
    optional = template.get("optional") or []

    if required:
        print(Fore.GREEN + "Required fields:")
        for field in required:
            print(Fore.GREEN + f"  - {field}")

    if optional:
        print(Fore.GREEN + "Optional fields:")
        for field in optional:
            print(Fore.GREEN + f"  - {field}")

    if not required and not optional:
        print(Fore.GREEN + "Prompt fields:")
        for field in fields:
            print(Fore.GREEN + f"  - {field}")

    confirm = input(Fore.YELLOW + f"\nUse metadata template v{selected_version}? [y/N]: ").strip().lower()
    if confirm in ("y", "yes"):
        SELECTED_METADATA_TEMPLATE_VERSION = selected_version
        print(Fore.GREEN + f"Metadata template v{selected_version} is now active.\n")
    else:
        print(Fore.YELLOW + "No changes made to active metadata template.\n")


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
    metadata_version = get_active_metadata_template_version()
    template = load_metadata_template(metadata_version)
    metadata["metadata_version"] = metadata_version

    print(Fore.MAGENTA + "Fill Metadata (Press ENTER to skip fields)\n")

    field_suggestions = {
        "release_status": SUGGESTED_RELEASE_STATUS,
        "distribution_model": SUGGESTED_DISTRIBUTION_MODEL,
        "build_type": SUGGESTED_BUILD_TYPE,
        "language": SUGGESTED_LANGUAGE,
        "distribution_platform": SUGGESTED_DISTRIBUTION_PLATFORM,
        "content_rating": SUGGESTED_CONTENT_RATING,
        "target_platform": SUGGESTED_TARGET_PLATFORM,
        "tags": [
            "romance", "drama", "comedy", "slice-of-life", "mystery", "horror",
            "sci-fi", "fantasy", "psychological", "thriller", "action", "historical",
            "supernatural", "nakige", "utsuge", "nukige", "moege", "dark", "wholesome",
            "tragic", "bittersweet", "school", "modern", "adult"
        ],
    }

    prompt_fields = resolve_prompt_fields(template)

    for field in prompt_fields:
        if field in ("tags", "target_platform"):
            suggestions = field_suggestions.get(field) or []
            if suggestions:
                print(Fore.CYAN + f"Suggested {field}:")
                print(", ".join(suggestions))
            value = input(Fore.YELLOW + f"{field} (comma separated): ").strip()
            metadata[field] = normalize_list(value)
            continue

        suggestions = field_suggestions.get(field)
        if suggestions:
            print(Fore.CYAN + f"Suggested {field}:")
            print(", ".join(suggestions))

        value = input(Fore.YELLOW + f"{field}: ").strip()
        metadata[field] = normalize_value(value)

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
            
            # ---- Metadata schema version detection ----
            metadata_version = metadata.get("metadata_version")
            if metadata_version is None:
                metadata_version = detect_latest_metadata_template_version()
                metadata["metadata_version"] = metadata_version

            # Ensure an installed template exists for the metadata version in use.
            load_metadata_template(metadata_version)
                
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
    upload_successful = upload_archive(archive_path, metadata, vn_id)

    if not upload_successful:
        print(Fore.YELLOW + "Upload was not completed. Archive left in processed.\n")
        return

    try:
        moved_path = move_uploaded_archive(archive_path, metadata)
    except Exception as e:
        print(Fore.RED + f"Upload succeeded but post-upload move failed: {e}\n")
        return

    with get_connection() as conn:
        conn.execute(
            "UPDATE visual_novels SET archive_path = ?, status = ? WHERE id = ?",
            (moved_path, "uploaded", row["id"])
        )

    print(Fore.GREEN + f"Upload complete. Archive moved to: {moved_path}\n")


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
        print(Fore.MAGENTA + "5) Config")
        print(Fore.MAGENTA + "6) Quit\n")

        active_version = get_active_metadata_template_version()
        print(Fore.CYAN + f"Active metadata template: v{active_version}\n")

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
            configure_metadata_template_version()
        elif choice == "6":
            print(Fore.CYAN + "\nGoodbye.\n")
            break
        else:
            print(Fore.RED + "Invalid option.\n")


if __name__ == "__main__":
    main()
