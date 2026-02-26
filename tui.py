#!/usr/bin/env python3

import os
import yaml
import shutil
import subprocess
import tempfile
import json
from db_manager import initialize_database, get_connection
from pathlib import Path
from colorama import init, Fore, Style
from vn_archiver import (
    create_archive_only,
    upload_archive,
    INCOMING_DIR,
    PROCESSED_DIR,
    sha256_file,
    load_metadata_template,
    resolve_prompt_fields,
    get_available_metadata_template_versions,
    detect_latest_metadata_template_version,
    insert_visual_novel
)

UPLOADED_DIR = "uploaded"

init(autoreset=True)

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

SUGGESTED_CONTENT_TYPE = [
    "main_story", "story_expansion", "seasonal_event",
    "april_fools", "side_story", "non_canon_special"
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
            if f.endswith("archive.zip")]


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
        "content_type": SUGGESTED_CONTENT_TYPE,
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

# Make sure to import insert_visual_novel if this is in tui.py

def edit_metadata_only():
    conn = get_connection()
    try:
        # 1. List available Visual Novels
        print("\n--- Select Visual Novel to Edit ---")
        vns = conn.execute("SELECT id, title FROM visual_novels").fetchall()
        if not vns:
            print("No visual novels in the database yet.")
            return

        for vn in vns:
            print(f"[{vn['id']}] {vn['title']}")

        vn_id_str = input("\nEnter VN ID to edit (or press Enter to cancel): ").strip()
        if not vn_id_str.isdigit():
            return
        vn_id = int(vn_id_str)

        # 2. List available builds for the selected VN
        print("\n--- Select Build to Edit ---")
        builds = conn.execute("SELECT id, version, build_type FROM builds WHERE vn_id = ?", (vn_id,)).fetchall()
        if not builds:
            print("No builds found for this visual novel.")
            return

        for build in builds:
            print(f"[{build['id']}] Version: {build['version']} - Type: {build['build_type']}")

        build_id_str = input("\nEnter Build ID to edit (or press Enter to cancel): ").strip()
        if not build_id_str.isdigit():
            return
        build_id = int(build_id_str)

        # 3. Fetch metadata for the specific build
        row = conn.execute('''
                    SELECT metadata_json 
                    FROM archives 
                    WHERE build_id = ?
                    LIMIT 1
                ''', (build_id,)).fetchone()

        if row:
            # Load metadata from the archive layer if physical files exist
            current_metadata = json.loads(row["metadata_json"])
        else:
            # FALLBACK: If "Create Metadata Only" was used, no archives exist.
            # Fetch the active master metadata for the Visual Novel instead.
            vn_row = conn.execute('''
                        SELECT mo.metadata_json 
                        FROM metadata_versions mv
                        JOIN metadata_objects mo ON mv.metadata_hash = mo.hash
                        WHERE mv.vn_id = ? AND mv.is_current = 1
                    ''', (vn_id,)).fetchone()

            if not vn_row:
                print(Fore.RED + "No metadata found in the database for this Visual Novel.")
                return

            current_metadata = json.loads(vn_row["metadata_json"])

            # Dynamically update the dictionary with the selected build's specifics
            # so the text editor shows the correct build version you selected.
            build_info = conn.execute(
                "SELECT version, build_type FROM builds WHERE id = ?",
                (build_id,)
            ).fetchone()

            if build_info:
                current_metadata["version"] = build_info["version"]
                current_metadata["build_type"] = build_info["build_type"]

    finally:
        conn.close()

    # 4. Show the entire metadata to the user for review
    print(Fore.CYAN + "\n--- Current Metadata Review ---")
    print(Fore.WHITE + yaml.dump(current_metadata, sort_keys=False, allow_unicode=True))
    print(Fore.CYAN + "-------------------------------")

    confirm = input(Fore.YELLOW + "\nDo you want to continue editing this metadata? [y/N]: ").strip().lower()
    if confirm not in ("y", "yes"):
        print(Fore.YELLOW + "Editing cancelled.")
        return

    # 5. Open in System Text Editor
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False, encoding="utf-8") as tf:
        yaml.dump(current_metadata, tf, sort_keys=False, allow_unicode=True)
        temp_path = tf.name

    editor = os.environ.get('EDITOR', 'notepad' if os.name == 'nt' else 'nano')

    print(f"\nOpening metadata in {editor}... Save and close the file when finished.")
    subprocess.call([editor, temp_path])

    # 6. Read the edited file and save
    try:
        with open(temp_path, "r", encoding="utf-8") as f:
            updated_metadata = yaml.safe_load(f)

        if updated_metadata == current_metadata:
            print("\nNo changes detected. Aborting update.")
            return

        # Pass the updated metadata back to the insert function
        insert_visual_novel(updated_metadata)
        print(Fore.GREEN + "\nMetadata successfully updated!")

    except Exception as e:
        print(Fore.RED + f"\nFailed to save metadata: {e}")
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


# =============================
# PROCESS ARCHIVE
# =============================

def process_archive():
    print(Fore.CYAN + "\n--- Process Archive ---")
    if not os.path.exists(INCOMING_DIR):
        os.makedirs(INCOMING_DIR)

    files = [f for f in os.listdir(INCOMING_DIR) if os.path.isfile(os.path.join(INCOMING_DIR, f))]
    if not files:
        print(Fore.RED + f"No files found in '{INCOMING_DIR}' directory.")
        return

    for i, f in enumerate(files, 1):
        print(f"[{i}] {f}")

    choice = input(
        Fore.YELLOW + "\nSelect file numbers to process together (comma-separated), or 0 to cancel: ").strip()
    if choice == "0" or not choice:
        return

    try:
        indices = [int(idx.strip()) - 1 for idx in choice.split(",") if idx.strip().isdigit()]
        selected_paths = []

        # Gathering files in a loop
        for idx in indices:
            if 0 <= idx < len(files):
                selected_paths.append(os.path.join(INCOMING_DIR, files[idx]))
            else:
                print(Fore.RED + f"Invalid selection: {idx + 1}")
                return

        if not selected_paths:
            print(Fore.RED + "No valid files selected.")
            return

        active_version = detect_latest_metadata_template_version()

        # Passing the gathered list ONCE to the backend
        create_archive_only(selected_paths, metadata_version=active_version)

    except ValueError:
        print(Fore.RED + "Invalid input.")


# =============================
# UPLOAD
# =============================

def upload_archives():
    print(Fore.CYAN + "\n--- Upload Archive ---")
    if not os.path.exists(UPLOADED_DIR):
        print(Fore.RED + "Uploaded directory does not exist.")
        return

    # Find all directories that contain a metadata.yaml
    release_folders = []
    for root, dirs, files in os.walk(UPLOADED_DIR):
        if "metadata.yaml" in files:
            release_folders.append(root)

    if not release_folders:
        print(Fore.RED + "No processed release folders found in the uploaded directory.")
        return

    for i, path in enumerate(release_folders, 1):
        rel_path = os.path.relpath(path, UPLOADED_DIR)
        print(f"[{i}] {rel_path}")

    choice = input(Fore.YELLOW + "\nSelect the release folder number to upload, or 0 to cancel: ").strip()
    if choice == "0" or not choice:
        return

    try:
        idx = int(choice) - 1
        if 0 <= idx < len(release_folders):
            selected_folder = release_folders[idx]
            upload_archive(selected_folder)
        else:
            print(Fore.RED + "Invalid selection.")
    except ValueError:
        print(Fore.RED + "Invalid input.")


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
