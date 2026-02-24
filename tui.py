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
    move_uploaded_archive,
    move_processed_metadata_to_uploaded,
    move_original_to_uploaded_local,
    upload_metadata_sidecar,
    INCOMING_DIR,
    PROCESSED_DIR,
    sha256_file,
    slugify_component,
    load_metadata_template,
    resolve_prompt_fields,
    get_available_metadata_template_versions,
    detect_latest_metadata_template_version,
)

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
    "full", "demo", "trial", "alpha", "beta", "release-candidate", "patch", "dlc", "seasonal", "side-story"
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

        # 2. Fetch the current active metadata
        row = conn.execute('''
            SELECT mo.metadata_json 
            FROM metadata_versions mv
            JOIN metadata_objects mo ON mv.metadata_hash = mo.hash
            WHERE mv.vn_id = ? AND mv.is_current = 1
        ''', (vn_id,)).fetchone()

        if not row:
            print("No current metadata found for this VN.")
            return

        current_metadata = json.loads(row["metadata_json"])

    finally:
        conn.close()

    # 3. Open in System Text Editor
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False, encoding="utf-8") as tf:
        yaml.dump(current_metadata, tf, sort_keys=False, allow_unicode=True)
        temp_path = tf.name

    # Chooses 'notepad' on Windows, 'nano' or system default on Linux/Mac
    editor = os.environ.get('EDITOR', 'notepad' if os.name == 'nt' else 'nano')

    print(f"\nOpening metadata in {editor}... Save and close the file when finished.")
    subprocess.call([editor, temp_path])

    # 4. Read the edited file and save
    try:
        with open(temp_path, "r", encoding="utf-8") as f:
            updated_metadata = yaml.safe_load(f)

        if updated_metadata == current_metadata:
            print("\nNo changes detected. Aborting update.")
            return

        # Re-run it through the engine. It will update the rows and create a new version!
        insert_visual_novel(updated_metadata)
        print("\nMetadata successfully updated and a new version history was created!")

    except Exception as e:
        print(f"\nFailed to save metadata: {e}")
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

    # Allow selecting multiple files (e.g., "1, 2, 4")
    choice = input(
        Fore.YELLOW + "\nSelect file numbers to process together (comma-separated), or 0 to cancel: ").strip()
    if choice == "0" or not choice:
        return

    try:
        # Parse the comma-separated choices
        indices = [int(idx.strip()) - 1 for idx in choice.split(",") if idx.strip().isdigit()]
        selected_paths = []
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

        # Pass the LIST of files to the backend
        create_archive_only(selected_paths, metadata_version=active_version)

    except ValueError:
        print(Fore.RED + "Invalid input.")

# =============================
# UPLOAD
# =============================

def upload_archives():
    archives = list_processed_archives()
    filename = choose_from_list(archives, "Select archive to upload")
    if not filename:
        return

    if not filename.endswith("archive.zip"):
        print(Fore.RED + "Only repackaged files ending with 'archive.zip' can be uploaded.\n")
        return

    archive_path = os.path.join(PROCESSED_DIR, filename)

    try:
        with open(archive_path, "rb") as f:
            pass
    except Exception as e:
        print(Fore.RED + f"Cannot open archive: {e}\n")
        return

    # ---- Load metadata + vn_id from DB ----
    from tools.db_manager import get_connection

    metadata = None
    try:
        import zipfile
        with zipfile.ZipFile(archive_path, "r") as archive:
            if "metadata.yml" not in archive.namelist():
                print(Fore.RED + "Archive does not contain metadata.yml. Upload blocked.\n")
                return
            with archive.open("metadata.yml") as metadata_file:
                metadata = yaml.safe_load(metadata_file.read().decode("utf-8")) or {}
    except Exception as e:
        print(Fore.RED + f"Failed to read metadata.yml from archive: {e}\n")
        return

    archive_source_sha = metadata.get("sha256")
    if not archive_source_sha:
        print(Fore.RED + "metadata.yml is missing sha256. Upload blocked.\n")
        return

    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT vn.id
            FROM archives a
            JOIN builds b ON a.build_id = b.id
            JOIN visual_novels vn ON b.vn_id = vn.id
            WHERE a.sha256 = ?
            """,
            (archive_source_sha,)
        ).fetchone()

    if not row:
        print(Fore.RED + "Archive not found in database by SHA256.\n")
        return

    vn_id = f"{row['id']:06d}"

    title_slug = slugify_component(metadata.get("title"), "unknown")
    build_slug = slugify_component(metadata.get("version"), "unknown")
    sha8 = str(metadata.get("sha256", ""))[:8] or "unknown"

    expected_meta_name = (
        f"{title_slug}_"
        f"{build_slug}_"
        f"{sha8}.meta.yml"
    )

    expected_meta_path = Path(PROCESSED_DIR) / expected_meta_name

    if not expected_meta_path.exists():
        print(Fore.RED + f"Corresponding metadata sidecar not found: {expected_meta_name}\n")
        return

    # ---- Call structured upload ----
    upload_successful = upload_archive(archive_path, metadata, vn_id)

    if not upload_successful:
        print(Fore.YELLOW + "Upload was not completed. Archive left in processed.\n")
        return

    upload_metadata = input(
        Fore.YELLOW + "Upload metadata sidecar to B2 metadata/<title>/vn_<id>/build_<version>/v* namespace? [y/N]: "
    ).strip().lower()

    if upload_metadata in ("y", "yes"):
        metadata_uploaded = upload_metadata_sidecar(metadata, vn_id)
        if not metadata_uploaded:
            print(Fore.YELLOW + "Metadata sidecar upload skipped/cancelled. Archive remains uploaded.\n")

    try:
        moved_path = move_uploaded_archive(archive_path, metadata)
    except Exception as e:
        print(Fore.RED + f"Upload succeeded but post-upload move failed: {e}\n")
        return

    try:
        moved_meta_path = move_processed_metadata_to_uploaded(str(expected_meta_path), metadata)
    except Exception as e:
        print(Fore.RED + f"Upload succeeded but metadata move to uploaded failed: {e}\n")
        return

    with get_connection() as conn:
        conn.execute(
            "UPDATE visual_novels SET status = ? WHERE id = ?",
            ("uploaded", row[0])
        )

    print(Fore.GREEN + f"Upload complete. Archive moved to: {moved_path}")
    print(Fore.GREEN + f"Metadata moved to: {moved_meta_path}\n")


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
