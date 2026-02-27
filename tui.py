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
    create_archive_from_metadata_file,
    upload_archive,
    INCOMING_DIR,
    UPLOADING_DIR,
    sha256_file,
    load_metadata_template,
    resolve_prompt_fields,
    get_available_metadata_template_versions,
    detect_latest_metadata_template_version,
    insert_visual_novel,
    get_current_metadata_version_number,
    stage_metadata_yaml_for_upload
)

init(autoreset=True)

SELECTED_METADATA_TEMPLATE_VERSION = None

# =============================
# THEME
# =============================

ACCENT = Fore.CYAN
PRIMARY = Fore.MAGENTA
SUCCESS = Fore.GREEN
WARNING = Fore.YELLOW
ERROR = Fore.RED
TEXT = Fore.WHITE


def term_width():
    return max(72, shutil.get_terminal_size().columns)


def rule(char="─", color=ACCENT):
    print(color + (char * term_width()))


def panel(title, subtitle=None):
    width = term_width()
    top = f"┌{'─' * (width - 2)}┐"
    mid = f"│ {title[:width - 4].ljust(width - 4)} │"
    bot = f"└{'─' * (width - 2)}┘"
    print(ACCENT + top)
    print(Style.BRIGHT + TEXT + mid)
    if subtitle:
        sub = f"│ {subtitle[:width - 4].ljust(width - 4)} │"
        print(ACCENT + sub)
    print(ACCENT + bot)


def notify(message, level="info"):
    if level == "ok":
        print(SUCCESS + f"✔ {message}")
    elif level == "warn":
        print(WARNING + f"⚠ {message}")
    elif level == "error":
        print(ERROR + f"✖ {message}")
    else:
        print(ACCENT + f"• {message}")


def prompt(label):
    return input(WARNING + f"➤ {label}").strip()

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
    print()
    panel("VN ARCHIVER SYSTEM", "Metadata + Archive Workflow Console")
    print()


def list_zips():
    return [f for f in os.listdir(INCOMING_DIR)
            if f.endswith(".zip")]


def list_metadata():
    return [f for f in os.listdir(INCOMING_DIR)
            if f.endswith(".yaml")]



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

    notify(f"File: {filename}", "ok")
    notify(f"Size: {size} bytes", "ok")
    notify(f"SHA256: {hash_val}", "ok")
    print()


def choose_from_list(items, title):
    if not items:
        notify("Nothing available.", "error")
        print()
        return None

    panel(title)
    for i, item in enumerate(items, 1):
        print(TEXT + f"  {i:>2}) {item}")

    try:
        selection = int(prompt("Select number: "))
        return items[selection - 1]
    except (ValueError, IndexError):
        notify("Invalid selection.", "error")
        print()
        return None


def get_active_metadata_template_version():
    if SELECTED_METADATA_TEMPLATE_VERSION is not None:
        return SELECTED_METADATA_TEMPLATE_VERSION
    return detect_latest_metadata_template_version()


def configure_metadata_template_version():
    global SELECTED_METADATA_TEMPLATE_VERSION

    versions = get_available_metadata_template_versions()
    if not versions:
        notify("No metadata templates found in metadata_templates/.", "error")
        print()
        return

    panel("Metadata Template Configuration")
    print(ACCENT + "Available metadata template versions:")
    for version in versions:
        tag = " (latest)" if version == versions[-1] else ""
        print(TEXT + f"  - v{version}{tag}")

    selected = prompt("Select metadata template version number: ")
    try:
        selected_version = int(selected)
    except ValueError:
        notify("Invalid version selection.", "error")
        print()
        return

    if selected_version not in versions:
        notify(f"Template v{selected_version} not found.", "error")
        print()
        return

    template = load_metadata_template(selected_version)
    fields = resolve_prompt_fields(template)

    print()
    panel(f"Template Preview v{selected_version}")
    print(ACCENT + f"metadata_version: {template.get('metadata_version', selected_version)}")

    required = template.get("required") or []
    optional = template.get("optional") or []

    if required:
        print(SUCCESS + "Required fields:")
        for field in required:
            print(TEXT + f"  - {field}")

    if optional:
        print(SUCCESS + "Optional fields:")
        for field in optional:
            print(TEXT + f"  - {field}")

    if not required and not optional:
        print(SUCCESS + "Prompt fields:")
        for field in fields:
            print(TEXT + f"  - {field}")

    confirm = prompt(f"Use metadata template v{selected_version}? [y/N]: ").lower()
    if confirm in ("y", "yes"):
        SELECTED_METADATA_TEMPLATE_VERSION = selected_version
        notify(f"Metadata template v{selected_version} is now active.", "ok")
        print()
    else:
        notify("No changes made to active metadata template.", "warn")
        print()


# =============================
# METADATA CREATION
# =============================

def create_metadata_only():
    print()
    panel("Create Metadata")
    if not os.path.exists(INCOMING_DIR):
        os.makedirs(INCOMING_DIR)

    files = [f for f in os.listdir(INCOMING_DIR) if os.path.isfile(os.path.join(INCOMING_DIR, f))]
    if not files:
        notify(f"No files found in '{INCOMING_DIR}' directory.", "error")
        return

    for i, filename in enumerate(files, 1):
        print(TEXT + f"[{i}] {filename}")

    choice = prompt("Select file numbers to process together (comma-separated), or 0 to cancel: ")
    if choice == "0" or not choice:
        return

    try:
        indices = [int(idx.strip()) - 1 for idx in choice.split(",") if idx.strip().isdigit()]
        selected_paths = []

        for idx in indices:
            if 0 <= idx < len(files):
                selected_filename = files[idx]
                show_file_info(selected_filename)
                selected_paths.append(os.path.join(INCOMING_DIR, selected_filename))
            else:
                notify(f"Invalid selection: {idx + 1}", "error")
                return

        if not selected_paths:
            notify("No valid files selected.", "error")
            return

        active_version = get_active_metadata_template_version()
        create_archive_only(selected_paths, metadata_version=active_version)

    except ValueError:
        notify("Invalid input.", "error")


def quick_process_with_metadata_yaml():
    print()
    panel("Quick Process (Archive + Metadata YAML)")

    if not os.path.exists(INCOMING_DIR):
        os.makedirs(INCOMING_DIR)

    zip_files = [f for f in os.listdir(INCOMING_DIR) if f.lower().endswith(".zip")]
    yaml_files = [f for f in os.listdir(INCOMING_DIR) if f.lower().endswith((".yaml", ".yml"))]

    if not zip_files:
        notify(f"No zip files found in '{INCOMING_DIR}'.", "error")
        return
    if not yaml_files:
        notify(f"No metadata yaml files found in '{INCOMING_DIR}'.", "error")
        return

    panel("Select ZIP file(s)")
    for i, filename in enumerate(zip_files, 1):
        print(TEXT + f"[{i}] {filename}")

    zip_choice = prompt("Select zip file numbers (comma-separated), or 0 to cancel: ")
    if zip_choice in ("", "0"):
        return

    panel("Select Metadata YAML")
    for i, filename in enumerate(yaml_files, 1):
        print(TEXT + f"[{i}] {filename}")

    yaml_choice = prompt("Select metadata yaml number, or 0 to cancel: ")
    if yaml_choice in ("", "0"):
        return

    try:
        selected_paths = []
        indices = [int(idx.strip()) - 1 for idx in zip_choice.split(",") if idx.strip().isdigit()]
        for idx in indices:
            if 0 <= idx < len(zip_files):
                selected_filename = zip_files[idx]
                show_file_info(selected_filename)
                selected_paths.append(os.path.join(INCOMING_DIR, selected_filename))
            else:
                notify(f"Invalid zip selection: {idx + 1}", "error")
                return

        if not selected_paths:
            notify("No valid zip files selected.", "error")
            return

        y_idx = int(yaml_choice) - 1
        if not (0 <= y_idx < len(yaml_files)):
            notify("Invalid metadata yaml selection.", "error")
            return

        metadata_path = os.path.join(INCOMING_DIR, yaml_files[y_idx])
        with open(metadata_path, "r", encoding="utf-8") as f:
            metadata = yaml.safe_load(f) or {}

        if not isinstance(metadata, dict):
            notify("Selected metadata yaml is not a valid object.", "error")
            return

        if not metadata.get("title"):
            notify("Metadata must include 'title'.", "error")
            return
        if not metadata.get("version"):
            notify("Metadata must include 'version'.", "error")
            return

        create_archive_from_metadata_file(selected_paths, metadata)
    except ValueError:
        notify("Invalid input.", "error")


# =============================
# METADATA EDITING
# =============================

# Make sure to import insert_visual_novel if this is in tui.py

def edit_metadata_only():
    conn = get_connection()
    try:
        # 1. List available Visual Novels
        print()
        panel("Select Visual Novel to Edit")
        vns = conn.execute("SELECT id, title FROM visual_novels").fetchall()
        if not vns:
            notify("No visual novels in the database yet.", "warn")
            return

        for vn in vns:
            print(f"[{vn['id']}] {vn['title']}")

        vn_id_str = prompt("Enter VN ID to edit (or press Enter to cancel): ")
        if not vn_id_str.isdigit():
            return
        vn_id = int(vn_id_str)

        # 2. List available builds for the selected VN
        print()
        panel("Select Build to Edit")
        builds = conn.execute("SELECT id, version, build_type FROM builds WHERE vn_id = ?", (vn_id,)).fetchall()
        if not builds:
            notify("No builds found for this visual novel.", "warn")
            return

        for build in builds:
            print(f"[{build['id']}] Version: {build['version']} - Type: {build['build_type']}")

        build_id_str = prompt("Enter Build ID to edit (or press Enter to cancel): ")
        if not build_id_str.isdigit():
            return
        build_id = int(build_id_str)

        # 3. Fetch metadata for the specific build.
        # Prefer the canonical current metadata version first, then fall back to
        # archive-layer metadata for legacy rows, then VN-level metadata.
        row = conn.execute('''
                    SELECT mo.metadata_json
                    FROM metadata_versions mv
                    JOIN metadata_objects mo ON mv.metadata_hash = mo.hash
                    WHERE mv.build_id = ? AND mv.is_current = 1
                    ORDER BY mv.created_at DESC, mv.id DESC
                    LIMIT 1
                ''', (build_id,)).fetchone()

        if not row:
            row = conn.execute('''
                        SELECT metadata_json
                        FROM archives
                        WHERE build_id = ?
                        ORDER BY created_at DESC, id DESC
                        LIMIT 1
                    ''', (build_id,)).fetchone()

        if not row:
            # FALLBACK: If "Create Metadata Only" was used, no archives exist.
            # Fetch the active master metadata for the Visual Novel instead.
            row = conn.execute('''
                        SELECT mo.metadata_json
                        FROM metadata_versions mv
                        JOIN metadata_objects mo ON mv.metadata_hash = mo.hash
                        WHERE mv.vn_id = ? AND mv.is_current = 1
                        ORDER BY mv.created_at DESC, mv.id DESC
                        LIMIT 1
                    ''', (vn_id,)).fetchone()

        if not row:
            notify("No metadata found in the database for this Visual Novel.", "error")
            return

        current_metadata = json.loads(row["metadata_json"])

        # Ensure build-specific fields reflect the selected build so the user
        # confirms/edits against the exact build context they chose.
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
    print()
    panel("Current Metadata Review")
    print(TEXT + yaml.dump(current_metadata, sort_keys=False, allow_unicode=True))
    rule()

    confirm = prompt("Do you want to continue editing this metadata? [y/N]: ").lower()
    if confirm not in ("y", "yes"):
        notify("Editing cancelled.", "warn")
        return

    # 5. Open in System Text Editor
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False, encoding="utf-8") as tf:
        yaml.dump(current_metadata, tf, sort_keys=False, allow_unicode=True)
        temp_path = tf.name

    editor = os.environ.get('EDITOR', 'notepad' if os.name == 'nt' else 'nano')

    notify(f"Opening metadata in {editor}... Save and close the file when finished.")
    subprocess.call([editor, temp_path])

    # 6. Read the edited file and save
    try:
        with open(temp_path, "r", encoding="utf-8") as f:
            updated_metadata = yaml.safe_load(f)

        if updated_metadata == current_metadata:
            notify("No changes detected. Aborting update.", "warn")
            return

        # Pass the updated metadata back to the insert function
        vn_id = insert_visual_novel(updated_metadata)
        notify("Metadata successfully updated!", "ok")

        build_row = None
        with get_connection() as conn:
            build_row = conn.execute(
                "SELECT id FROM builds WHERE vn_id = ? AND version = ?",
                (vn_id, updated_metadata.get("version"))
            ).fetchone()

        build_id = build_row["id"] if build_row else None
        metadata_version_number = get_current_metadata_version_number(vn_id=vn_id, build_id=build_id)
        print()
        panel(f"Updated Metadata Copy (v{metadata_version_number})")
        print(TEXT + yaml.dump(updated_metadata, sort_keys=False, allow_unicode=True))

        staged_path = stage_metadata_yaml_for_upload(updated_metadata, metadata_version_number)

        notify(f"Staged metadata copy for upload: {staged_path}", "ok")

    except Exception as e:
        notify(f"Failed to save metadata: {e}", "error")
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


# =============================
# UPLOAD
# =============================

def upload_archives():
    print()
    panel("Upload Archive")
    if not os.path.exists(UPLOADING_DIR):
        notify("Uploading directory does not exist.", "error")
        return

    # Find all zip files queued for upload
    upload_files = []
    for root, _, files in os.walk(UPLOADING_DIR):
        for file in files:
            if file.lower().endswith(".zip"):
                upload_files.append(os.path.join(root, file))

    if not upload_files:
        notify("No zip files found in the uploading directory.", "error")
        return

    for i, path in enumerate(upload_files, 1):
        rel_path = os.path.relpath(path, UPLOADING_DIR)
        print(TEXT + f"[{i}] {rel_path}")

    choice = prompt("Select zip number to upload, or 0 to cancel: ")
    if choice == "0" or not choice:
        return

    try:
        idx = int(choice) - 1
        if 0 <= idx < len(upload_files):
            selected_file = upload_files[idx]
            upload_archive(selected_file)
        else:
            notify("Invalid selection.", "error")
    except ValueError:
        notify("Invalid input.", "error")


# =============================
# MAIN MENU
# =============================

def main():
    initialize_database()

    while True:
        header()

        panel("Main Menu")
        print(PRIMARY + "  1) Create Metadata")
        print(PRIMARY + "  2) Quick Process (Zip + Metadata YAML)")
        print(PRIMARY + "  3) Edit Metadata")
        print(PRIMARY + "  4) Upload Archive")
        print(PRIMARY + "  5) Config")
        print(PRIMARY + "  6) Quit\n")

        active_version = get_active_metadata_template_version()
        notify(f"Active metadata template: v{active_version}")
        print()

        choice = prompt("Select option: ")

        if choice == "1":
            create_metadata_only()
        elif choice == "2":
            quick_process_with_metadata_yaml()
        elif choice == "3":
            edit_metadata_only()
        elif choice == "4":
            upload_archives()
        elif choice == "5":
            configure_metadata_template_version()
        elif choice == "6":
            print()
            panel("Goodbye", "Session closed")
            print()
            break
        else:
            notify("Invalid option.", "error")
            print()


if __name__ == "__main__":
    main()
