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
    stage_metadata_yaml_for_upload,
    order_metadata_for_yaml
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

        selected_sha256 = [sha256_file(path) for path in selected_paths]
        yaml_sha256 = []

        if isinstance(metadata.get("archives"), list):
            for archive in metadata["archives"]:
                if isinstance(archive, dict) and archive.get("sha256"):
                    yaml_sha256.append(str(archive["sha256"]).strip().lower())

        top_level_sha = str(metadata.get("sha256", "")).strip().lower()
        if top_level_sha and top_level_sha not in yaml_sha256:
            yaml_sha256.append(top_level_sha)

        if yaml_sha256:
            selected_sha_set = set(s.lower() for s in selected_sha256)
            yaml_sha_set = set(yaml_sha256)
            if selected_sha_set != yaml_sha_set:
                notify("Quick Process blocked: YAML sha256 does not match selected zip file(s).", "error")
                notify(f"ZIP sha256: {', '.join(sorted(selected_sha_set))}", "error")
                notify(f"YAML sha256: {', '.join(sorted(yaml_sha_set))}", "error")
                return
            notify("Confirmed: metadata YAML sha256 matches selected zip file(s).", "ok")
        else:
            notify("No sha256 found in metadata YAML; skipping sha256 confirmation.", "warn")

        ordered_metadata = order_metadata_for_yaml(metadata)
        if list(ordered_metadata.keys()) != list(metadata.keys()):
            notify("Corrected metadata YAML field order based on template before processing.", "info")

        create_archive_from_metadata_file(selected_paths, ordered_metadata)

        if os.path.exists(metadata_path):
            os.remove(metadata_path)
            notify(f"Removed processed metadata yaml: {os.path.basename(metadata_path)}", "info")
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
        builds = conn.execute("SELECT id, version, build_type, language FROM builds WHERE vn_id = ?", (vn_id,)).fetchall()
        if not builds:
            notify("No builds found for this visual novel.", "warn")
            return

        for build in builds:
            lang = build['language'] or 'default'
            print(f"[{build['id']}] Version: {build['version']} - Language: {lang} - Type: {build['build_type']}")

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
            "SELECT version, build_type, language FROM builds WHERE id = ?",
            (build_id,)
        ).fetchone()

        if build_info:
            current_metadata["version"] = build_info["version"]
            current_metadata["build_type"] = build_info["build_type"]
            current_metadata["language"] = build_info["language"]

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

    prior_metadata_revision = get_current_metadata_version_number(vn_id=vn_id, build_id=build_id)

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
                """
                SELECT id FROM builds
                WHERE vn_id = ? AND version = ?
                  AND COALESCE(language, '') = COALESCE(?, '')
                  AND COALESCE(edition, '') = COALESCE(?, '')
                """,
                (
                    vn_id,
                    updated_metadata.get("version"),
                    updated_metadata.get("language"),
                    updated_metadata.get("edition")
                )
            ).fetchone()

        build_id = build_row["id"] if build_row else None
        metadata_version_number = get_current_metadata_version_number(vn_id=vn_id, build_id=build_id)
        next_metadata_revision = prior_metadata_revision + 1
        print()
        panel(f"Updated Metadata Copy (db v{metadata_version_number}, staged v{next_metadata_revision})")
        print(TEXT + yaml.dump(updated_metadata, sort_keys=False, allow_unicode=True))

        staged_path = stage_metadata_yaml_for_upload(updated_metadata, next_metadata_revision)

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

    upload_files = get_uploading_zip_files()

    if not upload_files:
        notify("No zip files found in the uploading directory root.", "error")
        return

    for i, path in enumerate(upload_files, 1):
        rel_path = os.path.relpath(path, UPLOADING_DIR)
        print(TEXT + f"[{i}] {rel_path}")

    print(TEXT + "[A] Upload all files in uploading/")

    choice = prompt("Select zip number, 'A' for all, or 0 to cancel: ")
    if choice == "0" or not choice:
        return

    def is_already_uploaded(file_path):
        file_hash = sha256_file(file_path)
        with get_connection() as conn:
            existing_obj = conn.execute(
                "SELECT 1 FROM archive_objects WHERE sha256 = ?",
                (file_hash,)
            ).fetchone()
        return bool(existing_obj)

    if choice.lower() == "a":
        uploaded_count = 0
        skipped_count = 0
        failed_count = 0

        for file_path in upload_files:
            if is_already_uploaded(file_path):
                notify(f"Skipping already uploaded file: {os.path.basename(file_path)}", "warn")
                skipped_count += 1
                continue

            if upload_archive(file_path):
                uploaded_count += 1
            else:
                failed_count += 1

        notify(
            f"Bulk upload complete — uploaded: {uploaded_count}, skipped: {skipped_count}, failed: {failed_count}",
            "ok" if failed_count == 0 else "warn"
        )
        return

    try:
        idx = int(choice) - 1
        if 0 <= idx < len(upload_files):
            selected_file = upload_files[idx]
            if is_already_uploaded(selected_file):
                notify(f"Skipping already uploaded file: {os.path.basename(selected_file)}", "warn")
                return
            upload_archive(selected_file)
        else:
            notify("Invalid selection.", "error")
    except ValueError:
        notify("Invalid input.", "error")


def get_uploading_zip_files():
    """Return zip files from the root of uploading/ sorted by filename."""
    return sorted([
        os.path.join(UPLOADING_DIR, entry)
        for entry in os.listdir(UPLOADING_DIR)
        if entry.lower().endswith(".zip")
        and os.path.isfile(os.path.join(UPLOADING_DIR, entry))
    ])


def is_archive_hash_uploaded(file_path):
    """True when a file's sha256 exists as a stored archive object."""
    file_hash = sha256_file(file_path)
    with get_connection() as conn:
        existing_obj = conn.execute(
            "SELECT 1 FROM archive_objects WHERE sha256 = ?",
            (file_hash,)
        ).fetchone()
    return bool(existing_obj)


def get_sidecar_metadata_files(zip_path):
    """Return staged metadata sidecars matching a zip stem in uploading/."""
    stem = Path(zip_path).stem
    directory = Path(zip_path).parent
    return sorted(directory.glob(f"{stem}_meta_v*.yaml"))


def delete_uploading_files():
    print()
    panel("Delete Files From Uploading")

    if not os.path.exists(UPLOADING_DIR):
        notify("Uploading directory does not exist.", "error")
        return

    print(TEXT + "[1] Choose a file and optional metadata sidecar(s) to delete")
    print(TEXT + "[2] Scan uploading/ and delete only archives already confirmed uploaded")
    print(TEXT + "[0] Cancel")

    mode = prompt("Select deletion mode: ")
    if mode in ("", "0"):
        return

    upload_files = get_uploading_zip_files()
    if not upload_files:
        notify("No zip files found in uploading/.", "warn")
        return

    if mode == "1":
        panel("Choose Zip File")
        for i, path in enumerate(upload_files, 1):
            print(TEXT + f"[{i}] {os.path.basename(path)}")

        selection = prompt("Select zip number, or 0 to cancel: ")
        if selection in ("", "0"):
            return

        try:
            idx = int(selection) - 1
            if idx < 0 or idx >= len(upload_files):
                notify("Invalid selection.", "error")
                return
        except ValueError:
            notify("Invalid input.", "error")
            return

        selected_zip = upload_files[idx]
        sidecars = get_sidecar_metadata_files(selected_zip)

        print()
        notify(f"Selected archive: {os.path.basename(selected_zip)}")
        if sidecars:
            notify("Matching metadata sidecars:")
            for sidecar in sidecars:
                print(TEXT + f"  - {sidecar.name}")
        else:
            notify("No matching metadata sidecars found.", "warn")

        print()
        print(TEXT + "[1] Delete archive only")
        print(TEXT + "[2] Delete metadata sidecar(s) only")
        print(TEXT + "[3] Delete archive + metadata sidecar(s)")
        print(TEXT + "[0] Cancel")
        delete_mode = prompt("Select what to delete: ")
        if delete_mode in ("", "0"):
            return

        to_delete = []
        if delete_mode == "1":
            to_delete = [Path(selected_zip)]
        elif delete_mode == "2":
            if not sidecars:
                notify("No sidecar metadata files to delete.", "warn")
                return
            to_delete = sidecars
        elif delete_mode == "3":
            to_delete = [Path(selected_zip), *sidecars]
        else:
            notify("Invalid option.", "error")
            return

        print()
        notify("The following files will be deleted:", "warn")
        for path_obj in to_delete:
            print(TEXT + f"  - {path_obj.name}")

        confirm = prompt("Type DELETE to confirm: ")
        if confirm != "DELETE":
            notify("Deletion cancelled.", "warn")
            return

        deleted = 0
        for path_obj in to_delete:
            if path_obj.exists() and path_obj.is_file():
                path_obj.unlink()
                deleted += 1

        notify(f"Deleted {deleted} file(s).", "ok")
        return

    if mode == "2":
        confirmed = []
        for file_path in upload_files:
            try:
                if is_archive_hash_uploaded(file_path):
                    confirmed.append(Path(file_path))
            except Exception as e:
                notify(f"Could not validate {os.path.basename(file_path)}: {e}", "warn")

        if not confirmed:
            notify("No archives in uploading/ are confirmed as uploaded.", "warn")
            return

        panel("Confirmed Uploaded Archives")
        for i, path_obj in enumerate(confirmed, 1):
            print(TEXT + f"[{i}] {path_obj.name}")

        print()
        print(TEXT + "[A] Delete all confirmed uploaded archives listed above")
        choice = prompt("Select number, 'A' for all, or 0 to cancel: ")
        if choice in ("", "0"):
            return

        to_delete = []
        if choice.lower() == "a":
            to_delete = confirmed
        else:
            try:
                idx = int(choice) - 1
                if idx < 0 or idx >= len(confirmed):
                    notify("Invalid selection.", "error")
                    return
                to_delete = [confirmed[idx]]
            except ValueError:
                notify("Invalid input.", "error")
                return

        print()
        notify("The following confirmed uploaded archive(s) will be deleted:", "warn")
        for path_obj in to_delete:
            print(TEXT + f"  - {path_obj.name}")

        confirm = prompt("Type DELETE to confirm: ")
        if confirm != "DELETE":
            notify("Deletion cancelled.", "warn")
            return

        for path_obj in to_delete:
            if path_obj.exists() and path_obj.is_file():
                path_obj.unlink()

        notify(f"Deleted {len(to_delete)} confirmed uploaded archive(s).", "ok")
        return

    notify("Invalid option.", "error")


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
        print(PRIMARY + "  5) Delete From Uploading")
        print(PRIMARY + "  6) Config")
        print(PRIMARY + "  7) Quit\n")

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
            delete_uploading_files()
        elif choice == "6":
            configure_metadata_template_version()
        elif choice == "7":
            print()
            panel("Goodbye", "Session closed")
            print()
            break
        else:
            notify("Invalid option.", "error")
            print()


if __name__ == "__main__":
    main()
