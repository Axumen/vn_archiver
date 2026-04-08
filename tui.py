#!/usr/bin/env python3

import os
import yaml
import shutil
import subprocess
import tempfile
import json
import re
from db_manager import initialize_database, get_connection
from pathlib import Path
from colorama import init, Fore, Style
from vn_archiver import (
    create_archive_only,
    create_archive_from_metadata_file,
    upload_archive,
    upload_metadata_sidecar,
    INCOMING_DIR,
    UPLOADING_DIR,
    sha256_file,
    load_metadata_template,
    resolve_prompt_fields,
    get_available_metadata_template_versions,
    detect_latest_metadata_template_version,
    insert_visual_novel,
    get_latest_metadata_for_title,
    get_current_metadata_version_number,
    stage_metadata_yaml_for_upload,
    order_metadata_for_yaml,
    SUGGESTED_ARTIFACT_TYPE,
    DERIVED_ARTIFACT_TYPES,
    resolve_existing_build_for_artifact,
)

init(autoreset=True)

SELECTED_METADATA_TEMPLATE_VERSION = None
METADATA_EDITOR_MODE = False

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


def toggle_metadata_editor_mode():
    global METADATA_EDITOR_MODE
    METADATA_EDITOR_MODE = not METADATA_EDITOR_MODE
    mode_label = "Notepad/Editor mode" if METADATA_EDITOR_MODE else "Prompt mode"
    notify(f"Create Metadata input mode set to: {mode_label}.", "ok")
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
        metadata_mode = "editor" if METADATA_EDITOR_MODE else "prompt"
        create_archive_only(
            selected_paths,
            metadata_version=active_version,
            metadata_input_mode=metadata_mode
        )

    except ValueError:
        notify("Invalid input.", "error")


def quick_process_with_metadata_yaml():
    print()
    panel("Quick Process (Archive/Artifact + Metadata YAML)")

    if not os.path.exists(INCOMING_DIR):
        os.makedirs(INCOMING_DIR)

    candidate_files = sorted([
        f for f in os.listdir(INCOMING_DIR)
        if os.path.isfile(os.path.join(INCOMING_DIR, f))
        and not f.lower().endswith((".yaml", ".yml"))
    ])
    yaml_files = [f for f in os.listdir(INCOMING_DIR) if f.lower().endswith((".yaml", ".yml"))]

    if not candidate_files:
        notify(f"No archive/artifact files found in '{INCOMING_DIR}' (excluding yaml).", "error")
        return
    if not yaml_files:
        notify(f"No metadata yaml files found in '{INCOMING_DIR}'.", "error")
        return

    panel("Select Archive/Artifact file(s)")
    for i, filename in enumerate(candidate_files, 1):
        print(TEXT + f"[{i}] {filename}")

    file_choice = prompt("Select file numbers (comma-separated), or 0 to cancel: ")
    if file_choice in ("", "0"):
        return

    panel("Select Metadata YAML")
    for i, filename in enumerate(yaml_files, 1):
        print(TEXT + f"[{i}] {filename}")

    yaml_choice = prompt("Select metadata yaml number, or 0 to cancel: ")
    if yaml_choice in ("", "0"):
        return

    try:
        selected_paths = []
        indices = [int(idx.strip()) - 1 for idx in file_choice.split(",") if idx.strip().isdigit()]
        for idx in indices:
            if 0 <= idx < len(candidate_files):
                selected_filename = candidate_files[idx]
                show_file_info(selected_filename)
                selected_paths.append(os.path.join(INCOMING_DIR, selected_filename))
            else:
                notify(f"Invalid file selection: {idx + 1}", "error")
                return

        if not selected_paths:
            notify("No valid files selected.", "error")
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

        metadata_is_artifact = bool(str(metadata.get("artifact_type") or "").strip())
        if metadata_is_artifact:
            notify(
                "Artifact metadata detected in Quick Process YAML; preserving artifact_type and artifact linkage fields.",
                "info",
            )
            _validate_derived_artifact_base_reference(metadata)

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
                notify("Quick Process blocked: YAML sha256 does not match selected file(s).", "error")
                notify(f"Selected file sha256: {', '.join(sorted(selected_sha_set))}", "error")
                notify(f"YAML sha256: {', '.join(sorted(yaml_sha_set))}", "error")
                return
            notify("Confirmed: metadata YAML sha256 matches selected file(s).", "ok")
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


def process_artifact_with_metadata():
    print()
    panel("Process Artifact (Domain-Matched Metadata)")

    if not os.path.exists(INCOMING_DIR):
        os.makedirs(INCOMING_DIR)

    artifact_files = sorted([
        f for f in os.listdir(INCOMING_DIR)
        if os.path.isfile(os.path.join(INCOMING_DIR, f))
        and not f.lower().endswith((".yaml", ".yml"))
    ])

    if not artifact_files:
        notify(f"No artifact files found in '{INCOMING_DIR}' (zip/non-zip files, excluding yaml).", "error")
        return

    panel("Select Artifact File")
    for i, filename in enumerate(artifact_files, 1):
        print(TEXT + f"[{i}] {filename}")

    selection = prompt("Select artifact number, or 0 to cancel: ")
    if selection in ("", "0"):
        return

    try:
        idx = int(selection) - 1
    except ValueError:
        notify("Invalid input.", "error")
        return

    if idx < 0 or idx >= len(artifact_files):
        notify("Invalid artifact selection.", "error")
        return

    artifact_filename = artifact_files[idx]
    artifact_path = os.path.join(INCOMING_DIR, artifact_filename)
    show_file_info(artifact_filename)

    selected_build = _select_build_for_artifact_link()
    if not selected_build:
        return

    metadata = _prompt_artifact_linkage_metadata(selected_build)
    if metadata is None:
        return

    notify(f"Using selected build_id={selected_build['build_id']} for artifact linkage context.", "ok")

    create_archive_from_metadata_file([artifact_path], metadata)


def _select_build_for_artifact_link():
    title_input = prompt("title (search existing VN title): ")
    if not title_input:
        notify("title is required to list existing builds.", "error")
        return None

    with get_connection() as conn:
        build_rows = conn.execute(
            """
            SELECT
                b.id AS build_id,
                v.title AS vn_title,
                b.version,
                b.release_type,
                b.build_type,
                b.language,
                b.edition,
                b.distribution_platform
            FROM builds b
            JOIN visual_novels v ON v.id = b.vn_id
            WHERE TRIM(v.title) LIKE TRIM(?) COLLATE NOCASE
            ORDER BY v.title, b.version COLLATE NOCASE, b.id
            """,
            (f"%{title_input}%",)
        ).fetchall()

    if not build_rows:
        notify("No builds found for that title. Create metadata/build first, then add artifact.", "error")
        return None

    panel("Select Build For Artifact Link")
    for i, row in enumerate(build_rows, 1):
        lang = row["language"] or "default"
        release_type = row["release_type"] or row["build_type"] or "default"
        edition = row["edition"] or "default"
        platform = row["distribution_platform"] or "default"
        print(TEXT + f"[{i}] {row['vn_title']} | v{row['version']} | release_type={release_type} | lang={lang} | edition={edition} | platform={platform} | build_id={row['build_id']}")

    selection = prompt("Select build number, or 0 to cancel: ")
    if selection in ("", "0"):
        return None
    try:
        idx = int(selection) - 1
    except ValueError:
        notify("Invalid build selection.", "error")
        return None
    if idx < 0 or idx >= len(build_rows):
        notify("Invalid build selection.", "error")
        return None

    return build_rows[idx]


def _prompt_artifact_linkage_metadata(selected_build):
    title_input = selected_build["vn_title"]
    version_input = selected_build["version"]
    notify(f"Selected title/version: {title_input} v{version_input}", "info")

    panel("Optional Build-Context Fields (Template-Aligned)")
    default_build_type = selected_build["build_type"] or ""
    default_release_type = selected_build["release_type"] or default_build_type
    default_language = selected_build["language"] or ""
    default_edition = selected_build["edition"] or ""
    default_distribution_platform = selected_build["distribution_platform"] or ""

    build_type = prompt(f"build_type (optional, default: {default_build_type or 'empty'}): ") or default_build_type
    release_type = prompt(f"release_type (optional, default: {default_release_type or 'empty'}): ") or default_release_type
    language = prompt(f"language (optional, default: {default_language or 'empty'}): ") or default_language
    edition = prompt(f"edition (optional, default: {default_edition or 'empty'}): ") or default_edition
    distribution_platform = (
        prompt(f"distribution_platform (optional, default: {default_distribution_platform or 'empty'}): ")
        or default_distribution_platform
    )

    notify("Suggested artifact_type labels: " + ", ".join(SUGGESTED_ARTIFACT_TYPE), "info")
    artifact_type = prompt("artifact_type: ")
    if not artifact_type:
        artifact_type = "game_archive"
        notify("artifact_type not provided; defaulting to 'game_archive'.", "warn")

    base_artifact_sha256 = prompt("base_artifact_sha256 (optional, recommended for patch/mod/hotfix): ")
    base_artifact_filename = prompt("base_artifact_filename (optional fallback): ")

    artifact_release_date = prompt("artifact_release_date (optional, YYYY-MM-DD): ")

    notes = prompt("notes (optional): ")
    change_note = prompt("change_note (optional): ")

    metadata = {
        "metadata_version": get_active_metadata_template_version(),
        "title": title_input,
        "version": version_input,
        "build_type": build_type,
        "release_type": release_type,
        "distribution_platform": distribution_platform,
        "language": language,
        "edition": edition,
        "artifact_type": artifact_type,
        "base_artifact_sha256": base_artifact_sha256,
        "base_artifact_filename": base_artifact_filename,
        "release_date": artifact_release_date,
        "notes": notes,
        "change_note": change_note,
    }
    metadata = {k: v for k, v in metadata.items() if v not in ("", None)}

    _validate_derived_artifact_base_reference(metadata)

    try:
        with get_connection() as conn:
            _, resolved_build_id = resolve_existing_build_for_artifact(conn, metadata)
        if int(resolved_build_id) != int(selected_build["build_id"]):
            notify(
                f"Metadata resolved to build_id={resolved_build_id}, not selected build_id={selected_build['build_id']}. "
                "Using selected build context fields is recommended.",
                "warn",
            )
        else:
            notify(f"Confirmed metadata resolves selected build_id={resolved_build_id}.", "ok")
    except ValueError as exc:
        notify(str(exc), "error")
        notify("Adjust optional build context fields to match the selected build.", "warn")
        return None

    return metadata


def _validate_derived_artifact_base_reference(metadata):
    artifact_type_normalized = str(metadata.get("artifact_type") or "").strip().lower()
    base_sha = str(metadata.get("base_artifact_sha256") or "").strip()
    base_filename = str(metadata.get("base_artifact_filename") or "").strip()
    if artifact_type_normalized in DERIVED_ARTIFACT_TYPES and not (base_sha or base_filename):
        notify(
            "Derived artifact type detected. base_artifact_sha256/base_artifact_filename not provided; "
            "if multiple base archives exist on this build, ingestion may fail and ask for explicit base reference.",
            "warn",
        )
        return True
    return True


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
                  AND COALESCE(build_type, '') = COALESCE(?, '')
                  AND COALESCE(edition, '') = COALESCE(?, '')
                  AND COALESCE(distribution_platform, '') = COALESCE(?, '')
                """,
                (
                    vn_id,
                    updated_metadata.get("version"),
                    updated_metadata.get("language"),
                    updated_metadata.get("build_type"),
                    updated_metadata.get("edition"),
                    updated_metadata.get("distribution_platform")
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
    panel("Upload Queue")
    if not os.path.exists(UPLOADING_DIR):
        notify("Uploading directory does not exist.", "error")
        return

    upload_files = get_uploading_upload_files()

    if not upload_files:
        notify("No uploadable files found in the uploading directory root.", "error")
        return

    for i, path in enumerate(upload_files, 1):
        rel_path = os.path.relpath(path, UPLOADING_DIR)
        if rel_path.lower().endswith((".yaml", ".yml")):
            kind = "metadata"
        elif rel_path.lower().endswith(".zip"):
            kind = "archive"
        else:
            kind = "artifact"
        print(TEXT + f"[{i}] ({kind}) {rel_path}")

    print(TEXT + "[A] Upload all files in uploading/")

    choice = prompt("Select file number, 'A' for all, or 0 to cancel: ")
    if choice == "0" or not choice:
        return

    def is_already_uploaded(file_path):
        lower = file_path.lower()
        file_hash = sha256_file(file_path)
        with get_connection() as conn:
            if lower.endswith('.zip'):
                existing_obj = conn.execute(
                    "SELECT 1 FROM archive_objects WHERE sha256 = ?",
                    (file_hash,)
                ).fetchone()
            elif lower.endswith(('.yaml', '.yml')):
                existing_obj = conn.execute(
                    "SELECT 1 FROM metadata_file_objects WHERE sha256 = ?",
                    (file_hash,)
                ).fetchone()
            else:
                existing_obj = conn.execute(
                    "SELECT 1 FROM archive_objects WHERE sha256 = ?",
                    (file_hash,)
                ).fetchone()
        return bool(existing_obj)

    def dispatch_upload(file_path):
        lower = file_path.lower()
        if lower.endswith('.zip'):
            return upload_archive(file_path)
        if lower.endswith(('.yaml', '.yml')):
            return upload_metadata_sidecar(file_path)
        return upload_archive(file_path)

    if choice.lower() == "a":
        uploaded_count = 0
        skipped_count = 0
        failed_count = 0

        for file_path in upload_files:
            if is_already_uploaded(file_path):
                notify(f"Skipping already uploaded file: {os.path.basename(file_path)}", "warn")
                skipped_count += 1
                continue

            if dispatch_upload(file_path):
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
            dispatch_upload(selected_file)
        else:
            notify("Invalid selection.", "error")
    except ValueError:
        notify("Invalid input.", "error")


def get_uploading_upload_files():
    """Return uploadable files from uploading/ root.

    Uploadable includes:
      - metadata sidecars named *_meta_vN.yaml|yml
      - archives/artifacts that have a matching sidecar in the same directory
    """
    entries = [
        os.path.join(UPLOADING_DIR, entry)
        for entry in os.listdir(UPLOADING_DIR)
        if os.path.isfile(os.path.join(UPLOADING_DIR, entry))
    ]

    sidecar_pattern = re.compile(r"^(?P<stem>.+)_meta_v\d+\.ya?ml$", re.IGNORECASE)
    sidecar_stems = set()
    uploadable = []

    for path in entries:
        name = os.path.basename(path)
        match = sidecar_pattern.match(name)
        if match:
            uploadable.append(path)
            sidecar_stems.add(match.group("stem"))

    for path in entries:
        name = os.path.basename(path)
        if sidecar_pattern.match(name):
            continue
        if Path(name).stem in sidecar_stems:
            uploadable.append(path)

    return sorted(set(uploadable))


def is_upload_file_confirmed_uploaded(file_path):
    """True when a file is already present in DB object storage tables."""
    lower = str(file_path).lower()
    file_hash = sha256_file(file_path)
    with get_connection() as conn:
        if lower.endswith(".zip"):
            existing_obj = conn.execute(
                "SELECT 1 FROM archive_objects WHERE sha256 = ?",
                (file_hash,)
            ).fetchone()
        elif lower.endswith((".yaml", ".yml")):
            existing_obj = conn.execute(
                "SELECT 1 FROM metadata_file_objects WHERE sha256 = ?",
                (file_hash,)
            ).fetchone()
        else:
            existing_obj = conn.execute(
                "SELECT 1 FROM archive_objects WHERE sha256 = ?",
                (file_hash,)
            ).fetchone()
    return bool(existing_obj)


def get_sidecar_metadata_files(zip_path):
    """Return staged metadata sidecars matching a zip stem in uploading/."""
    stem = Path(zip_path).stem
    directory = Path(zip_path).parent
    pattern = re.compile(rf"^{re.escape(stem)}_meta_v\d+\.ya?ml$", re.IGNORECASE)
    return sorted([
        entry for entry in directory.iterdir()
        if entry.is_file() and pattern.match(entry.name)
    ])


def delete_uploading_files():
    print()
    panel("Delete Files From Uploading")

    if not os.path.exists(UPLOADING_DIR):
        notify("Uploading directory does not exist.", "error")
        return

    print(TEXT + "[1] Choose a file and optional metadata sidecar(s) to delete")
    print(TEXT + "[2] Scan uploading/ and delete only files already confirmed uploaded")
    print(TEXT + "[0] Cancel")

    mode = prompt("Select deletion mode: ")
    if mode in ("", "0"):
        return

    upload_files = [
        p for p in get_uploading_upload_files()
        if not p.lower().endswith((".yaml", ".yml"))
    ]
    if not upload_files:
        notify("No archive/artifact files with sidecar metadata found in uploading/.", "warn")
        return

    if mode == "1":
        panel("Choose Archive/Artifact File")
        for i, path in enumerate(upload_files, 1):
            print(TEXT + f"[{i}] {os.path.basename(path)}")

        selection = prompt("Select archive/artifact number, or 0 to cancel: ")
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
        upload_candidates = get_uploading_upload_files()
        if not upload_candidates:
            notify("No uploadable files found in uploading/.", "warn")
            return

        confirmed = []
        for file_path in upload_candidates:
            try:
                if is_upload_file_confirmed_uploaded(file_path):
                    confirmed.append(Path(file_path))
            except Exception as e:
                notify(f"Could not validate {os.path.basename(file_path)}: {e}", "warn")

        if not confirmed:
            notify("No uploadable files in uploading/ are confirmed as uploaded.", "warn")
            return

        panel("Confirmed Uploaded Files")
        for i, path_obj in enumerate(confirmed, 1):
            kind = "metadata" if path_obj.name.lower().endswith((".yaml", ".yml")) else "archive"
            print(TEXT + f"[{i}] ({kind}) {path_obj.name}")

        print()
        print(TEXT + "[A] Delete all confirmed uploaded files listed above")
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
        notify("The following confirmed uploaded file(s) will be deleted:", "warn")
        for path_obj in to_delete:
            print(TEXT + f"  - {path_obj.name}")

        confirm = prompt("Type DELETE to confirm: ")
        if confirm != "DELETE":
            notify("Deletion cancelled.", "warn")
            return

        for path_obj in to_delete:
            if path_obj.exists() and path_obj.is_file():
                path_obj.unlink()

        notify(f"Deleted {len(to_delete)} confirmed uploaded file(s).", "ok")
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
        print(PRIMARY + "  2) Quick Process (Archive/Artifact + Metadata YAML)")
        print(PRIMARY + "  3) Process Artifact (Minimal Metadata)")
        print(PRIMARY + "  4) Edit Metadata")
        print(PRIMARY + "  5) Upload Archive")
        print(PRIMARY + "  6) Delete From Uploading")
        print(PRIMARY + "  7) Config")
        print(PRIMARY + "  8) Toggle Metadata Editor Mode")
        print(PRIMARY + "  9) Quit\n")

        active_version = get_active_metadata_template_version()
        notify(f"Active metadata template: v{active_version}")
        mode_label = "Notepad/Editor mode" if METADATA_EDITOR_MODE else "Prompt mode"
        notify(f"Create Metadata mode: {mode_label}")
        print()

        choice = prompt("Select option: ")

        if choice == "1":
            create_metadata_only()
        elif choice == "2":
            quick_process_with_metadata_yaml()
        elif choice == "3":
            process_artifact_with_metadata()
        elif choice == "4":
            edit_metadata_only()
        elif choice == "5":
            upload_archives()
        elif choice == "6":
            delete_uploading_files()
        elif choice == "7":
            configure_metadata_template_version()
        elif choice == "8":
            toggle_metadata_editor_mode()
        elif choice == "9":
            print()
            panel("Goodbye", "Session closed")
            print()
            break
        else:
            notify("Invalid option.", "error")
            print()


if __name__ == "__main__":
    main()
