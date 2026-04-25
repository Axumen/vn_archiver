#!/usr/bin/env python3

import os
import yaml
import shutil
import subprocess
import tempfile
import json
import re
from db_manager import initialize_database, get_connection
from domain_layer import VisualNovelDomainService
from ingestion_repository import VnIngestionRepository
from pathlib import Path
from colorama import init, Fore, Style
from b2 import upload_archive, upload_metadata_sidecar
from utils import sha256_file
from staging import (
    INCOMING_DIR,
    UPLOADING_DIR,
    stage_metadata_yaml_for_upload,
    stage_ingested_files_for_upload,
)
from template_service import (
    load_metadata_template,
    load_file_metadata_template,
    resolve_prompt_fields,
    resolve_prompt_field_groups,
    get_available_metadata_template_versions,
    detect_latest_metadata_template_version,
    order_metadata_for_yaml,
    AUTO_METADATA_FIELDS,
    DEFAULT_METADATA_VERSION,
)
from vn_archiver import (
    create_archive_from_metadata_file,
    insert_visual_novel,
    get_latest_metadata_for_title,
    finalize_archive_creation,
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


def notify_pipeline(stage, message, level="info"):
    notify(f"Stage {stage}: {message}", level)

# =============================
# METADATA PROMPTING
# =============================

METADATA_LIST_FIELDS = {"tags", "target_platform", "aliases", "developer", "publisher"}

FIELD_SUGGESTIONS = {
    "release_status": ["ongoing", "completed", "hiatus", "cancelled", "abandoned"],
    "distribution_model": ["free", "paid", "freemium", "donationware", "subscription", "patron_only"],
    "build_type": ["full", "demo", "trial", "alpha", "beta", "release-candidate", "patch", "dlc", "standalone"],
    "language": ["japanese", "english", "chinese-simplified", "chinese-traditional", "korean", "spanish", "german",
                 "french", "russian", "multi-language"],
    "distribution_platform": ["steam", "itch.io", "dlsite", "fanza", "gumroad", "patreon", "booth",
                              "self-distributed", "other"],
    "content_rating": ["all-ages", "teen", "mature", "18+", "unrated"],
    "content_mode": ["sfw", "nsfw", "selectable", "patchable", "mixed", "unknown"],
    "content_type": ["main_story", "story_expansion", "seasonal_event", "april_fools", "side_story", "non_canon_special"],
    "target_platform": ["windows", "linux", "mac", "android", "web", "ios", "switch"],
    "tags": [
        "romance", "drama", "comedy", "slice-of-life", "mystery", "horror", "sci-fi",
        "fantasy", "psychological", "thriller", "action", "historical", "supernatural",
        "nakige", "utsuge", "nukige", "moege", "dark", "wholesome", "tragic", "bittersweet",
        "school", "modern", "adult"
    ],
}


def _is_empty_metadata_value(value):
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, list):
        return len(value) == 0
    return False


def open_metadata_in_editor_with_defaults(initial_metadata):
    """Open metadata YAML in an editor, then parse and return it."""
    editor_candidates = []
    configured_editor = os.environ.get("VN_ARCHIVER_EDITOR") or os.environ.get("EDITOR")
    if configured_editor:
        editor_candidates.append(configured_editor)
    editor_candidates.extend(["notepad", "nano", "vi"])

    with tempfile.NamedTemporaryFile("w+", suffix=".yaml", delete=False, encoding="utf-8") as tmp:
        temp_path = tmp.name
        yaml.safe_dump(initial_metadata, tmp, sort_keys=False, allow_unicode=True)

    selected_editor = None
    for editor in editor_candidates:
        command_name = editor.split()[0]
        if shutil.which(command_name):
            selected_editor = editor
            break

    if not selected_editor:
        os.remove(temp_path)
        raise RuntimeError("No supported editor found. Install notepad/nano/vi or set VN_ARCHIVER_EDITOR.")

    print(Fore.CYAN + f"Opening metadata in editor: {selected_editor}")
    subprocess.run(f'{selected_editor} "{temp_path}"', shell=True, check=True)

    try:
        with open(temp_path, "r", encoding="utf-8") as f:
            parsed = yaml.safe_load(f) or {}
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)

    if not isinstance(parsed, dict):
        raise ValueError("Edited metadata must be a YAML object.")

    return parsed


def create_archive_only(
    archive_paths=None,
    metadata_version=DEFAULT_METADATA_VERSION,
    metadata_input_mode="prompt",
):
    if archive_paths is None:
        archive_paths = []
    elif isinstance(archive_paths, str):
        archive_paths = [archive_paths]

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
            "size_bytes": file_size,
            "sha256": sha256
        })

    # -------------------------------------------------------------------
    # 2. Prepare metadata (Prompt or Editor)
    # -------------------------------------------------------------------
    base_template = load_metadata_template(metadata_version)
    required_fields, optional_fields = resolve_prompt_field_groups(base_template)
    prompt_fields = required_fields + optional_fields

    metadata = {"metadata_version": metadata_version}
    defaults = {}

    if metadata_input_mode == "editor":
        template_defaults = base_template.get("defaults", {}) if isinstance(base_template.get("defaults"), dict) else {}
        metadata_editor_seed = {"metadata_version": metadata_version}

        preselected_title = input(
            Fore.YELLOW + "title (used to preload defaults before editor opens): "
        ).strip()
        if preselected_title:
            defaults = get_latest_metadata_for_title(preselected_title) or {}
            defaults.pop("archives", None)
            defaults.pop("metadata_version", None)
            if defaults:
                print(Fore.GREEN + f"Loaded defaults from latest metadata for '{preselected_title}' before editor launch.")

        for field in prompt_fields:
            default_value = template_defaults.get(field)
            if field in defaults and not _is_empty_metadata_value(defaults.get(field)):
                default_value = defaults.get(field)
            if default_value is None and field in METADATA_LIST_FIELDS:
                default_value = []
            if default_value is None:
                default_value = ""
            if field == "title" and preselected_title:
                default_value = preselected_title
            metadata_editor_seed[field] = default_value

        edited_metadata = None
        while True:
            try:
                edited_metadata = open_metadata_in_editor_with_defaults(metadata_editor_seed)
            except (RuntimeError, ValueError, subprocess.CalledProcessError) as exc:
                print(Fore.RED + f"Editor metadata mode failed: {exc}")
                retry_choice = input(
                    Fore.YELLOW + "Retry opening editor? [y/N]: "
                ).strip().lower()
                if retry_choice in ("y", "yes"):
                    continue
                return

            confirm_choice = input(
                Fore.YELLOW
                + "Use edited metadata? [Y]es / [E]dit again / [C]ancel: "
            ).strip().lower()
            if confirm_choice in ("", "y", "yes"):
                break
            if confirm_choice in ("e", "edit", "edit again"):
                continue
            if confirm_choice in ("c", "cancel", "n", "no"):
                print(Fore.YELLOW + "Metadata editor flow cancelled.")
                return
            print(Fore.YELLOW + "Invalid choice. Re-opening editor.")

        title_value = str(edited_metadata.get("title", "")).strip()
        if title_value:
            defaults = get_latest_metadata_for_title(title_value) or {}
            defaults.pop("archives", None)
            defaults.pop("metadata_version", None)
            if defaults:
                print(Fore.GREEN + f"Loaded defaults from latest metadata for '{title_value}'.")

        for field in prompt_fields:
            default_value = metadata_editor_seed.get(field)
            user_value = edited_metadata.get(field, default_value)
            if _is_empty_metadata_value(user_value):
                user_value = defaults.get(field, default_value)

            if field in METADATA_LIST_FIELDS and isinstance(user_value, str):
                user_value = [v.strip() for v in user_value.split(',') if v.strip()]

            if not _is_empty_metadata_value(user_value):
                metadata[field] = user_value
    else:
        print(Fore.MAGENTA + "\nFill Metadata (Press ENTER to skip optional fields)\n")
        print(Fore.CYAN + "Tip: when a [default] is shown, press ENTER to keep it, or type '-' to clear it.")

        print(Fore.GREEN + "\nRequired fields for a valid build:")
        for field in required_fields:
            default_val = defaults.get(field)
            prompt_text = f"{field} (required)"
            if default_val not in (None, ""):
                prompt_text += f" [{default_val}]"
            raw_val = input(Fore.YELLOW + f"{prompt_text}: ").strip()
            if raw_val:
                metadata[field] = raw_val
            elif default_val not in (None, ""):
                metadata[field] = default_val

        print(Fore.CYAN + "\nOptional fields and suggestions:")
        for field in optional_fields:
            if field in METADATA_LIST_FIELDS:
                suggestions = FIELD_SUGGESTIONS.get(field) or []
                if suggestions:
                    print(Fore.CYAN + f"Suggested {field}: " + ", ".join(suggestions))

                default_val = defaults.get(field)
                if isinstance(default_val, list):
                    default_items = [str(v).strip() for v in default_val if str(v).strip()]
                elif field in ("developer", "publisher") and isinstance(default_val, str):
                    default_items = [v.strip() for v in default_val.split(',') if v.strip()]
                else:
                    default_items = []

                default_display = ", ".join(default_items) if default_items else ""
                prompt_text = f"{field} (comma separated)"
                if default_display:
                    prompt_text += f" [{default_display}]"
                raw_val = input(Fore.YELLOW + f"{prompt_text}: ").strip()

                if raw_val == "-":
                    metadata[field] = []
                elif raw_val:
                    metadata[field] = [v.strip() for v in raw_val.split(',') if v.strip()]
                elif default_items:
                    metadata[field] = default_items

            else:
                suggestions = FIELD_SUGGESTIONS.get(field) or []
                if suggestions:
                    print(Fore.CYAN + f"Suggested {field}: " + ", ".join(suggestions))
                default_val = defaults.get(field)
                prompt_text = f"{field}"
                if default_val not in (None, ""):
                    prompt_text += f" [{default_val}]"
                raw_val = input(Fore.YELLOW + f"{prompt_text}: ").strip()

                if raw_val == "-":
                    metadata[field] = ""
                elif raw_val:
                    metadata[field] = raw_val
                    if field == "title":
                        defaults = get_latest_metadata_for_title(raw_val)
                        if defaults:
                            print(Fore.GREEN + f"Loaded defaults from latest metadata for '{raw_val}'.")
                            latest_known_version = defaults.get('version')
                            if latest_known_version not in (None, ""):
                                print(
                                    Fore.CYAN
                                    + f"Latest known version is '{latest_known_version}'. "
                                    "Press ENTER on version to reuse it, or type a new version."
                                )
                            defaults.pop('archives', None)
                            defaults.pop('metadata_version', None)
                elif default_val not in (None, ""):
                    metadata[field] = default_val

    # -------------------------------------------------------------------
    # 3. Inject the multi-archive data
    # -------------------------------------------------------------------
    if archives_data:
        archives_list = []
        for a in archives_data:
            archives_list.append({
                "filename": a.get("filename"),
                "size_bytes": a.get("size_bytes"),
                "sha256": a.get("sha256")
            })
        metadata["archives"] = archives_list

    try:
        finalize_archive_creation(metadata, archives_data)
    except ValueError as exc:
        notify(str(exc), "error")


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


def process_incoming_pairs():
    """Pair-processing submenu: paired ingest, release-only upsert, or file-to-release linking."""
    print()
    panel("Process Incoming")
    print(PRIMARY + "  1) Process Incoming Pairs (File + YAML)")
    print(PRIMARY + "  2) Create Release from Metadata YAML")
    print(PRIMARY + "  3) Add File to Existing Release")
    print(PRIMARY + "  4) Process & Ingest from File (Prompt/Editor)")
    print(PRIMARY + "  0) Back\n")

    mode = prompt("Select option: ")
    if mode in ("", "0"):
        return
    if mode == "1":
        _process_incoming_pairs()
        return
    if mode == "2":
        upsert_release_from_metadata_yaml()
        return
    if mode == "3":
        add_file_to_existing_release()
        return
    if mode == "4":
        process_incoming_with_prompt()
        return

    notify("Invalid option.", "error")


def process_incoming_with_prompt():
    print()
    panel("Process & Ingest from File (Prompt)")
    if not os.path.exists(INCOMING_DIR):
        os.makedirs(INCOMING_DIR)

    files = [f for f in os.listdir(INCOMING_DIR) if os.path.isfile(os.path.join(INCOMING_DIR, f)) and not f.lower().endswith((".yaml", ".yml"))]
    
    selected_paths = []
    if files:
        for i, filename in enumerate(files, 1):
            print(TEXT + f"[{i}] {filename}")
        print(TEXT + "[0] Skip (No archive file)")
        
        choice = prompt("Select file number to pair with, or 0 for none: ")
        if choice and choice != "0":
            try:
                idx = int(choice.strip()) - 1
                if 0 <= idx < len(files):
                    selected_paths.append(os.path.join(INCOMING_DIR, files[idx]))
                else:
                    notify("Invalid selection.", "error")
                    return
            except ValueError:
                notify("Invalid input.", "error")
                return
    else:
        notify(f"No archive files found in '{INCOMING_DIR}'. Generating standalone release.", "info")

    active_version = get_active_metadata_template_version()
    metadata_mode = "editor" if METADATA_EDITOR_MODE else "prompt"
    
    create_archive_only(
        archive_paths=selected_paths,
        metadata_version=active_version,
        metadata_input_mode=metadata_mode
    )


def _process_incoming_pairs():
    """Minimal processing workflow: pair incoming file + YAML by stem and ingest."""
    print()
    panel("Process Incoming Pairs (Minimal Workflow)")

    if not os.path.exists(INCOMING_DIR):
        os.makedirs(INCOMING_DIR)

    files = [
        f for f in os.listdir(INCOMING_DIR)
        if os.path.isfile(os.path.join(INCOMING_DIR, f))
    ]
    archive_files = [f for f in files if not f.lower().endswith((".yaml", ".yml"))]
    yaml_files = [f for f in files if f.lower().endswith((".yaml", ".yml"))]

    if not archive_files or not yaml_files:
        notify("No pairable incoming files found.", "error")
        notify("Required: place both archive file and YAML sidecar in incoming/.", "info")
        notify("Recommended pairing rule: same filename stem, different extension.", "info")
        return

    yaml_by_stem = {Path(name).stem: name for name in yaml_files}
    pairs = []
    for archive_name in sorted(archive_files):
        stem = Path(archive_name).stem
        matched_yaml = yaml_by_stem.get(stem)
        if matched_yaml:
            pairs.append((archive_name, matched_yaml))

    if not pairs:
        notify("No pairable files matched by filename stem.", "error")
        notify("Example: clannad_v1.0.zip + clannad_v1.0.yaml", "info")
        return

    notify(f"Found {len(pairs)} pair(s). Starting pipeline.", "ok")

    for archive_name, yaml_name in pairs:
        archive_path = os.path.join(INCOMING_DIR, archive_name)
        metadata_path = os.path.join(INCOMING_DIR, yaml_name)
        notify_pipeline("1", f"Ingest pair: {archive_name} + {yaml_name}")

        with open(metadata_path, "r", encoding="utf-8") as f:
            raw_metadata_text = f.read()
        parsed = yaml.safe_load(raw_metadata_text) or {}
        if not isinstance(parsed, dict):
            notify(f"Skipping pair '{archive_name}': metadata is not a YAML object.", "error")
            continue

        ordered_metadata = order_metadata_for_yaml(parsed)
        
        # --- 1. Process Release Metadata ---
        release_metadata = dict(ordered_metadata)
        release_metadata.pop("archives", None)
        release_metadata["_raw_text"] = raw_metadata_text
        release_metadata["_source_file"] = metadata_path
        
        try:
            result = insert_visual_novel(release_metadata)
            release_id = result.release_id
            notify_pipeline("2", f"Release created (release_id={release_id})", "ok")
        except Exception as exc:
            notify(f"Release creation failed for '{archive_name}': {exc}", "error")
            continue
            
        # --- 2. Process File Metadata ---
        from datetime import datetime as _dt, timezone as _tz
        file_sha = sha256_file(archive_path)
        file_size = os.path.getsize(archive_path)
        archived_at = _dt.now(_tz.utc).isoformat().replace('+00:00', 'Z')
        
        metadata_version = get_active_metadata_template_version()
        template = load_file_metadata_template(metadata_version)
        prompt_fields = resolve_prompt_fields(template)
        
        file_metadata = {
            "metadata_version": metadata_version,
            "title": release_metadata.get("title") or "",
            "version": release_metadata.get("version") or "",
            "build_type": release_metadata.get("build_type") or release_metadata.get("release_type") or "",
            "language": release_metadata.get("language") or "",
            "distribution_platform": release_metadata.get("distribution_platform") or "",
        }
        
        print()
        panel(f"File Metadata for {archive_name}")
        notify("Press Enter to keep defaults/blank for each field.", "info")
        for field_name in prompt_fields:
            default_value = file_metadata.get(field_name, "")
            entered_value = prompt(f"{field_name} [{default_value}]: ")
            file_metadata[field_name] = entered_value if entered_value else default_value
            
        file_metadata["archives"] = [{"filename": archive_name, "sha256": file_sha, "size_bytes": file_size}]
        
        # --- 3. Attach File to Release ---
        with get_connection() as conn:
            repo = VnIngestionRepository(conn)
            domain_service = VisualNovelDomainService(
                conn,
                repository=repo,
                collect_archives_for_db=lambda _: ([], None),
            )
            file_id = domain_service.attach_file_to_release(
                release_id=release_id,
                metadata={
                    **file_metadata,
                    "archived_at": archived_at,
                    "artifact_type": file_metadata.get("artifact_type"),
                },
                archive_data={
                    "sha256": file_sha,
                    "filename": archive_name,
                    "size_bytes": file_size,
                },
            )
        notify_pipeline("3", f"File attached (file_id={file_id})", "ok")
        
        # --- 4. Stage Release Metadata Sidecar ---
        release_sidecar_path = stage_metadata_yaml_for_upload(
            release_metadata,
            result.metadata_version_number,
            sha256=file_sha,
            release_id=release_id,
        )
        if release_sidecar_path:
            notify_pipeline("4", f"Staged release metadata sidecar: {Path(release_sidecar_path).name}", "ok")
            
        # --- 5. Stage File + File Metadata Sidecar ---
        staged_archives, file_sidecar_path = stage_ingested_files_for_upload(
            file_metadata,
            [
                {
                    "original_path": archive_path,
                    "filename": archive_name,
                    "sha256": file_sha,
                }
            ],
            metadata_version_number=int(file_metadata.get("metadata_version") or 1),
            release_id=release_id,
        )
        
        for staged_path in staged_archives:
            notify_pipeline("5", f"Moved ingested archive to uploading: {staged_path.name}", "ok")
        if file_sidecar_path:
            notify_pipeline("6", f"Staged file metadata sidecar: {Path(file_sidecar_path).name}", "ok")
            
        notify_pipeline("7", f"Pipeline complete for pair.", "ok")
        
        if os.path.exists(metadata_path):
            os.remove(metadata_path)
            notify(f"Removed processed original YAML: {yaml_name}", "info")


def add_file_to_existing_release():
    from datetime import datetime as _dt, timezone as _tz

    print()
    panel("Add File to Existing Release")

    if not os.path.exists(INCOMING_DIR):
        os.makedirs(INCOMING_DIR)

    candidate_files = sorted([
        f for f in os.listdir(INCOMING_DIR)
        if os.path.isfile(os.path.join(INCOMING_DIR, f))
        and not f.lower().endswith((".yaml", ".yml"))
    ])
    if not candidate_files:
        notify(f"No archive/artifact files found in '{INCOMING_DIR}' (excluding yaml).", "error")
        return

    panel("Select File")
    for i, filename in enumerate(candidate_files, 1):
        print(TEXT + f"[{i}] {filename}")

    file_choice = prompt("Select file number, or 0 to cancel: ")
    if file_choice in ("", "0"):
        return
    try:
        file_idx = int(file_choice) - 1
        if not (0 <= file_idx < len(candidate_files)):
            notify("Invalid file selection.", "error")
            return
    except ValueError:
        notify("Invalid input.", "error")
        return

    selected_file = candidate_files[file_idx]
    selected_path = os.path.join(INCOMING_DIR, selected_file)

    with get_connection() as conn:
        repo = VnIngestionRepository(conn)
        release_rows = repo.list_releases()

    if not release_rows:
        notify("No releases found. Create a release first (option 2).", "error")
        return

    panel("Select Release")
    for i, row in enumerate(release_rows, 1):
        print(
            TEXT
            + f"[{i}] {row['title']} | v{row['version']} | "
            + f"type={row['release_type'] or '-'} | lang={row['language'] or '-'} | "
            + f"platform={row['distribution_platform'] or '-'}"
        )

    release_choice = prompt("Select release number, or 0 to cancel: ")
    if release_choice in ("", "0"):
        return
    try:
        release_idx = int(release_choice) - 1
        if not (0 <= release_idx < len(release_rows)):
            notify("Invalid release selection.", "error")
            return
    except ValueError:
        notify("Invalid input.", "error")
        return

    release_id = int(release_rows[release_idx]["release_id"])
    selected_release = release_rows[release_idx]
    file_sha = sha256_file(selected_path)
    file_size = os.path.getsize(selected_path)
    archived_at = _dt.now(_tz.utc).isoformat().replace('+00:00', 'Z')

    metadata_version = get_active_metadata_template_version()
    template = load_file_metadata_template(metadata_version)
    prompt_fields = resolve_prompt_fields(template)
    file_metadata = {
        "metadata_version": metadata_version,
        "title": selected_release["title"] or "",
        "version": selected_release["version"] or "",
        "build_type": selected_release["release_type"] or "",
        "language": selected_release["language"] or "",
        "distribution_platform": selected_release["distribution_platform"] or "",
    }

    panel("Optional File Metadata (Template Fields)")
    notify("Press Enter to keep defaults/blank for each field.", "info")
    for field_name in prompt_fields:
        default_value = file_metadata.get(field_name, "")
        entered_value = prompt(f"{field_name} [{default_value}]: ")
        file_metadata[field_name] = entered_value if entered_value else default_value

    file_metadata["archives"] = [{"filename": selected_file, "sha256": file_sha}]

    with get_connection() as conn:
        repo = VnIngestionRepository(conn)
        domain_service = VisualNovelDomainService(
            conn,
            repository=repo,
            collect_archives_for_db=lambda _: ([], None),
        )
        file_id = domain_service.attach_file_to_release(
            release_id=release_id,
            metadata={
                **file_metadata,
                "archived_at": archived_at,
                "artifact_type": file_metadata.get("artifact_type"),
            },
            archive_data={
                "sha256": file_sha,
                "filename": selected_file,
                "size_bytes": file_size,
            },
        )

    notify(f"Linked file '{selected_file}' to release_id={release_id}.", "ok")
    staged_archives, _ = stage_ingested_files_for_upload(
        file_metadata,
        [
            {
                "original_path": selected_path,
                "filename": selected_file,
                "sha256": file_sha,
            }
        ],
        metadata_version_number=None,
        release_id=release_id,
    )
    staged_meta_path = stage_metadata_yaml_for_upload(
        file_metadata,
        int(file_metadata.get("metadata_version") or 1),
        release_id=release_id,
    )
    for staged_path in staged_archives:
        notify(f"Moved ingested file to uploading: {staged_path.name}", "ok")
    if staged_meta_path:
        notify(f"Created metadata yaml copy: {Path(staged_meta_path).name}", "ok")

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


def upsert_release_from_metadata_yaml():
    print()
    panel("Upsert Release/Title From Metadata YAML (No File Required)")

    if not os.path.exists(INCOMING_DIR):
        os.makedirs(INCOMING_DIR)

    yaml_files = sorted([f for f in os.listdir(INCOMING_DIR) if f.lower().endswith((".yaml", ".yml"))])
    if not yaml_files:
        notify(f"No metadata yaml files found in '{INCOMING_DIR}'.", "error")
        return

    for i, filename in enumerate(yaml_files, 1):
        print(TEXT + f"[{i}] {filename}")

    yaml_choice = prompt("Select metadata yaml number, or 0 to cancel: ")
    if yaml_choice in ("", "0"):
        return

    try:
        y_idx = int(yaml_choice) - 1
        if not (0 <= y_idx < len(yaml_files)):
            notify("Invalid metadata yaml selection.", "error")
            return
    except ValueError:
        notify("Invalid input.", "error")
        return

    metadata_path = os.path.join(INCOMING_DIR, yaml_files[y_idx])
    with open(metadata_path, "r", encoding="utf-8") as f:
        raw_metadata_text = f.read()
    metadata = yaml.safe_load(raw_metadata_text) or {}

    if not isinstance(metadata, dict):
        notify("Selected metadata yaml is not a valid object.", "error")
        return

    if not metadata.get("title") or not metadata.get("version"):
        notify("Metadata must include non-empty title and version.", "error")
        return


    metadata = order_metadata_for_yaml(metadata)
    try:
        metadata["_raw_text"] = raw_metadata_text
        metadata["_source_file"] = metadata_path
        result = insert_visual_novel(metadata)
        notify(f"Release/Title metadata upserted successfully (title_id={result.title_id}, release_id={result.release_id}).", "ok")
        staged_archives, staged_meta_path = stage_ingested_files_for_upload(
            metadata,
            [],
            metadata_version_number=(
                result.metadata_version_number
                if result.metadata_version_number is not None
                else int(metadata.get("metadata_version") or get_active_metadata_template_version())
            ),
            release_id=result.release_id,
        )
        if staged_archives:
            for staged_path in staged_archives:
                notify(f"Moved ingested file to uploading: {staged_path.name}", "ok")
        if staged_meta_path:
            notify(f"Staged metadata sidecar: {Path(staged_meta_path).name}", "ok")
        if os.path.exists(metadata_path):
            os.remove(metadata_path)
            notify(f"Removed processed metadata yaml: {os.path.basename(metadata_path)}", "info")
    except Exception as exc:
        notify(f"Release/Title upsert failed: {exc}", "error")
        return


def quick_process_with_metadata_yaml():
    print()
    panel("Quick Process from Metadata YAML (Archive/Artifact)")

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

        notify_pipeline("1", "Files ingested independently.")

        y_idx = int(yaml_choice) - 1
        if not (0 <= y_idx < len(yaml_files)):
            notify("Invalid metadata yaml selection.", "error")
            return

        metadata_path = os.path.join(INCOMING_DIR, yaml_files[y_idx])
        with open(metadata_path, "r", encoding="utf-8") as f:
            raw_metadata_text = f.read()
        metadata = yaml.safe_load(raw_metadata_text) or {}

        notify_pipeline("2", "Metadata parsed independently.")

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

        create_archive_from_metadata_file(
            selected_paths,
            ordered_metadata,
            raw_text=raw_metadata_text,
            source_file=metadata_path,
        )
        notify_pipeline("3", "File linked and metadata routed to VN/Build fields.", "ok")
        notify_pipeline("4", "Processing complete.", "ok")

        if os.path.exists(metadata_path):
            os.remove(metadata_path)
            notify(f"Removed processed metadata yaml: {os.path.basename(metadata_path)}", "info")
    except ValueError as exc:
        notify(f"Invalid input: {exc}", "error")





# =============================
# METADATA EDITING
# =============================

# Make sure to import insert_visual_novel if this is in tui.py

def edit_metadata_only():
    conn = get_connection()
    try:
        repo = VnIngestionRepository(conn)
        # 1. List available Titles
        print()
        panel("Select Title to Edit")
        release_rows = repo.list_releases()
        titles_by_id = {}
        for row in release_rows:
            title_id = row["title_id"]
            if title_id not in titles_by_id:
                titles_by_id[title_id] = row["title"]
        titles = [{"title_id": k, "title": v} for k, v in titles_by_id.items()]
        if not titles:
            notify("No titles in the database yet.", "warn")
            return

        for t in titles:
            print(f"[{t['title_id']}] {t['title']}")

        title_id_str = prompt("Enter Title ID to edit (or press Enter to cancel): ")
        if not title_id_str.isdigit():
            return
        title_id = int(title_id_str)

        # 2. List available releases for the selected title
        print()
        panel("Select Release to Edit")
        releases = [row for row in release_rows if row["title_id"] == title_id]
        if not releases:
            notify("No releases found for this title.", "warn")
            return

        for rel in releases:
            lang = rel['language'] or 'default'
            print(f"[{rel['release_id']}] Version: {rel['version']} - Language: {lang} - Type: {rel['release_type']}")

        release_id_str = prompt("Enter Release ID to edit (or press Enter to cancel): ")
        if not release_id_str.isdigit():
            return
        release_id = int(release_id_str)

        # 3. Fetch metadata for the specific build.
        # Prefer the canonical current metadata version first, then fall back to
        # archive-layer metadata for legacy rows, then VN-level metadata.
        row = repo.get_current_revision(release_id)

        if not row:
            notify("No metadata found in the database for this Visual Novel.", "error")
            return

        current_metadata = json.loads(row["metadata_json"])

        # Ensure release-specific fields reflect the selected release so the user
        # confirms/edits against the exact release context they chose.
        release_details = repo.list_revisions_for_release(release_id)
        release_info = release_details[0] if release_details else None

        if release_info:
            current_metadata["version"] = release_info["version"]
            current_metadata["build_type"] = release_info["release_type"]
            current_metadata["language"] = release_info["language"]

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
        result = insert_visual_novel(updated_metadata)
        notify("Metadata successfully updated!", "ok")

        build_id = result.release_id
        next_metadata_revision = result.metadata_version_number or 1
        print()
        panel(f"Updated Metadata Copy (staged v{next_metadata_revision})")
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
            kind = "file"
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
                    "SELECT 1 FROM cloud_archive WHERE sha256 = ?",
                    (file_hash,)
                ).fetchone()
            elif lower.endswith(('.yaml', '.yml')):
                existing_obj = conn.execute(
                    "SELECT 1 FROM cloud_sidecar WHERE sha256 = ?",
                    (file_hash,)
                ).fetchone()
            else:
                existing_obj = conn.execute(
                    "SELECT 1 FROM cloud_archive WHERE sha256 = ?",
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
                "SELECT 1 FROM cloud_archive WHERE sha256 = ?",
                (file_hash,)
            ).fetchone()
        elif lower.endswith((".yaml", ".yml")):
            existing_obj = conn.execute(
                "SELECT 1 FROM cloud_sidecar WHERE sha256 = ?",
                (file_hash,)
            ).fetchone()
        else:
            existing_obj = conn.execute(
                "SELECT 1 FROM cloud_archive WHERE sha256 = ?",
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
        print(PRIMARY + "  1) Process Incoming Pairs (File + YAML)")
        print(PRIMARY + "  2) Edit Metadata")
        print(PRIMARY + "  3) Upload Archive")
        print(PRIMARY + "  4) Delete From Uploading")
        print(PRIMARY + "  5) Config")
        print(PRIMARY + "  6) Quit\n")

        active_version = get_active_metadata_template_version()
        notify(f"Active metadata template: v{active_version}")
        mode_label = "Notepad/Editor mode" if METADATA_EDITOR_MODE else "Prompt mode"
        notify(f"Create Metadata mode: {mode_label}")
        notify("Minimal processing workflow: place matching file+yaml pairs in incoming/, then run option 1.", "info")
        print()

        choice = prompt("Select option: ")

        if choice == "1":
            process_incoming_pairs()
        elif choice == "2":
            edit_metadata_only()
        elif choice == "3":
            upload_archives()
        elif choice == "4":
            delete_uploading_files()
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
