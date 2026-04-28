import os
import yaml
import tempfile
import subprocess
from pathlib import Path
import json

from tui import (
    notify,
    prompt,
    panel,
    rule,
    notify_pipeline,
    TEXT,
    PRIMARY,
    WARNING,
    SUCCESS,
    Fore
)
from db_manager import get_connection
from ingestion_service import ingest_incoming_pair, attach_file_to_release_pipeline
from ingestion_repository import VnIngestionRepository
from utils import sha256_file
from staging import INCOMING_DIR, stage_metadata_yaml_for_upload, stage_ingested_files_for_upload
from template_service import (
    load_metadata_template,
    load_file_metadata_template,
    resolve_prompt_fields,
    resolve_prompt_field_groups,
    order_metadata_for_yaml,
    DEFAULT_METADATA_VERSION
)
from vn_archiver import (
    create_archive_from_metadata_file,
    insert_visual_novel,
    get_latest_metadata_for_title,
    finalize_archive_creation,
)
import settings_workflow
from metadata_prompter import (
    METADATA_LIST_FIELDS,
    FIELD_SUGGESTIONS,
    _is_empty_metadata_value,
    open_metadata_in_editor_with_defaults
)

def list_zips():
    return [f for f in os.listdir(INCOMING_DIR) if f.endswith(".zip")]

def list_metadata():
    return [f for f in os.listdir(INCOMING_DIR) if f.endswith(".yaml")]

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

    # 1. Gather data for all archives
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

    # 2. Prepare metadata (Prompt or Editor)
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

    # 3. Inject the multi-archive data
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

    active_version = settings_workflow.get_active_metadata_template_version()
    metadata_mode = "editor" if settings_workflow.METADATA_EDITOR_MODE else "prompt"
    
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
    failed_pairs: list[tuple[str, str]] = []

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
        
        release_metadata = dict(ordered_metadata)
        
        metadata_version = settings_workflow.get_active_metadata_template_version()
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
            
        try:
            ingest_result = ingest_incoming_pair(
                archive_path,
                release_metadata,
                file_metadata,
                raw_metadata_text=raw_metadata_text,
                source_file=metadata_path,
            )
        except Exception as exc:
            notify(f"Pipeline failed for '{archive_name}': {exc}", "error")
            failed_pairs.append((archive_name, str(exc)))
            continue

        notify_pipeline("2", f"Release created (release_id={ingest_result.release_id})", "ok")
        notify_pipeline("3", f"File attached (file_id={ingest_result.file_id})", "ok")
        if ingest_result.release_sidecar_path:
            notify_pipeline(
                "4",
                f"Staged release metadata sidecar: {Path(ingest_result.release_sidecar_path).name}",
                "ok",
            )

        for staged_path in ingest_result.staged_archives:
            notify_pipeline("5", f"Moved ingested archive to uploading: {staged_path.name}", "ok")
        if ingest_result.file_sidecar_path:
            notify_pipeline("6", f"Staged file metadata sidecar: {Path(ingest_result.file_sidecar_path).name}", "ok")

        notify_pipeline("7", f"Pipeline complete for pair.", "ok")
        
        if os.path.exists(metadata_path):
            os.remove(metadata_path)
            notify(f"Removed processed original YAML: {yaml_name}", "info")

    print()
    rule()
    total = len(pairs)
    succeeded = total - len(failed_pairs)
    if failed_pairs:
        notify(
            f"Batch complete: {succeeded}/{total} pair(s) succeeded, "
            f"{len(failed_pairs)} failed.",
            "warn",
        )
        for pair_name, reason in failed_pairs:
            notify(f"  {pair_name}: {reason}", "error")
    else:
        notify(f"Batch complete: all {total} pair(s) processed successfully.", "ok")
    rule()


def add_file_to_existing_release():
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
    metadata_version = settings_workflow.get_active_metadata_template_version()
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

    attach_result = attach_file_to_release_pipeline(selected_path, release_id, file_metadata)

    notify(f"Linked file '{selected_file}' to release_id={release_id}.", "ok")
    for staged_path in attach_result.staged_archives:
        notify(f"Moved ingested file to uploading: {staged_path.name}", "ok")
    if attach_result.file_sidecar_path:
        notify(f"Created metadata yaml copy: {Path(attach_result.file_sidecar_path).name}", "ok")

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

        active_version = settings_workflow.get_active_metadata_template_version()
        metadata_mode = "editor" if settings_workflow.METADATA_EDITOR_MODE else "prompt"
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
                else int(metadata.get("metadata_version") or settings_workflow.get_active_metadata_template_version())
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

def edit_metadata_only():
    conn = get_connection()
    try:
        repo = VnIngestionRepository(conn)
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

        row = repo.get_current_revision(release_id)

        if not row:
            notify("No metadata found in the database for this Visual Novel.", "error")
            return

        current_metadata = json.loads(row["metadata_json"])

        release_details = repo.list_revisions_for_release(release_id)
        release_info = release_details[0] if release_details else None

        if release_info:
            current_metadata["version"] = release_info["version"]
            current_metadata["build_type"] = release_info["release_type"]
            current_metadata["language"] = release_info["language"]

    finally:
        conn.close()

    print()
    panel("Current Metadata Review")
    print(TEXT + yaml.dump(current_metadata, sort_keys=False, allow_unicode=True))
    rule()

    confirm = prompt("Do you want to continue editing this metadata? [y/N]: ").lower()
    if confirm not in ("y", "yes"):
        notify("Editing cancelled.", "warn")
        return

    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False, encoding="utf-8") as tf:
        yaml.dump(current_metadata, tf, sort_keys=False, allow_unicode=True)
        temp_path = tf.name

    editor = os.environ.get('EDITOR', 'notepad' if os.name == 'nt' else 'nano')

    notify(f"Opening metadata in {editor}... Save and close the file when finished.")
    subprocess.call([editor, temp_path])

    try:
        with open(temp_path, "r", encoding="utf-8") as f:
            updated_metadata = yaml.safe_load(f)

        if updated_metadata == current_metadata:
            notify("No changes detected. Aborting update.", "warn")
            return

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
