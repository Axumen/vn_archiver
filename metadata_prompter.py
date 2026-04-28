import os
import tempfile
import yaml
import subprocess
import shutil
from colorama import Fore

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
