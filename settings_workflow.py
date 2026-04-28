from tui import (
    notify,
    prompt,
    panel,
    TEXT,
    ACCENT,
    SUCCESS
)
from template_service import (
    load_metadata_template,
    resolve_prompt_fields,
    get_available_metadata_template_versions,
    detect_latest_metadata_template_version
)

SELECTED_METADATA_TEMPLATE_VERSION = None
METADATA_EDITOR_MODE = False

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

def config_menu():
    while True:
        print()
        panel("Configuration")
        print(TEXT + "  1) Select Metadata Template Version")
        print(TEXT + "  2) Toggle Create Metadata Mode (Prompt vs Editor)")
        print(TEXT + "  0) Back\n")

        choice = prompt("Select option: ")

        if choice == "1":
            configure_metadata_template_version()
        elif choice == "2":
            toggle_metadata_editor_mode()
        elif choice in ("", "0"):
            break
        else:
            notify("Invalid option.", "error")
