#!/usr/bin/env python3

import os
import shutil
from colorama import init, Fore, Style

from db_manager import initialize_database
from logger import configure_logging

init(autoreset=True)
configure_logging()

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

def header():
    print()
    panel("VN ARCHIVER SYSTEM", "Metadata + Archive Workflow Console")
    print()

# =============================
# MAIN MENU
# =============================

def main():
    initialize_database()

    # Import workflows here to avoid circular imports during initialization
    from ingest_workflow import process_incoming_pairs, edit_metadata_only
    from upload_workflow import upload_archives, delete_uploading_files
    from settings_workflow import config_menu, get_active_metadata_template_version, METADATA_EDITOR_MODE

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
            config_menu()
        elif choice == "6":
            notify("Exiting. Goodbye!", "ok")
            break
        else:
            notify("Invalid option. Please try again.", "error")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print()
        notify("Operation cancelled by user. Exiting.", "warn")
