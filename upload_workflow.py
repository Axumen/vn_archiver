import os
import re
from pathlib import Path

from tui import (
    notify,
    prompt,
    panel,
    TEXT,
)
from b2 import upload_archive, upload_metadata_sidecar
from staging import UPLOADING_DIR
from utils import sha256_file
from db_manager import get_connection

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
        from cloud_tracking_repository import CloudTrackingRepository
        lower = file_path.lower()
        file_hash = sha256_file(file_path)
        with get_connection() as conn:
            repo = CloudTrackingRepository(conn)
            if lower.endswith(('.yaml', '.yml')):
                return repo.is_sidecar_uploaded(file_hash)
            else:
                return repo.is_archive_uploaded(file_hash)

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
