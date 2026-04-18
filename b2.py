import os
import sys
import time
import json
import hashlib
import re
from pathlib import Path
from tqdm import tqdm
import yaml
from colorama import Fore
from b2sdk.v2 import InMemoryAccountInfo, B2Api
from db_manager import get_connection
from utils import sha1_file, sha256_file, safe_json_serialize, slugify_component
from staging import build_recommended_archive_name
from vn_archiver import normalize_metadata_fields, normalize_text_list_value

B2_CONFIG_FILE = "backblaze_config.yaml"
B2_KEY_ID = None
B2_APPLICATION_KEY = None
B2_BUCKET_NAME = None

def _extract_remote_hashes(file_info_obj):
    """Best-effort extraction of remote object hashes from B2 file metadata."""
    file_info_map = getattr(file_info_obj, "file_info", None) or {}
    remote_sha1 = (
        getattr(file_info_obj, "content_sha1", None)
        or file_info_map.get("large_file_sha1")
        or file_info_map.get("src_sha1")
    )
    remote_sha256 = file_info_map.get("src_sha256")
    return (
        str(remote_sha1).strip().lower() if remote_sha1 else None,
        str(remote_sha256).strip().lower() if remote_sha256 else None,
    )


def verify_remote_upload_integrity(
    remote_info,
    local_size,
    local_sha1,
    local_sha256,
    label,
):
    """Verify cloud object integrity using size + cloud-available hash metadata."""
    remote_size = getattr(remote_info, "size", None)
    if remote_size is not None and int(remote_size) != int(local_size):
        print(
            Fore.RED
            + f"Post-upload verification failed for {label}: remote size {remote_size} does not match local size {local_size}."
        )
        return False

    remote_sha1, remote_sha256 = _extract_remote_hashes(remote_info)
    if remote_sha256:
        if remote_sha256 != local_sha256:
            print(
                Fore.RED
                + f"Post-upload verification failed for {label}: remote SHA-256 does not match local SHA-256."
            )
            return False
        print(Fore.GREEN + f"Verified {label} integrity via remote SHA-256.")
        return True

    if remote_sha1:
        if remote_sha1 != local_sha1:
            print(
                Fore.RED
                + f"Post-upload verification failed for {label}: remote SHA-1 does not match local SHA-1."
            )
            return False
        print(Fore.GREEN + f"Verified {label} integrity via remote SHA-1 (B2-compatible fallback).")
        return True

    print(Fore.YELLOW + f"Remote hash unavailable for {label}; verified size only.")
    return True

def load_b2_config(config_path=B2_CONFIG_FILE):
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Backblaze config file not found: {config_path}. "
            "Create it from the project template."
        )

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    key_id = config.get("key_id")
    application_key = config.get("application_key")
    bucket_name = config.get("bucket_name")
    dry_run = config.get("dry_run", True)

    missing_fields = [
        field_name
        for field_name, field_value in (
            ("key_id", key_id),
            ("application_key", application_key),
            ("bucket_name", bucket_name),
        )
        if not field_value
    ]

    if missing_fields:
        missing = ", ".join(missing_fields)
        raise ValueError(f"Missing Backblaze config field(s): {missing}")

    return key_id, application_key, bucket_name, bool(dry_run)

# BACKBLAZE
# ==============================
def get_b2_api():
    key_id, application_key, _, _ = load_b2_config()

    info = InMemoryAccountInfo()
    b2_api = B2Api(info)
    b2_api.authorize_account(
        "production",
        key_id,
        application_key
    )
    return b2_api


def upload_to_b2(filepath, remote_folder=None):
    """
    Upload file to Backblaze.
    dry_run in config prevents any real upload.
    """

    _, _, bucket_name, dry_run = load_b2_config()

    if not os.path.exists(filepath):
        raise Exception("File does not exist for upload.")

    filename = os.path.basename(filepath)

    if remote_folder:
        remote_name = f"{remote_folder}/{filename}"
    else:
        remote_name = filename

    # ---------------------------
    # DRY RUN (SAFE MODE)
    # ---------------------------
    if dry_run:
        print("\n[DRY RUN ENABLED]")
        print(f"Would upload:")
        print(f"  Local file : {filepath}")
        print(f"  Bucket     : {bucket_name}")
        print(f"  Remote path: {remote_name}")
        print("No upload performed.\n")
        return False

    # ---------------------------
    # CONFIRMATION (EXTRA SAFETY)
    # ---------------------------
    confirm = input(f"Upload '{remote_name}' to Backblaze? (yes/no): ").strip().lower()
    if confirm != "yes":
        print("Upload cancelled.")
        return False

    # ---------------------------
    # REAL UPLOAD
    # ---------------------------
    b2_api = get_b2_api()
    bucket = b2_api.get_bucket_by_name(bucket_name)

    file_size = os.path.getsize(filepath)

    class UploadProgressListener:
        """Advanced console progress display for Backblaze uploads."""

        def __init__(self, total_bytes):
            self.total_bytes = total_bytes
            self.start_time = time.time()
            self.last_update_time = 0

        def set_total_bytes(self, total_bytes):
            self.total_bytes = total_bytes

        def bytes_completed(self, byte_count):
            if self.total_bytes <= 0:
                return

            now = time.time()

            # Limit redraw frequency to avoid flicker
            if now - self.last_update_time < 0.1 and byte_count < self.total_bytes:
                return

            self.last_update_time = now

            percent = (byte_count / self.total_bytes) * 100
            elapsed = now - self.start_time
            speed = byte_count / elapsed if elapsed > 0 else 0

            bar_length = 30
            filled = int((byte_count / self.total_bytes) * bar_length)
            bar = "#" * filled + "-" * (bar_length - filled)

            sys.stdout.write(
                f"\rUploading: [{bar}] "
                f"{percent:6.2f}% "
                f"{byte_count / 1024 / 1024:8.2f}MB / {self.total_bytes / 1024 / 1024:8.2f}MB "
                f"{speed / 1024 / 1024:6.2f} MB/s"
            )
            sys.stdout.flush()

        def close(self):
            sys.stdout.write("\n")
            sys.stdout.flush()

    progress_listener = UploadProgressListener(file_size)

    bucket.upload_local_file(
        local_file=filepath,
        file_name=remote_name,
        progress_listener=progress_listener
    )

    progress_listener.close()

    print(f"Uploaded to Backblaze: {remote_name}")
    return True


def upload_archive(file_path):
    if not os.path.exists(file_path):
        print(Fore.RED + f"File not found: {file_path}")
        return False

    print(Fore.CYAN + f"\nAnalyzing {os.path.basename(file_path)}...")

    # -------------------------------------------------------------------
    # 1. Read metadata only from queued sidecar file
    # -------------------------------------------------------------------
    metadata = None
    metadata_source = None
    selected_sidecar = None

    archive_stem = Path(file_path).stem
    sidecar_dir = Path(file_path).parent
    sidecar_pattern = re.compile(rf"^{re.escape(archive_stem)}_meta_v\d+\.ya?ml$", re.IGNORECASE)
    sidecar_candidates = [
        candidate for candidate in sidecar_dir.iterdir()
        if candidate.is_file() and sidecar_pattern.match(candidate.name)
    ]

    def sidecar_sort_key(path_obj):
        match = re.search(r"_meta_v(\d+)\.ya?ml$", path_obj.name)
        numeric_version = int(match.group(1)) if match else -1
        return (numeric_version, path_obj.stat().st_mtime, path_obj.name)

    sidecar_candidates.sort(key=sidecar_sort_key)

    if sidecar_candidates:
        selected_sidecar = sidecar_candidates[-1]
        try:
            with open(selected_sidecar, 'r', encoding='utf-8') as handle:
                metadata = yaml.safe_load(handle)
                metadata_source = str(selected_sidecar)
        except Exception as e:
            print(Fore.RED + f"Upload Blocked: Failed to read sidecar metadata file '{selected_sidecar.name}': {e}")
            return False

    if not isinstance(metadata, dict):
        print(Fore.RED + "Upload Blocked: Could not find valid metadata sidecar file.")
        print(Fore.YELLOW + "Expected '<archive_name>_meta_vN.yaml' next to the archive in uploading/.")
        return False

    metadata = normalize_metadata_fields(metadata)

    print(Fore.CYAN + f"Metadata source: sidecar file ({metadata_source})")

    revision_match = re.search(r"_meta_v(\d+)\.ya?ml$", Path(metadata_source).name)
    requested_metadata_revision = int(revision_match.group(1)) if revision_match else None

    title = str(metadata.get("title", "")).strip()
    version = str(metadata.get("version", "")).strip()
    language = normalize_text_list_value(metadata.get("language")) or ""
    build_type = str(metadata.get("build_type", "")).strip()
    edition = str(metadata.get("edition", "")).strip()
    distribution_platform = str(metadata.get("distribution_platform", "")).strip()
    if not title:
        print(Fore.RED + "Upload Blocked: metadata sidecar is missing 'title'.")
        return False

    # -------------------------------------------------------------------
    # 2. Block upload if it wasn't inserted into the Database
    # -------------------------------------------------------------------
    title_id = None
    release_id = None
    with get_connection() as conn:
        title_row = conn.execute("SELECT title_id FROM title WHERE title = ?", (title,)).fetchone()
        if not title_row:
            print(Fore.RED + f"Upload Blocked: Visual Novel '{title}' does not exist in the database.")
            print(Fore.YELLOW + "Please run '(1) Create Metadata' to register it before uploading.")
            return False

        title_id = title_row["title_id"]

        if version:
            release_row = conn.execute(
                """
                SELECT release_id, version FROM release
                WHERE title_id = ? AND version = ?
                  AND COALESCE(language, '') = COALESCE(?, '')
                  AND COALESCE(build_type, '') = COALESCE(?, '')
                  AND COALESCE(edition, '') = COALESCE(?, '')
                  AND COALESCE(distribution_platform, '') = COALESCE(?, '')
                """,
                (title_id, version, language, build_type, edition, distribution_platform)
            ).fetchone()
            if not release_row:
                lang_label = language if language else "default"
                edition_label = edition if edition else "default"
                print(Fore.RED + f"Upload Blocked: Version '{version}' (language={lang_label}, edition={edition_label}) for '{title}' does not exist in the database.")
                print(Fore.YELLOW + "Please run '(1) Create Metadata' to register this release before uploading.")
                return False
        else:
            release_row = conn.execute(
                "SELECT release_id, version FROM release WHERE title_id = ? ORDER BY release_id DESC LIMIT 1",
                (title_id,)
            ).fetchone()
            if not release_row:
                print(Fore.RED + f"Upload Blocked: Visual Novel '{title}' has no releases in the database.")
                print(Fore.YELLOW + "Please run '(1) Create Metadata' to register a release before uploading.")
                return False
            version = str(release_row["version"]).strip()
            print(Fore.YELLOW + f"No version supplied in sidecar metadata; using latest DB release version: {version}")

        release_id = release_row["release_id"]

    # -------------------------------------------------------------------
    # 3. Validate sidecar metadata revision against DB metadata history
    # -------------------------------------------------------------------
    with get_connection() as conn:
        if requested_metadata_revision is not None:
            metadata_row = conn.execute(
                "SELECT raw_sha256 AS metadata_hash, version_number FROM revision WHERE release_id = ? AND version_number = ?",
                (release_id, requested_metadata_revision)
            ).fetchone()
        else:
            metadata_row = conn.execute(
                "SELECT raw_sha256 AS metadata_hash, version_number FROM revision WHERE release_id = ? AND is_current = 1",
                (release_id,)
            ).fetchone()

    if not metadata_row:
        if requested_metadata_revision is not None:
            print(Fore.RED + f"Upload Blocked: Release {release_id} has no metadata version v{requested_metadata_revision} in database.")
        else:
            print(Fore.RED + f"Upload Blocked: Release {release_id} has no current metadata version in database.")
        print(Fore.YELLOW + "Please run '(1) Create Metadata' or update metadata before uploading.")
        return False

    db_metadata_hash = metadata_row["metadata_hash"]
    db_version_number = metadata_row["version_number"]

    canonical_metadata_json = json.dumps(
        metadata,
        default=safe_json_serialize,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":")
    )
    sidecar_metadata_hash = hashlib.sha256(canonical_metadata_json.encode("utf-8")).hexdigest()
    if sidecar_metadata_hash != db_metadata_hash:
        print(Fore.RED + "Upload Blocked: Sidecar metadata does not match metadata stored in database for this revision.")
        print(Fore.YELLOW + f"DB metadata hash : {db_metadata_hash}")
        print(Fore.YELLOW + f"Sidecar hash     : {sidecar_metadata_hash}")
        print(Fore.YELLOW + "Regenerate/stage metadata so the sidecar matches the intended build metadata revision.")
        return False

    # -------------------------------------------------------------------
    # 4. Formulate cloud naming paths & hashes
    # -------------------------------------------------------------------
    title_slug = slugify_component(title, "unknown")
    version_slug = slugify_component(version, "unknown")

    print(Fore.CYAN + "Calculating archive SHA-256 for cloud verification...")
    archive_sha256 = sha256_file(file_path)

    ext = os.path.splitext(file_path)[1].lower()
    # Standardized naming for VN archives (title + build version + hash)
    file_name = build_recommended_archive_name(metadata, archive_sha256, ext=ext)
    metadata_file_name = Path(metadata_source).name

    cloud_path = f"archives/{title_slug}/t-{title_id:05d}/{version_slug}/{file_name}"
    metadata_cloud_path = f"metadata/{title_slug}/t-{title_id:05d}/{version_slug}/{metadata_file_name}"

    if db_version_number > 1:
        parent_metadata_cloud_path = re.sub(r"_meta_v\d+(\.ya?ml)$", f"_meta_v{db_version_number - 1}\\1", metadata_cloud_path)
        with get_connection() as conn:
            parent_uploaded_row = conn.execute(
                "SELECT 1 FROM cloud_sidecar WHERE storage_path = ?",
                (parent_metadata_cloud_path,)
            ).fetchone()
        if not parent_uploaded_row:
            print(Fore.RED + f"Upload Blocked: Parent metadata revision v{db_version_number - 1} is not uploaded yet.")
            print(Fore.YELLOW + f"Expected parent path: {parent_metadata_cloud_path}")
            return False

    print(Fore.GREEN + f"Database verification passed (Title ID: {title_id}, metadata v{db_version_number})")

    # Ensure queued local file uses the same recommended naming scheme
    current_name = os.path.basename(file_path)
    if current_name != file_name:
        renamed_local_path = os.path.join(os.path.dirname(file_path), file_name)
        if os.path.exists(renamed_local_path):
            os.remove(renamed_local_path)
        os.rename(file_path, renamed_local_path)
        file_path = renamed_local_path
        print(Fore.CYAN + f"Renamed queued archive to: {file_name}")

    file_size = os.path.getsize(file_path)

    # -------------------------------------------------------------------
    # 5. CAS Deduplication Check (archive object only)
    # -------------------------------------------------------------------
    with get_connection() as conn:
        existing_obj = conn.execute(
            "SELECT storage_path FROM cloud_archive WHERE sha256 = ?",
            (archive_sha256,)
        ).fetchone()

    archive_needs_upload = existing_obj is None
    if not archive_needs_upload:
        existing_cloud_path = existing_obj["storage_path"]
        print(Fore.GREEN + f"\n[DEDUPLICATION MATCH] Archive already exists in cloud!")
        print(Fore.CYAN + f"Existing Path: {existing_cloud_path}")
        print(Fore.YELLOW + "Skipping archive upload. Linking database records...")

    # -------------------------------------------------------------------
    # 6. Backblaze B2 Authentication via Config
    # -------------------------------------------------------------------
    try:
        key_id, app_key, bucket_name, dry_run = load_b2_config()

        info = InMemoryAccountInfo()
        api = B2Api(info)
        api.authorize_account("production", key_id, app_key)
        bucket = api.get_bucket_by_name(bucket_name)
    except Exception as e:
        print(Fore.RED + f"B2 Authentication failed: {e}")
        return False

    # -------------------------------------------------------------------
    # 7. Upload archive object (if not deduplicated)
    # -------------------------------------------------------------------
    if dry_run:
        if archive_needs_upload:
            print(Fore.YELLOW + f"[DRY RUN] Would upload archive {file_name} to: {cloud_path}")
        else:
            print(Fore.YELLOW + f"[DRY RUN] Archive already deduplicated at: {cloud_path}")
        print(Fore.YELLOW + f"[DRY RUN] Would upload metadata {metadata_file_name} to: {metadata_cloud_path}")
        return True

    if archive_needs_upload:
        archive_sha1 = sha1_file(file_path)
        print(Fore.CYAN + f"\nUploading Archive: {file_name}")
        print(Fore.CYAN + f"Destination      : {cloud_path}")

        with tqdm(total=file_size, unit='B', unit_scale=True, desc="Progress", colour="green") as pbar:
            class TqdmProgressListener:
                def set_total_bytes(self, total_bytes):
                    pass

                def bytes_completed(self, byte_count):
                    pbar.update(byte_count - pbar.n)

                def close(self):
                    pass

            try:
                bucket.upload_local_file(
                    local_file=str(file_path),
                    file_name=cloud_path,
                    file_infos={
                        "src_sha256": archive_sha256,
                        "src_sha1": archive_sha1,
                    },
                    progress_listener=TqdmProgressListener()
                )
            except Exception as e:
                print(Fore.RED + f"\nUpload failed for {file_name}: {e}")
                return False

        print(Fore.GREEN + "\nArchive upload complete!")

        try:
            uploaded_info = bucket.get_file_info_by_name(cloud_path)
        except Exception as e:
            print(Fore.RED + f"Post-upload verification failed for {cloud_path}: {e}")
            return False

        if not verify_remote_upload_integrity(
            remote_info=uploaded_info,
            local_size=file_size,
            local_sha1=archive_sha1,
            local_sha256=archive_sha256,
            label=f"archive {cloud_path}",
        ):
            return False

        print(Fore.GREEN + f"Verified remote archive object: {cloud_path}")

        with get_connection() as conn:
            try:
                conn.execute(
                    '''
                    INSERT OR IGNORE INTO cloud_archive (sha256, file_size, storage_path)
                    VALUES (?, ?, ?)
                    ''',
                    (archive_sha256, file_size, cloud_path)
                )
            except Exception as e:
                print(Fore.RED + f"Database update failed after upload verification: {e}")
                return False

    # -------------------------------------------------------------------
    # 8. Upload metadata sidecar object (with CAS dedup + DB record)
    # -------------------------------------------------------------------
    metadata_sha256 = sha256_file(selected_sidecar)
    metadata_local_size = os.path.getsize(selected_sidecar)

    with get_connection() as conn:
        existing_meta_obj = conn.execute(
            "SELECT storage_path FROM cloud_sidecar WHERE sha256 = ?",
            (metadata_sha256,)
        ).fetchone()

    metadata_needs_upload = existing_meta_obj is None
    if not metadata_needs_upload:
        existing_meta_path = existing_meta_obj["storage_path"]
        print(Fore.GREEN + f"\n[DEDUPLICATION MATCH] Metadata sidecar already exists in cloud!")
        print(Fore.CYAN + f"Existing Path: {existing_meta_path}")

    if metadata_needs_upload:
        metadata_sha1 = sha1_file(selected_sidecar)
        print(Fore.CYAN + f"\nUploading Metadata: {metadata_file_name}")
        print(Fore.CYAN + f"Destination       : {metadata_cloud_path}")
        try:
            bucket.upload_local_file(
                local_file=str(selected_sidecar),
                file_name=metadata_cloud_path,
                file_infos={
                    "src_sha256": metadata_sha256,
                    "src_sha1": metadata_sha1,
                },
            )
        except Exception as e:
            print(Fore.RED + f"Upload failed for metadata sidecar {metadata_file_name}: {e}")
            return False

        try:
            metadata_info = bucket.get_file_info_by_name(metadata_cloud_path)
        except Exception as e:
            print(Fore.RED + f"Post-upload verification failed for metadata {metadata_cloud_path}: {e}")
            return False

        if not verify_remote_upload_integrity(
            remote_info=metadata_info,
            local_size=metadata_local_size,
            local_sha1=metadata_sha1,
            local_sha256=metadata_sha256,
            label=f"metadata {metadata_cloud_path}",
        ):
            return False

        print(Fore.GREEN + f"Metadata upload complete: {metadata_cloud_path}")

    with get_connection() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO cloud_sidecar (sha256, file_size, storage_path) VALUES (?, ?, ?)",
            (metadata_sha256, metadata_local_size, metadata_cloud_path)
        )

    return True


def upload_metadata_sidecar(sidecar_path):
    """Upload a metadata sidecar file independently of archive upload."""
    if not os.path.exists(sidecar_path):
        print(Fore.RED + f"Metadata sidecar not found: {sidecar_path}")
        return False

    sidecar_file = Path(sidecar_path)
    if not re.search(r"_meta_v\d+\.ya?ml$", sidecar_file.name):
        print(Fore.RED + "Upload Blocked: Metadata sidecar filename must follow '<archive_name>_meta_vN.yaml'.")
        return False

    try:
        with open(sidecar_file, 'r', encoding='utf-8') as handle:
            metadata = yaml.safe_load(handle)
    except Exception as e:
        print(Fore.RED + f"Upload Blocked: Failed to read metadata sidecar '{sidecar_file.name}': {e}")
        return False

    if not isinstance(metadata, dict):
        print(Fore.RED + "Upload Blocked: Metadata sidecar is not a valid YAML mapping.")
        return False

    metadata = normalize_metadata_fields(metadata)

    revision_match = re.search(r"_meta_v(\d+)\.ya?ml$", sidecar_file.name)
    requested_metadata_revision = int(revision_match.group(1)) if revision_match else None

    title = str(metadata.get("title", "")).strip()
    version = str(metadata.get("version", "")).strip()
    language = normalize_text_list_value(metadata.get("language")) or ""
    build_type = str(metadata.get("build_type", "")).strip()
    edition = str(metadata.get("edition", "")).strip()
    distribution_platform = str(metadata.get("distribution_platform", "")).strip()
    if not title:
        print(Fore.RED + "Upload Blocked: metadata sidecar is missing 'title'.")
        return False

    title_id = None
    release_id = None
    with get_connection() as conn:
        title_row = conn.execute("SELECT title_id FROM title WHERE title = ?", (title,)).fetchone()
        if not title_row:
            print(Fore.RED + f"Upload Blocked: Visual Novel '{title}' does not exist in the database.")
            return False

        title_id = title_row["title_id"]

        if version:
            release_row = conn.execute(
                """
                SELECT release_id, version FROM release
                WHERE title_id = ? AND version = ?
                  AND COALESCE(language, '') = COALESCE(?, '')
                  AND COALESCE(build_type, '') = COALESCE(?, '')
                  AND COALESCE(edition, '') = COALESCE(?, '')
                  AND COALESCE(distribution_platform, '') = COALESCE(?, '')
                """,
                (title_id, version, language, build_type, edition, distribution_platform)
            ).fetchone()
            if not release_row:
                print(Fore.RED + f"Upload Blocked: Version '{version}' for '{title}' does not exist in the database.")
                return False
        else:
            release_row = conn.execute(
                "SELECT release_id, version FROM release WHERE title_id = ? ORDER BY release_id DESC LIMIT 1",
                (title_id,)
            ).fetchone()
            if not release_row:
                print(Fore.RED + f"Upload Blocked: Visual Novel '{title}' has no releases in the database.")
                return False
            version = str(release_row["version"]).strip()
            print(Fore.YELLOW + f"No version supplied in sidecar metadata; using latest DB release version: {version}")

        release_id = release_row["release_id"]

    with get_connection() as conn:
        if requested_metadata_revision is not None:
            metadata_row = conn.execute(
                "SELECT raw_sha256 AS metadata_hash, version_number FROM revision WHERE release_id = ? AND version_number = ?",
                (release_id, requested_metadata_revision)
            ).fetchone()
        else:
            metadata_row = conn.execute(
                "SELECT raw_sha256 AS metadata_hash, version_number FROM revision WHERE release_id = ? AND is_current = 1",
                (release_id,)
            ).fetchone()

    if not metadata_row:
        print(Fore.RED + f"Upload Blocked: No matching metadata version found in database for release {release_id}.")
        return False

    db_metadata_hash = metadata_row["metadata_hash"]
    db_version_number = metadata_row["version_number"]
    canonical_metadata_json = json.dumps(
        metadata,
        default=safe_json_serialize,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":")
    )
    sidecar_metadata_hash = hashlib.sha256(canonical_metadata_json.encode("utf-8")).hexdigest()
    if sidecar_metadata_hash != db_metadata_hash:
        print(Fore.RED + "Upload Blocked: Sidecar metadata does not match metadata stored in database for this revision.")
        print(Fore.YELLOW + f"DB metadata hash : {db_metadata_hash}")
        print(Fore.YELLOW + f"Sidecar hash     : {sidecar_metadata_hash}")
        return False

    title_slug = slugify_component(title, "unknown")
    version_slug = slugify_component(version, "unknown")
    metadata_file_name = sidecar_file.name
    metadata_cloud_path = f"metadata/{title_slug}/t-{title_id:05d}/{version_slug}/{metadata_file_name}"

    if db_version_number > 1:
        parent_metadata_cloud_path = re.sub(r"_meta_v\d+(\.ya?ml)$", f"_meta_v{db_version_number - 1}\\1", metadata_cloud_path)
        with get_connection() as conn:
            parent_uploaded_row = conn.execute(
                "SELECT 1 FROM cloud_sidecar WHERE storage_path = ?",
                (parent_metadata_cloud_path,)
            ).fetchone()
        if not parent_uploaded_row:
            print(Fore.RED + f"Upload Blocked: Parent metadata revision v{db_version_number - 1} is not uploaded yet.")
            print(Fore.YELLOW + f"Expected parent path: {parent_metadata_cloud_path}")
            return False

    metadata_sha256 = sha256_file(sidecar_file)
    metadata_local_size = os.path.getsize(sidecar_file)

    with get_connection() as conn:
        existing_meta_obj = conn.execute(
            "SELECT storage_path FROM cloud_sidecar WHERE sha256 = ?",
            (metadata_sha256,)
        ).fetchone()

    metadata_needs_upload = existing_meta_obj is None

    try:
        key_id, app_key, bucket_name, dry_run = load_b2_config()
        info = InMemoryAccountInfo()
        api = B2Api(info)
        api.authorize_account("production", key_id, app_key)
        bucket = api.get_bucket_by_name(bucket_name)
    except Exception as e:
        print(Fore.RED + f"B2 Authentication failed: {e}")
        return False

    if dry_run:
        if metadata_needs_upload:
            print(Fore.YELLOW + f"[DRY RUN] Would upload metadata {metadata_file_name} to: {metadata_cloud_path}")
        else:
            print(Fore.YELLOW + f"[DRY RUN] Metadata already deduplicated at: {metadata_cloud_path}")
        return True

    if metadata_needs_upload:
        metadata_sha1 = sha1_file(sidecar_file)
        print(Fore.CYAN + f"\nUploading Metadata: {metadata_file_name}")
        print(Fore.CYAN + f"Destination       : {metadata_cloud_path}")
        try:
            bucket.upload_local_file(
                local_file=str(sidecar_file),
                file_name=metadata_cloud_path,
                file_infos={
                    "src_sha256": metadata_sha256,
                    "src_sha1": metadata_sha1,
                },
            )
        except Exception as e:
            print(Fore.RED + f"Upload failed for metadata sidecar {metadata_file_name}: {e}")
            return False

        try:
            metadata_info = bucket.get_file_info_by_name(metadata_cloud_path)
        except Exception as e:
            print(Fore.RED + f"Post-upload verification failed for metadata {metadata_cloud_path}: {e}")
            return False

        if not verify_remote_upload_integrity(
            remote_info=metadata_info,
            local_size=metadata_local_size,
            local_sha1=metadata_sha1,
            local_sha256=metadata_sha256,
            label=f"metadata {metadata_cloud_path}",
        ):
            return False

    with get_connection() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO cloud_sidecar (sha256, file_size, storage_path) VALUES (?, ?, ?)",
            (metadata_sha256, metadata_local_size, metadata_cloud_path)
        )

    print(Fore.GREEN + f"Metadata upload complete: {metadata_cloud_path}")
    return True
