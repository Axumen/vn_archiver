"""File staging, naming, and upload preparation for the VN Archiver.

This module owns the physical file workflow: naming archives for cloud
storage, moving files into the upload queue, and mirroring metadata
sidecars for rebuild.  It depends on ``utils`` for slugification and
hashing, and on ``db_manager`` for database access.

It does **not** import from ``vn_archiver`` — all metadata-specific
callbacks (e.g. field ordering) are accepted as optional parameters to
avoid circular dependencies.
"""

import shutil
import yaml
from pathlib import Path
from colorama import Fore

from db_manager import get_connection
from utils import (
    slugify_component,
    format_uploaded_component,
    determine_latest_version,
    table_exists,
)

# ==============================
# PATH CONSTANTS
# ==============================

INCOMING_DIR = "incoming"
UPLOADING_DIR = "uploading"
VN_ARCHIVE_DIR = "vn archive"
REBUILD_METADATA_DIR = "rebuild_metadata"


# ==============================
# NAMING
# ==============================

def build_recommended_archive_name(metadata, sha256, ext='.zip'):
    """Return a standardised archive filename: ``<title>_<version>_<hash><ext>``."""
    title_slug = slugify_component(metadata.get('title'), 'unknown')
    version_slug = slugify_component(metadata.get('version'), 'unknown')
    short_hash = (sha256 or 'nohash')[:8]
    safe_ext = ext if ext.startswith('.') else f'.{ext}'
    return f"{title_slug}_{version_slug}_{short_hash}{safe_ext}"


def build_recommended_metadata_name(metadata, sha256, metadata_version_number):
    """Return a standardised metadata sidecar filename."""
    title_slug = slugify_component(metadata.get('title'), 'unknown')
    version_slug = slugify_component(metadata.get('version'), 'unknown')
    short_hash = (sha256 or 'nohash')[:8]
    
    meta_type = "file" if metadata.get("artifact_type") else "release"
    return f"{title_slug}_{version_slug}_{short_hash}_{meta_type}_v{metadata_version_number}.yaml"


# ==============================
# UPLOAD QUEUE LAYOUT
# ==============================

def get_uploading_latest_dir(metadata):
    """Return the upload queue directory for a given metadata payload."""
    # Keep upload queue flat (no title/version folder structure required).
    return Path(UPLOADING_DIR)


# ==============================
# FILE STAGING
# ==============================

def stage_metadata_yaml_for_upload(metadata, metadata_version_number, target_dir=None, *, order_fn=None):
    """Create a metadata YAML sidecar and stage it in uploading/ with recommended naming.

    Parameters
    ----------
    metadata : dict
        The metadata payload to write.
    metadata_version_number : int
        Revision number used in the sidecar filename.
    target_dir : Path | str | None
        Override target directory.  Defaults to the upload queue root.
    order_fn : callable | None
        Optional callback ``(dict) -> dict`` that reorders metadata fields
        according to the active template.  When *None* the metadata is
        written in its current key order.
    """
    metadata_for_staging = dict(metadata or {})
    metadata_for_staging.pop("_raw_text", None)
    metadata_for_staging.pop("_source_file", None)

    meta_sha = metadata_for_staging.get('sha256')
    if not meta_sha and isinstance(metadata_for_staging.get('archives'), list) and metadata_for_staging['archives']:
        first_arch = metadata_for_staging['archives'][0]
        if isinstance(first_arch, dict):
            meta_sha = first_arch.get('sha256')

    final_name = build_recommended_metadata_name(metadata_for_staging, meta_sha, metadata_version_number)

    if target_dir is None:
        target_dir = get_uploading_latest_dir(metadata)
    target_dir = Path(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    temp_meta_path = target_dir / 'metadata.yaml'
    ordered_metadata = order_fn(metadata_for_staging) if order_fn is not None else metadata_for_staging
    with open(temp_meta_path, 'w', encoding='utf-8') as handle:
        yaml.dump(ordered_metadata, handle, sort_keys=False, allow_unicode=True)

    final_path = target_dir / final_name
    if final_path.exists():
        final_path.unlink()
    temp_meta_path.rename(final_path)
    return final_path


def stage_ingested_files_for_upload(metadata, archives_data, metadata_version_number=None, *, order_fn=None):
    """Move ingested archive files to uploading/ and stage metadata sidecar when available.

    Parameters
    ----------
    order_fn : callable | None
        Passed through to :func:`stage_metadata_yaml_for_upload`.
    """
    target_dir = Path(get_uploading_latest_dir(metadata))
    target_dir.mkdir(parents=True, exist_ok=True)

    staged_archives = []
    for archive_data in archives_data or []:
        source_path = archive_data.get("original_path") or archive_data.get("filepath")
        if not source_path:
            continue

        source = Path(source_path)
        if not source.exists() or not source.is_file():
            continue

        ext = source.suffix or Path(str(archive_data.get("filename") or "")).suffix or ".zip"
        staged_name = build_recommended_archive_name(metadata, archive_data.get("sha256"), ext=ext)
        destination = target_dir / staged_name

        try:
            same_file = source.resolve() == destination.resolve()
        except Exception:
            same_file = source == destination
        if same_file:
            staged_archives.append(destination)
            archive_data["staged_upload_path"] = str(destination)
            continue

        if destination.exists():
            destination.unlink()

        shutil.move(str(source), str(destination))
        staged_archives.append(destination)
        archive_data["staged_upload_path"] = str(destination)

    staged_meta_path = None
    if metadata_version_number is not None:
        staged_meta_path = stage_metadata_yaml_for_upload(
            metadata, metadata_version_number, target_dir=target_dir, order_fn=order_fn,
        )

    return staged_archives, staged_meta_path


# ==============================
# LOCAL ARCHIVE DIRECTORY
# ==============================

def get_vn_archive_version_dir(metadata):
    """Return (creating if needed) the versioned local archive directory for a title."""
    title = format_uploaded_component(metadata.get("title"), "Unknown Title")
    current_version = format_uploaded_component(metadata.get("version"), "unknown")

    title_root = Path(VN_ARCHIVE_DIR)
    title_root.mkdir(parents=True, exist_ok=True)

    sibling_versions = [current_version]
    existing_title_parent = None
    for entry in title_root.iterdir():
        if not entry.is_dir():
            continue
        prefix = f"{title} "
        if not entry.name.startswith(prefix):
            continue
        existing_title_parent = entry
        parent_version = entry.name[len(prefix):].strip()
        if parent_version:
            sibling_versions.append(parent_version)
        for child in entry.iterdir():
            if child.is_dir() and child.name:
                sibling_versions.append(child.name)
        break

    latest_version = determine_latest_version(sibling_versions)
    target_parent = title_root / f"{title} {latest_version}"

    if existing_title_parent and existing_title_parent != target_parent:
        if target_parent.exists():
            for child in existing_title_parent.iterdir():
                destination = target_parent / child.name
                if destination.exists():
                    if destination.is_dir():
                        shutil.rmtree(destination)
                    else:
                        destination.unlink(missing_ok=True)
                shutil.move(str(child), str(destination))
            existing_title_parent.rmdir()
        else:
            existing_title_parent.rename(target_parent)

    target_parent.mkdir(parents=True, exist_ok=True)
    target_version_dir = target_parent / current_version
    target_version_dir.mkdir(parents=True, exist_ok=True)
    return target_version_dir


# ==============================
# REBUILD METADATA MIRROR
# ==============================

def mirror_metadata_for_rebuild(staged_meta_path, archives_data, release_id):
    """Mirror staged sidecar metadata into rebuild_metadata/ with archive-id-prefixed names."""
    metadata_dir = Path(REBUILD_METADATA_DIR)
    metadata_dir.mkdir(parents=True, exist_ok=True)

    if not release_id:
        print(Fore.YELLOW + "[WARN] Rebuild metadata mirror skipped: missing release ID.")
        return []

    archive_id_by_sha = {}
    with get_connection() as conn:
        if table_exists(conn, "file") and table_exists(conn, "release_file"):
            rows = conn.execute(
                """
                SELECT f.file_id AS id, f.sha256 AS sha256
                FROM release_file rf
                JOIN file f ON f.file_id = rf.file_id
                WHERE rf.release_id = ?
                """,
                (release_id,),
            ).fetchall()
            for row in rows:
                archive_id_by_sha[str(row["sha256"]).strip().lower()] = int(row["id"])
        else:
            print(Fore.YELLOW + "[WARN] Rebuild metadata mirror skipped: missing file/release_file tables.")
            return []

    staged_name = Path(staged_meta_path).name
    mirrored_paths = []
    for archive in archives_data or []:
        archive_sha = str(archive.get("sha256") or "").strip().lower()
        archive_id = archive_id_by_sha.get(archive_sha)
        if not archive_id:
            print(Fore.YELLOW + f"[WARN] Could not resolve archive ID for metadata mirror ({archive_sha[:8]}...).")
            continue

        mirrored_path = metadata_dir / f"{archive_id}_{staged_name}"
        shutil.copy2(staged_meta_path, mirrored_path)
        mirrored_paths.append(mirrored_path)

    if mirrored_paths:
        print(Fore.GREEN + f"Mirrored metadata copies for rebuild: {len(mirrored_paths)} file(s).")
    return mirrored_paths
