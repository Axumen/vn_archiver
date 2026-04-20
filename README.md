vn_archiver is an automatic visual novel archiver and collection organizer.

Main Menu
<img width="1482" height="444" alt="image" src="https://github.com/user-attachments/assets/7442c18c-92a6-48a3-8e0a-c0d4ad3e796c" />

Metadata Entry 
<img width="1482" height="987" alt="image" src="https://github.com/user-attachments/assets/864a261c-1d1e-482d-9846-672a124c697d" />



## Developer / publisher formatting

Yes — comma-separated values in one line are fine for `developer` and `publisher` (for example: `developer: "Studio A, Studio B"`).
You can also use YAML list style (`-`) and vn_archiver will normalize it into a comma-separated text value before storing it in the database. In interactive metadata entry, `developer`/`publisher` now also accept comma-separated input the same way list-style fields are parsed.

```yaml
developer: "Studio A, Studio B"
# or
developer:
  - "Studio A"
  - "Studio B"
publisher: "Publisher X"
```

## Content mode metadata

Use `content_mode` to describe how adult content is presented without using a boolean field.
Suggested values: `sfw`, `nsfw`, `selectable`, `patchable`, `mixed`, `unknown`.

```yaml
content_rating: "18+"
content_mode: "selectable"
```

## Content type metadata (Title release flavor)

Use `content_type` to describe the release/story flavor for Title metadata.
Suggested values: `main_story`, `story_expansion`, `seasonal_event`, `april_fools`, `side_story`, `non_canon_special`.

```yaml
content_type: "seasonal_event"
```

## Metadata capitalization guidance

Capitalization is mostly a data-quality recommendation (not a strict parser rule).

- **Keep proper capitalization** for display/name fields:
  - `title`, `series`, `developer`, `publisher`, `parent_title`
  - free-text fields like `description`, `notes`, `change_note`
  - `release_status`, `distribution_model`, `release_type`, `distribution_platform`
  - `content_rating`, `content_mode`, `relationship_type`, `artifact_type`

Example:

```yaml
title: "Fate/Stay Night"
developer: "Type-Moon"
relationship_type: "spinoff"
content_mode: "selectable"
```

## Metadata validation behavior

Before DB insertion, vn_archiver now performs strict contract validation against metadata templates:
- required fields from the selected template must be present and non-empty
- unknown top-level fields are rejected
- date fields (`original_release_date`, `release_date`) must use `YYYY-MM-DD`

## Artifact type metadata

When processing a non-runnable artifact, set `artifact_type` using these suggested labels:
`base_game`, `game_archive`, `patch`, `mod`, `hotfix`, `translation_patch`,
`instructions`, `readme`, `manual`, `soundtrack`, `bonus`, `checksum`.


`Process Artifact` in the TUI accepts both `.zip` and non-zip artifact files (YAML files are excluded).
It now requires entering a title first, then selecting an existing release from the database so the artifact is linked to a specific release.

Artifact records are normalized in the `file` and `release_file` tables and linked to their parent `release` row.
Current core columns: `file_id`, `release_id`, `artifact_type`, `filename`, `sha256`,
`base_artifact_id`, `release_date`, `notes`, `created_at`.
Artifact sidecars use the same metadata object/version handling as other metadata sidecars.
Use `metadata/metadata_artifact_v1.yaml` as a baseline template for artifact-focused sidecars.

Uploaded artifacts are explicitly linked to content-addressed file objects through
`file.sha256 -> cloud_archive.sha256`.

```yaml
artifact_type: "patch"
```

## Translator metadata for multi-language releases

The `translator` field supports three formats:

```yaml
# single translator/group
translator: "Sekai Project"

# multiple translators for one release language
translator:
  - "Alice"
  - "Bob"

# translators grouped per language
translator:
  english:
    - "Alice"
    - "Bob"
  spanish:
    - "Carlos"
  japanese: "Original team"
```

When a list/map is used, vn_archiver stores it in the `release.translator` text column as JSON while preserving the full value in metadata history. Stored `revision.raw_json` keeps template-style field ordering for readability/export consistency while version hashing remains canonical.


## Upload format (separate archive + metadata sidecar)

Uploads now use a strict separation model:
- the Title archive file is uploaded as the cloud object
- metadata must be provided as a sidecar YAML file in `uploading/` with the pattern `<archive_name>_meta_vN.yaml` (where `vN` is the metadata revision number)
- archive upload path format: `archives/{title_slug}/title-{title_id:05d}/{version_slug}/{archive_file_name}`
- metadata upload path format: `metadata/{title_slug}/title-{title_id:05d}/{version_slug}/{sidecar_file_name}`
- upload menu accepts both VN archive files (`.zip`) and metadata sidecar files (`*_meta_vN.yaml`)
- upload requires sidecar metadata to match the corresponding metadata revision stored in database
- when uploading metadata revision `vN` (N > 1), the parent metadata revision `vN-1` must already be uploaded

Embedded `metadata.yaml` inside archives is no longer used by the upload pipeline.

## Undoing a mistaken metadata/create entry

Use `metadata_rollback_tool.py` to manage revisions and, if needed, remove the full created release entry.

### If you only want to undo the latest metadata edit
Use rollback (non-destructive history pointer move):

```bash
python metadata_rollback_tool.py --release-id 7 rollback --backup
```

### If you want it to look like the create never happened
Use hard undo (release-level removal):

```bash
# Removes the release row and cascaded release-linked rows
python metadata_rollback_tool.py --release-id 7 undo-release-create --backup

# Keep Title row even if that was its only release
python metadata_rollback_tool.py --release-id 7 undo-release-create --backup --keep-empty-title

# Preview affected rows without changing archive.db
python metadata_rollback_tool.py --release-id 7 undo-release-create --dry-run
```

`undo-release-create` performs:
- delete from `release` for that release id (which cascades to `file`, `release_file`, and `revision`)
- optional deletion of the now-empty `title` row (default behavior)
- cleanup of orphaned `tag` and `series`

### Targeting entries
```bash
# By release id
python metadata_rollback_tool.py --release-id 7 list

# Delete only the newest metadata version for this release
python metadata_rollback_tool.py --release-id 7 delete-version --latest --backup

# Undo latest update entry on an existing release (removes newest metadata version + newest archive row)
python metadata_rollback_tool.py --release-id 7 undo-latest-entry --backup

# By title/version
python metadata_rollback_tool.py --title "My VN" --version "1.2" list
```


Schema behavior note:
- Deleting the last `release_file` row for a release now automatically deletes that `release` row (which then cascades to release-linked tables via existing foreign keys).
- Deleting `revision` rows now automatically prunes orphaned `file_snapshot` rows via trigger.
- `undo-latest-entry` is intended for existing releases with at least 2 archives and 2 metadata versions; it avoids deleting the only archive row to prevent accidental release removal.

Safety files created when `--backup` is used:
- `db_backups/archive_backup_<timestamp>.db`

## Can `archive.db` be regenerated from `revision.raw_json`?

Partially. The JSON blobs in `revision.raw_json` are sufficient to reconstruct
most normalized metadata tables by re-feeding each blob through `insert_visual_novel()` in
`vn_archiver.py`, because that path upserts series/title/release/tag/platform rows and
re-materializes metadata history entries.

However, this is **not** a full-fidelity rebuild of every table:
- Auto-generated IDs and timestamps will differ.
- `cloud_archive` cannot be fully reconstructed from metadata JSON alone because that table
  stores storage-layer fields (`storage_path`, object `file_size`) that are not guaranteed to
  exist in metadata blobs.
- Any operational state not represented in metadata JSON (for example upload bookkeeping) must
  be restored separately.
### Rebuild directly from YAML files

Use `rebuild_archive_db_from_yaml.py` to recreate `archive.db` by scanning a folder tree for metadata YAML files and re-processing each document through the normal insert pipeline.
The rebuild now performs a second canon-relationship sync pass so parent/child Title links are restored even when child metadata is processed before parent metadata in file order.

When running `Create Archive` (both VN and artifact content flows), vn_archiver now also mirrors sidecar metadata into `rebuild_metadata/` using archive-id-prefixed names:

- format: `<archive_id>_<same_sidecar_name_as_uploading>.yaml`
- this mirror is intended as a rolling metadata collection for rebuild workflows
- `rebuild_archive.py` is automatically invoked against `rebuild_metadata/` and writes `rebuild_metadata/archive_rebuild.db`

```bash
# Rebuild archive.db from YAML files under current folder (creates backup if DB exists)
python rebuild_archive_db_from_yaml.py --source-dir .

# Rebuild a specific DB path without creating a backup
python rebuild_archive_db_from_yaml.py --source-dir ./metadata_dump --db-path ./archive.db --no-backup

# Rebuild from the metadata mirror folder used by archive creation
python rebuild_archive.py
```
