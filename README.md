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

When a list/map is used, vn_archiver stores it in the `builds.translator` text column as JSON while preserving the full value in metadata history. Stored `metadata_objects.metadata_json` keeps template-style field ordering for readability/export consistency while version hashing remains canonical.


## Upload format (separate archive + metadata sidecar)

Uploads now use a strict separation model:
- the VN archive file is uploaded as the cloud object
- metadata must be provided as a sidecar YAML file in `uploading/` with the pattern `<archive_name>_meta_vN.yaml` (where `vN` is the metadata revision number)
- archive upload path format: `archives/{title_slug}/vn-{vn_id:05d}/{version_slug}/{archive_file_name}`
- metadata upload path format: `metadata/{title_slug}/vn-{vn_id:05d}/{version_slug}/{sidecar_file_name}`
- upload requires sidecar metadata to match the corresponding metadata revision stored in database
- when uploading metadata revision `vN` (N > 1), the parent metadata revision `vN-1` must already be uploaded

Embedded `metadata.yaml` inside archives is no longer used by the upload pipeline.

## Undoing a mistaken metadata/create entry

Use `metadata_rollback_tool.py` to manage `metadata_versions` and, if needed, remove the full created build entry.

### If you only want to undo the latest metadata edit
Use rollback (non-destructive history pointer move):

```bash
python metadata_rollback_tool.py --build-id 7 rollback --backup
```

### If you want it to look like the create never happened
Use hard undo (build-level removal):

```bash
# Removes the build row and cascaded build-linked rows
python metadata_rollback_tool.py --build-id 7 undo-build-create --backup

# Keep VN row even if that was its only build
python metadata_rollback_tool.py --build-id 7 undo-build-create --backup --keep-empty-vn

# Preview affected rows without changing archive.db
python metadata_rollback_tool.py --build-id 7 undo-build-create --dry-run
```

`undo-build-create` performs:
- delete from `builds` for that build id (which cascades to `archives`, `build_target_platforms`, and `metadata_versions`)
- optional deletion of the now-empty `visual_novels` row (default behavior)
- cleanup of orphaned `metadata_objects`, `tags`, and `series`

### Targeting entries
```bash
# By build id
python metadata_rollback_tool.py --build-id 7 list

# Delete only the newest metadata version for this build
python metadata_rollback_tool.py --build-id 7 delete-version --latest --backup

# Undo latest update entry on an existing build (removes newest metadata version + newest archive row)
python metadata_rollback_tool.py --build-id 7 undo-latest-entry --backup

# By title/version
python metadata_rollback_tool.py --title "My VN" --version "1.2" list
```


Schema behavior note:
- Deleting the last `archives` row for a build now automatically deletes that `builds` row (which then cascades to build-linked tables via existing foreign keys).
- Deleting `metadata_versions` rows now automatically prunes orphaned `metadata_objects` rows via trigger.
- `undo-latest-entry` is intended for existing builds with at least 2 archives and 2 metadata versions; it avoids deleting the only archive row to prevent accidental build removal.

Safety files created when `--backup` is used:
- `db_backups/archive_backup_<timestamp>.db`

## Can `archive.db` be regenerated from `metadata_objects.metadata_json`?

Partially. The JSON blobs in `metadata_objects.metadata_json` are sufficient to reconstruct
most normalized metadata tables by re-feeding each blob through `insert_visual_novel()` in
`vn_archiver.py`, because that path upserts series/VN/build/tag/platform/canon rows and
re-materializes metadata history entries.

However, this is **not** a full-fidelity rebuild of every table:
- Auto-generated IDs and timestamps will differ.
- `archive_objects` cannot be fully reconstructed from metadata JSON alone because that table
  stores storage-layer fields (`storage_path`, object `file_size`) that are not guaranteed to
  exist in metadata blobs.
- Any operational state not represented in metadata JSON (for example upload bookkeeping) must
  be restored separately.
### Rebuild directly from YAML files

Use `rebuild_archive_db_from_yaml.py` to recreate `archive.db` by scanning a folder tree for metadata YAML files and re-processing each document through the normal insert pipeline.
The rebuild now performs a second canon-relationship sync pass so parent/child VN links are restored even when child metadata is processed before parent metadata in file order.

```bash
# Rebuild archive.db from YAML files under current folder (creates backup if DB exists)
python rebuild_archive_db_from_yaml.py --source-dir .

# Rebuild a specific DB path without creating a backup
python rebuild_archive_db_from_yaml.py --source-dir ./metadata_dump --db-path ./archive.db --no-backup
```

