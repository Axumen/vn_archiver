vn_archiver is an automatic visual novel archiver and collection organizer.

Main Menu
<img width="1482" height="444" alt="image" src="https://github.com/user-attachments/assets/7442c18c-92a6-48a3-8e0a-c0d4ad3e796c" />

Metadata Entry 
<img width="1482" height="987" alt="image" src="https://github.com/user-attachments/assets/864a261c-1d1e-482d-9846-672a124c697d" />


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

# Delete one log-book event, resequence seq_no, and rebuild normalized tables from the remaining log
python metadata_rollback_tool.py delete-log-entry --log-entry-id 42 --backup

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

```bash
# Rebuild archive.db from YAML files under current folder (creates backup if DB exists)
python rebuild_archive_db_from_yaml.py --source-dir .

# Rebuild a specific DB path without creating a backup
python rebuild_archive_db_from_yaml.py --source-dir ./metadata_dump --db-path ./archive.db --no-backup
```


## Log-book-first metadata design

A proposed event-sourced log-book model (ordered metadata spine + cascade-friendly derived tables) is documented in `docs/log_book_system.md`.

You can rebuild normalized tables from `metadata_log_book` payloads with:

```bash
python rebuild_archive_db_from_log_book.py
```
