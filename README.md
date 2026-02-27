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

# By title/version
python metadata_rollback_tool.py --title "My VN" --version "1.2" list
```

Safety files created when `--backup` is used:
- `db_backups/archive_backup_<timestamp>.db`
