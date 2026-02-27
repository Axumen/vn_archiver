vn_archiver is an automatic visual novel archiver and collection organizer.

Main Menu
<img width="1482" height="444" alt="image" src="https://github.com/user-attachments/assets/7442c18c-92a6-48a3-8e0a-c0d4ad3e796c" />

Metadata Entry 
<img width="1482" height="987" alt="image" src="https://github.com/user-attachments/assets/864a261c-1d1e-482d-9846-672a124c697d" />


## Rolling back or deleting a metadata entry

Use `metadata_rollback_tool.py` to manage `metadata_versions` for a specific build.

### Does rollback apply to newly created metadata?
Yes. If you just created new metadata, that new row becomes `is_current = 1`. Running `rollback` will move `is_current` back to the previous (or specified) version.

### Is actual deletion possible?
Yes. Use `delete-version` to remove a specific `metadata_versions` row by `version_number`.

### Does deletion affect other tables?
- Deleting from `metadata_versions` does **not** delete VN/build/archive records.
- If the deleted metadata hash is no longer referenced by any version row, the tool also removes that orphaned row from `metadata_objects`.
- If you delete the current version, the tool automatically assigns another version as current (unless it is the only version, which is blocked).

Examples:

```bash
# List versions for a VN build
python metadata_rollback_tool.py --title "My VN" --version "1.2" list

# Roll back to the previous metadata version, create DB backup, and export metadata JSON
python metadata_rollback_tool.py --title "My VN" --version "1.2" rollback --backup

# Roll back to an explicit metadata version using build_id
python metadata_rollback_tool.py --build-id 7 rollback --to-version 3 --backup --export-dir metadata_exports

# Permanently delete metadata version v4 for this build
python metadata_rollback_tool.py --build-id 7 delete-version --target-version 4 --backup
```

This tool can create separate files for safety and auditability:
- `db_backups/archive_backup_<timestamp>.db` before rollback/deletion (when `--backup` is used)
- `metadata_exports/*.json` for exported rollback metadata
