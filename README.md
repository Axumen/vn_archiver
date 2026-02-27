vn_archiver is an automatic visual novel archiver and collection organizer.

Main Menu
<img width="1482" height="444" alt="image" src="https://github.com/user-attachments/assets/7442c18c-92a6-48a3-8e0a-c0d4ad3e796c" />

Metadata Entry 
<img width="1482" height="987" alt="image" src="https://github.com/user-attachments/assets/864a261c-1d1e-482d-9846-672a124c697d" />


## Rolling back a recent metadata entry

Use `metadata_rollback_tool.py` to safely move the `is_current` pointer in `metadata_versions` back to an older version **without deleting history**.

Examples:

```bash
# List versions for a VN build
python metadata_rollback_tool.py --title "My VN" --version "1.2" list

# Roll back to the previous metadata version, create DB backup, and export metadata files
python metadata_rollback_tool.py --title "My VN" --version "1.2" rollback --backup

# Roll back to an explicit metadata version using build_id
python metadata_rollback_tool.py --build-id 7 rollback --to-version 3 --backup --export-dir metadata_exports
```

This tool can create separate files for safety and auditability:
- `db_backups/archive_backup_<timestamp>.db` before rollback (when `--backup` is used)
- `metadata_exports/*.json` for the selected metadata version
