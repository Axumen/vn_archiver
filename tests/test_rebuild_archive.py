"""Tests for rebuild_archive_db_from_yaml helper functions.

Focuses on the cloud-tracking snapshot/restore behavior added to prevent
upload history from being lost when rebuild_database is run.
"""

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from rebuild_archive_db_from_yaml import _snapshot_cloud_tracking, _restore_cloud_tracking
import db_manager


# ── helpers ──────────────────────────────────────────────────────────────────

def _make_db_with_cloud_tables(db_path: Path, *, insert_rows: bool = True) -> None:
    """Create a minimal DB that has cloud tracking tables with optional rows."""
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE cloud_archive ("
        "sha256 TEXT PRIMARY KEY, file_size INTEGER NOT NULL, storage_path TEXT NOT NULL UNIQUE)"
    )
    conn.execute(
        "CREATE TABLE cloud_sidecar ("
        "sha256 TEXT PRIMARY KEY, file_size INTEGER NOT NULL, storage_path TEXT NOT NULL UNIQUE)"
    )
    if insert_rows:
        conn.execute(
            "INSERT INTO cloud_archive VALUES (?, ?, ?)",
            ("a" * 64, 1000, "archives/a/v1/file.zip"),
        )
        conn.execute(
            "INSERT INTO cloud_sidecar VALUES (?, ?, ?)",
            ("b" * 64, 200, "metadata/a/v1/file_r01.yaml"),
        )
    conn.commit()
    conn.close()


# ── _snapshot_cloud_tracking ─────────────────────────────────────────────────

class TestSnapshotCloudTracking:
    def test_captures_archive_and_sidecar_rows(self, tmp_path):
        db_path = tmp_path / "archive.db"
        _make_db_with_cloud_tables(db_path)

        archive_rows, sidecar_rows = _snapshot_cloud_tracking(db_path)

        assert len(archive_rows) == 1
        assert archive_rows[0] == ("a" * 64, 1000, "archives/a/v1/file.zip")
        assert len(sidecar_rows) == 1
        assert sidecar_rows[0] == ("b" * 64, 200, "metadata/a/v1/file_r01.yaml")

    def test_returns_empty_lists_when_cloud_tables_absent(self, tmp_path):
        db_path = tmp_path / "archive.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE title (title_id INTEGER PRIMARY KEY, title TEXT)")
        conn.commit()
        conn.close()

        archive_rows, sidecar_rows = _snapshot_cloud_tracking(db_path)

        assert archive_rows == []
        assert sidecar_rows == []

    def test_returns_empty_when_tables_have_no_rows(self, tmp_path):
        db_path = tmp_path / "archive.db"
        _make_db_with_cloud_tables(db_path, insert_rows=False)

        archive_rows, sidecar_rows = _snapshot_cloud_tracking(db_path)

        assert archive_rows == []
        assert sidecar_rows == []


# ── _restore_cloud_tracking ──────────────────────────────────────────────────

class TestRestoreCloudTracking:
    def test_inserts_rows_into_fresh_db(self, tmp_path):
        db_path = tmp_path / "archive.db"
        _make_db_with_cloud_tables(db_path, insert_rows=False)

        archive_rows = [("a" * 64, 1000, "archives/a/v1/file.zip")]
        sidecar_rows = [("b" * 64, 200, "metadata/a/v1/file_r01.yaml")]

        original_path = db_manager.DB_PATH
        try:
            db_manager.DB_PATH = str(db_path)
            _restore_cloud_tracking(archive_rows, sidecar_rows)
        finally:
            db_manager.DB_PATH = original_path

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        assert conn.execute("SELECT COUNT(*) FROM cloud_archive").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM cloud_sidecar").fetchone()[0] == 1
        conn.close()

    def test_no_op_when_both_lists_empty(self):
        # Must not raise even with no DB configured
        _restore_cloud_tracking([], [])

    def test_uses_insert_or_ignore_so_duplicate_rows_are_safe(self, tmp_path):
        """Running restore twice must not raise a UNIQUE constraint error."""
        db_path = tmp_path / "archive.db"
        _make_db_with_cloud_tables(db_path, insert_rows=False)

        archive_rows = [("a" * 64, 1000, "archives/a/v1/file.zip")]
        sidecar_rows = [("b" * 64, 200, "metadata/a/v1/file_r01.yaml")]

        original_path = db_manager.DB_PATH
        try:
            db_manager.DB_PATH = str(db_path)
            _restore_cloud_tracking(archive_rows, sidecar_rows)
            _restore_cloud_tracking(archive_rows, sidecar_rows)  # second call is idempotent
        finally:
            db_manager.DB_PATH = original_path

        conn = sqlite3.connect(str(db_path))
        assert conn.execute("SELECT COUNT(*) FROM cloud_archive").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM cloud_sidecar").fetchone()[0] == 1
        conn.close()
