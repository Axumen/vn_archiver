"""Tests for ingestion_service.py — attach_file_to_release_pipeline and ingest_incoming_pair."""

import sqlite3
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

pytest.importorskip("yaml")
import ingestion_service
import staging
import db_manager
import vn_archiver as vn_archiver_mod


# ============================================================
# Fixtures
# ============================================================


SCHEMA_PATH = Path(__file__).resolve().parents[1] / "db_schema.sql"


def _make_test_db(conn):
    """Create the full canonical schema from db_schema.sql.

    The production schema uses ``GENERATED ALWAYS AS`` for ``normalized_version``,
    which requires SQLite ≥ 3.31.  For broader test compatibility we replace it
    with a plain column and handle normalization in the repository layer (which
    is what already happens at runtime).
    """
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    # Replace the generated column with a plain TEXT column
    schema_sql = schema_sql.replace(
        "normalized_version TEXT GENERATED ALWAYS AS (lower(trim(version))) VIRTUAL",
        "normalized_version TEXT DEFAULT ''",
    )
    conn.executescript(schema_sql)


@pytest.fixture
def test_db():
    """Return a fully-initialised in-memory DB connection."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _make_test_db(conn)
    return conn


@pytest.fixture
def patched_db(test_db, monkeypatch):
    """Monkey-patch db_manager so all get_connection() calls return our in-memory DB."""
    @contextmanager
    def fake_get_connection():
        yield test_db

    @contextmanager
    def fake_exclusive_transaction(conn):
        yield

    # Patch everywhere get_connection / exclusive_transaction is imported
    for mod in (db_manager, ingestion_service, vn_archiver_mod):
        monkeypatch.setattr(mod, "get_connection", fake_get_connection)
        monkeypatch.setattr(mod, "exclusive_transaction", fake_exclusive_transaction)
    return test_db


# ============================================================
# attach_file_to_release_pipeline
# ============================================================


class TestAttachFileToReleasePipeline:
    def test_attaches_file_and_stages(self, tmp_path, patched_db, monkeypatch):
        monkeypatch.setattr(staging, "UPLOADING_DIR", str(tmp_path / "uploading"))

        conn = patched_db
        conn.execute("INSERT INTO title (title_id, title) VALUES (1, 'Test VN')")
        conn.execute(
            "INSERT INTO release (release_id, title_id, version, normalized_version) "
            "VALUES (10, 1, '1.0', '1.0')"
        )

        archive = tmp_path / "incoming" / "game.zip"
        archive.parent.mkdir()
        archive.write_bytes(b"game content here")

        result = ingestion_service.attach_file_to_release_pipeline(
            str(archive),
            release_id=10,
            file_metadata={"title": "Test VN", "version": "1.0", "metadata_version": 1},
        )

        assert result.release_id == 10
        assert result.file_id is not None
        assert result.file_sha256  # non-empty
        assert result.file_size_bytes == len(b"game content here")

        # Verify file was persisted in DB
        row = conn.execute("SELECT * FROM file WHERE sha256 = ?", (result.file_sha256,)).fetchone()
        assert row is not None
        assert row["filename"] == "game.zip"

        # Verify release_file link
        link = conn.execute(
            "SELECT * FROM release_file WHERE release_id = ? AND file_id = ?",
            (10, result.file_id),
        ).fetchone()
        assert link is not None

    def test_stages_archive_in_uploading_dir(self, tmp_path, patched_db, monkeypatch):
        upload_dir = tmp_path / "uploading"
        monkeypatch.setattr(staging, "UPLOADING_DIR", str(upload_dir))

        conn = patched_db
        conn.execute("INSERT INTO title (title_id, title) VALUES (1, 'Test VN')")
        conn.execute(
            "INSERT INTO release (release_id, title_id, version, normalized_version) "
            "VALUES (10, 1, '1.0', '1.0')"
        )

        archive = tmp_path / "incoming" / "vn.zip"
        archive.parent.mkdir()
        archive.write_bytes(b"data")

        result = ingestion_service.attach_file_to_release_pipeline(
            str(archive),
            release_id=10,
            file_metadata={"title": "Test VN", "version": "1.0", "metadata_version": 1},
        )

        assert len(result.staged_archives) == 1
        assert result.staged_archives[0].parent == upload_dir
        assert result.staged_archives[0].exists()
        assert not archive.exists()  # moved, not copied


# ============================================================
# ingest_incoming_pair
# ============================================================


class TestIngestIncomingPair:
    def test_creates_release_and_attaches_file(self, tmp_path, patched_db, monkeypatch):
        upload_dir = tmp_path / "uploading"
        monkeypatch.setattr(staging, "UPLOADING_DIR", str(upload_dir))

        archive = tmp_path / "incoming" / "vn.zip"
        archive.parent.mkdir()
        archive.write_bytes(b"visual novel content")

        release_meta = {"title": "Fresh VN", "version": "2.0", "metadata_version": 1}
        file_meta = {"title": "Fresh VN", "version": "2.0", "metadata_version": 1}

        result = ingestion_service.ingest_incoming_pair(
            str(archive),
            release_meta,
            file_meta,
            raw_metadata_text="title: Fresh VN\nversion: 2.0\n",
            source_file="incoming/fresh.yaml",
        )

        assert result.release_id is not None
        assert result.file_id is not None
        assert result.file_sha256
        assert result.file_size_bytes == len(b"visual novel content")

        # Verify DB state
        conn = patched_db
        title_row = conn.execute("SELECT * FROM title WHERE title = 'Fresh VN'").fetchone()
        assert title_row is not None

        release_row = conn.execute(
            "SELECT * FROM release WHERE release_id = ?", (result.release_id,)
        ).fetchone()
        assert release_row is not None

        file_row = conn.execute(
            "SELECT * FROM file WHERE sha256 = ?", (result.file_sha256,)
        ).fetchone()
        assert file_row is not None

    def test_produces_release_sidecar(self, tmp_path, patched_db, monkeypatch):
        upload_dir = tmp_path / "uploading"
        monkeypatch.setattr(staging, "UPLOADING_DIR", str(upload_dir))

        archive = tmp_path / "incoming" / "vn.zip"
        archive.parent.mkdir()
        archive.write_bytes(b"data")

        result = ingestion_service.ingest_incoming_pair(
            str(archive),
            {"title": "Sidecar VN", "version": "1.0", "metadata_version": 1},
            {"title": "Sidecar VN", "version": "1.0", "metadata_version": 1},
        )

        assert result.release_sidecar_path is not None
        assert result.release_sidecar_path.exists()
        assert result.release_sidecar_path.suffix == ".yaml"
