import sqlite3
import sys
from contextlib import contextmanager
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

pytest.importorskip("yaml")
pytest.importorskip("b2sdk.v2")
import vn_archiver
import staging


def _make_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE title (title_id INTEGER PRIMARY KEY, title TEXT NOT NULL)")
    conn.execute(
        "CREATE TABLE release (release_id INTEGER PRIMARY KEY, title_id INTEGER NOT NULL, version TEXT, release_type TEXT, language TEXT, target_platform TEXT)"
    )
    conn.execute("CREATE TABLE file (file_id INTEGER PRIMARY KEY, sha256 TEXT NOT NULL UNIQUE, filename TEXT)")
    conn.execute(
        "CREATE TABLE release_file (release_id INTEGER NOT NULL, file_id INTEGER NOT NULL, PRIMARY KEY (release_id, file_id))"
    )
    return conn


def test_mirror_metadata_for_rebuild_uses_file_release_file_table(tmp_path, monkeypatch):
    conn = _make_conn()
    conn.execute("INSERT INTO title (title_id, title) VALUES (1, 'Example VN')")
    conn.execute("INSERT INTO release (release_id, title_id, version) VALUES (7, 1, '1.0')")
    conn.execute("INSERT INTO file (file_id, sha256, filename) VALUES (13, 'deadbeef', 'vn.zip')")
    conn.execute("INSERT INTO release_file (release_id, file_id) VALUES (7, 13)")

    staged = tmp_path / "meta.yaml"
    staged.write_text("title: Example VN\n", encoding="utf-8")

    rebuild_dir = tmp_path / "rebuild_metadata"

    @contextmanager
    def fake_connection():
        yield conn

    monkeypatch.setattr(staging, "get_connection", fake_connection)
    monkeypatch.setattr(staging, "REBUILD_METADATA_DIR", str(rebuild_dir))

    mirrored = staging.mirror_metadata_for_rebuild(
        str(staged),
        [{"sha256": "deadbeef"}],
        release_id=7,
    )

    assert len(mirrored) == 1
    assert mirrored[0].name.startswith("13_")
    assert mirrored[0].read_text(encoding="utf-8") == "title: Example VN\n"


def test_stage_ingested_files_for_upload_moves_archives_and_stages_metadata(tmp_path, monkeypatch):
    incoming_archive = tmp_path / "incoming_sample.zip"
    incoming_archive.write_bytes(b"payload")

    upload_dir = tmp_path / "uploading"
    staged_meta = upload_dir / "staged_meta.yaml"

    monkeypatch.setattr(staging, "UPLOADING_DIR", str(upload_dir))

    def fake_stage_metadata_yaml_for_upload(metadata, metadata_version_number, target_dir=None, *, order_fn=None):
        Path(target_dir).mkdir(parents=True, exist_ok=True)
        staged_meta.write_text("title: Sample\n", encoding="utf-8")
        return staged_meta

    monkeypatch.setattr(staging, "stage_metadata_yaml_for_upload", fake_stage_metadata_yaml_for_upload)

    archives_data = [
        {
            "original_path": str(incoming_archive),
            "filename": incoming_archive.name,
            "sha256": "abcd1234",
        }
    ]
    staged_archives, staged_meta_path = staging.stage_ingested_files_for_upload(
        {"title": "Sample VN", "version": "1.0"},
        archives_data,
        metadata_version_number=2,
    )

    assert len(staged_archives) == 1
    assert staged_archives[0].parent == upload_dir
    assert staged_archives[0].exists()
    assert not incoming_archive.exists()
    assert archives_data[0]["staged_upload_path"] == str(staged_archives[0])
    assert staged_meta_path == staged_meta
