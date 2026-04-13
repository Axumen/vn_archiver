import sqlite3
import sys
from contextlib import contextmanager
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

pytest.importorskip("yaml")
pytest.importorskip("b2sdk.v2")
import vn_archiver


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

    monkeypatch.setattr(vn_archiver, "get_connection", fake_connection)
    monkeypatch.setattr(vn_archiver, "REBUILD_METADATA_DIR", str(rebuild_dir))

    mirrored = vn_archiver.mirror_metadata_for_rebuild(
        str(staged),
        [{"sha256": "deadbeef"}],
        release_id=7,
    )

    assert len(mirrored) == 1
    assert mirrored[0].name.startswith("13_")
    assert mirrored[0].read_text(encoding="utf-8") == "title: Example VN\n"
