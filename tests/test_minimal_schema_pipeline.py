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
    conn.execute("CREATE TABLE vn (id INTEGER PRIMARY KEY, title TEXT NOT NULL)")
    conn.execute(
        "CREATE TABLE builds (id INTEGER PRIMARY KEY, vn_id INTEGER NOT NULL, version_string TEXT, release_type TEXT, language TEXT, platform TEXT)"
    )
    conn.execute("CREATE TABLE file (file_id INTEGER PRIMARY KEY, sha256 TEXT NOT NULL UNIQUE, filename TEXT)")
    conn.execute(
        "CREATE TABLE build_file (build_id INTEGER NOT NULL, file_id INTEGER NOT NULL, PRIMARY KEY (build_id, file_id))"
    )
    return conn


def test_resolve_artifact_id_for_metadata_uses_file_build_file_linkage():
    conn = _make_conn()
    conn.execute("INSERT INTO vn (id, title) VALUES (1, 'Example VN')")
    conn.execute("INSERT INTO builds (id, vn_id, version_string) VALUES (7, 1, '1.0')")
    conn.execute("INSERT INTO file (file_id, sha256, filename) VALUES (42, 'abc123', 'sample.zip')")
    conn.execute("INSERT INTO build_file (build_id, file_id) VALUES (7, 42)")

    artifact_id = vn_archiver.resolve_artifact_id_for_metadata(
        conn,
        7,
        {"archives": [{"sha256": "abc123"}]},
    )

    assert artifact_id == 42


def test_get_current_metadata_version_number_returns_default_one():
    assert vn_archiver.get_current_metadata_version_number(build_id=7) == 1


def test_mirror_metadata_for_rebuild_uses_file_build_file_table(tmp_path, monkeypatch):
    conn = _make_conn()
    conn.execute("INSERT INTO vn (id, title) VALUES (1, 'Example VN')")
    conn.execute("INSERT INTO builds (id, vn_id, version_string) VALUES (7, 1, '1.0')")
    conn.execute("INSERT INTO file (file_id, sha256, filename) VALUES (13, 'deadbeef', 'vn.zip')")
    conn.execute("INSERT INTO build_file (build_id, file_id) VALUES (7, 13)")

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
        build_id=7,
    )

    assert len(mirrored) == 1
    assert mirrored[0].name.startswith("13_")
    assert mirrored[0].read_text(encoding="utf-8") == "title: Example VN\n"
