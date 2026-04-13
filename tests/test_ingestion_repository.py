import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion_repository import VnIngestionRepository


def make_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE vn (id INTEGER PRIMARY KEY, title TEXT NOT NULL)")
    conn.execute(
        """
        CREATE TABLE builds (
            id INTEGER PRIMARY KEY,
            vn_id INTEGER NOT NULL,
            version_string TEXT,
            build_type TEXT,
            language TEXT,
            platform TEXT,
            UNIQUE (vn_id, version_string, language, build_type, platform)
        )
        """
    )
    conn.execute(
        "CREATE TABLE metadata_raw (id INTEGER PRIMARY KEY, artifact_id INTEGER, source_file TEXT, raw_text TEXT NOT NULL)"
    )
    conn.execute(
        """
        CREATE TABLE artifacts (
            id INTEGER PRIMARY KEY,
            build_id INTEGER,
            sha256 TEXT NOT NULL,
            path TEXT NOT NULL,
            type TEXT,
            UNIQUE (build_id, sha256)
        )
        """
    )
    return conn


def test_repository_requires_new_schema_tables():
    conn = make_conn()
    with pytest.raises(RuntimeError, match="New schema required"):
        VnIngestionRepository(conn)


def test_create_artifact_fails_without_file_tables():
    conn = make_conn()
    with pytest.raises(RuntimeError, match="New schema required"):
        VnIngestionRepository(conn)


def test_create_artifact_fails_for_legacy_artifacts_schema():
    conn = make_conn()
    with pytest.raises(RuntimeError, match="New schema required"):
        VnIngestionRepository(conn)


def make_conn_new_schema():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE vn (vn_id INTEGER PRIMARY KEY, title TEXT NOT NULL)")
    conn.execute(
        """
        CREATE TABLE build (
            build_id INTEGER PRIMARY KEY,
            vn_id INTEGER NOT NULL,
            version TEXT NOT NULL,
            build_type TEXT,
            distribution_model TEXT,
            distribution_platform TEXT,
            language TEXT,
            translator TEXT,
            edition TEXT,
            release_date TEXT,
            engine TEXT,
            engine_version TEXT,
            target_platform TEXT,
            notes TEXT,
            change_note TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE file (
            file_id INTEGER PRIMARY KEY,
            sha256 TEXT NOT NULL UNIQUE,
            filename TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE build_file (
            build_id INTEGER NOT NULL,
            file_id INTEGER NOT NULL,
            original_filename TEXT,
            archived_at TEXT,
            PRIMARY KEY (build_id, file_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE tags (
            tag_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE vn_tags (
            vn_id INTEGER NOT NULL,
            tag_id INTEGER NOT NULL,
            PRIMARY KEY (vn_id, tag_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE developers (
            developer_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE vn_developers (
            vn_id INTEGER NOT NULL,
            developer_id INTEGER NOT NULL,
            PRIMARY KEY (vn_id, developer_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE publishers (
            publisher_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE vn_publishers (
            vn_id INTEGER NOT NULL,
            publisher_id INTEGER NOT NULL,
            PRIMARY KEY (vn_id, publisher_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE languages (
            language_id INTEGER PRIMARY KEY,
            code TEXT NOT NULL UNIQUE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE build_languages (
            build_id INTEGER NOT NULL,
            language_id INTEGER NOT NULL,
            PRIMARY KEY (build_id, language_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE metadata_raw_versions (
            metadata_raw_id INTEGER PRIMARY KEY,
            build_id INTEGER NOT NULL,
            file_id INTEGER,
            raw_json TEXT NOT NULL,
            raw_sha256 TEXT NOT NULL,
            version_number INTEGER NOT NULL,
            is_current INTEGER NOT NULL DEFAULT 0,
            parent_version_id INTEGER,
            change_note TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    return conn


def test_repository_supports_new_build_file_schema():
    conn = make_conn_new_schema()
    repo = VnIngestionRepository(conn)

    vn_id = repo.get_or_create_vn({"title": "Rewrite"})
    build_id = repo.get_or_create_build(
        vn_id,
        {"version": "1.0", "build_type": "full", "language": "JP", "target_platform": "windows"},
    )
    file_id = repo.create_file_link(
        build_id,
        {"archived_at": "2026-04-10T00:00:00Z"},
        {"sha256": "abc123", "filename": "rewrite.zip"},
    )

    assert vn_id == 1
    assert build_id == 1
    assert file_id == 1

    row = conn.execute(
        "SELECT bf.build_id, bf.file_id, f.sha256 FROM build_file bf JOIN file f ON f.file_id = bf.file_id"
    ).fetchone()
    assert row["build_id"] == 1
    assert row["file_id"] == 1
    assert row["sha256"] == "abc123"


def test_repository_uses_canonical_build_keys_only():
    conn = make_conn_new_schema()
    repo = VnIngestionRepository(conn)

    vn_id = repo.get_or_create_vn({"title": "AIR"})
    build_id = repo.get_or_create_build(
        vn_id,
        {
            "version": "1.0",
            "release_type": "full",  # legacy key should not be consumed
            "platform": "windows",   # legacy key should not be consumed
        },
    )

    row = conn.execute(
        "SELECT build_type, target_platform FROM build WHERE build_id = ?",
        (build_id,),
    ).fetchone()
    assert row["build_type"] is None
    assert row["target_platform"] is None

def test_repository_syncs_vn_tags_when_tables_exist():
    conn = make_conn_new_schema()
    repo = VnIngestionRepository(conn)

    vn_id = repo.get_or_create_vn({"title": "Clannad", "tags": ["romance", "drama"]})
    rows = conn.execute(
        """
        SELECT t.name
        FROM vn_tags vt
        JOIN tags t ON t.tag_id = vt.tag_id
        WHERE vt.vn_id = ?
        ORDER BY t.name
        """,
        (vn_id,),
    ).fetchall()
    assert [row["name"] for row in rows] == ["drama", "romance"]

    repo.get_or_create_vn({"title": "Clannad", "tags": "nakige, drama"})
    rows = conn.execute(
        """
        SELECT t.name
        FROM vn_tags vt
        JOIN tags t ON t.tag_id = vt.tag_id
        WHERE vt.vn_id = ?
        ORDER BY t.name
        """,
        (vn_id,),
    ).fetchall()
    assert [row["name"] for row in rows] == ["drama", "nakige"]


def test_repository_syncs_vn_developers_and_publishers_when_tables_exist():
    conn = make_conn_new_schema()
    repo = VnIngestionRepository(conn)

    vn_id = repo.get_or_create_vn(
        {"title": "Rewrite", "developer": ["Key", "VisualArt's"], "publisher": "Key"}
    )

    dev_rows = conn.execute(
        """
        SELECT d.name
        FROM vn_developers vd
        JOIN developers d ON d.developer_id = vd.developer_id
        WHERE vd.vn_id = ?
        ORDER BY d.name
        """,
        (vn_id,),
    ).fetchall()
    pub_rows = conn.execute(
        """
        SELECT p.name
        FROM vn_publishers vp
        JOIN publishers p ON p.publisher_id = vp.publisher_id
        WHERE vp.vn_id = ?
        ORDER BY p.name
        """,
        (vn_id,),
    ).fetchall()

    assert [row["name"] for row in dev_rows] == ["key", "visualart's"]
    assert [row["name"] for row in pub_rows] == ["key"]


def test_repository_syncs_build_languages_when_tables_exist():
    conn = make_conn_new_schema()
    repo = VnIngestionRepository(conn)

    vn_id = repo.get_or_create_vn({"title": "Clannad"})
    build_id = repo.get_or_create_build(
        vn_id,
        {"version": "1.0", "build_type": "full", "language": ["english", "japanese"]},
    )

    rows = conn.execute(
        """
        SELECT l.code
        FROM build_languages bl
        JOIN languages l ON l.language_id = bl.language_id
        WHERE bl.build_id = ?
        ORDER BY l.code
        """,
        (build_id,),
    ).fetchall()
    assert [row["code"] for row in rows] == ["english", "japanese"]


def test_repository_tracks_raw_metadata_versions_per_build():
    conn = make_conn_new_schema()
    repo = VnIngestionRepository(conn)

    repo.create_metadata_raw({"title": "A", "version": "1.0"}, file_id=7, build_id=3)
    repo.create_metadata_raw({"title": "A", "version": "1.1"}, file_id=8, build_id=3)

    rows = conn.execute(
        """
        SELECT build_id, file_id, raw_json, version_number, raw_sha256
        FROM metadata_raw_versions
        WHERE build_id = 3
        ORDER BY version_number
        """
    ).fetchall()
    assert len(rows) == 2
    assert rows[0]["file_id"] == 7
    assert rows[0]["version_number"] == 1
    assert rows[1]["file_id"] == 8
    assert rows[1]["version_number"] == 2
    assert '"version": "1.0"' in rows[0]["raw_json"]
    assert '"version": "1.1"' in rows[1]["raw_json"]
    assert rows[0]["raw_sha256"] != rows[1]["raw_sha256"]
