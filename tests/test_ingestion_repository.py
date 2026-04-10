import sqlite3
import sys
from pathlib import Path

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


def test_get_or_create_vn_and_build_works_without_visual_novels_table():
    conn = make_conn()
    repo = VnIngestionRepository(
        conn,
        upsert_series=lambda *args, **kwargs: None,
        upsert_visual_novel_record=lambda *args, **kwargs: None,
        sync_vn_tags=lambda *args, **kwargs: None,
        sync_canon_relationship=lambda *args, **kwargs: None,
        upsert_build_record=lambda *args, **kwargs: None,
        sync_build_target_platforms=lambda *args, **kwargs: None,
        sync_build_relations=lambda *args, **kwargs: None,
        resolve_existing_build_for_artifact=lambda *args, **kwargs: None,
        create_artifact_record=lambda *args, **kwargs: None,
    )

    vn_id = repo.get_or_create_vn({"title": "Clannad"})
    build_id = repo.get_or_create_build(
        vn_id,
        {
            "version": "1.0",
            "language": "JP",
            "build_type": "original",
            "platform": "windows",
        },
    )

    assert vn_id == 1
    assert build_id == 1

    same_vn_id = repo.get_or_create_vn({"title": "Clannad"})
    same_build_id = repo.get_or_create_build(
        same_vn_id,
        {
            "version": "1.0",
            "language": "JP",
            "build_type": "original",
            "platform": "windows",
        },
    )

    assert same_vn_id == vn_id
    assert same_build_id == build_id


def test_create_artifact_does_not_require_files_table_in_current_schema():
    conn = make_conn()
    repo = VnIngestionRepository(
        conn,
        upsert_series=lambda *args, **kwargs: None,
        upsert_visual_novel_record=lambda *args, **kwargs: None,
        sync_vn_tags=lambda *args, **kwargs: None,
        sync_canon_relationship=lambda *args, **kwargs: None,
        upsert_build_record=lambda *args, **kwargs: None,
        sync_build_target_platforms=lambda *args, **kwargs: None,
        sync_build_relations=lambda *args, **kwargs: None,
        resolve_existing_build_for_artifact=lambda *args, **kwargs: None,
        create_artifact_record=lambda *args, **kwargs: None,
    )

    conn.execute("INSERT INTO vn (id, title) VALUES (1, 'Clannad')")
    conn.execute(
        "INSERT INTO builds (id, vn_id, version_string, build_type, language, platform) VALUES (1, 1, '1.0', 'original', 'JP', 'windows')"
    )

    artifact_id = repo.create_artifact(
        1,
        {"artifact_type": "game_archive"},
        {"sha256": "abc123", "filename": "clannad_v1.0.zip"},
    )

    row = conn.execute("SELECT id, build_id, sha256, path, type FROM artifacts WHERE id = ?", (artifact_id,)).fetchone()
    assert row is not None
    assert row["build_id"] == 1
    assert row["sha256"] == "abc123"
    assert row["path"] == "clannad_v1.0.zip"
    assert row["type"] == "game_archive"


def test_create_artifact_allows_shared_sha_across_different_builds():
    conn = make_conn()
    repo = VnIngestionRepository(
        conn,
        upsert_series=lambda *args, **kwargs: None,
        upsert_visual_novel_record=lambda *args, **kwargs: None,
        sync_vn_tags=lambda *args, **kwargs: None,
        sync_canon_relationship=lambda *args, **kwargs: None,
        upsert_build_record=lambda *args, **kwargs: None,
        sync_build_target_platforms=lambda *args, **kwargs: None,
        sync_build_relations=lambda *args, **kwargs: None,
        resolve_existing_build_for_artifact=lambda *args, **kwargs: None,
        create_artifact_record=lambda *args, **kwargs: None,
    )

    conn.execute("INSERT INTO vn (id, title) VALUES (1, 'Clannad')")
    conn.execute("INSERT INTO vn (id, title) VALUES (2, 'Tomoyo After')")
    conn.execute(
        "INSERT INTO builds (id, vn_id, version_string, build_type, language, platform) VALUES (1, 1, '1.0', 'original', 'JP', 'windows')"
    )
    conn.execute(
        "INSERT INTO builds (id, vn_id, version_string, build_type, language, platform) VALUES (2, 2, '1.0', 'original', 'JP', 'windows')"
    )

    first_id = repo.create_artifact(
        1,
        {"artifact_type": "game_archive"},
        {"sha256": "shared-sha", "filename": "readme.txt"},
    )
    second_id = repo.create_artifact(
        2,
        {"artifact_type": "game_archive"},
        {"sha256": "shared-sha", "filename": "readme.txt"},
    )

    assert first_id != second_id
    rows = conn.execute(
        "SELECT id, build_id, sha256 FROM artifacts WHERE sha256 = ? ORDER BY id",
        ("shared-sha",),
    ).fetchall()
    assert len(rows) == 2
    assert rows[0]["build_id"] == 1
    assert rows[1]["build_id"] == 2


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
            language TEXT,
            target_platform TEXT
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
    return conn


def test_repository_supports_new_build_file_schema():
    conn = make_conn_new_schema()
    repo = VnIngestionRepository(
        conn,
        upsert_series=lambda *args, **kwargs: None,
        upsert_visual_novel_record=lambda *args, **kwargs: None,
        sync_vn_tags=lambda *args, **kwargs: None,
        sync_canon_relationship=lambda *args, **kwargs: None,
        upsert_build_record=lambda *args, **kwargs: None,
        sync_build_target_platforms=lambda *args, **kwargs: None,
        sync_build_relations=lambda *args, **kwargs: None,
        resolve_existing_build_for_artifact=lambda *args, **kwargs: None,
        create_artifact_record=lambda *args, **kwargs: None,
    )

    vn_id = repo.get_or_create_vn({"title": "Rewrite"})
    build_id = repo.get_or_create_build(
        vn_id,
        {"version": "1.0", "build_type": "full", "language": "JP", "target_platform": "windows"},
    )
    file_id = repo.create_artifact(
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
