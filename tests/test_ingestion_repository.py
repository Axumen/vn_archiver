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
            release_type TEXT,
            language TEXT,
            platform TEXT,
            UNIQUE (vn_id, version_string, language, release_type, platform)
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
            sha256 TEXT NOT NULL UNIQUE,
            path TEXT NOT NULL,
            type TEXT
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
            "release_type": "original",
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
            "release_type": "original",
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
        "INSERT INTO builds (id, vn_id, version_string, release_type, language, platform) VALUES (1, 1, '1.0', 'original', 'JP', 'windows')"
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
