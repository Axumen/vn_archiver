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
    conn.execute("CREATE TABLE series (series_id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, description TEXT)")
    conn.execute("CREATE TABLE title (title_id INTEGER PRIMARY KEY, title TEXT NOT NULL, series_id INTEGER)")
    conn.execute(
        """
        CREATE TABLE release (
            release_id INTEGER PRIMARY KEY,
            title_id INTEGER NOT NULL,
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
            size_bytes INTEGER,
            filename TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE release_file (
            release_id INTEGER NOT NULL,
            file_id INTEGER NOT NULL,
            original_filename TEXT,
            artifact_type TEXT,
            archived_at TEXT,
            PRIMARY KEY (release_id, file_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE tag (
            tag_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE title_tag (
            title_id INTEGER NOT NULL,
            tag_id INTEGER NOT NULL,
            PRIMARY KEY (title_id, tag_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE developer (
            developer_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE title_developer (
            title_id INTEGER NOT NULL,
            developer_id INTEGER NOT NULL,
            PRIMARY KEY (title_id, developer_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE publisher (
            publisher_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE title_publisher (
            title_id INTEGER NOT NULL,
            publisher_id INTEGER NOT NULL,
            PRIMARY KEY (title_id, publisher_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE language (
            language_id INTEGER PRIMARY KEY,
            code TEXT NOT NULL UNIQUE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE release_language (
            release_id INTEGER NOT NULL,
            language_id INTEGER NOT NULL,
            PRIMARY KEY (release_id, language_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE revision (
            revision_id INTEGER PRIMARY KEY,
            release_id INTEGER NOT NULL,
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
    conn.execute(
        """
        CREATE TABLE file_snapshot (
            metadata_id INTEGER PRIMARY KEY,
            release_id INTEGER NOT NULL,
            file_id INTEGER NOT NULL,
            metadata_version INTEGER NOT NULL,
            title TEXT,
            version TEXT,
            build_type TEXT,
            normalized_version TEXT,
            distribution_platform TEXT,
            platform TEXT,
            language TEXT,
            edition TEXT,
            release_date TEXT,
            source_url TEXT,
            notes TEXT,
            change_note TEXT,
            raw_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (release_id, file_id) REFERENCES release_file(release_id, file_id) ON DELETE CASCADE
        )
        """
    )
    return conn


def test_repository_supports_new_release_file_schema():
    conn = make_conn_new_schema()
    repo = VnIngestionRepository(conn)

    title_id = repo.get_or_create_title({"title": "Rewrite"})
    release_id = repo.get_or_create_release(
        title_id,
        {"version": "1.0", "build_type": "full", "language": "JP", "target_platform": "windows"},
    )
    file_id = repo.create_file_link(
        release_id,
        {"archived_at": "2026-04-10T00:00:00Z"},
        {"sha256": "abc123", "filename": "rewrite.zip"},
    )

    assert title_id == 1
    assert release_id == 1
    assert file_id == 1

    row = conn.execute(
        "SELECT rf.release_id, rf.file_id, f.sha256 FROM release_file rf JOIN file f ON f.file_id = rf.file_id"
    ).fetchone()
    assert row["release_id"] == 1
    assert row["file_id"] == 1
    assert row["sha256"] == "abc123"


def test_repository_uses_canonical_release_keys_only():
    conn = make_conn_new_schema()
    repo = VnIngestionRepository(conn)

    title_id = repo.get_or_create_title({"title": "AIR"})
    release_id = repo.get_or_create_release(
        title_id,
        {
            "version": "1.0",
            "release_type": "full",  # legacy key should not be consumed
            "platform": "windows",   # legacy key should not be consumed
        },
    )

    row = conn.execute(
        "SELECT build_type, target_platform FROM release WHERE release_id = ?",
        (release_id,),
    ).fetchone()
    assert row["build_type"] is None
    assert row["target_platform"] is None

def test_repository_syncs_title_tags_when_tables_exist():
    conn = make_conn_new_schema()
    repo = VnIngestionRepository(conn)

    title_id = repo.get_or_create_title({"title": "Clannad", "tags": ["romance", "drama"]})
    rows = conn.execute(
        """
        SELECT t.name
        FROM title_tag tt
        JOIN tag t ON t.tag_id = tt.tag_id
        WHERE tt.title_id = ?
        ORDER BY t.name
        """,
        (title_id,),
    ).fetchall()
    assert [row["name"] for row in rows] == ["drama", "romance"]

    repo.get_or_create_title({"title": "Clannad", "tags": "nakige, drama"})
    rows = conn.execute(
        """
        SELECT t.name
        FROM title_tag tt
        JOIN tag t ON t.tag_id = tt.tag_id
        WHERE tt.title_id = ?
        ORDER BY t.name
        """,
        (title_id,),
    ).fetchall()
    assert [row["name"] for row in rows] == ["drama", "nakige"]


def test_repository_syncs_title_developers_and_publishers_when_tables_exist():
    conn = make_conn_new_schema()
    repo = VnIngestionRepository(conn)

    title_id = repo.get_or_create_title(
        {"title": "Rewrite", "developer": ["Key", "VisualArt's"], "publisher": "Key"}
    )

    dev_rows = conn.execute(
        """
        SELECT d.name
        FROM title_developer td
        JOIN developer d ON d.developer_id = td.developer_id
        WHERE td.title_id = ?
        ORDER BY d.name
        """,
        (title_id,),
    ).fetchall()
    pub_rows = conn.execute(
        """
        SELECT p.name
        FROM title_publisher tp
        JOIN publisher p ON p.publisher_id = tp.publisher_id
        WHERE tp.title_id = ?
        ORDER BY p.name
        """,
        (title_id,),
    ).fetchall()

    assert [row["name"] for row in dev_rows] == ["key", "visualart's"]
    assert [row["name"] for row in pub_rows] == ["key"]


def test_repository_syncs_release_languages_when_tables_exist():
    conn = make_conn_new_schema()
    repo = VnIngestionRepository(conn)

    title_id = repo.get_or_create_title({"title": "Clannad"})
    release_id = repo.get_or_create_release(
        title_id,
        {"version": "1.0", "build_type": "full", "language": ["english", "japanese"]},
    )

    rows = conn.execute(
        """
        SELECT l.code
        FROM release_language rl
        JOIN language l ON l.language_id = rl.language_id
        WHERE rl.release_id = ?
        ORDER BY l.code
        """,
        (release_id,),
    ).fetchall()
    assert [row["code"] for row in rows] == ["english", "japanese"]


def test_repository_tracks_raw_metadata_versions_per_release():
    conn = make_conn_new_schema()
    repo = VnIngestionRepository(conn)

    repo.create_metadata_raw({"title": "A", "version": "1.0"}, file_id=7, release_id=3)
    repo.create_metadata_raw({"title": "A", "version": "1.1"}, file_id=8, release_id=3)

    rows = conn.execute(
        """
        SELECT release_id, file_id, raw_json, version_number, raw_sha256
        FROM revision
        WHERE release_id = 3
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


def test_repository_populates_series_and_maps_id():
    conn = make_conn_new_schema()
    repo = VnIngestionRepository(conn)

    metadata = {
        "title": "Series VN 1",
        "version": "1.0",
        "series": "Epic Saga",
        "series_description": "The first book in the saga",
    }
    title_id = repo.get_or_create_title(metadata)
    repo.get_or_create_release(title_id, metadata)

    # Verify series was created
    series_row = conn.execute("SELECT series_id, name, description FROM series").fetchone()
    assert series_row is not None
    assert series_row["name"] == "Epic Saga"
    assert series_row["description"] == "The first book in the saga"
    series_id = series_row["series_id"]

    # Verify title is linked to the series
    title_row = conn.execute("SELECT series_id FROM title WHERE title_id = ?", (title_id,)).fetchone()
    assert title_row["series_id"] == series_id

    # Verify updating description of the series with another release
    metadata_2 = {
        "title": "Series VN 2",
        "version": "1.0",
        "series": "Epic Saga",
        "series_description": "Updated series description",
    }
    title_id_2 = repo.get_or_create_title(metadata_2)
    repo.get_or_create_release(title_id_2, metadata_2)

    series_row_2 = conn.execute("SELECT description FROM series WHERE series_id = ?", (series_id,)).fetchone()
    assert series_row_2["description"] == "Updated series description"

    title_row_2 = conn.execute("SELECT series_id FROM title WHERE title_id = ?", (title_id_2,)).fetchone()
    assert title_row_2["series_id"] == series_id


def test_repository_release_lookup_matches_documented_unique_identity():
    conn = make_conn_new_schema()
    repo = VnIngestionRepository(conn)
    title_id = repo.get_or_create_title({"title": "Identity VN"})

    base_release_id = repo.get_or_create_release(
        title_id,
        {
            "version": "1.0",
            "language": "EN",
            "edition": "standard",
            "distribution_platform": "steam",
            "build_type": "full",
            "target_platform": "windows",
        },
    )

    # Build/platform differences should not alter identity lookup.
    same_identity_release_id = repo.get_or_create_release(
        title_id,
        {
            "version": "1.0",
            "language": "EN",
            "edition": "standard",
            "distribution_platform": "steam",
            "build_type": "patch",
            "target_platform": "linux",
        },
    )
    assert same_identity_release_id == base_release_id

    # Edition change should produce a distinct release identity.
    new_edition_release_id = repo.get_or_create_release(
        title_id,
        {
            "version": "1.0",
            "language": "EN",
            "edition": "limited",
            "distribution_platform": "steam",
        },
    )
    assert new_edition_release_id != base_release_id


def test_repository_release_lookup_normalizes_v_prefix_in_version():
    conn = make_conn_new_schema()
    repo = VnIngestionRepository(conn)
    title_id = repo.get_or_create_title({"title": "Version VN"})

    release_id = repo.get_or_create_release(
        title_id,
        {
            "version": "v1.2",
            "language": "EN",
            "edition": "standard",
            "distribution_platform": "itch.io",
        },
    )
    looked_up = repo.get_or_create_release(
        title_id,
        {
            "version": "1.2",
            "language": "EN",
            "edition": "standard",
            "distribution_platform": "itch.io",
        },
    )
    assert looked_up == release_id
