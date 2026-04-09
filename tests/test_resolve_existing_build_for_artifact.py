import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
pytest.importorskip("yaml")

from vn_archiver import resolve_existing_build_for_artifact


def make_conn_with_vn_table_only():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE vn (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE builds (
            id INTEGER PRIMARY KEY,
            vn_id INTEGER NOT NULL,
            version_string TEXT,
            release_type TEXT,
            language TEXT,
            platform TEXT
        );
        """
    )
    return conn


def test_resolve_existing_build_for_artifact_supports_vn_table_without_visual_novels():
    conn = make_conn_with_vn_table_only()
    conn.execute("INSERT INTO vn (id, title) VALUES (?, ?)", (10, "Clannad"))
    conn.execute(
        "INSERT INTO builds (id, vn_id, version_string, release_type, language, platform) VALUES (?, ?, ?, ?, ?, ?)",
        (20, 10, "1.0", "original", "JP", "windows"),
    )

    vn_id, build_id = resolve_existing_build_for_artifact(
        conn,
        {
            "title": "Clannad",
            "version": "1.0",
            "release_type": "original",
            "language": "JP",
        },
    )

    assert vn_id == 10
    assert build_id == 20
