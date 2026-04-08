import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
pytest.importorskip("yaml")

from vn_archiver import upsert_artifact_record


def make_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE artifacts (
            artifact_id INTEGER PRIMARY KEY AUTOINCREMENT,
            build_id INTEGER NOT NULL,
            artifact_type TEXT NOT NULL,
            filename TEXT,
            sha256 TEXT NOT NULL,
            file_object_sha256 TEXT,
            base_artifact_id INTEGER,
            release_date TEXT,
            notes TEXT
        );
        """
    )
    return conn


def test_patch_requires_base_when_none_exists():
    conn = make_conn()
    with pytest.raises(ValueError, match="require"):
        upsert_artifact_record(
            conn,
            1,
            {"artifact_type": "patch"},
            {"sha256": "patch-sha", "filename": "p.zip"},
        )


def test_patch_auto_links_single_base_artifact():
    conn = make_conn()
    conn.execute(
        "INSERT INTO artifacts (build_id, artifact_type, filename, sha256, file_object_sha256, base_artifact_id) VALUES (?, ?, ?, ?, ?, ?)",
        (1, "game_archive", "base.zip", "base-sha", None, None),
    )

    artifact_id = upsert_artifact_record(
        conn,
        1,
        {"artifact_type": "patch"},
        {"sha256": "patch-sha", "filename": "patch.zip"},
    )

    row = conn.execute("SELECT base_artifact_id FROM artifacts WHERE artifact_id = ?", (artifact_id,)).fetchone()
    assert row["base_artifact_id"] is not None


def test_patch_requires_disambiguation_with_multiple_bases():
    conn = make_conn()
    conn.executemany(
        "INSERT INTO artifacts (build_id, artifact_type, filename, sha256, file_object_sha256, base_artifact_id) VALUES (?, ?, ?, ?, ?, ?)",
        [
            (1, "game_archive", "baseA.zip", "base-sha-a", None, None),
            (1, "base_game", "baseB.zip", "base-sha-b", None, None),
        ],
    )

    with pytest.raises(ValueError, match="multiple base artifacts"):
        upsert_artifact_record(
            conn,
            1,
            {"artifact_type": "patch"},
            {"sha256": "patch-sha", "filename": "patch.zip"},
        )
