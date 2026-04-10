import sqlite3
from pathlib import Path


def test_db_schema_sql_initializes_core_tables():
    schema_path = Path(__file__).resolve().parents[1] / "db_schema.sql"
    sql = schema_path.read_text(encoding="utf-8")

    conn = sqlite3.connect(":memory:")
    conn.executescript(sql)

    tables = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    }

    assert {"vn", "build", "file", "build_file", "build_relation"}.issubset(tables)


def test_db_schema_sql_enforces_release_type_vocab():
    schema_path = Path(__file__).resolve().parents[1] / "db_schema.sql"
    sql = schema_path.read_text(encoding="utf-8")

    conn = sqlite3.connect(":memory:")
    conn.executescript(sql)

    conn.execute("INSERT INTO vn (vn_id, title) VALUES (?, ?)", (1, "Example"))
    conn.execute(
        "INSERT INTO build (build_id, vn_id, version, release_type) VALUES (?, ?, ?, ?)",
        (10, 1, "1.0", "full"),
    )

    try:
        conn.execute(
            "INSERT INTO build (build_id, vn_id, version, release_type) VALUES (?, ?, ?, ?)",
            (11, 1, "1.1", "nightly"),
        )
        raised = False
    except sqlite3.IntegrityError:
        raised = True

    assert raised
