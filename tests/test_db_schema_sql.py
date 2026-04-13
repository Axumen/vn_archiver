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
    assert {"vn", "build", "file", "build_file"}.issubset(tables)


