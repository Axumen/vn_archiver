import sqlite3
import os

DB_PATH = "archive.db"
SCHEMA_PATH = "db_schema.sql"


def get_connection():
    """
    Returns a SQLite connection configured for the VN Archives project.
    Foreign keys and performance pragmas are enabled.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Enforce relational integrity
    conn.execute("PRAGMA foreign_keys = ON;")

    # Performance + safety balance
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA temp_store = MEMORY;")

    return conn


def initialize_database():
    if not os.path.exists(DB_PATH):
        print("Creating archive.db...")
        with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
            schema_sql = f.read()

        conn = get_connection()
        conn.executescript(schema_sql)
        conn.commit()
        conn.close()
        print("Database initialized.")