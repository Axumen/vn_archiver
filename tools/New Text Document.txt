import sqlite3
import os

DB_PATH = "archive.db"
SCHEMA_PATH = "db_schema.sql"


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Performance + integrity settings
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA temp_store = MEMORY;")

    return conn


def initialize_database():
    first_time = not os.path.exists(DB_PATH)

    conn = get_connection()

    if first_time:
        print("Creating archive.db...")
        with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
            conn.executescript(f.read())
        conn.commit()
        print("Database initialized.")

    conn.close()