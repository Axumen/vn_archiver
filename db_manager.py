import sqlite3
import os
import contextlib

DB_PATH = "archive.db"
SCHEMA_PATH = "db_schema.sql"

# Database is treated as fresh-initialized from db_schema.sql.
TARGET_SCHEMA_VERSION = 5


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
    """
    Initializes the database using the schema if it doesn't already exist.
    If it DOES exist, it checks if schema migrations are needed to update it.
    """
    is_new_db = not os.path.exists(DB_PATH)

    if is_new_db:
        print("Creating archive.db for the first time...")
        with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
            schema_sql = f.read()

        with get_connection() as conn:
            conn.executescript(schema_sql)
            # Stamp the fresh database with our target version
            conn.execute(f"PRAGMA user_version = {TARGET_SCHEMA_VERSION};")
        print("Database initialized successfully.")
    else:
        # Apply CREATE ... IF NOT EXISTS statements/triggers for existing DBs too.
        with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
            schema_sql = f.read()

        with get_connection() as conn:
            conn.executescript(schema_sql)
            conn.execute(f"PRAGMA user_version = {TARGET_SCHEMA_VERSION};")


def _column_exists(conn, table_name, column_name):
    cols = conn.execute(f"PRAGMA table_info({table_name});").fetchall()
    return any(col[1] == column_name for col in cols)


def run_migrations(conn, current_version):
    """
    No-op: migrations are intentionally disabled in the current fresh-schema workflow.
    """
    with exclusive_transaction(conn):
        # Stamp DB at the single supported schema version.
        conn.execute(f"PRAGMA user_version = {TARGET_SCHEMA_VERSION};")


def cleanup_orphaned_metadata(conn):
    """
    Garbage collection: Removes orphaned JSON blobs from metadata_objects
    that are no longer referenced by any metadata_versions.

    Returns the number of deleted blobs.
    """
    cursor = conn.cursor()
    cursor.execute('''
        DELETE FROM metadata_objects
        WHERE hash NOT IN (SELECT metadata_hash FROM metadata_versions);
    ''')
    deleted_count = cursor.rowcount
    conn.commit()
    return deleted_count


@contextlib.contextmanager
def exclusive_transaction(conn):
    """
    Context manager to run operations within an EXCLUSIVE TRANSACTION.
    This locks the database to safely perform read-modify-write operations
    without race conditions (e.g., SELECT MAX(version_number) + 1).
    """
    conn.execute("BEGIN EXCLUSIVE TRANSACTION;")
    try:
        yield
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
