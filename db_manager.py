import sqlite3
import os
import contextlib

DB_PATH = "archive.db"
SCHEMA_PATH = "db_schema.sql"

# Define the current required version of your database schema
# Increment this number by 1 every time you make a change to db_schema.sql!
TARGET_SCHEMA_VERSION = 2


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
        # The DB exists. Let's check if it needs an upgrade!
        with get_connection() as conn:
            current_version = conn.execute("PRAGMA user_version;").fetchone()[0]

            if current_version < TARGET_SCHEMA_VERSION:
                print(f"Upgrading database from v{current_version} to v{TARGET_SCHEMA_VERSION}...")
                run_migrations(conn, current_version)
                print("Database upgrade complete!")


def run_migrations(conn, current_version):
    """
    Applies incremental updates to the database schema.
    """
    with exclusive_transaction(conn):
        if current_version < 2:
            conn.execute("ALTER TABLE visual_novels ADD COLUMN description TEXT;")
            conn.execute("ALTER TABLE visual_novels ADD COLUMN source TEXT;")
            conn.execute("ALTER TABLE builds ADD COLUMN source TEXT;")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_archives_sha256 ON archives(sha256);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_canon_parent ON canon_relationships(parent_vn_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_canon_child ON canon_relationships(child_vn_id);")
            current_version = 2
        
        # Finally, stamp the DB with the newest version
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
