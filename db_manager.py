import sqlite3
import os
import contextlib
import queue
import threading
import time
from datetime import datetime

DB_PATH = "archive.db"
SCHEMA_PATH = "db_schema.sql"
BACKUP_DIR = "db_backups"
ENABLE_DATABASE_BACKUPS = False

# Database is treated as fresh-initialized from db_schema.sql.
TARGET_SCHEMA_VERSION = 6
BACKUP_DEBOUNCE_SECONDS = 1.0

WRITE_SQL_PREFIXES = (
    "insert",
    "update",
    "delete",
    "replace",
    "create",
    "alter",
    "drop",
)

_backup_queue = queue.Queue()
_backup_worker_started = False
_backup_worker_lock = threading.Lock()


def _should_backup_for_sql(sql):
    if not sql:
        return False

    normalized = sql.strip().lower()
    if not normalized:
        return False

    # Ignore transaction control statements and read-only PRAGMAs.
    if normalized.startswith(("begin", "commit", "rollback", "select")):
        return False

    return normalized.startswith(WRITE_SQL_PREFIXES)


def _ensure_backup_worker_started():
    global _backup_worker_started

    with _backup_worker_lock:
        if _backup_worker_started:
            return

        worker = threading.Thread(target=_backup_worker, daemon=True)
        worker.start()
        _backup_worker_started = True


def _backup_worker():
    while True:
        _backup_queue.get()
        try:
            # Debounce bursty write activity (e.g., insertion loops)
            # into a single backup snapshot.
            time.sleep(BACKUP_DEBOUNCE_SECONDS)
            while True:
                try:
                    _backup_queue.get_nowait()
                    _backup_queue.task_done()
                except queue.Empty:
                    break
            create_database_backup()
        finally:
            _backup_queue.task_done()


def queue_database_backup():
    if not ENABLE_DATABASE_BACKUPS:
        return

    _ensure_backup_worker_started()
    _backup_queue.put(object())


def create_database_backup():
    if not ENABLE_DATABASE_BACKUPS:
        return None

    if not os.path.exists(DB_PATH):
        return None

    os.makedirs(BACKUP_DIR, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    backup_path = os.path.join(BACKUP_DIR, f"archive_backup_{timestamp}.db")

    source_conn = sqlite3.connect(DB_PATH)
    backup_conn = sqlite3.connect(backup_path)
    try:
        source_conn.backup(backup_conn)
    finally:
        backup_conn.close()
        source_conn.close()

    return backup_path


class ArchiverCursor(sqlite3.Cursor):
    def execute(self, sql, parameters=()):
        result = super().execute(sql, parameters)
        self.connection._trigger_backup_if_needed(sql)
        return result

    def executemany(self, sql, seq_of_parameters):
        result = super().executemany(sql, seq_of_parameters)
        self.connection._trigger_backup_if_needed(sql)
        return result

    def executescript(self, sql_script):
        result = super().executescript(sql_script)
        self.connection._trigger_backup_if_needed(sql_script)
        return result


class ArchiverConnection(sqlite3.Connection):
    def cursor(self, factory=ArchiverCursor):
        return super().cursor(factory=factory)

    def _trigger_backup_if_needed(self, sql):
        if _should_backup_for_sql(sql):
            queue_database_backup()

WRITE_SQL_PREFIXES = (
    "insert",
    "update",
    "delete",
    "replace",
    "create",
    "alter",
    "drop",
)

_backup_queue = queue.Queue()
_backup_worker_started = False
_backup_worker_lock = threading.Lock()


def _should_backup_for_sql(sql):
    if not sql:
        return False

    normalized = sql.strip().lower()
    if not normalized:
        return False

    # Ignore transaction control statements and read-only PRAGMAs.
    if normalized.startswith(("begin", "commit", "rollback", "select")):
        return False

    return normalized.startswith(WRITE_SQL_PREFIXES)


def _ensure_backup_worker_started():
    global _backup_worker_started

    with _backup_worker_lock:
        if _backup_worker_started:
            return

        worker = threading.Thread(target=_backup_worker, daemon=True)
        worker.start()
        _backup_worker_started = True


def _backup_worker():
    while True:
        _backup_queue.get()
        try:
            create_database_backup()
        finally:
            _backup_queue.task_done()


def queue_database_backup():
    if not ENABLE_DATABASE_BACKUPS:
        return

    _ensure_backup_worker_started()
    _backup_queue.put(object())


def create_database_backup():
    if not ENABLE_DATABASE_BACKUPS:
        return None

    if not os.path.exists(DB_PATH):
        return None

    os.makedirs(BACKUP_DIR, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    backup_path = os.path.join(BACKUP_DIR, f"archive_backup_{timestamp}.db")

    source_conn = sqlite3.connect(DB_PATH)
    backup_conn = sqlite3.connect(backup_path)
    try:
        source_conn.backup(backup_conn)
    finally:
        backup_conn.close()
        source_conn.close()

    return backup_path


class ArchiverCursor(sqlite3.Cursor):
    def execute(self, sql, parameters=()):
        result = super().execute(sql, parameters)
        self.connection._trigger_backup_if_needed(sql)
        return result

    def executemany(self, sql, seq_of_parameters):
        result = super().executemany(sql, seq_of_parameters)
        self.connection._trigger_backup_if_needed(sql)
        return result

    def executescript(self, sql_script):
        result = super().executescript(sql_script)
        self.connection._trigger_backup_if_needed(sql_script)
        return result


class ArchiverConnection(sqlite3.Connection):
    def cursor(self, factory=ArchiverCursor):
        return super().cursor(factory=factory)

    def execute(self, sql, parameters=()):
        result = super().execute(sql, parameters)
        self._trigger_backup_if_needed(sql)
        return result

    def executemany(self, sql, seq_of_parameters):
        result = super().executemany(sql, seq_of_parameters)
        self._trigger_backup_if_needed(sql)
        return result

    def executescript(self, sql_script):
        result = super().executescript(sql_script)
        self._trigger_backup_if_needed(sql_script)
        return result

    def _trigger_backup_if_needed(self, sql):
        if _should_backup_for_sql(sql):
            queue_database_backup()

WRITE_SQL_PREFIXES = (
    "insert",
    "update",
    "delete",
    "replace",
    "create",
    "alter",
    "drop",
)

_backup_queue = queue.Queue()
_backup_worker_started = False
_backup_worker_lock = threading.Lock()


def _should_track_write_sql(sql, is_script=False):
    if not sql:
        return False

    normalized = sql.strip().lower()
    if not normalized:
        return False

    if is_script:
        return any(prefix in normalized for prefix in WRITE_SQL_PREFIXES)

    # Ignore transaction control statements and read-only PRAGMAs.
    if normalized.startswith(("begin", "commit", "rollback", "select")):
        return False

    if normalized.startswith("pragma") and "=" not in normalized:
        return False

    return normalized.startswith(WRITE_SQL_PREFIXES)


def _ensure_backup_worker_started():
    global _backup_worker_started

    with _backup_worker_lock:
        if _backup_worker_started:
            return

        worker = threading.Thread(target=_backup_worker, daemon=True)
        worker.start()
        _backup_worker_started = True


def _backup_worker():
    while True:
        _backup_queue.get()
        try:
            create_database_backup()
        finally:
            _backup_queue.task_done()


def queue_database_backup():
    if not ENABLE_DATABASE_BACKUPS:
        return

    _ensure_backup_worker_started()
    _backup_queue.put(object())


def create_database_backup():
    if not ENABLE_DATABASE_BACKUPS:
        return None

    if not os.path.exists(DB_PATH):
        return None

    os.makedirs(BACKUP_DIR, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    backup_path = os.path.join(BACKUP_DIR, f"archive_backup_{timestamp}.db")

    source_conn = sqlite3.connect(DB_PATH)
    backup_conn = sqlite3.connect(backup_path)
    try:
        source_conn.backup(backup_conn)
    finally:
        backup_conn.close()
        source_conn.close()

    return backup_path


class ArchiverCursor(sqlite3.Cursor):
    def execute(self, sql, parameters=()):
        result = super().execute(sql, parameters)
        self.connection._mark_successful_write(sql, is_script=False)
        return result

    def executemany(self, sql, seq_of_parameters):
        result = super().executemany(sql, seq_of_parameters)
        self.connection._mark_successful_write(sql, is_script=False)
        return result

    def executescript(self, sql_script):
        result = super().executescript(sql_script)
        self.connection._mark_successful_write(sql_script, is_script=True)
        return result


class ArchiverConnection(sqlite3.Connection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._has_successful_write = False

    def cursor(self, factory=ArchiverCursor):
        return super().cursor(factory=factory)

    def _mark_successful_write(self, sql, is_script=False):
        if _should_track_write_sql(sql, is_script=is_script):
            self._has_successful_write = True

    def commit(self):
        super().commit()
        if self._has_successful_write:
            queue_database_backup()
            self._has_successful_write = False

    def rollback(self):
        super().rollback()
        self._has_successful_write = False

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            try:
                self.commit()
            except Exception:
                self.rollback()
                raise
        else:
            self.rollback()
        return False


def create_database_backup():
    """
    Creates a timestamped backup of archive.db.
    Returns the backup path, or None if backups are disabled or the main DB does not exist.
    """
    if not ENABLE_DATABASE_BACKUPS:
        return None

    if not os.path.exists(DB_PATH):
        return None

    os.makedirs(BACKUP_DIR, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    backup_path = os.path.join(BACKUP_DIR, f"archive_backup_{timestamp}.db")

    source_conn = sqlite3.connect(DB_PATH)
    backup_conn = sqlite3.connect(backup_path)
    try:
        source_conn.backup(backup_conn)
    finally:
        backup_conn.close()
        source_conn.close()

    return backup_path


class ArchiverConnection(sqlite3.Connection):
    """
    Connection wrapper for project-specific SQLite behavior.
    Database backups are currently disabled.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._last_total_changes = self.total_changes

    def commit(self):
        super().commit()
        self._last_total_changes = self.total_changes

    def rollback(self):
        super().rollback()
        self._last_total_changes = self.total_changes

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            try:
                self.commit()
            except Exception:
                self.rollback()
                raise
        else:
            self.rollback()
        return False


def get_connection():
    """
    Returns a SQLite connection configured for the VN Archives project.
    Foreign keys and performance pragmas are enabled.
    """
    conn = sqlite3.connect(DB_PATH, factory=ArchiverConnection)
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
            current_version = conn.execute("PRAGMA user_version;").fetchone()[0]
            conn.executescript(schema_sql)
            run_migrations(conn, current_version)


def _column_exists(conn, table_name, column_name):
    cols = conn.execute(f"PRAGMA table_info({table_name});").fetchall()
    return any(col[1] == column_name for col in cols)


def run_migrations(conn, current_version):
    """
    Apply incremental schema/data migrations.
    """
    with exclusive_transaction(conn):
        if current_version < 2:
            _migrate_change_note_fallback_rows(conn)
        if current_version < 3:
            _migrate_build_identity_index(conn)
        if current_version < 4:
            # v4 relies on schema re-application for artifact metadata version tables/indexes.
            pass
        if current_version < 5:
            _migrate_artifact_file_object_link(conn)
        if current_version < 6:
            _migrate_artifact_sha_uniqueness(conn)

        # Stamp DB at the current supported schema version.
        conn.execute(f"PRAGMA user_version = {TARGET_SCHEMA_VERSION};")


def _migrate_change_note_fallback_rows(conn):
    """
    Data migration for v2:
    clear metadata_versions.change_note rows that were implicitly copied from
    metadata.notes (legacy fallback behavior), unless metadata explicitly stored
    a non-empty change_note value.
    """
    conn.execute(
        """
        UPDATE metadata_versions
        SET change_note = NULL
        WHERE change_note IS NOT NULL
          AND EXISTS (
              SELECT 1
              FROM metadata_objects mo
              WHERE mo.hash = metadata_versions.metadata_hash
                AND json_valid(mo.metadata_json)
                AND COALESCE(NULLIF(TRIM(json_extract(mo.metadata_json, '$.change_note')), ''), NULL) IS NULL
                AND TRIM(COALESCE(json_extract(mo.metadata_json, '$.notes'), '')) = TRIM(metadata_versions.change_note)
          );
        """
    )


def _migrate_build_identity_index(conn):
    """
    Schema migration for v3:
    rebuild builds uniqueness index so build identity includes build_type
    and distribution_platform in addition to vn_id/version/language/edition.
    """
    conn.execute("DROP INDEX IF EXISTS idx_unique_build_release;")
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_build_release
        ON builds(
            vn_id,
            version,
            COALESCE(language, ''),
            COALESCE(build_type, ''),
            COALESCE(edition, ''),
            COALESCE(distribution_platform, '')
        );
        """
    )


def _migrate_artifact_file_object_link(conn):
    """
    Schema migration for v5:
    add artifacts.file_object_sha256 to explicitly link artifacts to file objects.
    """
    if not _column_exists(conn, "artifacts", "file_object_sha256"):
        conn.execute("ALTER TABLE artifacts ADD COLUMN file_object_sha256 TEXT;")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_artifacts_file_object_sha
        ON artifacts(file_object_sha256);
        """
    )


def _migrate_artifact_sha_uniqueness(conn):
    """
    Schema migration for v6:
    allow shared artifact sha256 across different builds while keeping
    per-build uniqueness for (build_id, sha256).
    """
    conn.execute("ALTER TABLE artifacts RENAME TO artifacts_legacy_v5;")
    conn.execute(
        """
        CREATE TABLE artifacts (
            id INTEGER PRIMARY KEY,
            build_id INTEGER,
            sha256 TEXT NOT NULL,
            path TEXT NOT NULL,
            type TEXT,
            file_object_sha256 TEXT,
            FOREIGN KEY (build_id) REFERENCES builds(id) ON DELETE CASCADE,
            UNIQUE (build_id, sha256)
        );
        """
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO artifacts (id, build_id, sha256, path, type, file_object_sha256)
        SELECT id, build_id, sha256, path, type, file_object_sha256
        FROM artifacts_legacy_v5;
        """
    )
    conn.execute("DROP TABLE artifacts_legacy_v5;")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_build_id ON artifacts(build_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_sha256 ON artifacts(sha256);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_file_object_sha ON artifacts(file_object_sha256);")


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
