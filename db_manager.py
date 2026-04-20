import contextlib
import os
import queue
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = "archive.db"
SCHEMA_PATH = "db_schema.sql"
BACKUP_DIR = "db_backups"
ENABLE_DATABASE_BACKUPS = False
BACKUP_DEBOUNCE_SECONDS = 1.0

_WRITE_PREFIXES = ("insert", "update", "delete", "replace", "create", "alter", "drop")
_backup_queue: "queue.Queue[object]" = queue.Queue()
_backup_worker_started = False
_backup_worker_lock = threading.Lock()


def _is_write_sql(sql: str | None) -> bool:
    if not sql:
        return False
    normalized = sql.strip().lower()
    if not normalized:
        return False
    if normalized.startswith(("begin", "commit", "rollback", "select", "pragma")):
        return False
    return normalized.startswith(_WRITE_PREFIXES)


def _backup_worker() -> None:
    while True:
        _backup_queue.get()
        try:
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


def _ensure_backup_worker_started() -> None:
    global _backup_worker_started
    with _backup_worker_lock:
        if _backup_worker_started:
            return
        thread = threading.Thread(target=_backup_worker, daemon=True)
        thread.start()
        _backup_worker_started = True


def queue_database_backup() -> None:
    if not ENABLE_DATABASE_BACKUPS:
        return
    _ensure_backup_worker_started()
    _backup_queue.put(object())


def create_database_backup() -> str | None:
    if not ENABLE_DATABASE_BACKUPS:
        return None

    db_file = Path(DB_PATH)
    if not db_file.exists():
        return None

    Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
    backup_name = f"archive_backup_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')}.db"
    backup_path = Path(BACKUP_DIR) / backup_name

    src = sqlite3.connect(str(db_file))
    dst = sqlite3.connect(str(backup_path))
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()

    return str(backup_path)


class ArchiverConnection(sqlite3.Connection):
    def execute(self, sql, parameters=()):
        result = super().execute(sql, parameters)
        if _is_write_sql(sql):
            queue_database_backup()
        return result

    def executemany(self, sql, seq_of_parameters):
        result = super().executemany(sql, seq_of_parameters)
        if _is_write_sql(sql):
            queue_database_backup()
        return result

    def executescript(self, sql_script):
        result = super().executescript(sql_script)
        if _is_write_sql(sql_script):
            queue_database_backup()
        return result


def get_connection():
    conn = sqlite3.connect(DB_PATH, factory=ArchiverConnection)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA temp_store = MEMORY;")
    return conn


def initialize_database(*, reset: bool = True):
    schema_file = Path(SCHEMA_PATH)
    if not schema_file.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_file}")

    db_file = Path(DB_PATH)
    if reset and db_file.exists():
        db_file.unlink()
        wal = Path(f"{DB_PATH}-wal")
        shm = Path(f"{DB_PATH}-shm")
        if wal.exists():
            wal.unlink()
        if shm.exists():
            shm.unlink()

    schema_sql = schema_file.read_text(encoding="utf-8")
    with get_connection() as conn:
        conn.executescript(schema_sql)

        # Migration: backfill NULL identity columns in the release table.
        # The schema now enforces NOT NULL DEFAULT '' on these columns, but
        # rows created before the constraint was added may still have NULLs.
        for column in ("language", "edition", "distribution_platform"):
            conn.execute(
                f"UPDATE release SET {column} = '' WHERE {column} IS NULL"
            )
        conn.commit()


@contextlib.contextmanager
def exclusive_transaction(conn):
    conn.execute("BEGIN EXCLUSIVE TRANSACTION;")
    try:
        yield
        conn.commit()
    except Exception:
        conn.rollback()
        raise
