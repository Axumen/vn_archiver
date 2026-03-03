#!/usr/bin/env python3
"""Rebuild normalized metadata tables from metadata_log_book payloads."""

from db_manager import get_connection, initialize_database
from vn_archiver import rebuild_projections_from_metadata_log_book


def main():
    initialize_database()
    with get_connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM metadata_log_book").fetchone()[0]
        rebuild_projections_from_metadata_log_book(conn)
    print(f"Rebuilt normalized tables from {count} metadata_log_book entries.")


if __name__ == "__main__":
    main()
