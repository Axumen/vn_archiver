"""Repository layer for cloud upload tracking."""

class CloudTrackingRepository:
    def __init__(self, conn):
        self.conn = conn

    def is_archive_uploaded(self, sha256):
        """Check if an archive is already uploaded to the cloud."""
        row = self.conn.execute(
            "SELECT 1 FROM cloud_archive WHERE sha256 = ?",
            (sha256,)
        ).fetchone()
        return row is not None

    def is_sidecar_uploaded(self, sha256):
        """Check if a metadata sidecar is already uploaded to the cloud."""
        row = self.conn.execute(
            "SELECT 1 FROM cloud_sidecar WHERE sha256 = ?",
            (sha256,)
        ).fetchone()
        return row is not None

    def get_sidecar_storage_path(self, sha256):
        """Get the storage path of a metadata sidecar if it is uploaded."""
        row = self.conn.execute(
            "SELECT storage_path FROM cloud_sidecar WHERE sha256 = ?",
            (sha256,)
        ).fetchone()
        if row:
            return row["storage_path"]
        return None

    def mark_archive_uploaded(self, sha256, file_size, storage_path):
        """Record an archive as uploaded."""
        self.conn.execute(
            '''
            INSERT OR IGNORE INTO cloud_archive (sha256, file_size, storage_path)
            VALUES (?, ?, ?)
            ''',
            (sha256, file_size, storage_path)
        )

    def mark_sidecar_uploaded(self, sha256, file_size, storage_path):
        """Record a metadata sidecar as uploaded."""
        self.conn.execute(
            '''
            INSERT OR IGNORE INTO cloud_sidecar (sha256, file_size, storage_path)
            VALUES (?, ?, ?)
            ''',
            (sha256, file_size, storage_path)
        )
