import pytest
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from domain_layer import Release, Title, Version, VisualNovelDomainService


class FakeRepository:
    def __init__(self):
        self.calls = []
        self.created_artifacts = []
        self.raw_metadata_records = []

    def get_or_create_title(self, metadata):
        self.calls.append(("title", metadata["title"]))
        return 11

    def get_or_create_release(self, title_id, metadata):
        self.calls.append(("release", title_id, metadata["title"]))
        return 22

    def create_file_link(self, release_id, metadata, archive_data):
        self.created_artifacts.append(
            (
                release_id,
                archive_data.get("sha256"),
                archive_data.get("filepath") or archive_data.get("filename"),
            )
        )
        return 999

    def create_metadata_raw(self, raw_payload, artifact_id, release_id=None):
        self.raw_metadata_records.append((raw_payload, artifact_id, release_id))


def test_ingest_requires_title():
    repo = FakeRepository()
    service = VisualNovelDomainService(
        conn=object(),
        repository=repo,
        collect_archives_for_db=lambda _: ([], None),
    )

    with pytest.raises(ValueError, match="Title is required"):
        service.ingest({})


def test_ingest_uses_release_branch_for_non_artifact():
    repo = FakeRepository()

    service = VisualNovelDomainService(
        conn=object(),
        repository=repo,
        collect_archives_for_db=lambda _: ([{"sha256": "abc", "filename": "sample.zip"}], "abc"),
    )

    result = service.ingest({"title": "Sample VN", "version": "1.0"})

    assert result.title_id == 11
    assert result.release_id == 22
    assert repo.calls == [("title", "Sample VN"), ("release", 11, "Sample VN")]
    assert repo.created_artifacts == [(22, "abc", "sample.zip")]
    assert result.release is not None
    assert result.release.version.version_string == "1.0"
    assert result.title is not None
    assert result.title.canonical_title == "Sample VN"


def test_ingest_routes_all_ingests_through_get_or_create_title_and_release():
    repo = FakeRepository()
    called = {"processed": False}

    service = VisualNovelDomainService(
        conn=object(),
        repository=repo,
        collect_archives_for_db=lambda _: ([], None),
    )

    result = service.ingest({"title": "Sample Patch", "sha256": "patch-sha"})

    assert result.title_id == 11
    assert result.release_id == 22
    assert repo.calls == [("title", "Sample Patch"), ("release", 11, "Sample Patch")]
    assert called["processed"] is False
    assert repo.created_artifacts == []
    assert result.release is not None
    assert result.title is not None


def test_ingest_normalizes_version_language_and_creator_before_resolution():
    repo = FakeRepository()
    captured = {}

    def get_or_create_title(metadata):
        captured["metadata"] = metadata
        return 1

    def get_or_create_release(title_id, metadata):
        return 2

    repo.get_or_create_title = get_or_create_title
    repo.get_or_create_release = get_or_create_release

    service = VisualNovelDomainService(
        conn=object(),
        repository=repo,
        collect_archives_for_db=lambda _: ([{"sha256": "abc", "filename": "sample.zip"}], "abc"),
    )

    service.ingest(
        {
            "title": "Clannad",
            "creator": "Key",
            "version": "v1.0",
            "language": "jp",
            "release_type": "original",
        }
    )

    assert captured["metadata"]["developer"] == "Key"
    assert captured["metadata"]["version"] == "1.0"
    assert captured["metadata"]["normalized_version"] == "1.0"
    assert captured["metadata"]["language"] == "JP"


def test_ingest_persists_raw_metadata_with_primary_artifact_id_when_present():
    repo = FakeRepository()
    service = VisualNovelDomainService(
        conn=object(),
        repository=repo,
        collect_archives_for_db=lambda _: ([{"sha256": "abc", "filename": "sample.zip"}], "abc"),
    )

    service.ingest(
        {
            "title": "Clannad",
            "version": "1.0",
            "_raw_text": "title: Clannad\nversion: 1.0\n",
            "_source_file": "incoming/clannad_v1.yaml",
        }
    )

    assert repo.raw_metadata_records == [
        ({"title": "Clannad", "version": "1.0", "normalized_version": "1.0"}, 999, 22)
    ]


def test_ingest_skips_raw_metadata_persistence_when_no_artifact_id_available():
    repo = FakeRepository()
    service = VisualNovelDomainService(
        conn=object(),
        repository=repo,
        collect_archives_for_db=lambda _: ([], None),
    )

    # With no archives and no sha256 in metadata, ingest should succeed
    # but skip raw metadata persistence (no file_id available).
    result = service.ingest(
        {
            "title": "MetadataOnly",
            "version": "1.0",
            "_raw_text": "title: MetadataOnly\nversion: 1.0\n",
            "_source_file": "incoming/metadata_only.yaml",
        }
    )

    assert result.title_id == 11
    assert result.release_id == 22
    assert repo.raw_metadata_records == []


def test_domain_entities_model_release_to_version_to_title():
    title = Title(canonical_title="Example VN", developer="Dev Team", publisher="Pub Team")
    version = Version(version_string="2.0", normalized_version="2.0")
    release = Release(release_id=10, title_id=20, version=version, release_type="full")

    assert release.version.version_string == "2.0"
    assert release.release_type == "full"
    assert title.canonical_title == "Example VN"


def test_artifact_uses_metadata_sha256_when_archive_list_is_empty():
    repo = FakeRepository()
    service = VisualNovelDomainService(
        conn=object(),
        repository=repo,
        collect_archives_for_db=lambda _: ([], None),
    )

    result = service.ingest({"title": "Patch", "sha256": "from-metadata"})

    assert result.release is not None


def test_ingest_succeeds_without_files_when_no_sha256_available():
    repo = FakeRepository()
    service = VisualNovelDomainService(
        conn=object(),
        repository=repo,
        collect_archives_for_db=lambda _: ([], None),
    )

    # Ingest should succeed even without any files or sha256.
    # The release is created but no file links or raw metadata are persisted.
    result = service.ingest({"title": "Patch Without Files"})
    assert result.title_id == 11
    assert result.release_id == 22
    assert repo.created_artifacts == []
    assert repo.raw_metadata_records == []


def test_ingest_rejects_duplicate_archive_sha256_values():
    repo = FakeRepository()
    service = VisualNovelDomainService(
        conn=object(),
        repository=repo,
        collect_archives_for_db=lambda _: (
            [{"sha256": "dup"}, {"sha256": "dup"}],
            "dup",
        ),
    )

    with pytest.raises(ValueError, match="Duplicate file sha256"):
        service.ingest({"title": "Duplicate SHA VN"})


def test_ingest_skips_legacy_archive_processing_when_files_table_is_absent():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE vn (id INTEGER PRIMARY KEY, title TEXT NOT NULL)")
    conn.execute(
        "CREATE TABLE builds (id INTEGER PRIMARY KEY, vn_id INTEGER NOT NULL, version_string TEXT, release_type TEXT, language TEXT, platform TEXT)"
    )
    conn.execute(
        "CREATE TABLE artifacts (id INTEGER PRIMARY KEY, build_id INTEGER, sha256 TEXT NOT NULL UNIQUE, path TEXT NOT NULL, type TEXT)"
    )
    conn.execute(
        "CREATE TABLE metadata_raw (id INTEGER PRIMARY KEY, artifact_id INTEGER, source_file TEXT, raw_text TEXT NOT NULL)"
    )

    class SqliteRepo(FakeRepository):
        def __init__(self, conn):
            super().__init__()
            self.conn = conn

        def get_or_create_title(self, metadata):
            row = self.conn.execute("SELECT id FROM vn WHERE title = ?", (metadata["title"],)).fetchone()
            if row:
                return row["id"]
            self.conn.execute("INSERT INTO vn (title) VALUES (?)", (metadata["title"],))
            return self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        def get_or_create_release(self, title_id, metadata):
            row = self.conn.execute(
                "SELECT id FROM builds WHERE vn_id = ? AND version_string = ?",
                (title_id, metadata.get("version") or "1.0"),
            ).fetchone()
            if row:
                return row["id"]
            self.conn.execute(
                "INSERT INTO builds (vn_id, version_string, release_type, language, platform) VALUES (?, ?, ?, ?, ?)",
                (title_id, metadata.get("version") or "1.0", metadata.get("release_type"), metadata.get("language"), metadata.get("platform")),
            )
            return self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        def create_file_link(self, release_id, metadata, archive_data):
            self.conn.execute(
                "INSERT INTO artifacts (build_id, sha256, path, type) VALUES (?, ?, ?, ?)",
                (release_id, archive_data["sha256"], archive_data.get("filename") or archive_data.get("filepath"), metadata.get("artifact_type") or "game_archive"),
            )
            return self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        def create_metadata_raw(self, raw_payload, artifact_id, release_id=None):
            self.conn.execute(
                "INSERT INTO metadata_raw (artifact_id, source_file, raw_text) VALUES (?, ?, ?)",
                (artifact_id, None, str(raw_payload)),
            )

    repo = SqliteRepo(conn)
    called = {"processed": False}
    service = VisualNovelDomainService(
        conn=conn,
        repository=repo,
        collect_archives_for_db=lambda _: ([{"sha256": "abc", "filename": "sample.zip"}], "abc"),
    )

    result = service.ingest({"title": "Clannad", "version": "1.0"})
    assert result.release_id is not None
    assert called["processed"] is False
