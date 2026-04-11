import pytest
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from domain_layer import Artifact, Build, VN, Version, VisualNovelDomainService


class FakeRepository:
    def __init__(self):
        self.calls = []
        self.created_artifacts = []
        self.raw_metadata_records = []

    def get_or_create_vn(self, metadata):
        self.calls.append(("vn", metadata["title"]))
        return 11

    def get_or_create_build(self, vn_id, metadata):
        self.calls.append(("build", vn_id, metadata["title"]))
        return 22

    def create_artifact(self, build_id, metadata, archive_data):
        self.created_artifacts.append(
            (
                build_id,
                archive_data.get("sha256"),
                archive_data.get("filepath") or archive_data.get("filename"),
            )
        )
        return 999

    def create_metadata_raw(self, raw_text, source_file, artifact_id, build_id=None):
        self.raw_metadata_records.append((raw_text, source_file, artifact_id, build_id))


def test_ingest_requires_title():
    repo = FakeRepository()
    service = VisualNovelDomainService(
        conn=object(),
        repository=repo,
        is_artifact_metadata=lambda _: False,
        collect_archives_for_db=lambda _: ([], None),
        process_archives_for_build=lambda *args, **kwargs: None,
    )

    with pytest.raises(ValueError, match="Title is required"):
        service.ingest({})


def test_ingest_uses_build_branch_for_non_artifact():
    repo = FakeRepository()

    service = VisualNovelDomainService(
        conn=object(),
        repository=repo,
        is_artifact_metadata=lambda _: False,
        collect_archives_for_db=lambda _: ([{"sha256": "abc", "filename": "sample.zip"}], "abc"),
        process_archives_for_build=lambda *args, **kwargs: None,
    )

    result = service.ingest({"title": "Sample VN", "version": "1.0"})

    assert result.vn_id == 11
    assert result.build_id == 22
    assert repo.calls == [("vn", "Sample VN"), ("build", 11, "Sample VN")]
    assert repo.created_artifacts == [(22, "abc", "sample.zip")]
    assert result.artifact is not None
    assert result.artifact.file_sha256 == "abc"
    assert result.build is not None
    assert result.build.version.version_string == "1.0"
    assert result.vn is not None
    assert result.vn.canonical_title == "Sample VN"
    assert result.artifact_status == "classified"


def test_ingest_routes_all_ingests_through_get_or_create_vn_and_build():
    repo = FakeRepository()
    called = {"processed": False}

    service = VisualNovelDomainService(
        conn=object(),
        repository=repo,
        is_artifact_metadata=lambda _: True,
        collect_archives_for_db=lambda _: ([], None),
        process_archives_for_build=lambda *args, **kwargs: called.update(processed=True),
    )

    result = service.ingest({"title": "Sample Patch", "sha256": "patch-sha"})

    assert result.vn_id == 11
    assert result.build_id == 22
    assert repo.calls == [("vn", "Sample Patch"), ("build", 11, "Sample Patch")]
    assert called["processed"] is False
    assert repo.created_artifacts == []
    assert result.artifact is not None
    assert result.build is not None
    assert result.vn is not None
    assert result.artifact_status == "classified"


def test_ingest_normalizes_version_language_and_creator_before_resolution():
    repo = FakeRepository()
    captured = {}

    def get_or_create_vn(metadata):
        captured["metadata"] = metadata
        return 1

    def get_or_create_build(vn_id, metadata):
        return 2

    repo.get_or_create_vn = get_or_create_vn
    repo.get_or_create_build = get_or_create_build

    service = VisualNovelDomainService(
        conn=object(),
        repository=repo,
        is_artifact_metadata=lambda _: False,
        collect_archives_for_db=lambda _: ([{"sha256": "abc", "filename": "sample.zip"}], "abc"),
        process_archives_for_build=lambda *args, **kwargs: None,
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
        is_artifact_metadata=lambda _: False,
        collect_archives_for_db=lambda _: ([{"sha256": "abc", "filename": "sample.zip"}], "abc"),
        process_archives_for_build=lambda *args, **kwargs: None,
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
        ("title: Clannad\nversion: 1.0\n", "incoming/clannad_v1.yaml", 999, 22)
    ]


def test_ingest_skips_raw_metadata_persistence_when_no_artifact_id_available():
    repo = FakeRepository()
    service = VisualNovelDomainService(
        conn=object(),
        repository=repo,
        is_artifact_metadata=lambda _: False,
        collect_archives_for_db=lambda _: ([], None),
        process_archives_for_build=lambda *args, **kwargs: None,
    )

    with pytest.raises(ValueError, match="at least one Artifact sha256"):
        service.ingest(
            {
                "title": "MetadataOnly",
                "version": "1.0",
                "_raw_text": "title: MetadataOnly\nversion: 1.0\n",
                "_source_file": "incoming/metadata_only.yaml",
            }
        )

    assert repo.raw_metadata_records == []


def test_domain_entities_model_file_to_artifact_to_build_to_version_to_vn():
    vn = VN(canonical_title="Example VN", developer="Dev Team", publisher="Pub Team")
    version = Version(version_string="2.0", normalized_version="2.0")
    build = Build(build_id=10, vn_id=20, version=version, release_type="full")
    artifact = Artifact(file_sha256="deadbeef", build_id=build.build_id, artifact_type="archive")

    assert artifact.file_sha256 == "deadbeef"
    assert build.version.version_string == "2.0"
    assert build.release_type == "full"
    assert vn.canonical_title == "Example VN"


def test_artifact_uses_metadata_sha256_when_archive_list_is_empty():
    repo = FakeRepository()
    service = VisualNovelDomainService(
        conn=object(),
        repository=repo,
        is_artifact_metadata=lambda _: True,
        collect_archives_for_db=lambda _: ([], None),
        process_archives_for_build=lambda *args, **kwargs: None,
    )

    result = service.ingest({"title": "Patch", "sha256": "from-metadata"})

    assert result.artifact is not None
    assert result.artifact.file_sha256 == "from-metadata"
    assert result.build is not None
    assert result.artifact.build_id == result.build.build_id


def test_ingest_requires_artifact_sha256_to_satisfy_build_invariant():
    repo = FakeRepository()
    service = VisualNovelDomainService(
        conn=object(),
        repository=repo,
        is_artifact_metadata=lambda _: True,
        collect_archives_for_db=lambda _: ([], None),
        process_archives_for_build=lambda *args, **kwargs: None,
    )

    with pytest.raises(ValueError, match="at least one Artifact sha256"):
        service.ingest({"title": "Patch Without Files"})


def test_ingest_rejects_duplicate_archive_sha256_values():
    repo = FakeRepository()
    service = VisualNovelDomainService(
        conn=object(),
        repository=repo,
        is_artifact_metadata=lambda _: False,
        collect_archives_for_db=lambda _: (
            [{"sha256": "dup"}, {"sha256": "dup"}],
            "dup",
        ),
        process_archives_for_build=lambda *args, **kwargs: None,
    )

    with pytest.raises(ValueError, match="Duplicate artifact sha256"):
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

        def get_or_create_vn(self, metadata):
            row = self.conn.execute("SELECT id FROM vn WHERE title = ?", (metadata["title"],)).fetchone()
            if row:
                return row["id"]
            self.conn.execute("INSERT INTO vn (title) VALUES (?)", (metadata["title"],))
            return self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        def get_or_create_build(self, vn_id, metadata):
            row = self.conn.execute(
                "SELECT id FROM builds WHERE vn_id = ? AND version_string = ?",
                (vn_id, metadata.get("version") or "1.0"),
            ).fetchone()
            if row:
                return row["id"]
            self.conn.execute(
                "INSERT INTO builds (vn_id, version_string, release_type, language, platform) VALUES (?, ?, ?, ?, ?)",
                (vn_id, metadata.get("version") or "1.0", metadata.get("release_type"), metadata.get("language"), metadata.get("platform")),
            )
            return self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        def create_artifact(self, build_id, metadata, archive_data):
            self.conn.execute(
                "INSERT INTO artifacts (build_id, sha256, path, type) VALUES (?, ?, ?, ?)",
                (build_id, archive_data["sha256"], archive_data.get("filename") or archive_data.get("filepath"), metadata.get("artifact_type") or "game_archive"),
            )
            return self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        def create_metadata_raw(self, raw_text, source_file, artifact_id):
            self.conn.execute(
                "INSERT INTO metadata_raw (artifact_id, source_file, raw_text) VALUES (?, ?, ?)",
                (artifact_id, source_file, raw_text),
            )

    repo = SqliteRepo(conn)
    called = {"processed": False}
    service = VisualNovelDomainService(
        conn=conn,
        repository=repo,
        is_artifact_metadata=lambda _: False,
        collect_archives_for_db=lambda _: ([{"sha256": "abc", "filename": "sample.zip"}], "abc"),
        process_archives_for_build=lambda *args, **kwargs: called.update(processed=True),
    )

    result = service.ingest({"title": "Clannad", "version": "1.0"})
    assert result.build_id is not None
    assert called["processed"] is False
