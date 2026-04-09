import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from domain_layer import Artifact, Build, VN, Version, VisualNovelDomainService


class FakeRepository:
    def __init__(self):
        self.calls = []

    def resolve_existing_build_for_artifact(self, metadata):
        self.calls.append(("artifact", metadata["title"]))
        return 55, 77

    def upsert_vn_and_build(self, metadata):
        self.calls.append(("build", metadata["title"]))
        return 11, 22


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
    captured = {}
    repo = FakeRepository()

    def process_archives(conn, build_id, metadata, vn_id, archives_to_process):
        captured["args"] = (build_id, metadata["title"], vn_id, archives_to_process)

    service = VisualNovelDomainService(
        conn=object(),
        repository=repo,
        is_artifact_metadata=lambda _: False,
        collect_archives_for_db=lambda _: ([{"sha256": "abc"}], "abc"),
        process_archives_for_build=process_archives,
    )

    result = service.ingest({"title": "Sample VN", "version": "1.0"})

    assert result.vn_id == 11
    assert result.build_id == 22
    assert repo.calls == [("build", "Sample VN")]
    assert captured["args"] == (22, "Sample VN", 11, [{"sha256": "abc"}])
    assert result.artifact is not None
    assert result.artifact.file_sha256 == "abc"
    assert result.version is not None
    assert result.version.version_string == "1.0"
    assert result.vn is not None
    assert result.vn.canonical_title == "Sample VN"


def test_ingest_uses_artifact_branch():
    repo = FakeRepository()
    called = {"processed": False}

    service = VisualNovelDomainService(
        conn=object(),
        repository=repo,
        is_artifact_metadata=lambda _: True,
        collect_archives_for_db=lambda _: ([], None),
        process_archives_for_build=lambda *args, **kwargs: called.update(processed=True),
    )

    result = service.ingest({"title": "Sample Patch"})

    assert result.vn_id == 55
    assert result.build_id == 77
    assert repo.calls == [("artifact", "Sample Patch")]
    assert called["processed"] is True
    assert result.artifact is not None
    assert result.version is not None
    assert result.vn is not None


def test_domain_entities_model_file_to_artifact_to_version_to_vn():
    vn = VN(canonical_title="Example VN", developer="Dev Team", publisher="Pub Team")
    build = Build(build_id=10, vn_id=20, version_string="2.0")
    version = Version(version_string="2.0", vn=vn, build=build)
    artifact = Artifact(file_sha256="deadbeef", version=version, artifact_type="archive")

    assert artifact.file_sha256 == "deadbeef"
    assert artifact.version.version_string == "2.0"
    assert artifact.version.vn.canonical_title == "Example VN"
