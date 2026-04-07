import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from domain_layer import VisualNovelDomainService


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

    result = service.ingest({"title": "Sample VN"})

    assert result.vn_id == 11
    assert result.build_id == 22
    assert repo.calls == [("build", "Sample VN")]
    assert captured["args"] == (22, "Sample VN", 11, [{"sha256": "abc"}])


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
