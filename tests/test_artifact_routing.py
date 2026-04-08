import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
pytest.importorskip("yaml")

from vn_archiver import is_artifact_metadata


def test_is_artifact_metadata_requires_artifact_type():
    assert is_artifact_metadata({"title": "VN", "content_type": "artifact"}) is False
    assert is_artifact_metadata({"title": "Patch", "artifact_type": "patch"}) is True
