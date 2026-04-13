import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

pytest.importorskip("yaml")
import yaml

from canonical_schema import DOMAIN_TABLES, table_names


def _allowed_metadata_fields():
    template_path = Path(__file__).resolve().parents[1] / "metadata" / "metadata_v1.yaml"
    payload = yaml.safe_load(template_path.read_text(encoding="utf-8"))

    allowed = set(payload.get("required") or []) | set(payload.get("optional") or [])
    archives = payload.get("archives") or []
    if archives and isinstance(archives[0], dict):
        for key in archives[0].keys():
            allowed.add(f"archives.{key}")
    return allowed


def test_domain_tables_follow_title_release_file_core_order():
    assert table_names() == ["series", "title", "release", "file", "file_snapshot"]


def test_domain_columns_are_based_on_metadata_v1_fields():
    allowed = _allowed_metadata_fields()
    internal = {
        "id", "title_id", "release_id", "from_release_id", "series_id",
        "file_id", "metadata_id", "metadata_version", "relation_id",
        "series_description", "normalized_version", "created_at",
        "metadata_json", "platform", "source_url", "size_bytes",
    }

    for table in DOMAIN_TABLES:
        for column in table.columns:
            assert column.source_field in allowed or column.source_field in internal, (
                f"Column {table.name}.{column.name} has unknown source_field: {column.source_field}"
            )
