import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
pytest.importorskip("yaml")

from vn_archiver import (
    CATEGORY_ALL_FIELDS,
    FIELD_SUGGESTIONS,
    load_metadata_template,
)


def _template_fields(template):
    required = [f for f in (template.get("required") or []) if isinstance(f, str)]
    optional = [f for f in (template.get("optional") or []) if isinstance(f, str)]
    return set(required + optional)


def test_template_fields_are_known_to_normalizer():
    vn_fields = _template_fields(load_metadata_template(1))
    for field in sorted(vn_fields):
        assert field in CATEGORY_ALL_FIELDS, f"Template field not recognized by normalizer: {field}"


def test_field_suggestions_match_template_fields():
    vn_fields = _template_fields(load_metadata_template(1))
    for field in FIELD_SUGGESTIONS:
        assert field in vn_fields, f"Suggestion field is not present in templates: {field}"
