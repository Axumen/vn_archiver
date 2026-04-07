import pytest

from metadata_validation import MetadataValidationError, validate_metadata_contract


def test_validate_metadata_contract_accepts_valid_payload():
    template = {"required": ["title", "version"]}
    metadata = {"title": "VN", "version": "1.0", "release_date": "2026-04-07"}
    allowed = {"title", "version", "release_date"}

    validate_metadata_contract(metadata, template, allowed)


def test_validate_metadata_contract_rejects_missing_required():
    template = {"required": ["title", "version"]}
    metadata = {"title": "VN"}
    allowed = {"title", "version"}

    with pytest.raises(MetadataValidationError, match="Missing required"):
        validate_metadata_contract(metadata, template, allowed)


def test_validate_metadata_contract_rejects_unknown_fields():
    template = {"required": ["title"]}
    metadata = {"title": "VN", "extra": "oops"}
    allowed = {"title"}

    with pytest.raises(MetadataValidationError, match="Unknown metadata field"):
        validate_metadata_contract(metadata, template, allowed)


def test_validate_metadata_contract_rejects_bad_date_format():
    template = {"required": ["title"]}
    metadata = {"title": "VN", "release_date": "04/07/2026"}
    allowed = {"title", "release_date"}

    with pytest.raises(MetadataValidationError, match="Invalid date format"):
        validate_metadata_contract(metadata, template, allowed)
