import re


ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class MetadataValidationError(ValueError):
    pass


def _is_missing(value):
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    if isinstance(value, (list, dict)) and not value:
        return True
    return False


def validate_metadata_contract(metadata, template, allowed_fields):
    if not isinstance(metadata, dict):
        raise MetadataValidationError("Metadata must be a mapping/object.")

    required_fields = [field for field in (template.get("required") or []) if isinstance(field, str)]
    missing_required = [field for field in required_fields if _is_missing(metadata.get(field))]
    if missing_required:
        raise MetadataValidationError(
            f"Missing required metadata field(s): {', '.join(sorted(missing_required))}."
        )

    unknown_fields = sorted(set(metadata.keys()) - set(allowed_fields))
    if unknown_fields:
        raise MetadataValidationError(
            f"Unknown metadata field(s): {', '.join(unknown_fields)}."
        )

    date_fields = ("original_release_date", "release_date")
    invalid_date_fields = []
    for field in date_fields:
        value = metadata.get(field)
        if _is_missing(value):
            continue
        if not isinstance(value, str) or not ISO_DATE_RE.match(value.strip()):
            invalid_date_fields.append(field)

    if invalid_date_fields:
        raise MetadataValidationError(
            "Invalid date format for field(s): "
            + ", ".join(sorted(invalid_date_fields))
            + ". Use YYYY-MM-DD."
        )
