"""Shared validation and normalization helpers for ingestion runtime code."""

from __future__ import annotations


def normalize_optional_text(value: object | None) -> str | None:
    """Normalize optional text-like input to a stripped string.

    Args:
        value: Candidate text input.

    Returns:
        Trimmed string or ``None`` when the input is missing or blank.
    """
    if value is None:
        return None

    normalized_value = str(value).strip()
    if not normalized_value:
        return None

    return normalized_value


def require_text(value: object | None, field_name: str) -> str:
    """Return a required non-empty string.

    Args:
        value: Candidate text input.
        field_name: Field name for error context.

    Returns:
        Trimmed non-empty string.

    Raises:
        ValueError: If the value is missing or blank.
    """
    normalized_value = normalize_optional_text(value)
    if normalized_value is None:
        msg = f"{field_name} is required"
        raise ValueError(msg)

    return normalized_value


def parse_required_float(value: object | None, field_name: str) -> float:
    """Parse one required floating-point configuration value.

    Args:
        value: Candidate numeric input.
        field_name: Field name for error context.

    Returns:
        Parsed float value.

    Raises:
        ValueError: If the value is missing or malformed.
    """
    normalized_value = require_text(value, field_name)
    try:
        return float(normalized_value)
    except ValueError as exc:
        msg = f"{field_name} must be a valid number"
        raise ValueError(msg) from exc


def parse_required_int(value: object | None, field_name: str) -> int:
    """Parse one required integer configuration value.

    Args:
        value: Candidate numeric input.
        field_name: Field name for error context.

    Returns:
        Parsed integer value.

    Raises:
        ValueError: If the value is missing or malformed.
    """
    normalized_value = require_text(value, field_name)
    try:
        return int(normalized_value)
    except ValueError as exc:
        msg = f"{field_name} must be a valid integer"
        raise ValueError(msg) from exc
