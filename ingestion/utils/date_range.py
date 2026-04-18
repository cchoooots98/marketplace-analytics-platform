"""Date parsing and range helpers for ingestion jobs."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime, timedelta


def parse_date(value: str) -> date:
    """Parse an ISO date string.

    Args:
        value: Date string in YYYY-MM-DD format.

    Returns:
        Parsed date value.

    Raises:
        ValueError: If value is empty or not in YYYY-MM-DD format.
    """
    if not value or not value.strip():
        msg = "date value cannot be empty"
        raise ValueError(msg)

    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d").date()
    except ValueError as exc:
        msg = f"date must use YYYY-MM-DD format: {value}"
        raise ValueError(msg) from exc


def validate_date_range(start_date: date, end_date: date) -> None:
    """Validate an inclusive date range.

    Args:
        start_date: First date in the range.
        end_date: Last date in the range.

    Returns:
        None.

    Raises:
        ValueError: If start_date is after end_date.
    """
    if start_date > end_date:
        msg = "start_date must be on or before end_date"
        raise ValueError(msg)


def iter_date_range(start_date: date, end_date: date) -> Iterator[date]:
    """Yield every date in an inclusive date range.

    Args:
        start_date: First date in the range.
        end_date: Last date in the range.

    Yields:
        Dates from start_date through end_date.

    Raises:
        ValueError: If start_date is after end_date.
    """
    validate_date_range(start_date, end_date)
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += timedelta(days=1)


def count_days_in_range(start_date: date, end_date: date) -> int:
    """Count days in an inclusive date range.

    Args:
        start_date: First date in the range.
        end_date: Last date in the range.

    Returns:
        Number of days in the inclusive range.

    Raises:
        ValueError: If start_date is after end_date.
    """
    validate_date_range(start_date, end_date)
    return (end_date - start_date).days + 1
