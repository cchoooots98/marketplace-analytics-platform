"""Date-range helpers for bootstrap and enrichment orchestration."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from ingestion.utils.date_range import parse_date, validate_date_range

OLIST_ORDERS_FILE_NAME = "olist_orders_dataset.csv"


def resolve_olist_date_range(olist_data_dir: str | Path) -> tuple[date, date]:
    """Resolve the Olist order purchase date range from the local orders CSV.

    Args:
        olist_data_dir: Directory containing Olist CSV files.

    Returns:
        Inclusive minimum and maximum order purchase dates.

    Raises:
        FileNotFoundError: If the orders CSV is missing.
        ValueError: If no valid order_purchase_timestamp values are found.
    """
    orders_csv_path = Path(olist_data_dir) / OLIST_ORDERS_FILE_NAME
    if not orders_csv_path.is_file():
        msg = f"Olist orders CSV does not exist: {orders_csv_path}"
        raise FileNotFoundError(msg)

    orders_dataframe = pd.read_csv(
        orders_csv_path,
        usecols=["order_purchase_timestamp"],
    )
    purchase_timestamp_values = orders_dataframe["order_purchase_timestamp"]
    normalized_timestamp_values = (
        purchase_timestamp_values.fillna("").astype(str).str.strip()
    )
    parsed_purchase_timestamps = pd.to_datetime(
        purchase_timestamp_values,
        errors="coerce",
    )
    invalid_mask = (
        normalized_timestamp_values.ne("") & parsed_purchase_timestamps.isna()
    )
    if invalid_mask.any():
        invalid_examples = ", ".join(
            normalized_timestamp_values.loc[invalid_mask].head(3).tolist()
        )
        msg = (
            "Olist orders CSV contains invalid order_purchase_timestamp values "
            f"examples={invalid_examples}"
        )
        raise ValueError(msg)

    purchase_timestamps = parsed_purchase_timestamps.dropna()
    if purchase_timestamps.empty:
        msg = "Olist orders CSV has no valid order_purchase_timestamp values"
        raise ValueError(msg)

    return purchase_timestamps.dt.date.min(), purchase_timestamps.dt.date.max()


def resolve_enrichment_date_range(
    *,
    start_date_value: str | None,
    end_date_value: str | None,
    use_olist_date_range: bool,
    olist_data_dir: str | Path,
) -> tuple[date, date]:
    """Resolve holiday/weather enrichment date range from CLI values.

    Args:
        start_date_value: Optional explicit start date string.
        end_date_value: Optional explicit end date string.
        use_olist_date_range: Whether to derive dates from Olist orders.
        olist_data_dir: Directory containing Olist CSV files.

    Returns:
        Inclusive start and end dates for enrichment loaders.

    Raises:
        ValueError: If no complete date range is provided.
    """
    if use_olist_date_range:
        return resolve_olist_date_range(olist_data_dir)

    if not start_date_value or not end_date_value:
        msg = (
            "start_date and end_date are required when holiday or weather "
            "ingestion is enabled. Pass --start-date/--end-date or "
            "--use-olist-date-range."
        )
        raise ValueError(msg)

    start_date = parse_date(start_date_value)
    end_date = parse_date(end_date_value)
    validate_date_range(start_date, end_date)
    return start_date, end_date


def require_enrichment_date_range(
    enrichment_date_range: tuple[date, date] | None,
    *,
    consumer_name: str,
) -> tuple[date, date]:
    """Return a resolved enrichment date range for one downstream consumer.

    Args:
        enrichment_date_range: Previously resolved enrichment date range.
        consumer_name: Human-readable downstream consumer for error context.

    Returns:
        The resolved ``(start_date, end_date)`` tuple.

    Raises:
        ValueError: If the enrichment date range is unexpectedly missing.
    """
    if enrichment_date_range is None:
        msg = (
            "Enrichment date range is required before running "
            f"{consumer_name}. This indicates a CLI orchestration bug."
        )
        raise ValueError(msg)

    return enrichment_date_range
