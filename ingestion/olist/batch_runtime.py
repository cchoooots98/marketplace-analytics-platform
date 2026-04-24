"""Batch-file discovery and incremental window helpers for Olist ingestion."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from ingestion.utils.batch_key import BatchKey

logger = logging.getLogger(__name__)

ORDERS_REQUIRED_WINDOW_COLUMNS = (
    "order_purchase_timestamp",
    "order_delivered_customer_date",
)


@dataclass(frozen=True)
class DateWindow:
    """Inclusive date window used by one downstream enrichment loader.

    Args:
        start_date: First calendar date in the window.
        end_date: Last calendar date in the window.
    """

    start_date: date
    end_date: date

    def to_dict(self) -> dict[str, str]:
        """Convert the window into a JSON-safe dictionary."""
        return {
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
        }


@dataclass(frozen=True)
class IncrementalOrderWindows:
    """Derived enrichment windows from one or more incremental orders batches.

    Args:
        holiday_window: Purchase-date window for holiday enrichment.
        weather_window: Delivery-date window for weather enrichment.
    """

    holiday_window: DateWindow | None = None
    weather_window: DateWindow | None = None

    def merge(self, other: "IncrementalOrderWindows") -> "IncrementalOrderWindows":
        """Combine two sets of derived enrichment windows."""
        return IncrementalOrderWindows(
            holiday_window=_merge_optional_windows(
                self.holiday_window,
                other.holiday_window,
            ),
            weather_window=_merge_optional_windows(
                self.weather_window,
                other.weather_window,
            ),
        )


@dataclass(frozen=True)
class DiscoveredOlistBatchFile:
    """One Olist source file discovered in a landing batch directory.

    Args:
        batch_id: Stable batch identifier, usually the landing directory name.
        source_name: Human-readable source name such as ``orders``.
        csv_path: Path to the discovered CSV file.
    """

    batch_id: str
    source_name: str
    csv_path: Path

    @property
    def source_file_name(self) -> str:
        """Return the landing file name stored in batch metadata and state."""
        return self.csv_path.name

    def batch_key(self) -> BatchKey:
        """Return the stable key used for ingestion-state de-duplication."""
        return BatchKey(
            source_name=self.source_name,
            batch_id=self.batch_id,
            source_file_name=self.source_file_name,
        )

    def to_dict(self) -> dict[str, str]:
        """Convert the discovery result into a JSON-safe dictionary."""
        return {
            "batch_id": self.batch_id,
            "source_name": self.source_name,
            "source_file_name": self.source_file_name,
            "csv_path": str(self.csv_path),
        }


def discover_olist_batch_files(
    landing_dir: str | Path,
    *,
    expected_file_names: dict[str, str],
) -> list[DiscoveredOlistBatchFile]:
    """Discover Olist batch files from a landing directory.

    The landing contract is one directory per batch. Each batch directory may
    contain any subset of recognized Olist source files.

    Args:
        landing_dir: Directory containing batch subdirectories.
        expected_file_names: Mapping of source_name -> expected file name.

    Returns:
        Discovered batch files sorted by batch directory then source name.

    Raises:
        FileNotFoundError: If landing_dir does not exist.
        ValueError: If landing_dir is not a directory.
    """
    normalized_landing_dir = Path(landing_dir)
    if not normalized_landing_dir.exists():
        msg = f"Olist landing directory does not exist: {normalized_landing_dir}"
        raise FileNotFoundError(msg)

    if not normalized_landing_dir.is_dir():
        msg = f"Olist landing path must be a directory: {normalized_landing_dir}"
        raise ValueError(msg)

    discovered_files: list[DiscoveredOlistBatchFile] = []
    for batch_dir in sorted(
        path for path in normalized_landing_dir.iterdir() if path.is_dir()
    ):
        batch_id = batch_dir.name.strip()
        if not batch_id:
            logger.info("Skipping unnamed landing batch directory path=%s", batch_dir)
            continue

        for source_name, expected_file_name in sorted(expected_file_names.items()):
            csv_path = batch_dir / expected_file_name
            if csv_path.is_file():
                discovered_files.append(
                    DiscoveredOlistBatchFile(
                        batch_id=batch_id,
                        source_name=source_name,
                        csv_path=csv_path,
                    )
                )

    return discovered_files


def derive_incremental_order_windows(
    csv_path: str | Path,
    *,
    weather_lookback_days: int = 0,
) -> IncrementalOrderWindows:
    """Derive holiday and weather enrichment windows from one orders batch.

    Args:
        csv_path: Path to one incremental orders CSV file.
        weather_lookback_days: Replay lookback applied to the weather window.

    Returns:
        Derived holiday and weather windows. A window is ``None`` when the
        incremental batch does not contain any valid dates for that enrichment.
    """
    orders_dataframe = pd.read_csv(
        csv_path,
        usecols=list(ORDERS_REQUIRED_WINDOW_COLUMNS),
    )
    purchase_dates = _extract_date_series(
        orders_dataframe,
        "order_purchase_timestamp",
    )
    delivery_dates = _extract_date_series(
        orders_dataframe,
        "order_delivered_customer_date",
    )

    holiday_window = _build_window(purchase_dates)
    weather_window = _build_window(delivery_dates)
    if weather_window is not None and weather_lookback_days > 0:
        weather_window = DateWindow(
            start_date=weather_window.start_date
            - timedelta(days=weather_lookback_days),
            end_date=weather_window.end_date,
        )

    return IncrementalOrderWindows(
        holiday_window=holiday_window,
        weather_window=weather_window,
    )


def _extract_date_series(
    dataframe: pd.DataFrame,
    column_name: str,
) -> list[date]:
    """Extract validated date values from one orders CSV column."""
    source_series = dataframe[column_name]
    normalized_source_values = source_series.fillna("").astype(str).str.strip()
    parsed_timestamps = pd.to_datetime(
        source_series,
        errors="coerce",
    )
    invalid_mask = normalized_source_values.ne("") & parsed_timestamps.isna()
    if invalid_mask.any():
        invalid_examples = ", ".join(
            normalized_source_values.loc[invalid_mask].head(3).tolist()
        )
        msg = (
            "Orders batch contains invalid datetime values "
            f"column_name={column_name} examples={invalid_examples}"
        )
        raise ValueError(msg)

    return list(parsed_timestamps.dropna().dt.date)


def _build_window(values: list[date]) -> DateWindow | None:
    """Build an inclusive date window from a list of dates."""
    if not values:
        return None

    return DateWindow(
        start_date=min(values),
        end_date=max(values),
    )


def _merge_optional_windows(
    current_window: DateWindow | None,
    new_window: DateWindow | None,
) -> DateWindow | None:
    """Merge two optional date windows."""
    if current_window is None:
        return new_window

    if new_window is None:
        return current_window

    return DateWindow(
        start_date=min(current_window.start_date, new_window.start_date),
        end_date=max(current_window.end_date, new_window.end_date),
    )
