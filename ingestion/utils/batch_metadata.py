"""Batch metadata helpers for raw ingestion loads."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

METADATA_COLUMNS = ("batch_id", "ingested_at_utc", "source_file_name")


@dataclass(frozen=True)
class BatchMetadata:
    """Metadata that identifies one raw ingestion batch.

    Args:
        batch_id: Unique identifier for this ingestion run.
        ingested_at_utc: Time when the batch was prepared, stored in UTC.
        source_file_name: Base file name of the source object loaded into raw.
    """

    batch_id: str
    ingested_at_utc: datetime
    source_file_name: str

    def to_dict(self) -> dict[str, datetime | str]:
        """Convert metadata into a dictionary for DataFrame assignment.

        Returns:
            A dictionary keyed by raw-layer metadata column names.
        """
        return asdict(self)


def build_batch_metadata(
    source_file_path: str | Path,
    *,
    batch_id: str | None = None,
    ingested_at_utc: datetime | None = None,
) -> BatchMetadata:
    """Build raw-layer metadata for one ingestion batch.

    Args:
        source_file_path: Path to the source file being loaded.
        batch_id: Optional caller-provided batch identifier. When omitted, a
            timestamp-based identifier is generated.
        ingested_at_utc: Optional timezone-aware UTC timestamp. When omitted,
            the current UTC time is used.

    Returns:
        Batch metadata containing batch_id, ingested_at_utc, and source_file_name.

    Raises:
        ValueError: If source_file_path or batch_id is empty, or if
            ingested_at_utc is not timezone-aware.
    """
    source_file_name = _extract_source_file_name(source_file_path)
    normalized_ingested_at_utc = _normalize_ingested_at_utc(ingested_at_utc)
    normalized_batch_id = _normalize_batch_id(
        batch_id=batch_id,
        source_file_name=source_file_name,
        ingested_at_utc=normalized_ingested_at_utc,
    )

    return BatchMetadata(
        batch_id=normalized_batch_id,
        ingested_at_utc=normalized_ingested_at_utc,
        source_file_name=source_file_name,
    )


def add_batch_metadata(
    dataframe: pd.DataFrame,
    metadata: BatchMetadata,
) -> pd.DataFrame:
    """Return a copy of a DataFrame with raw ingestion metadata columns.

    Args:
        dataframe: Source DataFrame before raw-layer metadata is attached.
        metadata: Batch metadata to add to every row.

    Returns:
        A new DataFrame with batch_id, ingested_at_utc, and source_file_name.

    Raises:
        TypeError: If dataframe is not a pandas DataFrame.
        ValueError: If metadata columns already exist in the source DataFrame.
    """
    if not isinstance(dataframe, pd.DataFrame):
        msg = "dataframe must be a pandas DataFrame"
        raise TypeError(msg)

    existing_metadata_columns = set(dataframe.columns).intersection(METADATA_COLUMNS)
    if existing_metadata_columns:
        sorted_columns = ", ".join(sorted(existing_metadata_columns))
        msg = f"metadata columns already exist: {sorted_columns}"
        raise ValueError(msg)

    dataframe_with_metadata = dataframe.copy()
    for column_name, metadata_value in metadata.to_dict().items():
        dataframe_with_metadata[column_name] = metadata_value

    return dataframe_with_metadata


def _extract_source_file_name(source_file_path: str | Path) -> str:
    """Extract the base source file name from a path-like value."""
    source_path = Path(source_file_path)
    source_file_name = source_path.name.strip()

    if not source_file_name:
        msg = "source_file_path must include a file name"
        raise ValueError(msg)

    return source_file_name


def _normalize_batch_id(
    *,
    batch_id: str | None,
    source_file_name: str,
    ingested_at_utc: datetime,
) -> str:
    """Normalize or generate a batch identifier."""
    if batch_id is not None:
        normalized_batch_id = batch_id.strip()
        if not normalized_batch_id:
            msg = "batch_id cannot be empty"
            raise ValueError(msg)
        return normalized_batch_id

    timestamp = ingested_at_utc.strftime("%Y%m%dT%H%M%S%fZ")
    source_stem = Path(source_file_name).stem.lower().replace(" ", "_")
    return f"{source_stem}_{timestamp}"


def _normalize_ingested_at_utc(ingested_at_utc: datetime | None) -> datetime:
    """Normalize ingestion time to a timezone-aware UTC datetime."""
    if ingested_at_utc is None:
        return datetime.now(tz=UTC)

    if ingested_at_utc.tzinfo is None or ingested_at_utc.utcoffset() is None:
        msg = "ingested_at_utc must be timezone-aware"
        raise ValueError(msg)

    return ingested_at_utc.astimezone(UTC)
