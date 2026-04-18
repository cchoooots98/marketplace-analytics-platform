from datetime import UTC, datetime, timezone, timedelta

import pandas as pd
import pytest

from ingestion.utils.batch_metadata import (
    BatchMetadata,
    add_batch_metadata,
    build_batch_metadata,
)


def test_build_batch_metadata_uses_source_file_name_and_fixed_values() -> None:
    """Validate deterministic metadata when values are provided by the caller.

    Returns:
        None.
    """
    ingested_at_utc = datetime(2026, 4, 17, 8, 30, tzinfo=UTC)

    metadata = build_batch_metadata(
        "data/olist/olist_orders_dataset.csv",
        batch_id="orders_20260417",
        ingested_at_utc=ingested_at_utc,
    )

    assert metadata == BatchMetadata(
        batch_id="orders_20260417",
        ingested_at_utc=ingested_at_utc,
        source_file_name="olist_orders_dataset.csv",
    )
    assert metadata.to_dict() == {
        "batch_id": "orders_20260417",
        "ingested_at_utc": ingested_at_utc,
        "source_file_name": "olist_orders_dataset.csv",
    }


def test_build_batch_metadata_converts_timezone_to_utc() -> None:
    """Validate that ingestion timestamps are normalized to UTC.

    Returns:
        None.
    """
    paris_time = datetime(
        2026,
        4,
        17,
        10,
        30,
        tzinfo=timezone(timedelta(hours=2)),
    )

    metadata = build_batch_metadata(
        "olist_orders_dataset.csv",
        batch_id="orders_batch",
        ingested_at_utc=paris_time,
    )

    assert metadata.ingested_at_utc == datetime(2026, 4, 17, 8, 30, tzinfo=UTC)


def test_build_batch_metadata_generates_batch_id_when_missing() -> None:
    """Validate generated batch IDs include the source stem and UTC timestamp.

    Returns:
        None.
    """
    ingested_at_utc = datetime(2026, 4, 17, 8, 30, 45, 123456, tzinfo=UTC)

    metadata = build_batch_metadata(
        "Olist Orders Dataset.csv",
        ingested_at_utc=ingested_at_utc,
    )

    assert metadata.batch_id == "olist_orders_dataset_20260417T083045123456Z"


def test_build_batch_metadata_rejects_naive_datetime() -> None:
    """Validate that metadata timestamps must be timezone-aware.

    Returns:
        None.
    """
    naive_datetime = datetime(2026, 4, 17, 8, 30)

    with pytest.raises(ValueError, match="timezone-aware"):
        build_batch_metadata(
            "olist_orders_dataset.csv",
            ingested_at_utc=naive_datetime,
        )


def test_add_batch_metadata_adds_columns_without_mutating_source() -> None:
    """Validate metadata enrichment creates a new DataFrame.

    Returns:
        None.
    """
    orders_dataframe = pd.DataFrame({"order_id": ["order_1", "order_2"]})
    metadata = BatchMetadata(
        batch_id="orders_batch",
        ingested_at_utc=datetime(2026, 4, 17, 8, 30, tzinfo=UTC),
        source_file_name="olist_orders_dataset.csv",
    )

    enriched_dataframe = add_batch_metadata(orders_dataframe, metadata)

    assert list(orders_dataframe.columns) == ["order_id"]
    assert enriched_dataframe["batch_id"].tolist() == [
        "orders_batch",
        "orders_batch",
    ]
    assert enriched_dataframe["source_file_name"].tolist() == [
        "olist_orders_dataset.csv",
        "olist_orders_dataset.csv",
    ]
    assert enriched_dataframe["ingested_at_utc"].tolist() == [
        metadata.ingested_at_utc,
        metadata.ingested_at_utc,
    ]


def test_add_batch_metadata_rejects_existing_metadata_columns() -> None:
    """Validate metadata column collisions fail before raw load.

    Returns:
        None.
    """
    orders_dataframe = pd.DataFrame(
        {
            "order_id": ["order_1"],
            "batch_id": ["source_batch"],
        }
    )
    metadata = BatchMetadata(
        batch_id="orders_batch",
        ingested_at_utc=datetime(2026, 4, 17, 8, 30, tzinfo=UTC),
        source_file_name="olist_orders_dataset.csv",
    )

    with pytest.raises(ValueError, match="metadata columns already exist"):
        add_batch_metadata(orders_dataframe, metadata)
