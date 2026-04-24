from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from ingestion.olist.batch_runtime import (
    DateWindow,
    derive_incremental_order_windows,
    discover_olist_batch_files,
)


def test_discover_olist_batch_files_finds_recognized_source_files(
    tmp_path: Path,
) -> None:
    """Validate landing batch discovery returns recognized files only."""
    batch_dir = tmp_path / "batch_20260424T060000Z"
    batch_dir.mkdir()
    (batch_dir / "olist_orders_dataset.csv").write_text(
        "order_purchase_timestamp,order_delivered_customer_date\n",
        encoding="utf-8",
    )
    (batch_dir / "notes.txt").write_text("ignore\n", encoding="utf-8")

    discovered_batches = discover_olist_batch_files(
        tmp_path,
        expected_file_names={"orders": "olist_orders_dataset.csv"},
    )

    assert [batch.to_dict() for batch in discovered_batches] == [
        {
            "batch_id": "batch_20260424T060000Z",
            "source_name": "orders",
            "source_file_name": "olist_orders_dataset.csv",
            "csv_path": str(batch_dir / "olist_orders_dataset.csv"),
        }
    ]


def test_derive_incremental_order_windows_uses_purchase_and_delivery_dates(
    tmp_path: Path,
) -> None:
    """Validate incremental windows derive the expected holiday and weather spans."""
    csv_path = tmp_path / "olist_orders_dataset.csv"
    pd.DataFrame(
        {
            "order_purchase_timestamp": [
                "2026-01-03 10:00:00",
                "2026-01-05 11:30:00",
            ],
            "order_delivered_customer_date": [
                "2026-01-07 09:00:00",
                "2026-01-09 13:15:00",
            ],
        }
    ).to_csv(csv_path, index=False)

    derived_windows = derive_incremental_order_windows(
        csv_path,
        weather_lookback_days=2,
    )

    assert derived_windows.holiday_window == DateWindow(
        start_date=date(2026, 1, 3),
        end_date=date(2026, 1, 5),
    )
    assert derived_windows.weather_window == DateWindow(
        start_date=date(2026, 1, 5),
        end_date=date(2026, 1, 9),
    )


def test_derive_incremental_order_windows_allows_blank_delivery_dates(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "olist_orders_dataset.csv"
    pd.DataFrame(
        {
            "order_purchase_timestamp": [
                "2026-01-03 10:00:00",
                "2026-01-05 11:30:00",
            ],
            "order_delivered_customer_date": [
                None,
                "",
            ],
        }
    ).to_csv(csv_path, index=False)

    derived_windows = derive_incremental_order_windows(csv_path)

    assert derived_windows.holiday_window == DateWindow(
        start_date=date(2026, 1, 3),
        end_date=date(2026, 1, 5),
    )
    assert derived_windows.weather_window is None


def test_derive_incremental_order_windows_rejects_invalid_purchase_timestamp(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "olist_orders_dataset.csv"
    pd.DataFrame(
        {
            "order_purchase_timestamp": [
                "not-a-timestamp",
            ],
            "order_delivered_customer_date": [
                "2026-01-07 09:00:00",
            ],
        }
    ).to_csv(csv_path, index=False)

    with pytest.raises(ValueError, match="order_purchase_timestamp"):
        derive_incremental_order_windows(csv_path)


def test_derive_incremental_order_windows_rejects_invalid_delivery_timestamp(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "olist_orders_dataset.csv"
    pd.DataFrame(
        {
            "order_purchase_timestamp": [
                "2026-01-03 10:00:00",
            ],
            "order_delivered_customer_date": [
                "broken-delivery-date",
            ],
        }
    ).to_csv(csv_path, index=False)

    with pytest.raises(ValueError, match="order_delivered_customer_date"):
        derive_incremental_order_windows(csv_path)
