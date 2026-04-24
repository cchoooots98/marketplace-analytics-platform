from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from ingestion.olist import raw_csv_loader
from ingestion.olist.raw_csv_loader import OlistRawTableSpec
from ingestion.utils.batch_metadata import BatchMetadata
from ingestion.utils.bigquery_client import (
    BigQueryWriteResult,
    BigQueryWriteResultState,
)
from ingestion.utils.table_targets import BigQueryDatasetRole


TEST_SPEC = OlistRawTableSpec(
    source_name="orders",
    table_name="orders",
    dataset_role=BigQueryDatasetRole.RAW_OLIST,
    required_columns=frozenset({"order_id", "customer_id"}),
    default_file_name="olist_orders_dataset.csv",
)


def test_prepare_raw_dataframe_reads_csv_standardizes_columns_and_adds_metadata(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "olist_orders_dataset.csv"
    pd.DataFrame(
        {
            "Order ID": ["order_1"],
            "Customer-ID": ["customer_1"],
        }
    ).to_csv(csv_path, index=False)
    metadata = BatchMetadata(
        batch_id="orders_batch",
        ingested_at_utc=datetime(2026, 4, 17, 8, 30, tzinfo=UTC),
        source_file_name="olist_orders_dataset.csv",
    )

    prepared_dataframe = raw_csv_loader.prepare_raw_dataframe(
        csv_path,
        TEST_SPEC,
        metadata=metadata,
    )

    assert list(prepared_dataframe.columns) == [
        "order_id",
        "customer_id",
        "batch_id",
        "ingested_at_utc",
        "source_file_name",
    ]
    assert prepared_dataframe.loc[0, "batch_id"] == "orders_batch"
    assert prepared_dataframe.loc[0, "source_file_name"] == "olist_orders_dataset.csv"


def test_standardize_column_names_rejects_duplicate_results() -> None:
    source_dataframe = pd.DataFrame(
        {
            "Order ID": ["order_1"],
            "order-id": ["order_2"],
        }
    )

    with pytest.raises(ValueError, match="standardized column names must be unique"):
        raw_csv_loader.standardize_column_names(source_dataframe)


def test_prepare_raw_dataframe_rejects_missing_required_columns(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "olist_orders_dataset.csv"
    pd.DataFrame({"order_id": ["order_1"]}).to_csv(csv_path, index=False)

    with pytest.raises(ValueError, match="missing required columns"):
        raw_csv_loader.prepare_raw_dataframe(csv_path, TEST_SPEC)


def test_load_raw_csv_resolves_table_id_from_dataset_role(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "olist_orders_dataset.csv"
    pd.DataFrame(
        {
            "order_id": ["order_1"],
            "customer_id": ["customer_1"],
        }
    ).to_csv(csv_path, index=False)
    captured_write: dict[str, object] = {}
    monkeypatch.setenv("BQ_RAW_OLIST_DATASET", "raw_olist_runtime")

    def fake_write_dataframe_to_bigquery(
        dataframe: pd.DataFrame,
        table_id: str,
        *,
        write_mode: raw_csv_loader.WriteMode,
        client: object | None,
        project_id: str | None,
        location: str | None,
    ) -> BigQueryWriteResult:
        captured_write["dataframe"] = dataframe
        captured_write["table_id"] = table_id
        captured_write["write_mode"] = write_mode
        captured_write["project_id"] = project_id
        captured_write["location"] = location
        return BigQueryWriteResult(
            table_id=table_id,
            write_mode=write_mode,
            result_state=BigQueryWriteResultState.COMPLETED,
            job_id="raw_job",
            input_rows=len(dataframe.index),
            input_columns=len(dataframe.columns),
            loaded_rows=len(dataframe.index),
        )

    monkeypatch.setattr(
        raw_csv_loader,
        "write_dataframe_to_bigquery",
        fake_write_dataframe_to_bigquery,
    )

    write_result = raw_csv_loader.load_raw_csv(
        csv_path,
        TEST_SPEC,
        project_id="marketplace-prod",
        location="EU",
    )

    written_dataframe = captured_write["dataframe"]
    assert isinstance(written_dataframe, pd.DataFrame)
    assert "batch_id" in written_dataframe.columns
    assert captured_write["table_id"] == "raw_olist_runtime.orders"
    assert captured_write["write_mode"] == "replace"
    assert captured_write["project_id"] == "marketplace-prod"
    assert captured_write["location"] == "EU"
    assert write_result.job_id == "raw_job"
    assert write_result.result_state is BigQueryWriteResultState.COMPLETED


def test_load_raw_csv_respects_explicit_table_override(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "olist_orders_dataset.csv"
    pd.DataFrame(
        {
            "order_id": ["order_1"],
            "customer_id": ["customer_1"],
        }
    ).to_csv(csv_path, index=False)
    captured_table_ids: list[str] = []

    def fake_write_dataframe_to_bigquery(
        dataframe: pd.DataFrame,
        table_id: str,
        *,
        write_mode: raw_csv_loader.WriteMode,
        client: object | None,
        project_id: str | None,
        location: str | None,
    ) -> BigQueryWriteResult:
        captured_table_ids.append(table_id)
        return BigQueryWriteResult(
            table_id=table_id,
            write_mode=write_mode,
            result_state=BigQueryWriteResultState.COMPLETED,
            job_id="raw_job",
            input_rows=len(dataframe.index),
            input_columns=len(dataframe.columns),
            loaded_rows=len(dataframe.index),
        )

    monkeypatch.setattr(
        raw_csv_loader,
        "write_dataframe_to_bigquery",
        fake_write_dataframe_to_bigquery,
    )

    raw_csv_loader.load_raw_csv(
        csv_path,
        TEST_SPEC,
        table_id="custom_raw.orders",
    )

    assert captured_table_ids == ["custom_raw.orders"]


def test_load_raw_csv_rejects_missing_path_before_bigquery_write(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    missing_csv_path = tmp_path / "missing_orders.csv"

    def fake_write_dataframe_to_bigquery(
        dataframe: pd.DataFrame,
        table_id: str,
        *,
        write_mode: raw_csv_loader.WriteMode,
        client: object | None,
        project_id: str | None,
        location: str | None,
    ) -> BigQueryWriteResult:
        pytest.fail("BigQuery writer should not run when the source file is missing")

    monkeypatch.setattr(
        raw_csv_loader,
        "write_dataframe_to_bigquery",
        fake_write_dataframe_to_bigquery,
    )

    with pytest.raises(FileNotFoundError, match="CSV file does not exist"):
        raw_csv_loader.load_raw_csv(missing_csv_path, TEST_SPEC)
