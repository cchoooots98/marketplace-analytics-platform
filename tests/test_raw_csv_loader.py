import logging
import os
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from ingestion.olist import raw_csv_loader
from ingestion.olist.raw_csv_loader import OlistRawTableSpec
from ingestion.utils.batch_metadata import BatchMetadata
from ingestion.utils.bigquery_client import (
    BigQueryConfigurationError,
    BigQueryWriteResult,
)


TEST_SPEC = OlistRawTableSpec(
    source_name="orders",
    table_id="raw_olist.orders",
    required_columns=frozenset({"order_id", "customer_id"}),
    default_file_name="olist_orders_dataset.csv",
)


def test_prepare_raw_dataframe_reads_csv_standardizes_columns_and_adds_metadata(
    tmp_path: Path,
) -> None:
    """Validate the shared loader prepares a source CSV for raw loading.

    Args:
        tmp_path: Pytest fixture providing a temporary directory.

    Returns:
        None.
    """
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
    """Validate duplicate standardized names fail fast.

    Returns:
        None.
    """
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
    """Validate required source columns are enforced before BigQuery writes.

    Args:
        tmp_path: Pytest fixture providing a temporary directory.

    Returns:
        None.
    """
    csv_path = tmp_path / "olist_orders_dataset.csv"
    pd.DataFrame({"order_id": ["order_1"]}).to_csv(csv_path, index=False)

    with pytest.raises(ValueError, match="missing required columns"):
        raw_csv_loader.prepare_raw_dataframe(csv_path, TEST_SPEC)


def test_validate_required_columns_accepts_extra_columns_and_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Validate extra source columns are observable without failing the load.

    Args:
        caplog: Pytest fixture for capturing log records.

    Returns:
        None.
    """
    source_dataframe = pd.DataFrame(
        {
            "order_id": ["order_1"],
            "customer_id": ["customer_1"],
            "new_source_column": ["new_value"],
        }
    )

    with caplog.at_level(logging.INFO):
        raw_csv_loader.validate_required_columns(source_dataframe, TEST_SPEC)

    assert "additional columns" in caplog.text
    assert "new_source_column" in caplog.text


def test_load_raw_csv_writes_to_spec_table_with_replace_by_default(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Validate the shared loader calls BigQuery with the expected contract.

    Args:
        monkeypatch: Pytest fixture for replacing the BigQuery writer.
        tmp_path: Pytest fixture providing a temporary directory.

    Returns:
        None.
    """
    csv_path = tmp_path / "olist_orders_dataset.csv"
    pd.DataFrame(
        {
            "order_id": ["order_1"],
            "customer_id": ["customer_1"],
        }
    ).to_csv(csv_path, index=False)
    captured_write: dict[str, object] = {}

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
        captured_write["client"] = client
        captured_write["project_id"] = project_id
        captured_write["location"] = location
        return BigQueryWriteResult(
            table_id=table_id,
            write_mode=write_mode,
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
    assert captured_write["table_id"] == "raw_olist.orders"
    assert captured_write["write_mode"] == "replace"
    assert captured_write["project_id"] == "marketplace-prod"
    assert captured_write["location"] == "EU"
    assert write_result.job_id == "raw_job"


def test_load_raw_csv_rejects_missing_path_before_bigquery_write(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Validate missing source files fail before any BigQuery write.

    Args:
        monkeypatch: Pytest fixture for replacing the BigQuery writer.
        tmp_path: Pytest fixture providing a temporary directory.

    Returns:
        None.
    """
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


def test_run_olist_loader_loads_dotenv_before_resolving_defaults(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Validate CLI defaults can come from .env before parsing arguments.

    Args:
        monkeypatch: Pytest fixture for replacing environment and collaborators.
        tmp_path: Pytest fixture providing a temporary directory.

    Returns:
        None.
    """
    credentials_path = tmp_path / "service-account.json"
    credentials_path.write_text("{}", encoding="utf-8")
    captured_client_config: dict[str, str | None] = {}
    captured_loader_config: dict[str, object] = {}
    fake_client = object()

    monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
    monkeypatch.delenv("BIGQUERY_LOCATION", raising=False)
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)

    def fake_load_dotenv() -> None:
        monkeypatch.setenv("GCP_PROJECT_ID", "marketplace-prod")
        monkeypatch.setenv("BIGQUERY_LOCATION", "EU")
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(credentials_path))

    def fake_create_bigquery_client(
        *,
        project_id: str | None,
        location: str | None,
    ) -> object:
        captured_client_config["project_id"] = project_id
        captured_client_config["location"] = location
        return fake_client

    def fake_load_raw_csv(
        csv_path: str,
        spec: OlistRawTableSpec,
        *,
        table_id: str,
        write_mode: raw_csv_loader.WriteMode,
        client: object,
        project_id: str,
        location: str,
    ) -> BigQueryWriteResult:
        captured_loader_config["csv_path"] = csv_path
        captured_loader_config["spec"] = spec
        captured_loader_config["table_id"] = table_id
        captured_loader_config["write_mode"] = write_mode
        captured_loader_config["client"] = client
        captured_loader_config["project_id"] = project_id
        captured_loader_config["location"] = location
        return BigQueryWriteResult(
            table_id=table_id,
            write_mode=write_mode,
            job_id="raw_job",
            input_rows=1,
            input_columns=1,
            loaded_rows=1,
        )

    monkeypatch.setattr(raw_csv_loader, "load_dotenv", fake_load_dotenv)
    monkeypatch.setattr(
        raw_csv_loader,
        "create_bigquery_client",
        fake_create_bigquery_client,
    )
    monkeypatch.setattr(raw_csv_loader, "load_raw_csv", fake_load_raw_csv)

    exit_code = raw_csv_loader.run_olist_loader(["source.csv"], TEST_SPEC)

    assert exit_code == 0
    assert captured_client_config == {
        "project_id": "marketplace-prod",
        "location": "EU",
    }
    assert captured_loader_config["spec"] == TEST_SPEC
    assert captured_loader_config["client"] is fake_client


def test_run_olist_loader_fails_before_csv_read_when_bigquery_config_is_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validate BigQuery configuration is checked before reading the CSV.

    Args:
        monkeypatch: Pytest fixture for replacing environment and collaborators.

    Returns:
        None.
    """
    monkeypatch.setenv("GCP_PROJECT_ID", "marketplace-prod")
    monkeypatch.setenv("BIGQUERY_LOCATION", "EU")
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    monkeypatch.setattr(raw_csv_loader, "load_dotenv", lambda: None)

    def fake_create_bigquery_client(
        *,
        project_id: str | None,
        location: str | None,
    ) -> object:
        raise BigQueryConfigurationError("missing credentials")

    def fail_if_loader_reads_csv(
        *args: object, **kwargs: object
    ) -> BigQueryWriteResult:
        pytest.fail("CSV should not be read when BigQuery configuration is invalid")

    monkeypatch.setattr(
        raw_csv_loader,
        "create_bigquery_client",
        fake_create_bigquery_client,
    )
    monkeypatch.setattr(raw_csv_loader, "load_raw_csv", fail_if_loader_reads_csv)

    exit_code = raw_csv_loader.run_olist_loader(["source.csv"], TEST_SPEC)

    assert exit_code == 1


def test_configure_google_application_credentials_exports_resolved_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Validate credentials path validation sets the Google environment variable.

    Args:
        monkeypatch: Pytest fixture for replacing environment variables.
        tmp_path: Pytest fixture providing a temporary directory.

    Returns:
        None.
    """
    credentials_path = tmp_path / "service-account.json"
    credentials_path.write_text("{}", encoding="utf-8")
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)

    resolved_path = raw_csv_loader.configure_google_application_credentials(
        str(credentials_path)
    )

    assert resolved_path == credentials_path.resolve()
    assert (
        Path(os.environ["GOOGLE_APPLICATION_CREDENTIALS"]) == credentials_path.resolve()
    )


def test_configure_google_application_credentials_rejects_missing_file() -> None:
    """Validate missing service-account files fail with a clear config error.

    Returns:
        None.
    """
    with pytest.raises(BigQueryConfigurationError, match="service-account JSON file"):
        raw_csv_loader.configure_google_application_credentials(
            "missing-service-account.json"
        )
