import pandas as pd
import pytest
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery

from ingestion.utils import bigquery_client


class FakeLoadJob:
    """Small test double for a completed BigQuery load job."""

    def __init__(
        self,
        job_id: str = "job_123",
        output_rows: int | None = 2,
        *,
        result_exception: Exception | None = None,
    ) -> None:
        self.job_id = job_id
        self.output_rows = output_rows
        self.result_exception = result_exception
        self.result_called = False

    def result(self) -> "FakeLoadJob":
        """Record that the production function waited for job completion.

        Returns:
            The fake load job, matching the chainable shape of Google clients.
        """
        self.result_called = True
        if self.result_exception is not None:
            raise self.result_exception
        return self


class FakeBigQueryClient:
    """Small test double that captures load_table_from_dataframe inputs."""

    def __init__(
        self,
        load_job: FakeLoadJob,
        *,
        copy_job: FakeLoadJob | None = None,
        delete_exception: Exception | None = None,
    ) -> None:
        self.load_job = load_job
        self.copy_job = copy_job or FakeLoadJob(job_id="copy_job", output_rows=2)
        self.delete_exception = delete_exception
        self.loaded_dataframe: pd.DataFrame | None = None
        self.destination_table: str | None = None
        self.job_config: bigquery.LoadJobConfig | None = None
        self.location: str | None = None
        self.copy_source: str | None = None
        self.copy_destination: str | None = None
        self.copy_job_config: bigquery.CopyJobConfig | None = None
        self.deleted_table: str | None = None
        self.not_found_ok: bool | None = None

    def load_table_from_dataframe(
        self,
        dataframe: pd.DataFrame,
        destination: str,
        *,
        job_config: bigquery.LoadJobConfig,
        location: str | None,
    ) -> FakeLoadJob:
        """Capture the load request and return a fake load job.

        Args:
            dataframe: DataFrame passed by the production write helper.
            destination: Destination BigQuery table ID.
            job_config: BigQuery load job configuration.
            location: BigQuery job location.

        Returns:
            The fake load job configured for the test.
        """
        self.loaded_dataframe = dataframe
        self.destination_table = destination
        self.job_config = job_config
        self.location = location
        return self.load_job

    def copy_table(
        self,
        sources: str,
        destination: str,
        *,
        job_config: bigquery.CopyJobConfig,
        location: str | None,
    ) -> FakeLoadJob:
        """Capture the publish copy request for atomic replace tests."""
        self.copy_source = sources
        self.copy_destination = destination
        self.copy_job_config = job_config
        self.location = location
        return self.copy_job

    def delete_table(self, table: str, *, not_found_ok: bool) -> None:
        """Capture cleanup of the staging table."""
        self.deleted_table = table
        self.not_found_ok = not_found_ok
        if self.delete_exception is not None:
            raise self.delete_exception


def test_create_bigquery_client_passes_project_and_location(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validate that client creation forwards normalized project and location.

    Args:
        monkeypatch: Pytest fixture for replacing the Google client constructor.

    Returns:
        None.
    """
    captured_arguments: dict[str, str | None] = {}
    expected_client = object()

    def fake_client(
        *,
        project: str | None,
        location: str | None,
    ) -> object:
        captured_arguments["project"] = project
        captured_arguments["location"] = location
        return expected_client

    monkeypatch.setattr(bigquery_client.bigquery, "Client", fake_client)

    client = bigquery_client.create_bigquery_client(
        project_id=" marketplace-prod ",
        location=" EU ",
    )

    assert client is expected_client
    assert captured_arguments == {
        "project": "marketplace-prod",
        "location": "EU",
    }


def test_create_bigquery_client_wraps_missing_default_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validate missing Google credentials return a project-level error.

    Args:
        monkeypatch: Pytest fixture for replacing the Google client constructor.

    Returns:
        None.
    """

    def fake_client(
        *,
        project: str | None,
        location: str | None,
    ) -> object:
        raise DefaultCredentialsError("missing adc")

    monkeypatch.setattr(bigquery_client.bigquery, "Client", fake_client)

    with pytest.raises(
        bigquery_client.BigQueryConfigurationError,
        match="BigQuery credentials were not found",
    ):
        bigquery_client.create_bigquery_client(
            project_id="marketplace-prod",
            location="EU",
        )


def test_write_dataframe_to_bigquery_appends_dataframe() -> None:
    """Validate append-mode DataFrame loads.

    Returns:
        None.
    """
    orders_dataframe = pd.DataFrame(
        {
            "order_id": ["order_1", "order_2"],
            "payment_value": [100.0, 200.0],
        }
    )
    load_job = FakeLoadJob(job_id="append_job", output_rows=2)
    fake_client = FakeBigQueryClient(load_job)

    write_result = bigquery_client.write_dataframe_to_bigquery(
        orders_dataframe,
        "raw_olist.orders",
        write_mode="append",
        client=fake_client,
        location="EU",
    )

    assert fake_client.loaded_dataframe is orders_dataframe
    assert fake_client.destination_table == "raw_olist.orders"
    assert fake_client.location == "EU"
    assert fake_client.job_config is not None
    assert (
        fake_client.job_config.write_disposition
        == bigquery.WriteDisposition.WRITE_APPEND
    )
    assert load_job.result_called is True
    assert write_result.to_log_dict() == {
        "table_id": "raw_olist.orders",
        "write_mode": "append",
        "job_id": "append_job",
        "input_rows": 2,
        "input_columns": 2,
        "loaded_rows": 2,
    }


def test_write_dataframe_to_bigquery_replaces_table_contents() -> None:
    """Validate replace-mode DataFrame loads publish through a staging table.

    Returns:
        None.
    """
    holidays_dataframe = pd.DataFrame(
        {
            "holiday_date": ["2026-01-01"],
            "holiday_name": ["Confraternizacao Universal"],
        }
    )
    staging_job = FakeLoadJob(job_id="stage_job", output_rows=1)
    publish_job = FakeLoadJob(job_id="publish_job", output_rows=1)
    fake_client = FakeBigQueryClient(staging_job, copy_job=publish_job)

    write_result = bigquery_client.write_dataframe_to_bigquery(
        holidays_dataframe,
        "project.raw_ext.holidays",
        write_mode="replace",
        client=fake_client,
    )

    assert fake_client.destination_table is not None
    assert fake_client.destination_table.startswith(
        "project.raw_ext.__dbt_atomic_swap_holidays_"
    )
    assert fake_client.copy_source == fake_client.destination_table
    assert fake_client.copy_destination == "project.raw_ext.holidays"
    assert fake_client.copy_job_config is not None
    assert fake_client.job_config is not None
    assert (
        fake_client.job_config.write_disposition
        == bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    assert (
        fake_client.copy_job_config.write_disposition
        == bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    assert fake_client.deleted_table == fake_client.destination_table
    assert fake_client.not_found_ok is True
    assert write_result.loaded_rows == 1
    assert write_result.write_mode == "replace"
    assert write_result.job_id == "publish_job"


def test_atomic_replace_cleanup_does_not_mask_primary_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Validate staging cleanup warnings do not replace the original failure."""
    failing_stage_job = FakeLoadJob(
        job_id="stage_job",
        output_rows=1,
        result_exception=RuntimeError("stage load failed"),
    )
    fake_client = FakeBigQueryClient(
        failing_stage_job,
        delete_exception=RuntimeError("cleanup failed"),
    )

    with pytest.raises(RuntimeError, match="stage load failed"):
        bigquery_client.write_dataframe_to_bigquery(
            pd.DataFrame({"holiday_date": ["2026-01-01"]}),
            "project.raw_ext.holidays",
            write_mode="replace",
            client=fake_client,
        )

    assert "Atomic swap staging cleanup failed" in caplog.text


def test_write_dataframe_to_bigquery_rejects_invalid_write_mode() -> None:
    """Validate that unsupported write modes fail before a load job starts.

    Returns:
        None.
    """
    orders_dataframe = pd.DataFrame({"order_id": ["order_1"]})

    with pytest.raises(ValueError, match="write_mode must be one of"):
        bigquery_client.write_dataframe_to_bigquery(
            orders_dataframe,
            "raw_olist.orders",
            write_mode="merge",
            client=FakeBigQueryClient(FakeLoadJob()),
        )


def test_write_dataframe_to_bigquery_rejects_empty_dataframe() -> None:
    """Validate that empty loads fail fast.

    Returns:
        None.
    """
    empty_orders_dataframe = pd.DataFrame(columns=["order_id"])

    with pytest.raises(ValueError, match="at least one row"):
        bigquery_client.write_dataframe_to_bigquery(
            empty_orders_dataframe,
            "raw_olist.orders",
            client=FakeBigQueryClient(FakeLoadJob()),
        )
