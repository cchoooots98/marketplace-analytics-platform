import pandas as pd
import pytest
from google.api_core.exceptions import GoogleAPIError
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery

from ingestion.utils import bigquery_client
from ingestion.utils.bigquery_client import BigQueryWriteResultState


class FakeLoadJob:
    """Small test double for a completed BigQuery load or copy job."""

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
        self.result_called = True
        if self.result_exception is not None:
            raise self.result_exception
        return self


class FakeBigQueryClient:
    """Small test double that captures BigQuery write inputs."""

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
        self.copy_source = sources
        self.copy_destination = destination
        self.copy_job_config = job_config
        self.location = location
        return self.copy_job

    def delete_table(self, table: str, *, not_found_ok: bool) -> None:
        self.deleted_table = table
        self.not_found_ok = not_found_ok
        if self.delete_exception is not None:
            raise self.delete_exception


def test_create_bigquery_client_passes_project_and_location(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
        "result_state": BigQueryWriteResultState.COMPLETED,
        "job_id": "append_job",
        "input_rows": 2,
        "input_columns": 2,
        "loaded_rows": 2,
    }


def test_write_dataframe_to_bigquery_replaces_table_contents() -> None:
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
    assert write_result.result_state is BigQueryWriteResultState.COMPLETED


def test_atomic_replace_cleanup_failure_raises_after_success() -> None:
    fake_client = FakeBigQueryClient(
        FakeLoadJob(job_id="stage_job", output_rows=1),
        copy_job=FakeLoadJob(job_id="publish_job", output_rows=1),
        delete_exception=GoogleAPIError("cleanup failed"),
    )

    with pytest.raises(RuntimeError, match="Atomic swap staging cleanup failed"):
        bigquery_client.write_dataframe_to_bigquery(
            pd.DataFrame({"holiday_date": ["2026-01-01"]}),
            "project.raw_ext.holidays",
            write_mode="replace",
            client=fake_client,
        )


def test_atomic_replace_cleanup_does_not_mask_primary_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    failing_stage_job = FakeLoadJob(
        job_id="stage_job",
        output_rows=1,
        result_exception=GoogleAPIError("stage load failed"),
    )
    fake_client = FakeBigQueryClient(
        failing_stage_job,
        delete_exception=GoogleAPIError("cleanup failed"),
    )

    with pytest.raises(GoogleAPIError, match="stage load failed"):
        bigquery_client.write_dataframe_to_bigquery(
            pd.DataFrame({"holiday_date": ["2026-01-01"]}),
            "project.raw_ext.holidays",
            write_mode="replace",
            client=fake_client,
        )

    assert "Atomic swap staging cleanup failed" in caplog.text


def test_write_dataframe_to_bigquery_rejects_invalid_write_mode() -> None:
    orders_dataframe = pd.DataFrame({"order_id": ["order_1"]})

    with pytest.raises(ValueError, match="write_mode must be one of"):
        bigquery_client.write_dataframe_to_bigquery(
            orders_dataframe,
            "raw_olist.orders",
            write_mode="merge",
            client=FakeBigQueryClient(FakeLoadJob()),
        )


def test_write_dataframe_to_bigquery_rejects_empty_dataframe() -> None:
    empty_orders_dataframe = pd.DataFrame(columns=["order_id"])

    with pytest.raises(ValueError, match="at least one row"):
        bigquery_client.write_dataframe_to_bigquery(
            empty_orders_dataframe,
            "raw_olist.orders",
            client=FakeBigQueryClient(FakeLoadJob()),
        )
