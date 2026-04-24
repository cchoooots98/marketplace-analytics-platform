from datetime import UTC, date, datetime

import pandas as pd
import pytest
from google.api_core.exceptions import NotFound

from ingestion.utils.batch_key import BatchKey
from ingestion.utils.ingestion_state import (
    EnrichmentBatchStatus,
    IngestionBatchState,
    PublishBatchStatus,
    RawBatchStatus,
    fetch_batch_states,
    upsert_batch_states,
)


class FakeStateQueryJob:
    """Small BigQuery query-job double for ingestion state tests."""

    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    def result(self) -> list[dict[str, object]]:
        return self._rows


class FakeLoadJob:
    """Small BigQuery load-job double for ingestion state tests."""

    def result(self) -> "FakeLoadJob":
        return self


class FakeStateClient:
    """Small BigQuery client double for current-state control-table tests."""

    def __init__(
        self,
        *,
        query_rows: list[dict[str, object]] | None = None,
        table_exists: bool = True,
    ) -> None:
        self.project = "marketplace"
        self.table_exists = table_exists
        self.query_rows = query_rows or []
        self.queries: list[str] = []
        self.loaded_dataframe: pd.DataFrame | None = None
        self.loaded_table_id: str | None = None
        self.created_table: object | None = None
        self.deleted_table_id: str | None = None

    def get_table(self, table_id: str) -> object:
        if not self.table_exists:
            raise NotFound("missing")
        return object()

    def query(self, sql: str) -> FakeStateQueryJob:
        self.queries.append(sql)
        if sql.lstrip().lower().startswith("select"):
            return FakeStateQueryJob(self.query_rows)
        return FakeStateQueryJob([])

    def load_table_from_dataframe(
        self,
        dataframe: pd.DataFrame,
        table_id: str,
        *,
        job_config: object,
    ) -> FakeLoadJob:
        self.loaded_dataframe = dataframe
        self.loaded_table_id = table_id
        return FakeLoadJob()

    def create_table(self, table: object) -> object:
        self.created_table = table
        self.table_exists = True
        return table

    def delete_table(self, table_id: str, *, not_found_ok: bool) -> None:
        self.deleted_table_id = table_id


def _loaded_orders_state(
    *,
    publish_status: PublishBatchStatus = PublishBatchStatus.PENDING,
    holiday_status: EnrichmentBatchStatus = EnrichmentBatchStatus.PENDING,
    weather_status: EnrichmentBatchStatus = EnrichmentBatchStatus.PENDING,
) -> IngestionBatchState:
    processed_at_utc = datetime(2026, 4, 24, 8, 30, tzinfo=UTC)
    return IngestionBatchState.loaded(
        source_name="orders",
        batch_id="batch_20260424",
        source_file_name="olist_orders_dataset.csv",
        raw_table_id="raw_olist.orders",
        raw_loaded_rows=42,
        raw_job_id="job_123",
        created_at_utc=processed_at_utc,
        updated_at_utc=processed_at_utc,
        raw_loaded_at_utc=processed_at_utc,
        holiday_window_start_date=date(2026, 1, 1),
        holiday_window_end_date=date(2026, 1, 3),
        weather_window_start_date=date(2025, 12, 31),
        weather_window_end_date=date(2026, 1, 5),
        holiday_status=holiday_status,
        weather_status=weather_status,
        publish_status=publish_status,
        published_at_utc=(
            processed_at_utc if publish_status is PublishBatchStatus.PUBLISHED else None
        ),
    )


def test_fetch_batch_states_returns_empty_when_state_table_missing() -> None:
    client = FakeStateClient(table_exists=False)

    assert fetch_batch_states(client, "ops.ingestion_batch_state") == {}


def test_fetch_batch_states_materializes_current_state_rows() -> None:
    client = FakeStateClient(
        query_rows=[
            {
                "source_name": "orders",
                "batch_id": "batch_20260424",
                "source_file_name": "olist_orders_dataset.csv",
                "raw_table_id": "raw_olist.orders",
                "raw_loaded_rows": 42,
                "raw_job_id": "job_123",
                "raw_status": "loaded",
                "holiday_status": "succeeded",
                "weather_status": "pending",
                "publish_status": "pending",
                "holiday_window_start_date": date(2026, 1, 1),
                "holiday_window_end_date": date(2026, 1, 3),
                "weather_window_start_date": date(2025, 12, 31),
                "weather_window_end_date": date(2026, 1, 5),
                "last_error_class": None,
                "last_error_message": None,
                "created_at_utc": datetime(2026, 4, 24, 8, 30, tzinfo=UTC),
                "updated_at_utc": datetime(2026, 4, 24, 8, 35, tzinfo=UTC),
                "raw_loaded_at_utc": datetime(2026, 4, 24, 8, 30, tzinfo=UTC),
                "published_at_utc": None,
            }
        ]
    )

    latest_rows = fetch_batch_states(client, "ops.ingestion_batch_state")

    batch_key = BatchKey(
        source_name="orders",
        batch_id="batch_20260424",
        source_file_name="olist_orders_dataset.csv",
    )
    assert batch_key in latest_rows
    assert latest_rows[batch_key].holiday_status is EnrichmentBatchStatus.SUCCEEDED
    assert latest_rows[batch_key].weather_status is EnrichmentBatchStatus.PENDING
    assert latest_rows[batch_key].publish_status is PublishBatchStatus.PENDING


def test_upsert_batch_states_loads_staging_rows_and_merges() -> None:
    client = FakeStateClient()
    batch_state = _loaded_orders_state()

    upsert_batch_states(
        client,
        "ops.ingestion_batch_state",
        [batch_state],
    )

    assert client.loaded_table_id is not None
    assert client.loaded_table_id.startswith(
        "ops.__ingestion_state_staging_ingestion_batch_state_"
    )
    assert client.loaded_dataframe is not None
    assert client.loaded_dataframe.to_dict("records") == [batch_state.to_dict()]
    assert any(
        "merge `ops.ingestion_batch_state` as target" in query
        for query in client.queries
    )
    assert client.deleted_table_id == client.loaded_table_id


def test_upsert_batch_states_creates_table_when_missing() -> None:
    client = FakeStateClient(table_exists=False)

    upsert_batch_states(
        client,
        "ops.ingestion_batch_state",
        [_loaded_orders_state()],
    )

    assert client.created_table is not None


def test_orders_publish_ready_requires_completed_enrichment() -> None:
    pending_state = _loaded_orders_state()
    assert pending_state.publish_ready() is False

    ready_state = _loaded_orders_state(
        holiday_status=EnrichmentBatchStatus.SUCCEEDED,
        weather_status=EnrichmentBatchStatus.SUCCEEDED,
    )
    assert ready_state.publish_ready() is True


def test_ingestion_batch_state_requires_published_timestamp_when_published() -> None:
    with pytest.raises(ValueError, match="published_at_utc"):
        IngestionBatchState(
            source_name="orders",
            batch_id="batch_20260424",
            source_file_name="olist_orders_dataset.csv",
            raw_table_id="raw_olist.orders",
            raw_loaded_rows=42,
            raw_job_id="job_123",
            raw_status=RawBatchStatus.LOADED,
            holiday_status=EnrichmentBatchStatus.NOT_REQUIRED,
            weather_status=EnrichmentBatchStatus.NOT_REQUIRED,
            publish_status=PublishBatchStatus.PUBLISHED,
            holiday_window_start_date=None,
            holiday_window_end_date=None,
            weather_window_start_date=None,
            weather_window_end_date=None,
            last_error_class=None,
            last_error_message=None,
            created_at_utc=datetime(2026, 4, 24, 8, 30, tzinfo=UTC),
            updated_at_utc=datetime(2026, 4, 24, 8, 30, tzinfo=UTC),
            raw_loaded_at_utc=datetime(2026, 4, 24, 8, 30, tzinfo=UTC),
            published_at_utc=None,
        )


def test_ingestion_batch_state_requires_raw_loaded_timestamp_when_loaded() -> None:
    with pytest.raises(ValueError, match="raw_loaded_at_utc"):
        IngestionBatchState(
            source_name="orders",
            batch_id="batch_20260424",
            source_file_name="olist_orders_dataset.csv",
            raw_table_id="raw_olist.orders",
            raw_loaded_rows=42,
            raw_job_id="job_123",
            raw_status=RawBatchStatus.LOADED,
            holiday_status=EnrichmentBatchStatus.NOT_REQUIRED,
            weather_status=EnrichmentBatchStatus.NOT_REQUIRED,
            publish_status=PublishBatchStatus.PENDING,
            holiday_window_start_date=None,
            holiday_window_end_date=None,
            weather_window_start_date=None,
            weather_window_end_date=None,
            last_error_class=None,
            last_error_message=None,
            created_at_utc=datetime(2026, 4, 24, 8, 30, tzinfo=UTC),
            updated_at_utc=datetime(2026, 4, 24, 8, 30, tzinfo=UTC),
            raw_loaded_at_utc=None,
            published_at_utc=None,
        )


def test_ingestion_batch_state_rejects_partial_holiday_window() -> None:
    with pytest.raises(ValueError, match="holiday windows"):
        IngestionBatchState(
            source_name="orders",
            batch_id="batch_20260424",
            source_file_name="olist_orders_dataset.csv",
            raw_table_id="raw_olist.orders",
            raw_loaded_rows=42,
            raw_job_id="job_123",
            raw_status=RawBatchStatus.PENDING,
            holiday_status=EnrichmentBatchStatus.PENDING,
            weather_status=EnrichmentBatchStatus.NOT_REQUIRED,
            publish_status=PublishBatchStatus.PENDING,
            holiday_window_start_date=date(2026, 1, 1),
            holiday_window_end_date=None,
            weather_window_start_date=None,
            weather_window_end_date=None,
            last_error_class=None,
            last_error_message=None,
            created_at_utc=datetime(2026, 4, 24, 8, 30, tzinfo=UTC),
            updated_at_utc=datetime(2026, 4, 24, 8, 30, tzinfo=UTC),
            raw_loaded_at_utc=None,
            published_at_utc=None,
        )


def test_ingestion_batch_state_rejects_partial_weather_window() -> None:
    with pytest.raises(ValueError, match="weather windows"):
        IngestionBatchState(
            source_name="orders",
            batch_id="batch_20260424",
            source_file_name="olist_orders_dataset.csv",
            raw_table_id="raw_olist.orders",
            raw_loaded_rows=42,
            raw_job_id="job_123",
            raw_status=RawBatchStatus.PENDING,
            holiday_status=EnrichmentBatchStatus.NOT_REQUIRED,
            weather_status=EnrichmentBatchStatus.PENDING,
            publish_status=PublishBatchStatus.PENDING,
            holiday_window_start_date=None,
            holiday_window_end_date=None,
            weather_window_start_date=date(2025, 12, 31),
            weather_window_end_date=None,
            last_error_class=None,
            last_error_message=None,
            created_at_utc=datetime(2026, 4, 24, 8, 30, tzinfo=UTC),
            updated_at_utc=datetime(2026, 4, 24, 8, 30, tzinfo=UTC),
            raw_loaded_at_utc=None,
            published_at_utc=None,
        )


def test_ingestion_batch_state_rejects_negative_loaded_rows() -> None:
    with pytest.raises(ValueError, match="raw_loaded_rows cannot be negative"):
        IngestionBatchState(
            source_name="orders",
            batch_id="batch_20260424",
            source_file_name="olist_orders_dataset.csv",
            raw_table_id="raw_olist.orders",
            raw_loaded_rows=-1,
            raw_job_id="job_123",
            raw_status=RawBatchStatus.PENDING,
            holiday_status=EnrichmentBatchStatus.NOT_REQUIRED,
            weather_status=EnrichmentBatchStatus.NOT_REQUIRED,
            publish_status=PublishBatchStatus.PENDING,
            holiday_window_start_date=None,
            holiday_window_end_date=None,
            weather_window_start_date=None,
            weather_window_end_date=None,
            last_error_class=None,
            last_error_message=None,
            created_at_utc=datetime(2026, 4, 24, 8, 30, tzinfo=UTC),
            updated_at_utc=datetime(2026, 4, 24, 8, 30, tzinfo=UTC),
            raw_loaded_at_utc=None,
            published_at_utc=None,
        )


def test_mark_failure_records_error_metadata() -> None:
    failed_state = _loaded_orders_state().mark_failure(
        error=ValueError("quota exceeded"),
        updated_at_utc=datetime(2026, 4, 24, 8, 45, tzinfo=UTC),
        weather_status=EnrichmentBatchStatus.FAILED,
    )

    assert failed_state.weather_status is EnrichmentBatchStatus.FAILED
    assert failed_state.last_error_class == "ValueError"
    assert failed_state.last_error_message == "quota exceeded"
