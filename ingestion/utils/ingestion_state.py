"""BigQuery-backed current-state helpers for source batch processing."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from enum import StrEnum
import uuid

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from ingestion.utils.batch_key import BatchKey
from ingestion.utils.validation import normalize_optional_text, require_text


class RawBatchStatus(StrEnum):
    """Allowed raw-ingestion lifecycle states."""

    PENDING = "pending"
    LOADED = "loaded"
    FAILED = "failed"


class EnrichmentBatchStatus(StrEnum):
    """Allowed enrichment lifecycle states."""

    NOT_REQUIRED = "not_required"
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class PublishBatchStatus(StrEnum):
    """Allowed publish lifecycle states."""

    PENDING = "pending"
    PUBLISHED = "published"
    FAILED = "failed"


STATE_TABLE_FIELDS = (
    "source_name",
    "batch_id",
    "source_file_name",
    "raw_table_id",
    "raw_loaded_rows",
    "raw_job_id",
    "raw_status",
    "holiday_status",
    "weather_status",
    "publish_status",
    "holiday_window_start_date",
    "holiday_window_end_date",
    "weather_window_start_date",
    "weather_window_end_date",
    "last_error_class",
    "last_error_message",
    "created_at_utc",
    "updated_at_utc",
    "raw_loaded_at_utc",
    "published_at_utc",
)


@dataclass(frozen=True)
class IngestionBatchState:
    """Current control-plane state for one source batch.

    Args:
        source_name: Human-readable source name such as ``orders``.
        batch_id: Stable landing batch identifier.
        source_file_name: Source file name inside the landing batch.
        raw_table_id: Raw BigQuery destination table.
        raw_loaded_rows: Rows loaded into the raw table.
        raw_job_id: BigQuery load job identifier for the raw write.
        raw_status: Current raw-ingestion status.
        holiday_status: Current holiday-enrichment status.
        weather_status: Current weather-enrichment status.
        publish_status: Current publish status.
        holiday_window_start_date: Inclusive holiday window start for orders.
        holiday_window_end_date: Inclusive holiday window end for orders.
        weather_window_start_date: Inclusive weather window start for orders.
        weather_window_end_date: Inclusive weather window end for orders.
        last_error_class: Last recorded error class name.
        last_error_message: Last recorded error message.
        created_at_utc: UTC time when the state row was first created.
        updated_at_utc: UTC time when the state row was last updated.
        raw_loaded_at_utc: UTC time when raw ingestion completed.
        published_at_utc: UTC time when publish completed.
    """

    source_name: str
    batch_id: str
    source_file_name: str
    raw_table_id: str
    raw_loaded_rows: int
    raw_job_id: str | None
    raw_status: RawBatchStatus
    holiday_status: EnrichmentBatchStatus
    weather_status: EnrichmentBatchStatus
    publish_status: PublishBatchStatus
    holiday_window_start_date: date | None
    holiday_window_end_date: date | None
    weather_window_start_date: date | None
    weather_window_end_date: date | None
    last_error_class: str | None
    last_error_message: str | None
    created_at_utc: datetime
    updated_at_utc: datetime
    raw_loaded_at_utc: datetime | None
    published_at_utc: datetime | None

    def __post_init__(self) -> None:
        """Validate the state record invariants."""
        if self.raw_loaded_rows < 0:
            msg = "raw_loaded_rows cannot be negative"
            raise ValueError(msg)

        if self.raw_status is RawBatchStatus.LOADED and self.raw_loaded_at_utc is None:
            msg = "raw_loaded_at_utc is required when raw_status=loaded"
            raise ValueError(msg)

        if (
            self.publish_status is PublishBatchStatus.PUBLISHED
            and self.published_at_utc is None
        ):
            msg = "published_at_utc is required when publish_status=published"
            raise ValueError(msg)

        if (self.holiday_window_start_date is None) != (
            self.holiday_window_end_date is None
        ):
            msg = "holiday windows must be fully populated or fully null"
            raise ValueError(msg)

        if (self.weather_window_start_date is None) != (
            self.weather_window_end_date is None
        ):
            msg = "weather windows must be fully populated or fully null"
            raise ValueError(msg)

    @classmethod
    def loaded(
        cls,
        *,
        source_name: str,
        batch_id: str,
        source_file_name: str,
        raw_table_id: str,
        raw_loaded_rows: int,
        raw_job_id: str | None,
        created_at_utc: datetime,
        updated_at_utc: datetime,
        raw_loaded_at_utc: datetime,
        holiday_window_start_date: date | None = None,
        holiday_window_end_date: date | None = None,
        weather_window_start_date: date | None = None,
        weather_window_end_date: date | None = None,
        holiday_status: EnrichmentBatchStatus = EnrichmentBatchStatus.NOT_REQUIRED,
        weather_status: EnrichmentBatchStatus = EnrichmentBatchStatus.NOT_REQUIRED,
        publish_status: PublishBatchStatus = PublishBatchStatus.PENDING,
        published_at_utc: datetime | None = None,
    ) -> "IngestionBatchState":
        """Build a loaded batch state row."""
        return cls(
            source_name=require_text(source_name, "source_name"),
            batch_id=require_text(batch_id, "batch_id"),
            source_file_name=require_text(source_file_name, "source_file_name"),
            raw_table_id=require_text(raw_table_id, "raw_table_id"),
            raw_loaded_rows=raw_loaded_rows,
            raw_job_id=normalize_optional_text(raw_job_id),
            raw_status=RawBatchStatus.LOADED,
            holiday_status=holiday_status,
            weather_status=weather_status,
            publish_status=publish_status,
            holiday_window_start_date=holiday_window_start_date,
            holiday_window_end_date=holiday_window_end_date,
            weather_window_start_date=weather_window_start_date,
            weather_window_end_date=weather_window_end_date,
            last_error_class=None,
            last_error_message=None,
            created_at_utc=_coerce_utc_datetime(created_at_utc),
            updated_at_utc=_coerce_utc_datetime(updated_at_utc),
            raw_loaded_at_utc=_coerce_utc_datetime(raw_loaded_at_utc),
            published_at_utc=(
                _coerce_utc_datetime(published_at_utc)
                if published_at_utc is not None
                else None
            ),
        )

    def batch_key(self) -> BatchKey:
        """Return the stable batch key used for de-duplication."""
        return BatchKey(
            source_name=self.source_name,
            batch_id=self.batch_id,
            source_file_name=self.source_file_name,
        )

    def requires_holiday_run(self) -> bool:
        """Return whether the batch still needs holiday enrichment."""
        return self.holiday_status in {
            EnrichmentBatchStatus.PENDING,
            EnrichmentBatchStatus.FAILED,
        }

    def requires_weather_run(self) -> bool:
        """Return whether the batch still needs weather enrichment."""
        return self.weather_status in {
            EnrichmentBatchStatus.PENDING,
            EnrichmentBatchStatus.FAILED,
        }

    def publish_ready(self) -> bool:
        """Return whether publish may advance to ``published``."""
        return (
            self.raw_status is RawBatchStatus.LOADED
            and self.holiday_status
            in {
                EnrichmentBatchStatus.NOT_REQUIRED,
                EnrichmentBatchStatus.SUCCEEDED,
            }
            and self.weather_status
            in {
                EnrichmentBatchStatus.NOT_REQUIRED,
                EnrichmentBatchStatus.SUCCEEDED,
            }
        )

    def with_holiday_status(
        self,
        status: EnrichmentBatchStatus,
        *,
        updated_at_utc: datetime,
    ) -> "IngestionBatchState":
        """Return a copy with updated holiday status."""
        return replace(
            self,
            holiday_status=status,
            updated_at_utc=_coerce_utc_datetime(updated_at_utc),
            last_error_class=(
                None
                if status is EnrichmentBatchStatus.SUCCEEDED
                else self.last_error_class
            ),
            last_error_message=(
                None
                if status is EnrichmentBatchStatus.SUCCEEDED
                else self.last_error_message
            ),
        )

    def with_weather_status(
        self,
        status: EnrichmentBatchStatus,
        *,
        updated_at_utc: datetime,
    ) -> "IngestionBatchState":
        """Return a copy with updated weather status."""
        return replace(
            self,
            weather_status=status,
            updated_at_utc=_coerce_utc_datetime(updated_at_utc),
            last_error_class=(
                None
                if status is EnrichmentBatchStatus.SUCCEEDED
                else self.last_error_class
            ),
            last_error_message=(
                None
                if status is EnrichmentBatchStatus.SUCCEEDED
                else self.last_error_message
            ),
        )

    def with_publish_status(
        self,
        status: PublishBatchStatus,
        *,
        updated_at_utc: datetime,
        published_at_utc: datetime | None = None,
    ) -> "IngestionBatchState":
        """Return a copy with updated publish status."""
        normalized_updated_at = _coerce_utc_datetime(updated_at_utc)
        return replace(
            self,
            publish_status=status,
            updated_at_utc=normalized_updated_at,
            published_at_utc=(
                _coerce_utc_datetime(published_at_utc)
                if published_at_utc is not None
                else self.published_at_utc
            ),
            last_error_class=(
                None
                if status is PublishBatchStatus.PUBLISHED
                else self.last_error_class
            ),
            last_error_message=(
                None
                if status is PublishBatchStatus.PUBLISHED
                else self.last_error_message
            ),
        )

    def mark_failure(
        self,
        *,
        error: Exception,
        updated_at_utc: datetime,
        raw_status: RawBatchStatus | None = None,
        holiday_status: EnrichmentBatchStatus | None = None,
        weather_status: EnrichmentBatchStatus | None = None,
        publish_status: PublishBatchStatus | None = None,
    ) -> "IngestionBatchState":
        """Return a copy marked as failed at one or more lifecycle stages."""
        return replace(
            self,
            raw_status=raw_status or self.raw_status,
            holiday_status=holiday_status or self.holiday_status,
            weather_status=weather_status or self.weather_status,
            publish_status=publish_status or self.publish_status,
            updated_at_utc=_coerce_utc_datetime(updated_at_utc),
            last_error_class=error.__class__.__name__,
            last_error_message=str(error),
        )

    def to_dict(self) -> dict[str, date | datetime | int | str | None]:
        """Convert the state row into a DataFrame-friendly dictionary."""
        return {
            "source_name": self.source_name,
            "batch_id": self.batch_id,
            "source_file_name": self.source_file_name,
            "raw_table_id": self.raw_table_id,
            "raw_loaded_rows": self.raw_loaded_rows,
            "raw_job_id": self.raw_job_id,
            "raw_status": self.raw_status.value,
            "holiday_status": self.holiday_status.value,
            "weather_status": self.weather_status.value,
            "publish_status": self.publish_status.value,
            "holiday_window_start_date": self.holiday_window_start_date,
            "holiday_window_end_date": self.holiday_window_end_date,
            "weather_window_start_date": self.weather_window_start_date,
            "weather_window_end_date": self.weather_window_end_date,
            "last_error_class": self.last_error_class,
            "last_error_message": self.last_error_message,
            "created_at_utc": self.created_at_utc.astimezone(UTC),
            "updated_at_utc": self.updated_at_utc.astimezone(UTC),
            "raw_loaded_at_utc": (
                self.raw_loaded_at_utc.astimezone(UTC)
                if self.raw_loaded_at_utc is not None
                else None
            ),
            "published_at_utc": (
                self.published_at_utc.astimezone(UTC)
                if self.published_at_utc is not None
                else None
            ),
        }


def fetch_batch_states(
    client: bigquery.Client,
    state_table: str,
) -> dict[BatchKey, IngestionBatchState]:
    """Return the current control-plane row for each processed source batch."""
    try:
        client.get_table(state_table)
    except NotFound:
        return {}

    query = f"""
        select
            {", ".join(STATE_TABLE_FIELDS)}
        from `{state_table}`
    """
    query_job = client.query(query)
    batch_states: dict[BatchKey, IngestionBatchState] = {}
    for row in query_job.result():
        state = IngestionBatchState(
            source_name=str(row["source_name"]),
            batch_id=str(row["batch_id"]),
            source_file_name=str(row["source_file_name"]),
            raw_table_id=str(row["raw_table_id"]),
            raw_loaded_rows=int(row["raw_loaded_rows"]),
            raw_job_id=normalize_optional_text(row["raw_job_id"]),
            raw_status=RawBatchStatus(str(row["raw_status"])),
            holiday_status=EnrichmentBatchStatus(str(row["holiday_status"])),
            weather_status=EnrichmentBatchStatus(str(row["weather_status"])),
            publish_status=PublishBatchStatus(str(row["publish_status"])),
            holiday_window_start_date=_coerce_optional_date(
                row["holiday_window_start_date"]
            ),
            holiday_window_end_date=_coerce_optional_date(
                row["holiday_window_end_date"]
            ),
            weather_window_start_date=_coerce_optional_date(
                row["weather_window_start_date"]
            ),
            weather_window_end_date=_coerce_optional_date(
                row["weather_window_end_date"]
            ),
            last_error_class=normalize_optional_text(row["last_error_class"]),
            last_error_message=normalize_optional_text(row["last_error_message"]),
            created_at_utc=_coerce_utc_datetime(row["created_at_utc"]),
            updated_at_utc=_coerce_utc_datetime(row["updated_at_utc"]),
            raw_loaded_at_utc=_coerce_optional_datetime(row["raw_loaded_at_utc"]),
            published_at_utc=_coerce_optional_datetime(row["published_at_utc"]),
        )
        batch_states[state.batch_key()] = state

    return batch_states


def upsert_batch_states(
    client: bigquery.Client,
    state_table: str,
    rows: list[IngestionBatchState],
) -> None:
    """Upsert current-state rows into the control table."""
    if not rows:
        return

    _ensure_state_table(client, state_table)

    staging_table = _build_staging_table_id(state_table)
    state_dataframe = pd.DataFrame([row.to_dict() for row in rows])
    load_job = client.load_table_from_dataframe(
        state_dataframe,
        staging_table,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    )
    load_job.result()

    merge_job = client.query(_build_merge_sql(state_table, staging_table))
    try:
        merge_job.result()
    finally:
        client.delete_table(staging_table, not_found_ok=True)


def _ensure_state_table(client: bigquery.Client, state_table: str) -> None:
    """Create the current-state table when it does not exist."""
    try:
        client.get_table(state_table)
        return
    except NotFound:
        pass

    table = bigquery.Table(
        _resolve_api_table_id(client, state_table),
        schema=_build_state_table_schema(),
    )
    client.create_table(table)


def _build_state_table_schema() -> list[bigquery.SchemaField]:
    """Return the fixed ingestion control-plane schema."""
    return [
        bigquery.SchemaField("source_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source_file_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("raw_table_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("raw_loaded_rows", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("raw_job_id", "STRING"),
        bigquery.SchemaField("raw_status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("holiday_status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("weather_status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("publish_status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("holiday_window_start_date", "DATE"),
        bigquery.SchemaField("holiday_window_end_date", "DATE"),
        bigquery.SchemaField("weather_window_start_date", "DATE"),
        bigquery.SchemaField("weather_window_end_date", "DATE"),
        bigquery.SchemaField("last_error_class", "STRING"),
        bigquery.SchemaField("last_error_message", "STRING"),
        bigquery.SchemaField("created_at_utc", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("updated_at_utc", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("raw_loaded_at_utc", "TIMESTAMP"),
        bigquery.SchemaField("published_at_utc", "TIMESTAMP"),
    ]


def _build_staging_table_id(state_table: str) -> str:
    """Create a staging table ID for state upserts."""
    table_parts = state_table.split(".")
    table_name = table_parts[-1]
    dataset_parts = table_parts[:-1]
    staging_table_name = (
        f"__ingestion_state_staging_{table_name}_{uuid.uuid4().hex[:12]}"
    )
    return ".".join([*dataset_parts, staging_table_name])


def _build_merge_sql(state_table: str, staging_table: str) -> str:
    """Build the MERGE statement for current-state upserts."""
    update_columns = [
        field_name
        for field_name in STATE_TABLE_FIELDS
        if field_name not in {"source_name", "batch_id", "source_file_name"}
    ]
    update_assignments = ",\n            ".join(
        f"target.{field_name} = source.{field_name}" for field_name in update_columns
    )
    insert_columns = ", ".join(STATE_TABLE_FIELDS)
    insert_values = ", ".join(
        f"source.{field_name}" for field_name in STATE_TABLE_FIELDS
    )
    return f"""
        merge `{state_table}` as target
        using `{staging_table}` as source
            on target.source_name = source.source_name
            and target.batch_id = source.batch_id
            and target.source_file_name = source.source_file_name
        when matched then update set
            {update_assignments}
        when not matched then insert ({insert_columns})
        values ({insert_values})
    """


def _resolve_api_table_id(client: bigquery.Client, table_id: str) -> str:
    """Resolve a BigQuery API table ID using the client default project."""
    table_parts = table_id.split(".")
    if len(table_parts) == 2:
        client_project = normalize_optional_text(getattr(client, "project", None))
        if client_project is not None:
            return f"{client_project}.{table_id}"

    return table_id


def _coerce_optional_date(value: object) -> date | None:
    """Normalize optional BigQuery date values."""
    if value is None:
        return None

    return pd.Timestamp(value).date()


def _coerce_optional_datetime(value: object) -> datetime | None:
    """Normalize optional BigQuery timestamp values."""
    if value is None:
        return None

    return _coerce_utc_datetime(value)


def _coerce_utc_datetime(value: object) -> datetime:
    """Normalize BigQuery timestamps into timezone-aware UTC datetimes."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    parsed_value = pd.Timestamp(value)
    if parsed_value.tzinfo is None:
        parsed_value = parsed_value.tz_localize(UTC)

    return parsed_value.to_pydatetime().astimezone(UTC)
