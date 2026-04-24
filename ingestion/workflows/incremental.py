"""Incremental workflow for source-driven Olist batch ingestion."""

from __future__ import annotations

import argparse
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import assert_never

from google.cloud import bigquery

from ingestion.models import IngestionRunSummary, LoadedSourceBatch
from ingestion.olist.batch_runtime import (
    DateWindow,
    DiscoveredOlistBatchFile,
    IncrementalOrderWindows,
)
from ingestion.olist.raw_csv_loader import OlistRawTableSpec
from ingestion.utils.batch_key import BatchKey
from ingestion.utils.bigquery_client import BigQueryWriteResult
from ingestion.utils.ingestion_state import (
    EnrichmentBatchStatus,
    IngestionBatchState,
    PublishBatchStatus,
    RawBatchStatus,
)
from ingestion.weather.fetch_weather_daily import WeatherDailyConfig


@dataclass(frozen=True)
class IncrementalWorkflowServices:
    """Dependency bundle for the incremental runtime workflow."""

    require_cli_value: Callable[[str | None, str], str]
    configure_google_application_credentials: Callable[[str | None], None]
    create_bigquery_client: Callable[..., bigquery.Client]
    discover_olist_batch_files: Callable[..., list[DiscoveredOlistBatchFile]]
    fetch_batch_states: Callable[..., dict[BatchKey, IngestionBatchState]]
    upsert_batch_states: Callable[..., None]
    derive_incremental_order_windows: Callable[..., IncrementalOrderWindows]
    build_batch_metadata: Callable[..., object]
    load_raw_csv: Callable[..., BigQueryWriteResult]
    get_olist_spec: Callable[[str], OlistRawTableSpec]
    build_expected_olist_file_names: Callable[[], dict[str, str]]
    load_holidays: Callable[..., BigQueryWriteResult]
    build_weather_config_from_env: Callable[..., WeatherDailyConfig]
    validate_weather_api_budget: Callable[[date, date, int], None]
    load_weather_daily: Callable[..., BigQueryWriteResult]


class EnrichmentWindowType(StrEnum):
    """Persisted enrichment window categories supported by incremental recovery."""

    HOLIDAY = "holiday"
    WEATHER = "weather"


_ORDERS_SOURCE_NAME = "orders"


def run_incremental_workflow(
    parsed_args: argparse.Namespace,
    *,
    services: IncrementalWorkflowServices,
) -> IngestionRunSummary:
    """Run the incremental batch ingestion workflow."""
    if parsed_args.use_olist_date_range:
        msg = (
            "--use-olist-date-range is a bootstrap-only option. "
            "Incremental runtime derives enrichment windows from persisted "
            "orders batch state."
        )
        raise ValueError(msg)

    holidays_enabled = not parsed_args.skip_holidays
    weather_enabled = not parsed_args.skip_weather
    olist_enabled = not parsed_args.skip_olist
    if not any((olist_enabled, holidays_enabled, weather_enabled)):
        return IngestionRunSummary(
            mode="incremental",
            no_op=True,
            publish_complete=True,
        )

    state_table = services.require_cli_value(
        parsed_args.state_table,
        "INGESTION_STATE_TABLE",
    )
    project_id = services.require_cli_value(parsed_args.project_id, "GCP_PROJECT_ID")
    location = services.require_cli_value(parsed_args.location, "BIGQUERY_LOCATION")
    services.configure_google_application_credentials(parsed_args.credentials_path)
    bigquery_client = services.create_bigquery_client(
        project_id=project_id,
        location=location,
    )

    batch_states = services.fetch_batch_states(bigquery_client, state_table)
    raw_batches_loaded: list[LoadedSourceBatch] = []
    batches_marked_published: list[LoadedSourceBatch] = []
    made_progress = False

    if olist_enabled:
        discovered_batches = services.discover_olist_batch_files(
            parsed_args.landing_dir,
            expected_file_names=services.build_expected_olist_file_names(),
        )
        for batch_file in discovered_batches:
            previous_state = batch_states.get(batch_file.batch_key())
            if _raw_reload_blocked(previous_state):
                continue

            spec = services.get_olist_spec(batch_file.source_name)
            raw_loaded_at_utc = datetime.now(tz=UTC)
            metadata = services.build_batch_metadata(
                batch_file.csv_path,
                batch_id=batch_file.batch_id,
                ingested_at_utc=raw_loaded_at_utc,
            )
            try:
                write_result = services.load_raw_csv(
                    batch_file.csv_path,
                    spec,
                    write_mode="append",
                    metadata=metadata,
                    client=bigquery_client,
                    project_id=project_id,
                    location=location,
                )
                next_state = _build_loaded_batch_state(
                    batch_file,
                    write_result=write_result,
                    raw_loaded_at_utc=raw_loaded_at_utc,
                    previous_state=previous_state,
                    incremental_order_windows=(
                        services.derive_incremental_order_windows(
                            batch_file.csv_path,
                            weather_lookback_days=parsed_args.weather_runtime_lookback_days,
                        )
                        if batch_file.source_name == _ORDERS_SOURCE_NAME
                        else IncrementalOrderWindows()
                    ),
                )
                services.upsert_batch_states(
                    bigquery_client,
                    state_table,
                    [next_state],
                )
                batch_states[batch_file.batch_key()] = next_state
                raw_batches_loaded.append(
                    _build_loaded_source_batch(
                        source_name=batch_file.source_name,
                        table_id=write_result.table_id,
                        loaded_rows=write_result.loaded_rows,
                        batch_id=batch_file.batch_id,
                        source_file_name=batch_file.source_file_name,
                    )
                )
                if (
                    next_state.publish_status is PublishBatchStatus.PUBLISHED
                    and not _publish_already_completed(previous_state)
                ):
                    batches_marked_published.append(
                        _build_loaded_source_batch_from_state(next_state)
                    )
                made_progress = True
            except Exception as exc:
                failed_state = _build_failed_raw_state(
                    batch_file,
                    spec=spec,
                    previous_state=previous_state,
                    error=exc,
                    updated_at_utc=raw_loaded_at_utc,
                )
                services.upsert_batch_states(
                    bigquery_client,
                    state_table,
                    [failed_state],
                )
                batch_states[batch_file.batch_key()] = failed_state
                raise

    pending_order_states = _list_pending_order_states(batch_states)
    _validate_pending_order_states(pending_order_states)
    holiday_date_window = _merge_state_windows(
        pending_order_states,
        window_type=EnrichmentWindowType.HOLIDAY,
    )
    weather_date_window = _merge_state_windows(
        pending_order_states,
        window_type=EnrichmentWindowType.WEATHER,
    )

    if holidays_enabled:
        holiday_candidates = [
            state for state in pending_order_states if state.requires_holiday_run()
        ]
        if holiday_date_window is not None and holiday_candidates:
            try:
                services.load_holidays(
                    holiday_date_window.start_date,
                    holiday_date_window.end_date,
                    country_code=parsed_args.holiday_country_code,
                    write_mode="append",
                    allow_empty=True,
                    client=bigquery_client,
                    project_id=project_id,
                    location=location,
                )
            except Exception as exc:
                failed_states = [
                    state.mark_failure(
                        error=exc,
                        updated_at_utc=datetime.now(tz=UTC),
                        holiday_status=EnrichmentBatchStatus.FAILED,
                    )
                    for state in holiday_candidates
                ]
                services.upsert_batch_states(
                    bigquery_client,
                    state_table,
                    failed_states,
                )
                for failed_state in failed_states:
                    batch_states[failed_state.batch_key()] = failed_state
                raise

            succeeded_states = [
                state.with_holiday_status(
                    EnrichmentBatchStatus.SUCCEEDED,
                    updated_at_utc=datetime.now(tz=UTC),
                )
                for state in holiday_candidates
            ]
            services.upsert_batch_states(
                bigquery_client,
                state_table,
                succeeded_states,
            )
            for succeeded_state in succeeded_states:
                batch_states[succeeded_state.batch_key()] = succeeded_state
            if succeeded_states:
                made_progress = True

    if weather_enabled:
        weather_candidates = [
            state
            for state in _list_pending_order_states(batch_states)
            if state.requires_weather_run()
        ]
        if weather_date_window is not None and weather_candidates:
            weather_config = services.build_weather_config_from_env(
                max_api_calls=parsed_args.openweather_max_calls,
            )
            services.validate_weather_api_budget(
                weather_date_window.start_date,
                weather_date_window.end_date,
                weather_config.max_api_calls,
            )
            try:
                services.load_weather_daily(
                    weather_date_window.start_date,
                    weather_date_window.end_date,
                    weather_config,
                    write_mode="append",
                    client=bigquery_client,
                    project_id=project_id,
                    location=location,
                )
            except Exception as exc:
                failed_states = [
                    state.mark_failure(
                        error=exc,
                        updated_at_utc=datetime.now(tz=UTC),
                        weather_status=EnrichmentBatchStatus.FAILED,
                    )
                    for state in weather_candidates
                ]
                services.upsert_batch_states(
                    bigquery_client,
                    state_table,
                    failed_states,
                )
                for failed_state in failed_states:
                    batch_states[failed_state.batch_key()] = failed_state
                raise

            succeeded_states = [
                state.with_weather_status(
                    EnrichmentBatchStatus.SUCCEEDED,
                    updated_at_utc=datetime.now(tz=UTC),
                )
                for state in weather_candidates
            ]
            services.upsert_batch_states(
                bigquery_client,
                state_table,
                succeeded_states,
            )
            for succeeded_state in succeeded_states:
                batch_states[succeeded_state.batch_key()] = succeeded_state
            if succeeded_states:
                made_progress = True

    publishable_states = [
        state
        for state in batch_states.values()
        if state.raw_status is RawBatchStatus.LOADED
        and state.publish_status is not PublishBatchStatus.PUBLISHED
        and state.publish_ready()
    ]
    if publishable_states:
        published_at_utc = datetime.now(tz=UTC)
        published_states = [
            state.with_publish_status(
                PublishBatchStatus.PUBLISHED,
                updated_at_utc=published_at_utc,
                published_at_utc=published_at_utc,
            )
            for state in publishable_states
        ]
        services.upsert_batch_states(
            bigquery_client,
            state_table,
            published_states,
        )
        for published_state in published_states:
            previous_state = batch_states[published_state.batch_key()]
            batch_states[published_state.batch_key()] = published_state
            if not _publish_already_completed(previous_state):
                batches_marked_published.append(
                    _build_loaded_source_batch_from_state(published_state)
                )
        made_progress = True

    publish_complete = not any(
        state.raw_status is RawBatchStatus.LOADED
        and state.publish_status is not PublishBatchStatus.PUBLISHED
        for state in batch_states.values()
    )
    return IngestionRunSummary(
        mode="incremental",
        no_op=not made_progress,
        publish_complete=publish_complete,
        raw_batches_loaded=tuple(raw_batches_loaded),
        batches_marked_published=tuple(batches_marked_published),
        holiday_date_window=holiday_date_window,
        weather_date_window=weather_date_window,
    )


def _raw_reload_blocked(previous_state: IngestionBatchState | None) -> bool:
    """Return whether one discovered landing batch should skip raw reload."""
    return (
        previous_state is not None
        and previous_state.raw_status is RawBatchStatus.LOADED
    )


def _publish_already_completed(previous_state: IngestionBatchState | None) -> bool:
    """Return whether the batch was already published before this run."""
    return (
        previous_state is not None
        and previous_state.publish_status is PublishBatchStatus.PUBLISHED
    )


def _build_loaded_batch_state(
    batch_file: DiscoveredOlistBatchFile,
    *,
    write_result: BigQueryWriteResult,
    raw_loaded_at_utc: datetime,
    previous_state: IngestionBatchState | None,
    incremental_order_windows: IncrementalOrderWindows,
) -> IngestionBatchState:
    """Build the persisted state row for one successful raw batch load."""
    created_at_utc = (
        previous_state.created_at_utc
        if previous_state is not None
        else raw_loaded_at_utc
    )
    holiday_window = incremental_order_windows.holiday_window
    weather_window = incremental_order_windows.weather_window
    if batch_file.source_name == _ORDERS_SOURCE_NAME:
        holiday_status = (
            EnrichmentBatchStatus.PENDING
            if holiday_window is not None
            else EnrichmentBatchStatus.NOT_REQUIRED
        )
        weather_status = (
            EnrichmentBatchStatus.PENDING
            if weather_window is not None
            else EnrichmentBatchStatus.NOT_REQUIRED
        )
        publish_status = (
            PublishBatchStatus.PENDING
            if holiday_status is EnrichmentBatchStatus.PENDING
            or weather_status is EnrichmentBatchStatus.PENDING
            else PublishBatchStatus.PUBLISHED
        )
    else:
        holiday_status = EnrichmentBatchStatus.NOT_REQUIRED
        weather_status = EnrichmentBatchStatus.NOT_REQUIRED
        publish_status = PublishBatchStatus.PUBLISHED

    return IngestionBatchState.loaded(
        source_name=batch_file.source_name,
        batch_id=batch_file.batch_id,
        source_file_name=batch_file.source_file_name,
        raw_table_id=write_result.table_id,
        raw_loaded_rows=write_result.loaded_rows,
        raw_job_id=write_result.job_id,
        created_at_utc=created_at_utc,
        updated_at_utc=raw_loaded_at_utc,
        raw_loaded_at_utc=raw_loaded_at_utc,
        holiday_window_start_date=(
            holiday_window.start_date if holiday_window is not None else None
        ),
        holiday_window_end_date=(
            holiday_window.end_date if holiday_window is not None else None
        ),
        weather_window_start_date=(
            weather_window.start_date if weather_window is not None else None
        ),
        weather_window_end_date=(
            weather_window.end_date if weather_window is not None else None
        ),
        holiday_status=holiday_status,
        weather_status=weather_status,
        publish_status=publish_status,
        published_at_utc=(
            raw_loaded_at_utc
            if publish_status is PublishBatchStatus.PUBLISHED
            else None
        ),
    )


def _build_failed_raw_state(
    batch_file: DiscoveredOlistBatchFile,
    *,
    spec: OlistRawTableSpec,
    previous_state: IngestionBatchState | None,
    error: Exception,
    updated_at_utc: datetime,
) -> IngestionBatchState:
    """Build the persisted state row for one failed raw batch load."""
    created_at_utc = (
        previous_state.created_at_utc if previous_state is not None else updated_at_utc
    )
    return IngestionBatchState(
        source_name=batch_file.source_name,
        batch_id=batch_file.batch_id,
        source_file_name=batch_file.source_file_name,
        raw_table_id=spec.resolve_table_id(),
        raw_loaded_rows=0,
        raw_job_id=None,
        raw_status=RawBatchStatus.FAILED,
        holiday_status=EnrichmentBatchStatus.NOT_REQUIRED,
        weather_status=EnrichmentBatchStatus.NOT_REQUIRED,
        publish_status=PublishBatchStatus.PENDING,
        holiday_window_start_date=None,
        holiday_window_end_date=None,
        weather_window_start_date=None,
        weather_window_end_date=None,
        last_error_class=error.__class__.__name__,
        last_error_message=str(error),
        created_at_utc=created_at_utc,
        updated_at_utc=updated_at_utc,
        raw_loaded_at_utc=None,
        published_at_utc=None,
    )


def _build_loaded_source_batch(
    *,
    source_name: str,
    table_id: str,
    loaded_rows: int,
    batch_id: str,
    source_file_name: str,
) -> LoadedSourceBatch:
    """Build a summary row for one raw or publish batch transition."""
    return LoadedSourceBatch(
        source_name=source_name,
        table_id=table_id,
        loaded_rows=loaded_rows,
        batch_id=batch_id,
        source_file_name=source_file_name,
    )


def _build_loaded_source_batch_from_state(
    state: IngestionBatchState,
) -> LoadedSourceBatch:
    """Build a summary row from a persisted batch state."""
    return _build_loaded_source_batch(
        source_name=state.source_name,
        table_id=state.raw_table_id,
        loaded_rows=state.raw_loaded_rows,
        batch_id=state.batch_id,
        source_file_name=state.source_file_name,
    )


def _list_pending_order_states(
    batch_states: dict[BatchKey, IngestionBatchState],
) -> list[IngestionBatchState]:
    """Return loaded orders states that still need publish completion."""
    return [
        state
        for state in batch_states.values()
        if state.source_name == _ORDERS_SOURCE_NAME
        and state.raw_status is RawBatchStatus.LOADED
        and state.publish_status is not PublishBatchStatus.PUBLISHED
    ]


def _validate_pending_order_states(
    order_states: list[IngestionBatchState],
) -> None:
    """Fail fast when persisted order state is insufficient for recovery."""
    for state in order_states:
        if state.holiday_status is not EnrichmentBatchStatus.NOT_REQUIRED and (
            state.holiday_window_start_date is None
            or state.holiday_window_end_date is None
        ):
            msg = (
                "Persisted holiday window is required for incremental recovery "
                f"batch_key={state.batch_key()}"
            )
            raise ValueError(msg)

        if state.weather_status is not EnrichmentBatchStatus.NOT_REQUIRED and (
            state.weather_window_start_date is None
            or state.weather_window_end_date is None
        ):
            msg = (
                "Persisted weather window is required for incremental recovery "
                f"batch_key={state.batch_key()}"
            )
            raise ValueError(msg)


def _merge_state_windows(
    order_states: list[IngestionBatchState],
    *,
    window_type: EnrichmentWindowType,
) -> DateWindow | None:
    """Merge persisted holiday or weather windows across pending orders."""
    selected_windows: list[DateWindow] = []
    for state in order_states:
        window = _state_window(state, window_type=window_type)
        if window is not None:
            selected_windows.append(window)

    if not selected_windows:
        return None

    return DateWindow(
        start_date=min(window.start_date for window in selected_windows),
        end_date=max(window.end_date for window in selected_windows),
    )


def _state_window(
    state: IngestionBatchState,
    *,
    window_type: EnrichmentWindowType,
) -> DateWindow | None:
    """Return one persisted enrichment window from an order state."""
    if window_type is EnrichmentWindowType.HOLIDAY:
        if not state.requires_holiday_run():
            return None
        if (
            state.holiday_window_start_date is None
            or state.holiday_window_end_date is None
        ):
            return None
        return DateWindow(
            start_date=state.holiday_window_start_date,
            end_date=state.holiday_window_end_date,
        )

    if window_type is EnrichmentWindowType.WEATHER:
        if not state.requires_weather_run():
            return None
        if (
            state.weather_window_start_date is None
            or state.weather_window_end_date is None
        ):
            return None
        return DateWindow(
            start_date=state.weather_window_start_date,
            end_date=state.weather_window_end_date,
        )

    assert_never(window_type)


__all__ = [
    "IncrementalWorkflowServices",
    "run_incremental_workflow",
]
