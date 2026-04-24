"""Bootstrap workflow for historical ingestion and full publication."""

from __future__ import annotations

import argparse
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date

from google.cloud import bigquery

from ingestion.models import IngestionRunSummary, LoadedSourceBatch
from ingestion.olist.batch_runtime import DateWindow
from ingestion.olist.raw_csv_loader import OlistRawTableSpec
from ingestion.utils.bigquery_client import BigQueryWriteResult
from ingestion.weather.fetch_weather_daily import WeatherDailyConfig


@dataclass(frozen=True)
class BootstrapWorkflowServices:
    """Dependency bundle for the bootstrap workflow."""

    resolve_enrichment_date_range: Callable[..., tuple[date, date]]
    require_enrichment_date_range: Callable[..., tuple[date, date]]
    build_weather_config_from_env: Callable[..., WeatherDailyConfig]
    validate_weather_api_budget: Callable[[date, date, int], None]
    require_cli_value: Callable[[str | None, str], str]
    configure_google_application_credentials: Callable[[str | None], None]
    create_bigquery_client: Callable[..., bigquery.Client]
    iter_olist_specs: Callable[[], tuple[OlistRawTableSpec, ...]]
    run_olist_loaders: Callable[..., list[BigQueryWriteResult]]
    load_holidays: Callable[..., BigQueryWriteResult]
    load_weather_daily: Callable[..., BigQueryWriteResult]


def run_bootstrap_workflow(
    parsed_args: argparse.Namespace,
    *,
    services: BootstrapWorkflowServices,
) -> IngestionRunSummary:
    """Run the historical bootstrap ingestion workflow."""
    holidays_enabled = not parsed_args.skip_holidays
    weather_enabled = not parsed_args.skip_weather
    olist_enabled = not parsed_args.skip_olist
    if not any((olist_enabled, holidays_enabled, weather_enabled)):
        return IngestionRunSummary(
            mode="bootstrap",
            no_op=True,
            publish_complete=True,
        )

    enrichment_date_range: tuple[date, date] | None = None
    if holidays_enabled or weather_enabled:
        enrichment_date_range = services.resolve_enrichment_date_range(
            start_date_value=parsed_args.start_date,
            end_date_value=parsed_args.end_date,
            use_olist_date_range=parsed_args.use_olist_date_range,
            olist_data_dir=parsed_args.olist_data_dir,
        )

    weather_config: WeatherDailyConfig | None = None
    if weather_enabled:
        weather_config = services.build_weather_config_from_env(
            max_api_calls=parsed_args.openweather_max_calls,
        )
        weather_range = services.require_enrichment_date_range(
            enrichment_date_range,
            consumer_name="weather budget validation",
        )
        services.validate_weather_api_budget(
            weather_range[0],
            weather_range[1],
            weather_config.max_api_calls,
        )

    project_id = services.require_cli_value(parsed_args.project_id, "GCP_PROJECT_ID")
    location = services.require_cli_value(parsed_args.location, "BIGQUERY_LOCATION")
    services.configure_google_application_credentials(parsed_args.credentials_path)
    bigquery_client = services.create_bigquery_client(
        project_id=project_id,
        location=location,
    )

    raw_batches_loaded: tuple[LoadedSourceBatch, ...] = ()
    batches_marked_published: tuple[LoadedSourceBatch, ...] = ()
    made_progress = False
    if olist_enabled:
        olist_specs = services.iter_olist_specs()
        write_results = services.run_olist_loaders(
            parsed_args.olist_data_dir,
            client=bigquery_client,
            project_id=project_id,
            location=location,
            specs=olist_specs,
        )
        raw_batches_loaded = tuple(
            LoadedSourceBatch(
                source_name=spec.source_name,
                table_id=write_result.table_id,
                loaded_rows=write_result.loaded_rows,
                source_file_name=spec.default_file_name,
            )
            for spec, write_result in zip(olist_specs, write_results, strict=True)
        )
        batches_marked_published = raw_batches_loaded
        if write_results:
            made_progress = True

    holiday_window: DateWindow | None = None
    if holidays_enabled:
        holiday_range = services.require_enrichment_date_range(
            enrichment_date_range,
            consumer_name="holiday ingestion",
        )
        services.load_holidays(
            holiday_range[0],
            holiday_range[1],
            country_code=parsed_args.holiday_country_code,
            client=bigquery_client,
            project_id=project_id,
            location=location,
        )
        holiday_window = DateWindow(
            start_date=holiday_range[0],
            end_date=holiday_range[1],
        )
        made_progress = True

    weather_window: DateWindow | None = None
    if weather_enabled:
        weather_range = services.require_enrichment_date_range(
            enrichment_date_range,
            consumer_name="weather ingestion",
        )
        if weather_config is None:
            msg = "Weather configuration must be resolved before weather ingestion."
            raise ValueError(msg)
        services.load_weather_daily(
            weather_range[0],
            weather_range[1],
            weather_config,
            write_mode=parsed_args.weather_write_mode,
            client=bigquery_client,
            project_id=project_id,
            location=location,
        )
        weather_window = DateWindow(
            start_date=weather_range[0],
            end_date=weather_range[1],
        )
        made_progress = True

    return IngestionRunSummary(
        mode="bootstrap",
        no_op=not made_progress,
        publish_complete=True,
        raw_batches_loaded=raw_batches_loaded,
        batches_marked_published=batches_marked_published,
        holiday_date_window=holiday_window,
        weather_date_window=weather_window,
    )


__all__ = [
    "BootstrapWorkflowServices",
    "run_bootstrap_workflow",
]
