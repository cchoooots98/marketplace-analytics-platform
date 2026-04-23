"""Unified ingestion entrypoint for Olist and enrichment raw loads."""

from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Sequence

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

from ingestion.holidays.fetch_holidays import (
    DEFAULT_COUNTRY_CODE as DEFAULT_HOLIDAY_COUNTRY_CODE,
)
from ingestion.holidays.fetch_holidays import load_holidays
from ingestion.olist.load_customers import load_customers_csv
from ingestion.olist.load_geolocation import load_geolocation_csv
from ingestion.olist.load_order_items import load_order_items_csv
from ingestion.olist.load_order_payments import load_order_payments_csv
from ingestion.olist.load_order_reviews import load_order_reviews_csv
from ingestion.olist.load_orders import load_orders_csv
from ingestion.olist.load_products import load_products_csv
from ingestion.olist.load_sellers import load_sellers_csv
from ingestion.utils.bigquery_client import (
    BigQueryWriteResult,
    create_bigquery_client,
)
from ingestion.utils.date_range import parse_date, validate_date_range
from ingestion.utils.runtime_config import (
    CLI_HANDLED_EXCEPTIONS,
    configure_google_application_credentials,
    configure_logging_from_env,
    log_cli_failure,
    require_cli_value,
)
from ingestion.weather.fetch_weather_daily import (
    DEFAULT_MAX_API_CALLS,
    DEFAULT_WRITE_MODE as DEFAULT_WEATHER_WRITE_MODE,
    WeatherDailyConfig,
    build_weather_config_from_env,
    load_weather_daily,
    validate_weather_api_budget,
)

logger = logging.getLogger(__name__)

DEFAULT_OLIST_DATA_DIR = Path("data/olist")
OLIST_ORDERS_FILE_NAME = "olist_orders_dataset.csv"


@dataclass(frozen=True)
class OlistTableLoader:
    """Configuration for one Olist table load in the unified entrypoint.

    Args:
        source_name: Human-readable Olist source name used in logs.
        file_name: Expected local CSV file name.
        load_function: Table-specific loader function.
    """

    source_name: str
    file_name: str
    load_function: Callable[..., BigQueryWriteResult]


OLIST_TABLE_LOADERS: tuple[OlistTableLoader, ...] = (
    OlistTableLoader("orders", "olist_orders_dataset.csv", load_orders_csv),
    OlistTableLoader(
        "order_items", "olist_order_items_dataset.csv", load_order_items_csv
    ),
    OlistTableLoader(
        "order_payments",
        "olist_order_payments_dataset.csv",
        load_order_payments_csv,
    ),
    OlistTableLoader(
        "order_reviews",
        "olist_order_reviews_dataset.csv",
        load_order_reviews_csv,
    ),
    OlistTableLoader("customers", "olist_customers_dataset.csv", load_customers_csv),
    OlistTableLoader("sellers", "olist_sellers_dataset.csv", load_sellers_csv),
    OlistTableLoader("products", "olist_products_dataset.csv", load_products_csv),
    OlistTableLoader(
        "geolocation", "olist_geolocation_dataset.csv", load_geolocation_csv
    ),
)


def run_olist_loaders(
    olist_data_dir: str | Path,
    *,
    client: bigquery.Client,
    project_id: str,
    location: str,
    loaders: tuple[OlistTableLoader, ...] = OLIST_TABLE_LOADERS,
) -> list[BigQueryWriteResult]:
    """Run all configured Olist raw table loaders.

    Args:
        olist_data_dir: Directory containing Olist CSV files.
        client: Shared BigQuery client.
        project_id: Google Cloud project ID.
        location: BigQuery job location.
        loaders: Olist table loader configurations.

    Returns:
        BigQuery write results for the Olist loads.

    Raises:
        FileNotFoundError: If an expected Olist CSV is missing.
        ValueError: If a source contract is broken.
        google.api_core.exceptions.GoogleAPIError: If BigQuery loading fails.
    """
    normalized_data_dir = Path(olist_data_dir)
    write_results: list[BigQueryWriteResult] = []

    for loader in loaders:
        csv_path = normalized_data_dir / loader.file_name
        logger.info(
            "Running Olist raw loader source_name=%s csv_path=%s",
            loader.source_name,
            csv_path,
        )
        write_results.append(
            loader.load_function(
                csv_path,
                client=client,
                project_id=project_id,
                location=location,
            )
        )

    return write_results


def resolve_olist_date_range(olist_data_dir: str | Path) -> tuple[date, date]:
    """Resolve the Olist order purchase date range from the local orders CSV.

    Args:
        olist_data_dir: Directory containing Olist CSV files.

    Returns:
        Inclusive minimum and maximum order purchase dates.

    Raises:
        FileNotFoundError: If the orders CSV is missing.
        ValueError: If no valid order_purchase_timestamp values are found.
    """
    orders_csv_path = Path(olist_data_dir) / OLIST_ORDERS_FILE_NAME
    if not orders_csv_path.is_file():
        msg = f"Olist orders CSV does not exist: {orders_csv_path}"
        raise FileNotFoundError(msg)

    orders_dataframe = pd.read_csv(
        orders_csv_path,
        usecols=["order_purchase_timestamp"],
    )
    purchase_timestamps = pd.to_datetime(
        orders_dataframe["order_purchase_timestamp"],
        errors="coerce",
    ).dropna()
    if purchase_timestamps.empty:
        msg = "Olist orders CSV has no valid order_purchase_timestamp values"
        raise ValueError(msg)

    return purchase_timestamps.dt.date.min(), purchase_timestamps.dt.date.max()


def resolve_enrichment_date_range(
    *,
    start_date_value: str | None,
    end_date_value: str | None,
    use_olist_date_range: bool,
    olist_data_dir: str | Path,
) -> tuple[date, date]:
    """Resolve holiday/weather enrichment date range from CLI values.

    Args:
        start_date_value: Optional explicit start date string.
        end_date_value: Optional explicit end date string.
        use_olist_date_range: Whether to derive dates from Olist orders.
        olist_data_dir: Directory containing Olist CSV files.

    Returns:
        Inclusive start and end dates for enrichment loaders.

    Raises:
        ValueError: If no complete date range is provided.
    """
    if use_olist_date_range:
        return resolve_olist_date_range(olist_data_dir)

    if not start_date_value or not end_date_value:
        msg = (
            "start_date and end_date are required when holiday or weather "
            "ingestion is enabled. Pass --start-date/--end-date or "
            "--use-olist-date-range."
        )
        raise ValueError(msg)

    start_date = parse_date(start_date_value)
    end_date = parse_date(end_date_value)
    validate_date_range(start_date, end_date)
    return start_date, end_date


def require_enrichment_date_range(
    enrichment_date_range: tuple[date, date] | None,
    *,
    consumer_name: str,
) -> tuple[date, date]:
    """Return a resolved enrichment date range for one downstream consumer.

    Args:
        enrichment_date_range: Previously resolved enrichment date range.
        consumer_name: Human-readable downstream consumer for error context.

    Returns:
        The resolved `(start_date, end_date)` tuple.

    Raises:
        ValueError: If the enrichment date range is unexpectedly missing.
    """
    if enrichment_date_range is None:
        msg = (
            "Enrichment date range is required before running "
            f"{consumer_name}. This indicates a CLI orchestration bug."
        )
        raise ValueError(msg)

    return enrichment_date_range


def main(argv: Sequence[str] | None = None) -> int:
    """Run the unified ingestion workflow.

    Args:
        argv: Optional command-line argument sequence.

    Returns:
        Process exit code.
    """
    load_dotenv()

    parser = argparse.ArgumentParser(description="Run MerchantPulse raw ingestion.")
    parser.add_argument("--olist-data-dir", default=str(DEFAULT_OLIST_DATA_DIR))
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--use-olist-date-range", action="store_true")
    parser.add_argument("--skip-olist", action="store_true")
    parser.add_argument("--skip-holidays", action="store_true")
    parser.add_argument("--skip-weather", action="store_true")
    parser.add_argument(
        "--holiday-country-code",
        default=os.getenv("NAGER_COUNTRY_CODE", DEFAULT_HOLIDAY_COUNTRY_CODE),
    )
    parser.add_argument(
        "--openweather-max-calls",
        default=os.getenv("OPENWEATHER_MAX_CALLS_PER_RUN", DEFAULT_MAX_API_CALLS),
    )
    parser.add_argument(
        "--weather-write-mode",
        choices=("append", "replace"),
        default=DEFAULT_WEATHER_WRITE_MODE,
    )
    parser.add_argument("--project-id", default=os.getenv("GCP_PROJECT_ID"))
    parser.add_argument("--location", default=os.getenv("BIGQUERY_LOCATION"))
    parser.add_argument(
        "--credentials-path",
        default=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
    )
    parsed_args = parser.parse_args(argv)
    configure_logging_from_env()

    holidays_enabled = not parsed_args.skip_holidays
    weather_enabled = not parsed_args.skip_weather
    olist_enabled = not parsed_args.skip_olist

    try:
        enrichment_date_range: tuple[date, date] | None = None
        if holidays_enabled or weather_enabled:
            enrichment_date_range = resolve_enrichment_date_range(
                start_date_value=parsed_args.start_date,
                end_date_value=parsed_args.end_date,
                use_olist_date_range=parsed_args.use_olist_date_range,
                olist_data_dir=parsed_args.olist_data_dir,
            )

        weather_config: WeatherDailyConfig | None = None
        if weather_enabled:
            weather_config = build_weather_config_from_env(
                max_api_calls=parsed_args.openweather_max_calls,
            )
            enrichment_date_range = require_enrichment_date_range(
                enrichment_date_range,
                consumer_name="weather budget validation",
            )
            validate_weather_api_budget(
                enrichment_date_range[0],
                enrichment_date_range[1],
                weather_config.max_api_calls,
            )

        project_id = require_cli_value(parsed_args.project_id, "GCP_PROJECT_ID")
        location = require_cli_value(parsed_args.location, "BIGQUERY_LOCATION")
        configure_google_application_credentials(parsed_args.credentials_path)
        bigquery_client = create_bigquery_client(
            project_id=project_id,
            location=location,
        )

        if olist_enabled:
            run_olist_loaders(
                parsed_args.olist_data_dir,
                client=bigquery_client,
                project_id=project_id,
                location=location,
            )

        if holidays_enabled:
            enrichment_date_range = require_enrichment_date_range(
                enrichment_date_range,
                consumer_name="holiday ingestion",
            )
            load_holidays(
                enrichment_date_range[0],
                enrichment_date_range[1],
                country_code=parsed_args.holiday_country_code,
                client=bigquery_client,
                project_id=project_id,
                location=location,
            )

        if weather_enabled:
            enrichment_date_range = require_enrichment_date_range(
                enrichment_date_range,
                consumer_name="weather ingestion",
            )
            if weather_config is None:
                msg = (
                    "Weather configuration is required before weather ingestion. "
                    "This indicates a CLI orchestration bug."
                )
                raise ValueError(msg)
            load_weather_daily(
                enrichment_date_range[0],
                enrichment_date_range[1],
                weather_config,
                write_mode=parsed_args.weather_write_mode,
                client=bigquery_client,
                project_id=project_id,
                location=location,
            )

    except CLI_HANDLED_EXCEPTIONS as exc:
        return log_cli_failure(logger, "Unified ingestion", exc)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
