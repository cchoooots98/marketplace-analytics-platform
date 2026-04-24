"""CLI parsing helpers for unified ingestion entrypoints."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Sequence

from ingestion.holidays.fetch_holidays import (
    DEFAULT_COUNTRY_CODE as DEFAULT_HOLIDAY_COUNTRY_CODE,
)
from ingestion.weather.fetch_weather_daily import (
    DEFAULT_MAX_API_CALLS,
    DEFAULT_WRITE_MODE as DEFAULT_WEATHER_WRITE_MODE,
)

DEFAULT_OLIST_DATA_DIR = Path(
    os.getenv(
        "OLIST_DATA_DIR",
        "data/olist",
    )
)
DEFAULT_OLIST_LANDING_DIR = Path(
    os.getenv(
        "OLIST_LANDING_DIR",
        "data/olist_landing",
    )
)
DEFAULT_INGESTION_MODE = "bootstrap"
DEFAULT_INGESTION_STATE_TABLE = "ops.ingestion_batch_state"
DEFAULT_WEATHER_RUNTIME_LOOKBACK_DAYS = 7


def build_argument_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser for the unified ingestion workflow."""
    parser = argparse.ArgumentParser(description="Run MerchantPulse raw ingestion.")
    parser.add_argument(
        "--mode",
        choices=("bootstrap", "incremental"),
        default=DEFAULT_INGESTION_MODE,
    )
    parser.add_argument("--olist-data-dir", default=str(DEFAULT_OLIST_DATA_DIR))
    parser.add_argument("--landing-dir", default=str(DEFAULT_OLIST_LANDING_DIR))
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
    parser.add_argument(
        "--weather-runtime-lookback-days",
        type=int,
        default=int(
            os.getenv(
                "WEATHER_RUNTIME_LOOKBACK_DAYS",
                DEFAULT_WEATHER_RUNTIME_LOOKBACK_DAYS,
            )
        ),
        help=(
            "Replay N prior days of delivery-date weather because weather "
            "observations can arrive late or be corrected. Holiday calendars "
            "do not use lookback because published holiday history is static."
        ),
    )
    parser.add_argument(
        "--state-table",
        default=os.getenv("INGESTION_STATE_TABLE", DEFAULT_INGESTION_STATE_TABLE),
    )
    parser.add_argument("--project-id", default=os.getenv("GCP_PROJECT_ID"))
    parser.add_argument("--location", default=os.getenv("BIGQUERY_LOCATION"))
    parser.add_argument(
        "--credentials-path",
        default=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
    )
    return parser


def parse_arguments(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the unified ingestion workflow."""
    return build_argument_parser().parse_args(argv)
