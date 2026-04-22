"""Fetch OpenWeather daily summaries and load them into BigQuery raw_ext."""

from __future__ import annotations

import argparse
import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from typing import Any, Sequence
from collections.abc import Iterator

import pandas as pd
import requests
from dotenv import load_dotenv
from google.cloud import bigquery

from ingestion.utils.batch_metadata import add_batch_metadata, build_batch_metadata
from ingestion.utils.bigquery_client import (
    BigQueryWriteResult,
    WriteMode,
    create_bigquery_client,
    write_dataframe_to_bigquery,
)
from ingestion.utils.date_range import (
    count_days_in_range,
    iter_date_range,
    parse_date,
    validate_date_range,
)
from ingestion.utils.runtime_config import (
    CLI_HANDLED_EXCEPTIONS,
    configure_google_application_credentials,
    configure_logging_from_env,
    log_cli_failure,
    require_cli_value,
)
from ingestion.utils.http import build_retry_session

logger = logging.getLogger(__name__)

OPENWEATHER_DAILY_SUMMARY_URL = (
    "https://api.openweathermap.org/data/3.0/onecall/day_summary"
)
RAW_WEATHER_DAILY_TABLE_ID = "raw_ext.weather_daily"
DEFAULT_LOCATION_KEY = "sao_paulo"
DEFAULT_UNITS = "metric"
DEFAULT_LANG = "en"
DEFAULT_TIMEZONE_OFFSET = "-03:00"
DEFAULT_MAX_API_CALLS = 900
DEFAULT_WRITE_MODE: WriteMode = "replace"
DEFAULT_TIMEOUT_SECONDS = 30


@dataclass(frozen=True)
class WeatherDailyConfig:
    """Runtime configuration for OpenWeather daily summary ingestion.

    Args:
        api_key: OpenWeather One Call API 3.0 key.
        latitude: Latitude for the location.
        longitude: Longitude for the location.
        location_key: Stable warehouse key for the location grain.
        units: OpenWeather units such as "metric".
        lang: OpenWeather language code.
        timezone_offset: Optional timezone offset in +/-HH:MM format.
        max_api_calls: Maximum OpenWeather calls allowed for one run.
    """

    api_key: str
    latitude: float
    longitude: float
    location_key: str
    units: str
    lang: str
    timezone_offset: str | None
    max_api_calls: int

    def __post_init__(self) -> None:
        """Validate weather configuration immediately.

        Raises:
            ValueError: If any required value is missing or out of range.
        """
        if not self.api_key.strip():
            msg = "api_key cannot be empty"
            raise ValueError(msg)

        if not -90 <= self.latitude <= 90:
            msg = "latitude must be between -90 and 90"
            raise ValueError(msg)

        if not -180 <= self.longitude <= 180:
            msg = "longitude must be between -180 and 180"
            raise ValueError(msg)

        if not self.location_key.strip():
            msg = "location_key cannot be empty"
            raise ValueError(msg)

        if not self.units.strip():
            msg = "units cannot be empty"
            raise ValueError(msg)

        if not self.lang.strip():
            msg = "lang cannot be empty"
            raise ValueError(msg)

        if self.max_api_calls < 1:
            msg = "max_api_calls must be at least 1"
            raise ValueError(msg)


def build_weather_config_from_env(
    *,
    api_key: str | None = None,
    latitude: str | float | None = None,
    longitude: str | float | None = None,
    location_key: str | None = None,
    units: str | None = None,
    lang: str | None = None,
    timezone_offset: str | None = None,
    max_api_calls: str | int | None = None,
) -> WeatherDailyConfig:
    """Build weather runtime configuration from overrides and environment.

    Args:
        api_key: Optional OpenWeather API key override.
        latitude: Optional latitude override.
        longitude: Optional longitude override.
        location_key: Optional stable location key override.
        units: Optional units override.
        lang: Optional language override.
        timezone_offset: Optional timezone offset override.
        max_api_calls: Optional per-run API call budget override.

    Returns:
        Validated weather daily configuration.

    Raises:
        ValueError: If required configuration is missing or malformed.
    """
    resolved_api_key = _first_present(api_key, os.getenv("OPENWEATHER_API_KEY"))
    resolved_latitude = _first_present(latitude, os.getenv("OPENWEATHER_LATITUDE"))
    resolved_longitude = _first_present(longitude, os.getenv("OPENWEATHER_LONGITUDE"))
    resolved_max_calls = _first_present(
        max_api_calls,
        os.getenv("OPENWEATHER_MAX_CALLS_PER_RUN"),
        DEFAULT_MAX_API_CALLS,
    )

    return WeatherDailyConfig(
        api_key=str(resolved_api_key or "").strip(),
        latitude=_parse_float(resolved_latitude, "OPENWEATHER_LATITUDE"),
        longitude=_parse_float(resolved_longitude, "OPENWEATHER_LONGITUDE"),
        location_key=str(
            _first_present(
                location_key,
                os.getenv("OPENWEATHER_LOCATION_KEY"),
                DEFAULT_LOCATION_KEY,
            )
        ).strip(),
        units=str(
            _first_present(units, os.getenv("OPENWEATHER_UNITS"), DEFAULT_UNITS)
        ).strip(),
        lang=str(
            _first_present(lang, os.getenv("OPENWEATHER_LANG"), DEFAULT_LANG)
        ).strip(),
        timezone_offset=_normalize_optional_text(
            _first_present(
                timezone_offset,
                os.getenv("OPENWEATHER_TIMEZONE_OFFSET"),
                DEFAULT_TIMEZONE_OFFSET,
            )
        ),
        max_api_calls=_parse_int(resolved_max_calls, "OPENWEATHER_MAX_CALLS_PER_RUN"),
    )


def calculate_weather_api_call_count(
    start_date: date,
    end_date: date,
    *,
    location_count: int = 1,
) -> int:
    """Calculate OpenWeather daily summary API calls for a date-location range.

    Args:
        start_date: First weather date to fetch.
        end_date: Last weather date to fetch.
        location_count: Number of location grains to fetch per date.

    Returns:
        Number of OpenWeather API calls required.

    Raises:
        ValueError: If the date range or location_count is invalid.
    """
    if location_count < 1:
        msg = "location_count must be at least 1"
        raise ValueError(msg)

    return count_days_in_range(start_date, end_date) * location_count


def validate_weather_api_budget(
    start_date: date,
    end_date: date,
    max_api_calls: int,
) -> None:
    """Validate the requested weather date range stays inside the API budget.

    Args:
        start_date: First weather date to fetch.
        end_date: Last weather date to fetch.
        max_api_calls: Maximum allowed OpenWeather calls for one run.

    Returns:
        None.

    Raises:
        ValueError: If the requested range exceeds max_api_calls.
    """
    requested_calls = calculate_weather_api_call_count(start_date, end_date)
    if requested_calls > max_api_calls:
        msg = (
            "Requested weather API calls exceed budget "
            f"requested_calls={requested_calls} max_api_calls={max_api_calls}"
        )
        raise ValueError(msg)


def fetch_daily_weather(
    weather_date: date,
    config: WeatherDailyConfig,
    *,
    session: requests.Session | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, object]:
    """Fetch one OpenWeather daily aggregation record.

    Args:
        weather_date: Date to fetch in local location time.
        config: Weather API runtime configuration.
        session: Optional requests session for connection reuse and tests.
        timeout_seconds: HTTP timeout in seconds.

    Returns:
        One OpenWeather daily summary response.

    Raises:
        ValueError: If the API response is malformed.
        requests.HTTPError: If the API returns an HTTP error status.
        requests.Timeout: If the API call times out.
        requests.RequestException: If the API request fails.
    """
    params: dict[str, object] = {
        "lat": config.latitude,
        "lon": config.longitude,
        "date": weather_date.isoformat(),
        "appid": config.api_key,
        "units": config.units,
        "lang": config.lang,
    }
    if config.timezone_offset:
        params["tz"] = config.timezone_offset

    with _managed_requests_session(session) as request_session:
        try:
            response = request_session.get(
                OPENWEATHER_DAILY_SUMMARY_URL,
                params=params,
                timeout=timeout_seconds,
            )
            response.raise_for_status()
            record = response.json()
        except requests.Timeout:
            logger.error(
                "OpenWeather daily request timed out weather_date=%s location_key=%s",
                weather_date,
                config.location_key,
            )
            raise
        except requests.HTTPError as exc:
            status_code = getattr(exc.response, "status_code", None)
            logger.error(
                "OpenWeather daily request failed weather_date=%s location_key=%s "
                "status=%s",
                weather_date,
                config.location_key,
                status_code,
            )
            raise
        except requests.RequestException:
            logger.error(
                "OpenWeather daily request errored weather_date=%s location_key=%s",
                weather_date,
                config.location_key,
            )
            raise
        except ValueError as exc:
            msg = (
                "OpenWeather daily response must be valid JSON "
                f"weather_date={weather_date} location_key={config.location_key}"
            )
            raise ValueError(msg) from exc

    if not isinstance(record, dict):
        msg = (
            "OpenWeather daily response must be an object "
            f"weather_date={weather_date} location_key={config.location_key}"
        )
        raise ValueError(msg)

    return record


def fetch_weather_for_date_range(
    start_date: date,
    end_date: date,
    config: WeatherDailyConfig,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, object]]:
    """Fetch daily weather records for an inclusive date range.

    Args:
        start_date: First weather date to fetch.
        end_date: Last weather date to fetch.
        config: Weather API runtime configuration.
        session: Optional requests session for connection reuse and tests.

    Returns:
        OpenWeather daily summary records.

    Raises:
        ValueError: If the date range exceeds the configured API budget.
        requests.RequestException: If an OpenWeather request fails.
    """
    validate_date_range(start_date, end_date)
    validate_weather_api_budget(start_date, end_date, config.max_api_calls)
    with _managed_requests_session(session) as request_session:
        return [
            fetch_daily_weather(
                weather_date,
                config,
                session=request_session,
            )
            for weather_date in iter_date_range(start_date, end_date)
        ]


def normalize_daily_weather(
    records: list[dict[str, object]],
    config: WeatherDailyConfig,
) -> pd.DataFrame:
    """Normalize OpenWeather daily summary records into raw table shape.

    Args:
        records: OpenWeather daily summary records.
        config: Weather API runtime configuration.

    Returns:
        Weather daily DataFrame with one row per date-location.

    Raises:
        ValueError: If a record date is missing or malformed.
    """
    normalized_records: list[dict[str, Any]] = []
    for record in records:
        weather_date = parse_date(str(record.get("date", "")))
        normalized_records.append(
            {
                "weather_date": weather_date,
                "location_key": config.location_key,
                "latitude": record.get("lat", config.latitude),
                "longitude": record.get("lon", config.longitude),
                "timezone": record.get("tz", config.timezone_offset),
                "units": record.get("units", config.units),
                "cloud_cover_afternoon": _nested_value(
                    record, "cloud_cover", "afternoon"
                ),
                "humidity_afternoon": _nested_value(record, "humidity", "afternoon"),
                "precipitation_total": _nested_value(record, "precipitation", "total"),
                "temperature_min": _nested_value(record, "temperature", "min"),
                "temperature_max": _nested_value(record, "temperature", "max"),
                "temperature_afternoon": _nested_value(
                    record, "temperature", "afternoon"
                ),
                "temperature_night": _nested_value(record, "temperature", "night"),
                "temperature_evening": _nested_value(record, "temperature", "evening"),
                "temperature_morning": _nested_value(record, "temperature", "morning"),
                "pressure_afternoon": _nested_value(record, "pressure", "afternoon"),
                "wind_max_speed": _nested_value(record, "wind", "max", "speed"),
                "wind_max_direction": _nested_value(record, "wind", "max", "direction"),
            }
        )

    return pd.DataFrame(
        normalized_records,
        columns=[
            "weather_date",
            "location_key",
            "latitude",
            "longitude",
            "timezone",
            "units",
            "cloud_cover_afternoon",
            "humidity_afternoon",
            "precipitation_total",
            "temperature_min",
            "temperature_max",
            "temperature_afternoon",
            "temperature_night",
            "temperature_evening",
            "temperature_morning",
            "pressure_afternoon",
            "wind_max_speed",
            "wind_max_direction",
        ],
    )


def load_weather_daily(
    start_date: date,
    end_date: date,
    config: WeatherDailyConfig,
    *,
    table_id: str = RAW_WEATHER_DAILY_TABLE_ID,
    write_mode: WriteMode = DEFAULT_WRITE_MODE,
    client: bigquery.Client | None = None,
    project_id: str | None = None,
    location: str | None = None,
    session: requests.Session | None = None,
) -> BigQueryWriteResult:
    """Fetch, normalize, annotate, and load weather daily records into BigQuery.

    Args:
        start_date: First weather date to include.
        end_date: Last weather date to include.
        config: Weather API runtime configuration.
        table_id: Destination BigQuery table ID.
        write_mode: BigQuery write behavior.
        client: Optional preconfigured BigQuery client.
        project_id: Optional Google Cloud project ID used when creating a client.
        location: Optional BigQuery job location such as "EU" or "US".
        session: Optional requests session for API calls.

    Returns:
        A structured summary of the completed BigQuery load job.

    Raises:
        ValueError: If the date range is invalid or no records are returned.
        requests.RequestException: If an OpenWeather request fails.
        google.api_core.exceptions.GoogleAPIError: If BigQuery loading fails.
    """
    weather_records = fetch_weather_for_date_range(
        start_date,
        end_date,
        config,
        session=session,
    )
    weather_dataframe = normalize_daily_weather(weather_records, config)
    if weather_dataframe.empty:
        msg = (
            "No weather records returned for requested range "
            f"location_key={config.location_key} start_date={start_date} "
            f"end_date={end_date}"
        )
        raise ValueError(msg)

    source_file_name = (
        f"openweather_day_summary_{config.location_key}_{start_date}_{end_date}.json"
    )
    metadata = build_batch_metadata(source_file_name)
    weather_with_metadata = add_batch_metadata(weather_dataframe, metadata)

    logger.info(
        "Loading weather daily raw table location_key=%s start_date=%s end_date=%s "
        "rows=%s table_id=%s write_mode=%s",
        config.location_key,
        start_date,
        end_date,
        len(weather_with_metadata.index),
        table_id,
        write_mode,
    )

    write_result = write_dataframe_to_bigquery(
        weather_with_metadata,
        table_id,
        write_mode=write_mode,
        client=client,
        project_id=project_id,
        location=location,
    )

    logger.info(
        "Loaded weather daily raw table loaded_rows=%s job_id=%s",
        write_result.loaded_rows,
        write_result.job_id,
    )
    return write_result


def main(argv: Sequence[str] | None = None) -> int:
    """Run the weather daily loader from the command line.

    Args:
        argv: Optional command-line argument sequence.

    Returns:
        Process exit code.
    """
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Load OpenWeather daily summaries into BigQuery raw."
    )
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--table-id", default=RAW_WEATHER_DAILY_TABLE_ID)
    parser.add_argument(
        "--write-mode",
        choices=("append", "replace"),
        default=DEFAULT_WRITE_MODE,
    )
    parser.add_argument("--project-id", default=os.getenv("GCP_PROJECT_ID"))
    parser.add_argument("--location", default=os.getenv("BIGQUERY_LOCATION"))
    parser.add_argument(
        "--credentials-path",
        default=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
    )
    parser.add_argument("--api-key", default=os.getenv("OPENWEATHER_API_KEY"))
    parser.add_argument("--latitude", default=os.getenv("OPENWEATHER_LATITUDE"))
    parser.add_argument("--longitude", default=os.getenv("OPENWEATHER_LONGITUDE"))
    parser.add_argument(
        "--location-key",
        default=os.getenv("OPENWEATHER_LOCATION_KEY", DEFAULT_LOCATION_KEY),
    )
    parser.add_argument(
        "--units", default=os.getenv("OPENWEATHER_UNITS", DEFAULT_UNITS)
    )
    parser.add_argument("--lang", default=os.getenv("OPENWEATHER_LANG", DEFAULT_LANG))
    parser.add_argument(
        "--timezone-offset",
        default=os.getenv("OPENWEATHER_TIMEZONE_OFFSET", DEFAULT_TIMEZONE_OFFSET),
    )
    parser.add_argument(
        "--max-api-calls",
        default=os.getenv("OPENWEATHER_MAX_CALLS_PER_RUN", DEFAULT_MAX_API_CALLS),
    )
    parsed_args = parser.parse_args(argv)
    configure_logging_from_env()

    try:
        start_date = parse_date(parsed_args.start_date)
        end_date = parse_date(parsed_args.end_date)
        validate_date_range(start_date, end_date)
        project_id = require_cli_value(parsed_args.project_id, "GCP_PROJECT_ID")
        location = require_cli_value(parsed_args.location, "BIGQUERY_LOCATION")
        configure_google_application_credentials(parsed_args.credentials_path)
        weather_config = build_weather_config_from_env(
            api_key=parsed_args.api_key,
            latitude=parsed_args.latitude,
            longitude=parsed_args.longitude,
            location_key=parsed_args.location_key,
            units=parsed_args.units,
            lang=parsed_args.lang,
            timezone_offset=parsed_args.timezone_offset,
            max_api_calls=parsed_args.max_api_calls,
        )
        validate_weather_api_budget(
            start_date,
            end_date,
            weather_config.max_api_calls,
        )
        bigquery_client = create_bigquery_client(
            project_id=project_id,
            location=location,
        )
        load_weather_daily(
            start_date,
            end_date,
            weather_config,
            table_id=parsed_args.table_id,
            write_mode=parsed_args.write_mode,
            client=bigquery_client,
            project_id=project_id,
            location=location,
        )
    except CLI_HANDLED_EXCEPTIONS as exc:
        return log_cli_failure(logger, "Weather daily ingestion", exc)

    return 0


def _first_present(*values: object) -> object | None:
    """Return the first value that is not None."""
    for value in values:
        if value is not None:
            return value
    return None


def _parse_float(value: object | None, field_name: str) -> float:
    """Parse a required float configuration value."""
    if value is None or not str(value).strip():
        msg = f"{field_name} is required"
        raise ValueError(msg)

    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        msg = f"{field_name} must be a valid number"
        raise ValueError(msg) from exc


def _parse_int(value: object | None, field_name: str) -> int:
    """Parse a required integer configuration value."""
    if value is None or not str(value).strip():
        msg = f"{field_name} is required"
        raise ValueError(msg)

    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        msg = f"{field_name} must be a valid integer"
        raise ValueError(msg) from exc


def _normalize_optional_text(value: object | None) -> str | None:
    """Normalize optional text configuration values."""
    if value is None:
        return None

    normalized_value = str(value).strip()
    if not normalized_value:
        return None

    return normalized_value


def _nested_value(record: dict[str, object], *path: str) -> object | None:
    """Read a nested value from an OpenWeather response."""
    current_value: object = record
    for key in path:
        if not isinstance(current_value, dict):
            return None
        current_value = current_value.get(key)

    return current_value


@contextmanager
def _managed_requests_session(
    session: requests.Session | None,
) -> Iterator[requests.Session]:
    """Yield a requests session while closing only sessions created here."""
    if session is not None:
        yield session
        return

    with build_retry_session() as managed_session:
        yield managed_session


if __name__ == "__main__":
    raise SystemExit(main())
