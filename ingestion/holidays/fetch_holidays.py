"""Fetch Nager.Date holidays and load them into BigQuery raw_ext."""

from __future__ import annotations

import argparse
import json
import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from typing import Any, Sequence

import pandas as pd
import requests
from dotenv import load_dotenv
from google.cloud import bigquery

from ingestion.utils.batch_metadata import add_batch_metadata, build_batch_metadata
from ingestion.utils.bigquery_client import (
    BigQueryConfigurationError,
    BigQueryWriteResult,
    WriteMode,
    create_bigquery_client,
    write_dataframe_to_bigquery,
)
from ingestion.utils.date_range import (
    parse_date,
    validate_date_range,
)
from ingestion.utils.runtime_config import (
    configure_google_application_credentials,
    configure_logging_from_env,
    require_cli_value,
)

logger = logging.getLogger(__name__)

NAGER_PUBLIC_HOLIDAYS_URL = (
    "https://date.nager.at/api/v3/PublicHolidays/{year}/{country_code}"
)
RAW_HOLIDAYS_TABLE_ID = "raw_ext.holidays"
DEFAULT_COUNTRY_CODE = "BR"
DEFAULT_WRITE_MODE: WriteMode = "replace"
DEFAULT_TIMEOUT_SECONDS = 30


def fetch_public_holidays(
    year: int,
    country_code: str,
    *,
    session: requests.Session | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> list[dict[str, object]]:
    """Fetch public holidays for one country-year from Nager.Date.

    Args:
        year: Calendar year to fetch.
        country_code: ISO 3166-1 alpha-2 country code such as "BR".
        session: Optional caller-owned requests session for connection reuse and tests.
            When omitted, uses requests.get for a one-shot call without connection pooling.
        timeout_seconds: HTTP timeout in seconds.

    Returns:
        A list of holiday records from the Nager.Date API.

    Raises:
        ValueError: If country_code is invalid or the API response is malformed.
        requests.HTTPError: If the API returns an HTTP error status.
        requests.Timeout: If the API call times out.
        requests.RequestException: If the API request fails.
    """
    normalized_country_code = normalize_country_code(country_code)
    api_url = NAGER_PUBLIC_HOLIDAYS_URL.format(
        year=year,
        country_code=normalized_country_code,
    )
    request_get = session.get if session is not None else requests.get

    try:
        response = request_get(api_url, timeout=timeout_seconds)
        response.raise_for_status()
        records = response.json()
    except requests.Timeout:
        logger.error(
            "Nager.Date holiday request timed out year=%s country_code=%s url=%s",
            year,
            normalized_country_code,
            api_url,
        )
        raise
    except requests.HTTPError as exc:
        status_code = getattr(exc.response, "status_code", None)
        logger.error(
            "Nager.Date holiday request failed year=%s country_code=%s "
            "status=%s url=%s",
            year,
            normalized_country_code,
            status_code,
            api_url,
        )
        raise
    except requests.RequestException:
        logger.error(
            "Nager.Date holiday request errored year=%s country_code=%s url=%s",
            year,
            normalized_country_code,
            api_url,
        )
        raise
    except ValueError as exc:
        msg = (
            "Nager.Date holiday response must be valid JSON "
            f"year={year} country_code={normalized_country_code}"
        )
        raise ValueError(msg) from exc

    if not isinstance(records, list):
        msg = (
            "Nager.Date holiday response must be a list "
            f"year={year} country_code={normalized_country_code}"
        )
        raise ValueError(msg)

    return [dict(record) for record in records]


def fetch_holidays_for_date_range(
    start_date: date,
    end_date: date,
    country_code: str,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, object]]:
    """Fetch holidays for every year touched by a date range.

    Args:
        start_date: First holiday date to include.
        end_date: Last holiday date to include.
        country_code: ISO 3166-1 alpha-2 country code.
        session: Optional caller-owned requests session for connection reuse and tests.

    Returns:
        Holiday records filtered to the requested date range.

    Raises:
        ValueError: If the date range or country code is invalid.
        requests.RequestException: If a Nager.Date request fails.
    """
    validate_date_range(start_date, end_date)
    normalized_country_code = normalize_country_code(country_code)
    records: list[dict[str, object]] = []

    with _managed_requests_session(session) as request_session:
        for year in range(start_date.year, end_date.year + 1):
            records.extend(
                fetch_public_holidays(
                    year,
                    normalized_country_code,
                    session=request_session,
                )
            )

    filtered_records: list[dict[str, object]] = []
    for record in records:
        holiday_date = parse_date(str(record.get("date", "")))
        if start_date <= holiday_date <= end_date:
            filtered_records.append(record)

    return filtered_records


def normalize_holidays(
    records: list[dict[str, object]],
) -> pd.DataFrame:
    """Normalize Nager.Date holiday records into the raw table shape.

    Args:
        records: Raw holiday records returned by Nager.Date.

    Returns:
        Source-shaped holiday DataFrame.

    Raises:
        ValueError: If a record date is malformed.
    """
    normalized_records: list[dict[str, Any]] = []

    for record in records:
        holiday_date = parse_date(str(record.get("date", "")))

        normalized_records.append(
            {
                "holiday_date": holiday_date,
                "local_name": record.get("localName"),
                "holiday_name": record.get("name"),
                "country_code": record.get("countryCode"),
                "is_global": record.get("global"),
                "counties_json": _json_or_none(record.get("counties")),
                "holiday_types_json": _json_or_none(record.get("types")),
                "launch_year": record.get("launchYear"),
            }
        )

    return pd.DataFrame(
        normalized_records,
        columns=[
            "holiday_date",
            "local_name",
            "holiday_name",
            "country_code",
            "is_global",
            "counties_json",
            "holiday_types_json",
            "launch_year",
        ],
    )


def load_holidays(
    start_date: date,
    end_date: date,
    *,
    country_code: str = DEFAULT_COUNTRY_CODE,
    table_id: str = RAW_HOLIDAYS_TABLE_ID,
    write_mode: WriteMode = DEFAULT_WRITE_MODE,
    client: bigquery.Client | None = None,
    project_id: str | None = None,
    location: str | None = None,
    session: requests.Session | None = None,
) -> BigQueryWriteResult:
    """Fetch, normalize, annotate, and load holidays into BigQuery.

    Args:
        start_date: First holiday date to include.
        end_date: Last holiday date to include.
        country_code: ISO 3166-1 alpha-2 country code.
        table_id: Destination BigQuery table ID.
        write_mode: BigQuery write behavior.
        client: Optional preconfigured BigQuery client.
        project_id: Optional Google Cloud project ID used when creating a client.
        location: Optional BigQuery job location such as "EU" or "US".
        session: Optional requests session for API calls.

    Returns:
        A structured summary of the completed BigQuery load job.

    Raises:
        ValueError: If the date range is invalid or no holidays are returned.
        requests.RequestException: If a Nager.Date request fails.
        google.api_core.exceptions.GoogleAPIError: If BigQuery loading fails.
    """
    normalized_country_code = normalize_country_code(country_code)
    holiday_records = fetch_holidays_for_date_range(
        start_date,
        end_date,
        normalized_country_code,
        session=session,
    )
    holidays_dataframe = normalize_holidays(holiday_records)
    if holidays_dataframe.empty:
        msg = (
            "No holidays returned for requested range "
            f"country_code={normalized_country_code} start_date={start_date} "
            f"end_date={end_date}"
        )
        raise ValueError(msg)

    source_file_name = (
        f"nager_public_holidays_{normalized_country_code}_"
        f"{start_date}_{end_date}.json"
    )
    metadata = build_batch_metadata(source_file_name)
    holidays_with_metadata = add_batch_metadata(holidays_dataframe, metadata)

    logger.info(
        "Loading holiday raw table country_code=%s start_date=%s end_date=%s "
        "rows=%s table_id=%s write_mode=%s",
        normalized_country_code,
        start_date,
        end_date,
        len(holidays_with_metadata.index),
        table_id,
        write_mode,
    )

    write_result = write_dataframe_to_bigquery(
        holidays_with_metadata,
        table_id,
        write_mode=write_mode,
        client=client,
        project_id=project_id,
        location=location,
    )

    logger.info(
        "Loaded holiday raw table loaded_rows=%s job_id=%s",
        write_result.loaded_rows,
        write_result.job_id,
    )
    return write_result


def main(argv: Sequence[str] | None = None) -> int:
    """Run the holiday loader from the command line.

    Args:
        argv: Optional command-line argument sequence.

    Returns:
        Process exit code.
    """
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Load public holidays into BigQuery raw."
    )
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--country-code", default=os.getenv("NAGER_COUNTRY_CODE", "BR"))
    parser.add_argument("--table-id", default=RAW_HOLIDAYS_TABLE_ID)
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
    parsed_args = parser.parse_args(argv)
    configure_logging_from_env()

    try:
        start_date = parse_date(parsed_args.start_date)
        end_date = parse_date(parsed_args.end_date)
        validate_date_range(start_date, end_date)
        project_id = require_cli_value(parsed_args.project_id, "GCP_PROJECT_ID")
        location = require_cli_value(parsed_args.location, "BIGQUERY_LOCATION")
        configure_google_application_credentials(parsed_args.credentials_path)
        bigquery_client = create_bigquery_client(
            project_id=project_id,
            location=location,
        )
        load_holidays(
            start_date,
            end_date,
            country_code=parsed_args.country_code,
            table_id=parsed_args.table_id,
            write_mode=parsed_args.write_mode,
            client=bigquery_client,
            project_id=project_id,
            location=location,
        )
    except (BigQueryConfigurationError, ValueError, requests.RequestException) as exc:
        logger.error("Holiday ingestion failed: %s", exc)
        return 1

    return 0


def normalize_country_code(country_code: str) -> str:
    """Normalize and validate an ISO alpha-2 country code.

    Args:
        country_code: Country code to validate.

    Returns:
        Uppercase country code.

    Raises:
        ValueError: If country_code is empty or malformed.
    """
    normalized_country_code = country_code.strip().upper()
    if len(normalized_country_code) != 2 or not normalized_country_code.isalpha():
        msg = "country_code must be a two-letter ISO country code"
        raise ValueError(msg)

    return normalized_country_code


def _json_or_none(value: object) -> str | None:
    """Serialize optional nested API values as JSON for raw storage."""
    if value is None:
        return None

    return json.dumps(value, sort_keys=True)


@contextmanager
def _managed_requests_session(
    session: requests.Session | None,
) -> Iterator[requests.Session]:
    """Yield a requests session while closing only sessions created here."""
    if session is not None:
        yield session
        return

    with requests.Session() as managed_session:
        yield managed_session


if __name__ == "__main__":
    raise SystemExit(main())
