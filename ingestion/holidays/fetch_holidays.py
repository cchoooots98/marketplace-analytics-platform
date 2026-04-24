"""Fetch Nager.Date holidays and load them into BigQuery raw_ext."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from typing import Any

import pandas as pd
import requests
from google.cloud import bigquery

from ingestion.utils.batch_metadata import add_batch_metadata, build_batch_metadata
from ingestion.utils.bigquery_client import (
    BigQueryWriteResult,
    WriteMode,
    BigQueryWriteResultState,
    write_dataframe_to_bigquery,
)
from ingestion.utils.date_range import (
    parse_date,
    validate_date_range,
)
from ingestion.utils.http import build_retry_session
from ingestion.utils.table_targets import BigQueryDatasetRole, resolve_table_id

logger = logging.getLogger(__name__)

NAGER_PUBLIC_HOLIDAYS_URL = (
    "https://date.nager.at/api/v3/PublicHolidays/{year}/{country_code}"
)
RAW_HOLIDAYS_TABLE_ID = resolve_table_id("holidays", BigQueryDatasetRole.RAW_EXT)
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
            When omitted, uses a retry-enabled managed session.
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
    with _managed_requests_session(session) as request_session:
        try:
            response = request_session.get(api_url, timeout=timeout_seconds)
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
    allow_empty: bool = False,
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
        allow_empty: When true, an empty holiday range becomes a successful
            no-op result instead of an error.
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
        if allow_empty:
            logger.info(
                "No holidays returned for requested range country_code=%s "
                "start_date=%s end_date=%s; treating as a successful no-op",
                normalized_country_code,
                start_date,
                end_date,
            )
            return BigQueryWriteResult(
                table_id=table_id,
                write_mode=write_mode,
                result_state=BigQueryWriteResultState.NO_OP,
                job_id=None,
                input_rows=0,
                input_columns=0,
                loaded_rows=0,
            )
        msg = (
            "No holidays returned for requested range "
            f"country_code={normalized_country_code} start_date={start_date} "
            f"end_date={end_date}"
        )
        raise ValueError(msg)

    source_file_name = (
        f"nager_public_holidays_{normalized_country_code}_{start_date}_{end_date}.json"
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

    with build_retry_session() as managed_session:
        yield managed_session
