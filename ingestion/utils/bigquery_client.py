"""BigQuery write helpers for ingestion jobs."""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from typing import Literal

import pandas as pd
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery

logger = logging.getLogger(__name__)

WriteMode = Literal["append", "replace"]


class BigQueryConfigurationError(RuntimeError):
    """Raised when BigQuery runtime configuration is missing or invalid."""


@dataclass(frozen=True)
class BigQueryWriteResult:
    """Structured summary of a BigQuery DataFrame load job.

    Args:
        table_id: Destination table in dataset.table or project.dataset.table form.
        write_mode: Write behavior requested by the ingestion job.
        job_id: BigQuery load job identifier, when returned by the API.
        input_rows: Number of rows received from the input DataFrame.
        input_columns: Number of columns received from the input DataFrame.
        loaded_rows: Number of rows reported by the completed BigQuery load job.
    """

    table_id: str
    write_mode: WriteMode
    job_id: str | None
    input_rows: int
    input_columns: int
    loaded_rows: int

    def to_log_dict(self) -> dict[str, int | str | None]:
        """Convert the write result to a logging-friendly dictionary.

        Returns:
            A dictionary that can be passed to logs, orchestration metadata, or tests.
        """
        return asdict(self)


def create_bigquery_client(
    project_id: str | None = None,
    location: str | None = None,
) -> bigquery.Client:
    """Create a BigQuery client for ingestion jobs.

    Args:
        project_id: Optional Google Cloud project ID. When omitted, the Google
            client library reads the project from local credentials or
            environment configuration.
        location: Optional BigQuery location such as "EU" or "US".

    Returns:
        A configured BigQuery client.

    Raises:
        ValueError: If project_id or location is provided as an empty string.
        BigQueryConfigurationError: If credentials cannot be discovered by the
            Google client library.
    """
    normalized_project_id = _normalize_optional_text(project_id, "project_id")
    normalized_location = _normalize_optional_text(location, "location")

    try:
        return bigquery.Client(
            project=normalized_project_id,
            location=normalized_location,
        )
    except DefaultCredentialsError as exc:
        msg = (
            "BigQuery credentials were not found. Set GOOGLE_APPLICATION_CREDENTIALS "
            "to a readable service-account JSON file, or configure Application "
            "Default Credentials before running ingestion."
        )
        raise BigQueryConfigurationError(msg) from exc


def write_dataframe_to_bigquery(
    dataframe: pd.DataFrame,
    table_id: str,
    *,
    write_mode: WriteMode = "append",
    client: bigquery.Client | None = None,
    project_id: str | None = None,
    location: str | None = None,
) -> BigQueryWriteResult:
    """Write a pandas DataFrame to a BigQuery table.

    Args:
        dataframe: Source DataFrame to load into BigQuery.
        table_id: Destination table in dataset.table or project.dataset.table form.
        write_mode: Append rows to the table or replace the table contents.
        client: Optional preconfigured BigQuery client, useful for tests and
            orchestrated jobs that already own client setup.
        project_id: Optional Google Cloud project ID used when this function
            creates the client.
        location: Optional BigQuery location used for the client and load job.

    Returns:
        A structured summary of the completed BigQuery load job.

    Raises:
        TypeError: If dataframe is not a pandas DataFrame.
        ValueError: If the DataFrame is empty, table_id is malformed, or
            write_mode is unsupported.
        google.api_core.exceptions.GoogleAPIError: If the BigQuery load job
            fails in the Google client library.
    """
    _validate_dataframe(dataframe)
    normalized_table_id = _normalize_table_id(table_id)
    load_job_config = _build_load_job_config(write_mode)
    bigquery_client = client or create_bigquery_client(
        project_id=project_id,
        location=location,
    )

    logger.info(
        "Starting BigQuery DataFrame load table_id=%s write_mode=%s input_rows=%s",
        normalized_table_id,
        write_mode,
        len(dataframe.index),
    )

    # Loading via the official client keeps schema inference and retry behavior
    # aligned with BigQuery's production load-job API.
    load_job = bigquery_client.load_table_from_dataframe(
        dataframe,
        normalized_table_id,
        job_config=load_job_config,
        location=_normalize_optional_text(location, "location"),
    )
    load_job.result()

    loaded_rows = _get_loaded_rows(load_job, fallback_rows=len(dataframe.index))
    write_result = BigQueryWriteResult(
        table_id=normalized_table_id,
        write_mode=write_mode,
        job_id=getattr(load_job, "job_id", None),
        input_rows=len(dataframe.index),
        input_columns=len(dataframe.columns),
        loaded_rows=loaded_rows,
    )

    logger.info(
        "Completed BigQuery DataFrame load table_id=%s write_mode=%s "
        "loaded_rows=%s job_id=%s",
        write_result.table_id,
        write_result.write_mode,
        write_result.loaded_rows,
        write_result.job_id,
    )

    return write_result


def _build_load_job_config(write_mode: WriteMode) -> bigquery.LoadJobConfig:
    """Build the BigQuery load job configuration for the requested write mode."""
    write_dispositions = {
        "append": bigquery.WriteDisposition.WRITE_APPEND,
        "replace": bigquery.WriteDisposition.WRITE_TRUNCATE,
    }

    if write_mode not in write_dispositions:
        valid_modes = ", ".join(sorted(write_dispositions))
        msg = f"write_mode must be one of: {valid_modes}"
        raise ValueError(msg)

    return bigquery.LoadJobConfig(
        write_disposition=write_dispositions[write_mode],
    )


def _get_loaded_rows(load_job: object, fallback_rows: int) -> int:
    """Return loaded row count from a completed BigQuery load job."""
    loaded_rows = getattr(load_job, "output_rows", None)
    if loaded_rows is None:
        return fallback_rows

    return int(loaded_rows)


def _normalize_optional_text(value: str | None, field_name: str) -> str | None:
    """Normalize optional text configuration values."""
    if value is None:
        return None

    normalized_value = value.strip()
    if not normalized_value:
        msg = f"{field_name} cannot be empty"
        raise ValueError(msg)

    return normalized_value


def _normalize_table_id(table_id: str) -> str:
    """Validate and normalize a BigQuery destination table ID."""
    if not isinstance(table_id, str):
        msg = "table_id must be a string"
        raise TypeError(msg)

    normalized_table_id = table_id.strip()
    table_parts = normalized_table_id.split(".")

    if len(table_parts) not in {2, 3} or any(not part for part in table_parts):
        msg = "table_id must use dataset.table or project.dataset.table format"
        raise ValueError(msg)

    return normalized_table_id


def _validate_dataframe(dataframe: pd.DataFrame) -> None:
    """Validate the DataFrame contract before starting a load job."""
    if not isinstance(dataframe, pd.DataFrame):
        msg = "dataframe must be a pandas DataFrame"
        raise TypeError(msg)

    if dataframe.empty:
        msg = "dataframe must contain at least one row"
        raise ValueError(msg)
