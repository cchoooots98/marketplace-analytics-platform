"""Shared raw CSV loader for Olist source tables."""

from __future__ import annotations

import argparse
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

from ingestion.utils.batch_metadata import (
    BatchMetadata,
    add_batch_metadata,
    build_batch_metadata,
)
from ingestion.utils.bigquery_client import (
    BigQueryWriteResult,
    WriteMode,
    create_bigquery_client,
    write_dataframe_to_bigquery,
)
from ingestion.utils.runtime_config import (
    CLI_HANDLED_EXCEPTIONS,
    configure_google_application_credentials,
    configure_logging_from_env,
    log_cli_failure,
    require_cli_value,
)

logger = logging.getLogger(__name__)

DEFAULT_WRITE_MODE: WriteMode = "replace"


@dataclass(frozen=True)
class OlistRawTableSpec:
    """Source contract for one Olist raw table loader.

    Args:
        source_name: Human-readable Olist source name used in logs and errors.
        table_id: BigQuery raw destination table ID.
        required_columns: Standardized source columns required before loading.
        default_file_name: Expected local CSV file name for CLI help text.
    """

    source_name: str
    table_id: str
    required_columns: frozenset[str]
    default_file_name: str

    def __post_init__(self) -> None:
        """Validate the source spec as soon as it is declared.

        Raises:
            ValueError: If required spec fields are missing or malformed.
        """
        if not self.source_name.strip():
            msg = "source_name cannot be empty"
            raise ValueError(msg)

        if not self.table_id.strip():
            msg = "table_id cannot be empty"
            raise ValueError(msg)

        if not self.required_columns:
            msg = "required_columns cannot be empty"
            raise ValueError(msg)

        if not self.default_file_name.strip():
            msg = "default_file_name cannot be empty"
            raise ValueError(msg)


def prepare_raw_dataframe(
    csv_path: str | Path,
    spec: OlistRawTableSpec,
    *,
    metadata: BatchMetadata | None = None,
) -> pd.DataFrame:
    """Read, standardize, validate, and annotate an Olist raw CSV.

    Args:
        csv_path: Local path to the Olist CSV file.
        spec: Raw table source contract.
        metadata: Optional fixed batch metadata for deterministic tests.

    Returns:
        DataFrame ready to load into the raw BigQuery table.

    Raises:
        FileNotFoundError: If csv_path does not exist.
        ValueError: If required source columns are missing.
        pandas.errors.ParserError: If pandas cannot parse the CSV.
    """
    normalized_csv_path = validate_csv_path(csv_path)
    source_dataframe = _read_validated_raw_csv(normalized_csv_path, spec)
    standardized_dataframe = standardize_column_names(source_dataframe)
    validate_required_columns(standardized_dataframe, spec)

    batch_metadata = metadata or build_batch_metadata(normalized_csv_path)
    return add_batch_metadata(standardized_dataframe, batch_metadata)


def load_raw_csv(
    csv_path: str | Path,
    spec: OlistRawTableSpec,
    *,
    table_id: str | None = None,
    write_mode: WriteMode = DEFAULT_WRITE_MODE,
    client: bigquery.Client | None = None,
    project_id: str | None = None,
    location: str | None = None,
) -> BigQueryWriteResult:
    """Load one Olist raw CSV into BigQuery.

    Args:
        csv_path: Local path to the Olist CSV file.
        spec: Raw table source contract.
        table_id: Optional override destination table ID.
        write_mode: BigQuery write behavior.
        client: Optional preconfigured BigQuery client.
        project_id: Optional Google Cloud project ID used when creating a client.
        location: Optional BigQuery job location such as "EU" or "US".

    Returns:
        A structured summary of the completed BigQuery load job.

    Raises:
        FileNotFoundError: If csv_path does not exist.
        ValueError: If required source columns are missing.
        google.api_core.exceptions.GoogleAPIError: If BigQuery loading fails.
    """
    source_csv_path = Path(csv_path)
    destination_table_id = table_id or spec.table_id
    raw_dataframe = prepare_raw_dataframe(source_csv_path, spec)

    logger.info(
        "Loading Olist raw table source_name=%s source_file_name=%s rows=%s "
        "table_id=%s write_mode=%s",
        spec.source_name,
        source_csv_path.name,
        len(raw_dataframe.index),
        destination_table_id,
        write_mode,
    )

    write_result = write_dataframe_to_bigquery(
        raw_dataframe,
        destination_table_id,
        write_mode=write_mode,
        client=client,
        project_id=project_id,
        location=location,
    )

    logger.info(
        "Loaded Olist raw table source_name=%s loaded_rows=%s job_id=%s",
        spec.source_name,
        write_result.loaded_rows,
        write_result.job_id,
    )
    return write_result


def run_olist_loader(
    argv: Sequence[str] | None,
    spec: OlistRawTableSpec,
) -> int:
    """Run an Olist raw table loader from the command line.

    Args:
        argv: Optional command-line argument sequence.
        spec: Raw table source contract.

    Returns:
        Process exit code.
    """
    load_dotenv()

    parser = argparse.ArgumentParser(
        description=f"Load Olist {spec.source_name} into BigQuery raw."
    )
    parser.add_argument("csv_path", help=f"Path to {spec.default_file_name}")
    parser.add_argument("--table-id", default=spec.table_id)
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
        help=(
            "Path to a Google service-account JSON file. Defaults to "
            "GOOGLE_APPLICATION_CREDENTIALS when set."
        ),
    )
    parsed_args = parser.parse_args(argv)

    configure_logging_from_env()

    try:
        project_id = require_cli_value(parsed_args.project_id, "GCP_PROJECT_ID")
        location = require_cli_value(parsed_args.location, "BIGQUERY_LOCATION")
        configure_google_application_credentials(parsed_args.credentials_path)
        bigquery_client = create_bigquery_client(
            project_id=project_id,
            location=location,
        )
        load_raw_csv(
            parsed_args.csv_path,
            spec,
            table_id=parsed_args.table_id,
            write_mode=parsed_args.write_mode,
            client=bigquery_client,
            project_id=project_id,
            location=location,
        )
    except CLI_HANDLED_EXCEPTIONS as exc:
        return log_cli_failure(logger, f"Olist {spec.source_name} ingestion", exc)

    return 0


def read_raw_csv(csv_path: str | Path, spec: OlistRawTableSpec) -> pd.DataFrame:
    """Read one Olist CSV into a DataFrame.

    Args:
        csv_path: Local path to the Olist CSV file.
        spec: Raw table source contract used for logging context.

    Returns:
        Raw source DataFrame as parsed by pandas.

    Raises:
        FileNotFoundError: If csv_path does not exist.
        pandas.errors.ParserError: If pandas cannot parse the CSV.
    """
    normalized_csv_path = validate_csv_path(csv_path)
    return _read_validated_raw_csv(normalized_csv_path, spec)


def _read_validated_raw_csv(
    csv_path: Path,
    spec: OlistRawTableSpec,
) -> pd.DataFrame:
    """Read one already-validated Olist CSV path into a DataFrame."""
    logger.info(
        "Reading Olist raw CSV source_name=%s path=%s",
        spec.source_name,
        csv_path,
    )
    return pd.read_csv(csv_path)


def standardize_column_names(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of a DataFrame with lowercase snake_case column names.

    Args:
        dataframe: Input DataFrame with source column names.

    Returns:
        DataFrame copy with standardized column names.

    Raises:
        TypeError: If dataframe is not a pandas DataFrame.
        ValueError: If standardization creates duplicate column names.
    """
    if not isinstance(dataframe, pd.DataFrame):
        msg = "dataframe must be a pandas DataFrame"
        raise TypeError(msg)

    standardized_columns = [
        _to_snake_case(column_name) for column_name in dataframe.columns
    ]
    duplicate_columns = _find_duplicates(standardized_columns)
    if duplicate_columns:
        sorted_columns = ", ".join(sorted(duplicate_columns))
        msg = f"standardized column names must be unique: {sorted_columns}"
        raise ValueError(msg)

    standardized_dataframe = dataframe.copy()
    standardized_dataframe.columns = standardized_columns
    return standardized_dataframe


def validate_required_columns(
    dataframe: pd.DataFrame,
    spec: OlistRawTableSpec,
) -> None:
    """Validate one Olist source contract after column standardization.

    Args:
        dataframe: Source DataFrame after column name standardization.
        spec: Raw table source contract.

    Returns:
        None.

    Raises:
        TypeError: If dataframe is not a pandas DataFrame.
        ValueError: If required source columns are missing.
    """
    if not isinstance(dataframe, pd.DataFrame):
        msg = "dataframe must be a pandas DataFrame"
        raise TypeError(msg)

    missing_columns = spec.required_columns.difference(dataframe.columns)
    if missing_columns:
        sorted_columns = ", ".join(sorted(missing_columns))
        msg = f"Olist {spec.source_name} CSV is missing required columns: {sorted_columns}"
        raise ValueError(msg)

    extra_columns = set(dataframe.columns).difference(spec.required_columns)
    if extra_columns:
        logger.info(
            "Olist source has additional columns source_name=%s extra_columns=%s",
            spec.source_name,
            sorted(extra_columns),
        )


def validate_csv_path(csv_path: str | Path) -> Path:
    """Validate that the source CSV path exists and points to a file.

    Args:
        csv_path: Local path to validate.

    Returns:
        Normalized path to a source file.

    Raises:
        FileNotFoundError: If csv_path does not exist.
        ValueError: If csv_path does not point to a file.
    """
    normalized_csv_path = Path(csv_path)
    if not normalized_csv_path.exists():
        msg = f"CSV file does not exist: {normalized_csv_path}"
        raise FileNotFoundError(msg)

    if not normalized_csv_path.is_file():
        msg = f"CSV path must point to a file: {normalized_csv_path}"
        raise ValueError(msg)

    return normalized_csv_path


def _to_snake_case(column_name: object) -> str:
    """Normalize one source column name into lowercase snake_case."""
    normalized_column = str(column_name).strip().lower()
    normalized_column = re.sub(r"[^a-z0-9]+", "_", normalized_column)
    normalized_column = re.sub(r"_+", "_", normalized_column).strip("_")

    if not normalized_column:
        msg = "column names cannot be empty after standardization"
        raise ValueError(msg)

    return normalized_column


def _find_duplicates(values: list[str]) -> set[str]:
    """Return duplicate values from a list."""
    seen_values: set[str] = set()
    duplicate_values: set[str] = set()

    for value in values:
        if value in seen_values:
            duplicate_values.add(value)
        seen_values.add(value)

    return duplicate_values
