"""Runtime configuration helpers for ingestion command-line entrypoints."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from ingestion.utils.bigquery_client import BigQueryConfigurationError


def configure_logging_from_env() -> None:
    """Configure process logging from LOG_LEVEL.

    Returns:
        None.
    """
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def require_cli_value(value: str | None, environment_variable_name: str) -> str:
    """Return a required CLI or environment value after validation.

    Args:
        value: CLI or environment value to validate.
        environment_variable_name: Environment variable name for error context.

    Returns:
        The trimmed value.

    Raises:
        BigQueryConfigurationError: If the required value is missing.
    """
    if value is None or not value.strip():
        msg = (
            f"{environment_variable_name} is required. Set it in .env or pass the "
            "matching command-line option."
        )
        raise BigQueryConfigurationError(msg)

    return value.strip()


def configure_google_application_credentials(
    credentials_path: str | None,
) -> Path | None:
    """Validate and export GOOGLE_APPLICATION_CREDENTIALS when a path is provided.

    Args:
        credentials_path: Optional path to a service-account JSON file.

    Returns:
        Resolved credentials path when provided, otherwise None.

    Raises:
        BigQueryConfigurationError: If credentials_path points to a missing file.
    """
    if credentials_path is None or not credentials_path.strip():
        return None

    normalized_credentials_path = Path(credentials_path.strip()).expanduser()
    if not normalized_credentials_path.is_file():
        msg = (
            "GOOGLE_APPLICATION_CREDENTIALS must point to a readable "
            f"service-account JSON file: {normalized_credentials_path}"
        )
        raise BigQueryConfigurationError(msg)

    resolved_credentials_path = normalized_credentials_path.resolve()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(resolved_credentials_path)
    return resolved_credentials_path
