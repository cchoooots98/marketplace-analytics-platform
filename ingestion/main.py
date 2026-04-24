"""Unified ingestion entrypoint for bootstrap and incremental workflows."""

from __future__ import annotations

import argparse
import json
import logging
from typing import Sequence

from dotenv import load_dotenv

from ingestion.cli import (
    DEFAULT_INGESTION_MODE,
    DEFAULT_INGESTION_STATE_TABLE,
    DEFAULT_OLIST_DATA_DIR,
    DEFAULT_OLIST_LANDING_DIR,
    DEFAULT_WEATHER_RUNTIME_LOOKBACK_DAYS,
)
from ingestion.cli import build_argument_parser as _build_argument_parser
from ingestion.cli import parse_arguments as _parse_arguments
from ingestion.date_resolution import (
    require_enrichment_date_range,
    resolve_enrichment_date_range,
)
from ingestion.holidays.fetch_holidays import load_holidays
from ingestion.models import IngestionRunSummary
from ingestion.olist.batch_runtime import (
    derive_incremental_order_windows,
    discover_olist_batch_files,
)
from ingestion.olist.raw_csv_loader import load_raw_csv
from ingestion.olist.registry import (
    build_expected_olist_file_names,
    get_olist_spec,
    iter_olist_specs,
)
from ingestion.utils.batch_metadata import build_batch_metadata
from ingestion.utils.bigquery_client import create_bigquery_client
from ingestion.utils.ingestion_state import fetch_batch_states, upsert_batch_states
from ingestion.utils.runtime_config import (
    CLI_HANDLED_EXCEPTIONS,
    configure_google_application_credentials,
    configure_logging_from_env,
    log_cli_failure,
    require_cli_value,
)
from ingestion.weather.fetch_weather_daily import (
    build_weather_config_from_env,
    load_weather_daily,
    validate_weather_api_budget,
)
from ingestion.workflows.bootstrap import (
    BootstrapWorkflowServices,
    run_bootstrap_workflow as _run_bootstrap_workflow,
)
from ingestion.workflows.common import run_olist_loaders
from ingestion.workflows.incremental import (
    IncrementalWorkflowServices,
    run_incremental_workflow as _run_incremental_workflow,
)

logger = logging.getLogger(__name__)


def run_bootstrap_workflow(parsed_args: argparse.Namespace) -> IngestionRunSummary:
    """Run the historical bootstrap ingestion workflow."""
    return _run_bootstrap_workflow(
        parsed_args,
        services=BootstrapWorkflowServices(
            resolve_enrichment_date_range=resolve_enrichment_date_range,
            require_enrichment_date_range=require_enrichment_date_range,
            build_weather_config_from_env=build_weather_config_from_env,
            validate_weather_api_budget=validate_weather_api_budget,
            require_cli_value=require_cli_value,
            configure_google_application_credentials=(
                configure_google_application_credentials
            ),
            create_bigquery_client=create_bigquery_client,
            iter_olist_specs=iter_olist_specs,
            run_olist_loaders=run_olist_loaders,
            load_holidays=load_holidays,
            load_weather_daily=load_weather_daily,
        ),
    )


def run_incremental_workflow(parsed_args: argparse.Namespace) -> IngestionRunSummary:
    """Run the incremental batch ingestion workflow."""
    return _run_incremental_workflow(
        parsed_args,
        services=IncrementalWorkflowServices(
            require_cli_value=require_cli_value,
            configure_google_application_credentials=(
                configure_google_application_credentials
            ),
            create_bigquery_client=create_bigquery_client,
            discover_olist_batch_files=discover_olist_batch_files,
            fetch_batch_states=fetch_batch_states,
            upsert_batch_states=upsert_batch_states,
            derive_incremental_order_windows=derive_incremental_order_windows,
            build_batch_metadata=build_batch_metadata,
            load_raw_csv=load_raw_csv,
            get_olist_spec=get_olist_spec,
            build_expected_olist_file_names=build_expected_olist_file_names,
            load_holidays=load_holidays,
            build_weather_config_from_env=build_weather_config_from_env,
            validate_weather_api_budget=validate_weather_api_budget,
            load_weather_daily=load_weather_daily,
        ),
    )


def run_ingestion_workflow(parsed_args: argparse.Namespace) -> IngestionRunSummary:
    """Dispatch the selected ingestion mode and return a structured summary."""
    if parsed_args.mode == "bootstrap":
        return run_bootstrap_workflow(parsed_args)

    if parsed_args.mode == "incremental":
        return run_incremental_workflow(parsed_args)

    msg = f"Unsupported ingestion mode: {parsed_args.mode}"
    raise ValueError(msg)


def build_argument_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser for the unified ingestion workflow."""
    return _build_argument_parser()


def parse_arguments(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the unified ingestion workflow."""
    return _parse_arguments(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the unified ingestion workflow."""
    load_dotenv()
    parsed_args = parse_arguments(argv)
    configure_logging_from_env()

    try:
        ingestion_summary = run_ingestion_workflow(parsed_args)
        logger.info(
            "Unified ingestion completed summary=%s",
            json.dumps(
                ingestion_summary.to_dict(),
                sort_keys=True,
            ),
        )
    except CLI_HANDLED_EXCEPTIONS as exc:
        return log_cli_failure(logger, "Unified ingestion", exc)

    return 0


__all__ = [
    "DEFAULT_INGESTION_MODE",
    "DEFAULT_INGESTION_STATE_TABLE",
    "DEFAULT_OLIST_DATA_DIR",
    "DEFAULT_OLIST_LANDING_DIR",
    "DEFAULT_WEATHER_RUNTIME_LOOKBACK_DAYS",
    "build_argument_parser",
    "parse_arguments",
    "run_bootstrap_workflow",
    "run_incremental_workflow",
    "run_ingestion_workflow",
]


if __name__ == "__main__":
    raise SystemExit(main())
