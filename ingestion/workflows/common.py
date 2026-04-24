"""Shared orchestration helpers for ingestion workflows."""

from __future__ import annotations

import logging
from pathlib import Path

from google.cloud import bigquery

from ingestion.olist.raw_csv_loader import OlistRawTableSpec, load_raw_csv
from ingestion.olist.registry import iter_olist_specs
from ingestion.utils.bigquery_client import BigQueryWriteResult

logger = logging.getLogger(__name__)


def run_olist_loaders(
    olist_data_dir: str | Path,
    *,
    client: bigquery.Client,
    project_id: str,
    location: str,
    specs: tuple[OlistRawTableSpec, ...] | None = None,
) -> list[BigQueryWriteResult]:
    """Run all configured Olist raw table loaders."""
    normalized_data_dir = Path(olist_data_dir)
    write_results: list[BigQueryWriteResult] = []
    configured_specs = specs or iter_olist_specs()

    for spec in configured_specs:
        csv_path = normalized_data_dir / spec.default_file_name
        logger.info(
            "Running Olist raw loader source_name=%s csv_path=%s",
            spec.source_name,
            csv_path,
        )
        write_results.append(
            load_raw_csv(
                csv_path,
                spec,
                client=client,
                project_id=project_id,
                location=location,
            )
        )

    return write_results
