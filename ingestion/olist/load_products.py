"""Load Olist products CSV data into the raw BigQuery table."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

import pandas as pd
from google.cloud import bigquery

from ingestion.olist.raw_csv_loader import (
    DEFAULT_WRITE_MODE,
    OlistRawTableSpec,
    load_raw_csv,
    prepare_raw_dataframe,
    run_olist_loader,
)
from ingestion.utils.batch_metadata import BatchMetadata
from ingestion.utils.bigquery_client import BigQueryWriteResult, WriteMode

RAW_PRODUCTS_TABLE_ID = "raw_olist.products"
REQUIRED_PRODUCTS_COLUMNS = frozenset(
    {
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    }
)
PRODUCTS_SPEC = OlistRawTableSpec(
    source_name="products",
    table_id=RAW_PRODUCTS_TABLE_ID,
    required_columns=REQUIRED_PRODUCTS_COLUMNS,
    default_file_name="olist_products_dataset.csv",
)


def load_products_csv(
    csv_path: str | Path,
    *,
    table_id: str = RAW_PRODUCTS_TABLE_ID,
    write_mode: WriteMode = DEFAULT_WRITE_MODE,
    client: bigquery.Client | None = None,
    project_id: str | None = None,
    location: str | None = None,
) -> BigQueryWriteResult:
    """Load the Olist products CSV into the raw BigQuery table.

    Args:
        csv_path: Local path to the Olist products CSV file.
        table_id: Destination BigQuery table ID.
        write_mode: BigQuery write behavior.
        client: Optional preconfigured BigQuery client.
        project_id: Optional Google Cloud project ID used when creating a client.
        location: Optional BigQuery job location such as "EU" or "US".

    Returns:
        A structured summary of the completed BigQuery load job.
    """
    return load_raw_csv(
        csv_path,
        PRODUCTS_SPEC,
        table_id=table_id,
        write_mode=write_mode,
        client=client,
        project_id=project_id,
        location=location,
    )


def prepare_products_dataframe(
    csv_path: str | Path,
    *,
    metadata: BatchMetadata | None = None,
) -> pd.DataFrame:
    """Prepare the Olist products CSV for raw BigQuery loading.

    Args:
        csv_path: Local path to the Olist products CSV file.
        metadata: Optional batch metadata for deterministic tests.

    Returns:
        Products DataFrame ready to load into the raw BigQuery table.
    """
    return prepare_raw_dataframe(csv_path, PRODUCTS_SPEC, metadata=metadata)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the Olist products loader from the command line.

    Args:
        argv: Optional command-line argument sequence.

    Returns:
        Process exit code.
    """
    return run_olist_loader(argv, PRODUCTS_SPEC)


if __name__ == "__main__":
    raise SystemExit(main())
