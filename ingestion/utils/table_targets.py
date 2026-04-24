"""BigQuery dataset-role helpers for ingestion targets."""

from __future__ import annotations

import os
from enum import StrEnum

from ingestion.utils.validation import require_text


class BigQueryDatasetRole(StrEnum):
    """Logical dataset roles used by ingestion contracts."""

    RAW_OLIST = "raw_olist"
    RAW_EXT = "raw_ext"


DATASET_ROLE_ENV_VARS: dict[BigQueryDatasetRole, str] = {
    BigQueryDatasetRole.RAW_OLIST: "BQ_RAW_OLIST_DATASET",
    BigQueryDatasetRole.RAW_EXT: "BQ_RAW_EXT_DATASET",
}


DATASET_ROLE_DEFAULTS: dict[BigQueryDatasetRole, str] = {
    BigQueryDatasetRole.RAW_OLIST: "raw_olist",
    BigQueryDatasetRole.RAW_EXT: "raw_ext",
}


def resolve_dataset_name(dataset_role: BigQueryDatasetRole) -> str:
    """Resolve one logical dataset role to a concrete dataset name.

    Args:
        dataset_role: Logical ingestion dataset role.

    Returns:
        Dataset name configured for the role.
    """
    environment_variable = DATASET_ROLE_ENV_VARS[dataset_role]
    default_dataset = DATASET_ROLE_DEFAULTS[dataset_role]
    return require_text(
        os.getenv(environment_variable, default_dataset),
        environment_variable,
    )


def resolve_table_id(
    table_name: str,
    dataset_role: BigQueryDatasetRole,
) -> str:
    """Resolve a logical target table into ``dataset.table`` form.

    Args:
        table_name: Logical table name inside the dataset role.
        dataset_role: Logical dataset role for the target table.

    Returns:
        Fully resolved ``dataset.table`` string.
    """
    dataset_name = resolve_dataset_name(dataset_role)
    return f"{dataset_name}.{require_text(table_name, 'table_name')}"
