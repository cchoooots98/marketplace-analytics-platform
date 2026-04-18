from types import ModuleType

import pandas as pd
import pytest

from ingestion.olist import (
    load_customers,
    load_geolocation,
    load_order_items,
    load_order_payments,
    load_order_reviews,
    load_orders,
    load_products,
    load_sellers,
)
from ingestion.olist.raw_csv_loader import OlistRawTableSpec
from ingestion.utils.bigquery_client import BigQueryWriteResult

LOADER_MODULES = (
    (load_orders, "ORDERS_SPEC", "load_orders_csv", "prepare_orders_dataframe"),
    (
        load_order_items,
        "ORDER_ITEMS_SPEC",
        "load_order_items_csv",
        "prepare_order_items_dataframe",
    ),
    (
        load_order_payments,
        "ORDER_PAYMENTS_SPEC",
        "load_order_payments_csv",
        "prepare_order_payments_dataframe",
    ),
    (
        load_order_reviews,
        "ORDER_REVIEWS_SPEC",
        "load_order_reviews_csv",
        "prepare_order_reviews_dataframe",
    ),
    (
        load_customers,
        "CUSTOMERS_SPEC",
        "load_customers_csv",
        "prepare_customers_dataframe",
    ),
    (load_sellers, "SELLERS_SPEC", "load_sellers_csv", "prepare_sellers_dataframe"),
    (load_products, "PRODUCTS_SPEC", "load_products_csv", "prepare_products_dataframe"),
    (
        load_geolocation,
        "GEOLOCATION_SPEC",
        "load_geolocation_csv",
        "prepare_geolocation_dataframe",
    ),
)

EXPECTED_TABLE_IDS = {
    "orders": "raw_olist.orders",
    "order_items": "raw_olist.order_items",
    "order_payments": "raw_olist.order_payments",
    "order_reviews": "raw_olist.order_reviews",
    "customers": "raw_olist.customers",
    "sellers": "raw_olist.sellers",
    "products": "raw_olist.products",
    "geolocation": "raw_olist.geolocation",
}


@pytest.mark.parametrize(
    ("module", "spec_name", "load_function_name", "prepare_function_name"),
    LOADER_MODULES,
)
def test_loader_specs_target_expected_raw_tables(
    module: ModuleType,
    spec_name: str,
    load_function_name: str,
    prepare_function_name: str,
) -> None:
    """Validate each Olist loader declares the expected raw BigQuery table.

    Args:
        module: Loader module under test.
        spec_name: Name of the table spec in the loader module.
        load_function_name: Name of the table-specific load wrapper.
        prepare_function_name: Name of the table-specific prepare wrapper.

    Returns:
        None.
    """
    spec = getattr(module, spec_name)

    assert isinstance(spec, OlistRawTableSpec)
    assert spec.table_id == EXPECTED_TABLE_IDS[spec.source_name]
    assert hasattr(module, load_function_name)
    assert hasattr(module, prepare_function_name)


@pytest.mark.parametrize(
    ("module", "spec_name", "load_function_name", "prepare_function_name"),
    LOADER_MODULES,
)
def test_loader_required_columns_match_local_csv_headers(
    module: ModuleType,
    spec_name: str,
    load_function_name: str,
    prepare_function_name: str,
) -> None:
    """Validate each source spec requires columns present in the local CSV.

    Args:
        module: Loader module under test.
        spec_name: Name of the table spec in the loader module.
        load_function_name: Name of the table-specific load wrapper.
        prepare_function_name: Name of the table-specific prepare wrapper.

    Returns:
        None.
    """
    spec = getattr(module, spec_name)
    csv_path = f"data/olist/{spec.default_file_name}"

    try:
        csv_columns = set(pd.read_csv(csv_path, nrows=0).columns)
    except FileNotFoundError:
        pytest.skip(f"Local Olist CSV data is not available: {csv_path}")

    assert spec.required_columns.issubset(csv_columns)


@pytest.mark.parametrize(
    ("module", "spec_name", "load_function_name", "prepare_function_name"),
    LOADER_MODULES,
)
def test_thin_loader_delegates_load_to_shared_core(
    monkeypatch: pytest.MonkeyPatch,
    module: ModuleType,
    spec_name: str,
    load_function_name: str,
    prepare_function_name: str,
) -> None:
    """Validate each table-specific loader calls the shared raw loader.

    Args:
        monkeypatch: Pytest fixture for replacing shared loader logic.
        module: Loader module under test.
        spec_name: Name of the table spec in the loader module.
        load_function_name: Name of the table-specific load wrapper.
        prepare_function_name: Name of the table-specific prepare wrapper.

    Returns:
        None.
    """
    spec = getattr(module, spec_name)
    captured_call: dict[str, object] = {}

    def fake_load_raw_csv(
        csv_path: str,
        raw_spec: OlistRawTableSpec,
        *,
        table_id: str,
        write_mode: object,
        client: object | None,
        project_id: str | None,
        location: str | None,
    ) -> BigQueryWriteResult:
        captured_call["csv_path"] = csv_path
        captured_call["spec"] = raw_spec
        captured_call["table_id"] = table_id
        captured_call["write_mode"] = write_mode
        captured_call["client"] = client
        captured_call["project_id"] = project_id
        captured_call["location"] = location
        return BigQueryWriteResult(
            table_id=table_id,
            write_mode="replace",
            job_id="raw_job",
            input_rows=1,
            input_columns=1,
            loaded_rows=1,
        )

    monkeypatch.setattr(module, "load_raw_csv", fake_load_raw_csv)

    load_function = getattr(module, load_function_name)
    write_result = load_function(
        "source.csv",
        project_id="marketplace-prod",
        location="EU",
    )

    assert write_result.job_id == "raw_job"
    assert captured_call == {
        "csv_path": "source.csv",
        "spec": spec,
        "table_id": spec.table_id,
        "write_mode": "replace",
        "client": None,
        "project_id": "marketplace-prod",
        "location": "EU",
    }
