from ingestion.olist.registry import (
    ORDERS_SPEC,
    build_expected_olist_file_names,
    get_olist_spec,
    iter_olist_specs,
)


def test_iter_olist_specs_returns_expected_load_order() -> None:
    assert [spec.source_name for spec in iter_olist_specs()] == [
        "orders",
        "order_items",
        "order_payments",
        "order_reviews",
        "customers",
        "sellers",
        "products",
        "geolocation",
    ]


def test_get_olist_spec_returns_registered_contract() -> None:
    orders_spec = get_olist_spec("orders")

    assert orders_spec == ORDERS_SPEC
    assert orders_spec.table_name == "orders"
    assert "order_purchase_timestamp" in orders_spec.required_columns


def test_build_expected_olist_file_names_matches_registry_defaults() -> None:
    assert build_expected_olist_file_names()["orders"] == "olist_orders_dataset.csv"
    assert (
        build_expected_olist_file_names()["customers"] == "olist_customers_dataset.csv"
    )
