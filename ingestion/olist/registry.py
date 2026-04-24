"""Data-driven registry for Olist raw source contracts."""

from __future__ import annotations

from ingestion.olist.raw_csv_loader import OlistRawTableSpec
from ingestion.utils.table_targets import BigQueryDatasetRole

REQUIRED_ORDERS_COLUMNS = frozenset(
    {
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    }
)
ORDERS_SPEC = OlistRawTableSpec(
    source_name="orders",
    table_name="orders",
    dataset_role=BigQueryDatasetRole.RAW_OLIST,
    required_columns=REQUIRED_ORDERS_COLUMNS,
    default_file_name="olist_orders_dataset.csv",
)

REQUIRED_ORDER_ITEMS_COLUMNS = frozenset(
    {
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
    }
)
ORDER_ITEMS_SPEC = OlistRawTableSpec(
    source_name="order_items",
    table_name="order_items",
    dataset_role=BigQueryDatasetRole.RAW_OLIST,
    required_columns=REQUIRED_ORDER_ITEMS_COLUMNS,
    default_file_name="olist_order_items_dataset.csv",
)

REQUIRED_ORDER_PAYMENTS_COLUMNS = frozenset(
    {
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
    }
)
ORDER_PAYMENTS_SPEC = OlistRawTableSpec(
    source_name="order_payments",
    table_name="order_payments",
    dataset_role=BigQueryDatasetRole.RAW_OLIST,
    required_columns=REQUIRED_ORDER_PAYMENTS_COLUMNS,
    default_file_name="olist_order_payments_dataset.csv",
)

REQUIRED_ORDER_REVIEWS_COLUMNS = frozenset(
    {
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp",
    }
)
ORDER_REVIEWS_SPEC = OlistRawTableSpec(
    source_name="order_reviews",
    table_name="order_reviews",
    dataset_role=BigQueryDatasetRole.RAW_OLIST,
    required_columns=REQUIRED_ORDER_REVIEWS_COLUMNS,
    default_file_name="olist_order_reviews_dataset.csv",
)

REQUIRED_CUSTOMERS_COLUMNS = frozenset(
    {
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    }
)
CUSTOMERS_SPEC = OlistRawTableSpec(
    source_name="customers",
    table_name="customers",
    dataset_role=BigQueryDatasetRole.RAW_OLIST,
    required_columns=REQUIRED_CUSTOMERS_COLUMNS,
    default_file_name="olist_customers_dataset.csv",
)

REQUIRED_SELLERS_COLUMNS = frozenset(
    {
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
    }
)
SELLERS_SPEC = OlistRawTableSpec(
    source_name="sellers",
    table_name="sellers",
    dataset_role=BigQueryDatasetRole.RAW_OLIST,
    required_columns=REQUIRED_SELLERS_COLUMNS,
    default_file_name="olist_sellers_dataset.csv",
)

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
    table_name="products",
    dataset_role=BigQueryDatasetRole.RAW_OLIST,
    required_columns=REQUIRED_PRODUCTS_COLUMNS,
    default_file_name="olist_products_dataset.csv",
)

REQUIRED_GEOLOCATION_COLUMNS = frozenset(
    {
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state",
    }
)
GEOLOCATION_SPEC = OlistRawTableSpec(
    source_name="geolocation",
    table_name="geolocation",
    dataset_role=BigQueryDatasetRole.RAW_OLIST,
    required_columns=REQUIRED_GEOLOCATION_COLUMNS,
    default_file_name="olist_geolocation_dataset.csv",
)

OLIST_LOAD_SEQUENCE = (
    ORDERS_SPEC,
    ORDER_ITEMS_SPEC,
    ORDER_PAYMENTS_SPEC,
    ORDER_REVIEWS_SPEC,
    CUSTOMERS_SPEC,
    SELLERS_SPEC,
    PRODUCTS_SPEC,
    GEOLOCATION_SPEC,
)

OLIST_TABLE_REGISTRY = {spec.source_name: spec for spec in OLIST_LOAD_SEQUENCE}


def get_olist_spec(source_name: str) -> OlistRawTableSpec:
    """Return one registered raw-table contract by source name."""
    try:
        return OLIST_TABLE_REGISTRY[source_name]
    except KeyError as exc:
        msg = f"Unsupported Olist source: {source_name}"
        raise ValueError(msg) from exc


def iter_olist_specs() -> tuple[OlistRawTableSpec, ...]:
    """Return registered Olist specs in the intended raw load order."""
    return OLIST_LOAD_SEQUENCE


def build_expected_olist_file_names() -> dict[str, str]:
    """Return source-name to file-name mappings for landing discovery."""
    return {spec.source_name: spec.default_file_name for spec in OLIST_LOAD_SEQUENCE}
