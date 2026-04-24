# Data Contracts

This document defines the active warehouse contracts for MerchantPulse:
declared grain, join keys, freshness SLAs, snapshot semantics, conformed facts,
and published marts. It is the primary reference for what each warehouse layer
means and what downstream consumers are allowed to rely on.

Contracts are considered real only when they are implemented consistently in
dbt SQL, schema tests, singular tests, snapshots, and downstream
documentation.

For metric semantics, see `docs/metric_definitions.md`. For architecture-level
context, see `docs/architecture.md`.

## Contract Rules

| Rule | Standard |
|---|---|
| Grain first | Every intermediate model, fact, dimension, mart, and snapshot must declare what one row means |
| Explicit keys | Joins use named keys and documented cardinality |
| Pure dimensions | `dim_*` tables answer what an entity is now, not how it has performed over time |
| History is explicit | Historical master-data tracking lives in snapshots, not hidden inside current-state dimensions |
| Conformed facts | `fact_*` tables preserve event grain plus governed foreign keys and order-time snapshots |
| Governed marts | KPI formulas live in marts, not in dashboards |
| Observable sources | Supported raw sources publish freshness SLAs using `ingested_at_utc` |
| Optional enrichment | Holiday and weather fields may be null, but that missingness must remain measurable |
| Idempotent reruns | Rebuilding from the same upstream state must produce the same warehouse state |
| Explicit attribution | Seller experience metrics must declare the attributable seller-order population |

## Source Freshness SLAs

Freshness is measured from `ingested_at_utc`, the warehouse-arrival timestamp
set by ingestion. Pull-request GitHub Actions CI remains parse-only, while
warehouse-backed freshness, snapshots, and dbt tests can run through the
scheduled runtime workflow documented in the operations runbook.

| Source | Grain | Warn after | Error after | Why it matters |
|---|---|---|---|---|
| `raw_olist.orders` | One row per `order_id` | 24 hours | 48 hours | Core order lifecycle contract for nearly every downstream model |
| `raw_olist.order_items` | One row per `order_id`, `order_item_id` | 24 hours | 48 hours | Keeps commercial item-level logic aligned with order freshness |
| `raw_olist.order_payments` | One row per `order_id`, `payment_sequential` | 24 hours | 48 hours | Financial reconciliation depends on current payment rows |
| `raw_olist.order_reviews` | One row per `review_id`, `order_id` | 48 hours | 96 hours | Review data arrives later than core transactions and supports CX reporting |
| `raw_ext.holidays` | One row per `holiday_date`, `country_code`, `holiday_name` | 30 days | 60 days | Calendar enrichment changes slowly but still has a refresh contract |
| `raw_ext.weather_daily` | One row per `weather_date`, `location_key` | 48 hours | 96 hours | Delivery-weather analysis depends on recent proxy weather coverage |

Master-data backfills (`customers`, `sellers`, `products`, and `geolocation`)
intentionally do not publish freshness SLAs in V1. They are treated as static
reference loads; seller and product drift is governed through snapshots, while
customer and geolocation integrity is validated structurally rather than by
refresh cadence.

## Canonical Identity And Time Semantics

| Topic | Contract |
|---|---|
| Business customer grain | `customer_unique_id` is the canonical business-customer key |
| Source lineage customer key | `customer_id` remains on facts as source lineage only |
| Current-state customer dimension | `dim_customer` publishes one row per `customer_unique_id` with current-state attributes |
| Historical customer geography | Facts carry `customer_*_at_order` snapshot fields so historical orders do not depend on current-state customer attributes |
| Seller and product master data | `fact_order_items` intentionally uses current-state seller/product attributes; history lives in snapshots, not on every line item |
| Purchase-date cohorting | Executive, seller, fulfillment, and customer-experience marts cohort by `purchase_date` |
| Holiday semantics | `dim_date` owns the configured holiday-country contract; `int_order_delivery`, facts, and marts reuse that conformed meaning |
| Delay semantics | `delivery_delay_bucket` is the shared late-delivery bucket contract across facts and marts |
| Seller experience attribution | Seller review metrics use only single-seller orders derived from `fact_order_items` |

## Raw Layer

| Target table | Grain | Required metadata | Notes |
|---|---|---|---|
| `raw_olist.orders` | One source order row | `ingested_at_utc`, `source_file_name`, `batch_id` | Core transactional source |
| `raw_olist.order_items` | One source order-item row | `ingested_at_utc`, `source_file_name`, `batch_id` | Multiple rows per order are expected |
| `raw_olist.order_payments` | One source payment row | `ingested_at_utc`, `source_file_name`, `batch_id` | Aggregated before order-level finance logic |
| `raw_olist.order_reviews` | One source review row | `ingested_at_utc`, `source_file_name`, `batch_id` | Review timestamps support experience analysis |
| `raw_olist.customers` | One source customer row | `ingested_at_utc`, `source_file_name`, `batch_id` | Source for customer identity and geography |
| `raw_olist.sellers` | One source seller row | `ingested_at_utc`, `source_file_name`, `batch_id` | Source for seller master data |
| `raw_olist.products` | One source product row | `ingested_at_utc`, `source_file_name`, `batch_id` | Source for product catalog attributes |
| `raw_olist.geolocation` | One source postal-code geolocation observation row | `ingested_at_utc`, `source_file_name`, `batch_id` | Reference geospatial observations retained for structural completeness and future location-aware modeling |
| `raw_ext.holidays` | One holiday-date-country-name row | `ingested_at_utc`, `source_file_name`, `batch_id` | Purchase-date seasonality context |
| `raw_ext.weather_daily` | One weather-date-location row | `ingested_at_utc`, `source_file_name`, `batch_id` | Proxy delivery-weather context |

## Staging Layer

| Model | Grain | Expected key | Main checks |
|---|---|---|---|
| `stg_orders` | One order | `order_id` | not null key, valid order status, timestamp casts |
| `stg_order_items` | One order item | `order_id`, `order_item_id` | not null grain key, non-negative price and freight |
| `stg_payments` | One payment event | `order_id`, `payment_sequence` | non-negative payment value, accepted payment type |
| `stg_reviews` | One review-order relationship | `review_id`, `order_id` | review score between 1 and 5 |
| `stg_customers` | One source customer row | `customer_id` | standardized geography fields and `customer_unique_id` |
| `stg_sellers` | One seller row | `seller_id` | standardized geography fields |
| `stg_products` | One product row | `product_id` | standardized catalog attributes |
| `stg_geolocation` | One postal code, city, state, and rounded coordinate observation | `geolocation_observation_key` | normalized geography text, valid latitude/longitude, deterministic deduplication |
| `stg_holidays` | One holiday-date-country-name row | `holiday_date`, `country_code`, `holiday_name` | valid date and country code |
| `stg_weather_daily` | One date-location row | `weather_date`, `location_key` | valid date and numeric weather attributes |

`stg_geolocation` is currently maintained as structured reference data for
future location-aware modeling. Downstream marts do not consume it in V1.

## Intermediate Layer

| Model | Grain | Purpose | Required checks |
|---|---|---|---|
| `int_order_value` | One order | Aggregate item value, freight, payment value, and payment count while preserving all order IDs | one row per order, values non-negative, zero item count allowed, published aggregates reconcile back to independently rolled-up staging item and payment rows |
| `int_order_delivery` | One order | Calculate delivery flags, cancellation flags, late days, purchase-date holiday context, and delivery-date proxy weather context | late orders have positive late days; non-late orders keep `late_days = NULL`; `is_delivered = TRUE` only when `order_status = 'delivered'` and an actual delivery timestamp exists |
| `int_customer_order_sequence` | One customer-order relationship | Sequence orders by `customer_unique_id` so repeat-customer analysis works on business identity | order number starts at 1 and is unique per physical customer |
| `int_review_enriched` | One review-order relationship | Connect review score with delivery context, product context, and `delivery_delay_bucket` | review score valid, delivery bucket purity enforced |
| `int_order_review_metrics` | One order | Publish the canonical order-level review aggregation reused by seller and customer-experience models | one row per order, review counts non-negative, score range stays valid |
| `int_seller_daily_performance` | One seller-date | Aggregate seller orders, revenue, item volume, and operational defect signals at reusable seller-day grain | one row per seller-date, seller metrics reconcile, operational defect union is deduped |
| `int_seller_attributable_experience` | One seller-order relationship | Publish seller-attributable experience metrics on the single-seller order subset | only single-seller orders are included; order-level review metrics reconcile |

## Snapshot Contracts

Snapshots provide SCD2-style history tracking for cleaned master data without
changing the semantics of current-state dimensions.

| Snapshot | Grain | Source | Strategy | Tracked changes | Notes |
|---|---|---|---|---|---|
| `snap_sellers` | One row per seller version | `stg_sellers` | `check` | `seller_zip_code_prefix`, `seller_city`, `seller_state` | Tracks seller master-data history without using batch metadata as change signals |
| `snap_products` | One row per product version | `stg_products` | `check` | `product_category_name`, `product_name_length`, `product_description_length`, `product_photos_count`, `product_weight_g`, `product_length_cm`, `product_height_cm`, `product_width_cm` | Tracks catalog history on semantic attributes only |

Snapshot metadata semantics:

| Field | Meaning |
|---|---|
| `dbt_valid_from` | Timestamp when the snapshot version became active |
| `dbt_valid_to` | Timestamp when the snapshot version stopped being current; null means current version |
| `dbt_scd_id` | dbt-generated identifier for the versioned record |
| `dbt_updated_at` | Timestamp dbt assigned when the version was written |

Current-state boundary:

| Artifact | Purpose |
|---|---|
| `dim_seller`, `dim_product` | Current-state reusable dimensions for downstream joins |
| `snap_sellers`, `snap_products` | Historical version tracking for master-data change analysis and auditability |

## Dimensions

| Model | Grain | Purpose | Core fields |
|---|---|---|---|
| `dim_date` | One calendar date | Conformed reporting calendar and holiday contract | date, week, month, quarter, holiday attributes |
| `dim_customer` | One `customer_unique_id` | Current-state business-customer master dimension | `customer_unique_id`, current geography |
| `dim_seller` | One seller | Seller master dimension | seller geography only |
| `dim_product` | One product | Product catalog dimension | category and physical product attributes |

## Facts

| Model | Grain | Primary consumers | Core fields |
|---|---|---|---|
| `fact_orders` | One order | Executive, seller, fulfillment, and customer-experience marts | customer identity, order-time customer geography, order status, purchase/delivery dates, financials, SLA flags, holiday/weather context |
| `fact_order_items` | One order item | Seller performance and seller attribution analysis | seller, product, current-state product category, item price, freight, inherited order/customer context |
| `fact_reviews` | One review-order relationship | Customer experience analysis | customer identity, order-time customer geography, review score, review timestamps, `delivery_delay_bucket` |

## Aggregate Marts

| Model | Grain | Primary consumers | Core fields |
|---|---|---|---|
| `mart_exec_daily` | One calendar date | Executives | orders, new customers, GMV, items value, freight, payments, AOV, cancellation rate, late delivery rate, review score |
| `mart_seller_performance` | One seller-date | Marketplace operations | seller orders, GMV, AOV, cancellation, late delivery, operational defect |
| `mart_seller_experience` | One seller-date | Marketplace operations and CX analysts | attributable orders, review coverage, commented reviews, avg review score, avg time to review, low review rate |
| `mart_fulfillment_ops` | One purchase-date, customer-state, delivery-delay-bucket row | Fulfillment operations | order population, delivery outcomes, holiday context, proxy weather, late-day severity |
| `mart_customer_experience` | One purchase-date, customer-state, delivery-delay-bucket row | Customer experience and VOC analysis | reviewed orders, reviews, commented reviews, avg review score, avg time to review |

## Join Safety

| Join | Expected relationship | Risk to control |
|---|---|---|
| orders to order items | one-to-many | accidental row multiplication in order-level marts |
| orders to payments | one-to-many | double counting order finance if payments are not aggregated first |
| orders to reviews | one-to-many at review grain | review weighting errors if marts skip order-level aggregation |
| orders to customers | many source orders to one source customer row | identity drift if facts do not freeze order-time customer geography |
| facts to dim_customer | many facts to one `customer_unique_id` | broken business-customer lineage |
| holidays to dates | many business rows to one date | cross-layer holiday semantics drift |
| weather to delivery dates | many business rows to one date-location | proxy context must not be described as customer-level geospatial truth |
| seller experience to order items | single-seller subset only | copying one order review across multiple sellers |

## Test Design

| Category | Example |
|---|---|
| Generic schema tests | `not_null`, `unique`, `relationships`, `accepted_values`, and `accepted_range` on published contracts |
| Domain constraint authority | `review_score` range and non-negative `payment_value` remain in generic tests as the single authoritative column-level contract |
| Singular business invariants | `assert_delivered_orders_have_delivery_timestamp.sql` and `assert_order_payments_reconcile_within_tolerance.sql` enforce cross-column rules that generic tests cannot express cleanly |
| Framework-backed reconciliation | Relation-level parity tests use `reconciliation_mismatch_rows`; specialized invariants and weighting checks reuse scalar helper macros such as `required_rate_mismatch` and `nullable_rate_mismatch` so tolerance semantics are defined once |
| Intentionally specialized business proofs | Delivery-bucket purity, attributable single-seller subsets, order-time customer identity alignment, and order-weighting tests remain hand-authored because they prove domain semantics and guard against wrong-but-plausible modeling alternatives |
| Semantic invariant | `assert_int_review_enriched_delivery_delay_bucket_purity.sql` acts as the shared macro-contract test for `delivery_delay_bucket` across downstream marts |
| Identity contract | `fact_orders` customer identity and order-time customer snapshot align with source mapping |
| Cross-layer consistency | `fact_orders.is_purchase_on_holiday` and `mart_fulfillment_ops.is_purchase_on_holiday` align with `dim_date` |
| History contract | Snapshots track only semantic master-data columns so reruns do not create false versions from batch metadata churn |
| Attribution contract | `int_seller_attributable_experience` contains only single-seller orders via a direct singular test |
| Published shape | BI-facing marts enforce column names and data types through dbt model contracts |
| Shared-grain parity | `mart_fulfillment_ops` and `mart_customer_experience` expose the same order population on shared cohort keys |
