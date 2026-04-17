# Data Contracts

This document defines the planned table grains, keys, and quality expectations
for MerchantPulse. It is a design contract until the dbt models are implemented.
As models are built, each contract should be copied into model descriptions and
schema tests.

## Contract Rules

| Rule | Standard |
|---|---|
| Grain first | Every fact, intermediate model, and mart must state what one row means |
| Explicit keys | Joins must use named keys, never accidental column matching |
| Core identifiers | `order_id`, `customer_id`, `seller_id`, and reporting dates must not silently disappear |
| Optional enrichment | Holiday and weather fields may be null, but the null rate should be measurable |
| Metadata | Raw tables include `ingested_at_utc`, `source_file_name`, and `batch_id` |
| Reruns | Ingestion and dbt builds should be idempotent |

## Raw Layer

Raw tables preserve source fidelity. They should avoid business logic and keep
the original source columns where practical.

| Target table | Grain | Required metadata | Notes |
|---|---|---|---|
| `raw_olist.orders` | One source order row | `ingested_at_utc`, `source_file_name`, `batch_id` | Core transactional source |
| `raw_olist.order_items` | One source order-item row | `ingested_at_utc`, `source_file_name`, `batch_id` | Multiple rows can exist per order |
| `raw_olist.order_payments` | One source payment row | `ingested_at_utc`, `source_file_name`, `batch_id` | Multiple rows can exist per order |
| `raw_olist.order_reviews` | One source review row | `ingested_at_utc`, `source_file_name`, `batch_id` | Review timestamps support customer experience analysis |
| `raw_olist.customers` | One source customer row | `ingested_at_utc`, `source_file_name`, `batch_id` | Used for customer and geography dimensions |
| `raw_olist.sellers` | One source seller row | `ingested_at_utc`, `source_file_name`, `batch_id` | Used for seller operations analysis |
| `raw_olist.products` | One source product row | `ingested_at_utc`, `source_file_name`, `batch_id` | Used for category analysis |
| `raw_olist.geolocation` | One source geolocation row | `ingested_at_utc`, `source_file_name`, `batch_id` | May require deduplication downstream |
| `raw_ext.holidays` | One holiday-date-country row | `ingested_at_utc`, `source_file_name`, `batch_id` | Annual refresh target |
| `raw_ext.weather_daily` | One weather-date-location row | `ingested_at_utc`, `source_file_name`, `batch_id` | Daily or backfill batch target |

## Staging Layer

Staging models standardize source shape. They should be source-shaped and should
not contain complex cross-source business logic.

| Model | Grain | Expected key | Main checks |
|---|---|---|---|
| `stg_orders` | One order | `order_id` | not null key, valid order status, timestamp casts |
| `stg_order_items` | One order item | `order_id`, `order_item_id` | not null item key, non-negative price and freight |
| `stg_payments` | One payment event | `order_id`, `payment_sequential` | non-negative payment value, accepted payment type |
| `stg_reviews` | One review | `review_id` | review score between 1 and 5 |
| `stg_customers` | One customer | `customer_id` | not null key, standardized location fields |
| `stg_sellers` | One seller | `seller_id` | not null key, standardized location fields |
| `stg_products` | One product | `product_id` | not null key, standardized category fields |
| `stg_geolocation` | One cleaned geolocation record | location fields | deduplication rule documented |
| `stg_holidays` | One holiday-date-country row | `holiday_date`, `country_code` | valid date and country code |
| `stg_weather_daily` | One date-location row | `weather_date`, `location_key` | valid date, numeric weather fields |

## Intermediate Layer

Intermediate models centralize reusable business logic so marts and dashboards do
not duplicate rules.

| Model | Grain | Purpose | Required checks |
|---|---|---|---|
| `int_order_value` | One order | Aggregate item value, freight, payment value, and payment count | one row per order, values non-negative |
| `int_order_delivery` | One order | Calculate delivery status, late flag, late days, and cancellation flag | delivered orders have delivered timestamp |
| `int_customer_order_sequence` | One customer-order relationship | Sequence orders per customer | order number starts at 1 per customer |
| `int_review_enriched` | One review or order-review relationship | Connect review score with delivery and product context | review score valid, delay bucket documented |
| `int_seller_daily_performance` | One seller-date | Aggregate seller order, revenue, cancellation, and delay signals | one row per seller-date |

## Marts Layer

Marts are the only tables dashboards should query directly. Each mart owns a
stable grain and a clear business use case.

| Model | Grain | Primary consumers | Core fields |
|---|---|---|---|
| `dim_date` | One calendar date | All dashboards | date, week, month, quarter, holiday flag |
| `dim_customer` | One customer | Growth and customer analysis | customer id, location fields |
| `dim_seller` | One seller | Seller operations | seller id, location fields |
| `dim_product` | One product | Product and category analysis | product id, category fields |
| `fact_orders` | One order | Executive and operations marts | order status, dates, values, delivery flags |
| `fact_order_items` | One order item | Seller and product analysis | seller id, product id, item value, freight |
| `fact_payments` | One payment event | Finance and payment quality | payment type, installments, payment value |
| `fact_reviews` | One review | Customer experience analysis | review score, review dates, delay bucket |
| `mart_exec_daily` | One calendar date | Executives | GMV, orders, AOV, cancellation rate, late delivery rate, review score |
| `mart_seller_performance` | One seller-date | Operations | seller GMV, orders, defect rate, late delivery rate, review score |
| `mart_fulfillment_ops` | One date-region or date-status grain | Operations | delay, weather, holiday, cancellation, review impact |

## Join Safety

| Join | Expected relationship | Risk to control |
|---|---|---|
| orders to order items | one-to-many | accidental row multiplication in order-level marts |
| orders to payments | one-to-many | double counting GMV if payment rows are not aggregated first |
| orders to reviews | usually one-to-one but validate | review duplication or missing review records |
| sellers to order items | one-to-many | seller-level aggregation must use item grain carefully |
| holidays to dates | many orders to one date | enrichment can be nullable |
| weather to dates and location | many orders to one date-location | avoid pretending weather is order-level precision |

## Freshness Targets

| Source | Target freshness |
|---|---|
| Orders | 24 hours |
| Payments | 24 hours |
| Reviews | 48 hours |
| Holidays | 30 days |
| Weather | 48 hours |

## Test Design

No production functions are changed by this documentation update. When the
models are implemented, tests should cover:

| Category | Example |
|---|---|
| Happy path | A normal order with item, payment, and delivery timestamps appears once in `fact_orders` |
| Boundary | Orders without optional review, holiday, or weather context still appear in core marts |
| Invalid input | Missing `order_id` or negative payment value fails validation |
| Regression | A one-to-many payment join cannot duplicate order-level GMV |
