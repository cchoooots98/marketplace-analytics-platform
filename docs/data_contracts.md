# Data Contracts

This document defines the active warehouse contracts for MerchantPulse.
Contracts are considered real only when they are implemented consistently in
dbt SQL, schema tests, singular tests, and downstream documentation.

## Contract Rules

| Rule | Standard |
|---|---|
| Grain first | Every intermediate model, fact, dimension, and mart must declare what one row means |
| Explicit keys | Joins use named keys and documented cardinality |
| Pure dimensions | `dim_*` tables answer what an entity is, not how it has performed over time |
| Conformed facts | `fact_*` tables preserve event grain plus governed foreign keys and order-time snapshots |
| Conformed dimensions | `dim_*` contracts may be reused outside dashboard marts when they centralize shared business meaning |
| Governed marts | KPI formulas live in marts, not in dashboards |
| Optional enrichment | Holiday and weather fields may be null, but that missingness must remain measurable |
| Idempotent reruns | Rebuilding from the same upstream state must produce the same warehouse state |
| Explicit attribution | Seller experience metrics must declare the attributable seller-order population |

## Canonical Identity And Time Semantics

| Topic | Contract |
|---|---|
| Business customer grain | `customer_unique_id` is the canonical business-customer key |
| Source lineage customer key | `customer_id` remains on facts as source lineage only |
| Current-state customer dimension | `dim_customer` publishes one row per `customer_unique_id` with current-state attributes |
| Historical customer geography | Facts carry `customer_*_at_order` snapshot fields so historical orders do not depend on current-state customer attributes |
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
| `stg_holidays` | One holiday-date-country-name row | `holiday_date`, `country_code`, `holiday_name` | valid date and country code |
| `stg_weather_daily` | One date-location row | `weather_date`, `location_key` | valid date and numeric weather attributes |

## Intermediate Layer

| Model | Grain | Purpose | Required checks |
|---|---|---|---|
| `int_order_value` | One order | Aggregate item value, freight, payment value, and payment count while preserving all order IDs | one row per order, values non-negative, zero item count allowed |
| `int_order_delivery` | One order | Calculate delivery flags, cancellation flags, late days, purchase-date holiday context, and delivery-date proxy weather context | late orders have positive late days; non-late orders keep `late_days = NULL`; holiday flag is non-null |
| `int_customer_order_sequence` | One customer-order relationship | Sequence orders by `customer_unique_id` so repeat-customer analysis works on business identity | order number starts at 1 and is unique per physical customer |
| `int_review_enriched` | One review-order relationship | Connect review score with delivery context, product context, and `delivery_delay_bucket` | review score valid, delivery bucket purity enforced |
| `int_order_review_metrics` | One order | Publish the canonical order-level review aggregation reused by seller and customer-experience models | one row per order, review counts non-negative, score range stays valid |
| `int_seller_daily_performance` | One seller-date | Aggregate seller orders, revenue, item volume, and operational defect signals at reusable seller-day grain | one row per seller-date, seller metrics reconcile, operational defect union is deduped |
| `int_seller_attributable_experience` | One seller-order relationship | Publish seller-attributable experience metrics on the single-seller order subset | only single-seller orders are included; order-level review metrics reconcile |

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
| `fact_order_items` | One order item | Seller performance and seller attribution analysis | seller, product, item price, freight, inherited order/customer context |
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
| Business reconciliation | Marts reconcile to facts or reusable intermediates at their published grain without re-implementing full mart SQL |
| Semantic invariant | `assert_int_review_enriched_delivery_delay_bucket_purity.sql` acts as the shared macro-contract test for `delivery_delay_bucket` across downstream marts |
| Identity contract | `fact_orders` customer identity and order-time customer snapshot align with source mapping |
| Cross-layer consistency | `fact_orders.is_purchase_on_holiday` and `mart_fulfillment_ops.is_purchase_on_holiday` align with `dim_date` |
| Attribution contract | `int_seller_attributable_experience` contains only single-seller orders via a direct singular test |
| Published shape | BI-facing marts enforce column names and data types through dbt model contracts |
| Shared-grain parity | `mart_fulfillment_ops` and `mart_customer_experience` expose the same order population on shared cohort keys |
