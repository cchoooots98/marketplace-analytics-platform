# Metric Definitions

This document defines the canonical KPI rules for MerchantPulse. It is the
authoritative semantic reference for published metrics, denominators, grain,
and weighting behavior across executive, seller, fulfillment, and
customer-experience reporting.

Dashboards must read these metrics from governed marts instead of rejoining
facts in the business-intelligence layer. When a dashboard rolls metrics across
multiple mart rows, it must use the mart-published additive support columns
instead of averaging precomputed rates or averages.

For dashboard usage, see `docs/dashboard_specs.md`. For warehouse grain and
mart ownership, see `docs/data_contracts.md`.

## Metric Rules

| Rule | Standard |
|---|---|
| One definition | Each metric has one canonical warehouse definition |
| Declared denominator | Rates must name numerator and denominator explicitly |
| Declared grain | Every metric names its reporting grain |
| Cancellation explicit | Revenue and order metrics must state how cancelled orders are handled |
| Weighting explicit | Review metrics must state whether they are review-row or order weighted |
| Attribution explicit | Seller experience metrics must state which seller-order population is attributable |

## Executive Metrics

| Metric | Formula | Grain | Canonical source | Notes |
|---|---|---|---|---|
| `orders_count` | Count of all placed orders | `calendar_date` | `mart_exec_daily` | Includes cancelled orders so demand and cancellation stay visible |
| `non_cancelled_orders_count` | Count of orders where `is_cancelled = false` | `calendar_date` | `mart_exec_daily` | Commercial order count used as the AOV denominator |
| `cancelled_orders_count` | Count of orders where `is_cancelled = true` | `calendar_date` | `mart_exec_daily` | Supports cancellation monitoring |
| `delivered_orders_count` | Count of orders where `is_delivered = true` | `calendar_date` | `mart_exec_daily` | Denominator support for late delivery rate |
| `late_orders_count` | Count of orders where `is_late = true` | `calendar_date` | `mart_exec_daily` | Numerator for late delivery rate |
| `new_customers_count` | Count of orders where `is_first_order = true` | `calendar_date` | `mart_exec_daily` | Uses business-customer identity based on `customer_unique_id` |
| `gmv` | Sum of `order_item_value + order_freight_total` for non-cancelled orders | `calendar_date` | `mart_exec_daily` | Canonical net commercial GMV |
| `items_value` | Sum of `order_item_value` across all orders | `calendar_date` | `mart_exec_daily` | Includes cancelled orders as a commercial drill-down |
| `freight_total` | Sum of `order_freight_total` across all orders | `calendar_date` | `mart_exec_daily` | Includes cancelled orders as a logistics drill-down |
| `payment_total` | Sum of `order_payment_total` across all orders | `calendar_date` | `mart_exec_daily` | Nullable payments are treated as zero in aggregation |
| `aov` | `gmv / non_cancelled_orders_count` | `calendar_date` | `mart_exec_daily` | Nullable when a date has no non-cancelled orders |
| `cancellation_rate` | `cancelled_orders_count / orders_count` | `calendar_date` | `mart_exec_daily` | Captures attrition without polluting GMV |
| `late_delivery_rate` | `late_orders_count / delivered_orders_count` | `calendar_date` | `mart_exec_daily` | Nullable when there are no deliveries |
| `reviews_count` | Count of review rows for orders in the purchase-date cohort | `calendar_date` | `mart_exec_daily` | Review-row count, not reviewed-order count |
| `review_score_sum` | Sum of `review_score` across review rows in the purchase-date cohort | `calendar_date` | `mart_exec_daily` | Additive sentiment numerator for cross-period rollups |
| `avg_review_score` | `review_score_sum / reviews_count` | `calendar_date` | `mart_exec_daily` | Convenience row-grain average; cross-period rollups should use `review_score_sum` plus `reviews_count` |

`mart_exec_daily` intentionally emits only dates with at least one order. Zero-
activity dates belong in `dim_date`, not in the executive KPI series.

## Seller Performance Metrics

| Metric | Formula | Grain | Canonical source | Notes |
|---|---|---|---|---|
| `orders_count` | Count of distinct seller orders | `seller_id, calendar_date` | `mart_seller_performance` | Includes cancelled seller orders |
| `non_cancelled_orders_count` | Count of seller orders where `is_cancelled = false` | `seller_id, calendar_date` | `mart_seller_performance` | AOV denominator |
| `items_count` | Count of seller line items | `seller_id, calendar_date` | `mart_seller_performance` | Item-grain operational volume |
| `items_value` | Sum of seller item prices across all seller orders | `seller_id, calendar_date` | `mart_seller_performance` | Includes cancelled orders |
| `freight_total` | Sum of seller freight values across all seller orders | `seller_id, calendar_date` | `mart_seller_performance` | Includes cancelled orders |
| `gmv` | Sum of seller item price plus freight for non-cancelled seller orders | `seller_id, calendar_date` | `mart_seller_performance` | Canonical seller revenue metric |
| `delivered_orders_count` | Count of delivered seller orders | `seller_id, calendar_date` | `mart_seller_performance` | Late-rate denominator support |
| `cancelled_orders_count` | Count of cancelled seller orders | `seller_id, calendar_date` | `mart_seller_performance` | Cancelled multi-seller orders count once per seller-order |
| `late_orders_count` | Count of late delivered seller orders | `seller_id, calendar_date` | `mart_seller_performance` | Numerator for late delivery rate |
| `operational_defect_orders_count` | Count of seller orders that are cancelled OR late | `seller_id, calendar_date` | `mart_seller_performance` | Set union, so one seller-order contributes at most once |
| `aov` | `gmv / non_cancelled_orders_count` | `seller_id, calendar_date` | `mart_seller_performance` | Nullable when every order in the slice is cancelled |
| `cancellation_rate` | `cancelled_orders_count / orders_count` | `seller_id, calendar_date` | `mart_seller_performance` | Uses all seller orders as denominator |
| `late_delivery_rate` | `late_orders_count / delivered_orders_count` | `seller_id, calendar_date` | `mart_seller_performance` | Nullable when the seller has no deliveries |
| `operational_defect_rate` | `operational_defect_orders_count / orders_count` | `seller_id, calendar_date` | `mart_seller_performance` | Operational defect means cancelled OR late only |

## Seller Experience Metrics

Seller experience is published separately from seller performance so review
metrics are only attributed where the seller-order relationship is unambiguous.
Only orders with exactly one distinct seller are included in this contract.

| Metric | Formula | Grain | Canonical source | Notes |
|---|---|---|---|---|
| `attributable_orders_count` | Count of single-seller attributable orders | `seller_id, calendar_date` | `mart_seller_experience` | Full attributable seller-order population |
| `reviewed_attributable_orders_count` | Count of attributable orders with at least one review | `seller_id, calendar_date` | `mart_seller_experience` | Review-coverage numerator |
| `review_coverage_rate` | `reviewed_attributable_orders_count / attributable_orders_count` | `seller_id, calendar_date` | `mart_seller_experience` | Always based on attributable orders only |
| `reviews_count` | Count of review rows on attributable orders | `seller_id, calendar_date` | `mart_seller_experience` | Review-row volume on the attributable subset |
| `commented_reviews_count` | Count of review rows where `has_comment = true` on attributable orders | `seller_id, calendar_date` | `mart_seller_experience` | Customer text-feedback coverage |
| `avg_review_score` | Average order-level review score on reviewed attributable orders | `seller_id, calendar_date` | `mart_seller_experience` | Order weighted, not item weighted and not raw review-row weighted |
| `avg_time_to_review_days` | Average order-level `time_to_review_days` on reviewed attributable orders | `seller_id, calendar_date` | `mart_seller_experience` | Nullable when the slice has no reviews |
| `low_review_orders_count` | Count of reviewed attributable orders where order-level average review score is `<= low_review_score_threshold` | `seller_id, calendar_date` | `mart_seller_experience` | Threshold is controlled in `dbt_project.yml` |
| `low_review_rate` | `low_review_orders_count / reviewed_attributable_orders_count` | `seller_id, calendar_date` | `mart_seller_experience` | Nullable when there are no reviewed attributable orders |

## Fulfillment Operations Metrics

| Metric | Formula | Grain | Canonical source | Notes |
|---|---|---|---|---|
| `orders_count` | Count of orders in the cohort slice | `purchase_date, customer_state, delivery_delay_bucket` | `mart_fulfillment_ops` | Full order population |
| `delivered_orders_count` | Count of delivered orders | `purchase_date, customer_state, delivery_delay_bucket` | `mart_fulfillment_ops` | Delivered orders only |
| `late_orders_count` | Count of late delivered orders | `purchase_date, customer_state, delivery_delay_bucket` | `mart_fulfillment_ops` | Numerator for late delivery rate |
| `cancelled_orders_count` | Count of cancelled orders | `purchase_date, customer_state, delivery_delay_bucket` | `mart_fulfillment_ops` | Tracks attrition without mixing in reviews |
| `late_days_sum` | Sum of `late_days` across late orders only | `purchase_date, customer_state, delivery_delay_bucket` | `mart_fulfillment_ops` | Additive severity numerator for cross-slice rollups |
| `avg_late_days` | `late_days_sum / late_orders_count` | `purchase_date, customer_state, delivery_delay_bucket` | `mart_fulfillment_ops` | Convenience row-grain average; nullable when the slice has no late orders |
| `late_delivery_rate` | `late_orders_count / delivered_orders_count` | `purchase_date, customer_state, delivery_delay_bucket` | `mart_fulfillment_ops` | Nullable when the slice has no delivered orders |

## Customer Experience Metrics

| Metric | Formula | Grain | Canonical source | Notes |
|---|---|---|---|---|
| `orders_count` | Count of orders in the cohort slice | `purchase_date, customer_state, delivery_delay_bucket` | `mart_customer_experience` | Full order population, not review-only rows |
| `reviewed_orders_count` | Count of orders with at least one review | `purchase_date, customer_state, delivery_delay_bucket` | `mart_customer_experience` | Order coverage indicator |
| `reviews_count` | Count of review rows in the slice | `purchase_date, customer_state, delivery_delay_bucket` | `mart_customer_experience` | Review-row volume |
| `commented_reviews_count` | Count of review rows where `has_comment = true` | `purchase_date, customer_state, delivery_delay_bucket` | `mart_customer_experience` | Customer text-feedback coverage |
| `avg_review_score` | Average order-level review score | `purchase_date, customer_state, delivery_delay_bucket` | `mart_customer_experience` | Order weighted so multi-review orders do not dominate |
| `avg_time_to_review_days` | Average order-level `time_to_review_days` | `purchase_date, customer_state, delivery_delay_bucket` | `mart_customer_experience` | Nullable when the slice has no reviews |

## Delivery Delay Bucket Contract

| Bucket | Meaning |
|---|---|
| `not_delivered` | Cancelled orders or orders never delivered |
| `on_time` | Delivered on or before the estimated delivery date |
| `1_to_3_days` | Delivered 1 to 3 days late |
| `4_to_7_days` | Delivered 4 to 7 days late |
| `8_to_14_days` | Delivered 8 to 14 days late |
| `15_plus_days` | Delivered 15 or more days late |

The bucket contract is centralized in the `delivery_delay_bucket` macro.
Facts and marts must reuse that logic rather than redefining bucket boundaries.

## Identity And Enrichment Semantics

| Topic | Meaning |
|---|---|
| `customer_unique_id` | Canonical business-customer key for analysis and repeat-purchase logic |
| `customer_id` | Source lineage key retained on facts for traceability |
| `customer_*_at_order` | Order-time customer geography snapshot used for historical analysis |
| `is_purchase_on_holiday`, `holiday_name_at_purchase` | Purchase-date holiday context owned by `dim_date` and reused in delivery/facts/marts |
| `delivery_weather_location_key` | Proxy delivery-weather location key, not customer-level geospatial precision |
| `delivery_temperature_*_sum`, `delivery_precipitation_total_sum`, `delivery_humidity_afternoon_sum` | Additive proxy weather numerators across orders with a non-null observation |
| `delivery_temperature_*_observation_count`, `delivery_precipitation_total_observation_count`, `delivery_humidity_afternoon_observation_count` | Observation-count supports for proxy weather rollups |
| `avg_delivery_temperature_*`, `avg_delivery_precipitation_total`, `avg_delivery_humidity_afternoon` | Convenience row-grain proxy weather averages derived from the corresponding sum and observation-count support columns |
| Seller experience attribution | Seller review metrics are published only for single-seller attributable orders |

## Test Design

| Category | Example |
|---|---|
| Happy path | A delivered non-cancelled seller order contributes to GMV, delivered orders, and late-rate denominators consistently |
| Boundary | A seller-date with no reviewed attributable orders yields non-null `review_coverage_rate`, nullable `low_review_rate`, and null sentiment averages |
| Invalid input | A negative item or freight value fails upstream DQ tests |
| Regression | One-to-many joins cannot inflate seller performance metrics or seller attributable experience coverage |
