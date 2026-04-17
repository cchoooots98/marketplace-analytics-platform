# Dashboard Specifications

This document defines the target dashboards for MerchantPulse. Dashboards should
read from marts only. If a chart needs logic that does not exist in a mart, the
mart should be improved instead of hiding logic in the dashboard layer.

## Dashboard 1: Executive Overview

| Field | Spec |
|---|---|
| Audience | Executives and hiring reviewers |
| Primary mart | `mart_exec_daily` |
| Grain | One row per calendar date |
| Goal | Show revenue, demand, cancellation, delivery, and review health in one page |

Planned visuals:

| Visual | Fields |
|---|---|
| GMV trend | `calendar_date`, `gmv` |
| Orders trend | `calendar_date`, `orders_count` |
| AOV card | `aov` |
| Cancellation rate trend | `calendar_date`, `cancellation_rate` |
| Late delivery rate trend | `calendar_date`, `late_delivery_rate` |
| Review score trend | `calendar_date`, `average_review_score` |

## Dashboard 2: Seller Performance

| Field | Spec |
|---|---|
| Audience | Marketplace operations |
| Primary mart | `mart_seller_performance` |
| Grain | One row per seller-date |
| Goal | Identify sellers with revenue opportunity or operational risk |

Planned visuals:

| Visual | Fields |
|---|---|
| Top sellers by GMV | `seller_id`, `gmv` |
| Late delivery ranking | `seller_id`, `late_delivery_rate` |
| Cancellation ranking | `seller_id`, `cancellation_rate` |
| Seller defect view | `seller_id`, `defect_rate` |
| Review score distribution | `seller_id`, `average_review_score` |

## Dashboard 3: Fulfillment and Customer Experience

| Field | Spec |
|---|---|
| Audience | Fulfillment operations and customer experience |
| Primary mart | `mart_fulfillment_ops` |
| Grain | Target grain to be finalized as date-region or date-status |
| Goal | Explain delivery delay patterns and their relationship with holidays, weather, and reviews |

Planned visuals:

| Visual | Fields |
|---|---|
| Delivery delay by region | `region`, `late_delivery_rate`, `average_late_days` |
| Delay bucket distribution | `delay_bucket`, `orders_count` |
| Holiday impact | `is_holiday`, `orders_count`, `late_delivery_rate` |
| Weather impact | `weather_condition`, `orders_count`, `late_delivery_rate` |
| Review by delay bucket | `delay_bucket`, `average_review_score` |

## Dashboard Quality Rules

- Every chart must list its source mart and fields.
- KPI cards must use canonical definitions from `docs/metric_definitions.md`.
- Dashboard filters should use documented dimensions such as date, seller, region,
  product category, and order status.
- Screenshots should be saved in `dashboards/screenshots/` when dashboards are
  implemented.

## Test Design

When dashboard support scripts or field mappings are implemented, tests should
cover:

| Category | Example |
|---|---|
| Happy path | Required dashboard fields exist in the source marts |
| Boundary | Optional enrichment filters do not remove all core transaction rows |
| Invalid input | A dashboard spec that references a missing field fails validation |
| Regression | A KPI field cannot be renamed without updating the dashboard spec |
