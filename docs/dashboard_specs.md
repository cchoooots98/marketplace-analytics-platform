# Dashboard Specifications

This document defines the warehouse-backed dashboard contracts for
MerchantPulse. Dashboards read governed marts only. If a chart needs logic that
does not exist in a mart, the mart should be improved instead of hiding logic
in the dashboard layer.

## Dashboard 1: Executive Overview

| Field | Spec |
|---|---|
| Audience | Executives and hiring reviewers |
| Primary mart | `mart_exec_daily` |
| Grain | One row per calendar date |
| Goal | Show revenue, demand, customer acquisition, cancellation, delivery, and review health in one page on non-empty order dates only |

Planned visuals:

| Visual | Fields |
|---|---|
| GMV trend | `calendar_date`, `gmv` |
| Orders trend | `calendar_date`, `orders_count` |
| New customers trend | `calendar_date`, `new_customers_count` |
| AOV card | `aov` |
| Cancellation rate trend | `calendar_date`, `cancellation_rate` |
| Late delivery rate trend | `calendar_date`, `late_delivery_rate` |
| Review score trend | `calendar_date`, `avg_review_score` |

## Dashboard 2: Seller Operations

| Field | Spec |
|---|---|
| Audience | Marketplace operations |
| Primary mart | `mart_seller_performance` |
| Grain | One row per seller-date |
| Goal | Identify sellers with revenue opportunity or operational risk on the full seller-order population |

Planned visuals:

| Visual | Fields |
|---|---|
| Top sellers by GMV | `seller_id`, `calendar_date`, `gmv` |
| Top sellers by AOV | `seller_id`, `calendar_date`, `aov` |
| Late delivery ranking | `seller_id`, `calendar_date`, `late_delivery_rate` |
| Cancellation ranking | `seller_id`, `calendar_date`, `cancellation_rate` |
| Operational defect view | `seller_id`, `calendar_date`, `operational_defect_rate`, `operational_defect_orders_count` |

## Dashboard 3: Fulfillment Operations

| Field | Spec |
|---|---|
| Audience | Fulfillment operations |
| Primary mart | `mart_fulfillment_ops` |
| Grain | One row per `purchase_date`, `customer_state`, and `delivery_delay_bucket` |
| Goal | Explain delivery delay patterns with geography, holiday context, and proxy weather context aggregated across each slice's delivery-date distribution |

Planned visuals:

| Visual | Fields |
|---|---|
| Delay rate by customer state | `customer_state`, `late_delivery_rate` |
| Delay bucket distribution | `delivery_delay_bucket`, `orders_count` |
| Holiday impact | `purchase_date`, `is_purchase_on_holiday`, `orders_count`, `late_delivery_rate` |
| Proxy weather impact | `purchase_date`, `avg_delivery_precipitation_total`, `avg_delivery_temperature_max`, `late_delivery_rate` |
| Average late days by state | `customer_state`, `avg_late_days` |
| Cancelled order trend | `purchase_date`, `cancelled_orders_count` |

## Dashboard 4: Customer Experience

| Field | Spec |
|---|---|
| Audience | Customer experience and operations analysts |
| Primary mart | `mart_customer_experience` |
| Grain | One row per `purchase_date`, `customer_state`, and `delivery_delay_bucket` |
| Goal | Explain review coverage, sentiment, and time-to-review by the same purchase cohort and delay bucket contract |

Planned visuals:

| Visual | Fields |
|---|---|
| Review score by delay bucket | `delivery_delay_bucket`, `avg_review_score` |
| Reviewed-order coverage | `purchase_date`, `reviewed_orders_count`, `orders_count` |
| Comment coverage | `purchase_date`, `commented_reviews_count`, `reviews_count` |
| Time to review by state | `customer_state`, `avg_time_to_review_days` |
| Reviews volume trend | `purchase_date`, `reviews_count` |
| Experience heatmap | `customer_state`, `delivery_delay_bucket`, `avg_review_score` |

## Dashboard 5: Seller Experience

| Field | Spec |
|---|---|
| Audience | Marketplace operations and customer experience analysts |
| Primary mart | `mart_seller_experience` |
| Grain | One row per seller-date on the attributable order subset |
| Goal | Explain seller-level review coverage and sentiment only where order reviews can be attributed to one seller |

Planned visuals:

| Visual | Fields |
|---|---|
| Seller review coverage trend | `seller_id`, `calendar_date`, `review_coverage_rate` |
| Seller attributable review score | `seller_id`, `calendar_date`, `avg_review_score` |
| Seller low-review risk | `seller_id`, `calendar_date`, `low_review_rate`, `low_review_orders_count` |
| Attributable review volume | `seller_id`, `calendar_date`, `reviews_count`, `commented_reviews_count` |

## Dashboard Quality Rules

- Every chart must list its source mart and fields.
- KPI cards must use canonical definitions from `docs/metric_definitions.md`.
- Dashboard filters should use documented dimensions such as date, seller,
  customer state, delay bucket, product category, and order status.
- Seller operations charts must read `mart_seller_performance`.
- Seller experience charts must read `mart_seller_experience`.
- Fulfillment charts must read `mart_fulfillment_ops`; customer experience
  charts must read `mart_customer_experience`.
- Each dashboard should remain declared as a dbt exposure so mart-to-dashboard
  lineage stays machine readable.
- Dashboard copy must explain that weather fields are proxy delivery-weather
  context rather than customer-level geospatial truth.
- Seller experience dashboard copy must explain that metrics are based on the
  single-seller attributable order subset.
- Screenshots should be saved in `dashboards/screenshots/` when dashboards are
  implemented.

## Test Design

| Category | Example |
|---|---|
| Happy path | Required dashboard fields exist in the source marts |
| Boundary | Optional enrichment fields can be null without removing core order slices |
| Invalid input | A dashboard spec that references a missing field fails validation |
| Regression | A KPI field cannot be renamed or moved across marts without updating the dashboard spec |
