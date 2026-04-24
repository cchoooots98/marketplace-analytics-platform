# Dashboard Specifications

This document defines the Core Trio dashboard package for MerchantPulse.
Dashboards read governed marts only. If a chart needs logic that does not
exist in a mart, the mart is the place to fix it; business logic must not
live inside Metabase.

The machine-readable dashboard contract lives in
`dashboards/specs/core_trio.json`. Each card now separates two interface
contracts:

- `output_columns`: the exact result-set columns returned by the SQL asset.
- `dependency_columns`: the mart or helper-dimension columns used for filters, labels, weighting, or numerator and denominator support.
- `derived_columns`: optional human-readable provenance for display labels or calculated outputs whose source is not obvious from the result-set alone.

In Metabase OSS, the saved-question publication step remains operator-managed.
The checked-in screenshot artifacts under `dashboards/screenshots/` are the
current exported reference captures from the live dashboard layouts.

## Dashboard 1: Executive Overview

| Field | Spec |
|---|---|
| Audience | Executives and finance |
| Primary mart | `mart_exec_daily` |
| Grain | One row per calendar date in `mart_exec_daily` |
| Collection | `MerchantPulse / Executive` |
| Reference capture path | `dashboards/screenshots/executive_overview_final.png` |
| Goal | Show revenue, demand, acquisition, cancellation, service health, and customer sentiment in one purchase-date-cohorted page. |

Dashboard copy:

- GMV excludes cancelled orders by contract.
- Late delivery rate uses delivered orders only as the denominator.
- All metrics are cohorted by purchase date so each row describes one comparable order population.

Filters:

- `date_range` mapped to `mart_exec_daily.calendar_date`

Implemented visuals:

| Visual | Output columns | Dependency columns |
|---|---|---|
| GMV | `gmv` | `mart_exec_daily.calendar_date`, `mart_exec_daily.gmv` |
| Orders | `orders_count` | `mart_exec_daily.calendar_date`, `mart_exec_daily.orders_count` |
| AOV | `aov` | `mart_exec_daily.calendar_date`, `mart_exec_daily.gmv`, `mart_exec_daily.non_cancelled_orders_count` |
| Cancellation Rate | `cancellation_rate` | `mart_exec_daily.calendar_date`, `mart_exec_daily.cancelled_orders_count`, `mart_exec_daily.orders_count` |
| Late Delivery Rate | `late_delivery_rate` | `mart_exec_daily.calendar_date`, `mart_exec_daily.late_orders_count`, `mart_exec_daily.delivered_orders_count` |
| Average Review Score | `avg_review_score` | `mart_exec_daily.calendar_date`, `mart_exec_daily.review_score_sum`, `mart_exec_daily.reviews_count` |
| New Customers | `new_customers_count` | `mart_exec_daily.calendar_date`, `mart_exec_daily.new_customers_count` |
| GMV and Orders Trend | `calendar_date`, `gmv`, `orders_count` | `mart_exec_daily.calendar_date`, `mart_exec_daily.gmv`, `mart_exec_daily.orders_count` |
| Cancellation vs Late Delivery | `calendar_date`, `cancellation_rate`, `late_delivery_rate` | `mart_exec_daily.calendar_date`, `mart_exec_daily.cancelled_orders_count`, `mart_exec_daily.orders_count`, `mart_exec_daily.late_orders_count`, `mart_exec_daily.delivered_orders_count` |
| New Customers Trend | `calendar_date`, `new_customers_count` | `mart_exec_daily.calendar_date`, `mart_exec_daily.new_customers_count` |
| Review Score Trend | `calendar_date`, `avg_review_score`, `reviews_count` | `mart_exec_daily.calendar_date`, `mart_exec_daily.review_score_sum`, `mart_exec_daily.reviews_count` |

## Dashboard 2: Seller Operations

| Field | Spec |
|---|---|
| Audience | Marketplace operations |
| Primary mart | `mart_seller_performance` |
| Grain | One row per seller-date in `mart_seller_performance` |
| Collection | `MerchantPulse / Seller Ops` |
| Reference capture path | `dashboards/screenshots/seller_operations_final.png` |
| Goal | Translate executive-level variance into accountable seller-level commercial and operational ownership. |

Dashboard copy:

- The dashboard covers the full seller-order population from mart_seller_performance.
- Seller labels may fall back to seller_id when dimension attributes are missing.
- `seller_label` is documented in the machine-readable spec as a derived display field sourced from `seller_id`, `seller_city`, and `seller_state`.
- Operational defect rate means cancelled or late seller orders.

Filters:

- `date_range` mapped to `mart_seller_performance.calendar_date`
- `seller_id` mapped to `mart_seller_performance.seller_id`
- `seller_state` mapped to `dim_seller.seller_state`

Implemented visuals:

| Visual | Output columns | Dependency columns |
|---|---|---|
| Top Sellers by GMV | `seller_id`, `seller_label`, `seller_state`, `gmv` | `mart_seller_performance.seller_id`, `mart_seller_performance.calendar_date`, `mart_seller_performance.gmv`, `dim_seller.seller_city`, `dim_seller.seller_state` |
| Top Sellers by AOV | `seller_id`, `seller_label`, `seller_state`, `aov`, `gmv`, `non_cancelled_orders_count` | `mart_seller_performance.seller_id`, `mart_seller_performance.calendar_date`, `mart_seller_performance.gmv`, `mart_seller_performance.non_cancelled_orders_count`, `dim_seller.seller_city`, `dim_seller.seller_state` |
| Late Delivery Ranking | `seller_id`, `seller_label`, `seller_state`, `late_delivery_rate`, `late_orders_count`, `delivered_orders_count` | `mart_seller_performance.seller_id`, `mart_seller_performance.calendar_date`, `mart_seller_performance.late_orders_count`, `mart_seller_performance.delivered_orders_count`, `dim_seller.seller_city`, `dim_seller.seller_state` |
| Cancellation Ranking | `seller_id`, `seller_label`, `seller_state`, `cancellation_rate`, `cancelled_orders_count`, `orders_count` | `mart_seller_performance.seller_id`, `mart_seller_performance.calendar_date`, `mart_seller_performance.cancelled_orders_count`, `mart_seller_performance.orders_count`, `dim_seller.seller_city`, `dim_seller.seller_state` |
| Operational Defect View | `seller_id`, `seller_label`, `seller_state`, `operational_defect_orders_count`, `orders_count`, `operational_defect_rate` | `mart_seller_performance.seller_id`, `mart_seller_performance.calendar_date`, `mart_seller_performance.operational_defect_orders_count`, `mart_seller_performance.orders_count`, `dim_seller.seller_city`, `dim_seller.seller_state` |

## Dashboard 3: Fulfillment Operations

| Field | Spec |
|---|---|
| Audience | Fulfillment operations and analytics |
| Primary mart | `mart_fulfillment_ops` |
| Grain | One row per `purchase_date`, `customer_state`, and `delivery_delay_bucket` slice in `mart_fulfillment_ops` |
| Collection | `MerchantPulse / Fulfillment Ops` |
| Reference capture path | `dashboards/screenshots/fulfillment_operations_final.png` |
| Goal | Explain delivery delay patterns with geography, holiday context, and bucketed proxy weather risk bands aggregated across each slice's delivery-date distribution. |

Dashboard copy:

- Weather fields are proxy delivery-weather context, not customer-level geospatial truth.
- Late delivery metrics are nullable when a slice has no delivered orders.
- Holiday context comes from dim_date and stays conformed across warehouse layers.

Filters:

- `date_range` mapped to `mart_fulfillment_ops.purchase_date`
- `customer_state` mapped to `mart_fulfillment_ops.customer_state`
- `delivery_delay_bucket` mapped to `mart_fulfillment_ops.delivery_delay_bucket`
- `holiday_flag` mapped to `mart_fulfillment_ops.is_purchase_on_holiday`

Implemented visuals:

| Visual | Output columns | Dependency columns |
|---|---|---|
| Delay Rate by Customer State | `customer_state`, `orders_count`, `late_orders_count`, `delivered_orders_count`, `late_delivery_rate` | `mart_fulfillment_ops.purchase_date`, `mart_fulfillment_ops.customer_state`, `mart_fulfillment_ops.delivery_delay_bucket`, `mart_fulfillment_ops.is_purchase_on_holiday`, `mart_fulfillment_ops.orders_count`, `mart_fulfillment_ops.late_orders_count`, `mart_fulfillment_ops.delivered_orders_count` |
| Delay Bucket Distribution | `distribution_view`, `delay_bucket`, `orders_count` | `mart_fulfillment_ops.purchase_date`, `mart_fulfillment_ops.customer_state`, `mart_fulfillment_ops.delivery_delay_bucket`, `mart_fulfillment_ops.is_purchase_on_holiday`, `mart_fulfillment_ops.orders_count` |
| Holiday Impact | `holiday_cohort`, `orders_count`, `late_delivery_rate` | `mart_fulfillment_ops.purchase_date`, `mart_fulfillment_ops.customer_state`, `mart_fulfillment_ops.delivery_delay_bucket`, `mart_fulfillment_ops.is_purchase_on_holiday`, `mart_fulfillment_ops.orders_count`, `mart_fulfillment_ops.late_orders_count`, `mart_fulfillment_ops.delivered_orders_count` |
| Late Rate by Precipitation Bucket | `precipitation_bucket`, `bucket_sort`, `delivered_orders_count`, `late_orders_count`, `late_delivery_rate`, `orders_count` | `mart_fulfillment_ops.purchase_date`, `mart_fulfillment_ops.customer_state`, `mart_fulfillment_ops.delivery_delay_bucket`, `mart_fulfillment_ops.is_purchase_on_holiday`, `mart_fulfillment_ops.avg_delivery_precipitation_total`, `mart_fulfillment_ops.orders_count`, `mart_fulfillment_ops.late_orders_count`, `mart_fulfillment_ops.delivered_orders_count` |
| Late Rate by Temperature Bucket | `temperature_bucket`, `bucket_sort`, `delivered_orders_count`, `late_orders_count`, `late_delivery_rate`, `orders_count` | `mart_fulfillment_ops.purchase_date`, `mart_fulfillment_ops.customer_state`, `mart_fulfillment_ops.delivery_delay_bucket`, `mart_fulfillment_ops.is_purchase_on_holiday`, `mart_fulfillment_ops.avg_delivery_temperature_max`, `mart_fulfillment_ops.orders_count`, `mart_fulfillment_ops.late_orders_count`, `mart_fulfillment_ops.delivered_orders_count` |
| Average Late Days by State | `customer_state`, `avg_late_days`, `late_orders_count` | `mart_fulfillment_ops.purchase_date`, `mart_fulfillment_ops.customer_state`, `mart_fulfillment_ops.delivery_delay_bucket`, `mart_fulfillment_ops.is_purchase_on_holiday`, `mart_fulfillment_ops.late_days_sum`, `mart_fulfillment_ops.late_orders_count` |
| Cancelled Order Trend | `purchase_date`, `cancelled_orders_count` | `mart_fulfillment_ops.purchase_date`, `mart_fulfillment_ops.customer_state`, `mart_fulfillment_ops.delivery_delay_bucket`, `mart_fulfillment_ops.is_purchase_on_holiday`, `mart_fulfillment_ops.cancelled_orders_count` |

## Dashboard Quality Rules

- Every chart must list its `output_columns` and `dependency_columns` in `dashboards/specs/core_trio.json`.
- `output_columns` must match the SQL result contract exactly; silent KPI renames are treated as breaking changes.
- `dependency_columns` must cover filter-backed fields plus the parseable mart or helper-dimension columns referenced by the stakeholder-facing SQL expressions.
- `derived_columns` should be used when a display label or calculated output needs explicit provenance for downstream consumers.
- KPI cards must use canonical definitions from `docs/metric_definitions.md`.
- Dashboard SQL may read only `mart_*` plus `dim_date` / `dim_seller` when a display label or shared calendar field is required.
- Executive charts must read `mart_exec_daily`.
- Seller operations charts must read `mart_seller_performance`.
- Fulfillment charts must read `mart_fulfillment_ops`.
- The Core Trio dashboards remain declared as dbt exposures so mart-to-dashboard lineage stays machine readable.
- Every reference capture path must exist in `dashboards/screenshots/` and stay aligned with the current published dashboard layout.

## Roadmap: Experience Dashboards

The warehouse already publishes two additional governed marts that are not part
of the Core Trio release:

| Future dashboard | Primary mart | Why it is not in V1 |
|---|---|---|
| Customer Experience | `mart_customer_experience` | Warehouse contract is published; V1 prioritizes the commercial, seller, and fulfillment surface before expanding into customer-experience reporting |
| Seller Experience | `mart_seller_experience` | Review attribution semantics ship as a follow-on release after the operational Core Trio stabilizes |

## Test Design

| Category | Example |
|---|---|
| Happy path | Every dashboard, card, SQL asset, output contract, dependency contract, and reference capture path in `core_trio.json` validates against an active dbt exposure |
| Boundary | Approved helper joins to `dim_date` / `dim_seller` pass validation, and optional enrichment fields can be null without removing the operational order population |
| Invalid input | A dashboard spec that references a missing output column, dependency column, screenshot, exposure, filter variable, or disallowed dataset fails validation |
| Regression | A KPI output column cannot be renamed, moved, or re-sourced away from its approved mart without CI failing |
