# Metric Definitions

This document defines the canonical KPI rules for MerchantPulse. KPI means Key
Performance Indicator. Dashboards should read these metrics from marts instead
of rebuilding formulas in the business intelligence layer.

## Metric Rules

| Rule | Standard |
|---|---|
| One definition | Each metric has one canonical warehouse definition |
| Clear denominator | Rates must name their numerator and denominator |
| Time field | Each metric must define the reporting date |
| Cancellation handling | Metrics must state whether canceled orders are included |
| Payment handling | Revenue metrics must state whether failed payments are included |

## Executive Metrics

| Metric | Formula | Reporting grain | Notes |
|---|---|---|---|
| GMV | Sum of item value plus freight value for valid marketplace orders | date | Use the canonical value from `int_order_value` or `fact_orders`; exclude canceled orders unless a cancellation analysis explicitly includes them |
| Orders | Count of unique orders | date | Use `order_purchase_date` as the default reporting date |
| AOV | GMV divided by orders | date | AOV means Average Order Value |
| Cancellation rate | Canceled orders divided by all orders | date | Use order status from `stg_orders` or cancellation flag from `int_order_delivery` |
| Late delivery rate | Late delivered orders divided by delivered orders | date | Exclude orders that were never delivered from the denominator |
| Delivered-on-time rate | On-time delivered orders divided by delivered orders | date | Complement of late delivery rate for delivered orders |
| Payment success rate | Orders with successful payment divided by orders with payment records | date | Final success logic should match available Olist payment fields |
| Average review score | Average review score | date | Use review creation date or order purchase date consistently per mart |

## Seller and Operations Metrics

| Metric | Formula | Grain | Notes |
|---|---|---|---|
| Seller GMV | Sum of seller item value and allocated freight | seller-date | Use item-level grain before aggregating to seller |
| Seller late delivery rate | Seller late delivered orders divided by seller delivered orders | seller-date | Delivery flag should come from `int_order_delivery` |
| Seller cancellation rate | Seller canceled orders divided by seller orders | seller-date | Define whether multi-seller orders are allocated by item or order |
| Seller defect rate | Orders with cancellation, late delivery, or low review divided by seller orders | seller-date | The low review threshold should be documented before implementation |
| Freight-to-item ratio | Freight value divided by item value | seller-date or order | Useful for fulfillment cost monitoring |

## Customer and Growth Metrics

| Metric | Formula | Grain | Notes |
|---|---|---|---|
| Repeat purchase rate | Customers with more than one order divided by customers with at least one order | cohort or period | Use `int_customer_order_sequence` |
| First-to-second order conversion | Customers with a second order divided by customers with a first order | cohort | Cohort date should be first order date |
| Regional demand concentration | Orders or GMV by region divided by total orders or GMV | region-period | Use a documented geography dimension |

## Enrichment Metrics

| Metric | Formula | Grain | Notes |
|---|---|---|---|
| Holiday order lift | Holiday-period orders compared with baseline orders | date or holiday window | Baseline window must be explicit |
| Bad-weather delay rate | Late delivered orders during bad weather divided by delivered orders during bad weather | date-location | Weather is enrichment, not a core identifier |
| Weather GMV comparison | GMV by weather condition | date-location | Avoid order-level precision claims unless join keys support it |

## Dashboard Ownership

| Dashboard | Metrics |
|---|---|
| Executive Overview | GMV, orders, AOV, cancellation rate, late delivery rate, average review score |
| Seller Performance | seller GMV, seller late delivery rate, seller cancellation rate, seller defect rate, average review score |
| Fulfillment and Customer Experience | delivery delay, bad-weather delay rate, holiday order lift, review score by delay bucket |

## Test Design

When marts are implemented, metric tests should cover:

| Category | Example |
|---|---|
| Happy path | A delivered order with item and payment value contributes to GMV and order count |
| Boundary | A day with zero delivered orders returns a safe null or zero late delivery rate according to the mart contract |
| Invalid input | Negative payment or item value fails a data quality test |
| Regression | One-to-many joins cannot inflate GMV, orders, or seller counts |
