# MerchantPulse dbt Project

This folder contains the dbt project for MerchantPulse. dbt is used for
warehouse transformations, tests, documentation, snapshots, and lineage.

## Current Status

| Area | Status |
|---|---|
| dbt project | Initialized |
| BigQuery profile | Must be configured locally before `dbt debug` |
| Staging models | Planned |
| Intermediate models | Planned |
| Marts | Planned |
| dbt tests | Planned |
| dbt snapshots | Planned |
| dbt docs | Planned |

## Target Model Layout

```text
models/
  staging/
    stg_orders.sql
    stg_order_items.sql
    stg_payments.sql
    stg_reviews.sql
    stg_customers.sql
    stg_sellers.sql
    stg_products.sql
    stg_holidays.sql
    stg_weather_daily.sql
  intermediate/
    int_order_value.sql
    int_order_delivery.sql
    int_customer_order_sequence.sql
    int_review_enriched.sql
    int_seller_daily_performance.sql
  marts/
    dim_date.sql
    dim_customer.sql
    dim_seller.sql
    dim_product.sql
    fact_orders.sql
    fact_order_items.sql
    fact_payments.sql
    fact_reviews.sql
    mart_exec_daily.sql
    mart_seller_performance.sql
    mart_fulfillment_ops.sql
```

## Layering Rules

| Layer | Materialization | Responsibility |
|---|---|---|
| Staging | View | Standardize source names, types, timestamps, enums, and null-like values |
| Intermediate | View | Centralize reusable business logic across sources |
| Marts | Table | Serve stable business-ready tables and dashboard metrics |

## Common Commands

Run from this folder after a dbt profile is configured. A starter profile lives
at `profiles.yml.example`; copy it to your local dbt profile location and keep
real credentials out of git.

```bash
dbt debug
dbt parse
dbt build
dbt docs generate
```

## Documentation

- Project architecture: `../docs/architecture.md`
- Data contracts: `../docs/data_contracts.md`
- Metric definitions: `../docs/metric_definitions.md`
- Operations runbook: `../docs/operations_runbook.md`

## Test Design

When models are implemented, dbt tests should cover:

| Category | Example |
|---|---|
| Happy path | Standard order records flow from staging to marts |
| Boundary | Orders without optional enrichment remain in core marts |
| Invalid input | Null `order_id`, invalid review score, or negative payment value fails tests |
| Regression | One-to-many joins do not duplicate order-level revenue |
