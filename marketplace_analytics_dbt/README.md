# MerchantPulse dbt Project

This folder contains the dbt project for MerchantPulse. dbt is used for
warehouse transformations, tests, documentation, and lineage on top of
BigQuery.

## Current Status

| Area | Status |
|---|---|
| dbt project | Implemented |
| BigQuery profile | Must be configured locally before `dbt debug`, `dbt test`, or `dbt build` |
| Staging models | Implemented with schema tests |
| Intermediate models | Implemented with schema and singular tests |
| Conformed dimensions and facts | Implemented with dbt model contracts |
| Governed marts | Implemented for executive, seller operations, seller experience, fulfillment, and customer-experience reporting |
| dbt docs | Model and column descriptions are maintained in the project |
| dbt snapshots | Implemented for seller and product history tracking |

## Model Layout

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
    stg_geolocation.sql
    stg_holidays.sql
    stg_weather_daily.sql
  intermediate/
    int_order_value.sql
    int_order_delivery.sql
    int_customer_order_sequence.sql
    int_review_enriched.sql
    int_order_review_metrics.sql
    int_seller_daily_performance.sql
    int_seller_attributable_experience.sql
  exposures.yml
  marts/
    dimensions/
      dim_date.sql
      dim_customer.sql
      dim_seller.sql
      dim_product.sql
      schema.yml
    facts/
      fact_orders.sql
      fact_order_items.sql
      fact_reviews.sql
      schema.yml
    aggregates/
      mart_exec_daily.sql
      mart_seller_performance.sql
      mart_seller_experience.sql
      mart_fulfillment_ops.sql
      mart_customer_experience.sql
      schema.yml
snapshots/
  snap_sellers.sql
  snap_products.sql
tests/
  assert_*.sql
macros/
  delivery_delay_bucket.sql
  holiday_country_code.sql
```

## Warehouse Contract

The warehouse follows a Kimball-style conformed pattern:

- `dim_*` tables are pure entity dimensions.
- `fact_*` tables are conformed event facts with governed foreign keys.
- `mart_*` tables are the only approved dashboard-facing KPI contracts.

The `models/marts/` folder is organizational rather than a strict DAG boundary.
Conformed dimensions such as `dim_date` may be reused by facts or
post-conformed intermediates when that centralizes one business definition.

```text
dim_date -----------+
dim_customer -------+--> fact_orders ----------> mart_exec_daily
dim_seller ---------+--> fact_order_items -----> mart_seller_performance
dim_product --------+--> fact_reviews ---------> mart_customer_experience
fact_reviews -------+--> int_order_review_metrics
fact_order_items ---+--> int_seller_attributable_experience --> mart_seller_experience
fact_orders --------+--> mart_fulfillment_ops
```

| Model | Grain | Purpose |
|---|---|---|
| `dim_date` | One row per `calendar_date` | Calendar attributes and configured holiday flags |
| `dim_customer` | One row per `customer_unique_id` | Current-state business-customer master dimension |
| `dim_seller` | One row per `seller_id` | Seller master data |
| `dim_product` | One row per `product_id` | Product catalog attributes |
| `fact_orders` | One row per `order_id` | Order-level financials, SLA flags, customer identity, and order-time customer geography |
| `fact_order_items` | One row per `order_id + order_item_id` | Line-item revenue and inherited conformed context with intentional current-state seller/product attributes |
| `fact_reviews` | One row per `review_id + order_id` | Review score with conformed customer and order context |
| `int_order_review_metrics` | One row per `order_id` | Canonical order-level review aggregation reused across marts and tests |
| `mart_exec_daily` | One row per `calendar_date` | Executive KPIs |
| `mart_seller_performance` | One row per `seller_id + calendar_date` | Seller operations and commercial KPIs |
| `mart_seller_experience` | One row per `seller_id + calendar_date` | Seller attributable review coverage and sentiment |
| `mart_fulfillment_ops` | One row per `purchase_date + customer_state + delivery_delay_bucket` | Fulfillment operations metrics |
| `mart_customer_experience` | One row per `purchase_date + customer_state + delivery_delay_bucket` | Review coverage and sentiment metrics |

## Identity And Geography Semantics

| Topic | Standard |
|---|---|
| Canonical customer key | `customer_unique_id` |
| Source lineage customer key | `customer_id` retained on facts |
| Customer dimension semantics | Current-state customer master attributes only |
| Historical customer geography | Facts publish `customer_*_at_order` snapshots |
| Delay bucket semantics | Shared `delivery_delay_bucket` macro reused across facts and marts |
| Holiday semantics | Shared `holiday_country_code` macro reused across calendar and delivery models |

## Canonical KPI Semantics

| Metric | Definition |
|---|---|
| `orders_count` | All placed orders in the reporting slice |
| `non_cancelled_orders_count` | Orders in the slice where `is_cancelled = false` |
| `gmv` | Item value plus freight for non-cancelled orders only |
| `aov` | `gmv / non_cancelled_orders_count` |
| `cancellation_rate` | `cancelled_orders_count / orders_count` |
| `late_delivery_rate` | `late_orders_count / delivered_orders_count` |
| `operational_defect_rate` | `operational_defect_orders_count / orders_count`, where an operational defect is cancelled OR late |
| `review_coverage_rate` | `reviewed_attributable_orders_count / attributable_orders_count` on the seller-attributable subset |

Derived rates and averages published in marts are convenience fields at the
mart grain. Cross-period dashboard rollups should aggregate the corresponding
mart-published support columns instead of averaging daily derived values.

`low_review_score_threshold` is configured at the project level in
`dbt_project.yml`.

## BI Contracts And Lineage

- All published marts enforce schema with dbt model contracts.
- Dashboard-facing marts declare physical design explicitly with partitioning
  and clustering for BigQuery.
- Dashboard lineage is declared through dbt exposures so impact analysis
  remains machine readable.

## Reliability And History Contracts

- Source freshness uses `ingested_at_utc` as the loaded-at field and applies
  runtime SLAs to supported transactional and enrichment tables only when
  `WAREHOUSE_FRESHNESS_MODE=runtime`.
- Static backfill mode skips source freshness and relies on completeness,
  enrichment coverage, grain, relationship, and reconciliation tests.
- The runtime freshness policy is `warn_after = SLA` and
  `error_after = 2x SLA`.
- `snap_sellers` and `snap_products` use dbt snapshots with `check` strategy to
  track master-data history without turning current-state dimensions into SCD2
  tables.
- Snapshot change detection is limited to business attributes, not batch
  metadata, so reruns do not create false new versions.
- `fact_order_items` intentionally keeps seller and product attributes
  current-state; historical master-data analysis should join snapshots
  explicitly rather than assuming order-time copies exist.

## Seller Subject Split

The seller domain is intentionally split into two marts:

- `mart_seller_performance` publishes full-population seller commercial and
  fulfillment metrics.
- `mart_seller_experience` publishes review coverage and sentiment only on the
  single-seller attributable order subset.

This prevents the same order review from being copied across multiple sellers
while preserving a clean operations mart for seller monitoring.

## Fulfillment And Experience Marts

The project intentionally separates operational and customer-experience
reporting:

- `mart_fulfillment_ops` publishes order-population metrics only.
- `mart_customer_experience` publishes review coverage, review sentiment, and
  time-to-review metrics.
- Both marts share the same cohorting keys:
  `purchase_date`, `customer_state`, and `delivery_delay_bucket`.

This split avoids hybrid marts that mix operational and sentiment semantics in
one contract.

## Incremental Materialization

`fact_orders` is materialized incrementally so the model scales with growing
order history instead of rebuilding the full table on every run. Other marts
rebuild as tables because they are derived from the governed fact layer and
their sizes remain bounded.

| Config | Value | Why |
|---|---|---|
| `materialized` | `incremental` | Avoid rescanning full order history on every run |
| `incremental_strategy` | `merge` | Order status mutates over time and must upsert, not append |
| `unique_key` | `order_id` | Merge key for the upsert |
| `partition_by` | `purchase_date` (day) | BigQuery prunes touched partitions during merge |
| `cluster_by` | `customer_unique_id, order_status` | Speeds up common BI filter patterns |
| `on_schema_change` | `sync_all_columns` | New columns can be synchronized without a manual table rebuild |
| Lookback window | 90 days behind `max(order_purchased_at_utc)` by default | Replays a business-SLA window because no reliable row-level updated_at watermark exists |

## Common Commands

Install the base repository dependency set from the repository root:

```bash
python tasks.py install
```

Install optional orchestration dependencies only if you want the future Airflow
surface area:

```bash
python tasks.py install-orchestration
```

dbt-only local work and GitHub Actions workflows install `requirements.txt`
only. Airflow remains optional until DAG work is implemented.

Run the most common dbt commands from the repository root after a dbt profile is
configured. A starter profile lives at `profiles.yml.example`; keep real
credentials out of git.

```bash
python tasks.py dbt-debug
python tasks.py dbt-parse
python tasks.py dbt-freshness
python tasks.py dbt-snapshot
python tasks.py dbt-build
```

Direct dbt equivalents from this folder remain available when you need more
granular control. Direct `dbt source freshness` bypasses the task runner's
`WAREHOUSE_FRESHNESS_MODE` guard, so run it only for runtime feed validation:

```bash
dbt debug
dbt parse --no-partial-parse --target-path target_validation
dbt source freshness --target-path target_validation
dbt snapshot --select snap_sellers snap_products --target-path target_validation
dbt test --select path:models/marts/dimensions --target-path target_validation
dbt test --select path:models/marts/facts --target-path target_validation
dbt test --select path:models/marts/aggregates --target-path target_validation
dbt test --select assert_delivered_orders_have_delivery_timestamp assert_order_payments_reconcile_within_tolerance --target-path target_validation
dbt build --select mart_exec_daily mart_seller_performance mart_seller_experience mart_fulfillment_ops mart_customer_experience --target-path target_validation
dbt docs generate --target-path target_validation
```

Using an isolated target path is recommended on Windows to avoid
`partial_parse.msgpack` file-lock conflicts under the default `target/`
directory.

## Documentation

- Project architecture: `../docs/architecture.md`
- Data contracts: `../docs/data_contracts.md`
- Metric definitions: `../docs/metric_definitions.md`
- Dashboard specs: `../docs/dashboard_specs.md`
- Operations runbook: `../docs/operations_runbook.md`
- Minimal dbt CI: `../.github/workflows/dbt_contracts.yml`
- Manual runtime checks: `../.github/workflows/dbt_runtime_checks.yml`

The current CI workflow validates dbt project structure with `dbt parse`. It
does not execute warehouse-backed SQL, so `dbt test` still needs a configured
BigQuery environment.

## Test Design

Current dbt validation covers:

| Category | Example |
|---|---|
| Generic schema tests | Keys, relationships, ranges, and accepted values on published contracts |
| Domain constraint authority | Review score range and non-negative payment values stay in generic tests to avoid duplicate contract definitions |
| Happy path | Standard order, item, and review records flow from staging to facts and marts |
| Boundary | All-cancelled slices still appear with zero GMV and nullable AOV |
| Invalid input | Null keys, invalid review scores, or negative numeric measures fail tests |
| Regression | Grain-level reconciliations catch fan-out, holiday drift, wrong KPI denominators, and delivered/payment invariant regressions |
