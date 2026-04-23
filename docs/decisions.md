# Architecture Decisions

This document records the major design decisions for MerchantPulse. Each
decision explains the trade-off, not just the selected implementation.

## ADR 001: Use BigQuery as the warehouse

| Field | Decision |
|---|---|
| Status | Accepted |
| Context | The platform needs a cloud-native, SQL-first analytics warehouse with managed scaling and first-class dbt support |
| Decision | Use BigQuery as the SQL-first analytics warehouse |
| Why | BigQuery offers serverless scaling, native partitioning and clustering, and a mature dbt adapter, which fits the ELT pattern used throughout this platform |
| Trade-off | It requires GCP credentials and project setup for any warehouse-backed validation |

## ADR 002: Use dbt for warehouse transformations

| Field | Decision |
|---|---|
| Status | Accepted |
| Context | SQL logic, tests, and docs should live close to the warehouse models |
| Decision | Use dbt-bigquery for staging, intermediate, conformed models, marts, tests, and docs |
| Why | dbt provides version-controlled SQL, declarative tests, lineage, model contracts, and generated documentation as one toolchain |
| Trade-off | dbt adds project structure and profile setup that must be documented carefully |

## ADR 003: Follow raw -> staging -> intermediate -> conformed contracts -> marts layering

| Field | Decision |
|---|---|
| Status | Accepted |
| Context | The project needs clear ownership for source fidelity, cleaning, reusable logic, conformed entities, and analytics consumption |
| Decision | Use raw, staging, reusable intermediates, conformed facts/dimensions, and governed marts as the warehouse layering standard |
| Why | This is a standard warehouse layering pattern that gives each layer one responsibility and makes grain ownership, testing, and documentation easier to govern. Folder paths remain organizational, so a conformed dimension such as `dim_date` may be reused outside a strict "last step only" mart flow. |
| Trade-off | It creates more files and some folder names no longer imply strict DAG order, so the architecture docs must explain semantic ownership clearly |

## ADR 004: Keep dimensions pure

| Field | Decision |
|---|---|
| Status | Accepted |
| Context | Lifetime metrics inside dimensions blur the boundary between entity attributes and performance facts |
| Decision | `dim_product`, `dim_seller`, and `dim_customer` publish entity attributes only |
| Why | This keeps dimensions stable, reusable, and semantically clean; performance belongs in facts or marts |
| Trade-off | Analysts who want lifetime or profile metrics must read dedicated marts rather than dimensions |

## ADR 005: Use `customer_unique_id` as the canonical customer grain

| Field | Decision |
|---|---|
| Status | Accepted |
| Context | Olist issues a new `customer_id` per order, so `customer_id` alone does not represent a business customer |
| Decision | Make `dim_customer` grain one row per `customer_unique_id` and use `customer_unique_id` as the fact foreign key |
| Why | Repeat-purchase logic, new-customer counts, and customer-level reporting must operate on business identity, not source-record identity |
| Trade-off | `customer_id` remains necessary on facts as source lineage even though it is not the business key |

## ADR 006: Put historical customer geography on facts

| Field | Decision |
|---|---|
| Status | Accepted |
| Context | A current-state customer dimension cannot safely answer historical order geography if customer attributes ever change |
| Decision | Facts publish `customer_zip_code_prefix_at_order`, `customer_city_at_order`, and `customer_state_at_order` |
| Why | This is the standard V1 compromise: a current-state customer dimension for "what is true now" plus order-time snapshots on events for "what was true at the time of order" |
| Trade-off | Geography appears in both the dimension and facts, but with different semantics that must be documented clearly |

## ADR 007: Split fulfillment operations and customer experience into separate marts

| Field | Decision |
|---|---|
| Status | Accepted |
| Context | Mixing operational delivery metrics and review metrics into one mart creates ambiguous ownership and noisy contracts |
| Decision | Publish `mart_fulfillment_ops` for operational metrics and `mart_customer_experience` for review metrics |
| Why | Each mart has one subject-area contract, one testing strategy, and one clear dashboard audience |
| Trade-off | Some dashboards will query two marts instead of one if they want both operations and experience views on the same page |

## ADR 008: Centralize holiday-country selection and publish holiday semantics through dim_date

| Field | Decision |
|---|---|
| Status | Accepted |
| Context | Holiday filtering existed in multiple models and risked semantic drift |
| Decision | Use one shared macro to derive the configured holiday country code, then publish the conformed holiday meaning once in `dim_date` |
| Why | `int_order_delivery`, facts, and marts now reuse the same calendar-owned holiday semantics instead of re-joining raw holiday data independently |
| Trade-off | Calendar semantics become more centralized, so `dim_date` must remain robust during bootstrap and partial-load states |

## ADR 009: Use net GMV as the canonical commercial revenue metric

| Field | Decision |
|---|---|
| Status | Accepted |
| Context | Executive and seller reporting need a revenue metric aligned to completed commercial activity rather than cancelled demand |
| Decision | Define `gmv` as item value plus freight for non-cancelled orders only |
| Why | This aligns the revenue numerator with the commercial population that actually converted, while cancellations remain visible through their own metrics |
| Trade-off | Analysts who want gross demand including cancellations must use `items_value`, `freight_total`, or `orders_count` instead of `gmv` |

## ADR 010: Use non-cancelled orders as the AOV denominator

| Field | Decision |
|---|---|
| Status | Accepted |
| Context | Once GMV excludes cancelled orders, dividing by all orders would mix different business populations in one KPI |
| Decision | Define `aov` as `gmv / non_cancelled_orders_count` |
| Why | This keeps the numerator and denominator aligned to the same commercial population |
| Trade-off | AOV becomes nullable on slices where every order is cancelled |

## ADR 011: Define seller operational defect as a fulfillment union metric

| Field | Decision |
|---|---|
| Status | Accepted |
| Context | Seller performance needs one governed operational-risk KPI, but seller review metrics are no longer attributed across the full seller-order population |
| Decision | A seller order is an operational defect when it is cancelled OR late |
| Why | This keeps seller performance focused on full-population commercial and fulfillment behavior without mixing in ambiguous customer-experience attribution |
| Trade-off | Customer sentiment is no longer part of seller performance and must be read from the separate seller experience mart |

## ADR 012: Materialize `fact_orders` incrementally

| Field | Decision |
|---|---|
| Status | Accepted |
| Context | Order facts grow over time and order status changes across the lifecycle |
| Decision | Materialize `fact_orders` incrementally with merge on `order_id`, partition by `purchase_date`, cluster by `customer_unique_id`, `order_status`, and replay a 90-day business-SLA lookback window by default |
| Why | Olist does not expose a trustworthy row-level updated_at watermark, so a wide replay window is the safest pragmatic strategy for catching late lifecycle changes without depending on an unreliable change marker |
| Trade-off | The model scans more history than a true CDC watermark would, and historical logic changes still require a full refresh |

## ADR 013: Publish seller experience only on the attributable order subset

| Field | Decision |
|---|---|
| Status | Accepted |
| Context | A marketplace review is naturally an order-level signal, not a seller-level signal, and copying one order review to every seller on a multi-seller order creates false precision |
| Decision | Publish `mart_seller_experience` on the subset of orders that have exactly one distinct seller in `fact_order_items` |
| Why | This preserves attribution correctness while still publishing a governed seller experience contract with an explicit review-coverage metric on the attributable subset |
| Trade-off | Seller experience metrics no longer represent the full seller-order population; coverage must be published explicitly |

## ADR 014: Centralize order-level review aggregation in one intermediate contract

| Field | Decision |
|---|---|
| Status | Accepted |
| Context | Order-level review counts and averages were being re-implemented across marts and tests, creating drift risk |
| Decision | Publish `int_order_review_metrics` as the only canonical order-grain review aggregation contract |
| Why | Customer-experience and seller-experience models now share one order-weighted review definition instead of duplicating aggregation logic |
| Trade-off | Downstream models must depend on one more intermediate, but the semantic clarity is worth the extra node |

## ADR 015: Use runtime freshness SLAs with warn-at-SLA and fail-at-2x-SLA

| Field | Decision |
|---|---|
| Status | Accepted |
| Context | The project needs source observability, but freshness checks run against the warehouse and should not be confused with parse-only CI |
| Decision | Use `ingested_at_utc` as the source freshness timestamp, warn at the stated SLA, and fail at 2x the SLA for supported transactional and enrichment sources |
| Why | This matches the standard freshness-SLA operating pattern: operators see early degradation before the source becomes truly stale |
| Trade-off | Freshness remains a runtime control in a configured environment; pull-request CI still cannot prove data recency. Static master-data backfills (`customers`, `sellers`, `products`, `geolocation`) intentionally do not publish freshness SLAs in V1. |

## ADR 016: Track seller and product history with check-based snapshots

| Field | Decision |
|---|---|
| Status | Accepted |
| Context | Olist seller and product sources do not provide a trustworthy business `updated_at`, but the platform still needs a credible history-tracking story |
| Decision | Use dbt snapshots with `check` strategy on cleaned staging models `stg_sellers` and `stg_products` |
| Why | This delivers SCD2-style history tracking on the cleaned staging layer while restricting change detection to semantic master-data attributes instead of batch metadata noise |
| Trade-off | Snapshot change detection must be curated carefully; adding noisy columns to `check_cols` would create false history versions |

## ADR 017: Keep domain constraints in generic tests and reserve singular tests for business invariants

| Field | Decision |
|---|---|
| Status | Accepted |
| Context | The project already uses generic dbt tests for review-score and payment-value domains, and duplicating them as singular tests would create two competing contract definitions |
| Decision | Keep single-column domain rules in generic tests and add singular tests only for cross-column or cross-model invariants such as delivery timestamp integrity and payment reconciliation |
| Why | This preserves one authoritative place for column-level contracts while still adding higher-signal custom coverage where generic tests are insufficient |
| Trade-off | Some requested "custom tests" remain implemented as generic tests, so documentation must explain the distinction clearly |

## ADR 018: Keep seller and product attributes current-state in line-item marts

| Field | Decision |
|---|---|
| Status | Accepted |
| Context | Customer geography is copied onto facts at order time, but seller and product attributes could either be copied as order-time snapshots or left as current-state joins |
| Decision | Keep seller and product attributes current-state in `fact_order_items` and track their history in snapshots instead of copying them onto every order item row |
| Why | This keeps the line-item fact lean and prevents implying a frozen-at-order-time contract that the source data does not support, while history remains accessible through snapshots |
| Trade-off | Historical seller/product attribute analysis requires an explicit join to snapshots; marts should not imply those attributes are frozen at order time |

## ADR 019: Run warehouse-backed freshness and dbt tests on a schedule

| Field | Decision |
|---|---|
| Status | Accepted |
| Context | Parse-only CI proves project structure, but it does not turn freshness SLAs or dbt tests into an actual operating alarm |
| Decision | Add a scheduled GitHub Actions workflow for `dbt source freshness`, `dbt snapshot`, and dbt tests when runtime secrets are configured |
| Why | This makes the documented SLA and DQ contracts observable in a recurring runtime environment instead of leaving them as manual-only checks |
| Trade-off | The scheduled workflow depends on repository secrets and warehouse access, so it is more operationally coupled than parse-only CI |
