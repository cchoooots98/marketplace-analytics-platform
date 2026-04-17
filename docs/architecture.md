# Architecture: MerchantPulse

MerchantPulse is a target production-style analytics platform for marketplace
revenue and fulfillment reporting. The design follows an ELT pattern: Extract,
Load, Transform. Source data is loaded into BigQuery first, then dbt transforms
it into trusted analytics layers.

This document is intentionally split into current state and target state. That
keeps the portfolio credible while still showing the enterprise-grade design the
project is building toward.

## 1. Current State

| Area | Current repository state |
|---|---|
| Project framing | README and architecture are defined |
| Environment | Dependency files and `.env.example` exist |
| BigQuery | Smoke test exists; datasets and tables are not yet created by code |
| dbt | `marketplace_analytics_dbt` is initialized; no models yet |
| Ingestion | Folder skeleton exists; loaders are not yet implemented |
| Airflow | DAG folder exists; DAGs are not yet implemented |
| Dashboards | Screenshot folder exists; dashboards are not yet built |
| CI | Workflow folder exists; workflows are not yet implemented |

## 2. Business Context

The platform simulates a marketplace business that needs one trusted reporting
layer for revenue, fulfillment, seller quality, customer experience, and
external context such as holidays and weather.

The target consumers are:

| Persona | Needs |
|---|---|
| Executives | Daily revenue, order count, AOV, cancellation rate, late delivery rate, review score |
| Operations | Seller health, regional delay patterns, fulfillment risk, payment failure trends |
| Analytics engineers | Documented sources, grains, model lineage, reusable business logic, tests |

## 3. System Context

```mermaid
flowchart LR
    subgraph sources["External and source systems"]
        olist["Olist CSV dataset"]
        holiday_api["Nager.Date holiday API"]
        weather_api["OpenWeather API"]
    end

    subgraph platform["MerchantPulse platform"]
        ingestion["Python batch ingestion"]
        warehouse[("BigQuery warehouse")]
        transform["dbt project"]
        dashboards["Metabase dashboards"]
    end

    subgraph delivery["Delivery and operations"]
        airflow["Airflow DAGs"]
        docker["Docker Compose"]
        github["GitHub Actions"]
        docs["Project documentation"]
    end

    olist --> ingestion
    holiday_api --> ingestion
    weather_api --> ingestion
    ingestion --> warehouse
    warehouse --> transform
    transform --> warehouse
    warehouse --> dashboards
    transform --> docs

    airflow -. schedules .-> ingestion
    airflow -. triggers .-> transform
    docker -. runs local services .-> airflow
    docker -. runs local services .-> dashboards
    github -. validates .-> ingestion
    github -. validates .-> transform
```

## 4. Data Flow

```mermaid
flowchart TB
    source1["Olist orders, items, payments, reviews"]
    source2["Olist customers, sellers, products, geolocation"]
    source3["Holiday and weather enrichment"]

    raw_olist[("raw_olist")]
    raw_ext[("raw_ext")]
    staging[("staging")]
    intermediate[("intermediate")]
    marts[("marts")]

    executive["Executive Overview"]
    seller_ops["Seller Performance"]
    fulfillment["Fulfillment and Customer Experience"]

    source1 --> raw_olist
    source2 --> raw_olist
    source3 --> raw_ext
    raw_olist --> staging
    raw_ext --> staging
    staging --> intermediate
    intermediate --> marts
    marts --> executive
    marts --> seller_ops
    marts --> fulfillment
```

### Layer Responsibilities

| Layer | Target dataset | Responsibility | What does not belong here |
|---|---|---|---|
| Raw | `raw_olist`, `raw_ext` | Preserve source data and batch metadata | Business rules, metric logic, destructive cleanup |
| Staging | `staging` | Rename, cast, deduplicate, normalize timestamps and enums | Cross-source business calculations |
| Intermediate | `intermediate` | Reusable business logic across sources | Dashboard-only formatting |
| Marts | `marts` | Stable business-ready tables with defined grains | One-off dashboard SQL and duplicated KPI formulas |

## 5. Target Warehouse Model

```mermaid
flowchart LR
    subgraph staging["Staging models"]
        stg_orders["stg_orders"]
        stg_items["stg_order_items"]
        stg_payments["stg_payments"]
        stg_reviews["stg_reviews"]
        stg_customers["stg_customers"]
        stg_sellers["stg_sellers"]
        stg_products["stg_products"]
        stg_holidays["stg_holidays"]
        stg_weather["stg_weather_daily"]
    end

    subgraph intermediate["Intermediate models"]
        int_value["int_order_value"]
        int_delivery["int_order_delivery"]
        int_sequence["int_customer_order_sequence"]
        int_review["int_review_enriched"]
        int_seller["int_seller_daily_performance"]
    end

    subgraph marts["Facts, dimensions, and marts"]
        facts["fact_orders / fact_payments / fact_reviews"]
        dims["dim_date / dim_customer / dim_seller / dim_product"]
        exec["mart_exec_daily"]
        seller["mart_seller_performance"]
        ops["mart_fulfillment_ops"]
    end

    stg_orders --> int_value
    stg_items --> int_value
    stg_payments --> int_value
    stg_orders --> int_delivery
    stg_customers --> int_sequence
    stg_reviews --> int_review
    int_delivery --> int_review
    stg_sellers --> int_seller
    int_value --> facts
    int_delivery --> facts
    int_sequence --> dims
    int_review --> facts
    int_seller --> seller
    facts --> exec
    facts --> ops
    dims --> exec
    dims --> seller
    stg_holidays --> ops
    stg_weather --> ops
```

## 6. Data Quality and Reliability Gates

Data quality is treated as a platform feature, not as an afterthought. Core
transaction identifiers must fail fast. Optional enrichment can be missing, but
that missingness should be visible and documented.

```mermaid
flowchart TB
    ingest["Ingestion job"]
    raw_checks["Raw checks: required columns, metadata, row count"]
    dbt_parse["dbt parse"]
    dbt_tests["dbt tests: not null, unique, relationships, accepted values"]
    custom_tests["Custom tests: business rules and grain checks"]
    freshness["Freshness checks"]
    snapshots["Snapshots for history tracking"]
    publish["Publish marts and dashboards"]

    ingest --> raw_checks
    raw_checks --> dbt_parse
    dbt_parse --> dbt_tests
    dbt_tests --> custom_tests
    custom_tests --> freshness
    freshness --> snapshots
    snapshots --> publish
```

### Planned Checks

| Check type | Example |
|---|---|
| Required columns | Raw order data must contain `order_id` and timestamp fields |
| Metadata | Every raw row should include `ingested_at_utc`, `source_file_name`, and `batch_id` |
| Grain uniqueness | `mart_exec_daily` should have one row per `calendar_date` |
| Accepted values | Review score should be between 1 and 5 |
| Relationship integrity | Order items should reference known orders |
| Business rule | Delivered orders should have a delivered timestamp |
| Freshness | Orders and payments target 24-hour freshness; reviews target 48-hour freshness |

## 7. Orchestration and Local Runtime

```mermaid
sequenceDiagram
    participant Dev as Developer
    participant Docker as Docker Compose
    participant Airflow as Airflow
    participant Python as Python loaders
    participant BigQuery as BigQuery
    participant dbt as dbt
    participant Metabase as Metabase

    Dev->>Docker: start local services
    Docker->>Airflow: run scheduler and webserver
    Docker->>Metabase: run dashboard service
    Airflow->>Python: run ingestion DAG
    Python->>BigQuery: load raw datasets
    Airflow->>dbt: run dbt build DAG
    dbt->>BigQuery: build staging, intermediate, and marts
    Metabase->>BigQuery: query marts only
```

## 8. Scope

### In Scope for the target version

- Olist marketplace data ingestion
- Public holiday and weather enrichment
- BigQuery raw, staging, intermediate, and marts datasets
- dbt models, tests, source freshness, snapshots, and docs
- Metabase dashboards for executives and operations
- Airflow DAGs for ingestion and dbt execution
- Docker Compose local runtime
- GitHub Actions checks for Python, SQL, dbt, and tests
- Portfolio documentation, architecture images, dashboard screenshots, and interview notes

### Out of Scope for the target version

| Excluded | Reason |
|---|---|
| Streaming ingestion | The source data and analytics use case are batch-oriented |
| Machine learning models | The project is focused on data engineering and analytics engineering |
| Kubernetes | Docker Compose is enough for the local portfolio runtime |
| Multi-tenant access control | The project has one portfolio environment |
| Real production deployment | The target is a reproducible portfolio platform, not a live SaaS system |

## 9. Roadmap by Delivery Phase

```mermaid
flowchart LR
    p1["1. Foundation docs and environment"]
    p2["2. Raw ingestion with batch metadata"]
    p3["3. dbt staging models and source tests"]
    p4["4. Intermediate business logic"]
    p5["5. Facts, dimensions, and marts"]
    p6["6. Freshness, snapshots, and CI"]
    p7["7. Metabase dashboards and screenshots"]
    p8["8. Interview package and resume bullets"]

    p1 --> p2 --> p3 --> p4 --> p5 --> p6 --> p7 --> p8
```

## 10. Architecture Principles

- The warehouse is the source of truth for business metrics.
- Raw data preserves evidence; staging cleans shape; intermediate models hold
  reusable logic; marts serve business users.
- Every fact and mart must document its grain before implementation.
- Dashboards must read marts, not rebuild metric logic.
- Ingestion and transformations should be idempotent, so reruns do not create
  duplicates.
- Core identifiers fail fast; optional enrichment may degrade to null with
  monitoring.
- Documentation should match the current repository state and clearly label
  planned work.
