# Architecture Decisions

This document records the main design decisions for MerchantPulse. Each decision
should explain the trade-off, not just the selected tool.

## ADR 001: Use BigQuery as the warehouse

| Field | Decision |
|---|---|
| Status | Accepted |
| Context | The project needs a cloud warehouse that is recognizable to data engineering and analytics engineering reviewers |
| Decision | Use BigQuery as the SQL-first warehouse |
| Why | BigQuery is easy to explain as a scalable analytics database and maps well to the target portfolio story |
| Trade-off | It requires Google Cloud credentials and may add setup friction for local-only reviewers |

## ADR 002: Use dbt for warehouse transformations

| Field | Decision |
|---|---|
| Status | Accepted |
| Context | Metric logic, tests, and documentation should live close to SQL models |
| Decision | Use dbt-bigquery for staging, intermediate, marts, tests, docs, and snapshots |
| Why | dbt makes the project read like a real analytics engineering workflow instead of a collection of ad hoc SQL scripts |
| Trade-off | dbt adds project structure and profile setup that must be documented carefully |

## ADR 003: Follow raw, staging, intermediate, marts layering

| Field | Decision |
|---|---|
| Status | Accepted |
| Context | The project needs clear ownership for source fidelity, cleaning, reusable logic, and business consumption |
| Decision | Use raw -> staging -> intermediate -> marts |
| Why | This is a common warehouse pattern and makes grain, tests, and metric ownership easier to review |
| Trade-off | It creates more files than a small demo would, but the clarity is worth it |

## ADR 004: Keep dashboards on marts only

| Field | Decision |
|---|---|
| Status | Accepted |
| Context | Dashboards can become inconsistent if every chart rebuilds metric logic |
| Decision | Metabase dashboards should query marts, not staging models or raw tables |
| Why | This keeps KPI logic governed in dbt and makes numbers traceable |
| Trade-off | Marts must be designed before dashboard work can be considered complete |

## ADR 005: Use batch ingestion, not streaming

| Field | Decision |
|---|---|
| Status | Accepted |
| Context | Olist is a historical public dataset and enrichment sources can be loaded in batches |
| Decision | Build batch loaders instead of streaming ingestion |
| Why | Batch is the correct fit for the source data and keeps the project focused on reliable analytics delivery |
| Trade-off | The project will not demonstrate Kafka or streaming concepts |

## ADR 006: Treat enrichment as optional context

| Field | Decision |
|---|---|
| Status | Accepted |
| Context | Holiday and weather data can improve analysis but should not block core transaction reporting |
| Decision | Core transaction keys fail fast; holiday and weather fields may be null with monitoring |
| Why | This mirrors production reliability: core business facts must be trusted, while external enrichment can degrade gracefully |
| Trade-off | Dashboards must explain missing enrichment coverage clearly |

## ADR 007: Use Airflow and Docker Compose for portfolio operations

| Field | Decision |
|---|---|
| Status | Planned |
| Context | The project needs to show repeatable execution without requiring managed cloud orchestration |
| Decision | Use Airflow for DAG-based orchestration and Docker Compose for local services |
| Why | This demonstrates operational thinking while remaining reproducible on a local machine |
| Trade-off | It is heavier than simple shell scripts and requires clear runbook instructions |
