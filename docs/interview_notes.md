# Interview Notes

This document helps explain MerchantPulse in interviews. It should stay aligned
with the actual repository state.

## 30-Second Pitch

MerchantPulse is a production-style marketplace analytics platform. I designed
it to show the full data engineering path from source ingestion to BigQuery,
dbt modeling, data quality checks, marts, and stakeholder dashboards. The
project focuses on marketplace revenue and fulfillment metrics such as GMV,
AOV, cancellation rate, late delivery rate, seller performance, and review
quality.

## 2-Minute Pitch

MerchantPulse simulates the analytics platform of a marketplace company. The
core source is the Olist e-commerce dataset, enriched with public holidays and
weather context. The target architecture loads source data into BigQuery raw
datasets, uses dbt to build staging, intermediate, and marts layers, and serves
executive and operations dashboards from the marts.

I chose this structure because it mirrors how analytics teams reduce ambiguity:
raw data preserves source evidence, staging standardizes data, intermediate
models centralize reusable business logic, and marts expose stable business
tables. The project also includes planned data quality rules, freshness targets,
snapshots for history tracking, Airflow orchestration, Docker Compose local
runtime, and GitHub Actions checks.

## Questions and Answers

**Q: Why did you build this project?**

> I built it to demonstrate an end-to-end data platform, not just isolated SQL
> queries or notebooks. The project starts from business questions and carries
> them through ingestion, warehouse modeling, quality checks, metrics, and
> dashboards.

**Q: Why BigQuery?**

> BigQuery is a cloud warehouse that is easy to explain and inspect. It supports
> SQL-first analytics and maps well to common enterprise systems such as
> Snowflake and Redshift.

**Q: Why dbt?**

> dbt lets me treat SQL transformations like software. Models are versioned,
> tested, documented, and connected through lineage. In this project, dbt owns
> staging, intermediate logic, marts, tests, freshness, snapshots, and docs.

**Q: Why use raw, staging, intermediate, and marts layers?**

> Each layer has a different responsibility. Raw keeps source evidence, staging
> standardizes shape and types, intermediate models hold reusable business
> logic, and marts serve business users. This prevents dashboards from
> duplicating logic and makes metrics easier to audit.

**Q: What are the most important data quality controls?**

> The most important controls are grain uniqueness, not-null core identifiers,
> relationship integrity, accepted values, freshness checks, and business-rule
> tests. For example, delivered orders should have a delivered timestamp, and
> order-level revenue should not be duplicated by one-to-many joins.

**Q: What trade-off did you make?**

> I chose batch ingestion instead of streaming because the source data is a
> historical marketplace dataset and the business questions are analytical.
> That keeps the project focused on reliable warehouse modeling and reproducible
> delivery instead of unnecessary streaming complexity.

## Concepts to Be Ready to Explain

| Concept | Project example |
|---|---|
| Grain | `mart_exec_daily` has one row per calendar date |
| Idempotency | Rerunning a batch should not duplicate raw or mart rows |
| Data quality | dbt tests validate keys, values, relationships, and business rules |
| Freshness | Source update expectations differ for orders, reviews, holidays, and weather |
| Snapshot | Seller or product history can be tracked as a slowly changing dimension |
| Metric ownership | GMV and AOV are defined in marts, not in dashboards |
| Optional enrichment | Weather can be null without dropping core orders |
