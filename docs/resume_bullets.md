# Resume Bullets

These bullets describe the target finished version of MerchantPulse. Use them
only after the relevant implementation exists in the repository.

## Data Engineering Version

- Built a production-style marketplace analytics platform using Python,
  BigQuery, dbt, Airflow, Docker Compose, and GitHub Actions to connect source
  ingestion, warehouse modeling, quality checks, and dashboard delivery.
- Designed idempotent batch ingestion for Olist marketplace data plus holiday
  and weather enrichment, adding batch metadata for traceability and reruns.
- Modeled raw, staging, intermediate, and marts layers in dbt with documented
  grains, schema tests, freshness checks, and snapshot-based history tracking.
- Created executive and operations marts for GMV, AOV, cancellation rate, late
  delivery rate, seller performance, and customer review quality.

## Analytics Engineering Version

- Centralized marketplace KPI logic in dbt marts so Metabase dashboards read
  governed tables instead of duplicating business logic in chart SQL.
- Defined table grains, metric formulas, and dashboard field mappings for
  executive revenue reporting, seller operations, and fulfillment analytics.
- Implemented data quality contracts for uniqueness, not-null keys,
  relationship integrity, accepted values, and business-rule validation.

## Short Project Summary

MerchantPulse is a production-style marketplace analytics platform that turns
transactional and enrichment data into trusted BigQuery marts and stakeholder
dashboards using dbt, Python, Airflow, Docker Compose, and GitHub Actions.
