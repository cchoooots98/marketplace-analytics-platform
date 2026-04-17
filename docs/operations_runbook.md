# Operations Runbook

This runbook explains how to set up, run, rerun, and troubleshoot
MerchantPulse. Some commands are current, while later pipeline commands are
target-state commands that should be enabled as implementation progresses.

## 1. Local Prerequisites

- Python 3.11
- BigQuery access in a Google Cloud project
- A service account JSON file with BigQuery permissions
- dbt-bigquery dependencies installed from `requirements.txt`
- Docker Desktop for the future Airflow and Metabase runtime

## 2. Environment Setup

Copy the template and fill in local values.

```bash
cp .env.example .env
```

PowerShell equivalent:

```powershell
Copy-Item .env.example .env
```

Required values:

| Variable | Purpose |
|---|---|
| `GCP_PROJECT_ID` | Google Cloud project for BigQuery |
| `BIGQUERY_LOCATION` | BigQuery location, for example `EU` or `US` |
| `GOOGLE_APPLICATION_CREDENTIALS` | Absolute path to the service account JSON file |
| `BQ_RAW_OLIST_DATASET` | Raw Olist dataset name |
| `BQ_RAW_EXT_DATASET` | Raw enrichment dataset name |
| `BQ_STAGING_DATASET` | Staging dataset name |
| `BQ_INTERMEDIATE_DATASET` | Intermediate dataset name |
| `BQ_MARTS_DATASET` | Marts dataset name |

## 3. Current Smoke Checks

Install dependencies.

```bash
pip install -r requirements.txt
```

Run the BigQuery connection smoke test after `.env` values are exported into the
shell environment.

```bash
pytest tests/test_bigquery_connection.py -q
```

Validate dbt configuration after a dbt profile is configured.

```bash
mkdir -p ~/.dbt
cp marketplace_analytics_dbt/profiles.yml.example ~/.dbt/profiles.yml
cd marketplace_analytics_dbt
dbt debug
```

PowerShell equivalent:

```powershell
New-Item -ItemType Directory -Force (Join-Path $HOME ".dbt")
Copy-Item marketplace_analytics_dbt\profiles.yml.example (Join-Path $HOME ".dbt\profiles.yml")
Set-Location marketplace_analytics_dbt
dbt debug
```

## 4. Target Pipeline Run Order

The target pipeline is batch-oriented.

```text
1. Load Olist raw data
2. Load holiday enrichment
3. Load weather enrichment
4. Run dbt source freshness
5. Run dbt build
6. Run dbt docs generate
7. Refresh Metabase dashboards
```

## 5. Rerun Strategy

| Layer | Rerun rule |
|---|---|
| Raw Olist | Prefer deterministic full reload or batch replacement for this historical dataset |
| Raw holidays | Replace the target country-year partition or full small table |
| Raw weather | Replace the target date-location batch |
| dbt staging | Rebuild from raw state |
| dbt intermediate | Rebuild from staging state |
| dbt marts | Rebuild from intermediate and fact state |

The key principle is idempotency: rerunning the same batch should not create
duplicate business records.

## 6. Troubleshooting

| Symptom | First place to check | Likely cause |
|---|---|---|
| BigQuery auth fails | `.env` and service account path | Missing or invalid `GOOGLE_APPLICATION_CREDENTIALS` |
| dbt debug fails | dbt profile and project name | Profile name mismatch or missing credentials |
| Raw row count is zero | ingestion logs and source path | Wrong local data path or failed API response |
| Mart row count is unexpectedly high | join grain and intermediate aggregation | One-to-many join was not aggregated before mart logic |
| Dashboard number does not match docs | metric definition and mart SQL | Metric was rebuilt in dashboard instead of read from mart |

## 7. Failure Policy

| Failure type | Expected behavior |
|---|---|
| Missing core key | Fail the job |
| Missing optional weather or holiday value | Allow null and log or test the missing rate |
| API timeout | Retry or fail clearly with request context |
| dbt test failure | Stop publishing downstream marts until resolved |
| CI failure | Do not merge until fixed |

## 8. Test Design

No production functions are changed by this runbook. Operational validation
should eventually include:

| Category | Example |
|---|---|
| Happy path | Full local run completes from ingestion through dbt build |
| Boundary | Empty optional enrichment does not block core transaction marts |
| Invalid input | Missing credentials fail with a clear error |
| Regression | Rerunning a completed batch does not duplicate raw or mart rows |
