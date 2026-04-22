# Operations Runbook

This runbook explains how to set up, run, rerun, and troubleshoot
MerchantPulse as a batch-oriented data platform. The goal is operational
clarity: source freshness, snapshots, dbt builds, and dashboard-facing marts
should all have an explicit run order and failure path.

## 1. Local Prerequisites

- Python 3.11
- BigQuery access in a Google Cloud project
- A service account JSON file with BigQuery permissions
- Base dbt and ingestion dependencies installed from `requirements.txt`
- Optional orchestration dependencies from `requirements-orchestration.txt` only
  if you want to prototype future Airflow work
- Docker Desktop only if you want the future Airflow and Metabase runtime

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
| `BQ_SNAPSHOTS_DATASET` | Snapshot dataset for historical seller and product versions |

## 3. Core Validation Commands

The primary repository task entrypoint is `python tasks.py <command>`. `make`
targets remain available as compatibility wrappers around the same commands.

Install the base dependency set.

```bash
python tasks.py install
```

Underlying equivalent:

```bash
pip install -r requirements.txt
```

Install optional orchestration dependencies only when you need the future
Airflow surface area.

```bash
python tasks.py install-orchestration
```

Underlying equivalent:

```bash
pip install -r requirements-orchestration.txt
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
python tasks.py dbt-debug
```

PowerShell equivalent:

```powershell
New-Item -ItemType Directory -Force (Join-Path $HOME ".dbt")
Copy-Item marketplace_analytics_dbt\profiles.yml.example (Join-Path $HOME ".dbt\profiles.yml")
Set-Location marketplace_analytics_dbt
dbt debug
```

Recommended operator commands from the repository root:

```bash
python tasks.py dbt-parse
python tasks.py dbt-freshness
python tasks.py dbt-snapshot
```

Scheduled runtime checks are also available through
`.github/workflows/dbt_runtime_checks.yml` once repository secrets for BigQuery
credentials and project configuration are set.

## 4. Batch Operator Flow

The operating sequence is:

```text
1. Load or refresh raw Olist data
2. Load or refresh holiday enrichment
3. Load or refresh weather enrichment
4. Run dbt source freshness
5. Run dbt snapshot
6. Run dbt build
7. Run dbt docs generate
8. Refresh BI surfaces such as Metabase
```

Primary raw ingestion entrypoints from the repository root:

```bash
python tasks.py ingest --use-olist-date-range
python tasks.py ingest --skip-olist --start-date 2026-01-01 --end-date 2026-12-31
```

Suggested dbt commands:

```bash
python tasks.py dbt-freshness
python tasks.py dbt-snapshot

cd marketplace_analytics_dbt
dbt build
dbt docs generate
```

## 5. Freshness Policy

Freshness uses `ingested_at_utc` as the loaded-at timestamp because it reflects
when data actually landed in the warehouse.

| Source | Warn after | Error after |
|---|---|---|
| `raw_olist.orders` | 24 hours | 48 hours |
| `raw_olist.order_items` | 24 hours | 48 hours |
| `raw_olist.order_payments` | 24 hours | 48 hours |
| `raw_olist.order_reviews` | 48 hours | 96 hours |
| `raw_ext.holidays` | 30 days | 60 days |
| `raw_ext.weather_daily` | 48 hours | 96 hours |

These checks are runtime operational controls. GitHub Actions CI stays
parse-only for pull requests, while the scheduled runtime workflow can execute
warehouse-backed freshness, snapshots, and dbt tests once secrets are
configured.
Static master-data backfills such as `customers`, `sellers`, `products`, and
`geolocation` intentionally do not publish freshness SLAs in V1.

## 6. Snapshot Policy

Historical tracking is implemented through dbt snapshots:

| Snapshot | Source | Strategy | Why |
|---|---|---|---|
| `snap_sellers` | `stg_sellers` | `check` | Track seller master-data changes without relying on missing business update timestamps |
| `snap_products` | `stg_products` | `check` | Track semantic catalog changes without turning `dim_product` into a history table |

Operator guidance:

- Run snapshots after raw data is refreshed and before `dbt build`.
- Track only business attributes in `check_cols`.
- Do not add `batch_id`, `ingested_at_utc`, or `source_file_name` to
  `check_cols`; those fields would create false new versions on reruns.
- Treat snapshots as history tables, not as dashboard marts.
- Snapshot no-op churn tests become informative from the second successful
  snapshot run onward; the first run only establishes the initial current
  version for each business key.

## 7. Rerun Strategy

| Layer | Rerun rule |
|---|---|
| Raw Olist | Prefer deterministic full reload or batch replacement for this historical dataset |
| Raw holidays | Current V1 behavior is full-table replace through staging -> atomic swap because the table is small and bounded |
| Raw weather | Current V1 behavior is full-table replace through staging -> atomic swap because the table is still portfolio-scale and bounded |
| Snapshots | Re-run after source refresh; unchanged business attributes must not create new versions |
| dbt staging | Rebuild from raw state |
| dbt intermediate | Rebuild from staging state |
| dbt marts | Rebuild from intermediate and fact state |

The key principle is idempotency: rerunning the same batch should not create
duplicate business records or false historical versions.

## 8. Failure Triage

| Symptom | First place to check | Likely cause | Immediate action |
|---|---|---|---|
| Source freshness warning or failure | `max(ingested_at_utc)` in the affected raw table | Loader did not run, loaded stale data, or upstream source lagged | Confirm loader execution, inspect ingestion logs, then decide whether to rerun or accept the lag |
| Snapshot creates unexpected new versions | Snapshot source row values and `check_cols` | Batch metadata or noisy fields were included as change signals | Verify `check_cols` and compare only business attributes |
| Delivered timestamp invariant fails | `stg_orders` timestamp cast and raw order status values | Source row says delivered but actual delivery timestamp is null or malformed | Inspect source completeness and casting logic before changing marts |
| Payment reconciliation test fails | `int_order_value` aggregation logic | Join fan-out, wrong aggregation grain, or inconsistent payment population | Recheck item and payment aggregation before any downstream fix |
| dbt debug fails | dbt profile and service account path | Profile mismatch or missing credentials | Repair local profile and environment variables |
| Dashboard number does not match docs | Metric definition and mart SQL | Metric was redefined downstream instead of read from the mart contract | Validate mart SQL and dashboard field mapping together |

## 9. Failure Policy

Use Section 8 to diagnose the symptom first. This table only defines whether
publishing may continue once the condition is confirmed.

| Failure type | Diagnose in | Expected behavior |
|---|---|---|
| Missing core key | Model-specific investigation | Fail the job |
| Missing optional weather or holiday value | Model-specific investigation | Allow null and log or test the missing rate |
| Source freshness warning | Section 8: Source freshness warning or failure | Surface to operators and investigate before the next SLA breach |
| Source freshness error | Section 8: Source freshness warning or failure | Treat as an operational failure for supported sources |
| Snapshot change on tracked attributes | Section 8: Snapshot creates unexpected new versions | Create a new history version |
| Snapshot churn from batch metadata | Section 8: Snapshot creates unexpected new versions | Treat as a configuration bug and fix snapshot logic |
| dbt test failure | Section 8: Delivered timestamp invariant fails or Payment reconciliation test fails | Stop publishing downstream marts until resolved |

## 10. Test Design

Operational validation should include:

| Category | Example |
|---|---|
| Happy path | Raw refresh, freshness, snapshots, dbt build, and docs generation complete in order |
| Boundary | Orders without payment rows do not fail payment reconciliation because the contract excludes null-payment states |
| Invalid input | Delivered orders missing delivery timestamps fail the singular invariant test |
| Regression | Rerunning an unchanged seller or product source does not create a new snapshot version |

## 11. CI And Scheduled Workflows

Use GitHub Actions in two distinct modes: fast parse-only CI for pull requests
and scheduled warehouse-backed checks when you want the documented SLA to
become an actual alarm.

| Workflow | Purpose |
|---|---|
| `dbt_contracts.yml` | Parse-only CI for project structure and compile safety |
| `dbt_runtime_checks.yml` | Scheduled warehouse-backed `dbt source freshness`, snapshots, and dbt tests when secrets are configured |

Workflow expectations:

- Parse or compile failures in `dbt_contracts.yml` are merge-blocking.
- Diagnose `dbt_contracts.yml` failures directly in workflow logs because they
  are code or project-configuration issues, not warehouse runtime incidents.
