# Operations Runbook

This runbook defines how to set up, run, rerun, and troubleshoot MerchantPulse
as a batch-oriented data platform. It is the primary operator reference for
environment setup, execution order, rerun strategy, failure handling, and
published BI refresh flow.

The goal is operational clarity: source freshness in runtime feed mode,
snapshots, dbt builds, and dashboard-facing marts should all have an explicit
run order and failure path.

For platform context, see `docs/architecture.md`. For data-layer guarantees,
see `docs/data_contracts.md`.

## 1. Local Prerequisites

- Python 3.11
- BigQuery access in a Google Cloud project
- A service account JSON file with BigQuery permissions
- Base dbt and ingestion dependencies installed from `requirements.txt`
- Optional orchestration dependencies from `requirements-orchestration.txt` only
  if you want to prototype future Airflow work
- Docker Desktop if you want the local Metabase runtime

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
| `OLIST_DATA_DIR` | Historical Olist CSV directory used by bootstrap ingestion |
| `OLIST_LANDING_DIR` | Landing directory containing incremental batch subdirectories |
| `INGESTION_STATE_TABLE` | Current-state control table used for incremental recovery and publish tracking |
| `WAREHOUSE_FRESHNESS_MODE` | Warehouse validation mode; `static` skips source freshness, `runtime` enforces source freshness SLAs |
| `DBT_PACKAGES_INSTALL_PATH` | Local dbt package install path for repository task execution |
| `BQ_RAW_OLIST_DATASET` | Raw Olist dataset name |
| `BQ_RAW_EXT_DATASET` | Raw enrichment dataset name |
| `BQ_STAGING_DATASET` | Staging dataset name |
| `BQ_INTERMEDIATE_DATASET` | Intermediate dataset name |
| `BQ_MARTS_DATASET` | Marts dataset name |
| `BQ_SNAPSHOTS_DATASET` | Snapshot dataset for historical seller and product versions |
| `NAGER_COUNTRY_CODE` | Holiday-country contract for enrichment loads |
| `OPENWEATHER_API_KEY` / `OPENWEATHER_LATITUDE` / `OPENWEATHER_LONGITUDE` | Required weather API connection settings |
| `OPENWEATHER_LOCATION_KEY` | Stable warehouse location grain for weather loads |
| `OPENWEATHER_MAX_CALLS_PER_RUN` | Weather API budget guardrail for one run |
| `METABASE_VERSION` | Pinned Metabase image tag for reproducible local runtime |
| `METABASE_PORT` | Local Metabase UI port |
| `METABASE_SITE_NAME` | Site name shown in the Metabase UI |
| `METABASE_ADMIN_EMAIL` | Admin contact shown by Metabase |
| `METABASE_DB_NAME` / `METABASE_DB_USER` / `METABASE_DB_PASS` | PostgreSQL app-database settings for Metabase |
| `AIRFLOW_DAILY_RUNTIME_SCHEDULE` | Optional cron schedule for the local daily Airflow DAG; leave empty for manual-only static mode |

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
python tasks.py dbt-build --select mart_exec_daily
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
python tasks.py dbt-deps
python tasks.py dbt-parse
python tasks.py dbt-freshness
python tasks.py dbt-snapshot
python tasks.py dbt-build
python tasks.py dbt-docs-generate
python tasks.py dashboard-validate
python tasks.py metabase-up
```

`python tasks.py dbt-deps` installs locked dbt packages into the local package
path before warehouse-backed commands run. `python tasks.py dbt-docs-generate` persists the latest dbt artifacts required
by downstream validation under `.cache/dbt_artifacts/` before the mandatory
`target*` cleanup runs. `python tasks.py dashboard-validate` automatically uses
that cached manifest when available.

Warehouse runtime checks are available through
`.github/workflows/dbt_runtime_checks.yml` as a manual workflow once repository
secrets for BigQuery credentials and project configuration are set. The
workflow defaults to `WAREHOUSE_FRESHNESS_MODE=static`, which skips source
freshness and relies on static backfill completeness, enrichment coverage,
grain, relationship, and reconciliation tests. Select `runtime` only when
source data is expected to arrive continuously.

## 4. Local Metabase Runtime

The Core Trio release uses a local self-hosted Metabase runtime backed by
PostgreSQL so dashboard metadata remains persistent and reproducible.

Start and stop the runtime from the repository root:

```bash
python tasks.py metabase-up
python tasks.py metabase-logs
python tasks.py metabase-down
```

Initial Metabase setup checklist:

1. Open `http://localhost:${METABASE_PORT}` after `python tasks.py metabase-up`.
2. Create the initial admin user.
3. Add a BigQuery connection using the service account JSON file described in
   the Metabase BigQuery docs.
4. Sync only the `marts` dataset.
5. Build three collections only:
   `MerchantPulse / Executive`, `MerchantPulse / Seller Ops`, and
   `MerchantPulse / Fulfillment Ops`.
6. Set money fields to currency, rate fields to percentage, and keep seller/date
   relationships aligned to `dim_seller` and `dim_date`.
7. Create saved questions from the SQL assets in `dashboards/sql/core_trio/`.
8. Treat the checked-in screenshot files as reference captures from the live
   Metabase UI.

Before publishing the dashboards, validate the repository contract:

```bash
python tasks.py dashboard-validate
```

## 5. Batch Operator Flow

The operating sequence is:

```text
1. Load or refresh raw Olist data
2. Load or refresh holiday enrichment
3. Load or refresh weather enrichment
4. Run dbt source freshness when `WAREHOUSE_FRESHNESS_MODE=runtime`
5. Run dbt snapshot
6. Run dbt build
7. Generate dbt docs artifacts for lineage and contract validation
8. Run dashboard asset validation
9. Refresh BI surfaces such as Metabase
```

Manual operator flows from the repository root align with the Airflow DAG
contracts:

```bash
python tasks.py bootstrap-backfill
python tasks.py bootstrap-backfill --skip-weather
python tasks.py daily-runtime
python tasks.py daily-runtime --skip-weather
```

Operator guidance:

- `bootstrap-backfill` is the standard cold-start historical path. It runs
  bootstrap ingestion, `dbt deps`, freshness when runtime mode is enabled, `dbt snapshot`,
  `dbt build --full-refresh`, `dbt docs generate`, and dashboard validation in
  one sequence.
- `bootstrap-backfill` defaults to `--use-olist-date-range` unless an explicit
  `--start-date` / `--end-date` range is supplied.
- `bootstrap-backfill --skip-weather` is the standard same-day replay path when
  the weather raw table was already loaded earlier and should not be fetched
  again.
- `daily-runtime` mirrors the incremental runtime DAG from local shell
  execution.
- `daily-runtime --skip-weather` is intended for controlled replays when the
  day already has weather data loaded. If new orders batches are ingested while
  weather is skipped, the ingestion control plane may intentionally leave
  `publish_complete = false` until the weather catch-up run succeeds.

Primary low-level ingestion entrypoints remain available when an operator needs
finer control over one source window:

```bash
python tasks.py ingest --use-olist-date-range
python tasks.py ingest --mode incremental
python tasks.py ingest --skip-olist --start-date 2026-01-01 --end-date 2026-12-31
```

The only supported ingestion entrypoints are `python tasks.py ingest` and
`python -m ingestion.main`. Per-source CLIs are intentionally unsupported so
logging, configuration loading, failure handling, and state persistence stay on
one shared control-plane path.

Suggested dbt commands:

```bash
python tasks.py dbt-deps
python tasks.py dbt-freshness
python tasks.py dbt-snapshot
python tasks.py dbt-build
python tasks.py dbt-docs-generate
```

Then validate the dashboard package and refresh the Metabase collections:

```bash
python tasks.py dashboard-validate
python tasks.py metabase-up
python tasks.py metabase-logs
```

`dbt_contracts.yml` also runs `python tasks.py dashboard-validate` in CI, so the
manual command is primarily for local pre-publish or pre-commit checks.

## 6. Freshness And Static Backfill Policy

Warehouse validation has two operating modes:

| Mode | Setting | Use case | Source freshness behavior |
|---|---|---|---|
| Static backfill | `WAREHOUSE_FRESHNESS_MODE=static` | Current bounded Olist historical dataset with holiday and weather enrichment over the same fixed date range | Skip source freshness and rely on completeness, coverage, grain, relationship, and reconciliation tests |
| Runtime feed | `WAREHOUSE_FRESHNESS_MODE=runtime` | Continuously arriving source batches with an actual refresh SLA | Run `dbt source freshness` and fail at the configured error threshold |

Freshness uses `ingested_at_utc` as the loaded-at timestamp because it reflects
when data actually landed in the warehouse. These SLAs are operational alarms
only in runtime feed mode.

| Source | Warn after | Error after |
|---|---|---|
| `raw_olist.orders` | 24 hours | 48 hours |
| `raw_olist.order_items` | 24 hours | 48 hours |
| `raw_olist.order_payments` | 24 hours | 48 hours |
| `raw_olist.order_reviews` | 48 hours | 96 hours |
| `raw_ext.holidays` | 30 days | 60 days |
| `raw_ext.weather_daily` | 48 hours | 96 hours |

These checks are runtime operational controls. GitHub Actions CI stays
parse-only for pull requests, while the manual runtime workflow can execute
snapshots and dbt tests once secrets are configured. It executes source
freshness as well when runtime freshness mode is selected.
Static master-data backfills such as `customers`, `sellers`, `products`, and
`geolocation` intentionally do not publish freshness SLAs in V1.

In static backfill mode, the warehouse contract shifts from freshness to
backfill completeness:

- `assert_static_backfill_sources_nonempty` fails when any required raw source
  table is empty after bootstrap ingestion.
- `assert_static_weather_delivery_date_coverage` fails when delivered Olist
  order dates do not have weather rows for the configured proxy location.
- Existing generic schema tests continue to enforce grain, relationships,
  accepted values, and numeric domains.
- Existing singular reconciliation tests continue to compare published marts
  back to their governed upstream facts and intermediates.

## 7. Snapshot Policy

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

## 8. Rerun Strategy

| Layer | Rerun rule |
|---|---|
| Raw Olist | Prefer deterministic full reload or batch replacement for this historical dataset |
| Incremental control plane | Persist current batch state in `INGESTION_STATE_TABLE`; orders reruns must resume from persisted enrichment windows instead of recomputing from missing landing files |
| Raw holidays | Current V1 behavior is full-table replace through staging -> atomic swap because the table is small and bounded |
| Raw weather | Current V1 behavior is full-table replace through staging -> atomic swap because the table volume is bounded; switch to date-partitioned merge once cumulative weather history grows beyond the full-replace threshold |
| Snapshots | Re-run after source refresh; unchanged business attributes must not create new versions |
| dbt staging | Rebuild from raw state |
| dbt intermediate | Rebuild from staging state |
| dbt marts | Rebuild from intermediate and fact state |

The key principle is idempotency: rerunning the same batch should not create
duplicate business records or false historical versions.

## 9. Failure Triage

| Symptom | First place to check | Likely cause | Immediate action |
|---|---|---|---|
| Runtime source freshness warning or failure | `max(ingested_at_utc)` in the affected raw table | Loader did not run, loaded stale data, or upstream source lagged | Confirm loader execution, inspect ingestion logs, then decide whether to rerun or accept the lag |
| Snapshot creates unexpected new versions | Snapshot source row values and `check_cols` | Batch metadata or noisy fields were included as change signals | Verify `check_cols` and compare only business attributes |
| Delivered timestamp invariant fails | `int_order_delivery` logic plus `stg_orders` timestamp cast | Semantic drift between `is_delivered` and the underlying status/timestamp fields | Recheck the `is_delivered` rule and confirm status-only delivered source anomalies are not promoted to `TRUE` |
| Payment reconciliation test fails | `int_order_value` aggregation logic plus staging rollups | Join fan-out, wrong aggregation grain, or intermediate rollup drift | Recompute expected order-level item and payment aggregates from staging before any downstream fix |
| dbt debug fails | dbt profile and service account path | Profile mismatch or missing credentials | Repair local profile and environment variables |
| Dashboard validation fails | `dashboards/specs/core_trio.json`, SQL asset path, and dbt manifest path | Missing screenshot, missing exposure, missing field, or SQL reading a disallowed dataset | Fix the spec or SQL asset before refreshing Metabase |
| Dashboard number does not match docs | Metric definition and mart SQL | Metric was redefined downstream instead of read from the mart contract | Validate mart SQL and dashboard field mapping together |

## 10. Failure Policy

Use Section 9 to diagnose the symptom first. This table only defines whether
publishing may continue once the condition is confirmed.

| Failure type | Diagnose in | Expected behavior |
|---|---|---|
| Missing core key | Model-specific investigation | Fail the job |
| Missing optional weather or holiday value | Model-specific investigation | Allow null and log or test the missing rate |
| Runtime source freshness warning | Section 9: Runtime source freshness warning or failure | Surface to operators and investigate before the next SLA breach |
| Runtime source freshness error | Section 9: Runtime source freshness warning or failure | Treat as an operational failure for supported sources |
| Snapshot change on tracked attributes | Section 9: Snapshot creates unexpected new versions | Create a new history version |
| Snapshot churn from batch metadata | Section 9: Snapshot creates unexpected new versions | Treat as a configuration bug and fix snapshot logic |
| dbt test failure | Section 9: Delivered timestamp invariant fails or Payment reconciliation test fails | Stop publishing downstream marts until resolved |
| Dashboard validation failure | Section 9: Dashboard validation fails | Stop publishing the Core Trio assets until the contract is repaired |

## 11. Test Design

Operational validation should include:

| Category | Example |
|---|---|
| Happy path | Raw refresh, static backfill completeness, snapshots, dbt build, and docs generation complete in order |
| Boundary | Orders without item rows or payment rows remain preserved in `int_order_value`; reconciliation compares the published order-grain aggregates back to staging rollups |
| Invalid input | Rows marked `is_delivered = TRUE` without an actual customer delivery timestamp fail the singular invariant test |
| Regression | Rerunning an unchanged seller or product source does not create a new snapshot version; runtime freshness remains disabled in static mode; dashboard validation catches spec drift before publish time |

## 12. CI And Warehouse Workflows

Use GitHub Actions in two distinct modes: fast parse-only CI for pull requests
and manual warehouse-backed validation when you want to validate a configured
warehouse environment. Keep scheduled execution disabled for the current static
backfill unless a real source-arrival SLA is introduced.

| Workflow | Purpose |
|---|---|
| `dbt_contracts.yml` | Parse-only CI for project structure, compile safety, and dashboard-asset validation |
| `dbt_runtime_checks.yml` | Manual warehouse-backed validation. Static mode skips source freshness and runs snapshots plus dbt tests; runtime mode also runs `dbt source freshness`. Uploads `target_runtime` artifacts on failure for triage |

Workflow expectations:

- Parse, compile, or dashboard-validation failures in `dbt_contracts.yml` are merge-blocking.
- Diagnose `dbt_contracts.yml` failures directly in workflow logs because they
  are code or project-configuration issues, not warehouse runtime incidents.
- Leave `dbt_runtime_checks.yml` manual for the current static dataset. Add a
  schedule only when the warehouse has a real recurring data-arrival SLA.
