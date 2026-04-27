"""MerchantPulse Airflow DAGs for bootstrap and dynamic daily runtime."""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import TypeVar

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException

import ingestion.main as ingestion_main
import tasks

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}
BOOTSTRAP_START_DATE = datetime(2026, 1, 1, tzinfo=UTC)
DAILY_SCHEDULE = os.getenv("AIRFLOW_DAILY_RUNTIME_SCHEDULE") or None
TaskResult = TypeVar("TaskResult")


def _dag_failure_callback(context: dict[str, object]) -> None:
    """Log one structured task failure for future alerting integrations."""
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    logger.error(
        "Airflow task failed dag_id=%s task_id=%s run_id=%s try_number=%s",
        getattr(task_instance, "dag_id", None),
        getattr(task_instance, "task_id", None),
        getattr(dag_run, "run_id", None),
        getattr(task_instance, "try_number", None),
    )


DEFAULT_ARGS["on_failure_callback"] = _dag_failure_callback


def _run_task_entrypoint(
    task_name: str,
    task_runner: Callable[[], TaskResult],
) -> TaskResult:
    """Run one repository entrypoint and surface failures as Airflow errors."""
    try:
        return task_runner()
    except AirflowException:
        raise
    except Exception as exc:
        raise AirflowException(f"{task_name} failed") from exc


def _run_repository_task(*arguments: str) -> None:
    """Run one repository task and fail the Airflow task on non-zero exit."""

    def task_runner() -> None:
        exit_code = tasks.main(list(arguments))
        if exit_code != 0:
            command = " ".join(arguments)
            raise AirflowException(f"Repository task failed command={command}")

    _run_task_entrypoint("repository task", task_runner)


def _run_ingestion_task(*arguments: str) -> dict[str, object]:
    """Run one ingestion workflow and return its structured summary."""

    def task_runner() -> dict[str, object]:
        parsed_args = ingestion_main.parse_arguments(arguments)
        summary = ingestion_main.run_ingestion_workflow(parsed_args)
        return summary.to_dict()

    return _run_task_entrypoint("ingestion workflow", task_runner)


def _log_ingestion_summary(summary: dict[str, object]) -> None:
    """Log one compact ingestion summary instead of storing unused XCom data."""
    logger.info(
        "Ingestion task completed mode=%s no_op=%s publish_complete=%s "
        "raw_batches_loaded=%s batches_marked_published=%s",
        summary.get("mode"),
        summary.get("no_op"),
        summary.get("publish_complete"),
        len(summary.get("raw_batches_loaded", [])),
        len(summary.get("batches_marked_published", [])),
    )


with DAG(
    dag_id="merchantpulse_bootstrap_backfill",
    description="Bootstrap MerchantPulse raw ingestion and warehouse rebuild.",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=BOOTSTRAP_START_DATE,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    tags=["merchantpulse", "bootstrap"],
) as merchantpulse_bootstrap_backfill:

    @task(
        task_id="ingest_marketplace_data",
        execution_timeout=timedelta(minutes=90),
    )
    def bootstrap_ingest() -> None:
        ingestion_summary = _run_ingestion_task(
            "--mode",
            "bootstrap",
            "--use-olist-date-range",
        )
        _log_ingestion_summary(ingestion_summary)

    @task(
        task_id="dbt_source_freshness",
        execution_timeout=timedelta(minutes=20),
    )
    def bootstrap_freshness() -> None:
        _run_repository_task("dbt-freshness")

    @task(
        task_id="dbt_snapshot",
        execution_timeout=timedelta(minutes=30),
    )
    def bootstrap_snapshot() -> None:
        _run_repository_task("dbt-snapshot")

    @task(
        task_id="dbt_build_full_refresh",
        execution_timeout=timedelta(minutes=90),
    )
    def bootstrap_build() -> None:
        _run_repository_task("dbt-build", "--full-refresh")

    @task(
        task_id="dashboard_validate",
        execution_timeout=timedelta(minutes=15),
    )
    def bootstrap_dashboard_validate() -> None:
        _run_repository_task("dashboard-validate")

    (
        bootstrap_ingest()
        >> bootstrap_freshness()
        >> bootstrap_snapshot()
        >> bootstrap_build()
        >> bootstrap_dashboard_validate()
    )


with DAG(
    dag_id="merchantpulse_daily_runtime",
    description="Dynamic-ready daily runtime for MerchantPulse batch files.",
    default_args=DEFAULT_ARGS,
    schedule=DAILY_SCHEDULE,
    start_date=BOOTSTRAP_START_DATE,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    tags=["merchantpulse", "runtime"],
) as merchantpulse_daily_runtime:

    @task(
        task_id="ingest_marketplace_data",
        execution_timeout=timedelta(minutes=90),
    )
    def daily_ingest() -> None:
        ingestion_summary = _run_ingestion_task(
            "--mode",
            "incremental",
        )
        _log_ingestion_summary(ingestion_summary)

    @task(
        task_id="dbt_source_freshness",
        execution_timeout=timedelta(minutes=20),
    )
    def daily_freshness() -> None:
        _run_repository_task("dbt-freshness")

    @task(
        task_id="dbt_snapshot",
        execution_timeout=timedelta(minutes=30),
    )
    def daily_snapshot() -> None:
        _run_repository_task("dbt-snapshot")

    @task(
        task_id="dbt_build",
        execution_timeout=timedelta(minutes=90),
    )
    def daily_build() -> None:
        _run_repository_task("dbt-build")

    @task(
        task_id="dashboard_validate",
        execution_timeout=timedelta(minutes=15),
    )
    def daily_dashboard_validate() -> None:
        _run_repository_task("dashboard-validate")

    (
        daily_ingest()
        >> daily_freshness()
        >> daily_snapshot()
        >> daily_build()
        >> daily_dashboard_validate()
    )
