from datetime import timedelta
import importlib
import os
from pathlib import Path
import py_compile

import pytest


def _linear_edges(task_ids: list[str]) -> list[tuple[str, str]]:
    """Return the expected linear edges for an ordered task chain."""
    return list(zip(task_ids, task_ids[1:], strict=True))


def _schedule_summary(dag: object) -> object:
    """Read a schedule-like value across Airflow versions."""
    timetable = getattr(dag, "timetable", None)
    if timetable is not None and getattr(timetable, "summary", None) is not None:
        return timetable.summary

    schedule = getattr(dag, "schedule", None)
    if schedule is not None:
        return schedule

    return getattr(dag, "schedule_interval", None)


def test_airflow_dag_module_compiles_locally() -> None:
    """Validate the DAG file has no syntax errors on any local OS."""
    dag_path = Path(__file__).resolve().parents[1] / "airflow" / "dags"
    dag_module = dag_path / "merchantpulse_orchestration.py"

    py_compile.compile(str(dag_module), doraise=True)


@pytest.mark.skipif(
    os.name == "nt", reason="Airflow DAG parsing is Linux-container only"
)
def test_airflow_dagbag_loads_expected_dags() -> None:
    """Validate the Airflow DAG file parses and exposes the expected DAG IDs."""
    pytest.importorskip("airflow")
    models = importlib.import_module("airflow.models")
    dagbag = models.DagBag(
        dag_folder=str(Path(__file__).resolve().parents[1] / "airflow" / "dags"),
        include_examples=False,
    )

    assert dagbag.import_errors == {}
    assert set(dagbag.dags) == {
        "merchantpulse_bootstrap_backfill",
        "merchantpulse_daily_runtime",
    }

    bootstrap_dag = dagbag.dags["merchantpulse_bootstrap_backfill"]
    daily_dag = dagbag.dags["merchantpulse_daily_runtime"]

    assert _schedule_summary(bootstrap_dag) is None
    assert str(_schedule_summary(daily_dag)) == "0 6 * * *"

    for dag in (bootstrap_dag, daily_dag):
        assert dag.catchup is False
        assert dag.max_active_runs == 1
        assert dag.dagrun_timeout == timedelta(hours=3)
        assert dag.default_args["retries"] == 1
        assert dag.default_args["retry_delay"] == timedelta(minutes=10)
        assert callable(dag.default_args["on_failure_callback"])

    bootstrap_task_ids = [
        "ingest_marketplace_data",
        "dbt_source_freshness",
        "dbt_snapshot",
        "dbt_build_full_refresh",
        "dashboard_validate",
    ]
    daily_task_ids = [
        "ingest_marketplace_data",
        "dbt_source_freshness",
        "dbt_snapshot",
        "dbt_build",
        "dashboard_validate",
    ]

    assert set(bootstrap_dag.task_ids) == set(bootstrap_task_ids)
    assert set(daily_dag.task_ids) == set(daily_task_ids)

    for upstream_task_id, downstream_task_id in _linear_edges(bootstrap_task_ids):
        assert (
            downstream_task_id
            in bootstrap_dag.get_task(upstream_task_id).downstream_task_ids
        )

    for upstream_task_id, downstream_task_id in _linear_edges(daily_task_ids):
        assert (
            downstream_task_id
            in daily_dag.get_task(upstream_task_id).downstream_task_ids
        )

    assert bootstrap_dag.get_task(
        "ingest_marketplace_data"
    ).execution_timeout == timedelta(minutes=90)
    assert bootstrap_dag.get_task(
        "dbt_source_freshness"
    ).execution_timeout == timedelta(minutes=20)
    assert bootstrap_dag.get_task("dbt_snapshot").execution_timeout == timedelta(
        minutes=30
    )
    assert bootstrap_dag.get_task(
        "dbt_build_full_refresh"
    ).execution_timeout == timedelta(minutes=90)
    assert bootstrap_dag.get_task("dashboard_validate").execution_timeout == timedelta(
        minutes=15
    )
    assert daily_dag.get_task("dbt_build").execution_timeout == timedelta(minutes=90)
