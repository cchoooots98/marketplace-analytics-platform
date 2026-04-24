from datetime import UTC, date, datetime
from pathlib import Path

import pytest

import ingestion.main as ingestion_main
from ingestion.olist.batch_runtime import (
    DateWindow,
    DiscoveredOlistBatchFile,
    IncrementalOrderWindows,
)
from ingestion.olist.registry import CUSTOMERS_SPEC, ORDERS_SPEC
from ingestion.utils.batch_key import BatchKey
from ingestion.utils.bigquery_client import (
    BigQueryWriteResult,
    BigQueryWriteResultState,
)
from ingestion.utils.ingestion_state import (
    EnrichmentBatchStatus,
    IngestionBatchState,
    PublishBatchStatus,
    RawBatchStatus,
)


def _parse_args(*arguments: str):
    return ingestion_main.parse_arguments(arguments)


def _write_result(table_id: str, loaded_rows: int = 1) -> BigQueryWriteResult:
    return BigQueryWriteResult(
        table_id=table_id,
        write_mode="append",
        result_state=BigQueryWriteResultState.COMPLETED,
        job_id="job_123",
        input_rows=loaded_rows,
        input_columns=3,
        loaded_rows=loaded_rows,
    )


def _loaded_order_state(
    *,
    holiday_status: EnrichmentBatchStatus,
    weather_status: EnrichmentBatchStatus,
    publish_status: PublishBatchStatus,
) -> IngestionBatchState:
    loaded_at_utc = datetime(2026, 4, 24, 8, 30, tzinfo=UTC)
    return IngestionBatchState.loaded(
        source_name="orders",
        batch_id="batch_20260424T060000Z",
        source_file_name="olist_orders_dataset.csv",
        raw_table_id="raw_olist.orders",
        raw_loaded_rows=2,
        raw_job_id="job_123",
        created_at_utc=loaded_at_utc,
        updated_at_utc=loaded_at_utc,
        raw_loaded_at_utc=loaded_at_utc,
        holiday_window_start_date=date(2026, 1, 1),
        holiday_window_end_date=date(2026, 1, 3),
        weather_window_start_date=date(2025, 12, 31),
        weather_window_end_date=date(2026, 1, 5),
        holiday_status=holiday_status,
        weather_status=weather_status,
        publish_status=publish_status,
        published_at_utc=(
            loaded_at_utc if publish_status is PublishBatchStatus.PUBLISHED else None
        ),
    )


def test_bootstrap_all_skipped_returns_true_no_op_without_bigquery_setup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        ingestion_main,
        "create_bigquery_client",
        lambda **_: pytest.fail("BigQuery client should not be created for no-op"),
    )

    summary = ingestion_main.run_bootstrap_workflow(
        _parse_args("--skip-olist", "--skip-holidays", "--skip-weather")
    )

    assert summary.no_op is True
    assert summary.publish_complete is True
    assert summary.raw_batches_loaded == ()


def test_incremental_all_skipped_returns_true_no_op_without_bigquery_setup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        ingestion_main,
        "create_bigquery_client",
        lambda **_: pytest.fail("BigQuery client should not be created for no-op"),
    )

    summary = ingestion_main.run_incremental_workflow(
        _parse_args(
            "--mode",
            "incremental",
            "--skip-olist",
            "--skip-holidays",
            "--skip-weather",
        )
    )

    assert summary.no_op is True
    assert summary.publish_complete is True
    assert summary.raw_batches_loaded == ()
    assert summary.batches_marked_published == ()


def test_incremental_non_order_batch_publishes_immediately(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    state_rows: list[IngestionBatchState] = []
    landing_file = tmp_path / "batch_20260424T060000Z" / "olist_customers_dataset.csv"
    landing_file.parent.mkdir(parents=True)
    landing_file.write_text("customer_id\ncustomer_1\n", encoding="utf-8")

    monkeypatch.setattr(
        ingestion_main,
        "require_cli_value",
        lambda value, _: str(value),
    )
    monkeypatch.setattr(
        ingestion_main,
        "configure_google_application_credentials",
        lambda _: None,
    )
    monkeypatch.setattr(ingestion_main, "create_bigquery_client", lambda **_: object())
    monkeypatch.setattr(
        ingestion_main,
        "discover_olist_batch_files",
        lambda *_, **__: [
            DiscoveredOlistBatchFile(
                batch_id="batch_20260424T060000Z",
                source_name="customers",
                csv_path=landing_file,
            )
        ],
    )
    monkeypatch.setattr(ingestion_main, "fetch_batch_states", lambda *_, **__: {})
    monkeypatch.setattr(ingestion_main, "build_expected_olist_file_names", lambda: {})
    monkeypatch.setattr(ingestion_main, "get_olist_spec", lambda _: CUSTOMERS_SPEC)
    monkeypatch.setattr(
        ingestion_main,
        "build_batch_metadata",
        lambda *_, **__: object(),
    )
    monkeypatch.setattr(
        ingestion_main,
        "load_raw_csv",
        lambda *_, **__: _write_result("raw_olist.customers"),
    )
    monkeypatch.setattr(
        ingestion_main,
        "upsert_batch_states",
        lambda _client, _table, rows: state_rows.extend(rows),
    )

    summary = ingestion_main.run_incremental_workflow(
        _parse_args(
            "--mode",
            "incremental",
            "--project-id",
            "marketplace-prod",
            "--location",
            "EU",
            "--state-table",
            "ops.ingestion_batch_state",
            "--landing-dir",
            str(tmp_path),
        )
    )

    assert summary.no_op is False
    assert summary.publish_complete is True
    assert [batch.source_name for batch in summary.raw_batches_loaded] == ["customers"]
    assert [batch.source_name for batch in summary.batches_marked_published] == [
        "customers"
    ]
    assert [row.publish_status for row in state_rows] == [PublishBatchStatus.PUBLISHED]


def test_incremental_skip_enrichment_does_not_advance_publish(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    state_rows: list[IngestionBatchState] = []
    landing_file = tmp_path / "batch_20260424T060000Z" / "olist_orders_dataset.csv"
    landing_file.parent.mkdir(parents=True)
    landing_file.write_text("order_id\norder_1\n", encoding="utf-8")

    monkeypatch.setattr(
        ingestion_main,
        "require_cli_value",
        lambda value, _: str(value),
    )
    monkeypatch.setattr(
        ingestion_main,
        "configure_google_application_credentials",
        lambda _: None,
    )
    monkeypatch.setattr(ingestion_main, "create_bigquery_client", lambda **_: object())
    monkeypatch.setattr(
        ingestion_main,
        "discover_olist_batch_files",
        lambda *_, **__: [
            DiscoveredOlistBatchFile(
                batch_id="batch_20260424T060000Z",
                source_name="orders",
                csv_path=landing_file,
            )
        ],
    )
    monkeypatch.setattr(ingestion_main, "fetch_batch_states", lambda *_, **__: {})
    monkeypatch.setattr(ingestion_main, "build_expected_olist_file_names", lambda: {})
    monkeypatch.setattr(ingestion_main, "get_olist_spec", lambda _: ORDERS_SPEC)
    monkeypatch.setattr(
        ingestion_main,
        "build_batch_metadata",
        lambda *_, **__: object(),
    )
    monkeypatch.setattr(
        ingestion_main,
        "load_raw_csv",
        lambda *_, **__: _write_result("raw_olist.orders", loaded_rows=2),
    )
    monkeypatch.setattr(
        ingestion_main,
        "derive_incremental_order_windows",
        lambda *_, **__: IncrementalOrderWindows(
            holiday_window=DateWindow(
                start_date=date(2026, 1, 1),
                end_date=date(2026, 1, 3),
            ),
            weather_window=DateWindow(
                start_date=date(2025, 12, 31),
                end_date=date(2026, 1, 5),
            ),
        ),
    )
    monkeypatch.setattr(
        ingestion_main,
        "upsert_batch_states",
        lambda _client, _table, rows: state_rows.extend(rows),
    )
    monkeypatch.setattr(
        ingestion_main,
        "load_holidays",
        lambda *_, **__: pytest.fail("Holiday loader should be skipped"),
    )
    monkeypatch.setattr(
        ingestion_main,
        "load_weather_daily",
        lambda *_, **__: pytest.fail("Weather loader should be skipped"),
    )

    summary = ingestion_main.run_incremental_workflow(
        _parse_args(
            "--mode",
            "incremental",
            "--project-id",
            "marketplace-prod",
            "--location",
            "EU",
            "--state-table",
            "ops.ingestion_batch_state",
            "--landing-dir",
            str(tmp_path),
            "--skip-holidays",
            "--skip-weather",
        )
    )

    assert summary.no_op is False
    assert summary.publish_complete is False
    assert [batch.source_name for batch in summary.raw_batches_loaded] == ["orders"]
    assert summary.batches_marked_published == ()
    assert [row.publish_status for row in state_rows] == [PublishBatchStatus.PENDING]
    assert state_rows[0].holiday_status is EnrichmentBatchStatus.PENDING
    assert state_rows[0].weather_status is EnrichmentBatchStatus.PENDING


def test_incremental_recovers_publish_from_persisted_state_without_landing_files(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded_rows: list[IngestionBatchState] = []
    batch_key = BatchKey(
        source_name="orders",
        batch_id="batch_20260424T060000Z",
        source_file_name="olist_orders_dataset.csv",
    )

    monkeypatch.setattr(
        ingestion_main,
        "require_cli_value",
        lambda value, _: str(value),
    )
    monkeypatch.setattr(
        ingestion_main,
        "configure_google_application_credentials",
        lambda _: None,
    )
    monkeypatch.setattr(ingestion_main, "create_bigquery_client", lambda **_: object())
    monkeypatch.setattr(
        ingestion_main,
        "fetch_batch_states",
        lambda *_, **__: {
            batch_key: _loaded_order_state(
                holiday_status=EnrichmentBatchStatus.SUCCEEDED,
                weather_status=EnrichmentBatchStatus.SUCCEEDED,
                publish_status=PublishBatchStatus.PENDING,
            )
        },
    )
    monkeypatch.setattr(
        ingestion_main,
        "upsert_batch_states",
        lambda _client, _table, rows: recorded_rows.extend(rows),
    )

    summary = ingestion_main.run_incremental_workflow(
        _parse_args(
            "--mode",
            "incremental",
            "--project-id",
            "marketplace-prod",
            "--location",
            "EU",
            "--state-table",
            "ops.ingestion_batch_state",
            "--skip-olist",
            "--skip-holidays",
        )
    )

    assert summary.no_op is False
    assert summary.publish_complete is True
    assert summary.raw_batches_loaded == ()
    assert [batch.source_name for batch in summary.batches_marked_published] == [
        "orders"
    ]
    assert [row.publish_status for row in recorded_rows] == [
        PublishBatchStatus.PUBLISHED
    ]


def test_incremental_fails_fast_when_persisted_window_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded_at_utc = datetime(2026, 4, 24, 8, 30, tzinfo=UTC)
    invalid_state = IngestionBatchState.loaded(
        source_name="orders",
        batch_id="batch_20260424T060000Z",
        source_file_name="olist_orders_dataset.csv",
        raw_table_id="raw_olist.orders",
        raw_loaded_rows=2,
        raw_job_id="job_123",
        created_at_utc=loaded_at_utc,
        updated_at_utc=loaded_at_utc,
        raw_loaded_at_utc=loaded_at_utc,
        holiday_window_start_date=None,
        holiday_window_end_date=None,
        weather_window_start_date=date(2025, 12, 31),
        weather_window_end_date=date(2026, 1, 5),
        holiday_status=EnrichmentBatchStatus.PENDING,
        weather_status=EnrichmentBatchStatus.SUCCEEDED,
        publish_status=PublishBatchStatus.PENDING,
    )
    batch_key = invalid_state.batch_key()

    monkeypatch.setattr(
        ingestion_main,
        "require_cli_value",
        lambda value, _: str(value),
    )
    monkeypatch.setattr(
        ingestion_main,
        "configure_google_application_credentials",
        lambda _: None,
    )
    monkeypatch.setattr(ingestion_main, "create_bigquery_client", lambda **_: object())
    monkeypatch.setattr(
        ingestion_main,
        "fetch_batch_states",
        lambda *_, **__: {batch_key: invalid_state},
    )

    with pytest.raises(ValueError, match="Persisted holiday window"):
        ingestion_main.run_incremental_workflow(
            _parse_args(
                "--mode",
                "incremental",
                "--project-id",
                "marketplace-prod",
                "--location",
                "EU",
                "--state-table",
                "ops.ingestion_batch_state",
                "--skip-olist",
            )
        )


def test_incremental_records_failed_raw_state_when_raw_load_raises(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    recorded_rows: list[IngestionBatchState] = []
    landing_file = tmp_path / "batch_20260424T060000Z" / "olist_orders_dataset.csv"
    landing_file.parent.mkdir(parents=True)
    landing_file.write_text("order_id\norder_1\n", encoding="utf-8")

    monkeypatch.setattr(
        ingestion_main,
        "require_cli_value",
        lambda value, _: str(value),
    )
    monkeypatch.setattr(
        ingestion_main,
        "configure_google_application_credentials",
        lambda _: None,
    )
    monkeypatch.setattr(ingestion_main, "create_bigquery_client", lambda **_: object())
    monkeypatch.setattr(
        ingestion_main,
        "discover_olist_batch_files",
        lambda *_, **__: [
            DiscoveredOlistBatchFile(
                batch_id="batch_20260424T060000Z",
                source_name="orders",
                csv_path=landing_file,
            )
        ],
    )
    monkeypatch.setattr(ingestion_main, "fetch_batch_states", lambda *_, **__: {})
    monkeypatch.setattr(ingestion_main, "build_expected_olist_file_names", lambda: {})
    monkeypatch.setattr(ingestion_main, "get_olist_spec", lambda _: ORDERS_SPEC)
    monkeypatch.setattr(
        ingestion_main,
        "build_batch_metadata",
        lambda *_, **__: object(),
    )

    def fake_load_raw_csv(*args: object, **kwargs: object) -> BigQueryWriteResult:
        raise RuntimeError("raw load failed")

    monkeypatch.setattr(ingestion_main, "load_raw_csv", fake_load_raw_csv)
    monkeypatch.setattr(
        ingestion_main,
        "upsert_batch_states",
        lambda _client, _table, rows: recorded_rows.extend(rows),
    )

    with pytest.raises(RuntimeError, match="raw load failed"):
        ingestion_main.run_incremental_workflow(
            _parse_args(
                "--mode",
                "incremental",
                "--project-id",
                "marketplace-prod",
                "--location",
                "EU",
                "--state-table",
                "ops.ingestion_batch_state",
                "--landing-dir",
                str(tmp_path),
            )
        )

    assert len(recorded_rows) == 1
    failed_state = recorded_rows[0]
    assert failed_state.raw_status is RawBatchStatus.FAILED
    assert failed_state.publish_status is PublishBatchStatus.PENDING
    assert failed_state.raw_table_id == ORDERS_SPEC.resolve_table_id()
    assert failed_state.last_error_class == "RuntimeError"
    assert failed_state.last_error_message == "raw load failed"
