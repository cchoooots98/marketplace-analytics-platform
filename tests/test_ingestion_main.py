from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from ingestion import main as ingestion_main
from ingestion.utils.bigquery_client import (
    BigQueryConfigurationError,
    BigQueryWriteResult,
)
from ingestion.weather.fetch_weather_daily import WeatherDailyConfig


def test_resolve_olist_date_range_reads_local_orders_csv(tmp_path: Path) -> None:
    """Validate Olist date range resolution from the local orders file.

    Args:
        tmp_path: Pytest fixture providing a temporary directory.

    Returns:
        None.
    """
    orders_csv_path = tmp_path / "olist_orders_dataset.csv"
    pd.DataFrame(
        {
            "order_purchase_timestamp": [
                "2016-09-04 21:15:19",
                "2018-10-17 17:30:18",
            ]
        }
    ).to_csv(orders_csv_path, index=False)

    assert ingestion_main.resolve_olist_date_range(tmp_path) == (
        date(2016, 9, 4),
        date(2018, 10, 17),
    )


def test_main_runs_olist_holidays_and_weather_in_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validate the unified entrypoint orchestrates all raw loaders in order.

    Args:
        monkeypatch: Pytest fixture for replacing collaborators.

    Returns:
        None.
    """
    calls: list[str] = []
    fake_client = object()

    def fake_create_bigquery_client(
        *,
        project_id: str | None,
        location: str | None,
    ) -> object:
        calls.append("client")
        return fake_client

    def fake_run_olist_loaders(
        olist_data_dir: str,
        *,
        client: object,
        project_id: str,
        location: str,
        loaders: tuple[ingestion_main.OlistTableLoader, ...] = (),
    ) -> list[BigQueryWriteResult]:
        calls.append("olist")
        return []

    def fake_load_holidays(
        start_date: date,
        end_date: date,
        *,
        country_code: str,
        client: object,
        project_id: str,
        location: str,
    ) -> BigQueryWriteResult:
        calls.append("holidays")
        return _write_result("raw_ext.holidays")

    def fake_load_weather_daily(
        start_date: date,
        end_date: date,
        config: WeatherDailyConfig,
        *,
        write_mode: str,
        client: object,
        project_id: str,
        location: str,
    ) -> BigQueryWriteResult:
        calls.append("weather")
        return _write_result("raw_ext.weather_daily")

    monkeypatch.setenv("GCP_PROJECT_ID", "marketplace-prod")
    monkeypatch.setenv("BIGQUERY_LOCATION", "EU")
    monkeypatch.setenv("OPENWEATHER_API_KEY", "test-key")
    monkeypatch.setenv("OPENWEATHER_LATITUDE", "-23.5505")
    monkeypatch.setenv("OPENWEATHER_LONGITUDE", "-46.6333")
    monkeypatch.setattr(ingestion_main, "load_dotenv", lambda: None)
    monkeypatch.setattr(
        ingestion_main, "configure_google_application_credentials", lambda _: None
    )
    monkeypatch.setattr(
        ingestion_main, "create_bigquery_client", fake_create_bigquery_client
    )
    monkeypatch.setattr(ingestion_main, "run_olist_loaders", fake_run_olist_loaders)
    monkeypatch.setattr(ingestion_main, "load_holidays", fake_load_holidays)
    monkeypatch.setattr(ingestion_main, "load_weather_daily", fake_load_weather_daily)

    exit_code = ingestion_main.main(
        [
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-02",
        ]
    )

    assert exit_code == 0
    assert calls == ["client", "olist", "holidays", "weather"]


def test_main_skip_flags_skip_requested_sections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validate skip flags disable only the requested sections.

    Args:
        monkeypatch: Pytest fixture for replacing collaborators.

    Returns:
        None.
    """
    calls: list[str] = []

    monkeypatch.setenv("GCP_PROJECT_ID", "marketplace-prod")
    monkeypatch.setenv("BIGQUERY_LOCATION", "EU")
    monkeypatch.setattr(ingestion_main, "load_dotenv", lambda: None)
    monkeypatch.setattr(
        ingestion_main, "configure_google_application_credentials", lambda _: None
    )
    monkeypatch.setattr(
        ingestion_main,
        "create_bigquery_client",
        lambda **_: object(),
    )
    monkeypatch.setattr(
        ingestion_main, "run_olist_loaders", lambda *_, **__: calls.append("olist")
    )
    monkeypatch.setattr(
        ingestion_main, "load_holidays", lambda *_, **__: calls.append("holidays")
    )
    monkeypatch.setattr(
        ingestion_main, "load_weather_daily", lambda *_, **__: calls.append("weather")
    )

    exit_code = ingestion_main.main(
        [
            "--skip-olist",
            "--skip-weather",
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-02",
        ]
    )

    assert exit_code == 0
    assert calls == ["holidays"]


def test_main_missing_dates_fails_before_loader_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validate enrichment loaders require an explicit or derived date range.

    Args:
        monkeypatch: Pytest fixture for replacing collaborators.

    Returns:
        None.
    """
    monkeypatch.setattr(ingestion_main, "load_dotenv", lambda: None)
    monkeypatch.setattr(
        ingestion_main,
        "run_olist_loaders",
        lambda *_, **__: pytest.fail(
            "Olist should not run after date validation failure"
        ),
    )

    exit_code = ingestion_main.main([])

    assert exit_code == 1


def test_main_use_olist_date_range_resolves_expected_dates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validate the full-history shortcut uses the Olist-derived range.

    Args:
        monkeypatch: Pytest fixture for replacing collaborators.

    Returns:
        None.
    """
    captured_dates: dict[str, date] = {}

    def fake_load_holidays(
        start_date: date,
        end_date: date,
        **kwargs: object,
    ) -> BigQueryWriteResult:
        captured_dates["start_date"] = start_date
        captured_dates["end_date"] = end_date
        return _write_result("raw_ext.holidays")

    monkeypatch.setenv("GCP_PROJECT_ID", "marketplace-prod")
    monkeypatch.setenv("BIGQUERY_LOCATION", "EU")
    monkeypatch.setattr(ingestion_main, "load_dotenv", lambda: None)
    monkeypatch.setattr(
        ingestion_main,
        "resolve_olist_date_range",
        lambda _: (date(2016, 9, 4), date(2018, 10, 17)),
    )
    monkeypatch.setattr(
        ingestion_main, "configure_google_application_credentials", lambda _: None
    )
    monkeypatch.setattr(ingestion_main, "create_bigquery_client", lambda **_: object())
    monkeypatch.setattr(ingestion_main, "run_olist_loaders", lambda *_, **__: [])
    monkeypatch.setattr(ingestion_main, "load_holidays", fake_load_holidays)

    exit_code = ingestion_main.main(["--use-olist-date-range", "--skip-weather"])

    assert exit_code == 0
    assert captured_dates == {
        "start_date": date(2016, 9, 4),
        "end_date": date(2018, 10, 17),
    }


def test_main_weather_over_budget_fails_before_api_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validate weather budget failures stop before any loader work.

    Args:
        monkeypatch: Pytest fixture for replacing collaborators.

    Returns:
        None.
    """
    monkeypatch.setenv("GCP_PROJECT_ID", "marketplace-prod")
    monkeypatch.setenv("BIGQUERY_LOCATION", "EU")
    monkeypatch.setenv("OPENWEATHER_API_KEY", "test-key")
    monkeypatch.setenv("OPENWEATHER_LATITUDE", "-23.5505")
    monkeypatch.setenv("OPENWEATHER_LONGITUDE", "-46.6333")
    monkeypatch.setattr(ingestion_main, "load_dotenv", lambda: None)
    monkeypatch.setattr(
        ingestion_main,
        "load_weather_daily",
        lambda *_, **__: pytest.fail("Weather loader should not run over budget"),
    )

    exit_code = ingestion_main.main(
        [
            "--skip-olist",
            "--skip-holidays",
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-03",
            "--openweather-max-calls",
            "2",
        ]
    )

    assert exit_code == 1


def test_main_bigquery_config_failure_stops_before_expensive_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validate BigQuery config validation happens before loader work.

    Args:
        monkeypatch: Pytest fixture for replacing collaborators.

    Returns:
        None.
    """
    monkeypatch.setenv("GCP_PROJECT_ID", "marketplace-prod")
    monkeypatch.setenv("BIGQUERY_LOCATION", "EU")
    monkeypatch.setattr(ingestion_main, "load_dotenv", lambda: None)
    monkeypatch.setattr(
        ingestion_main,
        "create_bigquery_client",
        lambda **_: (_ for _ in ()).throw(BigQueryConfigurationError("missing")),
    )
    monkeypatch.setattr(
        ingestion_main,
        "run_olist_loaders",
        lambda *_, **__: pytest.fail("Olist should not run after config failure"),
    )

    exit_code = ingestion_main.main(
        [
            "--skip-holidays",
            "--skip-weather",
        ]
    )

    assert exit_code == 1


def _write_result(table_id: str) -> BigQueryWriteResult:
    """Build a small BigQuery write result for orchestration tests."""
    return BigQueryWriteResult(
        table_id=table_id,
        write_mode="replace",
        job_id="job",
        input_rows=1,
        input_columns=1,
        loaded_rows=1,
    )
