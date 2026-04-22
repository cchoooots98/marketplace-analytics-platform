import logging
from datetime import date

import pandas as pd
import pytest
import requests
from google.api_core.exceptions import GoogleAPIError

from ingestion.weather import fetch_weather_daily
from ingestion.utils.bigquery_client import BigQueryWriteResult


class FakeResponse:
    """Small response double for OpenWeather request tests."""

    def __init__(
        self,
        payload: object,
        *,
        status_code: int = 200,
        error: requests.HTTPError | None = None,
    ) -> None:
        self.payload = payload
        self.status_code = status_code
        self.error = error

    def raise_for_status(self) -> None:
        """Raise the configured HTTP error, if any.

        Returns:
            None.
        """
        if self.error is not None:
            self.error.response = self
            raise self.error

    def json(self) -> object:
        """Return the configured payload.

        Returns:
            Configured payload.
        """
        return self.payload


class FakeSession:
    """Small requests session double for OpenWeather tests."""

    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[dict[str, object]] = []

    def get(
        self,
        url: str,
        *,
        params: dict[str, object],
        timeout: int,
    ) -> FakeResponse:
        """Record a GET call and return the next response.

        Args:
            url: Requested URL.
            params: Request query parameters.
            timeout: Timeout passed by production code.

        Returns:
            Next fake response.
        """
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return self.responses.pop(0)

    def __enter__(self) -> "FakeSession":
        """Support context-manager use in production code."""
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        """Do nothing on context-manager exit for tests."""
        return None


def _weather_config(max_api_calls: int = 900) -> fetch_weather_daily.WeatherDailyConfig:
    """Build a valid weather config for tests."""
    return fetch_weather_daily.WeatherDailyConfig(
        api_key="test-key",
        latitude=-23.5505,
        longitude=-46.6333,
        location_key="sao_paulo",
        units="metric",
        lang="en",
        timezone_offset="-03:00",
        max_api_calls=max_api_calls,
    )


def _daily_payload(weather_date: str = "2026-01-01") -> dict[str, object]:
    """Build a minimal OpenWeather daily summary payload."""
    return {
        "lat": -23.5505,
        "lon": -46.6333,
        "tz": "-03:00",
        "date": weather_date,
        "units": "metric",
        "cloud_cover": {"afternoon": 42},
        "humidity": {"afternoon": 70},
        "precipitation": {"total": 1.2},
        "temperature": {
            "min": 18.0,
            "max": 28.0,
            "afternoon": 26.0,
            "night": 19.0,
            "evening": 24.0,
            "morning": 20.0,
        },
        "pressure": {"afternoon": 1012},
        "wind": {"max": {"speed": 6.2, "direction": 120}},
    }


def test_fetch_daily_weather_uses_day_summary_endpoint() -> None:
    """Validate OpenWeather daily aggregation endpoint and parameters.

    Returns:
        None.
    """
    session = FakeSession([FakeResponse(_daily_payload())])
    config = _weather_config()

    record = fetch_weather_daily.fetch_daily_weather(
        date(2026, 1, 1),
        config,
        session=session,
    )

    assert record["date"] == "2026-01-01"
    assert session.calls[0]["url"].endswith("/data/3.0/onecall/day_summary")
    assert session.calls[0]["params"]["date"] == "2026-01-01"
    assert "hourly" not in session.calls[0]["url"]


def test_fetch_daily_weather_without_session_uses_retry_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validate one-shot weather fetches use a retry-enabled managed session.

    Args:
        monkeypatch: Pytest fixture for replacing the retry session builder.

    Returns:
        None.
    """
    session = FakeSession([FakeResponse(_daily_payload())])
    monkeypatch.setattr(fetch_weather_daily, "build_retry_session", lambda: session)

    record = fetch_weather_daily.fetch_daily_weather(
        date(2026, 1, 1),
        _weather_config(),
    )

    assert record["date"] == "2026-01-01"
    assert session.calls[0]["params"]["date"] == "2026-01-01"


def test_normalize_daily_weather_returns_expected_row() -> None:
    """Validate OpenWeather payloads are normalized into raw table columns.

    Returns:
        None.
    """
    config = _weather_config()

    weather_dataframe = fetch_weather_daily.normalize_daily_weather(
        [_daily_payload()],
        config,
    )

    assert weather_dataframe.to_dict("records") == [
        {
            "weather_date": date(2026, 1, 1),
            "location_key": "sao_paulo",
            "latitude": -23.5505,
            "longitude": -46.6333,
            "timezone": "-03:00",
            "units": "metric",
            "cloud_cover_afternoon": 42,
            "humidity_afternoon": 70,
            "precipitation_total": 1.2,
            "temperature_min": 18.0,
            "temperature_max": 28.0,
            "temperature_afternoon": 26.0,
            "temperature_night": 19.0,
            "temperature_evening": 24.0,
            "temperature_morning": 20.0,
            "pressure_afternoon": 1012,
            "wind_max_speed": 6.2,
            "wind_max_direction": 120,
        }
    ]


def test_weather_budget_allows_olist_full_history_under_900_calls() -> None:
    """Validate the Olist full-history range fits the default weather budget.

    Returns:
        None.
    """
    requested_calls = fetch_weather_daily.calculate_weather_api_call_count(
        date(2016, 9, 4),
        date(2018, 10, 17),
    )

    assert requested_calls == 774
    fetch_weather_daily.validate_weather_api_budget(
        date(2016, 9, 4),
        date(2018, 10, 17),
        900,
    )


def test_weather_budget_rejects_ranges_above_max_calls() -> None:
    """Validate over-budget weather ranges fail before API calls.

    Returns:
        None.
    """
    with pytest.raises(ValueError, match="exceed budget"):
        fetch_weather_daily.validate_weather_api_budget(
            date(2026, 1, 1),
            date(2026, 1, 3),
            2,
        )


def test_weather_config_rejects_invalid_values() -> None:
    """Validate weather configuration fails fast on unsafe values.

    Returns:
        None.
    """
    with pytest.raises(ValueError, match="api_key"):
        fetch_weather_daily.WeatherDailyConfig(
            api_key="",
            latitude=-23.5505,
            longitude=-46.6333,
            location_key="sao_paulo",
            units="metric",
            lang="en",
            timezone_offset="-03:00",
            max_api_calls=900,
        )

    with pytest.raises(ValueError, match="latitude"):
        fetch_weather_daily.WeatherDailyConfig(
            api_key="test-key",
            latitude=-123,
            longitude=-46.6333,
            location_key="sao_paulo",
            units="metric",
            lang="en",
            timezone_offset="-03:00",
            max_api_calls=900,
        )


def test_fetch_daily_weather_raises_http_errors() -> None:
    """Validate OpenWeather HTTP failures are raised.

    Returns:
        None.
    """
    session = FakeSession(
        [FakeResponse({}, status_code=429, error=requests.HTTPError("too many"))]
    )

    with pytest.raises(requests.HTTPError):
        fetch_weather_daily.fetch_daily_weather(
            date(2026, 1, 1),
            _weather_config(),
            session=session,
        )


def test_build_retry_session_mounts_http_retry_adapter() -> None:
    """Validate the default weather HTTP session enables retry behavior.

    Returns:
        None.
    """
    session = fetch_weather_daily.build_retry_session()

    https_adapter = session.adapters["https://"]
    assert https_adapter.max_retries.total == 5
    assert https_adapter.max_retries.backoff_factor == 1.0

    session.close()


def test_load_weather_daily_writes_to_raw_ext_weather_daily(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validate weather loading writes normalized data to raw_ext.weather_daily.

    Args:
        monkeypatch: Pytest fixture for replacing API and BigQuery collaborators.

    Returns:
        None.
    """
    captured_write: dict[str, object] = {}

    def fake_fetch_weather_for_date_range(
        start_date: date,
        end_date: date,
        config: fetch_weather_daily.WeatherDailyConfig,
        *,
        session: object | None,
    ) -> list[dict[str, object]]:
        return [_daily_payload(start_date.isoformat())]

    def fake_write_dataframe_to_bigquery(
        dataframe: pd.DataFrame,
        table_id: str,
        *,
        write_mode: fetch_weather_daily.WriteMode,
        client: object | None,
        project_id: str | None,
        location: str | None,
    ) -> BigQueryWriteResult:
        captured_write["dataframe"] = dataframe
        captured_write["table_id"] = table_id
        captured_write["write_mode"] = write_mode
        captured_write["project_id"] = project_id
        captured_write["location"] = location
        return BigQueryWriteResult(
            table_id=table_id,
            write_mode=write_mode,
            job_id="weather_job",
            input_rows=len(dataframe.index),
            input_columns=len(dataframe.columns),
            loaded_rows=len(dataframe.index),
        )

    monkeypatch.setattr(
        fetch_weather_daily,
        "fetch_weather_for_date_range",
        fake_fetch_weather_for_date_range,
    )
    monkeypatch.setattr(
        fetch_weather_daily,
        "write_dataframe_to_bigquery",
        fake_write_dataframe_to_bigquery,
    )

    write_result = fetch_weather_daily.load_weather_daily(
        date(2026, 1, 1),
        date(2026, 1, 1),
        _weather_config(),
        project_id="marketplace-prod",
        location="EU",
    )

    written_dataframe = captured_write["dataframe"]
    assert isinstance(written_dataframe, pd.DataFrame)
    assert "batch_id" in written_dataframe.columns
    assert captured_write["table_id"] == "raw_ext.weather_daily"
    assert captured_write["write_mode"] == "replace"
    assert write_result.job_id == "weather_job"


def test_main_returns_one_on_google_api_error(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Validate CLI BigQuery runtime failures return exit code 1 with traceback.

    Args:
        monkeypatch: Pytest fixture for replacing runtime collaborators.
        caplog: Pytest fixture for capturing log records.

    Returns:
        None.
    """
    monkeypatch.setattr(fetch_weather_daily, "load_dotenv", lambda: None)
    monkeypatch.setattr(
        fetch_weather_daily,
        "configure_google_application_credentials",
        lambda _: None,
    )
    monkeypatch.setattr(
        fetch_weather_daily,
        "create_bigquery_client",
        lambda **_: object(),
    )

    def fake_load_weather_daily(*args: object, **kwargs: object) -> BigQueryWriteResult:
        raise GoogleAPIError("load failed")

    monkeypatch.setattr(
        fetch_weather_daily, "load_weather_daily", fake_load_weather_daily
    )

    with caplog.at_level(logging.ERROR):
        exit_code = fetch_weather_daily.main(
            [
                "--start-date",
                "2026-01-01",
                "--end-date",
                "2026-01-01",
                "--project-id",
                "marketplace-prod",
                "--location",
                "EU",
                "--api-key",
                "test-key",
                "--latitude",
                "-23.5505",
                "--longitude",
                "-46.6333",
            ]
        )

    assert exit_code == 1
    assert "Weather daily ingestion failed" in caplog.text
    assert caplog.records[-1].exc_info is not None
