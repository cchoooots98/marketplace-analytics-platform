from datetime import date

import pandas as pd
import pytest
import requests

from ingestion.holidays import fetch_holidays
from ingestion.utils.bigquery_client import BigQueryWriteResult


class FakeResponse:
    """Small response double for requests tests."""

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
        """Return the configured JSON payload.

        Returns:
            Configured payload.
        """
        return self.payload


class FakeSession:
    """Small session double that returns responses in order."""

    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, int]] = []

    def get(self, url: str, *, timeout: int) -> FakeResponse:
        """Record a GET call and return the next fake response.

        Args:
            url: Requested URL.
            timeout: Timeout passed by the production function.

        Returns:
            Next fake response.
        """
        self.calls.append((url, timeout))
        return self.responses.pop(0)


class ManagedFakeSession(FakeSession):
    """Session double that tracks context manager ownership."""

    def __init__(self, responses: list[FakeResponse]) -> None:
        super().__init__(responses)
        self.entered = False
        self.exited = False

    def __enter__(self) -> "ManagedFakeSession":
        """Record that the owned session context was entered.

        Returns:
            The fake session instance.
        """
        self.entered = True
        return self

    def __exit__(
        self,
        exc_type: object,
        exc_value: object,
        traceback: object,
    ) -> None:
        """Record that the owned session context was exited.

        Args:
            exc_type: Exception type passed by the context manager protocol.
            exc_value: Exception value passed by the context manager protocol.
            traceback: Traceback passed by the context manager protocol.

        Returns:
            None.
        """
        self.exited = True


def test_fetch_public_holidays_returns_api_records() -> None:
    """Validate one Nager.Date country-year fetch.

    Returns:
        None.
    """
    session = FakeSession(
        [
            FakeResponse(
                [
                    {
                        "date": "2026-01-01",
                        "localName": "Confraternizacao Universal",
                        "name": "New Year's Day",
                        "countryCode": "BR",
                        "global": True,
                        "counties": None,
                        "launchYear": None,
                        "types": ["Public"],
                    }
                ]
            )
        ]
    )

    records = fetch_holidays.fetch_public_holidays(2026, "br", session=session)

    assert records[0]["countryCode"] == "BR"
    assert session.calls == [
        (
            "https://date.nager.at/api/v3/PublicHolidays/2026/BR",
            30,
        )
    ]


def test_fetch_public_holidays_without_session_uses_one_shot_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validate direct single-year calls do not create a managed session.

    Args:
        monkeypatch: Pytest fixture for replacing the requests module call.

    Returns:
        None.
    """
    calls: list[tuple[str, int]] = []

    def fake_get(url: str, *, timeout: int) -> FakeResponse:
        calls.append((url, timeout))
        return FakeResponse(
            [
                {
                    "date": "2026-01-01",
                    "countryCode": "BR",
                }
            ]
        )

    monkeypatch.setattr(fetch_holidays.requests, "get", fake_get)

    records = fetch_holidays.fetch_public_holidays(2026, "BR")

    assert records == [{"date": "2026-01-01", "countryCode": "BR"}]
    assert calls == [
        (
            "https://date.nager.at/api/v3/PublicHolidays/2026/BR",
            30,
        )
    ]


def test_fetch_holidays_for_date_range_closes_owned_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validate range fetches close sessions created for connection reuse.

    Args:
        monkeypatch: Pytest fixture for replacing the requests session factory.

    Returns:
        None.
    """
    managed_session = ManagedFakeSession(
        [
            FakeResponse(
                [
                    {
                        "date": "2026-01-01",
                        "countryCode": "BR",
                    }
                ]
            )
        ]
    )
    monkeypatch.setattr(fetch_holidays.requests, "Session", lambda: managed_session)

    records = fetch_holidays.fetch_holidays_for_date_range(
        date(2026, 1, 1),
        date(2026, 1, 1),
        "BR",
    )

    assert records == [{"date": "2026-01-01", "countryCode": "BR"}]
    assert managed_session.entered is True
    assert managed_session.exited is True


def test_fetch_holidays_for_date_range_spans_years_and_filters() -> None:
    """Validate yearly API responses are filtered to the requested date range.

    Returns:
        None.
    """
    session = FakeSession(
        [
            FakeResponse(
                [
                    {"date": "2025-12-25", "countryCode": "BR"},
                    {"date": "2025-12-31", "countryCode": "BR"},
                ]
            ),
            FakeResponse(
                [
                    {"date": "2026-01-01", "countryCode": "BR"},
                    {"date": "2026-04-21", "countryCode": "BR"},
                ]
            ),
        ]
    )

    records = fetch_holidays.fetch_holidays_for_date_range(
        date(2025, 12, 31),
        date(2026, 1, 1),
        "BR",
        session=session,
    )

    assert [record["date"] for record in records] == ["2025-12-31", "2026-01-01"]
    assert len(session.calls) == 2


def test_normalize_holidays_returns_expected_dataframe() -> None:
    """Validate Nager.Date records are normalized into raw table columns.

    Returns:
        None.
    """
    records = [
        {
            "date": "2026-01-01",
            "localName": "Confraternizacao Universal",
            "name": "New Year's Day",
            "countryCode": "BR",
            "global": True,
            "counties": None,
            "launchYear": None,
            "types": ["Public"],
        }
    ]

    holidays_dataframe = fetch_holidays.normalize_holidays(records)

    assert holidays_dataframe.to_dict("records") == [
        {
            "holiday_date": date(2026, 1, 1),
            "local_name": "Confraternizacao Universal",
            "holiday_name": "New Year's Day",
            "country_code": "BR",
            "is_global": True,
            "counties_json": None,
            "holiday_types_json": '["Public"]',
            "launch_year": None,
        }
    ]


def test_fetch_public_holidays_rejects_non_list_response() -> None:
    """Validate malformed Nager.Date payloads fail clearly.

    Returns:
        None.
    """
    session = FakeSession([FakeResponse({"date": "2026-01-01"})])

    with pytest.raises(ValueError, match="response must be a list"):
        fetch_holidays.fetch_public_holidays(2026, "BR", session=session)


def test_fetch_public_holidays_raises_http_errors() -> None:
    """Validate HTTP failures are raised with request context.

    Returns:
        None.
    """
    session = FakeSession(
        [FakeResponse({}, status_code=500, error=requests.HTTPError("server error"))]
    )

    with pytest.raises(requests.HTTPError):
        fetch_holidays.fetch_public_holidays(2026, "BR", session=session)


def test_validate_date_range_rejects_start_after_end() -> None:
    """Validate invalid date ranges fail before API calls.

    Returns:
        None.
    """
    with pytest.raises(ValueError, match="start_date"):
        fetch_holidays.validate_date_range(date(2026, 1, 2), date(2026, 1, 1))


def test_normalize_country_code_rejects_empty_code() -> None:
    """Validate empty country codes fail fast.

    Returns:
        None.
    """
    with pytest.raises(ValueError, match="two-letter"):
        fetch_holidays.normalize_country_code("")


def test_load_holidays_writes_to_raw_ext_holidays(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validate holiday loading writes the normalized data to raw_ext.holidays.

    Args:
        monkeypatch: Pytest fixture for replacing API and BigQuery collaborators.

    Returns:
        None.
    """
    captured_write: dict[str, object] = {}

    def fake_fetch_holidays_for_date_range(
        start_date: date,
        end_date: date,
        country_code: str,
        *,
        session: object | None,
    ) -> list[dict[str, object]]:
        return [
            {
                "date": "2026-01-01",
                "localName": "Confraternizacao Universal",
                "name": "New Year's Day",
                "countryCode": country_code,
                "global": True,
                "counties": None,
                "launchYear": None,
                "types": ["Public"],
            }
        ]

    def fake_write_dataframe_to_bigquery(
        dataframe: pd.DataFrame,
        table_id: str,
        *,
        write_mode: fetch_holidays.WriteMode,
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
            job_id="holiday_job",
            input_rows=len(dataframe.index),
            input_columns=len(dataframe.columns),
            loaded_rows=len(dataframe.index),
        )

    monkeypatch.setattr(
        fetch_holidays,
        "fetch_holidays_for_date_range",
        fake_fetch_holidays_for_date_range,
    )
    monkeypatch.setattr(
        fetch_holidays,
        "write_dataframe_to_bigquery",
        fake_write_dataframe_to_bigquery,
    )

    write_result = fetch_holidays.load_holidays(
        date(2026, 1, 1),
        date(2026, 12, 31),
        country_code="BR",
        project_id="marketplace-prod",
        location="EU",
    )

    written_dataframe = captured_write["dataframe"]
    assert isinstance(written_dataframe, pd.DataFrame)
    assert "batch_id" in written_dataframe.columns
    assert captured_write["table_id"] == "raw_ext.holidays"
    assert captured_write["write_mode"] == "replace"
    assert write_result.job_id == "holiday_job"
