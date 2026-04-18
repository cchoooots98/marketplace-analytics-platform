import pandas as pd
import pytest

from ingestion.olist import load_orders
from ingestion.olist.raw_csv_loader import OlistRawTableSpec


def test_prepare_orders_dataframe_delegates_to_shared_core(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validate the orders compatibility wrapper uses the orders spec.

    Args:
        monkeypatch: Pytest fixture for replacing shared preparation logic.

    Returns:
        None.
    """
    captured_call: dict[str, object] = {}
    expected_dataframe = pd.DataFrame({"order_id": ["order_1"]})

    def fake_prepare_raw_dataframe(
        csv_path: str,
        spec: OlistRawTableSpec,
        *,
        metadata: object | None = None,
    ) -> pd.DataFrame:
        captured_call["csv_path"] = csv_path
        captured_call["spec"] = spec
        captured_call["metadata"] = metadata
        return expected_dataframe

    monkeypatch.setattr(
        load_orders, "prepare_raw_dataframe", fake_prepare_raw_dataframe
    )

    prepared_dataframe = load_orders.prepare_orders_dataframe("orders.csv")

    assert prepared_dataframe is expected_dataframe
    assert captured_call == {
        "csv_path": "orders.csv",
        "spec": load_orders.ORDERS_SPEC,
        "metadata": None,
    }


def test_main_delegates_to_shared_cli_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    """Validate the orders module keeps a table-specific CLI entrypoint.

    Args:
        monkeypatch: Pytest fixture for replacing the shared CLI runner.

    Returns:
        None.
    """
    captured_call: dict[str, object] = {}

    def fake_run_olist_loader(
        argv: list[str],
        spec: OlistRawTableSpec,
    ) -> int:
        captured_call["argv"] = argv
        captured_call["spec"] = spec
        return 0

    monkeypatch.setattr(load_orders, "run_olist_loader", fake_run_olist_loader)

    exit_code = load_orders.main(["data/olist/olist_orders_dataset.csv"])

    assert exit_code == 0
    assert captured_call == {
        "argv": ["data/olist/olist_orders_dataset.csv"],
        "spec": load_orders.ORDERS_SPEC,
    }
