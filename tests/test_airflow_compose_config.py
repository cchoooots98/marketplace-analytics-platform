from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read_text(relative_path: str) -> str:
    """Read one repository text file for contract assertions."""
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def test_airflow_compose_uses_same_default_state_table_as_env_template() -> None:
    """Keep Airflow compose defaults aligned with the documented env template."""
    compose_text = _read_text("docker-compose.yml")
    env_example_text = _read_text(".env.example")

    assert "INGESTION_STATE_TABLE=ops.ingestion_batch_state" in env_example_text
    assert (
        "INGESTION_STATE_TABLE: ${INGESTION_STATE_TABLE:-ops.ingestion_batch_state}"
        in compose_text
    )


def test_airflow_compose_passes_required_ingestion_environment_variables() -> None:
    """Validate Airflow runtime services receive the required ingestion env vars."""
    compose_text = _read_text("docker-compose.yml")

    required_environment_entries = (
        "LOG_LEVEL: ${LOG_LEVEL:-INFO}",
        "OLIST_DATA_DIR: ${OLIST_DATA_DIR:-data/olist}",
        "OLIST_LANDING_DIR: ${OLIST_LANDING_DIR:-data/olist_landing}",
        "NAGER_COUNTRY_CODE: ${NAGER_COUNTRY_CODE:-BR}",
        "OPENWEATHER_LATITUDE: ${OPENWEATHER_LATITUDE:--23.5505}",
        "OPENWEATHER_LONGITUDE: ${OPENWEATHER_LONGITUDE:--46.6333}",
        "OPENWEATHER_UNITS: ${OPENWEATHER_UNITS:-metric}",
        "OPENWEATHER_LANG: ${OPENWEATHER_LANG:-en}",
        "OPENWEATHER_TIMEZONE_OFFSET: ${OPENWEATHER_TIMEZONE_OFFSET:--03:00}",
    )

    for entry in required_environment_entries:
        assert entry in compose_text
