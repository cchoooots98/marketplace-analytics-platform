import os

import pytest


def test_bigquery_connection_lists_datasets() -> None:
    """Validate that local credentials can reach BigQuery when configured.

    Returns:
        None.
    """
    project_id = os.getenv("GCP_PROJECT_ID")
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    if not project_id or not credentials_path:
        pytest.skip(
            "Set GCP_PROJECT_ID and GOOGLE_APPLICATION_CREDENTIALS to run this smoke test."
        )

    bigquery = pytest.importorskip(
        "google.cloud.bigquery",
        reason="Install google-cloud-bigquery to run this smoke test.",
    )
    client = bigquery.Client(project=project_id)
    datasets = list(client.list_datasets(max_results=1))

    assert isinstance(datasets, list)
