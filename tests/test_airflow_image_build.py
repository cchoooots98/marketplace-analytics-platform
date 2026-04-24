from pathlib import Path


def test_airflow_dockerfile_copies_dbt_metadata_with_airflow_ownership() -> None:
    """Keep dbt package metadata writable during image builds.

    Docker COPY defaults to root ownership, but the Apache Airflow base image
    runs build commands as the non-root ``airflow`` user. ``dbt deps`` may
    refresh ``package-lock.yml`` while resolving packages, so the copied dbt
    metadata must stay writable by ``airflow`` to keep image builds hermetic.
    """

    dockerfile_path = (
        Path(__file__).resolve().parents[1] / "docker" / "airflow" / "Dockerfile"
    )
    dockerfile_text = dockerfile_path.read_text(encoding="utf-8")

    for relative_path in (
        "marketplace_analytics_dbt/dbt_project.yml",
        "marketplace_analytics_dbt/packages.yml",
        "marketplace_analytics_dbt/package-lock.yml",
    ):
        expected_copy_line = (
            f"COPY --chown=airflow:0 {relative_path} /tmp/marketplace_analytics_dbt/"
        )
        assert expected_copy_line in dockerfile_text
