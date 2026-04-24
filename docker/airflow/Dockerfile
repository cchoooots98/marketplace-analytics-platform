FROM apache/airflow:3.2.1-python3.11

COPY requirements.txt requirements-orchestration.txt /tmp/

RUN pip install --no-cache-dir --user -r /tmp/requirements-orchestration.txt

ENV PYTHONPATH=/opt/merchantpulse
ENV DBT_PACKAGES_INSTALL_PATH=/opt/airflow/dbt_packages

COPY --chown=airflow:0 marketplace_analytics_dbt/dbt_project.yml /tmp/marketplace_analytics_dbt/
COPY --chown=airflow:0 marketplace_analytics_dbt/packages.yml /tmp/marketplace_analytics_dbt/
COPY --chown=airflow:0 marketplace_analytics_dbt/package-lock.yml /tmp/marketplace_analytics_dbt/

RUN mkdir -p /opt/airflow/dbt_packages /tmp/dbt-profiles && \
    printf '%s\n' \
      'marketplace_analytics_dbt:' \
      '  target: build' \
      '  outputs:' \
      '    build:' \
      '      type: bigquery' \
      '      method: oauth' \
      '      project: build' \
      '      dataset: build' \
      '      threads: 1' \
      '      location: EU' \
      > /tmp/dbt-profiles/profiles.yml && \
    dbt deps --project-dir /tmp/marketplace_analytics_dbt --profiles-dir /tmp/dbt-profiles && \
    rm -rf /tmp/dbt-profiles /tmp/marketplace_analytics_dbt

WORKDIR /opt/merchantpulse
