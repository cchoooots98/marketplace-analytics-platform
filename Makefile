PYTHON ?= python
SAFE_HOME := $(CURDIR)

.PHONY: help setup install lint format format-check test ingest dbt-debug dbt-parse

help:
	@echo "Available commands:"
	@echo "  make setup      - create starter folders and copy .env"
	@echo "  make install    - install dependencies"
	@echo "  make lint       - run lint tools"
	@echo "  make format     - format Python files"
	@echo "  make format-check - check Python formatting"
	@echo "  make test       - run tests"
	@echo "  make ingest     - planned ingestion entrypoint placeholder"
	@echo "  make dbt-debug  - test dbt connection"
	@echo "  make dbt-parse  - parse dbt project"

setup:
	mkdir -p airflow/dags ingestion/olist ingestion/holidays ingestion/weather ingestion/utils marketplace_analytics_dbt docs dashboards/screenshots docker .github/workflows logs data/olist
	cp -n .env.example .env || true

install:
	$(PYTHON) -m pip install -r requirements.txt

lint:
	HOME="$(SAFE_HOME)" USERPROFILE="$(SAFE_HOME)" $(PYTHON) -m ruff check .
	HOME="$(SAFE_HOME)" USERPROFILE="$(SAFE_HOME)" $(PYTHON) -m sqlfluff lint marketplace_analytics_dbt --dialect bigquery

format:
	HOME="$(SAFE_HOME)" USERPROFILE="$(SAFE_HOME)" BLACK_CACHE_DIR="$(SAFE_HOME)/.cache/black" $(PYTHON) -m black ingestion tests

format-check:
	HOME="$(SAFE_HOME)" USERPROFILE="$(SAFE_HOME)" BLACK_CACHE_DIR="$(SAFE_HOME)/.cache/black" $(PYTHON) -m black --check ingestion tests

test:
	$(PYTHON) -m pytest -q

ingest:
	@echo "Ingestion entrypoint is planned. Implement ingestion/main.py before using this target."
	@exit 1

dbt-debug:
	cd marketplace_analytics_dbt && dbt debug

dbt-parse:
	cd marketplace_analytics_dbt && dbt parse
