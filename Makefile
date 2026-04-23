PYTHON ?= python

.PHONY: help setup install install-orchestration lint format format-check test ingest dbt-debug dbt-parse dbt-freshness dbt-snapshot dbt-build dashboard-validate metabase-up metabase-down metabase-logs

help:
	$(PYTHON) tasks.py --help

setup:
	$(PYTHON) tasks.py setup

install:
	$(PYTHON) tasks.py install

install-orchestration:
	$(PYTHON) tasks.py install-orchestration

lint:
	$(PYTHON) tasks.py lint

format:
	$(PYTHON) tasks.py format

format-check:
	$(PYTHON) tasks.py format-check

test:
	$(PYTHON) tasks.py test

ingest:
	$(PYTHON) tasks.py ingest

dbt-debug:
	$(PYTHON) tasks.py dbt-debug

dbt-parse:
	$(PYTHON) tasks.py dbt-parse

dbt-freshness:
	$(PYTHON) tasks.py dbt-freshness

dbt-snapshot:
	$(PYTHON) tasks.py dbt-snapshot

dbt-build:
	$(PYTHON) tasks.py dbt-build

dashboard-validate:
	$(PYTHON) tasks.py dashboard-validate

metabase-up:
	$(PYTHON) tasks.py metabase-up

metabase-down:
	$(PYTHON) tasks.py metabase-down

metabase-logs:
	$(PYTHON) tasks.py metabase-logs
