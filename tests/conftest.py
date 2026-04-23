"""Shared pytest fixtures and session setup for the repository test suite."""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_repo_dotenv_once() -> None:
    """Populate os.environ from the project .env before tests collect.

    Running ``pytest`` directly (without going through ``tasks.py``) leaves
    ``os.environ`` without the values developers typically keep in ``.env``.
    That causes integration smoke tests such as
    ``test_bigquery_connection_lists_datasets`` to skip even when credentials
    are configured. ``override=False`` preserves the industry convention that
    shell-exported values win over ``.env``.
    """
    env_file = REPO_ROOT / ".env"
    if env_file.is_file():
        load_dotenv(dotenv_path=env_file, override=False)


_load_repo_dotenv_once()
