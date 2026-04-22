import sys
from pathlib import Path

import tasks


def test_setup_creates_directories_and_preserves_existing_env(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Validate setup bootstraps the workspace without overwriting `.env`.

    Args:
        monkeypatch: Pytest fixture for replacing module constants.
        tmp_path: Pytest fixture providing a temporary repository root.

    Returns:
        None.
    """
    monkeypatch.setattr(tasks, "REPO_ROOT", tmp_path)
    (tmp_path / ".env.example").write_text("EXAMPLE=true\n", encoding="utf-8")

    first_exit_code = tasks.main(["setup"])

    assert first_exit_code == 0
    for relative_path in tasks.SETUP_DIRECTORIES:
        assert (tmp_path / relative_path).exists()
    assert (tmp_path / ".env").read_text(encoding="utf-8") == "EXAMPLE=true\n"

    (tmp_path / ".env").write_text("LOCAL_ONLY=true\n", encoding="utf-8")

    second_exit_code = tasks.main(["setup"])

    assert second_exit_code == 0
    assert (tmp_path / ".env").read_text(encoding="utf-8") == "LOCAL_ONLY=true\n"


def test_install_commands_use_expected_requirements_files(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Validate install commands point at the intended lockfiles.

    Args:
        monkeypatch: Pytest fixture for replacing subprocess execution.
        tmp_path: Pytest fixture providing a temporary repository root.

    Returns:
        None.
    """
    monkeypatch.setattr(tasks, "REPO_ROOT", tmp_path)
    captured_commands: list[list[str]] = []

    def fake_run_subprocess(
        command: list[str],
        *,
        cwd: Path | None = None,
        repo_root: Path | None = None,
    ) -> int:
        captured_commands.append(command)
        return 0

    monkeypatch.setattr(tasks, "_run_subprocess", fake_run_subprocess)

    assert tasks.main(["install"]) == 0
    assert tasks.main(["install-orchestration"]) == 0
    assert captured_commands == [
        [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"],
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "-r",
            "requirements-orchestration.txt",
        ],
    ]


def test_dbt_commands_use_project_working_directory(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Validate dbt task wrappers run inside the dbt project directory.

    Args:
        monkeypatch: Pytest fixture for replacing subprocess execution.
        tmp_path: Pytest fixture providing a temporary repository root.

    Returns:
        None.
    """
    monkeypatch.setattr(tasks, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(tasks, "_resolve_script_command", lambda _: ["dbt"])
    captured_calls: list[tuple[list[str], Path | None]] = []

    def fake_run_subprocess(
        command: list[str],
        *,
        cwd: Path | None = None,
        repo_root: Path | None = None,
    ) -> int:
        captured_calls.append((command, cwd))
        return 0

    monkeypatch.setattr(tasks, "_run_subprocess", fake_run_subprocess)

    assert tasks.main(["dbt-debug"]) == 0
    assert tasks.main(["dbt-parse"]) == 0
    assert tasks.main(["dbt-freshness"]) == 0
    assert tasks.main(["dbt-snapshot"]) == 0

    assert captured_calls == [
        (["dbt", "debug"], tmp_path / "marketplace_analytics_dbt"),
        (["dbt", "parse"], tmp_path / "marketplace_analytics_dbt"),
        (["dbt", "source", "freshness"], tmp_path / "marketplace_analytics_dbt"),
        (["dbt", "snapshot"], tmp_path / "marketplace_analytics_dbt"),
    ]


def test_ingest_command_passes_arguments_through_to_unified_cli(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Validate the ingest task forwards CLI arguments unchanged.

    Args:
        monkeypatch: Pytest fixture for replacing subprocess execution.
        tmp_path: Pytest fixture providing a temporary repository root.

    Returns:
        None.
    """
    monkeypatch.setattr(tasks, "REPO_ROOT", tmp_path)
    captured_command: dict[str, list[str]] = {}

    def fake_run_subprocess(
        command: list[str],
        *,
        cwd: Path | None = None,
        repo_root: Path | None = None,
    ) -> int:
        captured_command["command"] = command
        return 0

    monkeypatch.setattr(tasks, "_run_subprocess", fake_run_subprocess)

    exit_code = tasks.main(
        [
            "ingest",
            "--skip-weather",
            "--use-olist-date-range",
        ]
    )

    assert exit_code == 0
    assert captured_command["command"] == [
        sys.executable,
        "-m",
        "ingestion.main",
        "--skip-weather",
        "--use-olist-date-range",
    ]
