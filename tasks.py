"""Cross-platform development task runner for MerchantPulse."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO
from collections.abc import Callable, Sequence

from dotenv import dotenv_values

REPO_ROOT = Path(__file__).resolve().parent
SETUP_DIRECTORIES = (
    Path("airflow/dags"),
    Path("ingestion/olist"),
    Path("ingestion/holidays"),
    Path("ingestion/weather"),
    Path("ingestion/utils"),
    Path("marketplace_analytics_dbt"),
    Path("docs"),
    Path("dashboards/screenshots"),
    Path("docker"),
    Path(".github/workflows"),
    Path("logs"),
    Path("data/olist"),
)
PRIMARY_COMMANDS = (
    "setup",
    "install",
    "install-orchestration",
    "lint",
    "format",
    "format-check",
    "test",
    "ingest",
    "dbt-debug",
    "dbt-parse",
    "dbt-freshness",
    "dbt-snapshot",
    "dbt-build",
    "dashboard-validate",
    "metabase-up",
    "metabase-down",
    "metabase-logs",
)


@dataclass(frozen=True)
class TaskSpec:
    """Runtime configuration for one repository task command."""

    description: str
    accepts_extra_args: bool
    handler: Callable[[Sequence[str], Path], int]


def _resolve_repo_root(repo_root: Path | None = None) -> Path:
    """Resolve the repository root for task execution."""
    return (repo_root or REPO_ROOT).resolve()


def _dbt_project_dir(repo_root: Path | None = None) -> Path:
    """Return the dbt project directory for command execution."""
    return _resolve_repo_root(repo_root) / "marketplace_analytics_dbt"


def _build_safe_environment(repo_root: Path | None = None) -> dict[str, str]:
    """Build a cross-platform subprocess environment for developer tools."""
    resolved_repo_root = _resolve_repo_root(repo_root)
    environment = os.environ.copy()
    # Inject project .env into subprocess environments so downstream tools
    # (dbt, pytest, ingestion CLI) see the same config without requiring the
    # shell to export each value. Shell values win over .env by convention.
    env_file = resolved_repo_root / ".env"
    if env_file.is_file():
        for env_key, env_value in dotenv_values(env_file).items():
            if env_value is not None and env_key not in environment:
                environment[env_key] = env_value
    # Resolve the real user home before we sandbox HOME below, so tools that
    # legitimately need the user config directory (e.g. dbt reading
    # ~/.dbt/profiles.yml) can still find it.
    real_user_home = Path.home()
    safe_home = str(resolved_repo_root)
    environment["HOME"] = safe_home
    environment["USERPROFILE"] = safe_home
    environment["BLACK_CACHE_DIR"] = str(resolved_repo_root / ".cache" / "black")
    # Pin dbt's profiles directory to the real user home so the sandboxed HOME
    # above does not hide ~/.dbt/profiles.yml from dbt subprocesses.
    environment.setdefault("DBT_PROFILES_DIR", str(real_user_home / ".dbt"))
    return environment


def _resolve_script_command(script_name: str) -> list[str]:
    """Resolve a console script from the active interpreter when possible."""
    executable_directory = Path(sys.executable).resolve().parent
    candidate_names = [script_name]
    if os.name == "nt":
        candidate_names.insert(0, f"{script_name}.exe")

    for candidate_name in candidate_names:
        candidate_path = executable_directory / candidate_name
        if candidate_path.exists():
            return [str(candidate_path)]

    return [script_name]


def _run_subprocess(
    command: Sequence[str],
    *,
    cwd: Path | None = None,
    repo_root: Path | None = None,
) -> int:
    """Run one subprocess and return its exit code without raising."""
    resolved_repo_root = _resolve_repo_root(repo_root)
    completed_process = subprocess.run(
        list(command),
        check=False,
        cwd=cwd or resolved_repo_root,
        env=_build_safe_environment(resolved_repo_root),
    )
    return completed_process.returncode


def _run_command_chain(
    commands: Sequence[tuple[Sequence[str], Path | None]],
    *,
    repo_root: Path | None = None,
) -> int:
    """Run a sequence of subprocess commands and stop on the first failure."""
    for command, working_directory in commands:
        exit_code = _run_subprocess(
            command,
            cwd=working_directory,
            repo_root=repo_root,
        )
        if exit_code != 0:
            return exit_code

    return 0


def _run_setup(_: Sequence[str], repo_root: Path) -> int:
    """Create starter directories and initialize `.env` from the template."""
    for relative_path in SETUP_DIRECTORIES:
        (repo_root / relative_path).mkdir(parents=True, exist_ok=True)

    env_example_path = repo_root / ".env.example"
    env_path = repo_root / ".env"
    if not env_example_path.is_file():
        print(
            f"Missing environment template: {env_example_path}",
            file=sys.stderr,
        )
        return 1

    if not env_path.exists():
        shutil.copyfile(env_example_path, env_path)

    return 0


def _run_install(_: Sequence[str], repo_root: Path) -> int:
    """Install the base runnable dependency set."""
    return _run_subprocess(
        [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"],
        repo_root=repo_root,
    )


def _run_install_orchestration(_: Sequence[str], repo_root: Path) -> int:
    """Install the optional orchestration dependency set."""
    return _run_subprocess(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "-r",
            "requirements-orchestration.txt",
        ],
        repo_root=repo_root,
    )


def _run_lint(_: Sequence[str], repo_root: Path) -> int:
    """Run repository lint checks."""
    return _run_command_chain(
        (
            ([sys.executable, "-m", "ruff", "check", "."], None),
            (
                [
                    sys.executable,
                    "-m",
                    "sqlfluff",
                    "lint",
                    "marketplace_analytics_dbt",
                    "--dialect",
                    "bigquery",
                ],
                None,
            ),
        ),
        repo_root=repo_root,
    )


def _run_format(_: Sequence[str], repo_root: Path) -> int:
    """Format Python sources used in the ingestion and test workflow."""
    return _run_subprocess(
        [sys.executable, "-m", "black", "ingestion", "tests", "tasks.py"],
        repo_root=repo_root,
    )


def _run_format_check(_: Sequence[str], repo_root: Path) -> int:
    """Check Python formatting without mutating files."""
    return _run_subprocess(
        [
            sys.executable,
            "-m",
            "black",
            "--check",
            "ingestion",
            "tests",
            "tasks.py",
        ],
        repo_root=repo_root,
    )


def _run_tests(_: Sequence[str], repo_root: Path) -> int:
    """Run the Python test suite."""
    return _run_subprocess(
        [sys.executable, "-m", "pytest", "-q"],
        repo_root=repo_root,
    )


def _run_ingest(arguments: Sequence[str], repo_root: Path) -> int:
    """Run the unified ingestion CLI with pass-through arguments."""
    return _run_subprocess(
        [sys.executable, "-m", "ingestion.main", *arguments],
        repo_root=repo_root,
    )


def _run_dbt_debug(arguments: Sequence[str], repo_root: Path) -> int:
    """Run `dbt debug` from the project root."""
    return _run_subprocess(
        [*_resolve_script_command("dbt"), "debug", *arguments],
        cwd=_dbt_project_dir(repo_root),
        repo_root=repo_root,
    )


def _run_dbt_parse(arguments: Sequence[str], repo_root: Path) -> int:
    """Run `dbt parse` from the project root."""
    return _run_subprocess(
        [*_resolve_script_command("dbt"), "parse", *arguments],
        cwd=_dbt_project_dir(repo_root),
        repo_root=repo_root,
    )


def _run_dbt_freshness(arguments: Sequence[str], repo_root: Path) -> int:
    """Run `dbt source freshness` from the project root."""
    return _run_subprocess(
        [*_resolve_script_command("dbt"), "source", "freshness", *arguments],
        cwd=_dbt_project_dir(repo_root),
        repo_root=repo_root,
    )


def _run_dbt_snapshot(arguments: Sequence[str], repo_root: Path) -> int:
    """Run `dbt snapshot` from the project root."""
    return _run_subprocess(
        [*_resolve_script_command("dbt"), "snapshot", *arguments],
        cwd=_dbt_project_dir(repo_root),
        repo_root=repo_root,
    )


def _run_dbt_build(arguments: Sequence[str], repo_root: Path) -> int:
    """Run `dbt build` from the project root."""
    return _run_subprocess(
        [*_resolve_script_command("dbt"), "build", *arguments],
        cwd=_dbt_project_dir(repo_root),
        repo_root=repo_root,
    )


def _run_dashboard_validate(arguments: Sequence[str], repo_root: Path) -> int:
    """Validate version-controlled dashboard assets against the dbt manifest."""
    return _run_subprocess(
        [sys.executable, "-m", "dashboards.validation", *arguments],
        repo_root=repo_root,
    )


def _run_metabase_up(_: Sequence[str], repo_root: Path) -> int:
    """Start the local Metabase runtime with Docker Compose."""
    return _run_subprocess(
        ["docker", "compose", "up", "-d"],
        repo_root=repo_root,
    )


def _run_metabase_down(_: Sequence[str], repo_root: Path) -> int:
    """Stop the local Metabase runtime with Docker Compose."""
    return _run_subprocess(
        ["docker", "compose", "down"],
        repo_root=repo_root,
    )


def _run_metabase_logs(arguments: Sequence[str], repo_root: Path) -> int:
    """Stream recent Metabase logs from Docker Compose."""
    command = ["docker", "compose", "logs"]
    if arguments:
        command.extend(arguments)
    else:
        command.extend(["--tail", "120", "metabase"])
    return _run_subprocess(command, repo_root=repo_root)


COMMAND_SPECS = {
    "setup": TaskSpec(
        description="Create starter directories and initialize .env",
        accepts_extra_args=False,
        handler=_run_setup,
    ),
    "install": TaskSpec(
        description="Install base runtime, dbt, and test dependencies",
        accepts_extra_args=False,
        handler=_run_install,
    ),
    "install-orchestration": TaskSpec(
        description="Install optional Airflow orchestration dependencies",
        accepts_extra_args=False,
        handler=_run_install_orchestration,
    ),
    "lint": TaskSpec(
        description="Run ruff and sqlfluff checks",
        accepts_extra_args=False,
        handler=_run_lint,
    ),
    "format": TaskSpec(
        description="Format Python sources used by ingestion and tests",
        accepts_extra_args=False,
        handler=_run_format,
    ),
    "format-check": TaskSpec(
        description="Check Python formatting without rewriting files",
        accepts_extra_args=False,
        handler=_run_format_check,
    ),
    "test": TaskSpec(
        description="Run the Python test suite",
        accepts_extra_args=False,
        handler=_run_tests,
    ),
    "ingest": TaskSpec(
        description="Run the unified ingestion CLI",
        accepts_extra_args=True,
        handler=_run_ingest,
    ),
    "dbt-debug": TaskSpec(
        description="Run dbt debug from marketplace_analytics_dbt",
        accepts_extra_args=True,
        handler=_run_dbt_debug,
    ),
    "dbt-parse": TaskSpec(
        description="Run dbt parse from marketplace_analytics_dbt",
        accepts_extra_args=True,
        handler=_run_dbt_parse,
    ),
    "dbt-freshness": TaskSpec(
        description="Run dbt source freshness from marketplace_analytics_dbt",
        accepts_extra_args=True,
        handler=_run_dbt_freshness,
    ),
    "dbt-snapshot": TaskSpec(
        description="Run dbt snapshot from marketplace_analytics_dbt",
        accepts_extra_args=True,
        handler=_run_dbt_snapshot,
    ),
    "dbt-build": TaskSpec(
        description="Run dbt build from marketplace_analytics_dbt",
        accepts_extra_args=True,
        handler=_run_dbt_build,
    ),
    "dashboard-validate": TaskSpec(
        description="Validate dashboard specs, SQL assets, and screenshots",
        accepts_extra_args=True,
        handler=_run_dashboard_validate,
    ),
    "metabase-up": TaskSpec(
        description="Start the local Metabase and PostgreSQL runtime",
        accepts_extra_args=False,
        handler=_run_metabase_up,
    ),
    "metabase-down": TaskSpec(
        description="Stop the local Metabase and PostgreSQL runtime",
        accepts_extra_args=False,
        handler=_run_metabase_down,
    ),
    "metabase-logs": TaskSpec(
        description="Show recent logs from the local Metabase runtime",
        accepts_extra_args=True,
        handler=_run_metabase_logs,
    ),
}


def _print_help(output_stream: TextIO = sys.stdout) -> None:
    """Print usage instructions for the repository task runner."""
    print("Usage: python tasks.py <command> [args]", file=output_stream)
    print("", file=output_stream)
    print("Primary commands:", file=output_stream)
    for command_name in PRIMARY_COMMANDS:
        spec = COMMAND_SPECS[command_name]
        print(f"  {command_name:<22} {spec.description}", file=output_stream)
    print("", file=output_stream)
    print(
        "Compatibility note: `make <target>` delegates to `python tasks.py`.",
        file=output_stream,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Dispatch one cross-platform repository task.

    Args:
        argv: Optional command-line arguments excluding the program name.

    Returns:
        Process exit code for the selected task.
    """
    command_line_arguments = list(sys.argv[1:] if argv is None else argv)
    if not command_line_arguments or command_line_arguments[0] in {
        "-h",
        "--help",
        "help",
    }:
        _print_help()
        return 0

    command_name, extra_arguments = (
        command_line_arguments[0],
        command_line_arguments[1:],
    )
    command_spec = COMMAND_SPECS.get(command_name)
    if command_spec is None:
        print(f"Unknown command: {command_name}", file=sys.stderr)
        _print_help(sys.stderr)
        return 2

    if extra_arguments and not command_spec.accepts_extra_args:
        print(
            f"Command does not accept extra arguments: {command_name}",
            file=sys.stderr,
        )
        return 2

    return command_spec.handler(extra_arguments, _resolve_repo_root())


if __name__ == "__main__":
    raise SystemExit(main())
