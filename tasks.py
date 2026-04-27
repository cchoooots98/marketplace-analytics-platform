"""Cross-platform development task runner for MerchantPulse."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

from dotenv import dotenv_values

REPO_ROOT = Path(__file__).resolve().parent
DBT_ARTIFACT_CACHE_DIR = Path(".cache/dbt_artifacts")
DBT_MANIFEST_CACHE_PATH = DBT_ARTIFACT_CACHE_DIR / "manifest.json"
DBT_CACHED_ARTIFACT_NAMES = (
    "manifest.json",
    "catalog.json",
    "index.html",
)
WAREHOUSE_FRESHNESS_MODE_ENV = "WAREHOUSE_FRESHNESS_MODE"
WAREHOUSE_FRESHNESS_STATIC_MODE = "static"
WAREHOUSE_FRESHNESS_RUNTIME_MODE = "runtime"
VALID_WAREHOUSE_FRESHNESS_MODES = frozenset(
    {
        WAREHOUSE_FRESHNESS_STATIC_MODE,
        WAREHOUSE_FRESHNESS_RUNTIME_MODE,
    }
)
DEFAULT_LOCAL_DBT_PACKAGES_INSTALL_PATH = "dbt_packages"
DEFAULT_AIRFLOW_DBT_PACKAGES_INSTALL_PATH = "/opt/airflow/dbt_packages"
SETUP_DIRECTORIES = (
    Path("airflow/config"),
    Path("airflow/dags"),
    Path("airflow/logs"),
    Path("airflow/plugins"),
    Path("ingestion/olist"),
    Path("ingestion/holidays"),
    Path("ingestion/weather"),
    Path("ingestion/utils"),
    Path("marketplace_analytics_dbt"),
    Path("docs"),
    Path("dashboards/screenshots"),
    Path("docker"),
    Path("docker/airflow"),
    Path(".github/workflows"),
    Path("logs"),
    Path("data/olist"),
    Path("data/olist_landing"),
    Path("secrets"),
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
    "dbt-deps",
    "dbt-debug",
    "dbt-parse",
    "dbt-freshness",
    "dbt-snapshot",
    "dbt-build",
    "dbt-docs-generate",
    "bootstrap-backfill",
    "daily-runtime",
    "dashboard-validate",
    "airflow-init",
    "airflow-up",
    "airflow-down",
    "airflow-logs",
    "metabase-up",
    "metabase-down",
    "metabase-logs",
)
DBT_PROFILE_TEMPLATE = """marketplace_analytics_dbt:
  target: local
  outputs:
    local:
      type: bigquery
      method: service-account
      keyfile: "{{ env_var('GOOGLE_APPLICATION_CREDENTIALS') }}"
      project: "{{ env_var('GCP_PROJECT_ID') }}"
      dataset: "{{ env_var('BQ_STAGING_DATASET', 'staging') }}"
      threads: 4
      location: "{{ env_var('BIGQUERY_LOCATION', 'EU') }}"
      priority: interactive
"""


@dataclass(frozen=True)
class TaskSpec:
    """Runtime configuration for one repository task command."""

    description: str
    accepts_extra_args: bool
    handler: Callable[[Sequence[str], Path], int]


TaskHandler = Callable[[Sequence[str], Path], int]


def _resolve_repo_root(repo_root: Path | None = None) -> Path:
    """Resolve the repository root for task execution."""
    return (repo_root or REPO_ROOT).resolve()


def _dbt_project_dir(repo_root: Path | None = None) -> Path:
    """Return the dbt project directory for command execution."""
    return _resolve_repo_root(repo_root) / "marketplace_analytics_dbt"


def _dbt_artifact_cache_dir(repo_root: Path | None = None) -> Path:
    """Return the repository-local cache directory for persisted dbt artifacts."""
    return _resolve_repo_root(repo_root) / DBT_ARTIFACT_CACHE_DIR


def _create_temporary_dbt_profile_dir(repo_root: Path) -> Path:
    """Create a temporary dbt profiles directory inside `.cache`."""
    cache_root = repo_root / ".cache"
    cache_root.mkdir(parents=True, exist_ok=True)
    profile_dir = Path(
        tempfile.mkdtemp(
            prefix="dbt-profile-",
            dir=cache_root,
        )
    )
    (profile_dir / "profiles.yml").write_text(
        DBT_PROFILE_TEMPLATE,
        encoding="utf-8",
    )
    return profile_dir


def _cleanup_dbt_artifacts(project_dir: Path) -> None:
    """Delete dbt target and log directories after every dbt invocation."""
    for artifact_dir in project_dir.glob("target*"):
        if artifact_dir.is_dir():
            shutil.rmtree(artifact_dir, ignore_errors=True)

    logs_dir = project_dir / "logs"
    if logs_dir.is_dir():
        shutil.rmtree(logs_dir, ignore_errors=True)


def _latest_dbt_target_dir(project_dir: Path) -> Path | None:
    """Return the most recently modified dbt target directory, if present."""
    target_directories = [
        artifact_dir
        for artifact_dir in project_dir.glob("target*")
        if artifact_dir.is_dir()
    ]
    if not target_directories:
        return None

    return max(
        target_directories,
        key=lambda artifact_dir: artifact_dir.stat().st_mtime,
    )


def _cache_dbt_artifacts(project_dir: Path, repo_root: Path) -> None:
    """Copy dbt artifacts needed by downstream tasks into `.cache`."""
    latest_target_dir = _latest_dbt_target_dir(project_dir)
    if latest_target_dir is None:
        return

    cache_dir = _dbt_artifact_cache_dir(repo_root)
    cache_dir.mkdir(parents=True, exist_ok=True)
    for artifact_name in DBT_CACHED_ARTIFACT_NAMES:
        source_path = latest_target_dir / artifact_name
        if source_path.is_file():
            shutil.copyfile(source_path, cache_dir / artifact_name)


def _run_dbt_subprocess(
    command: Sequence[str],
    *args: str,
    repo_root: Path,
) -> int:
    """Run a dbt command with an isolated temporary profile and cleanup."""
    project_dir = _dbt_project_dir(repo_root)
    profile_dir = _create_temporary_dbt_profile_dir(repo_root)
    dbt_command = [
        *_resolve_script_command("dbt"),
        *command,
        *args,
        "--profiles-dir",
        str(profile_dir),
    ]
    try:
        return _run_subprocess(
            dbt_command,
            cwd=project_dir,
            repo_root=repo_root,
            environment=_build_local_dbt_environment(repo_root),
        )
    finally:
        _cache_dbt_artifacts(project_dir, repo_root)
        _cleanup_dbt_artifacts(project_dir)
        shutil.rmtree(profile_dir, ignore_errors=True)


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


def _build_local_dbt_environment(repo_root: Path) -> dict[str, str]:
    """Build a local dbt environment that avoids container-only package paths."""
    environment = _build_safe_environment(repo_root)
    packages_install_path = environment.get("DBT_PACKAGES_INSTALL_PATH")
    if (
        not packages_install_path
        or packages_install_path == DEFAULT_AIRFLOW_DBT_PACKAGES_INSTALL_PATH
    ):
        environment["DBT_PACKAGES_INSTALL_PATH"] = (
            DEFAULT_LOCAL_DBT_PACKAGES_INSTALL_PATH
        )

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
    environment: dict[str, str] | None = None,
) -> int:
    """Run one subprocess and return its exit code without raising."""
    resolved_repo_root = _resolve_repo_root(repo_root)
    completed_process = subprocess.run(
        list(command),
        check=False,
        cwd=cwd or resolved_repo_root,
        env=environment or _build_safe_environment(resolved_repo_root),
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


def _arguments_include_option(arguments: Sequence[str], option_name: str) -> bool:
    """Return whether one CLI argument list includes a flag or key-value option."""
    return any(
        argument == option_name or argument.startswith(f"{option_name}=")
        for argument in arguments
    )


def _run_task_flow(
    *,
    repo_root: Path,
    steps: Sequence[tuple[str, TaskHandler, Sequence[str]]],
) -> int:
    """Run a named sequence of repository task handlers in order."""
    for step_name, step_handler, step_arguments in steps:
        print(f"==> {step_name}")
        exit_code = step_handler(step_arguments, repo_root)
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
    return _run_dbt_subprocess(("debug",), *arguments, repo_root=repo_root)


def _run_dbt_deps(arguments: Sequence[str], repo_root: Path) -> int:
    """Run `dbt deps` from the project root."""
    return _run_dbt_subprocess(("deps",), *arguments, repo_root=repo_root)


def _run_dbt_parse(arguments: Sequence[str], repo_root: Path) -> int:
    """Run `dbt parse` from the project root."""
    return _run_dbt_subprocess(("parse",), *arguments, repo_root=repo_root)


def _run_dbt_freshness(arguments: Sequence[str], repo_root: Path) -> int:
    """Run `dbt source freshness` from the project root."""
    freshness_mode = (
        os.getenv(
            WAREHOUSE_FRESHNESS_MODE_ENV,
            WAREHOUSE_FRESHNESS_STATIC_MODE,
        )
        .strip()
        .lower()
    )
    if freshness_mode not in VALID_WAREHOUSE_FRESHNESS_MODES:
        print(
            f"{WAREHOUSE_FRESHNESS_MODE_ENV} must be one of "
            f"{sorted(VALID_WAREHOUSE_FRESHNESS_MODES)}; got {freshness_mode!r}.",
            file=sys.stderr,
        )
        return 2

    if freshness_mode == WAREHOUSE_FRESHNESS_STATIC_MODE:
        print(
            "Skipping dbt source freshness because "
            f"{WAREHOUSE_FRESHNESS_MODE_ENV}=static. Static backfill mode is "
            "validated by dbt structural, coverage, and reconciliation tests."
        )
        return 0

    return _run_dbt_subprocess(
        ("source", "freshness"),
        *arguments,
        repo_root=repo_root,
    )


def _run_dbt_snapshot(arguments: Sequence[str], repo_root: Path) -> int:
    """Run `dbt snapshot` from the project root."""
    return _run_dbt_subprocess(("snapshot",), *arguments, repo_root=repo_root)


def _run_dbt_build(arguments: Sequence[str], repo_root: Path) -> int:
    """Run `dbt build` from the project root."""
    return _run_dbt_subprocess(("build",), *arguments, repo_root=repo_root)


def _run_dbt_docs_generate(arguments: Sequence[str], repo_root: Path) -> int:
    """Run `dbt docs generate` from the project root."""
    return _run_dbt_subprocess(
        ("docs", "generate"),
        *arguments,
        repo_root=repo_root,
    )


def _run_dashboard_validate(arguments: Sequence[str], repo_root: Path) -> int:
    """Validate version-controlled dashboard assets against the dbt manifest."""
    resolved_arguments = list(arguments)
    cached_manifest_path = _dbt_artifact_cache_dir(repo_root) / "manifest.json"
    if (
        not _arguments_include_option(resolved_arguments, "--manifest")
        and cached_manifest_path.is_file()
    ):
        resolved_arguments = [
            "--manifest",
            DBT_MANIFEST_CACHE_PATH.as_posix(),
            *resolved_arguments,
        ]

    return _run_subprocess(
        [sys.executable, "-m", "dashboards.validation", *resolved_arguments],
        repo_root=repo_root,
    )


def _normalize_bootstrap_ingestion_arguments(
    arguments: Sequence[str],
) -> list[str] | None:
    """Return bootstrap ingestion arguments with the standard date-range default."""
    if _arguments_include_option(arguments, "--mode"):
        print(
            "bootstrap-backfill manages --mode automatically; remove --mode from the command.",
            file=sys.stderr,
        )
        return None

    normalized_arguments = list(arguments)
    if not any(
        _arguments_include_option(normalized_arguments, option_name)
        for option_name in (
            "--use-olist-date-range",
            "--start-date",
            "--end-date",
        )
    ):
        normalized_arguments.insert(0, "--use-olist-date-range")

    return ["--mode", "bootstrap", *normalized_arguments]


def _normalize_incremental_ingestion_arguments(
    arguments: Sequence[str],
) -> list[str] | None:
    """Return incremental ingestion arguments with the fixed runtime mode."""
    if _arguments_include_option(arguments, "--mode"):
        print(
            "daily-runtime manages --mode automatically; remove --mode from the command.",
            file=sys.stderr,
        )
        return None

    return ["--mode", "incremental", *arguments]


def _run_bootstrap_backfill(arguments: Sequence[str], repo_root: Path) -> int:
    """Run the full historical operator flow from ingestion through validation."""
    normalized_ingestion_arguments = _normalize_bootstrap_ingestion_arguments(arguments)
    if normalized_ingestion_arguments is None:
        return 2

    return _run_task_flow(
        repo_root=repo_root,
        steps=(
            (
                "Run bootstrap ingestion",
                _run_ingest,
                tuple(normalized_ingestion_arguments),
            ),
            ("Install dbt package dependencies", _run_dbt_deps, ()),
            ("Run dbt source freshness", _run_dbt_freshness, ()),
            ("Run dbt snapshots", _run_dbt_snapshot, ()),
            ("Run dbt build", _run_dbt_build, ("--full-refresh",)),
            ("Generate dbt docs artifacts", _run_dbt_docs_generate, ()),
            ("Validate dashboard package", _run_dashboard_validate, ()),
        ),
    )


def _run_daily_runtime(arguments: Sequence[str], repo_root: Path) -> int:
    """Run the incremental operator flow from ingestion through validation."""
    normalized_ingestion_arguments = _normalize_incremental_ingestion_arguments(
        arguments
    )
    if normalized_ingestion_arguments is None:
        return 2

    return _run_task_flow(
        repo_root=repo_root,
        steps=(
            (
                "Run incremental ingestion",
                _run_ingest,
                tuple(normalized_ingestion_arguments),
            ),
            ("Install dbt package dependencies", _run_dbt_deps, ()),
            ("Run dbt source freshness", _run_dbt_freshness, ()),
            ("Run dbt snapshots", _run_dbt_snapshot, ()),
            ("Run dbt build", _run_dbt_build, ()),
            ("Generate dbt docs artifacts", _run_dbt_docs_generate, ()),
            ("Validate dashboard package", _run_dashboard_validate, ()),
        ),
    )


def _run_metabase_up(_: Sequence[str], repo_root: Path) -> int:
    """Start the local Metabase runtime with Docker Compose."""
    return _run_subprocess(
        ["docker", "compose", "up", "-d", "metabase-postgres", "metabase"],
        repo_root=repo_root,
    )


def _run_metabase_down(_: Sequence[str], repo_root: Path) -> int:
    """Stop the local Metabase runtime with Docker Compose."""
    return _run_subprocess(
        ["docker", "compose", "stop", "metabase", "metabase-postgres"],
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


def _run_airflow_init(_: Sequence[str], repo_root: Path) -> int:
    """Initialize the local Airflow metadata database and admin account."""
    return _run_subprocess(
        ["docker", "compose", "up", "airflow-init"],
        repo_root=repo_root,
    )


def _run_airflow_up(_: Sequence[str], repo_root: Path) -> int:
    """Start the local Airflow runtime with Docker Compose."""
    return _run_subprocess(
        [
            "docker",
            "compose",
            "up",
            "-d",
            "airflow-postgres",
            "airflow-api-server",
            "airflow-scheduler",
            "airflow-dag-processor",
            "airflow-triggerer",
        ],
        repo_root=repo_root,
    )


def _run_airflow_down(_: Sequence[str], repo_root: Path) -> int:
    """Stop the local Airflow runtime without affecting Metabase services."""
    return _run_subprocess(
        [
            "docker",
            "compose",
            "stop",
            "airflow-api-server",
            "airflow-scheduler",
            "airflow-dag-processor",
            "airflow-triggerer",
            "airflow-postgres",
        ],
        repo_root=repo_root,
    )


def _run_airflow_logs(arguments: Sequence[str], repo_root: Path) -> int:
    """Stream recent logs from the local Airflow runtime."""
    command = ["docker", "compose", "logs"]
    if arguments:
        command.extend(arguments)
    else:
        command.extend(
            [
                "--tail",
                "120",
                "airflow-api-server",
                "airflow-scheduler",
                "airflow-dag-processor",
                "airflow-triggerer",
            ]
        )
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
    "dbt-deps": TaskSpec(
        description="Run dbt deps from marketplace_analytics_dbt",
        accepts_extra_args=True,
        handler=_run_dbt_deps,
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
    "dbt-docs-generate": TaskSpec(
        description="Run dbt docs generate from marketplace_analytics_dbt",
        accepts_extra_args=True,
        handler=_run_dbt_docs_generate,
    ),
    "bootstrap-backfill": TaskSpec(
        description="Run bootstrap ingestion plus dbt validation and dashboard checks",
        accepts_extra_args=True,
        handler=_run_bootstrap_backfill,
    ),
    "daily-runtime": TaskSpec(
        description="Run incremental ingestion plus dbt validation and dashboard checks",
        accepts_extra_args=True,
        handler=_run_daily_runtime,
    ),
    "dashboard-validate": TaskSpec(
        description="Validate dashboard specs, SQL assets, and screenshots",
        accepts_extra_args=True,
        handler=_run_dashboard_validate,
    ),
    "airflow-init": TaskSpec(
        description="Initialize the local Airflow metadata database and admin user",
        accepts_extra_args=False,
        handler=_run_airflow_init,
    ),
    "airflow-up": TaskSpec(
        description="Start the local Airflow runtime services",
        accepts_extra_args=False,
        handler=_run_airflow_up,
    ),
    "airflow-down": TaskSpec(
        description="Stop the local Airflow runtime services",
        accepts_extra_args=False,
        handler=_run_airflow_down,
    ),
    "airflow-logs": TaskSpec(
        description="Show recent logs from the local Airflow runtime",
        accepts_extra_args=True,
        handler=_run_airflow_logs,
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
