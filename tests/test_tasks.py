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
        environment: dict[str, str] | None = None,
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


def test_dbt_commands_use_isolated_profile_wrapper(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Validate dbt task wrappers dispatch through the hermetic dbt helper.

    Args:
        monkeypatch: Pytest fixture for replacing subprocess execution.
        tmp_path: Pytest fixture providing a temporary repository root.

    Returns:
        None.
    """
    monkeypatch.setattr(tasks, "REPO_ROOT", tmp_path)
    captured_calls: list[tuple[tuple[str, ...], tuple[str, ...], Path]] = []

    def fake_run_dbt_subprocess(
        command: tuple[str, ...],
        *args: str,
        repo_root: Path,
    ) -> int:
        captured_calls.append((command, args, repo_root))
        return 0

    monkeypatch.setattr(tasks, "_run_dbt_subprocess", fake_run_dbt_subprocess)

    assert tasks.main(["dbt-debug"]) == 0
    assert tasks.main(["dbt-parse"]) == 0
    assert tasks.main(["dbt-freshness"]) == 0
    assert tasks.main(["dbt-snapshot"]) == 0
    assert tasks.main(["dbt-build"]) == 0

    assert captured_calls == [
        (("debug",), (), tmp_path),
        (("parse",), (), tmp_path),
        (("source", "freshness"), (), tmp_path),
        (("snapshot",), (), tmp_path),
        (("build",), (), tmp_path),
    ]


def test_dbt_build_passes_arguments_through_to_dbt_cli(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Validate dbt-build forwards extra CLI arguments unchanged.

    Args:
        monkeypatch: Pytest fixture for replacing subprocess execution.
        tmp_path: Pytest fixture providing a temporary repository root.

    Returns:
        None.
    """
    monkeypatch.setattr(tasks, "REPO_ROOT", tmp_path)
    captured_calls: list[tuple[tuple[str, ...], tuple[str, ...], Path]] = []

    def fake_run_dbt_subprocess(
        command: tuple[str, ...],
        *args: str,
        repo_root: Path,
    ) -> int:
        captured_calls.append((command, args, repo_root))
        return 0

    monkeypatch.setattr(tasks, "_run_dbt_subprocess", fake_run_dbt_subprocess)

    assert (
        tasks.main(
            [
                "dbt-build",
                "--select",
                "mart_exec_daily",
                "--target-path",
                "target_ci",
            ]
        )
        == 0
    )

    assert captured_calls == [
        (
            ("build",),
            ("--select", "mart_exec_daily", "--target-path", "target_ci"),
            tmp_path,
        ),
    ]


def test_dbt_deps_uses_isolated_profile_wrapper(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Validate dbt-deps dispatches through the hermetic dbt helper."""
    monkeypatch.setattr(tasks, "REPO_ROOT", tmp_path)
    captured_calls: list[tuple[tuple[str, ...], tuple[str, ...], Path]] = []

    def fake_run_dbt_subprocess(
        command: tuple[str, ...],
        *args: str,
        repo_root: Path,
    ) -> int:
        captured_calls.append((command, args, repo_root))
        return 0

    monkeypatch.setattr(tasks, "_run_dbt_subprocess", fake_run_dbt_subprocess)

    assert tasks.main(["dbt-deps"]) == 0

    assert captured_calls == [
        (("deps",), (), tmp_path),
    ]


def test_dbt_docs_generate_uses_isolated_profile_wrapper(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Validate dbt-docs-generate dispatches through the hermetic dbt helper."""
    monkeypatch.setattr(tasks, "REPO_ROOT", tmp_path)
    captured_calls: list[tuple[tuple[str, ...], tuple[str, ...], Path]] = []

    def fake_run_dbt_subprocess(
        command: tuple[str, ...],
        *args: str,
        repo_root: Path,
    ) -> int:
        captured_calls.append((command, args, repo_root))
        return 0

    monkeypatch.setattr(tasks, "_run_dbt_subprocess", fake_run_dbt_subprocess)

    assert tasks.main(["dbt-docs-generate", "--target-path", "target_validation"]) == 0

    assert captured_calls == [
        (("docs", "generate"), ("--target-path", "target_validation"), tmp_path),
    ]


def test_run_dbt_subprocess_adds_profiles_dir_and_cleans_artifacts(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Validate dbt subprocesses use a temp profile and always clean artifacts."""

    project_dir = tmp_path / "marketplace_analytics_dbt"
    project_dir.mkdir(parents=True)
    (project_dir / "target").mkdir()
    (project_dir / "target_ci").mkdir()
    (project_dir / "logs").mkdir()

    profile_dir = tmp_path / ".cache" / "profile"
    profile_dir.mkdir(parents=True)
    (profile_dir / "profiles.yml").write_text("profile: true\n", encoding="utf-8")
    monkeypatch.setattr(tasks, "_resolve_script_command", lambda _: ["dbt"])
    monkeypatch.setattr(
        tasks,
        "_create_temporary_dbt_profile_dir",
        lambda repo_root: profile_dir,
    )
    captured_call: dict[str, object] = {}

    def fake_run_subprocess(
        command: list[str],
        *,
        cwd: Path | None = None,
        repo_root: Path | None = None,
        environment: dict[str, str] | None = None,
    ) -> int:
        captured_call["command"] = command
        captured_call["cwd"] = cwd
        captured_call["environment"] = environment
        return 1

    monkeypatch.setattr(tasks, "_run_subprocess", fake_run_subprocess)

    exit_code = tasks._run_dbt_subprocess(
        ("build",), "--full-refresh", repo_root=tmp_path
    )

    assert exit_code == 1
    assert captured_call["command"] == [
        "dbt",
        "build",
        "--full-refresh",
        "--profiles-dir",
        str(profile_dir),
    ]
    assert captured_call["cwd"] == project_dir
    assert captured_call["environment"]["DBT_PACKAGES_INSTALL_PATH"] == "dbt_packages"
    assert not (project_dir / "target").exists()
    assert not (project_dir / "target_ci").exists()
    assert not (project_dir / "logs").exists()
    assert not profile_dir.exists()


def test_build_local_dbt_environment_rewrites_container_package_path(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Validate local dbt tasks do not inherit the container package path."""
    monkeypatch.setenv(
        "DBT_PACKAGES_INSTALL_PATH",
        tasks.DEFAULT_AIRFLOW_DBT_PACKAGES_INSTALL_PATH,
    )

    environment = tasks._build_local_dbt_environment(tmp_path)

    assert environment["DBT_PACKAGES_INSTALL_PATH"] == "dbt_packages"


def test_run_dbt_subprocess_caches_manifest_before_cleanup(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Validate dbt subprocesses persist manifest artifacts outside target dirs."""
    project_dir = tmp_path / "marketplace_analytics_dbt"
    project_dir.mkdir(parents=True)
    target_dir = project_dir / "target"
    target_dir.mkdir()
    (target_dir / "manifest.json").write_text("{}", encoding="utf-8")
    (target_dir / "catalog.json").write_text("{}", encoding="utf-8")
    (target_dir / "index.html").write_text("<html></html>", encoding="utf-8")

    profile_dir = tmp_path / ".cache" / "profile"
    profile_dir.mkdir(parents=True)
    (profile_dir / "profiles.yml").write_text("profile: true\n", encoding="utf-8")
    monkeypatch.setattr(tasks, "_resolve_script_command", lambda _: ["dbt"])
    monkeypatch.setattr(
        tasks,
        "_create_temporary_dbt_profile_dir",
        lambda repo_root: profile_dir,
    )
    monkeypatch.setattr(tasks, "_run_subprocess", lambda *args, **kwargs: 0)

    exit_code = tasks._run_dbt_subprocess(("build",), repo_root=tmp_path)

    assert exit_code == 0
    assert (tmp_path / tasks.DBT_MANIFEST_CACHE_PATH).read_text(
        encoding="utf-8"
    ) == "{}"
    assert (tmp_path / tasks.DBT_ARTIFACT_CACHE_DIR / "catalog.json").read_text(
        encoding="utf-8"
    ) == "{}"
    assert (tmp_path / tasks.DBT_ARTIFACT_CACHE_DIR / "index.html").read_text(
        encoding="utf-8"
    ) == "<html></html>"
    assert not target_dir.exists()


def test_dashboard_validate_command_uses_expected_wrapper(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Validate dashboard validation uses the intended CLI wrapper.

    Args:
        monkeypatch: Pytest fixture for replacing subprocess execution.
        tmp_path: Pytest fixture providing a temporary repository root.

    Returns:
        None.
    """
    monkeypatch.setattr(tasks, "REPO_ROOT", tmp_path)
    captured_calls: list[tuple[list[str], Path | None]] = []

    def fake_run_subprocess(
        command: list[str],
        *,
        cwd: Path | None = None,
        repo_root: Path | None = None,
        environment: dict[str, str] | None = None,
    ) -> int:
        captured_calls.append((command, cwd))
        return 0

    monkeypatch.setattr(tasks, "_run_subprocess", fake_run_subprocess)

    assert (
        tasks.main(["dashboard-validate", "--manifest", "target_ci/manifest.json"]) == 0
    )

    assert captured_calls == [
        (
            [
                sys.executable,
                "-m",
                "dashboards.validation",
                "--manifest",
                "target_ci/manifest.json",
            ],
            None,
        ),
    ]


def test_dashboard_validate_uses_cached_manifest_when_available(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Validate dashboard validation falls back to the cached dbt manifest."""
    monkeypatch.setattr(tasks, "REPO_ROOT", tmp_path)
    cached_manifest_path = tmp_path / tasks.DBT_MANIFEST_CACHE_PATH
    cached_manifest_path.parent.mkdir(parents=True)
    cached_manifest_path.write_text("{}", encoding="utf-8")
    captured_calls: list[list[str]] = []

    def fake_run_subprocess(
        command: list[str],
        *,
        cwd: Path | None = None,
        repo_root: Path | None = None,
        environment: dict[str, str] | None = None,
    ) -> int:
        captured_calls.append(command)
        return 0

    monkeypatch.setattr(tasks, "_run_subprocess", fake_run_subprocess)

    assert tasks.main(["dashboard-validate"]) == 0

    assert captured_calls == [
        [
            sys.executable,
            "-m",
            "dashboards.validation",
            "--manifest",
            tasks.DBT_MANIFEST_CACHE_PATH.as_posix(),
        ]
    ]


def test_metabase_up_command_uses_expected_wrapper(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Validate Metabase startup uses the intended Docker Compose wrapper.

    Args:
        monkeypatch: Pytest fixture for replacing subprocess execution.
        tmp_path: Pytest fixture providing a temporary repository root.

    Returns:
        None.
    """
    monkeypatch.setattr(tasks, "REPO_ROOT", tmp_path)
    captured_calls: list[tuple[list[str], Path | None]] = []

    def fake_run_subprocess(
        command: list[str],
        *,
        cwd: Path | None = None,
        repo_root: Path | None = None,
        environment: dict[str, str] | None = None,
    ) -> int:
        captured_calls.append((command, cwd))
        return 0

    monkeypatch.setattr(tasks, "_run_subprocess", fake_run_subprocess)

    assert tasks.main(["metabase-up"]) == 0

    assert captured_calls == [
        (
            ["docker", "compose", "up", "-d", "metabase-postgres", "metabase"],
            None,
        ),
    ]


def test_metabase_down_command_uses_expected_wrapper(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Validate Metabase shutdown uses the intended Docker Compose wrapper.

    Args:
        monkeypatch: Pytest fixture for replacing subprocess execution.
        tmp_path: Pytest fixture providing a temporary repository root.

    Returns:
        None.
    """
    monkeypatch.setattr(tasks, "REPO_ROOT", tmp_path)
    captured_calls: list[tuple[list[str], Path | None]] = []

    def fake_run_subprocess(
        command: list[str],
        *,
        cwd: Path | None = None,
        repo_root: Path | None = None,
        environment: dict[str, str] | None = None,
    ) -> int:
        captured_calls.append((command, cwd))
        return 0

    monkeypatch.setattr(tasks, "_run_subprocess", fake_run_subprocess)

    assert tasks.main(["metabase-down"]) == 0

    assert captured_calls == [
        (["docker", "compose", "stop", "metabase", "metabase-postgres"], None),
    ]


def test_metabase_logs_command_uses_expected_wrapper(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Validate Metabase logs use the intended Docker Compose wrapper.

    Args:
        monkeypatch: Pytest fixture for replacing subprocess execution.
        tmp_path: Pytest fixture providing a temporary repository root.

    Returns:
        None.
    """
    monkeypatch.setattr(tasks, "REPO_ROOT", tmp_path)
    captured_calls: list[tuple[list[str], Path | None]] = []

    def fake_run_subprocess(
        command: list[str],
        *,
        cwd: Path | None = None,
        repo_root: Path | None = None,
        environment: dict[str, str] | None = None,
    ) -> int:
        captured_calls.append((command, cwd))
        return 0

    monkeypatch.setattr(tasks, "_run_subprocess", fake_run_subprocess)

    assert tasks.main(["metabase-logs"]) == 0

    assert captured_calls == [
        (["docker", "compose", "logs", "--tail", "120", "metabase"], None),
    ]


def test_airflow_lifecycle_commands_use_expected_wrappers(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Validate Airflow Docker Compose wrappers target only Airflow services."""

    monkeypatch.setattr(tasks, "REPO_ROOT", tmp_path)
    captured_calls: list[tuple[list[str], Path | None]] = []

    def fake_run_subprocess(
        command: list[str],
        *,
        cwd: Path | None = None,
        repo_root: Path | None = None,
        environment: dict[str, str] | None = None,
    ) -> int:
        captured_calls.append((command, cwd))
        return 0

    monkeypatch.setattr(tasks, "_run_subprocess", fake_run_subprocess)

    assert tasks.main(["airflow-init"]) == 0
    assert tasks.main(["airflow-up"]) == 0
    assert tasks.main(["airflow-down"]) == 0
    assert tasks.main(["airflow-logs"]) == 0

    assert captured_calls == [
        (["docker", "compose", "up", "airflow-init"], None),
        (
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
            None,
        ),
        (
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
            None,
        ),
        (
            [
                "docker",
                "compose",
                "logs",
                "--tail",
                "120",
                "airflow-api-server",
                "airflow-scheduler",
                "airflow-dag-processor",
                "airflow-triggerer",
            ],
            None,
        ),
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
        environment: dict[str, str] | None = None,
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


def test_bootstrap_backfill_runs_expected_operator_flow(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Validate bootstrap-backfill runs the standard operator sequence."""
    monkeypatch.setattr(tasks, "REPO_ROOT", tmp_path)
    captured_calls: list[tuple[str, tuple[str, ...]]] = []

    def record_call(step_name: str):
        def _runner(arguments: list[str] | tuple[str, ...], repo_root: Path) -> int:
            captured_calls.append((step_name, tuple(arguments)))
            assert repo_root == tmp_path
            return 0

        return _runner

    monkeypatch.setattr(tasks, "_run_ingest", record_call("ingest"))
    monkeypatch.setattr(tasks, "_run_dbt_deps", record_call("deps"))
    monkeypatch.setattr(tasks, "_run_dbt_freshness", record_call("freshness"))
    monkeypatch.setattr(tasks, "_run_dbt_snapshot", record_call("snapshot"))
    monkeypatch.setattr(tasks, "_run_dbt_build", record_call("build"))
    monkeypatch.setattr(tasks, "_run_dbt_docs_generate", record_call("docs"))
    monkeypatch.setattr(tasks, "_run_dashboard_validate", record_call("dashboard"))

    assert tasks.main(["bootstrap-backfill", "--skip-weather"]) == 0

    assert captured_calls == [
        (
            "ingest",
            ("--mode", "bootstrap", "--use-olist-date-range", "--skip-weather"),
        ),
        ("deps", ()),
        ("freshness", ()),
        ("snapshot", ()),
        ("build", ("--full-refresh",)),
        ("docs", ()),
        ("dashboard", ()),
    ]


def test_daily_runtime_runs_expected_operator_flow(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Validate daily-runtime runs the standard incremental operator sequence."""
    monkeypatch.setattr(tasks, "REPO_ROOT", tmp_path)
    captured_calls: list[tuple[str, tuple[str, ...]]] = []

    def record_call(step_name: str):
        def _runner(arguments: list[str] | tuple[str, ...], repo_root: Path) -> int:
            captured_calls.append((step_name, tuple(arguments)))
            assert repo_root == tmp_path
            return 0

        return _runner

    monkeypatch.setattr(tasks, "_run_ingest", record_call("ingest"))
    monkeypatch.setattr(tasks, "_run_dbt_deps", record_call("deps"))
    monkeypatch.setattr(tasks, "_run_dbt_freshness", record_call("freshness"))
    monkeypatch.setattr(tasks, "_run_dbt_snapshot", record_call("snapshot"))
    monkeypatch.setattr(tasks, "_run_dbt_build", record_call("build"))
    monkeypatch.setattr(tasks, "_run_dbt_docs_generate", record_call("docs"))
    monkeypatch.setattr(tasks, "_run_dashboard_validate", record_call("dashboard"))

    assert tasks.main(["daily-runtime", "--skip-weather", "--skip-olist"]) == 0

    assert captured_calls == [
        ("ingest", ("--mode", "incremental", "--skip-weather", "--skip-olist")),
        ("deps", ()),
        ("freshness", ()),
        ("snapshot", ()),
        ("build", ()),
        ("docs", ()),
        ("dashboard", ()),
    ]
