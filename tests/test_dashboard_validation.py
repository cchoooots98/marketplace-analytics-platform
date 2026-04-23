import json
from pathlib import Path

import pytest

from dashboards.validation import (
    DEFAULT_MANIFEST_PATH,
    _extract_sql_output_columns,
    _find_top_level_keyword,
    _resolve_manifest_path,
    _split_top_level_csv,
    validate_dashboard_assets,
)


DEFAULT_NODES = {
    "model.marketplace_analytics_dbt.mart_exec_daily": {
        "resource_type": "model",
        "name": "mart_exec_daily",
        "columns": {
            "calendar_date": {},
            "gmv": {},
            "orders_count": {},
            "cancelled_orders_count": {},
            "late_orders_count": {},
            "delivered_orders_count": {},
            "non_cancelled_orders_count": {},
            "reviews_count": {},
        },
    },
    "model.marketplace_analytics_dbt.dim_date": {
        "resource_type": "model",
        "name": "dim_date",
        "columns": {
            "calendar_date": {},
        },
    },
}

DEFAULT_EXPOSURES = {
    "exposure.marketplace_analytics_dbt.executive_overview_dashboard": {
        "name": "executive_overview_dashboard",
        "depends_on": {
            "nodes": [
                "model.marketplace_analytics_dbt.mart_exec_daily",
            ]
        },
    }
}


def _write_manifest(
    manifest_path: Path,
    *,
    nodes: dict[str, object] | None = None,
    exposures: dict[str, object] | None = None,
) -> None:
    manifest = {
        "nodes": nodes or DEFAULT_NODES,
        "exposures": exposures if exposures is not None else DEFAULT_EXPOSURES,
    }
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")


def _write_spec(
    spec_path: Path,
    *,
    sql_path: str = "dashboards/sql/core_trio/executive/exec_gmv.sql",
    screenshot_path: str = "dashboards/screenshots/executive.svg",
    output_columns: list[str] | None = None,
    dependency_columns: list[dict[str, str]] | None = None,
    derived_columns: list[dict[str, object]] | None = None,
    filter_names: list[str] | None = None,
    filters: list[dict[str, str]] | None = None,
    allowed_join_models: list[str] | None = None,
) -> None:
    card_spec = {
        "title": "GMV",
        "sql_path": sql_path,
        "source_model": "mart_exec_daily",
        "allowed_join_models": allowed_join_models or [],
        "output_columns": output_columns or ["gmv"],
        "dependency_columns": dependency_columns
        or [
            {
                "name": "calendar_date",
                "source_model": "mart_exec_daily",
            },
            {
                "name": "gmv",
                "source_model": "mart_exec_daily",
            },
        ],
        "filter_names": filter_names or ["date_range"],
    }
    if derived_columns is not None:
        card_spec["derived_columns"] = derived_columns

    spec = {
        "dashboards": [
            {
                "name": "Executive Overview",
                "exposure_name": "executive_overview_dashboard",
                "primary_model": "mart_exec_daily",
                "screenshot_path": screenshot_path,
                "filters": filters
                or [
                    {
                        "name": "date_range",
                        "variable": "date_range",
                        "field_name": "calendar_date",
                        "source_model": "mart_exec_daily",
                    }
                ],
                "cards": [card_spec],
            }
        ]
    }
    spec_path.write_text(json.dumps(spec), encoding="utf-8")


def _validate_tmp_assets(
    tmp_path: Path,
    *,
    sql_text: str,
    spec_kwargs: dict[str, object] | None = None,
    manifest_kwargs: dict[str, object] | None = None,
) -> list[str]:
    manifest_path = tmp_path / "manifest.json"
    spec_path = tmp_path / "dashboards" / "specs" / "core_trio.json"
    sql_path = (
        tmp_path / "dashboards" / "sql" / "core_trio" / "executive" / "exec_gmv.sql"
    )
    screenshot_path = tmp_path / "dashboards" / "screenshots" / "executive.svg"

    spec_path.parent.mkdir(parents=True)
    sql_path.parent.mkdir(parents=True)
    screenshot_path.parent.mkdir(parents=True)
    _write_manifest(manifest_path, **(manifest_kwargs or {}))
    _write_spec(spec_path, **(spec_kwargs or {}))
    sql_path.write_text(sql_text, encoding="utf-8")
    screenshot_path.write_text("<svg></svg>", encoding="utf-8")

    return validate_dashboard_assets(
        repo_root=tmp_path,
        spec_path=spec_path,
        manifest_path=manifest_path,
    )


def test_dashboard_validation_passes_for_valid_assets(tmp_path: Path) -> None:
    """Validate the happy path for one minimal dashboard contract."""
    errors = _validate_tmp_assets(
        tmp_path,
        sql_text=(
            "select sum(gmv) as gmv from `marts.mart_exec_daily` "
            "where 1 = 1 [[and {{date_range}}]]"
        ),
    )

    assert errors == []


def test_find_top_level_keyword_skips_cte_inner_selects() -> None:
    """Validate keyword scans ignore nested selects inside CTE bodies."""
    sql_text = """
        with ranked as (
            select
                seller_id,
                row_number() over (
                    partition by seller_id
                    order by calendar_date
                ) as row_num
            from `marts.mart_seller_performance`
        )
        select seller_id, row_num
        from ranked
    """

    select_index = _find_top_level_keyword(sql_text, "select")

    assert select_index >= 0
    assert (
        sql_text[select_index:].lstrip().lower().startswith("select seller_id, row_num")
    )


def test_split_top_level_csv_keeps_nested_function_arguments_together() -> None:
    """Validate commas inside function calls do not split select expressions."""
    select_clause = """
        concat(seller_city, ", ", seller_state) as seller_label,
        safe_divide(sum(gmv), sum(orders_count)) as aov,
        calendar_date
    """

    expressions = _split_top_level_csv(select_clause)

    assert expressions == [
        'concat(seller_city, ", ", seller_state) as seller_label',
        "safe_divide(sum(gmv), sum(orders_count)) as aov",
        "calendar_date",
    ]


def test_extract_sql_output_columns_supports_cte_and_window_function() -> None:
    """Validate output-column parsing survives common analytics SQL shapes."""
    sql_text = """
        with ranked as (
            select
                seller_id,
                row_number() over (
                    partition by seller_id
                    order by calendar_date
                ) as row_num
            from `marts.mart_seller_performance`
        )
        select
            seller_id,
            row_num
        from ranked
    """

    assert _extract_sql_output_columns(sql_text) == ["seller_id", "row_num"]


def test_extract_sql_output_columns_rejects_top_level_union() -> None:
    """Validate set operators fail fast instead of producing silent drift."""
    sql_text = """
        select seller_id
        from `marts.mart_seller_performance`
        union all
        select seller_id
        from `marts.dim_seller`
    """

    with pytest.raises(ValueError, match="set operators"):
        _extract_sql_output_columns(sql_text)


def test_resolve_manifest_path_prefers_ci_fallback_when_default_is_missing(
    tmp_path: Path,
) -> None:
    """Validate the CLI can reuse a CI manifest when the local default is absent."""
    ci_manifest_path = (
        tmp_path / "marketplace_analytics_dbt" / "target_ci" / "manifest.json"
    )
    ci_manifest_path.parent.mkdir(parents=True)
    ci_manifest_path.write_text("{}", encoding="utf-8")

    resolved_manifest_path = _resolve_manifest_path(tmp_path, DEFAULT_MANIFEST_PATH)

    assert resolved_manifest_path == ci_manifest_path


def test_dashboard_validation_passes_for_allowed_dim_date_join(
    tmp_path: Path,
) -> None:
    """Validate approved helper dimensions may join the primary mart."""
    errors = _validate_tmp_assets(
        tmp_path,
        sql_text=(
            "select mart.calendar_date as calendar_date, mart.gmv as gmv "
            "from `marts.mart_exec_daily` as mart "
            "left join `marts.dim_date` as dim_date "
            "on mart.calendar_date = dim_date.calendar_date "
            "where 1 = 1 [[and {{date_range}}]]"
        ),
        spec_kwargs={
            "allowed_join_models": ["dim_date"],
            "output_columns": ["calendar_date", "gmv"],
            "dependency_columns": [
                {
                    "name": "calendar_date",
                    "source_model": "mart_exec_daily",
                },
                {
                    "name": "gmv",
                    "source_model": "mart_exec_daily",
                },
                {
                    "name": "calendar_date",
                    "source_model": "dim_date",
                },
            ],
        },
    )

    assert errors == []


def test_dashboard_validation_fails_for_missing_dependency_column(
    tmp_path: Path,
) -> None:
    """Validate missing mart dependencies fail the dashboard contract."""
    errors = _validate_tmp_assets(
        tmp_path,
        sql_text=(
            "select sum(gmv) as gmv from `marts.mart_exec_daily` "
            "where 1 = 1 [[and {{date_range}}]]"
        ),
        spec_kwargs={
            "dependency_columns": [
                {
                    "name": "calendar_date",
                    "source_model": "mart_exec_daily",
                },
                {
                    "name": "missing_metric",
                    "source_model": "mart_exec_daily",
                },
            ]
        },
    )

    assert any("missing_metric" in error for error in errors)


def test_dashboard_validation_fails_for_output_column_drift(
    tmp_path: Path,
) -> None:
    """Validate KPI output-column renames are caught before demo time."""
    errors = _validate_tmp_assets(
        tmp_path,
        sql_text=(
            "select sum(gmv) as gross_merchandise_value from `marts.mart_exec_daily` "
            "where 1 = 1 [[and {{date_range}}]]"
        ),
    )

    assert any("output_columns do not match SQL output" in error for error in errors)


def test_dashboard_validation_fails_for_missing_sql_derived_dependency(
    tmp_path: Path,
) -> None:
    """Validate derived outputs must declare the upstream columns they use."""
    errors = _validate_tmp_assets(
        tmp_path,
        sql_text=(
            "select safe_divide(gmv, non_cancelled_orders_count) as aov "
            "from `marts.mart_exec_daily` where 1 = 1 [[and {{date_range}}]]"
        ),
        spec_kwargs={
            "output_columns": ["aov"],
            "dependency_columns": [
                {
                    "name": "calendar_date",
                    "source_model": "mart_exec_daily",
                },
                {
                    "name": "gmv",
                    "source_model": "mart_exec_daily",
                },
            ],
        },
    )

    assert any(
        "SQL-derived field 'mart_exec_daily.non_cancelled_orders_count'" in error
        for error in errors
    )


def test_dashboard_validation_fails_for_invalid_derived_column_source(
    tmp_path: Path,
) -> None:
    """Validate derived-column provenance must point back to dependency_columns."""
    errors = _validate_tmp_assets(
        tmp_path,
        sql_text=(
            "select gmv as gmv from `marts.mart_exec_daily` "
            "where 1 = 1 [[and {{date_range}}]]"
        ),
        spec_kwargs={
            "derived_columns": [
                {
                    "name": "gmv",
                    "description": "Example derived metadata.",
                    "source_columns": [
                        {
                            "name": "orders_count",
                            "source_model": "mart_exec_daily",
                        }
                    ],
                }
            ]
        },
    )

    assert any(
        "derived column 'gmv' references 'mart_exec_daily.orders_count' outside dependency_columns"
        in error
        for error in errors
    )


def test_dashboard_validation_fails_for_missing_filter_variable(
    tmp_path: Path,
) -> None:
    """Validate every declared dashboard filter is wired into the SQL asset."""
    errors = _validate_tmp_assets(
        tmp_path,
        sql_text="select sum(gmv) as gmv from `marts.mart_exec_daily`",
    )

    assert any("missing variable '{{ date_range }}'" in error for error in errors)


def test_dashboard_validation_fails_for_missing_filter_dependency(
    tmp_path: Path,
) -> None:
    """Validate filter-backed fields must stay visible in the dependency contract."""
    errors = _validate_tmp_assets(
        tmp_path,
        sql_text=(
            "select sum(gmv) as gmv from `marts.mart_exec_daily` "
            "where 1 = 1 [[and {{date_range}}]]"
        ),
        spec_kwargs={
            "dependency_columns": [
                {
                    "name": "gmv",
                    "source_model": "mart_exec_daily",
                }
            ]
        },
    )

    assert any(
        "dependency_columns are missing filter-backed field 'mart_exec_daily.calendar_date'"
        in error
        for error in errors
    )


def test_dashboard_validation_fails_for_disallowed_dataset(tmp_path: Path) -> None:
    """Validate SQL assets cannot read from raw, staging, or intermediate datasets."""
    errors = _validate_tmp_assets(
        tmp_path,
        sql_text=(
            "select order_id as gmv from `raw_olist.orders` "
            "where 1 = 1 [[and {{date_range}}]]"
        ),
    )

    assert any("disallowed dataset 'raw_olist'" in error for error in errors)


def test_dashboard_validation_fails_for_missing_exposure(tmp_path: Path) -> None:
    """Validate a missing dbt exposure breaks the dashboard contract."""
    errors = _validate_tmp_assets(
        tmp_path,
        sql_text=(
            "select sum(gmv) as gmv from `marts.mart_exec_daily` "
            "where 1 = 1 [[and {{date_range}}]]"
        ),
        manifest_kwargs={"exposures": {}},
    )

    assert any(
        "exposure 'executive_overview_dashboard' is missing" in error
        for error in errors
    )


def test_dashboard_validation_fails_for_missing_screenshot(tmp_path: Path) -> None:
    """Validate a missing screenshot artifact breaks the repository contract."""
    manifest_path = tmp_path / "manifest.json"
    spec_path = tmp_path / "dashboards" / "specs" / "core_trio.json"
    sql_path = (
        tmp_path / "dashboards" / "sql" / "core_trio" / "executive" / "exec_gmv.sql"
    )

    spec_path.parent.mkdir(parents=True)
    sql_path.parent.mkdir(parents=True)
    _write_manifest(manifest_path)
    _write_spec(
        spec_path,
        screenshot_path="dashboards/screenshots/missing.svg",
    )
    sql_path.write_text(
        "select sum(gmv) as gmv from `marts.mart_exec_daily` "
        "where 1 = 1 [[and {{date_range}}]]",
        encoding="utf-8",
    )

    errors = validate_dashboard_assets(
        repo_root=tmp_path,
        spec_path=spec_path,
        manifest_path=manifest_path,
    )

    assert any("screenshot asset is missing" in error for error in errors)


def test_dashboard_validation_fails_for_wrong_exposure_dependency(
    tmp_path: Path,
) -> None:
    """Validate exposures must point back to the dashboard's primary mart."""
    errors = _validate_tmp_assets(
        tmp_path,
        sql_text=(
            "select sum(gmv) as gmv from `marts.mart_exec_daily` "
            "where 1 = 1 [[and {{date_range}}]]"
        ),
        manifest_kwargs={
            "exposures": {
                "exposure.marketplace_analytics_dbt.executive_overview_dashboard": {
                    "name": "executive_overview_dashboard",
                    "depends_on": {
                        "nodes": ["model.marketplace_analytics_dbt.dim_date"]
                    },
                }
            }
        },
    )

    assert any("does not depend on 'mart_exec_daily'" in error for error in errors)


def test_dashboard_validation_fails_when_sql_skips_primary_mart(
    tmp_path: Path,
) -> None:
    """Validate helper dimensions cannot replace the dashboard's primary mart."""
    errors = _validate_tmp_assets(
        tmp_path,
        sql_text=(
            "select calendar_date as gmv from `marts.dim_date` "
            "where 1 = 1 [[and {{date_range}}]]"
        ),
        spec_kwargs={
            "allowed_join_models": ["dim_date"],
        },
    )

    assert any(
        "does not read the primary mart 'mart_exec_daily'" in error for error in errors
    )


def test_core_trio_spec_validates_against_repo_contracts() -> None:
    """Validate the checked-in Core Trio package against the repository manifest."""
    repo_root = Path(__file__).resolve().parents[1]
    spec_path = repo_root / "dashboards" / "specs" / "core_trio.json"
    manifest_candidates = [
        repo_root / "marketplace_analytics_dbt" / "target_ci" / "manifest.json",
        repo_root / "marketplace_analytics_dbt" / "target" / "manifest.json",
    ]
    manifest_path = next(
        (candidate for candidate in manifest_candidates if candidate.exists()),
        None,
    )
    if manifest_path is None:
        pytest.skip("manifest not generated; run dbt parse or CI manifest build")

    errors = validate_dashboard_assets(
        repo_root=repo_root,
        spec_path=spec_path,
        manifest_path=manifest_path,
    )

    assert errors == []
