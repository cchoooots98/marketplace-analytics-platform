"""Validate dashboard contracts against version-controlled assets and dbt docs."""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path
from typing import Any

import yaml

LOGGER = logging.getLogger(__name__)
DEFAULT_MANIFEST_PATH = "marketplace_analytics_dbt/target/manifest.json"
CI_MANIFEST_PATH = "marketplace_analytics_dbt/target_ci/manifest.json"

TABLE_REFERENCE_PATTERN = re.compile(
    r"(?i)\b(?:from|join)\s+`?(?P<identifier>[a-zA-Z0-9_.-]+)`?"
)
TABLE_ALIAS_PATTERN = re.compile(
    r"(?i)\b(?:from|join)\s+`?(?P<identifier>[a-zA-Z0-9_.-]+)`?"
    r"(?:\s+(?:as\s+)?(?P<alias>[a-zA-Z_][a-zA-Z0-9_]*))?"
)
SQL_IDENTIFIER_PATTERN = re.compile(
    r"`?(?:[a-zA-Z_][a-zA-Z0-9_]*\.)?(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)`?$"
)
QUALIFIED_COLUMN_PATTERN = re.compile(
    r"`?(?P<qualifier>[a-zA-Z_][a-zA-Z0-9_]*)`?\s*\.\s*"
    r"`?(?P<column>[a-zA-Z_][a-zA-Z0-9_]*)`?"
)
IDENTIFIER_TOKEN_PATTERN = re.compile(r"`?(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)`?")
SQL_RESERVED_WORDS = {
    "and",
    "as",
    "asc",
    "by",
    "case",
    "cast",
    "desc",
    "distinct",
    "else",
    "end",
    "false",
    "from",
    "group",
    "having",
    "in",
    "is",
    "join",
    "left",
    "like",
    "limit",
    "not",
    "null",
    "on",
    "or",
    "order",
    "over",
    "partition",
    "right",
    "rows",
    "select",
    "then",
    "true",
    "union",
    "using",
    "when",
    "where",
    "with",
}
SQL_ALIAS_STOP_WORDS = {
    "cross",
    "except",
    "full",
    "group",
    "having",
    "inner",
    "intersect",
    "join",
    "left",
    "limit",
    "on",
    "order",
    "outer",
    "qualify",
    "right",
    "union",
    "using",
    "where",
}


def _load_json_document(document_path: Path) -> dict[str, Any]:
    """Load one JSON document from disk.

    Args:
        document_path: Location of the JSON file to read.

    Returns:
        Parsed JSON object.

    Raises:
        FileNotFoundError: If the path does not exist.
        json.JSONDecodeError: If the file is not valid JSON.
        ValueError: If the file does not contain a JSON object.
    """
    with document_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, dict):
        raise ValueError(f"Expected a JSON object in {document_path}")

    return payload


def _manifest_models(manifest: dict[str, Any]) -> dict[str, set[str]]:
    """Build a model-to-columns lookup from a dbt manifest.

    Args:
        manifest: Parsed dbt manifest JSON.

    Returns:
        Mapping of model names to the set of published column names.
    """
    model_columns: dict[str, set[str]] = {}
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue

        model_name = node.get("name")
        if not isinstance(model_name, str):
            continue

        columns = node.get("columns", {})
        model_columns[model_name] = set(columns.keys())

    return model_columns


def _manifest_exposures(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Build an exposure-name lookup from a dbt manifest.

    Args:
        manifest: Parsed dbt manifest JSON.

    Returns:
        Mapping of exposure names to exposure nodes.
    """
    exposures: dict[str, dict[str, Any]] = {}
    for exposure in manifest.get("exposures", {}).values():
        exposure_name = exposure.get("name")
        if isinstance(exposure_name, str):
            exposures[exposure_name] = exposure
    return exposures


def _repo_model_contracts(repo_root: Path) -> dict[str, set[str]]:
    """Load model column contracts directly from dbt YAML files in the repo.

    Args:
        repo_root: Repository root containing the dbt project.

    Returns:
        Mapping of model names to the set of documented column names.
    """
    model_columns: dict[str, set[str]] = {}
    models_root = repo_root / "marketplace_analytics_dbt" / "models"
    for yaml_path in models_root.rglob("*.yml"):
        documents = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
        if not isinstance(documents, dict):
            continue
        for model_spec in documents.get("models", []):
            model_name = model_spec.get("name")
            if not isinstance(model_name, str):
                continue
            columns = {
                column_spec.get("name")
                for column_spec in model_spec.get("columns", [])
                if isinstance(column_spec, dict)
                and isinstance(column_spec.get("name"), str)
            }
            if columns:
                model_columns[model_name] = columns
    return model_columns


def _repo_exposures(repo_root: Path) -> dict[str, dict[str, Any]]:
    """Load active dashboard exposures from the repository YAML file.

    Args:
        repo_root: Repository root containing the dbt project.

    Returns:
        Mapping of exposure names to exposure specs from the repo.
    """
    exposures_path = (
        repo_root / "marketplace_analytics_dbt" / "models" / "exposures.yml"
    )
    if not exposures_path.exists():
        return {}

    payload = yaml.safe_load(exposures_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return {}

    exposures: dict[str, dict[str, Any]] = {}
    for exposure_spec in payload.get("exposures", []):
        exposure_name = exposure_spec.get("name")
        if isinstance(exposure_name, str):
            exposures[exposure_name] = exposure_spec
    return exposures


def _strip_sql_line_comments(sql_text: str) -> str:
    """Remove SQL line comments before table-reference inspection.

    Args:
        sql_text: Raw SQL asset text.

    Returns:
        SQL text with `--` line comments removed.
    """
    return re.sub(r"--.*$", "", sql_text, flags=re.MULTILINE)


def _exposure_depends_on_model(exposure_spec: dict[str, Any], model_name: str) -> bool:
    """Check whether one exposure depends on the expected dbt model.

    Args:
        exposure_spec: Exposure definition from a dbt manifest or YAML file.
        model_name: Model name that should back the dashboard exposure.

    Returns:
        True when the exposure depends on the expected model.
    """
    depends_on = exposure_spec.get("depends_on")
    expected_manifest_node = f"model.marketplace_analytics_dbt.{model_name}"
    expected_yaml_ref = f"ref('{model_name}')"

    if isinstance(depends_on, dict):
        nodes = depends_on.get("nodes", [])
        return expected_manifest_node in nodes

    if isinstance(depends_on, list):
        return expected_yaml_ref in depends_on

    return False


def _extract_dataset_table_references(sql_text: str) -> set[tuple[str, str]]:
    """Extract dataset-qualified table references from one SQL asset.

    Args:
        sql_text: SQL question text.

    Returns:
        Set of `(dataset_name, table_name)` tuples for physical table references.
    """
    references: set[tuple[str, str]] = set()
    for match in TABLE_REFERENCE_PATTERN.finditer(_strip_sql_line_comments(sql_text)):
        identifier = match.group("identifier")
        parts = identifier.split(".")
        if len(parts) < 2:
            continue
        dataset_name = parts[-2]
        table_name = parts[-1]
        references.add((dataset_name, table_name))
    return references


def _sql_contains_variable(sql_text: str, variable_name: str) -> bool:
    """Check whether one SQL asset references the given Metabase variable.

    Args:
        sql_text: SQL question text.
        variable_name: Variable name to search for.

    Returns:
        True when the variable is referenced in `{{ ... }}` syntax.
    """
    variable_pattern = re.compile(r"{{\s*" + re.escape(variable_name) + r"\s*}}")
    return bool(variable_pattern.search(sql_text))


def _find_top_level_keyword(
    sql_text: str,
    keyword: str,
    *,
    start_index: int = 0,
) -> int:
    """Find one SQL keyword outside parentheses and quoted strings.

    Args:
        sql_text: SQL text to scan.
        keyword: Lowercase SQL keyword to search for.
        start_index: Character offset where scanning should begin.

    Returns:
        The character offset of the first matching keyword, or `-1` if absent.
    """
    lowered_sql = sql_text.lower()
    depth = 0
    in_single_quote = False
    in_double_quote = False
    in_backticks = False

    for index in range(start_index, len(sql_text)):
        character = sql_text[index]

        if character == "'" and not in_double_quote and not in_backticks:
            in_single_quote = not in_single_quote
            continue
        if character == '"' and not in_single_quote and not in_backticks:
            in_double_quote = not in_double_quote
            continue
        if character == "`" and not in_single_quote and not in_double_quote:
            in_backticks = not in_backticks
            continue
        if in_single_quote or in_double_quote or in_backticks:
            continue

        if character == "(":
            depth += 1
            continue
        if character == ")":
            depth = max(depth - 1, 0)
            continue
        if depth != 0 or not lowered_sql.startswith(keyword, index):
            continue

        previous_character = lowered_sql[index - 1] if index > 0 else " "
        next_index = index + len(keyword)
        next_character = lowered_sql[next_index] if next_index < len(sql_text) else " "
        if not (previous_character.isalnum() or previous_character == "_") and not (
            next_character.isalnum() or next_character == "_"
        ):
            return index

    return -1


def _split_top_level_csv(select_clause: str) -> list[str]:
    """Split one SQL select clause on top-level commas only.

    Args:
        select_clause: Raw text between the outer `SELECT` and `FROM`.

    Returns:
        Individual select expressions in declaration order.
    """
    expressions: list[str] = []
    current_expression: list[str] = []
    depth = 0
    in_single_quote = False
    in_double_quote = False
    in_backticks = False

    for character in select_clause:
        if character == "'" and not in_double_quote and not in_backticks:
            in_single_quote = not in_single_quote
        elif character == '"' and not in_single_quote and not in_backticks:
            in_double_quote = not in_double_quote
        elif character == "`" and not in_single_quote and not in_double_quote:
            in_backticks = not in_backticks
        elif not (in_single_quote or in_double_quote or in_backticks):
            if character == "(":
                depth += 1
            elif character == ")":
                depth = max(depth - 1, 0)
            elif character == "," and depth == 0:
                expression = "".join(current_expression).strip()
                if expression:
                    expressions.append(expression)
                current_expression = []
                continue

        current_expression.append(character)

    final_expression = "".join(current_expression).strip()
    if final_expression:
        expressions.append(final_expression)

    return expressions


def _derive_output_column_name(select_expression: str) -> str:
    """Derive one output column name from a SQL select expression.

    Args:
        select_expression: One select expression from the outer query.

    Returns:
        The output column name exposed by the expression.

    Raises:
        ValueError: If the expression does not expose a parseable column name.
    """
    normalized_expression = select_expression.strip().rstrip(",")
    normalized_expression = re.sub(r"(?i)^distinct\s+", "", normalized_expression)

    alias_match = re.search(
        r"(?i)\bas\s+`?(?P<alias>[a-zA-Z_][a-zA-Z0-9_]*)`?\s*$",
        normalized_expression,
        flags=re.DOTALL,
    )
    if alias_match:
        return alias_match.group("alias")

    identifier_match = SQL_IDENTIFIER_PATTERN.fullmatch(normalized_expression)
    if identifier_match:
        return identifier_match.group("name")

    raise ValueError(
        "Select expression must expose an explicit alias when it is not a bare "
        f"identifier: '{select_expression.strip()}'"
    )


def _extract_top_level_select_expressions(sql_text: str) -> list[str]:
    """Extract top-level select expressions from the outermost SQL query.

    Args:
        sql_text: SQL asset text.

    Returns:
        Top-level select expressions in declaration order.

    Raises:
        ValueError: If the outer query cannot be parsed safely.
    """
    normalized_sql = _strip_sql_line_comments(sql_text)
    select_index = _find_top_level_keyword(normalized_sql, "select")
    if select_index < 0:
        raise ValueError("SQL asset is missing a top-level SELECT clause.")

    from_index = _find_top_level_keyword(
        normalized_sql,
        "from",
        start_index=select_index + len("select"),
    )
    if from_index < 0:
        raise ValueError("SQL asset is missing a top-level FROM clause.")

    select_clause = normalized_sql[select_index + len("select") : from_index].strip()
    if not select_clause:
        raise ValueError("Top-level SELECT clause is empty.")

    for set_operator in ("union", "intersect", "except"):
        if (
            _find_top_level_keyword(
                normalized_sql,
                set_operator,
                start_index=from_index + len("from"),
            )
            >= 0
        ):
            raise ValueError(
                "Top-level set operators are not supported in dashboard SQL assets."
            )

    select_expressions = _split_top_level_csv(select_clause)
    if not select_expressions:
        raise ValueError("No output columns were found in the top-level SELECT.")

    return select_expressions


def _extract_sql_output_columns(sql_text: str) -> list[str]:
    """Extract output column names from the outermost SQL query.

    Args:
        sql_text: SQL asset text.

    Returns:
        Output column names in result-set order.

    Raises:
        ValueError: If the outer query cannot be parsed into explicit columns.
    """
    output_columns = [
        _derive_output_column_name(select_expression)
        for select_expression in _extract_top_level_select_expressions(sql_text)
    ]

    return output_columns


def _strip_expression_alias(select_expression: str) -> tuple[str, str | None]:
    """Remove one trailing `AS alias` declaration from a select expression.

    Args:
        select_expression: One top-level select expression.

    Returns:
        Tuple of `(expression_without_alias, alias_name_or_none)`.
    """
    alias_match = re.search(
        r"(?i)^(?P<body>.+?)\bas\s+`?(?P<alias>[a-zA-Z_][a-zA-Z0-9_]*)`?\s*$",
        select_expression.strip(),
        flags=re.DOTALL,
    )
    if alias_match:
        return alias_match.group("body").strip(), alias_match.group("alias")
    return select_expression.strip(), None


def _extract_table_aliases(sql_text: str) -> dict[str, str]:
    """Map physical table aliases back to dashboard-allowed model names.

    Args:
        sql_text: SQL asset text.

    Returns:
        Mapping from alias or table name to dbt model name.
    """
    alias_lookup: dict[str, str] = {}
    for match in TABLE_ALIAS_PATTERN.finditer(_strip_sql_line_comments(sql_text)):
        identifier = match.group("identifier")
        parts = identifier.split(".")
        if len(parts) < 2:
            continue
        model_name = parts[-1]
        alias_lookup[model_name] = model_name

        alias_name = match.group("alias")
        if alias_name and alias_name.lower() not in SQL_ALIAS_STOP_WORDS:
            alias_lookup[alias_name] = model_name

    return alias_lookup


def _extract_expression_dependency_columns(
    *,
    select_expression: str,
    allowed_models: set[str],
    model_columns: dict[str, set[str]],
    alias_lookup: dict[str, str],
) -> set[tuple[str, str]]:
    """Extract model-linked dependency columns from one select expression.

    Args:
        select_expression: One top-level select expression.
        allowed_models: Models the card is allowed to read.
        model_columns: Repository-backed model-to-column contract lookup.
        alias_lookup: Mapping from SQL aliases to physical model names.

    Returns:
        Parseable `(model_name, column_name)` dependencies used by the
        expression.
    """
    expression_body, expression_alias = _strip_expression_alias(select_expression)
    dependency_columns: set[tuple[str, str]] = set()

    for qualified_match in QUALIFIED_COLUMN_PATTERN.finditer(expression_body):
        qualifier = qualified_match.group("qualifier")
        column_name = qualified_match.group("column")
        model_name = alias_lookup.get(qualifier)
        if model_name in allowed_models and column_name in model_columns.get(
            model_name, set()
        ):
            dependency_columns.add((model_name, column_name))

    bare_expression = QUALIFIED_COLUMN_PATTERN.sub(" ", expression_body)
    for identifier_match in IDENTIFIER_TOKEN_PATTERN.finditer(bare_expression):
        token = identifier_match.group("name")
        lowered_token = token.lower()
        if lowered_token in SQL_RESERVED_WORDS or token == expression_alias:
            continue
        if token in alias_lookup:
            continue

        remaining_text = bare_expression[identifier_match.end() :]
        if remaining_text.lstrip().startswith("("):
            continue

        candidate_models = [
            model_name
            for model_name in allowed_models
            if token in model_columns.get(model_name, set())
        ]
        if len(candidate_models) == 1:
            dependency_columns.add((candidate_models[0], token))

    return dependency_columns


def _extract_sql_expression_dependencies(
    *,
    sql_text: str,
    allowed_models: set[str],
    model_columns: dict[str, set[str]],
) -> set[tuple[str, str]]:
    """Extract parseable dependency columns from top-level select expressions.

    Args:
        sql_text: SQL asset text.
        allowed_models: Models the card is allowed to read.
        model_columns: Repository-backed model-to-column contract lookup.

    Returns:
        Parseable `(model_name, column_name)` dependencies referenced by the
        stakeholder-facing output expressions.

    Raises:
        ValueError: If the top-level select expressions cannot be parsed.
    """
    alias_lookup = _extract_table_aliases(sql_text)
    dependency_columns: set[tuple[str, str]] = set()
    for select_expression in _extract_top_level_select_expressions(sql_text):
        dependency_columns.update(
            _extract_expression_dependency_columns(
                select_expression=select_expression,
                allowed_models=allowed_models,
                model_columns=model_columns,
                alias_lookup=alias_lookup,
            )
        )
    return dependency_columns


def validate_dashboard_assets(
    *,
    repo_root: Path,
    spec_path: Path,
    manifest_path: Path,
) -> list[str]:
    """Validate dashboard specs, SQL assets, screenshots, and dbt exposures.

    Args:
        repo_root: Repository root used to resolve relative asset paths.
        spec_path: Path to the version-controlled dashboard manifest.
        manifest_path: Path to a dbt `manifest.json` file.

    Returns:
        A list of human-readable validation errors. The list is empty when all
        checks pass.
    """
    errors: list[str] = []
    spec = _load_json_document(spec_path)
    manifest = _load_json_document(manifest_path)

    model_columns = _manifest_models(manifest)
    model_columns.update(_repo_model_contracts(repo_root))
    exposure_lookup = _manifest_exposures(manifest)
    exposure_lookup.update(_repo_exposures(repo_root))
    dashboards = spec.get("dashboards", [])
    if not isinstance(dashboards, list):
        return ["Dashboard spec must contain a top-level 'dashboards' list."]

    for dashboard in dashboards:
        dashboard_name = dashboard.get("name", "<unknown dashboard>")
        primary_model = dashboard.get("primary_model")
        exposure_name = dashboard.get("exposure_name")
        screenshot_path_text = dashboard.get("screenshot_path")
        filters = dashboard.get("filters", [])
        cards = dashboard.get("cards", [])

        if primary_model not in model_columns:
            errors.append(
                f"{dashboard_name}: primary_model '{primary_model}' is missing from the dbt manifest."
            )
            continue

        if not isinstance(exposure_name, str) or exposure_name not in exposure_lookup:
            errors.append(
                f"{dashboard_name}: exposure '{exposure_name}' is missing from the dbt manifest."
            )
        else:
            if not _exposure_depends_on_model(
                exposure_lookup[exposure_name],
                primary_model,
            ):
                errors.append(
                    f"{dashboard_name}: exposure '{exposure_name}' does not depend on '{primary_model}'."
                )

        if not isinstance(screenshot_path_text, str):
            errors.append(f"{dashboard_name}: screenshot_path must be a string.")
        else:
            screenshot_path = repo_root / screenshot_path_text
            if not screenshot_path.exists():
                errors.append(
                    f"{dashboard_name}: screenshot asset is missing at '{screenshot_path_text}'."
                )

        filter_lookup: dict[str, dict[str, Any]] = {}
        if not isinstance(filters, list):
            errors.append(f"{dashboard_name}: filters must be a list.")
            filters = []
        for filter_spec in filters:
            filter_name = filter_spec.get("name")
            variable_name = filter_spec.get("variable")
            field_name = filter_spec.get("field_name")
            filter_model = filter_spec.get("source_model", primary_model)
            if not isinstance(filter_name, str):
                errors.append(
                    f"{dashboard_name}: every filter must define a string name."
                )
                continue
            filter_lookup[filter_name] = filter_spec
            if not isinstance(variable_name, str):
                errors.append(
                    f"{dashboard_name}: filter '{filter_name}' is missing a variable."
                )
            if filter_model not in model_columns:
                errors.append(
                    f"{dashboard_name}: filter '{filter_name}' references unknown model '{filter_model}'."
                )
                continue
            if field_name not in model_columns[filter_model]:
                errors.append(
                    f"{dashboard_name}: filter '{filter_name}' references missing field "
                    f"'{field_name}' on '{filter_model}'."
                )

        if not isinstance(cards, list):
            errors.append(f"{dashboard_name}: cards must be a list.")
            continue

        for card in cards:
            card_title = card.get("title", "<unknown card>")
            sql_path_text = card.get("sql_path")
            card_source_model = card.get("source_model", primary_model)
            allowed_join_models = set(card.get("allowed_join_models", []))
            output_columns = card.get("output_columns", [])
            dependency_columns = card.get("dependency_columns", [])
            filter_names = card.get("filter_names", [])

            if card_source_model != primary_model:
                errors.append(
                    f"{dashboard_name} / {card_title}: source_model must match dashboard primary_model."
                )
                continue

            unknown_join_models = allowed_join_models.difference(
                {"dim_date", "dim_seller"}
            )
            if unknown_join_models:
                errors.append(
                    f"{dashboard_name} / {card_title}: unsupported join models "
                    f"{sorted(unknown_join_models)}."
                )

            if not isinstance(sql_path_text, str):
                errors.append(
                    f"{dashboard_name} / {card_title}: sql_path must be a string."
                )
                continue

            sql_path = repo_root / sql_path_text
            if not sql_path.exists():
                errors.append(
                    f"{dashboard_name} / {card_title}: SQL asset is missing at '{sql_path_text}'."
                )
                continue

            sql_text = sql_path.read_text(encoding="utf-8")
            allowed_models = {primary_model, *allowed_join_models}
            try:
                actual_output_columns = _extract_sql_output_columns(sql_text)
            except ValueError as exc:
                errors.append(
                    f"{dashboard_name} / {card_title}: unable to parse SQL output columns: {exc}"
                )
                actual_output_columns = []
            try:
                expression_dependency_columns = _extract_sql_expression_dependencies(
                    sql_text=sql_text,
                    allowed_models=allowed_models,
                    model_columns=model_columns,
                )
            except ValueError as exc:
                errors.append(
                    f"{dashboard_name} / {card_title}: unable to parse SQL dependency columns: {exc}"
                )
                expression_dependency_columns = set()

            actual_references = _extract_dataset_table_references(sql_text)
            if not actual_references:
                errors.append(
                    f"{dashboard_name} / {card_title}: SQL asset does not contain any dataset-qualified table references."
                )
            for dataset_name, table_name in actual_references:
                if dataset_name != "marts":
                    errors.append(
                        f"{dashboard_name} / {card_title}: SQL asset references disallowed dataset "
                        f"'{dataset_name}'."
                    )
                if table_name not in allowed_models:
                    errors.append(
                        f"{dashboard_name} / {card_title}: SQL asset references disallowed model "
                        f"'{table_name}'."
                    )
            if primary_model not in {table_name for _, table_name in actual_references}:
                errors.append(
                    f"{dashboard_name} / {card_title}: SQL asset does not read the primary mart '{primary_model}'."
                )

            if not isinstance(output_columns, list) or not all(
                isinstance(column_name, str) for column_name in output_columns
            ):
                errors.append(
                    f"{dashboard_name} / {card_title}: output_columns must be a list of strings."
                )
                output_columns = []
            if output_columns and actual_output_columns != output_columns:
                errors.append(
                    f"{dashboard_name} / {card_title}: output_columns do not match SQL output. "
                    f"Expected {output_columns}, found {actual_output_columns}."
                )

            if not isinstance(dependency_columns, list):
                errors.append(
                    f"{dashboard_name} / {card_title}: dependency_columns must be a list."
                )
                dependency_columns = []
            dependency_pairs: set[tuple[str, str]] = set()
            for field_spec in dependency_columns:
                field_name = field_spec.get("name")
                field_model = field_spec.get("source_model", primary_model)
                if field_model not in allowed_models:
                    errors.append(
                        f"{dashboard_name} / {card_title}: dependency column '{field_name}' references "
                        f"disallowed model '{field_model}'."
                    )
                    continue
                if field_name not in model_columns.get(field_model, set()):
                    errors.append(
                        f"{dashboard_name} / {card_title}: dependency column '{field_name}' is missing from "
                        f"'{field_model}'."
                    )
                    continue
                dependency_pairs.add((field_model, field_name))
            missing_expression_dependencies = sorted(
                expression_dependency_columns.difference(dependency_pairs)
            )
            for dependency_model, dependency_name in missing_expression_dependencies:
                errors.append(
                    f"{dashboard_name} / {card_title}: dependency_columns are missing "
                    f"SQL-derived field '{dependency_model}.{dependency_name}'."
                )

            derived_columns = card.get("derived_columns", [])
            if derived_columns is not None and not isinstance(derived_columns, list):
                errors.append(
                    f"{dashboard_name} / {card_title}: derived_columns must be a list when provided."
                )
                derived_columns = []
            for derived_column in derived_columns:
                derived_name = derived_column.get("name")
                if not isinstance(derived_name, str):
                    errors.append(
                        f"{dashboard_name} / {card_title}: each derived column must define a string name."
                    )
                    continue
                if derived_name not in output_columns:
                    errors.append(
                        f"{dashboard_name} / {card_title}: derived column '{derived_name}' must also appear in output_columns."
                    )
                source_columns = derived_column.get("source_columns", [])
                if not isinstance(source_columns, list):
                    errors.append(
                        f"{dashboard_name} / {card_title}: derived column '{derived_name}' must define source_columns as a list."
                    )
                    continue
                for source_column in source_columns:
                    source_model = source_column.get("source_model", primary_model)
                    source_name = source_column.get("name")
                    if not isinstance(source_model, str) or not isinstance(
                        source_name, str
                    ):
                        errors.append(
                            f"{dashboard_name} / {card_title}: derived column '{derived_name}' has an invalid source_columns entry."
                        )
                        continue
                    if (source_model, source_name) not in dependency_pairs:
                        errors.append(
                            f"{dashboard_name} / {card_title}: derived column '{derived_name}' references "
                            f"'{source_model}.{source_name}' outside dependency_columns."
                        )

            if not isinstance(filter_names, list):
                errors.append(
                    f"{dashboard_name} / {card_title}: filter_names must be a list."
                )
                filter_names = []
            for filter_name in filter_names:
                if filter_name not in filter_lookup:
                    errors.append(
                        f"{dashboard_name} / {card_title}: references unknown filter '{filter_name}'."
                    )
                    continue
                variable_name = filter_lookup[filter_name].get("variable")
                if isinstance(variable_name, str) and not _sql_contains_variable(
                    sql_text,
                    variable_name,
                ):
                    errors.append(
                        f"{dashboard_name} / {card_title}: SQL asset is missing variable "
                        f"'{{{{ {variable_name} }}}}'."
                    )
                filter_model = filter_lookup[filter_name].get(
                    "source_model", primary_model
                )
                filter_field_name = filter_lookup[filter_name].get("field_name")
                if (
                    isinstance(filter_model, str)
                    and isinstance(filter_field_name, str)
                    and (filter_model, filter_field_name) not in dependency_pairs
                ):
                    errors.append(
                        f"{dashboard_name} / {card_title}: dependency_columns are missing "
                        f"filter-backed field '{filter_model}.{filter_field_name}'."
                    )

    return errors


def _resolve_manifest_path(repo_root: Path, manifest_argument: str) -> Path:
    """Resolve the manifest path with a CI fallback for the default local path.

    Args:
        repo_root: Repository root used to resolve relative asset paths.
        manifest_argument: CLI manifest path argument relative to the repo root.

    Returns:
        Resolved manifest path to use for validation.
    """
    manifest_path = repo_root / manifest_argument
    if manifest_path.exists():
        return manifest_path
    if manifest_argument == DEFAULT_MANIFEST_PATH:
        ci_manifest_path = repo_root / CI_MANIFEST_PATH
        if ci_manifest_path.exists():
            return ci_manifest_path
    return manifest_path


def _parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the dashboard validator.

    Args:
        argv: Optional CLI arguments excluding the executable name.

    Returns:
        Parsed argument namespace.
    """
    parser = argparse.ArgumentParser(
        description="Validate dashboard specs, SQL assets, and screenshots.",
    )
    parser.add_argument(
        "--repo-root",
        default=".",
        help="Repository root used to resolve relative asset paths.",
    )
    parser.add_argument(
        "--spec",
        default="dashboards/specs/core_trio.json",
        help="Path to the dashboard spec JSON relative to the repository root.",
    )
    parser.add_argument(
        "--manifest",
        default=DEFAULT_MANIFEST_PATH,
        help="Path to the dbt manifest JSON relative to the repository root.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the dashboard validation CLI.

    Args:
        argv: Optional CLI arguments excluding the executable name.

    Returns:
        Exit code for the validation run.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    arguments = _parse_arguments(argv)
    repo_root = Path(arguments.repo_root).resolve()
    spec_path = repo_root / arguments.spec
    manifest_path = _resolve_manifest_path(repo_root, arguments.manifest)

    try:
        validation_errors = validate_dashboard_assets(
            repo_root=repo_root,
            spec_path=spec_path,
            manifest_path=manifest_path,
        )
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as exc:
        LOGGER.error("Dashboard validation setup failed: %s", exc)
        return 1

    if validation_errors:
        for validation_error in validation_errors:
            LOGGER.error(validation_error)
        LOGGER.error(
            "Dashboard validation failed with %s issue(s).", len(validation_errors)
        )
        return 1

    LOGGER.info(
        "Dashboard assets validated successfully against %s.",
        manifest_path.relative_to(repo_root),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
