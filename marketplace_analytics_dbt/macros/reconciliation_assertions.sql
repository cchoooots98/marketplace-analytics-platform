{% macro strict_exact_mismatch(left_expr, right_expr) -%}
(
    {{ left_expr }} is null
    or {{ right_expr }} is null
    or {{ left_expr }} != {{ right_expr }}
)
{%- endmacro %}

{% macro nullable_exact_mismatch(left_expr, right_expr) -%}
(
    not (
        ({{ left_expr }} is null and {{ right_expr }} is null)
        or {{ left_expr }} = {{ right_expr }}
    )
)
{%- endmacro %}

{% macro required_numeric_mismatch(left_expr, right_expr, tolerance_sql) -%}
(
    {{ left_expr }} is null
    or {{ right_expr }} is null
    or abs({{ left_expr }} - {{ right_expr }}) > {{ tolerance_sql }}
)
{%- endmacro %}

{% macro nullable_numeric_mismatch(left_expr, right_expr, tolerance_sql) -%}
(
    not (
        ({{ left_expr }} is null and {{ right_expr }} is null)
        or abs({{ left_expr }} - {{ right_expr }}) <= {{ tolerance_sql }}
    )
)
{%- endmacro %}

{% macro required_amount_mismatch(left_expr, right_expr) -%}
{{ required_numeric_mismatch(left_expr, right_expr, var('reconciliation_amount_tolerance', 0.01)) }}
{%- endmacro %}

{% macro nullable_amount_mismatch(left_expr, right_expr) -%}
{{ nullable_numeric_mismatch(left_expr, right_expr, var('reconciliation_amount_tolerance', 0.01)) }}
{%- endmacro %}

{% macro required_rate_mismatch(left_expr, right_expr) -%}
{{ required_numeric_mismatch(left_expr, right_expr, var('reconciliation_rate_tolerance', 0.000001)) }}
{%- endmacro %}

{% macro nullable_rate_mismatch(left_expr, right_expr) -%}
{{ nullable_numeric_mismatch(left_expr, right_expr, var('reconciliation_rate_tolerance', 0.000001)) }}
{%- endmacro %}

{% macro reconciliation_mismatch_rows(
    expected_alias,
    actual_alias,
    key_columns,
    exact_columns=[],
    nullable_exact_columns=[],
    required_amount_columns=[],
    nullable_amount_columns=[],
    required_rate_columns=[],
    nullable_rate_columns=[],
    diagnostic_columns=[]
) %}
{#-
  Parameter null semantics:
    exact_columns:           strict; null on either side is a mismatch
    nullable_exact_columns:  nullable-tolerant; (null, null) counts as equal
    required_amount_columns: strict; null on either side is a mismatch
    nullable_amount_columns: nullable-tolerant; (null, null) counts as equal
    required_rate_columns:   strict; null on either side is a mismatch
    nullable_rate_columns:   nullable-tolerant; (null, null) counts as equal
-#}
select
    {%- for key_column in key_columns %}
    coalesce({{ expected_alias }}.{{ key_column }}, {{ actual_alias }}.{{ key_column }}) as {{ key_column }}{% if not loop.last or diagnostic_columns %}, {% endif %}
    {%- endfor %}
    {%- for diagnostic_column in diagnostic_columns %}
    {{ expected_alias }}.{{ diagnostic_column }} as expected_{{ diagnostic_column }},
    {{ actual_alias }}.{{ diagnostic_column }} as actual_{{ diagnostic_column }}{% if not loop.last %}, {% endif %}
    {%- endfor %}
from {{ expected_alias }}
full outer join {{ actual_alias }}
    on
    {%- for key_column in key_columns %}
    {{ expected_alias }}.{{ key_column }} = {{ actual_alias }}.{{ key_column }}{% if not loop.last %} and {% endif %}
    {%- endfor %}
where
    {{ expected_alias }}.{{ key_columns[0] }} is null
    or {{ actual_alias }}.{{ key_columns[0] }} is null
    {%- for column_name in exact_columns %}
    or {{ strict_exact_mismatch(
        expected_alias ~ '.' ~ column_name,
        actual_alias ~ '.' ~ column_name
    ) }}
    {%- endfor %}
    {%- for column_name in nullable_exact_columns %}
    or {{ nullable_exact_mismatch(
        expected_alias ~ '.' ~ column_name,
        actual_alias ~ '.' ~ column_name
    ) }}
    {%- endfor %}
    {%- for column_name in required_amount_columns %}
    or {{ required_amount_mismatch(
        expected_alias ~ '.' ~ column_name,
        actual_alias ~ '.' ~ column_name
    ) }}
    {%- endfor %}
    {%- for column_name in nullable_amount_columns %}
    or {{ nullable_amount_mismatch(
        expected_alias ~ '.' ~ column_name,
        actual_alias ~ '.' ~ column_name
    ) }}
    {%- endfor %}
    {%- for column_name in required_rate_columns %}
    or {{ required_rate_mismatch(
        expected_alias ~ '.' ~ column_name,
        actual_alias ~ '.' ~ column_name
    ) }}
    {%- endfor %}
    {%- for column_name in nullable_rate_columns %}
    or {{ nullable_rate_mismatch(
        expected_alias ~ '.' ~ column_name,
        actual_alias ~ '.' ~ column_name
    ) }}
    {%- endfor %}
{% endmacro %}
