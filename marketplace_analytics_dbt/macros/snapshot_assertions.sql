{% macro snapshot_noop_version_rows(snapshot_name, unique_key, tracked_columns) %}
with version_states as (

    select
        *,
        to_json_string(
            struct(
                {%- for column_name in tracked_columns -%}
                {{ column_name }} as {{ column_name }}{% if not loop.last %}, {% endif %}
                {%- endfor -%}
            )
        ) as tracked_state_json
    from {{ ref(snapshot_name) }}

),

ordered_versions as (

    select
        {{ unique_key }},
        dbt_scd_id,
        dbt_valid_from,
        dbt_valid_to,
        tracked_state_json,
        lag(tracked_state_json) over (
            partition by {{ unique_key }}
            order by dbt_valid_from, dbt_scd_id
        ) as previous_tracked_state_json
    from version_states

)

select
    {{ unique_key }},
    dbt_scd_id,
    dbt_valid_from,
    dbt_valid_to,
    previous_tracked_state_json,
    tracked_state_json
from ordered_versions
where
    previous_tracked_state_json is not null
    and previous_tracked_state_json = tracked_state_json
{% endmacro %}
