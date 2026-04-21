{# =============================================================================
   Macro: holiday_country_code
   Purpose: Return the configured holiday country code as a single source of
            truth for every model that joins to holiday data. Centralizing this
            avoids semantic drift between dims, facts, marts, and tests.
   ============================================================================= #}
{% macro holiday_country_code() -%}
{{ return(env_var('NAGER_COUNTRY_CODE', env_var('DEFAULT_COUNTRY_CODE', 'BR')) | upper) }}
{%- endmacro %}
