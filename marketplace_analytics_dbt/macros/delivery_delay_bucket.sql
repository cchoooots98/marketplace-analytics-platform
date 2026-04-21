{# =============================================================================
   Macro: delivery_delay_bucket
   Purpose: Return the standardized delivery-delay classification for a single
            order row. Centralizes bucket semantics so every layer
            (intermediate, fact, mart) classifies the same way and any change to
            bucket boundaries happens in one place.
   Inputs:
     is_delivered   BOOL     True when the order was delivered with a timestamp
     is_cancelled   BOOL     True when the order was cancelled
     is_late        BOOL     True when delivery occurred past the estimate
     late_days      INT64    Days past the estimated delivery date (nullable)
   Accepted outputs:
     'not_delivered'  - cancelled or never delivered. Delay is undefined here;
                        grouping these into 'on_time' would contaminate customer
                        experience dashboards.
     'on_time'        - delivered on or before the estimated delivery date.
     '1_to_3_days'    - delivered 1 to 3 days after the estimate.
     '4_to_7_days'    - delivered 4 to 7 days after the estimate.
     '8_to_14_days'   - delivered 8 to 14 days after the estimate.
     '15_plus_days'   - delivered 15 or more days after the estimate.
   ============================================================================= #}
{% macro delivery_delay_bucket(is_delivered, is_cancelled, is_late, late_days) %}
case
    when {{ is_cancelled }} or not coalesce({{ is_delivered }}, false)
        then 'not_delivered'
    when not coalesce({{ is_late }}, false)
        then 'on_time'
    when {{ late_days }} between 1 and 3
        then '1_to_3_days'
    when {{ late_days }} between 4 and 7
        then '4_to_7_days'
    when {{ late_days }} between 8 and 14
        then '8_to_14_days'
    when {{ late_days }} >= 15
        then '15_plus_days'
    else 'not_delivered'
end
{% endmacro %}
