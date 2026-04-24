{% snapshot snap_sellers %}

{{
    config(
        unique_key='seller_id',
        strategy='check',
        check_cols=[
            'seller_zip_code_prefix',
            'seller_city',
            'seller_state',
        ],
        invalidate_hard_deletes=True
    )
}}

-- Snapshot the cleaned seller master-data contract rather than raw batch
-- metadata. This keeps SCD2-style history tracking without creating false
-- new versions on every rerun.
select
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
from {{ ref('stg_sellers') }}

{% endsnapshot %}
