{% snapshot snap_products %}

{{
    config(
        unique_key='product_id',
        strategy='check',
        check_cols=[
            'product_category_name',
            'product_name_length',
            'product_description_length',
            'product_photos_count',
            'product_weight_g',
            'product_length_cm',
            'product_height_cm',
            'product_width_cm',
        ],
        invalidate_hard_deletes=True
    )
}}

-- Snapshot the cleaned product master-data contract rather than raw batch
-- metadata. The goal is to track semantic catalog changes, not ingestion-noise
-- fields such as batch identifiers.
select
    product_id,
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_count,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
from {{ ref('stg_products') }}

{% endsnapshot %}
