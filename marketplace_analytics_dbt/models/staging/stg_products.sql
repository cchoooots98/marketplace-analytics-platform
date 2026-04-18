-- =============================================================================
-- Model: stg_products
-- Grain: One row per product_id
-- Source: raw_olist.products
-- Purpose: Standardize Olist product catalog attributes for product and seller
--          performance modeling.
-- Key fields:
--   product_id                  STRING     Product identifier
--   product_category_name       STRING     Standardized category name
--   product_weight_g            NUMERIC    Product weight in grams
--   ingested_at_utc             TIMESTAMP  Raw ingestion timestamp
-- Update frequency: Daily batch rebuild
-- =============================================================================
with source_data as (

    select
        *
    from {{ source('raw_olist', 'products') }}

),

renamed as (

    select
        nullif(trim(product_id), '') as product_id,
        lower(nullif(trim(product_category_name), '')) as product_category_name,
        safe_cast(product_name_lenght as int64) as product_name_length,
        safe_cast(product_description_lenght as int64)
            as product_description_length,
        safe_cast(product_photos_qty as int64) as product_photos_count,
        safe_cast(product_weight_g as numeric) as product_weight_g,
        safe_cast(product_length_cm as numeric) as product_length_cm,
        safe_cast(product_height_cm as numeric) as product_height_cm,
        safe_cast(product_width_cm as numeric) as product_width_cm,

        nullif(trim(batch_id), '') as batch_id,
        safe_cast(ingested_at_utc as timestamp) as ingested_at_utc,
        nullif(trim(source_file_name), '') as source_file_name

    from source_data

),

deduplicated as (

    select *
    from renamed
    qualify row_number() over (
        partition by product_id
        order by ingested_at_utc desc, batch_id desc
    ) = 1

)

select *
from deduplicated
