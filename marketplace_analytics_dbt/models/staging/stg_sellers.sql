-- =============================================================================
-- Model: stg_sellers
-- Grain: One row per seller_id
-- Source: raw_olist.sellers
-- Purpose: Standardize Olist sellers for seller operations modeling.
-- Key fields:
--   seller_id                   STRING     Seller identifier
--   seller_state                STRING     Standardized state code
--   ingested_at_utc             TIMESTAMP  Raw ingestion timestamp
-- Update frequency: Daily batch rebuild
-- =============================================================================
with source_data as (

    select
        *
    from {{ source('raw_olist', 'sellers') }}

),

renamed as (

    select
        nullif(trim(seller_id), '') as seller_id,
        safe_cast(seller_zip_code_prefix as int64) as seller_zip_code_prefix,
        lower(nullif(trim(seller_city), '')) as seller_city,
        upper(nullif(trim(seller_state), '')) as seller_state,

        nullif(trim(batch_id), '') as batch_id,
        safe_cast(ingested_at_utc as timestamp) as ingested_at_utc,
        nullif(trim(source_file_name), '') as source_file_name

    from source_data

),

deduplicated as (

    select *
    from renamed
    qualify row_number() over (
        partition by seller_id
        order by ingested_at_utc desc, batch_id desc
    ) = 1

)

select *
from deduplicated
