-- =============================================================================
-- Model: stg_customers
-- Grain: One row per customer_id
-- Source: raw_olist.customers
-- Purpose: Standardize Olist customers for customer and geography modeling.
-- Key fields:
--   customer_id                 STRING     Customer identifier
--   customer_unique_id          STRING     Stable customer identifier
--   customer_state              STRING     Standardized state code
--   ingested_at_utc             TIMESTAMP  Raw ingestion timestamp
-- Update frequency: Daily batch rebuild
-- =============================================================================
with source_data as (

    select
        *
    from {{ source('raw_olist', 'customers') }}

),

renamed as (

    select
        nullif(trim(customer_id), '') as customer_id,
        nullif(trim(customer_unique_id), '') as customer_unique_id,
        safe_cast(customer_zip_code_prefix as int64) as customer_zip_code_prefix,
        lower(nullif(trim(customer_city), '')) as customer_city,
        upper(nullif(trim(customer_state), '')) as customer_state,

        nullif(trim(batch_id), '') as batch_id,
        safe_cast(ingested_at_utc as timestamp) as ingested_at_utc,
        nullif(trim(source_file_name), '') as source_file_name

    from source_data

),

deduplicated as (

    select *
    from renamed
    qualify row_number() over (
        partition by customer_id
        order by ingested_at_utc desc, batch_id desc
    ) = 1

)

select *
from deduplicated
