-- =============================================================================
-- Model: stg_holidays
-- Grain: One row per holiday_date, country_code, and holiday_name
-- Source: raw_ext.holidays
-- Purpose: Standardize public holiday records for date enrichment.
-- Key fields:
--   holiday_date                DATE       Holiday calendar date
--   country_code                STRING     ISO country code
--   holiday_name                STRING     Holiday name
--   ingested_at_utc             TIMESTAMP  Raw ingestion timestamp
-- Update frequency: Daily batch rebuild
-- =============================================================================
with source_data as (

    select
        *
    from {{ source('raw_ext', 'holidays') }}

),

renamed as (

    select
        safe_cast(holiday_date as date) as holiday_date,
        nullif(trim(local_name), '') as local_name,
        nullif(trim(holiday_name), '') as holiday_name,
        upper(nullif(trim(country_code), '')) as country_code,
        safe_cast(is_global as bool) as is_global,
        nullif(trim(counties_json), '') as counties_json,
        nullif(trim(holiday_types_json), '') as holiday_types_json,
        safe_cast(launch_year as int64) as launch_year,

        nullif(trim(batch_id), '') as batch_id,
        safe_cast(ingested_at_utc as timestamp) as ingested_at_utc,
        nullif(trim(source_file_name), '') as source_file_name

    from source_data

),

deduplicated as (

    select *
    from renamed
    qualify row_number() over (
        partition by holiday_date, country_code, holiday_name
        order by ingested_at_utc desc, batch_id desc
    ) = 1

)

select *
from deduplicated
