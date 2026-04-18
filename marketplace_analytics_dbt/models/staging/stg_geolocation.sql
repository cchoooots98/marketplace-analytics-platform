-- =============================================================================
-- Model: stg_geolocation
-- Grain: One row per postal code, city, state, and rounded coordinate pair
-- Source: raw_olist.geolocation
-- Purpose: Standardize Olist postal-code geolocation observations while keeping
--          postal-code-level ambiguity visible for downstream modeling.
-- Key fields:
--   geolocation_observation_key STRING     Stable key for the staged observation
--   geolocation_zip_code_prefix INTEGER    Postal code prefix
--   geolocation_lat             FLOAT      Latitude
--   geolocation_lng             FLOAT      Longitude
--   ingested_at_utc             TIMESTAMP  Raw ingestion timestamp
-- Update frequency: Daily batch rebuild
-- =============================================================================
with source_data as (

    select
        *
    from {{ source('raw_olist', 'geolocation') }}

),

renamed as (

    select
        safe_cast(geolocation_zip_code_prefix as int64)
            as geolocation_zip_code_prefix,
        safe_cast(geolocation_lat as float64) as geolocation_lat,
        safe_cast(geolocation_lng as float64) as geolocation_lng,
        lower(nullif(trim(geolocation_city), '')) as geolocation_city,
        upper(nullif(trim(geolocation_state), '')) as geolocation_state,

        nullif(trim(batch_id), '') as batch_id,
        safe_cast(ingested_at_utc as timestamp) as ingested_at_utc,
        nullif(trim(source_file_name), '') as source_file_name

    from source_data

),

dedupe_keyed as (

    select
        renamed.*,
        -- BigQuery cannot partition analytic windows by FLOAT64. Formatting to
        -- six decimal places gives a stable geospatial key at postal-code grain.
        to_hex(md5(concat(
            coalesce(cast(geolocation_zip_code_prefix as string), '__null__'),
            '|',
            coalesce(format('%.6f', geolocation_lat), '__null__'),
            '|',
            coalesce(format('%.6f', geolocation_lng), '__null__'),
            '|',
            coalesce(geolocation_city, '__null__'),
            '|',
            coalesce(geolocation_state, '__null__')
        ))) as geolocation_observation_key

    from renamed

),

deduplicated as (

    select *
    from dedupe_keyed
    qualify row_number() over (
        partition by geolocation_observation_key
        order by ingested_at_utc desc, batch_id desc
    ) = 1

)

select *
from deduplicated
