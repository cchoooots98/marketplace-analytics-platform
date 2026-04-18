-- =============================================================================
-- Model: stg_weather_daily
-- Grain: One row per weather_date and location_key
-- Source: raw_ext.weather_daily
-- Purpose: Standardize daily weather summaries for fulfillment and operations
--          enrichment.
-- Key fields:
--   weather_date                DATE       Weather calendar date
--   location_key                STRING     Stable weather location key
--   temperature_min             FLOAT      Minimum daily temperature
--   temperature_max             FLOAT      Maximum daily temperature
--   ingested_at_utc             TIMESTAMP  Raw ingestion timestamp
-- Update frequency: Daily batch rebuild
-- =============================================================================
with source_data as (

    select
        *
    from {{ source('raw_ext', 'weather_daily') }}

),

renamed as (

    select
        safe_cast(weather_date as date) as weather_date,
        lower(nullif(trim(location_key), '')) as location_key,
        safe_cast(latitude as float64) as latitude,
        safe_cast(longitude as float64) as longitude,
        nullif(trim(timezone), '') as timezone,
        lower(nullif(trim(units), '')) as units,

        safe_cast(cloud_cover_afternoon as float64) as cloud_cover_afternoon,
        safe_cast(humidity_afternoon as float64) as humidity_afternoon,
        safe_cast(precipitation_total as float64) as precipitation_total,
        safe_cast(temperature_min as float64) as temperature_min,
        safe_cast(temperature_max as float64) as temperature_max,
        safe_cast(temperature_afternoon as float64) as temperature_afternoon,
        safe_cast(temperature_night as float64) as temperature_night,
        safe_cast(temperature_evening as float64) as temperature_evening,
        safe_cast(temperature_morning as float64) as temperature_morning,
        safe_cast(pressure_afternoon as float64) as pressure_afternoon,
        safe_cast(wind_max_speed as float64) as wind_max_speed,
        safe_cast(wind_max_direction as float64) as wind_max_direction,

        nullif(trim(batch_id), '') as batch_id,
        safe_cast(ingested_at_utc as timestamp) as ingested_at_utc,
        nullif(trim(source_file_name), '') as source_file_name

    from source_data

),

deduplicated as (

    select *
    from renamed
    qualify row_number() over (
        partition by weather_date, location_key
        order by ingested_at_utc desc, batch_id desc
    ) = 1

)

select *
from deduplicated
