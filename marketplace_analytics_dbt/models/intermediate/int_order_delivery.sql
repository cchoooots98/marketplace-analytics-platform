-- =============================================================================
-- Model: int_order_delivery
-- Grain: One row per order_id
-- Source: stg_orders, stg_holidays, stg_weather_daily
-- Purpose: Calculate delivery SLA flags, late delivery day counts, and
--          cancellation status. Optionally enrich with Brazilian public holiday
--          context at purchase date and configured proxy weather context at
--          delivery date.
--          This is the single authoritative delivery reference for all marts.
-- Key fields:
--   order_id                        STRING     Primary order identifier
--   is_delivered                    BOOL       True when status=delivered and
--                                              delivery timestamp is not null
--   is_late                         BOOL       True when delivered after estimate
--   late_days                       INT64      Calendar days past estimate; null
--                                              when not late
--   is_cancelled                    BOOL       True when status=canceled
--   purchase_date                   DATE       Calendar date of purchase
--   delivery_date                   DATE       Calendar date of delivery; nullable
--   is_purchase_on_holiday          BOOL       True when purchase date is a
--                                              Brazilian public holiday
--   holiday_name_at_purchase        STRING     Holiday name(s); nullable
--   delivery_weather_location_key   STRING     Configured weather location; nullable
--   delivery_temperature_max        FLOAT64    Max temp on delivery day; nullable
--   delivery_temperature_min        FLOAT64    Min temp on delivery day; nullable
--   delivery_precipitation_total    FLOAT64    Precipitation on delivery day; nullable
--   delivery_humidity_afternoon     FLOAT64    Afternoon humidity on delivery day; nullable
-- Update frequency: Daily batch rebuild
-- =============================================================================

with orders as (

    select
        order_id,
        customer_id,
        order_status,
        order_purchased_at_utc,
        order_approved_at_utc,
        order_delivered_to_carrier_at_utc,
        order_delivered_to_customer_at_utc,
        order_estimated_delivery_at_utc
    from {{ ref('stg_orders') }}

),

-- Collapse holidays to one row per date before joining to orders.
-- stg_holidays grain is (holiday_date, country_code, holiday_name): a single
-- calendar date can have multiple rows when multiple holidays share a date
-- (e.g. Carnival Monday and Carnival Tuesday). Without this collapse a LEFT JOIN
-- to orders would fan-out, one order becoming two rows. STRING_AGG produces a
-- comma-separated label when multiple holidays share a date.
holidays_by_date as (

    select
        holiday_date,
        country_code,
        string_agg(holiday_name order by holiday_name) as holiday_names
    from {{ ref('stg_holidays') }}
    where country_code = 'BR'
    group by holiday_date, country_code

),

-- The current weather ingestion loads a configured proxy location
-- (default: sao_paulo), not per-customer regional weather. Keeping this as a
-- dbt env-driven filter avoids pretending the enrichment has order-level
-- geographic precision.
weather_daily as (

    select
        weather_date,
        location_key,
        temperature_max,
        temperature_min,
        precipitation_total,
        humidity_afternoon
    from {{ ref('stg_weather_daily') }}
    where
        location_key
        = lower('{{ env_var("OPENWEATHER_LOCATION_KEY", "sao_paulo") }}')

),

delivery_flags as (

    select
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchased_at_utc,
        o.order_approved_at_utc,
        o.order_delivered_to_carrier_at_utc,
        o.order_delivered_to_customer_at_utc,
        o.order_estimated_delivery_at_utc,

        -- Both conditions must hold: a status='delivered' row with a null
        -- delivery timestamp is not treated as delivered.
        (
            o.order_status = 'delivered'
            and o.order_delivered_to_customer_at_utc is not null
        ) as is_delivered,

        -- Late only when we have both the actual and estimated delivery timestamps.
        -- NULL estimated delivery means lateness is indeterminate, not late.
        (
            o.order_status = 'delivered'
            and o.order_delivered_to_customer_at_utc is not null
            and o.order_estimated_delivery_at_utc is not null
            and date(o.order_delivered_to_customer_at_utc)
                > date(o.order_estimated_delivery_at_utc)
        ) as is_late,

        -- DATE_DIFF at day granularity matches the business SLA definition.
        -- Same-day deliveries are on-time even when the source estimate is
        -- stored at midnight.
        case
            when
                o.order_status = 'delivered'
                and o.order_delivered_to_customer_at_utc is not null
                and o.order_estimated_delivery_at_utc is not null
                and date(o.order_delivered_to_customer_at_utc)
                    > date(o.order_estimated_delivery_at_utc)
            then date_diff(
                date(o.order_delivered_to_customer_at_utc),
                date(o.order_estimated_delivery_at_utc),
                day
            )
            else null
        end as late_days,

        (o.order_status = 'canceled') as is_cancelled,

        date(o.order_purchased_at_utc)             as purchase_date,
        date(o.order_delivered_to_customer_at_utc) as delivery_date

    from orders as o

),

enriched_with_holidays as (

    select
        df.*,
        (h.holiday_date is not null) as is_purchase_on_holiday,
        h.holiday_names            as holiday_name_at_purchase
    from delivery_flags as df
    left join holidays_by_date as h
        on df.purchase_date = h.holiday_date

),

enriched_with_weather as (

    select
        ewh.*,
        w.location_key          as delivery_weather_location_key,
        w.temperature_max      as delivery_temperature_max,
        w.temperature_min      as delivery_temperature_min,
        w.precipitation_total  as delivery_precipitation_total,
        w.humidity_afternoon   as delivery_humidity_afternoon
    from enriched_with_holidays as ewh
    left join weather_daily as w
        on ewh.delivery_date = w.weather_date

),

final as (

    select
        order_id,
        customer_id,
        order_status,
        order_purchased_at_utc,
        order_approved_at_utc,
        order_delivered_to_carrier_at_utc,
        order_delivered_to_customer_at_utc,
        order_estimated_delivery_at_utc,
        purchase_date,
        delivery_date,
        is_delivered,
        is_late,
        late_days,
        is_cancelled,
        is_purchase_on_holiday,
        holiday_name_at_purchase,
        delivery_weather_location_key,
        delivery_temperature_max,
        delivery_temperature_min,
        delivery_precipitation_total,
        delivery_humidity_afternoon
    from enriched_with_weather

)

select * from final
