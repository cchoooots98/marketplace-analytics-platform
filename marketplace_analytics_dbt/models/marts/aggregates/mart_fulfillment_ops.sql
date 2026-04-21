-- =============================================================================
-- Model: mart_fulfillment_ops
-- Grain: One row per purchase_date, customer_state, and delivery_delay_bucket
-- Source: fact_orders
-- Purpose: Fulfillment operations mart. Publishes order-population metrics only:
--          volume, delivery outcomes, late-day severity, holiday context, and
--          proxy delivery-weather context. Customer-experience metrics live in a
--          dedicated mart so operational and experience contracts stay cleanly
--          separated.
-- Key measures:
--   orders_count                        INT64    Orders in the cohort slice
--   delivered_orders_count              INT64    Delivered orders in the slice
--   late_orders_count                   INT64    Late delivered orders in the slice
--   cancelled_orders_count              INT64    Cancelled orders in the slice
--   avg_late_days                       FLOAT64  Average late_days among late orders
--   late_delivery_rate                  NUMERIC  late_orders_count / delivered_orders_count
-- Update frequency: Daily batch rebuild. Full rebuild is intentional because
--                   this mart is small, audit-friendly, and derived from
--                   conformed facts. Weather fields are proxy order-weighted
--                   averages across each slice's delivery-date distribution,
--                   not a single "weather of the purchase day" snapshot.
-- =============================================================================

{{
    config(
        materialized='table',
        contract={'enforced': true},
        partition_by={
            'field': 'purchase_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['customer_state']
    )
}}

with orders_with_context as (

    select
        order_id,
        purchase_date,
        customer_state_at_order as customer_state,
        is_delivered,
        is_late,
        late_days,
        is_cancelled,
        delivery_weather_location_key,
        delivery_temperature_max,
        delivery_temperature_min,
        delivery_precipitation_total,
        delivery_humidity_afternoon,
        {{ delivery_delay_bucket(
            'is_delivered',
            'is_cancelled',
            'is_late',
            'late_days'
        ) }} as delivery_delay_bucket
    from {{ ref('fact_orders') }}

),

date_context as (

    select
        calendar_date,
        is_holiday,
        holiday_name
    from {{ ref('dim_date') }}

),

final as (

    select
        purchase_date,
        customer_state,
        delivery_delay_bucket,
        count(*) as orders_count,
        countif(is_delivered) as delivered_orders_count,
        countif(is_late) as late_orders_count,
        countif(is_cancelled) as cancelled_orders_count,
        avg(case
            when is_late then cast(late_days as float64)
        end) as avg_late_days,
        safe_divide(
            countif(is_late),
            nullif(countif(is_delivered), 0)
        ) as late_delivery_rate,
        -- Holiday meaning is owned by dim_date and reused here so purchase-date
        -- seasonality semantics stay conformed across facts and marts.
        any_value(dc.is_holiday) as is_purchase_on_holiday,
        any_value(dc.holiday_name) as holiday_name_at_purchase,
        any_value(delivery_weather_location_key) as delivery_weather_location_key,
        avg(cast(delivery_temperature_max as float64))
            as avg_delivery_temperature_max,
        avg(cast(delivery_temperature_min as float64))
            as avg_delivery_temperature_min,
        avg(cast(delivery_precipitation_total as float64))
            as avg_delivery_precipitation_total,
        avg(cast(delivery_humidity_afternoon as float64))
            as avg_delivery_humidity_afternoon
    from orders_with_context as owc
    inner join date_context as dc
        on owc.purchase_date = dc.calendar_date
    group by purchase_date, customer_state, delivery_delay_bucket

)

select * from final
