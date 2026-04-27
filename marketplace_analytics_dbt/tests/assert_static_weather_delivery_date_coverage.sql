-- Static enrichment coverage: delivered orders should resolve weather for the
-- configured proxy location on the delivery date. Weather values remain
-- nullable at model runtime, but a completed static backfill should cover the
-- delivery-date window it was loaded for.
with delivered_order_dates as (

    select distinct
        date(order_delivered_to_customer_at_utc) as delivery_date
    from {{ ref('stg_orders') }}
    where
        order_status = 'delivered'
        and order_delivered_to_customer_at_utc is not null

),

weather_dates as (

    select distinct
        weather_date
    from {{ ref('stg_weather_daily') }}
    where
        location_key
        = lower('{{ env_var("OPENWEATHER_LOCATION_KEY", "sao_paulo") }}')

)

select
    delivered_order_dates.delivery_date
from delivered_order_dates
left join weather_dates
    on delivered_order_dates.delivery_date = weather_dates.weather_date
where weather_dates.weather_date is null
