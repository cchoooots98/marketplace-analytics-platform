-- Fulfillment weather profile: bucket slice-level proxy max temperature so the
-- dashboard communicates directional risk bands instead of exploration clouds.
with bucketed_slices as (

    select
        case
            when avg_delivery_temperature_max < 22 then "<22 C"
            when avg_delivery_temperature_max < 24 then "22 to <24 C"
            when avg_delivery_temperature_max < 26 then "24 to <26 C"
            when avg_delivery_temperature_max < 28 then "26 to <28 C"
            when avg_delivery_temperature_max < 30 then "28 to <30 C"
            else "30+ C"
        end as temperature_bucket,
        case
            when avg_delivery_temperature_max < 22 then 1
            when avg_delivery_temperature_max < 24 then 2
            when avg_delivery_temperature_max < 26 then 3
            when avg_delivery_temperature_max < 28 then 4
            when avg_delivery_temperature_max < 30 then 5
            else 6
        end as bucket_sort,
        delivered_orders_count,
        late_orders_count,
        orders_count
    from `marts.mart_fulfillment_ops`
    where 1 = 1
        [[and {{date_range}}]]
        [[and {{customer_state}}]]
        [[and {{delivery_delay_bucket}}]]
        [[and {{holiday_flag}}]]
        and avg_delivery_temperature_max is not null

)

select
    temperature_bucket as temperature_bucket,
    bucket_sort as bucket_sort,
    sum(delivered_orders_count) as delivered_orders_count,
    sum(late_orders_count) as late_orders_count,
    safe_divide(
        sum(late_orders_count),
        nullif(sum(delivered_orders_count), 0)
    ) as late_delivery_rate,
    sum(orders_count) as orders_count
from bucketed_slices
group by temperature_bucket, bucket_sort
order by bucket_sort
