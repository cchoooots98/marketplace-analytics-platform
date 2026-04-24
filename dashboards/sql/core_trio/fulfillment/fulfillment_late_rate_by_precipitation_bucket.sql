-- Fulfillment weather profile: bucket slice-level proxy precipitation so the
-- dashboard shows business-readable risk bands instead of exploratory scatter.
with bucketed_slices as (

    select
        case
            when avg_delivery_precipitation_total = 0 then "0 mm"
            when avg_delivery_precipitation_total < 1 then ">0 to <1 mm"
            when avg_delivery_precipitation_total < 2 then "1 to <2 mm"
            when avg_delivery_precipitation_total < 4 then "2 to <4 mm"
            when avg_delivery_precipitation_total < 6 then "4 to <6 mm"
            else "6+ mm"
        end as precipitation_bucket,
        case
            when avg_delivery_precipitation_total = 0 then 1
            when avg_delivery_precipitation_total < 1 then 2
            when avg_delivery_precipitation_total < 2 then 3
            when avg_delivery_precipitation_total < 4 then 4
            when avg_delivery_precipitation_total < 6 then 5
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
        and avg_delivery_precipitation_total is not null

)

select
    precipitation_bucket as precipitation_bucket,
    bucket_sort as bucket_sort,
    sum(delivered_orders_count) as delivered_orders_count,
    sum(late_orders_count) as late_orders_count,
    safe_divide(
        sum(late_orders_count),
        nullif(sum(delivered_orders_count), 0)
    ) as late_delivery_rate,
    sum(orders_count) as orders_count
from bucketed_slices
group by precipitation_bucket, bucket_sort
order by bucket_sort
