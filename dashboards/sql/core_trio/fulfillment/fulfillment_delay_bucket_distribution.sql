-- Fulfillment composition chart: return both the full-population mix and the
-- late-only severity mix so a 100% stacked bar can show structure without the
-- on-time population hiding the operational buckets.
with bucket_rollup as (

    select
        delivery_delay_bucket,
        sum(orders_count) as orders_count
    from `marts.mart_fulfillment_ops`
    where 1 = 1
        [[and {{date_range}}]]
        [[and {{customer_state}}]]
        [[and {{delivery_delay_bucket}}]]
        [[and {{holiday_flag}}]]
    group by delivery_delay_bucket

),

distribution_views as (

    select
        "All Orders Mix" as distribution_view,
        delivery_delay_bucket,
        orders_count
    from bucket_rollup

    union all

    select
        "Late / Failed Mix" as distribution_view,
        delivery_delay_bucket,
        orders_count
    from bucket_rollup
    where delivery_delay_bucket != "on_time"

)

select
    distribution_view,
    case delivery_delay_bucket
        when "not_delivered" then "Not Delivered"
        when "on_time" then "On Time"
        when "1_to_3_days" then "1-3 Days Late"
        when "4_to_7_days" then "4-7 Days Late"
        when "8_to_14_days" then "8-14 Days Late"
        when "15_plus_days" then "15+ Days Late"
        else delivery_delay_bucket
    end as delay_bucket,
    orders_count
from distribution_views
order by
    case distribution_view
        when "All Orders Mix" then 1
        when "Late / Failed Mix" then 2
        else 3
    end,
    case delivery_delay_bucket
        when "on_time" then 1
        when "1_to_3_days" then 2
        when "4_to_7_days" then 3
        when "8_to_14_days" then 4
        when "15_plus_days" then 5
        when "not_delivered" then 6
        else 7
    end
