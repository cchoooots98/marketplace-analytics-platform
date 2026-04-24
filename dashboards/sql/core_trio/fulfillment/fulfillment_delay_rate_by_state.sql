-- Fulfillment map: regional late-delivery rate weighted by delivered orders.
-- late_delivery_rate_pct is scaled to 0–100 so Metabase map legend shows
-- human-readable percent values instead of raw decimals.
with state_rollup as (

    select
        customer_state,
        sum(orders_count) as orders_count,
        sum(late_orders_count) as late_orders_count,
        sum(delivered_orders_count) as delivered_orders_count
    from `marts.mart_fulfillment_ops`
    where 1 = 1
        [[and {{date_range}}]]
        [[and {{customer_state}}]]
        [[and {{delivery_delay_bucket}}]]
        [[and {{holiday_flag}}]]
    group by customer_state

)

select
    customer_state,
    orders_count,
    late_orders_count,
    delivered_orders_count,
    round(
        safe_divide(late_orders_count, delivered_orders_count) * 100,
        2
    ) as late_delivery_rate_pct
from state_rollup
order by late_delivery_rate_pct desc, orders_count desc, customer_state
