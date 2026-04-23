-- Fulfillment map: regional late-delivery rate weighted by delivered orders.
select
    customer_state,
    sum(orders_count) as orders_count,
    sum(late_orders_count) as late_orders_count,
    sum(delivered_orders_count) as delivered_orders_count,
    safe_divide(
        sum(late_orders_count),
        sum(delivered_orders_count)
    ) as late_delivery_rate
from `marts.mart_fulfillment_ops`
where 1 = 1
    [[and {{date_range}}]]
    [[and {{customer_state}}]]
    [[and {{delivery_delay_bucket}}]]
    [[and {{holiday_flag}}]]
group by customer_state
order by late_delivery_rate desc, orders_count desc, customer_state
