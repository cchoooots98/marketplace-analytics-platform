-- Fulfillment bar chart: aggregate the mart-published late-day numerator and
-- late-order support count so severity stays additive across slices.
select
    customer_state,
    safe_divide(
        sum(late_days_sum),
        nullif(sum(late_orders_count), 0)
    ) as avg_late_days,
    sum(late_orders_count) as late_orders_count
from `marts.mart_fulfillment_ops`
where 1 = 1
    [[and {{date_range}}]]
    [[and {{customer_state}}]]
    [[and {{delivery_delay_bucket}}]]
    [[and {{holiday_flag}}]]
group by customer_state
order by avg_late_days desc, late_orders_count desc, customer_state
