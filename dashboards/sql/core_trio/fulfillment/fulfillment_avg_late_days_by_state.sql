-- Fulfillment bar chart: weight late-day averages by the late-order population
-- so severe but sparse slices do not dominate the regional ranking.
select
    customer_state,
    safe_divide(
        sum(
            case
                when avg_late_days is not null then avg_late_days * late_orders_count
                else 0
            end
        ),
        sum(
            case
                when avg_late_days is not null then late_orders_count
                else 0
            end
        )
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
