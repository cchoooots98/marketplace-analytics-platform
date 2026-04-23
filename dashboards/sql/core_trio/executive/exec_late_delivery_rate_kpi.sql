-- Executive KPI card: late delivery is always a delivered-order rate, so the
-- dashboard uses the published delivered denominator support column.
select
    safe_divide(
        sum(late_orders_count),
        sum(delivered_orders_count)
    ) as late_delivery_rate
from `marts.mart_exec_daily`
where 1 = 1
    [[and {{date_range}}]]
