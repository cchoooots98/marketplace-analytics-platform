-- Executive KPI card: roll up the published cancellation numerator and demand
-- denominator from the executive mart.
select
    safe_divide(
        sum(cancelled_orders_count),
        sum(orders_count)
    ) as cancellation_rate
from `marts.mart_exec_daily`
where 1 = 1
    [[and {{date_range}}]]
