-- Executive trend: aggregate to weekly cohorts and recompute the published
-- rates from numerator and denominator support columns to avoid averaging daily
-- percentages blindly.
select
    date_trunc(calendar_date, week(monday)) as calendar_date,
    safe_divide(
        sum(cancelled_orders_count),
        sum(orders_count)
    ) as cancellation_rate,
    safe_divide(
        sum(late_orders_count),
        sum(delivered_orders_count)
    ) as late_delivery_rate
from `marts.mart_exec_daily`
where 1 = 1
    [[and {{date_range}}]]
group by 1
order by 1
