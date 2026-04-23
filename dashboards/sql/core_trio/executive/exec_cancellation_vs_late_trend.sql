-- Executive trend: separate demand attrition from post-purchase service risk.
select
    calendar_date,
    cancellation_rate,
    late_delivery_rate
from `marts.mart_exec_daily`
where 1 = 1
    [[and {{date_range}}]]
order by calendar_date
