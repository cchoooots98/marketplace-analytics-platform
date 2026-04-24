-- Executive trend: roll daily volatility into weekly cohorts so the dashboard
-- reads like a management summary instead of an operations monitor.
select
    date_trunc(calendar_date, week(monday)) as calendar_date,
    sum(gmv) as gmv,
    sum(orders_count) as orders_count
from `marts.mart_exec_daily`
where 1 = 1
    [[and {{date_range}}]]
group by 1
order by 1
