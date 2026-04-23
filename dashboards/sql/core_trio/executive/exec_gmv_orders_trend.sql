-- Executive trend: show revenue and demand on the same purchase-date cohort.
select
    calendar_date,
    gmv,
    orders_count
from `marts.mart_exec_daily`
where 1 = 1
    [[and {{date_range}}]]
order by calendar_date
