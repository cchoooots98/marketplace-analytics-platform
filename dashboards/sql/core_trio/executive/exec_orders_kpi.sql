-- Executive KPI card: demand volume across the selected purchase-date window.
select
    sum(orders_count) as orders_count
from `marts.mart_exec_daily`
where 1 = 1
    [[and {{date_range}}]]
