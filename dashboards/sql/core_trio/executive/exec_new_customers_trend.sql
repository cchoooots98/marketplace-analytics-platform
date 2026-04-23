-- Executive trend: first-time customer acquisition over the selected window.
select
    calendar_date,
    new_customers_count
from `marts.mart_exec_daily`
where 1 = 1
    [[and {{date_range}}]]
order by calendar_date
