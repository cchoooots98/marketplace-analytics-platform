-- Executive trend: aggregate to weekly cohorts so acquisition direction is
-- easier to read in stakeholder reviews.
select
    date_trunc(calendar_date, week(monday)) as calendar_date,
    sum(new_customers_count) as new_customers_count
from `marts.mart_exec_daily`
where 1 = 1
    [[and {{date_range}}]]
group by 1
order by 1
