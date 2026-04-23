-- Executive trend: daily customer sentiment on the same purchase-date cohort
-- as the commercial KPIs.
select
    calendar_date,
    avg_review_score,
    reviews_count
from `marts.mart_exec_daily`
where 1 = 1
    [[and {{date_range}}]]
order by calendar_date
