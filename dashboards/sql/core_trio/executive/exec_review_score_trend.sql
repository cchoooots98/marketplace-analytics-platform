-- Executive trend: aggregate sentiment to weekly cohorts using the mart-
-- published additive review numerator and support count.
select
    date_trunc(calendar_date, week(monday)) as calendar_date,
    safe_divide(
        sum(review_score_sum),
        nullif(sum(reviews_count), 0)
    ) as avg_review_score,
    sum(reviews_count) as reviews_count
from `marts.mart_exec_daily`
where 1 = 1
    [[and {{date_range}}]]
group by 1
order by 1
