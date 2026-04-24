-- Executive KPI card: aggregate the mart-published additive review numerator
-- and support count so BI does not have to reverse-engineer weighting from a
-- pre-averaged daily column.
select
    safe_divide(
        sum(review_score_sum),
        nullif(sum(reviews_count), 0)
    ) as avg_review_score
from `marts.mart_exec_daily`
where 1 = 1
    [[and {{date_range}}]]
