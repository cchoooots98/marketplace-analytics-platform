-- Executive KPI card: weight daily review averages by published review volume
-- so sparse days do not distort the selected-period sentiment score.
select
    safe_divide(
        sum(
            case
                when avg_review_score is not null then avg_review_score * reviews_count
                else 0
            end
        ),
        sum(
            case
                when avg_review_score is not null then reviews_count
                else 0
            end
        )
    ) as avg_review_score
from `marts.mart_exec_daily`
where 1 = 1
    [[and {{date_range}}]]
