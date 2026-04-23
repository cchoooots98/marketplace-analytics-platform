-- Executive KPI card: aggregate published numerator and denominator support
-- fields instead of re-reading facts or averaging daily AOV blindly.
select
    safe_divide(
        sum(gmv),
        sum(non_cancelled_orders_count)
    ) as aov
from `marts.mart_exec_daily`
where 1 = 1
    [[and {{date_range}}]]
