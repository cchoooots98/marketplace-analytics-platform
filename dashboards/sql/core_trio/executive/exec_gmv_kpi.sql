-- Executive KPI card: roll up only the published GMV field from mart_exec_daily.
-- This keeps cancelled-order exclusions owned by the warehouse contract.
select
    sum(gmv) as gmv
from `marts.mart_exec_daily`
where 1 = 1
    [[and {{date_range}}]]
