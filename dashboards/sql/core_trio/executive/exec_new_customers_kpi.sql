-- Executive KPI card: customer acquisition stays aligned to the warehouse's
-- first-order contract through the published new_customers_count field.
select
    sum(new_customers_count) as new_customers_count
from `marts.mart_exec_daily`
where 1 = 1
    [[and {{date_range}}]]
