-- Coverage test: mart_exec_daily intentionally emits only non-empty order
-- dates, so every purchase_date present in fact_orders must appear exactly
-- once in the mart and the mart must not emit extra dates.
with expected as (

    select distinct
        purchase_date as calendar_date
    from {{ ref('fact_orders') }}

),

actual as (

    select
        calendar_date
    from {{ ref('mart_exec_daily') }}

)

select
    coalesce(expected.calendar_date, actual.calendar_date) as calendar_date
from expected
full outer join actual
    on expected.calendar_date = actual.calendar_date
where
    expected.calendar_date is null
    or actual.calendar_date is null
