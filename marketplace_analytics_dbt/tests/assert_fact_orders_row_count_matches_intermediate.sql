-- Regression test: fact_orders must preserve the exact order_id set published
-- by int_order_delivery. This catches silent drops or unexpected extra rows
-- even when the total row counts happen to stay equal.
with missing_from_fact as (

    select
        i.order_id,
        'missing_from_fact_orders' as mismatch_type
    from {{ ref('int_order_delivery') }} as i
    left join {{ ref('fact_orders') }} as f
        on i.order_id = f.order_id
    where f.order_id is null

),

extra_in_fact as (

    select
        f.order_id,
        'extra_in_fact_orders' as mismatch_type
    from {{ ref('fact_orders') }} as f
    left join {{ ref('int_order_delivery') }} as i
        on f.order_id = i.order_id
    where i.order_id is null

)

select *
from missing_from_fact

union all

select *
from extra_in_fact
