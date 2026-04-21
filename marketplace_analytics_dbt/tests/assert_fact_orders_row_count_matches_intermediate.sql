-- Regression test: fact_orders must preserve every order_id from
-- int_order_delivery. A silent drop between the intermediate and marts layer
-- would hide cancelled or undelivered orders from executive dashboards. The
-- test passes when the row counts match exactly.
with fact_count as (
    select count(*) as row_count
    from {{ ref('fact_orders') }}
),
intermediate_count as (
    select count(*) as row_count
    from {{ ref('int_order_delivery') }}
)
select
    fact_count.row_count      as fact_orders_count,
    intermediate_count.row_count as int_order_delivery_count,
    fact_count.row_count - intermediate_count.row_count as delta
from fact_count, intermediate_count
where fact_count.row_count != intermediate_count.row_count
