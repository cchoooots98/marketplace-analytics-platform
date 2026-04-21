-- Contract test: fact_orders must preserve the source customer mapping and the
-- order-time customer geography snapshot. This is the warehouse guardrail for
-- the V1 pattern "current-state dim_customer + order-time snapshots on facts".
select
    fo.order_id,
    fo.customer_id as fact_customer_id,
    so.customer_id as source_customer_id,
    fo.customer_unique_id as fact_customer_unique_id,
    sc.customer_unique_id as source_customer_unique_id,
    fo.customer_zip_code_prefix_at_order as fact_zip_code_prefix,
    sc.customer_zip_code_prefix as source_zip_code_prefix,
    fo.customer_city_at_order as fact_customer_city,
    sc.customer_city as source_customer_city,
    fo.customer_state_at_order as fact_customer_state,
    sc.customer_state as source_customer_state
from {{ ref('fact_orders') }} as fo
left join {{ ref('stg_orders') }} as so
    on fo.order_id = so.order_id
left join {{ ref('stg_customers') }} as sc
    on so.customer_id = sc.customer_id
where
    so.order_id is null
    or fo.customer_id != so.customer_id
    or coalesce(fo.customer_unique_id, '__NULL__')
        != coalesce(sc.customer_unique_id, '__NULL__')
    or coalesce(cast(fo.customer_zip_code_prefix_at_order as string), '__NULL__')
        != coalesce(cast(sc.customer_zip_code_prefix as string), '__NULL__')
    or coalesce(fo.customer_city_at_order, '__NULL__')
        != coalesce(sc.customer_city, '__NULL__')
    or coalesce(fo.customer_state_at_order, '__NULL__')
        != coalesce(sc.customer_state, '__NULL__')
