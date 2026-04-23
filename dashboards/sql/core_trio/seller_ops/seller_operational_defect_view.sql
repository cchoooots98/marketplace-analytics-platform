-- Seller operations action list: operational defect means cancelled or late at
-- the seller-order population and is already defined in the mart contract.
with seller_rollup as (

    select
        seller_id,
        any_value(seller_city) as seller_city,
        any_value(seller_state) as seller_state,
        sum(operational_defect_orders_count) as operational_defect_orders_count,
        sum(orders_count) as orders_count
    from `marts.mart_seller_performance`
    left join `marts.dim_seller`
        using (seller_id)
    where 1 = 1
        [[and {{date_range}}]]
        [[and {{seller_id}}]]
        [[and {{seller_state}}]]
    group by seller_id

)

select
    seller_id,
    case
        when seller_city is null or seller_state is null then seller_id
        else concat(seller_city, ", ", seller_state, " (", seller_id, ")")
    end as seller_label,
    seller_state,
    operational_defect_orders_count,
    orders_count,
    safe_divide(operational_defect_orders_count, orders_count) as operational_defect_rate
from seller_rollup
order by
    case
        when safe_divide(operational_defect_orders_count, orders_count) is null then 1
        else 0
    end,
    operational_defect_rate desc,
    operational_defect_orders_count desc,
    seller_id
limit 15
