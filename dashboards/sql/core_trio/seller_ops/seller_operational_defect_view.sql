-- Seller operations action list: operational defect means cancelled or late at
-- the seller-order population and is already defined in the mart contract.
-- Prioritize scaled sellers first so the combined defect list reflects
-- operational burden, not just extreme percentages on tiny seller populations.
with params as (

    select 5 as min_orders_for_operational_defect_ranking

),

seller_rollup as (

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

),

ranked_sellers as (

    select
        seller_id,
        case
            when seller_city is null or seller_state is null
                then concat(left(seller_id, 8), "...")
            else concat(
                seller_city, ", ", seller_state, " (", left(seller_id, 8), "...)"
            )
        end as seller_label,
        seller_state,
        operational_defect_orders_count,
        orders_count,
        orders_count >= min_orders_for_operational_defect_ranking
            as meets_min_order_support
    from seller_rollup
    cross join params

)

select
    seller_id,
    seller_label,
    seller_state,
    operational_defect_orders_count,
    orders_count,
    safe_divide(
        operational_defect_orders_count,
        orders_count
    ) as operational_defect_rate
from ranked_sellers
order by
    case
        when meets_min_order_support then 0
        else 1
    end,
    case
        when safe_divide(operational_defect_orders_count, orders_count) is null
            then 1
        else 0
    end,
    operational_defect_orders_count desc,
    operational_defect_rate desc,
    orders_count desc,
    seller_id
limit 15
