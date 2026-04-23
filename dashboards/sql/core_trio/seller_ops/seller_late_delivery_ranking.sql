-- Seller operations ranking: use published delivered-order support columns so
-- late delivery stays a service-level rate rather than a demand-level rate.
with seller_rollup as (

    select
        seller_id,
        any_value(seller_city) as seller_city,
        any_value(seller_state) as seller_state,
        sum(late_orders_count) as late_orders_count,
        sum(delivered_orders_count) as delivered_orders_count
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
    safe_divide(late_orders_count, delivered_orders_count) as late_delivery_rate,
    late_orders_count,
    delivered_orders_count
from seller_rollup
order by
    case
        when safe_divide(late_orders_count, delivered_orders_count) is null then 1
        else 0
    end,
    late_delivery_rate desc,
    late_orders_count desc,
    seller_id
limit 15
