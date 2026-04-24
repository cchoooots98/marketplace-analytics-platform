-- Seller operations ranking: use published delivered-order support columns so
-- late delivery stays a service-level rate rather than a demand-level rate.
-- Rank action owners by late-order burden first, then by rate, while
-- prioritizing sellers with enough delivered-order support to make the rate
-- operationally reliable.
with params as (

    select 5 as min_delivered_orders_for_late_rate_ranking

),

seller_rollup as (

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
        late_orders_count,
        delivered_orders_count,
        delivered_orders_count >= min_delivered_orders_for_late_rate_ranking
            as meets_min_delivered_order_support
    from seller_rollup
    cross join params

)

select
    seller_id,
    seller_label,
    seller_state,
    safe_divide(late_orders_count, delivered_orders_count) as late_delivery_rate,
    late_orders_count,
    delivered_orders_count
from ranked_sellers
order by
    case
        when meets_min_delivered_order_support then 0
        else 1
    end,
    case
        when safe_divide(late_orders_count, delivered_orders_count) is null then 1
        else 0
    end,
    late_orders_count desc,
    late_delivery_rate desc,
    delivered_orders_count desc,
    seller_id
limit 15
