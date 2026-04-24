-- Seller operations ranking: keep the numerator and denominator aligned to the
-- published seller-order population.
-- Prioritize sellers with enough seller-order support for a stable rate, then
-- sort by cancelled-order burden so the table behaves like an action list
-- instead of a small-sample outlier board.
with params as (

    select 5 as min_orders_for_cancellation_rate_ranking

),

seller_rollup as (

    select
        seller_id,
        any_value(seller_city) as seller_city,
        any_value(seller_state) as seller_state,
        sum(cancelled_orders_count) as cancelled_orders_count,
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
        cancelled_orders_count,
        orders_count,
        orders_count >= min_orders_for_cancellation_rate_ranking
            as meets_min_order_support
    from seller_rollup
    cross join params

)

select
    seller_id,
    seller_label,
    seller_state,
    safe_divide(cancelled_orders_count, orders_count) as cancellation_rate,
    cancelled_orders_count,
    orders_count
from ranked_sellers
order by
    case
        when meets_min_order_support then 0
        else 1
    end,
    case
        when safe_divide(cancelled_orders_count, orders_count) is null then 1
        else 0
    end,
    cancelled_orders_count desc,
    cancellation_rate desc,
    orders_count desc,
    seller_id
limit 15
