-- Seller operations ranking: aggregate published GMV and commercial-order
-- counts to compare ticket size across sellers on the filtered date window.
-- Support-aware ranking keeps one-off luxury orders from crowding out scaled
-- commercial sellers while still allowing low-support sellers to appear when
-- the filter window is narrow.
with params as (

    select 5 as min_non_cancelled_orders_for_aov_ranking

),

seller_rollup as (

    select
        seller_id,
        any_value(seller_city) as seller_city,
        any_value(seller_state) as seller_state,
        sum(gmv) as gmv,
        sum(non_cancelled_orders_count) as non_cancelled_orders_count
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
        gmv,
        non_cancelled_orders_count,
        non_cancelled_orders_count >= min_non_cancelled_orders_for_aov_ranking
            as meets_min_non_cancelled_order_support
    from seller_rollup
    cross join params

)

select
    seller_id,
    seller_label,
    seller_state,
    safe_divide(gmv, non_cancelled_orders_count) as aov,
    gmv,
    non_cancelled_orders_count
from ranked_sellers
order by
    case
        when meets_min_non_cancelled_order_support then 0
        else 1
    end,
    case
        when safe_divide(gmv, non_cancelled_orders_count) is null then 1
        else 0
    end,
    aov desc,
    gmv desc,
    non_cancelled_orders_count desc,
    seller_id
limit 15
