-- Seller operations ranking: aggregate published GMV and commercial-order
-- counts to compare ticket size across sellers on the filtered date window.
with seller_rollup as (

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

)

select
    seller_id,
    case
        when seller_city is null or seller_state is null then seller_id
        else concat(seller_city, ", ", seller_state, " (", seller_id, ")")
    end as seller_label,
    seller_state,
    safe_divide(gmv, non_cancelled_orders_count) as aov,
    gmv,
    non_cancelled_orders_count
from seller_rollup
order by
    case
        when safe_divide(gmv, non_cancelled_orders_count) is null then 1
        else 0
    end,
    aov desc,
    seller_id
limit 15
