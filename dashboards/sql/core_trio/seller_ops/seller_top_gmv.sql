-- Seller operations ranking: commercial owner list ordered by published GMV.
-- Keep seller_label compact so bar charts remain readable while seller_id stays
-- available for exact drill-through and filtering.
with seller_rollup as (

    select
        seller_id,
        any_value(seller_city) as seller_city,
        any_value(seller_state) as seller_state,
        sum(gmv) as gmv
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
        when seller_city is null or seller_state is null
            then concat(left(seller_id, 8), "...")
        else concat(seller_city, ", ", seller_state, " (", left(seller_id, 8), "...)")
    end as seller_label,
    seller_state,
    gmv
from seller_rollup
order by gmv desc, seller_id
limit 15
