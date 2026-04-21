-- Contract test: int_seller_attributable_experience must contain only orders
-- whose fact_order_items population resolves to exactly one distinct seller.
with seller_counts as (

    select
        order_id,
        count(distinct seller_id) as seller_count
    from {{ ref('fact_order_items') }}
    group by order_id

)

select
    sae.seller_id,
    sae.order_id,
    sc.seller_count
from {{ ref('int_seller_attributable_experience') }} as sae
left join seller_counts as sc
    on sae.order_id = sc.order_id
where
    sc.seller_count is null
    or sc.seller_count != 1
