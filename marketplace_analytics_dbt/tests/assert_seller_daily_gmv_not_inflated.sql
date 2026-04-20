-- Singular test: total seller daily GMV should normally equal total item_price
-- in stg_order_items. A positive difference indicates fan-out inflation. A
-- negative difference can be legitimate if the INNER JOIN to int_order_delivery
-- excludes item rows whose order_id failed the upstream order relationship
-- contract; that should be investigated as upstream DQ, not as seller logic.
with seller_daily_total as (
    select sum(gmv) as total_gmv
    from {{ ref('int_seller_daily_performance') }}
),
item_total as (
    select sum(item_price) as total_item_value
    from {{ ref('stg_order_items') }}
)
select
    seller_daily_total.total_gmv,
    item_total.total_item_value,
    seller_daily_total.total_gmv - item_total.total_item_value as inflated_amount
from seller_daily_total, item_total
where seller_daily_total.total_gmv - item_total.total_item_value > 0.01
