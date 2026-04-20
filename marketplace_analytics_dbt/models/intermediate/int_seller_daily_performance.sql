-- =============================================================================
-- Model: int_seller_daily_performance
-- Grain: One row per seller_id and calendar_date
-- Source: stg_order_items, stg_sellers, int_order_delivery, int_review_enriched
-- Purpose: Aggregate daily seller revenue, order count, item count, late
--          delivery signals, cancellation signals, and review scores. Serves as
--          the direct source for mart_seller_performance, keeping aggregation
--          logic in one place.
-- Key fields:
--   seller_id                   STRING     Seller identifier (grain key)
--   calendar_date               DATE       Purchase date (grain key)
--   orders_count                INT64      Distinct orders with items from seller
--   items_count                 INT64      Total items sold by this seller
--   gmv                         NUMERIC    Sum of item prices
--   freight_total               NUMERIC    Sum of freight values
--   delivered_orders_count      INT64      Delivered orders
--   cancelled_orders_count      INT64      Orders marked as cancelled
--   late_orders_count           INT64      Delivered orders that were late
--   avg_review_score            FLOAT64    Average review score; nullable
-- Update frequency: Daily batch rebuild
-- =============================================================================

with order_items as (

    select
        order_id,
        seller_id,
        item_price,
        freight_value
    from {{ ref('stg_order_items') }}

),

sellers as (

    select
        seller_id,
        seller_city,
        seller_state
    from {{ ref('stg_sellers') }}

),

delivery as (

    select
        order_id,
        purchase_date,
        is_cancelled,
        is_late,
        is_delivered
    from {{ ref('int_order_delivery') }}

),

-- Pre-aggregate reviews to one row per order before joining to seller orders.
-- int_review_enriched grain is (review_id, order_id); an order can have multiple
-- review rows. Collapsing to one avg_review_score_for_order row per order
-- prevents review fan-out and keeps seller averages order-weighted.
reviews as (

    select
        order_id,
        avg(cast(review_score as float64)) as avg_review_score_for_order
    from {{ ref('int_review_enriched') }}
    group by order_id

),

-- Join at item level so each item is attributed to the correct calendar date and
-- seller. INNER JOIN to delivery: items with no delivery context indicate a
-- staging data quality gap and are excluded here rather than silently counted.
items_with_context as (

    select
        oi.seller_id,
        oi.order_id,
        oi.item_price,
        oi.freight_value,
        d.purchase_date    as calendar_date,
        d.is_cancelled,
        d.is_late,
        d.is_delivered
    from order_items as oi
    inner join delivery as d
        on oi.order_id = d.order_id

),

-- Collapse item rows to one seller-order row before calculating order-level
-- counts and review averages. This keeps revenue at item grain while preventing
-- multi-item orders from weighting review scores more heavily than one-item
-- orders.
seller_order_rollup as (

    select
        seller_id,
        order_id,
        calendar_date,
        count(*)            as items_count,
        sum(item_price)     as gmv,
        sum(freight_value)  as freight_total,
        logical_or(is_cancelled) as is_cancelled,
        logical_or(is_late)      as is_late,
        logical_or(is_delivered) as is_delivered
    from items_with_context
    group by seller_id, order_id, calendar_date

),

aggregated as (

    select
        seller_id,
        calendar_date,
        count(*)                               as orders_count,
        sum(items_count)                       as items_count,
        sum(gmv)                               as gmv,
        sum(freight_total)                     as freight_total,
        countif(is_delivered)                  as delivered_orders_count,
        countif(is_cancelled)                  as cancelled_orders_count,
        countif(is_late)                       as late_orders_count,
        avg(r.avg_review_score_for_order)      as avg_review_score
    from seller_order_rollup as sor
    left join reviews as r
        on sor.order_id = r.order_id
    group by seller_id, calendar_date

),

final as (

    select
        a.seller_id,
        s.seller_city,
        s.seller_state,
        a.calendar_date,
        a.orders_count,
        a.items_count,
        a.gmv,
        a.freight_total,
        a.delivered_orders_count,
        a.cancelled_orders_count,
        a.late_orders_count,
        a.avg_review_score
    from aggregated as a
    left join sellers as s
        on a.seller_id = s.seller_id

)

select * from final
