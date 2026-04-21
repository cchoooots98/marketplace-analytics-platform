-- =============================================================================
-- Model: int_seller_daily_performance
-- Grain: One row per seller_id and calendar_date
-- Source: stg_order_items, int_order_delivery
-- Purpose: Aggregate daily seller revenue, item volume, and operational
--          fulfillment signals at seller-date grain. This model is the
--          authoritative operational base for mart_seller_performance and
--          intentionally excludes customer-experience metrics so seller
--          performance and seller experience stay in separate governed
--          contracts.
-- KPI contract (matches docs/metric_definitions.md):
--   gmv                       Item value + freight value, EXCLUDING cancelled
--                             orders. Aligns the revenue numerator to the
--                             converted commercial population.
--   non_cancelled_orders      Count of seller orders that are not cancelled.
--                             This is the denominator for AOV.
--   operational_defect        An order is an operational defect when it is
--                             cancelled OR late. Each seller-order contributes
--                             at most once (set union, not sum of conditions).
-- Key fields:
--   seller_id                        STRING     Seller identifier (grain key)
--   calendar_date                    DATE       Purchase date (grain key)
--   orders_count                     INT64      Distinct seller orders
--   items_count                      INT64      Total seller items
--   items_value                      NUMERIC    Sum of item prices (all orders)
--   freight_total                    NUMERIC    Sum of freight values (all orders)
--   gmv                              NUMERIC    KPI-aligned revenue (item +
--                                               freight, excluding cancelled)
--   non_cancelled_orders_count       INT64      Seller orders that are not
--                                               cancelled
--   delivered_orders_count           INT64      Delivered orders
--   cancelled_orders_count           INT64      Cancelled orders
--   late_orders_count                INT64      Late delivered orders
--   operational_defect_orders_count  INT64      Orders that are cancelled OR
--                                               late (deduped per order)
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

delivery as (

    select
        order_id,
        purchase_date,
        is_cancelled,
        is_late,
        is_delivered
    from {{ ref('int_order_delivery') }}

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
-- counts and defect flags. This keeps revenue at item grain while preventing
-- multi-item orders from inflating seller order counts.
seller_order_rollup as (

    select
        iwc.seller_id,
        iwc.order_id,
        iwc.calendar_date,
        count(*)                              as items_count,
        sum(iwc.item_price)                   as items_value,
        sum(iwc.freight_value)                as freight_total,
        sum(iwc.item_price + iwc.freight_value) as gross_value,
        logical_or(iwc.is_cancelled)          as is_cancelled,
        logical_or(iwc.is_late)               as is_late,
        logical_or(iwc.is_delivered)          as is_delivered
    from items_with_context as iwc
    group by
        iwc.seller_id,
        iwc.order_id,
        iwc.calendar_date

),

aggregated as (

    -- GMV excludes cancelled orders per docs/metric_definitions.md. items_value
    -- and freight_total remain unconditional so fulfillment-cost monitoring
    -- (which wants all freight, including cancelled) still works.
    select
        seller_id,
        calendar_date,
        count(*)                                                         as orders_count,
        sum(items_count)                                                 as items_count,
        sum(items_value)                                                 as items_value,
        sum(freight_total)                                               as freight_total,
        sum(case when not is_cancelled then gross_value else 0 end)      as gmv,
        countif(not is_cancelled)                                        as non_cancelled_orders_count,
        countif(is_delivered)                                            as delivered_orders_count,
        countif(is_cancelled)                                            as cancelled_orders_count,
        countif(is_late)                                                 as late_orders_count,
        countif(
            is_cancelled
            or is_late
        )                                                                as operational_defect_orders_count
    from seller_order_rollup
    group by seller_id, calendar_date

),

final as (

    select
        seller_id,
        calendar_date,
        orders_count,
        items_count,
        items_value,
        freight_total,
        gmv,
        non_cancelled_orders_count,
        delivered_orders_count,
        cancelled_orders_count,
        late_orders_count,
        operational_defect_orders_count
    from aggregated

)

select * from final
