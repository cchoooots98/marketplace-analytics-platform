-- =============================================================================
-- Model: int_order_value
-- Grain: One row per order_id
-- Source: stg_orders, stg_order_items, stg_payments
-- Purpose: Aggregate item prices, freight, and payment totals at order level to
--          serve as the single financial reference for all downstream mart joins.
--          Prevents independent aggregations that would fan-out on multi-table joins.
-- Key fields:
--   order_id                    STRING     Primary order identifier
--   order_item_value            NUMERIC    Sum of item prices; zero when no items exist
--   order_freight_total         NUMERIC    Sum of freight values; zero when no items exist
--   order_items_count           INT64      Number of items in the order; can be zero
--   order_payment_total         NUMERIC    Sum of payment values for the order; nullable
--   payment_methods_count       INT64      Count of distinct payment types used; nullable
-- Update frequency: Daily batch rebuild
-- =============================================================================

with orders as (

    select order_id
    from {{ ref('stg_orders') }}

),

items_aggregated as (

    -- Collapse item-level rows to one row per order.
    -- item_price and freight_value are NUMERIC in staging so SUM is type-safe.
    select
        order_id,
        sum(item_price)    as order_item_value,
        sum(freight_value) as order_freight_total,
        count(*)           as order_items_count
    from {{ ref('stg_order_items') }}
    group by order_id

),

payments_aggregated as (

    -- Collapse payment-level rows to one row per order.
    -- Multiple payment rows per order are expected (e.g. credit_card + voucher).
    select
        order_id,
        sum(payment_value)           as order_payment_total,
        count(distinct payment_type) as payment_methods_count
    from {{ ref('stg_payments') }}
    group by order_id

),

final as (

    -- Use stg_orders as the base so core order identifiers do not disappear
    -- when a cancelled or unavailable order has no item rows.
    -- LEFT JOIN preserves orders that have no items or no confirmed payment yet.
    -- NULL on the payment side is allowed and explicitly documented in schema.yml.
    -- order_item_value and order_freight_total are kept separate because some KPIs
    -- (e.g. seller GMV) use item value only, excluding freight.
    select
        o.order_id,
        coalesce(i.order_item_value, 0)    as order_item_value,
        coalesce(i.order_freight_total, 0) as order_freight_total,
        coalesce(i.order_items_count, 0)   as order_items_count,
        p.order_payment_total,
        p.payment_methods_count
    from orders as o
    left join items_aggregated as i
        on o.order_id = i.order_id
    left join payments_aggregated as p
        on o.order_id = p.order_id

)

select * from final
