-- =============================================================================
-- Model: fact_order_items
-- Grain: One row per order_id and order_item_id
-- Source: stg_order_items, stg_products, fact_orders
-- Purpose: Line-item fact enabling seller-level, product-level, and
--          category-level analysis. Conformed order and customer context are
--          inherited from fact_orders so downstream models never need to choose
--          between source-customer IDs and business-customer IDs on their own.
-- Foreign keys:
--   order_id                        -> fact_orders.order_id
--   customer_unique_id              -> dim_customer.customer_unique_id
--   seller_id                       -> dim_seller.seller_id
--   product_id                      -> dim_product.product_id
--   purchase_date                   -> dim_date.calendar_date
--   delivery_date                   -> dim_date.calendar_date (nullable)
-- Key measures:
--   item_price                      NUMERIC  Item price (excludes freight)
--   freight_value                   NUMERIC  Freight charged to the item
--   item_total_with_freight         NUMERIC  item_price + freight_value
--   is_delivered / is_late / is_cancelled   BOOL    Inherited from order header
-- Update frequency: Daily batch rebuild
-- =============================================================================

with order_items as (

    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_at_utc,
        item_price,
        freight_value
    from {{ ref('stg_order_items') }}

),

orders as (

    select
        order_id,
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix_at_order,
        customer_city_at_order,
        customer_state_at_order,
        order_status,
        purchase_date,
        delivery_date,
        is_delivered,
        is_late,
        late_days,
        is_cancelled
    from {{ ref('fact_orders') }}

),

products as (

    select
        product_id,
        product_category_name
    from {{ ref('stg_products') }}

),

final as (

    -- INNER JOIN to fact_orders: an item whose order_id has no conformed order
    -- header indicates a broken warehouse contract. The relationships test on
    -- fact_order_items.order_id catches the issue; joining here keeps item rows
    -- aligned to the published order header contract.
    -- LEFT JOIN to products: a missing product is tolerable (category becomes
    -- NULL) because it does not affect the grain or primary measures. Product
    -- attributes here are intentionally current-state, not order-time copied.
    select
        oi.order_id,
        oi.order_item_id,
        o.customer_id,
        o.customer_unique_id,
        o.customer_zip_code_prefix_at_order,
        o.customer_city_at_order,
        o.customer_state_at_order,
        oi.seller_id,
        oi.product_id,
        p.product_category_name,
        o.order_status,
        o.purchase_date,
        o.delivery_date,
        oi.shipping_limit_at_utc,
        oi.item_price,
        oi.freight_value,
        (oi.item_price + oi.freight_value) as item_total_with_freight,
        o.is_delivered,
        o.is_late,
        o.late_days,
        o.is_cancelled
    from order_items as oi
    inner join orders as o
        on oi.order_id = o.order_id
    left join products as p
        on oi.product_id = p.product_id

)

select * from final
