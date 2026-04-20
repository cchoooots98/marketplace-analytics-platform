-- =============================================================================
-- Model: int_customer_order_sequence
-- Grain: One row per customer_id and order_id
-- Source: stg_orders, stg_customers
-- Purpose: Sequence orders within each physical customer identity using
--          customer_unique_id. Olist assigns a new customer_id per order for
--          the same physical person, so sequencing on customer_id alone would
--          make every customer appear as a first-time buyer. This model is the
--          authoritative source for repeat-purchase rate and cohort analysis.
-- Key fields:
--   order_id                    STRING     Order identifier
--   customer_id                 STRING     Order-level customer identifier (grain key)
--   customer_unique_id          STRING     Stable physical customer identifier
--   customer_order_number       INT64      Order rank within the physical customer (1=first)
--   is_first_order              BOOL       True when customer_order_number = 1
-- Update frequency: Daily batch rebuild
-- =============================================================================

with orders as (

    select
        order_id,
        customer_id,
        order_status,
        order_purchased_at_utc
    from {{ ref('stg_orders') }}

),

customers as (

    select
        customer_id,
        customer_unique_id,
        customer_city,
        customer_state
    from {{ ref('stg_customers') }}

),

joined as (

    -- INNER JOIN: an order without a matching customer_id is a data quality
    -- failure already caught by the relationship test on stg_orders.customer_id.
    -- Dropping such an order here is intentional.
    select
        o.order_id,
        o.customer_id,
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,
        o.order_status,
        o.order_purchased_at_utc
    from orders as o
    inner join customers as c
        on o.customer_id = c.customer_id

),

sequenced as (

    select
        order_id,
        customer_id,
        customer_unique_id,
        customer_city,
        customer_state,
        order_status,
        order_purchased_at_utc,
        -- ROW_NUMBER (not RANK) ensures two orders with the same timestamp each
        -- receive a distinct number. Tie-break by order_id ASC for determinism.
        row_number() over (
            partition by customer_unique_id
            order by order_purchased_at_utc asc, order_id asc
        ) as customer_order_number
    from joined

),

final as (

    select
        order_id,
        customer_id,
        customer_unique_id,
        customer_city,
        customer_state,
        order_status,
        order_purchased_at_utc,
        customer_order_number,
        (customer_order_number = 1) as is_first_order
    from sequenced

)

select * from final
