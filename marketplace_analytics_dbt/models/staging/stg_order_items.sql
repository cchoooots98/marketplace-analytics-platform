-- =============================================================================
-- Model: stg_order_items
-- Grain: One row per order_id and order_item_id
-- Source: raw_olist.order_items
-- Purpose: Standardize Olist order-item rows for product, seller, and order
--          value modeling.
-- Key fields:
--   order_id                    STRING     Order identifier
--   order_item_id               INTEGER    Item sequence inside the order
--   product_id                  STRING     Product identifier
--   seller_id                   STRING     Seller identifier
--   item_price                  NUMERIC    Item price
--   freight_value               NUMERIC    Freight amount
--   ingested_at_utc             TIMESTAMP  Raw ingestion timestamp
-- Update frequency: Daily batch rebuild
-- =============================================================================
with source_data as (

    select
        *
    from {{ source('raw_olist', 'order_items') }}

),

renamed as (

    select
        nullif(trim(order_id), '') as order_id,
        safe_cast(order_item_id as int64) as order_item_id,
        nullif(trim(product_id), '') as product_id,
        nullif(trim(seller_id), '') as seller_id,

        safe_cast(nullif(trim(shipping_limit_date), '') as timestamp)
            as shipping_limit_at_utc,
        safe_cast(price as numeric) as item_price,
        safe_cast(freight_value as numeric) as freight_value,

        nullif(trim(batch_id), '') as batch_id,
        safe_cast(ingested_at_utc as timestamp) as ingested_at_utc,
        nullif(trim(source_file_name), '') as source_file_name

    from source_data

),

deduplicated as (

    select *
    from renamed
    qualify row_number() over (
        partition by order_id, order_item_id
        order by ingested_at_utc desc, batch_id desc
    ) = 1

)

select *
from deduplicated
