-- =============================================================================
-- Model: stg_orders
-- Grain: One row per order_id
-- Source: raw_olist.orders
-- Purpose: Standardize Olist order headers for downstream delivery, customer,
--          and order-value modeling.
-- Key fields:
--   order_id                    STRING     Primary order identifier
--   customer_id                 STRING     Customer identifier
--   order_status                STRING     Standardized order lifecycle status
--   order_purchased_at_utc      TIMESTAMP  Purchase timestamp
--   ingested_at_utc             TIMESTAMP  Raw ingestion timestamp
-- Update frequency: Daily batch rebuild
-- =============================================================================
with source_data as (

    select
        *
    from {{ source('raw_olist', 'orders') }}

),

renamed as (

    select
        nullif(trim(order_id), '') as order_id,
        nullif(trim(customer_id), '') as customer_id,

        lower(nullif(trim(order_status), '')) as order_status,

        safe_cast(nullif(trim(order_purchase_timestamp), '') as timestamp)
            as order_purchased_at_utc,
        safe_cast(nullif(trim(order_approved_at), '') as timestamp)
            as order_approved_at_utc,
        safe_cast(nullif(trim(order_delivered_carrier_date), '') as timestamp)
            as order_delivered_to_carrier_at_utc,
        safe_cast(nullif(trim(order_delivered_customer_date), '') as timestamp)
            as order_delivered_to_customer_at_utc,
        safe_cast(nullif(trim(order_estimated_delivery_date), '') as timestamp)
            as order_estimated_delivery_at_utc,

        nullif(trim(batch_id), '') as batch_id,
        safe_cast(ingested_at_utc as timestamp) as ingested_at_utc,
        nullif(trim(source_file_name), '') as source_file_name

    from source_data

),

deduplicated as (

    select *
    from renamed
    qualify row_number() over (
        partition by order_id
        order by ingested_at_utc desc, batch_id desc
    ) = 1

)

select *
from deduplicated
