-- =============================================================================
-- Model: stg_payments
-- Grain: One row per order_id and payment_sequence
-- Source: raw_olist.order_payments
-- Purpose: Standardize Olist payment events for finance and order-value
--          modeling.
-- Key fields:
--   order_id                    STRING     Order identifier
--   payment_sequence            INTEGER    Payment sequence inside the order
--   payment_type                STRING     Standardized payment method
--   payment_value               NUMERIC    Payment amount
--   ingested_at_utc             TIMESTAMP  Raw ingestion timestamp
-- Update frequency: Daily batch rebuild
-- =============================================================================
with source_data as (

    select
        *
    from {{ source('raw_olist', 'order_payments') }}

),

renamed as (

    select
        nullif(trim(order_id), '') as order_id,
        safe_cast(payment_sequential as int64) as payment_sequence,

        lower(nullif(trim(payment_type), '')) as payment_type,

        safe_cast(payment_installments as int64) as payment_installments_count,
        safe_cast(payment_value as numeric) as payment_value,

        nullif(trim(batch_id), '') as batch_id,
        safe_cast(ingested_at_utc as timestamp) as ingested_at_utc,
        nullif(trim(source_file_name), '') as source_file_name

    from source_data

),

deduplicated as (

    select *
    from renamed
    qualify row_number() over (
        partition by order_id, payment_sequence
        order by ingested_at_utc desc, batch_id desc
    ) = 1

)

select *
from deduplicated
