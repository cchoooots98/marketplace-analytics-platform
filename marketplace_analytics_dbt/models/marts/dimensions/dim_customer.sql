-- =============================================================================
-- Model: dim_customer
-- Grain: One row per customer_unique_id
-- Source: stg_customers
-- Purpose: Canonical business-customer dimension. Holds current-state customer
--          identity and geography only. Historical order-time geography stays
--          on facts so this dimension remains a pure entity table rather than a
--          profile or lifetime-performance table.
-- Key fields:
--   customer_unique_id          STRING  Primary key; physical customer identifier
--   customer_zip_code_prefix    INT64   Current-state postal code prefix
--   customer_city               STRING  Current-state city
--   customer_state              STRING  Current-state Brazilian state code
-- Update frequency: Daily batch rebuild
-- =============================================================================

with customers as (

    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        ingested_at_utc,
        batch_id
    from {{ ref('stg_customers') }}

),

canonical_customer_record as (

    -- Olist mints a new customer_id per order for the same physical person.
    -- The business dimension therefore keys on customer_unique_id and keeps the
    -- latest available source record as the current-state representation. This
    -- is the standard non-SCD v1 trade-off: facts preserve history, dims expose
    -- the latest entity attributes.
    select
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    from customers
    qualify row_number() over (
        partition by customer_unique_id
        order by ingested_at_utc desc nulls last, batch_id desc, customer_id desc
    ) = 1

)

select * from canonical_customer_record
