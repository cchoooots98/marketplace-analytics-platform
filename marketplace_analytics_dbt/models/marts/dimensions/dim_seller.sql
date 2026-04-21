-- =============================================================================
-- Model: dim_seller
-- Grain: One row per seller_id
-- Source: stg_sellers
-- Purpose: Canonical seller dimension. Holds seller master data only. Seller
--          performance and defect metrics are published in marts, not embedded
--          into the dimension.
-- Key fields:
--   seller_id                 STRING  Primary key; FK target for fact_order_items
--   seller_zip_code_prefix    INT64   Seller postal code prefix
--   seller_city               STRING  Seller city
--   seller_state              STRING  Brazilian two-letter state code
-- Update frequency: Daily batch rebuild
-- =============================================================================

select
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
from {{ ref('stg_sellers') }}
