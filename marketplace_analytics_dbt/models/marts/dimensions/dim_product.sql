-- =============================================================================
-- Model: dim_product
-- Grain: One row per product_id
-- Source: stg_products
-- Purpose: Canonical product dimension. Holds catalog attributes only. Product
--          sales and demand metrics belong in facts or future product
--          performance marts, not in the dimension.
-- Key fields:
--   product_id                   STRING   Primary key; FK target for fact_order_items
--   product_category_name        STRING   Normalized category; nullable when
--                                         source catalog omits it
--   product_name_length          INT64    Product name length
--   product_description_length   INT64    Product description length
--   product_photos_count         INT64    Number of product photos
--   product_weight_g             INT64    Weight in grams
--   product_length_cm            INT64    Length in centimeters
--   product_height_cm            INT64    Height in centimeters
--   product_width_cm             INT64    Width in centimeters
-- Update frequency: Daily batch rebuild
-- =============================================================================

select
    product_id,
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_count,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
from {{ ref('stg_products') }}
