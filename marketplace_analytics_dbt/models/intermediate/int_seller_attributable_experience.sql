-- =============================================================================
-- Model: int_seller_attributable_experience
-- Grain: One row per seller_id and order_id
-- Source: fact_order_items, fact_orders, fact_reviews
-- Purpose: Build the attributable seller-experience base at seller-order grain.
--          Only single-seller orders are included so review coverage and
--          sentiment can be attributed to one seller without copying the same
--          order review across multiple sellers.
-- Key fields:
--   seller_id                        STRING     Seller identifier (grain key)
--   order_id                         STRING     Order identifier (grain key)
--   calendar_date                    DATE       Purchase date used for seller
--                                               experience cohorting
--   reviews_count                    INT64      Review rows on the attributable
--                                               order
--   commented_reviews_count          INT64      Review rows with non-empty
--                                               comment text
--   avg_review_score_for_order       FLOAT64    Order-weighted review score;
--                                               nullable when no review exists
--   avg_time_to_review_days_for_order FLOAT64   Order-weighted days from
--                                               delivery to review; nullable
--   is_reviewed_order                BOOL       True when the order has at
--                                               least one review
--   is_low_review_order              BOOL       True when the attributable
--                                               order average review score is
--                                               at or below the configured
--                                               threshold
-- Update frequency: Daily batch rebuild
-- =============================================================================

with seller_cardinality_by_order as (

    select
        order_id,
        any_value(seller_id) as seller_id,
        count(distinct seller_id) as seller_count
    from {{ ref('fact_order_items') }}
    group by order_id

),

attributable_orders as (

    select
        order_id,
        seller_id
    from seller_cardinality_by_order
    where seller_count = 1

),

final as (

    select
        ao.seller_id,
        ao.order_id,
        fo.purchase_date as calendar_date,
        rmo.reviews_count,
        rmo.commented_reviews_count,
        rmo.avg_review_score_for_order,
        rmo.avg_time_to_review_days_for_order,
        (rmo.reviews_count is not null) as is_reviewed_order,
        coalesce(
            rmo.avg_review_score_for_order
                <= {{ var('low_review_score_threshold') }},
            false
        ) as is_low_review_order
    from attributable_orders as ao
    inner join {{ ref('fact_orders') }} as fo
        on ao.order_id = fo.order_id
    left join {{ ref('int_order_review_metrics') }} as rmo
        on ao.order_id = rmo.order_id

)

select * from final
