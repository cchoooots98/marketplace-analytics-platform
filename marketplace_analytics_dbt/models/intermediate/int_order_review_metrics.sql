-- =============================================================================
-- Model: int_order_review_metrics
-- Grain: One row per order_id
-- Source: fact_reviews
-- Purpose: Centralized order-level review aggregation contract reused by
--          customer-experience and seller-experience models. Reviews are
--          collapsed once at order grain so downstream marts stay order
--          weighted instead of silently drifting to review-row weighting.
-- Key fields:
--   order_id                         STRING     Order identifier
--   reviews_count                    INT64      Review-row count on the order
--   commented_reviews_count          INT64      Review rows with non-empty comment
--   avg_review_score_for_order       FLOAT64    Average score across review rows
--   avg_time_to_review_days_for_order FLOAT64   Average days from delivery to
--                                               review across review rows
-- Update frequency: Daily batch rebuild
-- =============================================================================

with final as (

    select
        order_id,
        count(*) as reviews_count,
        countif(has_comment) as commented_reviews_count,
        avg(cast(review_score as float64)) as avg_review_score_for_order,
        avg(cast(time_to_review_days as float64))
            as avg_time_to_review_days_for_order
    from {{ ref('fact_reviews') }}
    group by order_id

)

select * from final
