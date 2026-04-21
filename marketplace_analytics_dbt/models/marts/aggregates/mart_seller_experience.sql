-- =============================================================================
-- Model: mart_seller_experience
-- Grain: One row per seller_id and calendar_date (purchase date)
-- Source: int_seller_attributable_experience
-- Purpose: Seller-facing customer-experience mart for attributable orders
--          only. Publishes review coverage, sentiment, and time-to-review
--          metrics on the single-seller order subset where the order's review
--          can be attributed to one seller without ambiguous cross-seller
--          copying.
-- KPI contract (matches docs/metric_definitions.md):
--   attributable_orders_count         Count of single-seller attributable orders.
--   reviewed_attributable_orders_count Count of attributable orders with at
--                                      least one review.
--   review_coverage_rate              reviewed_attributable_orders_count /
--                                      attributable_orders_count.
--   avg_review_score                  Average order-level review score across
--                                      reviewed attributable orders.
--   low_review_rate                   low_review_orders_count /
--                                      reviewed_attributable_orders_count.
-- Update frequency: Daily batch rebuild. Full rebuild is intentional because
--                   this mart is small, audit-friendly, and derived from
--                   governed facts and intermediates.
-- =============================================================================

{{
    config(
        materialized='table',
        contract={'enforced': true},
        partition_by={
            'field': 'calendar_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['seller_id']
    )
}}

select
    seller_id,
    calendar_date,
    count(*) as attributable_orders_count,
    countif(is_reviewed_order) as reviewed_attributable_orders_count,
    safe_divide(countif(is_reviewed_order), count(*)) as review_coverage_rate,
    sum(coalesce(reviews_count, 0)) as reviews_count,
    sum(coalesce(commented_reviews_count, 0)) as commented_reviews_count,
    avg(avg_review_score_for_order) as avg_review_score,
    avg(avg_time_to_review_days_for_order) as avg_time_to_review_days,
    countif(is_low_review_order) as low_review_orders_count,
    safe_divide(
        countif(is_low_review_order),
        nullif(countif(is_reviewed_order), 0)
    ) as low_review_rate
from {{ ref('int_seller_attributable_experience') }}
group by seller_id, calendar_date
