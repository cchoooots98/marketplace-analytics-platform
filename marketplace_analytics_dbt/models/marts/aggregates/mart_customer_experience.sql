-- =============================================================================
-- Model: mart_customer_experience
-- Grain: One row per purchase_date, customer_state, and delivery_delay_bucket
-- Source: fact_orders, fact_reviews
-- Purpose: Customer-experience mart. Publishes review coverage and sentiment
--          metrics without mixing them into the fulfillment operations contract.
--          Experience metrics are aggregated to one row per order before
--          rolling up so multi-review orders do not overweight the slice.
-- Key measures:
--   orders_count                        INT64    Orders in the cohort slice
--   reviewed_orders_count               INT64    Orders with at least one review
--   reviews_count                       INT64    Review rows in the slice
--   commented_reviews_count             INT64    Review rows with a non-empty comment
--   avg_review_score                    FLOAT64  Average order-level review score
--   avg_time_to_review_days             FLOAT64  Average order-level time to review
-- Update frequency: Daily batch rebuild. Full rebuild is intentional because
--                   this mart is small, audit-friendly, and derived from
--                   conformed facts plus the shared order-level review
--                   contract.
-- =============================================================================

{{
    config(
        materialized='table',
        contract={'enforced': true},
        partition_by={
            'field': 'purchase_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['customer_state']
    )
}}

with
orders_with_experience as (

    select
        fo.order_id,
        fo.purchase_date,
        fo.customer_state_at_order as customer_state,
        {{ delivery_delay_bucket(
            'fo.is_delivered',
            'fo.is_cancelled',
            'fo.is_late',
            'fo.late_days'
        ) }} as delivery_delay_bucket,
        rmo.reviews_count,
        rmo.commented_reviews_count,
        rmo.avg_review_score_for_order,
        rmo.avg_time_to_review_days_for_order
    from {{ ref('fact_orders') }} as fo
    left join {{ ref('int_order_review_metrics') }} as rmo
        on fo.order_id = rmo.order_id

),

final as (

    select
        purchase_date,
        customer_state,
        delivery_delay_bucket,
        count(*) as orders_count,
        countif(reviews_count is not null) as reviewed_orders_count,
        sum(coalesce(reviews_count, 0)) as reviews_count,
        sum(coalesce(commented_reviews_count, 0)) as commented_reviews_count,
        avg(avg_review_score_for_order) as avg_review_score,
        avg(avg_time_to_review_days_for_order) as avg_time_to_review_days
    from orders_with_experience
    group by purchase_date, customer_state, delivery_delay_bucket

)

select * from final
