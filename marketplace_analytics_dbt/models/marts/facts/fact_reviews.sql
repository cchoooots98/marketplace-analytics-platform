-- =============================================================================
-- Model: fact_reviews
-- Grain: One row per review_id and order_id
-- Source: int_review_enriched, fact_orders
-- Purpose: Customer experience fact. Exposes review score, delivery delay
--          bucket, and product context alongside conformed order and business-
--          customer identity so experience marts do not re-join raw customer
--          mappings.
-- Foreign keys:
--   order_id                        -> fact_orders.order_id
--   customer_unique_id              -> dim_customer.customer_unique_id
--   purchase_date                   -> dim_date.calendar_date
-- Key measures:
--   review_score                    INT64    Customer rating 1..5
--   has_comment                     BOOL     True when a non-empty message exists
--   time_to_review_days             INT64    Days from delivery to review; nullable;
--                                            may be negative (DQ signal preserved)
--   delivery_delay_bucket           STRING   Standardized delay bucket from
--                                            int_review_enriched
--   primary_product_category        STRING   Category of the most expensive item; nullable
-- Update frequency: Daily batch rebuild
-- =============================================================================

with reviews as (

    select
        review_id,
        order_id,
        review_score,
        has_comment,
        review_created_at_utc,
        review_answered_at_utc,
        review_comment_title,
        review_comment_message,
        purchase_date,
        order_delivered_to_customer_at_utc,
        primary_product_category,
        is_delivered,
        is_late,
        is_cancelled,
        late_days,
        time_to_review_days,
        delivery_delay_bucket
    from {{ ref('int_review_enriched') }}

),

orders as (

    select
        order_id,
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix_at_order,
        customer_city_at_order,
        customer_state_at_order,
        purchase_date
    from {{ ref('fact_orders') }}

),

final as (

    -- LEFT JOIN: a review with no matching conformed order header is an
    -- upstream DQ signal. Keeping the review row with NULL customer/order
    -- context lets relationship tests surface the issue instead of silently
    -- dropping customer-experience evidence.
    select
        r.review_id,
        r.order_id,
        o.customer_id,
        o.customer_unique_id,
        o.customer_zip_code_prefix_at_order,
        o.customer_city_at_order,
        o.customer_state_at_order,
        r.review_score,
        r.has_comment,
        r.review_created_at_utc,
        r.review_answered_at_utc,
        r.review_comment_title,
        r.review_comment_message,
        coalesce(o.purchase_date, r.purchase_date) as purchase_date,
        r.order_delivered_to_customer_at_utc as delivered_at_utc,
        r.primary_product_category,
        r.is_delivered,
        r.is_late,
        r.is_cancelled,
        r.late_days,
        r.time_to_review_days,
        r.delivery_delay_bucket
    from reviews as r
    left join orders as o
        on r.order_id = o.order_id

)

select * from final
