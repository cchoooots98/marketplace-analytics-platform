-- =============================================================================
-- Model: int_review_enriched
-- Grain: One row per review_id and order_id
-- Source: stg_reviews, stg_order_items, stg_products, int_order_delivery
-- Purpose: Enrich each customer review with delivery SLA context and primary
--          product category. Classify delivery lateness into standardized delay
--          buckets so the same rule is not duplicated across multiple marts.
-- Key fields:
--   review_id                   STRING     Review identifier (grain key)
--   order_id                    STRING     Order identifier (grain key)
--   review_score                INT64      Customer rating 1-5
--   has_comment                 BOOL       True when a non-empty message exists
--   delivery_delay_bucket       STRING     Delay classification bucket:
--                                          not_delivered, on_time, 1_to_3_days,
--                                          4_to_7_days, 8_to_14_days,
--                                          15_plus_days
--   time_to_review_days         INT64      Days from delivery to review creation;
--                                          nullable; may be negative (DQ signal)
--   is_delivered                BOOL       From int_order_delivery
--   is_late                     BOOL       From int_order_delivery
--   late_days                   INT64      From int_order_delivery
--   primary_product_category    STRING     Category of the most expensive item
-- Update frequency: Daily batch rebuild
-- =============================================================================

with reviews as (

    select
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_created_at_utc,
        review_answered_at_utc
    from {{ ref('stg_reviews') }}

),

delivery as (

    select
        order_id,
        is_delivered,
        is_late,
        is_cancelled,
        late_days,
        order_delivered_to_customer_at_utc,
        purchase_date
    from {{ ref('int_order_delivery') }}

),

-- Resolve the primary product category per order as the category of the most
-- expensive item. When two items tie on price, the alphabetically first
-- product_id wins. QUALIFY applies the row filter after the window function,
-- which is the BigQuery-idiomatic way to select the top-1 row per group.
order_primary_category as (

    select
        oi.order_id,
        p.product_category_name as primary_product_category
    from {{ ref('stg_order_items') }} as oi
    left join {{ ref('stg_products') }} as p
        on oi.product_id = p.product_id
    qualify row_number() over (
        partition by oi.order_id
        order by oi.item_price desc, oi.product_id asc
    ) = 1

),

joined as (

    -- All joins are LEFT JOINs from reviews. A review can exist for an order
    -- that was cancelled, never delivered, or has incomplete delivery context.
    -- Dropping those reviews would silently reduce apparent review counts in marts.
    select
        r.review_id,
        r.order_id,
        r.review_score,
        r.review_comment_title,
        r.review_comment_message,
        r.review_created_at_utc,
        r.review_answered_at_utc,
        d.is_delivered,
        d.is_late,
        d.is_cancelled,
        d.late_days,
        d.order_delivered_to_customer_at_utc,
        d.purchase_date,
        pc.primary_product_category
    from reviews as r
    left join delivery as d
        on r.order_id = d.order_id
    left join order_primary_category as pc
        on r.order_id = pc.order_id

),

enriched as (

    select
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_created_at_utc,
        review_answered_at_utc,
        primary_product_category,
        -- Preserve all review rows while keeping downstream boolean grouping
        -- safe. Missing optional delivery context defaults to false; the
        -- nullable delivery timestamps remain visible as the DQ signal.
        coalesce(is_delivered, false) as is_delivered,
        coalesce(is_late, false)      as is_late,
        coalesce(is_cancelled, false) as is_cancelled,
        late_days,
        order_delivered_to_customer_at_utc,
        purchase_date,

        (
            review_comment_message is not null
            and trim(review_comment_message) != ''
        ) as has_comment,

        -- Negative values are kept visible rather than clamped to zero so
        -- data quality tests can detect reviews created before delivery.
        case
            when
                order_delivered_to_customer_at_utc is not null
                and review_created_at_utc is not null
            then date_diff(
                date(review_created_at_utc),
                date(order_delivered_to_customer_at_utc),
                day
            )
            else null
        end as time_to_review_days,

        -- Delegate bucket classification to the shared macro so the boundary
        -- definitions live in one place. Cancelled / undelivered orders land
        -- in 'not_delivered' rather than 'on_time'; folding them into on_time
        -- would make the customer-experience dashboards understate late rates
        -- and inflate the on-time cohort with orders that were never delivered.
        {{ delivery_delay_bucket(
            'is_delivered',
            'is_cancelled',
            'is_late',
            'late_days'
        ) }} as delivery_delay_bucket

    from joined

),

final as (

    select
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_created_at_utc,
        review_answered_at_utc,
        primary_product_category,
        is_delivered,
        is_late,
        is_cancelled,
        late_days,
        order_delivered_to_customer_at_utc,
        purchase_date,
        has_comment,
        time_to_review_days,
        delivery_delay_bucket
    from enriched

)

select * from final
