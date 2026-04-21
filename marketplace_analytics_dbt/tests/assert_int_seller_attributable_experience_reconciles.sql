-- Reconciliation test: int_seller_attributable_experience must include only
-- single-seller orders and must preserve the attributable order-level review
-- metrics used by mart_seller_experience.
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

expected as (

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

),

actual as (

    select
        seller_id,
        order_id,
        calendar_date,
        reviews_count,
        commented_reviews_count,
        avg_review_score_for_order,
        avg_time_to_review_days_for_order,
        is_reviewed_order,
        is_low_review_order
    from {{ ref('int_seller_attributable_experience') }}

)

select
    coalesce(expected.seller_id, actual.seller_id) as seller_id,
    coalesce(expected.order_id, actual.order_id) as order_id,
    expected.calendar_date as expected_calendar_date,
    actual.calendar_date as actual_calendar_date
from expected
full outer join actual
    on expected.seller_id = actual.seller_id
    and expected.order_id = actual.order_id
where
    expected.order_id is null
    or actual.order_id is null
    or expected.calendar_date != actual.calendar_date
    or coalesce(expected.reviews_count, -1) != coalesce(actual.reviews_count, -1)
    or coalesce(expected.commented_reviews_count, -1)
        != coalesce(actual.commented_reviews_count, -1)
    or not (
        (expected.avg_review_score_for_order is null and actual.avg_review_score_for_order is null)
        or abs(expected.avg_review_score_for_order - actual.avg_review_score_for_order) <= 0.000001
    )
    or not (
        (expected.avg_time_to_review_days_for_order is null and actual.avg_time_to_review_days_for_order is null)
        or abs(
            expected.avg_time_to_review_days_for_order
            - actual.avg_time_to_review_days_for_order
        ) <= 0.000001
    )
    or expected.is_reviewed_order != actual.is_reviewed_order
    or expected.is_low_review_order != actual.is_low_review_order
