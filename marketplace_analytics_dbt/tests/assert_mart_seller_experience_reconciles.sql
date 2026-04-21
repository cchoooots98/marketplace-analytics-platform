-- Reconciliation test: mart_seller_experience must preserve the attributable
-- seller-date experience contract derived from int_seller_attributable_experience.
with expected as (

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

),

actual as (

    select
        seller_id,
        calendar_date,
        attributable_orders_count,
        reviewed_attributable_orders_count,
        review_coverage_rate,
        reviews_count,
        commented_reviews_count,
        avg_review_score,
        avg_time_to_review_days,
        low_review_orders_count,
        low_review_rate
    from {{ ref('mart_seller_experience') }}

)

select
    coalesce(expected.seller_id, actual.seller_id) as seller_id,
    coalesce(expected.calendar_date, actual.calendar_date) as calendar_date,
    expected.attributable_orders_count as expected_attributable_orders_count,
    actual.attributable_orders_count as actual_attributable_orders_count
from expected
full outer join actual
    on expected.seller_id = actual.seller_id
    and expected.calendar_date = actual.calendar_date
where
    expected.seller_id is null
    or actual.seller_id is null
    or expected.attributable_orders_count != actual.attributable_orders_count
    or expected.reviewed_attributable_orders_count
        != actual.reviewed_attributable_orders_count
    or not (
        (expected.review_coverage_rate is null and actual.review_coverage_rate is null)
        or abs(expected.review_coverage_rate - actual.review_coverage_rate) <= 0.000001
    )
    or expected.reviews_count != actual.reviews_count
    or expected.commented_reviews_count != actual.commented_reviews_count
    or not (
        (expected.avg_review_score is null and actual.avg_review_score is null)
        or abs(expected.avg_review_score - actual.avg_review_score) <= 0.000001
    )
    or not (
        (expected.avg_time_to_review_days is null and actual.avg_time_to_review_days is null)
        or abs(expected.avg_time_to_review_days - actual.avg_time_to_review_days) <= 0.000001
    )
    or expected.low_review_orders_count != actual.low_review_orders_count
    or not (
        (expected.low_review_rate is null and actual.low_review_rate is null)
        or abs(expected.low_review_rate - actual.low_review_rate) <= 0.000001
    )
