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

{{ reconciliation_mismatch_rows(
    'expected',
    'actual',
    ['seller_id', 'calendar_date'],
    exact_columns=[
        'attributable_orders_count',
        'reviewed_attributable_orders_count',
        'reviews_count',
        'commented_reviews_count',
        'low_review_orders_count'
    ],
    nullable_rate_columns=[
        'review_coverage_rate',
        'avg_review_score',
        'avg_time_to_review_days',
        'low_review_rate'
    ],
    diagnostic_columns=[
        'attributable_orders_count'
    ]
) }}
