-- Invariant test: mart_seller_experience stays specialized because it asserts
-- attributable-population rules, while scalar rate checks reuse shared helper
-- macros.

select
    seller_id,
    calendar_date,
    attributable_orders_count,
    reviewed_attributable_orders_count,
    reviews_count,
    commented_reviews_count,
    low_review_orders_count,
    review_coverage_rate,
    low_review_rate,
    avg_review_score
from {{ ref('mart_seller_experience') }}
where
    reviewed_attributable_orders_count > attributable_orders_count
    or reviews_count < reviewed_attributable_orders_count
    or commented_reviews_count > reviews_count
    or low_review_orders_count > reviewed_attributable_orders_count
    or {{ required_rate_mismatch(
        'review_coverage_rate',
        'safe_divide(reviewed_attributable_orders_count, attributable_orders_count)'
    ) }}
    or {{ nullable_rate_mismatch(
        'low_review_rate',
        'safe_divide(low_review_orders_count, nullif(reviewed_attributable_orders_count, 0))'
    ) }}
    or (
        reviewed_attributable_orders_count = 0
        and avg_review_score is not null
    )
