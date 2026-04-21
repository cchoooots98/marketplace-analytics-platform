-- Invariant test: mart_customer_experience must keep review coverage and
-- sentiment fields internally consistent on every published slice.
select
    purchase_date,
    customer_state,
    delivery_delay_bucket,
    orders_count,
    reviewed_orders_count,
    reviews_count,
    commented_reviews_count,
    avg_review_score,
    avg_time_to_review_days
from {{ ref('mart_customer_experience') }}
where
    reviewed_orders_count > orders_count
    or reviews_count < reviewed_orders_count
    or commented_reviews_count > reviews_count
    or (
        reviewed_orders_count = 0
        and (
            avg_review_score is not null
            or avg_time_to_review_days is not null
        )
    )
