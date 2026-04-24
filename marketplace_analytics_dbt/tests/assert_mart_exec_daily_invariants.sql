-- Invariant test: mart_exec_daily remains a specialized business test because
-- it proves denominator relationships, while scalar rate comparisons reuse the
-- shared reconciliation helper macros.

select
    calendar_date,
    orders_count,
    non_cancelled_orders_count,
    cancelled_orders_count,
    delivered_orders_count,
    late_orders_count,
    new_customers_count,
    cancellation_rate,
    late_delivery_rate
from {{ ref('mart_exec_daily') }}
where
    cancelled_orders_count + non_cancelled_orders_count != orders_count
    or late_orders_count > delivered_orders_count
    or new_customers_count > orders_count
    or {{ required_rate_mismatch(
        'cancellation_rate',
        'safe_divide(cancelled_orders_count, orders_count)'
    ) }}
    or {{ nullable_rate_mismatch(
        'late_delivery_rate',
        'safe_divide(late_orders_count, nullif(delivered_orders_count, 0))'
    ) }}
    or {{ nullable_amount_mismatch(
        'avg_review_score',
        'safe_divide(review_score_sum, nullif(reviews_count, 0))'
    ) }}
