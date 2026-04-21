-- Invariant test: mart_exec_daily must keep denominator relationships and
-- internal populations consistent on every published date.
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
    or abs(
        cancellation_rate
        - safe_divide(cancelled_orders_count, orders_count)
    ) > 0.000001
    or not (
        (
            late_delivery_rate is null
            and safe_divide(
                late_orders_count,
                nullif(delivered_orders_count, 0)
            ) is null
        )
        or abs(
            late_delivery_rate
            - safe_divide(
                late_orders_count,
                nullif(delivered_orders_count, 0)
            )
        ) <= 0.000001
    )
