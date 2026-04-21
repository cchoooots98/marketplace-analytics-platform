-- Invariant test: mart_seller_performance must keep core population and rate
-- relationships internally consistent.
select
    seller_id,
    calendar_date,
    orders_count,
    non_cancelled_orders_count,
    cancelled_orders_count,
    delivered_orders_count,
    late_orders_count,
    operational_defect_orders_count,
    cancellation_rate,
    late_delivery_rate,
    operational_defect_rate
from {{ ref('mart_seller_performance') }}
where
    cancelled_orders_count + non_cancelled_orders_count != orders_count
    or late_orders_count > delivered_orders_count
    or operational_defect_orders_count > orders_count
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
    or abs(
        operational_defect_rate
        - safe_divide(operational_defect_orders_count, orders_count)
    ) > 0.000001
