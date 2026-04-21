-- Invariant test: mart_fulfillment_ops must preserve bucket purity and
-- denominator relationships for operational monitoring.
select
    purchase_date,
    customer_state,
    delivery_delay_bucket,
    orders_count,
    delivered_orders_count,
    late_orders_count,
    cancelled_orders_count,
    late_delivery_rate
from {{ ref('mart_fulfillment_ops') }}
where
    late_orders_count > delivered_orders_count
    or delivered_orders_count > orders_count
    or cancelled_orders_count > orders_count
    or (
        delivery_delay_bucket = 'not_delivered'
        and delivered_orders_count != 0
    )
    or (
        delivery_delay_bucket != 'not_delivered'
        and cancelled_orders_count != 0
    )
    or (
        orders_count - delivered_orders_count - cancelled_orders_count < 0
    )
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
