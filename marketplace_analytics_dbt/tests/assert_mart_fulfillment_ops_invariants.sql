-- Invariant test: mart_fulfillment_ops stays specialized because bucket purity
-- is domain logic, while scalar rate comparisons reuse shared helper macros.

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
    or {{ nullable_rate_mismatch(
        'late_delivery_rate',
        'safe_divide(late_orders_count, nullif(delivered_orders_count, 0))'
    ) }}
