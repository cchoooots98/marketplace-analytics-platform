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
    late_days_sum,
    delivery_temperature_max_observation_count,
    delivery_temperature_max_sum,
    delivery_temperature_min_observation_count,
    delivery_temperature_min_sum,
    delivery_precipitation_total_observation_count,
    delivery_precipitation_total_sum,
    delivery_humidity_afternoon_observation_count,
    delivery_humidity_afternoon_sum,
    avg_late_days,
    avg_delivery_temperature_max,
    avg_delivery_temperature_min,
    avg_delivery_precipitation_total,
    avg_delivery_humidity_afternoon,
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
    or late_days_sum < 0
    or delivery_temperature_max_observation_count > orders_count
    or delivery_temperature_min_observation_count > orders_count
    or delivery_precipitation_total_observation_count > orders_count
    or delivery_humidity_afternoon_observation_count > orders_count
    or {{ nullable_amount_mismatch(
        'avg_late_days',
        'safe_divide(late_days_sum, nullif(late_orders_count, 0))'
    ) }}
    or {{ nullable_amount_mismatch(
        'avg_delivery_temperature_max',
        'safe_divide(delivery_temperature_max_sum, nullif(delivery_temperature_max_observation_count, 0))'
    ) }}
    or {{ nullable_amount_mismatch(
        'avg_delivery_temperature_min',
        'safe_divide(delivery_temperature_min_sum, nullif(delivery_temperature_min_observation_count, 0))'
    ) }}
    or {{ nullable_amount_mismatch(
        'avg_delivery_precipitation_total',
        'safe_divide(delivery_precipitation_total_sum, nullif(delivery_precipitation_total_observation_count, 0))'
    ) }}
    or {{ nullable_amount_mismatch(
        'avg_delivery_humidity_afternoon',
        'safe_divide(delivery_humidity_afternoon_sum, nullif(delivery_humidity_afternoon_observation_count, 0))'
    ) }}
    or {{ nullable_rate_mismatch(
        'late_delivery_rate',
        'safe_divide(late_orders_count, nullif(delivered_orders_count, 0))'
    ) }}
