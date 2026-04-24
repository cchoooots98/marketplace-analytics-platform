-- Reconciliation test: mart_fulfillment_ops must preserve the shared grain,
-- core operational counts, and published order population. Holiday alignment
-- is validated separately. Bucket semantics are governed by the shared
-- delivery_delay_bucket contract test in
-- assert_int_review_enriched_delivery_delay_bucket_purity.sql, so this test
-- focuses on grain-level population parity instead of re-implementing every
-- bucket boundary again.
with orders_with_context as (

    select
        order_id,
        purchase_date,
        customer_state_at_order as customer_state,
        is_delivered,
        is_late,
        late_days,
        is_cancelled,
        delivery_weather_location_key,
        delivery_temperature_max,
        delivery_temperature_min,
        delivery_precipitation_total,
        delivery_humidity_afternoon,
        {{ delivery_delay_bucket(
            'is_delivered',
            'is_cancelled',
            'is_late',
            'late_days'
        ) }} as delivery_delay_bucket
    from {{ ref('fact_orders') }}

),

expected as (

    select
        purchase_date,
        customer_state,
        delivery_delay_bucket,
        count(*) as orders_count,
        countif(is_delivered) as delivered_orders_count,
        countif(is_late) as late_orders_count,
        countif(is_cancelled) as cancelled_orders_count,
        sum(case
            when is_late then cast(late_days as float64)
            else 0
        end) as late_days_sum,
        countif(delivery_temperature_max is not null)
            as delivery_temperature_max_observation_count,
        sum(case
            when delivery_temperature_max is not null
                then cast(delivery_temperature_max as float64)
            else 0
        end) as delivery_temperature_max_sum,
        countif(delivery_temperature_min is not null)
            as delivery_temperature_min_observation_count,
        sum(case
            when delivery_temperature_min is not null
                then cast(delivery_temperature_min as float64)
            else 0
        end) as delivery_temperature_min_sum,
        countif(delivery_precipitation_total is not null)
            as delivery_precipitation_total_observation_count,
        sum(case
            when delivery_precipitation_total is not null
                then cast(delivery_precipitation_total as float64)
            else 0
        end) as delivery_precipitation_total_sum,
        countif(delivery_humidity_afternoon is not null)
            as delivery_humidity_afternoon_observation_count,
        sum(case
            when delivery_humidity_afternoon is not null
                then cast(delivery_humidity_afternoon as float64)
            else 0
        end) as delivery_humidity_afternoon_sum,
        avg(case
            when is_late then cast(late_days as float64)
        end) as avg_late_days
    from orders_with_context
    group by purchase_date, customer_state, delivery_delay_bucket

),

actual as (

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
        avg_late_days
    from {{ ref('mart_fulfillment_ops') }}

)

{{ reconciliation_mismatch_rows(
    'expected',
    'actual',
    ['purchase_date', 'customer_state', 'delivery_delay_bucket'],
    exact_columns=[
        'orders_count',
        'delivered_orders_count',
        'late_orders_count',
        'cancelled_orders_count',
        'delivery_temperature_max_observation_count',
        'delivery_temperature_min_observation_count',
        'delivery_precipitation_total_observation_count',
        'delivery_humidity_afternoon_observation_count'
    ],
    required_amount_columns=[
        'late_days_sum',
        'delivery_temperature_max_sum',
        'delivery_temperature_min_sum',
        'delivery_precipitation_total_sum',
        'delivery_humidity_afternoon_sum'
    ],
    nullable_amount_columns=[
        'avg_late_days'
    ],
    diagnostic_columns=[
        'orders_count'
    ]
) }}
