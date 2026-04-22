-- Reconciliation test: mart_seller_performance must preserve every seller-date
-- row from int_seller_daily_performance and must derive the published rate
-- fields from the documented denominators.
with expected as (

    select
        seller_id,
        calendar_date,
        orders_count,
        non_cancelled_orders_count,
        items_count,
        items_value,
        freight_total,
        gmv,
        delivered_orders_count,
        cancelled_orders_count,
        late_orders_count,
        operational_defect_orders_count,
        safe_divide(gmv, nullif(non_cancelled_orders_count, 0)) as aov,
        safe_divide(cancelled_orders_count, orders_count) as cancellation_rate,
        safe_divide(
            late_orders_count,
            nullif(delivered_orders_count, 0)
        ) as late_delivery_rate,
        safe_divide(
            operational_defect_orders_count,
            orders_count
        ) as operational_defect_rate
    from {{ ref('int_seller_daily_performance') }}

),

actual as (

    select
        seller_id,
        calendar_date,
        orders_count,
        non_cancelled_orders_count,
        items_count,
        items_value,
        freight_total,
        gmv,
        delivered_orders_count,
        cancelled_orders_count,
        late_orders_count,
        operational_defect_orders_count,
        aov,
        cancellation_rate,
        late_delivery_rate,
        operational_defect_rate
    from {{ ref('mart_seller_performance') }}

)

{{ reconciliation_mismatch_rows(
    'expected',
    'actual',
    ['seller_id', 'calendar_date'],
    exact_columns=[
        'orders_count',
        'non_cancelled_orders_count',
        'items_count',
        'delivered_orders_count',
        'cancelled_orders_count',
        'late_orders_count',
        'operational_defect_orders_count'
    ],
    required_amount_columns=[
        'items_value',
        'freight_total',
        'gmv'
    ],
    required_rate_columns=[
        'cancellation_rate',
        'operational_defect_rate'
    ],
    nullable_rate_columns=[
        'aov',
        'late_delivery_rate'
    ],
    diagnostic_columns=[
        'orders_count',
        'operational_defect_rate'
    ]
) }}
