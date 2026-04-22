-- Reconciliation test: mart_exec_daily must preserve the executive purchase-
-- date grain and the core conserved order/review measures that feed its KPI
-- formulas. Rate invariants are validated separately.
with order_conservation as (

    select
        purchase_date as calendar_date,
        count(*) as orders_count,
        countif(not is_cancelled) as non_cancelled_orders_count,
        countif(is_cancelled) as cancelled_orders_count,
        countif(is_delivered) as delivered_orders_count,
        countif(is_late) as late_orders_count,
        countif(is_first_order) as new_customers_count,
        sum(case
            when not is_cancelled
                then coalesce(order_item_value, 0) + coalesce(order_freight_total, 0)
            else 0
        end) as gmv,
        sum(order_item_value) as items_value,
        sum(order_freight_total) as freight_total,
        sum(coalesce(order_payment_total, 0)) as payment_total
    from {{ ref('fact_orders') }}
    group by purchase_date

),

review_conservation as (

    select
        purchase_date as calendar_date,
        count(*) as reviews_count,
        avg(cast(review_score as float64)) as avg_review_score
    from {{ ref('fact_reviews') }}
    group by purchase_date

),

expected as (

    select
        oc.calendar_date,
        oc.orders_count,
        oc.non_cancelled_orders_count,
        oc.cancelled_orders_count,
        oc.delivered_orders_count,
        oc.late_orders_count,
        oc.new_customers_count,
        oc.gmv,
        oc.items_value,
        oc.freight_total,
        oc.payment_total,
        coalesce(rc.reviews_count, 0) as reviews_count,
        rc.avg_review_score
    from order_conservation as oc
    left join review_conservation as rc
        on oc.calendar_date = rc.calendar_date

),

actual as (

    select
        calendar_date,
        orders_count,
        non_cancelled_orders_count,
        cancelled_orders_count,
        delivered_orders_count,
        late_orders_count,
        new_customers_count,
        gmv,
        items_value,
        freight_total,
        payment_total,
        reviews_count,
        avg_review_score
    from {{ ref('mart_exec_daily') }}

)

{{ reconciliation_mismatch_rows(
    'expected',
    'actual',
    ['calendar_date'],
    exact_columns=[
        'orders_count',
        'non_cancelled_orders_count',
        'cancelled_orders_count',
        'delivered_orders_count',
        'late_orders_count',
        'new_customers_count',
        'reviews_count'
    ],
    required_amount_columns=[
        'gmv',
        'items_value',
        'freight_total',
        'payment_total'
    ],
    nullable_rate_columns=[
        'avg_review_score'
    ],
    diagnostic_columns=[
        'orders_count',
        'gmv'
    ]
) }}
