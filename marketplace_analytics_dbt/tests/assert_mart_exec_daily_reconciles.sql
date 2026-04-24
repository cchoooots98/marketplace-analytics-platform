-- Reconciliation test: mart_exec_daily must preserve the executive purchase-
-- date grain and the core conserved order/review measures published from the
-- conformed facts. This is intentionally not framed as a proof that the KPI
-- business logic is independently correct; it is a conservation check between
-- governed upstream facts and the mart. Coverage and rate-specific invariants
-- are validated separately.
with order_conservation as (

    select
        purchase_date as calendar_date,
        sum(1) as orders_count,
        sum(case when is_cancelled then 0 else 1 end) as non_cancelled_orders_count,
        sum(case when is_cancelled then 1 else 0 end) as cancelled_orders_count,
        sum(case when is_delivered then 1 else 0 end) as delivered_orders_count,
        sum(case when is_late then 1 else 0 end) as late_orders_count,
        sum(case when is_first_order then 1 else 0 end) as new_customers_count,
        sum(case
            when is_cancelled then 0
            else coalesce(order_item_value, 0)
        end) + sum(case
            when is_cancelled then 0
            else coalesce(order_freight_total, 0)
        end) as gmv,
        sum(coalesce(order_item_value, 0)) as items_value,
        sum(coalesce(order_freight_total, 0)) as freight_total,
        sum(case
            when order_payment_total is null then 0
            else order_payment_total
        end) as payment_total
    from {{ ref('fact_orders') }}
    group by purchase_date

),

review_conservation as (

    select
        purchase_date as calendar_date,
        count(*) as reviews_count,
        sum(cast(review_score as float64)) as review_score_sum,
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
        coalesce(rc.review_score_sum, 0) as review_score_sum,
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
        review_score_sum,
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
        'payment_total',
        'review_score_sum'
    ],
    nullable_rate_columns=[
        'avg_review_score'
    ],
    diagnostic_columns=[
        'orders_count',
        'gmv'
    ]
) }}
