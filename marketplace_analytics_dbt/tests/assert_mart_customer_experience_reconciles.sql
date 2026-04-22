-- Reconciliation test: mart_customer_experience must preserve the published
-- purchase_date x customer_state x delivery_delay_bucket contract for review
-- coverage populations. Review-weighting semantics are validated separately,
-- and bucket semantics are governed by the shared delivery_delay_bucket
-- contract test in assert_int_review_enriched_delivery_delay_bucket_purity.sql.
with
orders_with_experience as (

    select
        fo.order_id,
        fo.purchase_date,
        fo.customer_state_at_order as customer_state,
        {{ delivery_delay_bucket(
            'fo.is_delivered',
            'fo.is_cancelled',
            'fo.is_late',
            'fo.late_days'
        ) }} as delivery_delay_bucket,
        rmo.reviews_count,
        rmo.commented_reviews_count,
        rmo.avg_review_score_for_order,
        rmo.avg_time_to_review_days_for_order
    from {{ ref('fact_orders') }} as fo
    left join {{ ref('int_order_review_metrics') }} as rmo
        on fo.order_id = rmo.order_id

),

expected as (

    select
        purchase_date,
        customer_state,
        delivery_delay_bucket,
        count(*) as orders_count,
        countif(reviews_count is not null) as reviewed_orders_count,
        sum(coalesce(reviews_count, 0)) as reviews_count,
        sum(coalesce(commented_reviews_count, 0)) as commented_reviews_count
    from orders_with_experience
    group by purchase_date, customer_state, delivery_delay_bucket

),

actual as (

    select
        purchase_date,
        customer_state,
        delivery_delay_bucket,
        orders_count,
        reviewed_orders_count,
        reviews_count,
        commented_reviews_count
    from {{ ref('mart_customer_experience') }}

)

{{ reconciliation_mismatch_rows(
    'expected',
    'actual',
    ['purchase_date', 'customer_state', 'delivery_delay_bucket'],
    exact_columns=[
        'orders_count',
        'reviewed_orders_count',
        'reviews_count',
        'commented_reviews_count'
    ],
    diagnostic_columns=[
        'orders_count'
    ]
) }}
