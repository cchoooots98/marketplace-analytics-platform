-- Weighting test: mart_customer_experience sentiment averages must follow the
-- canonical order-level review contract instead of raw review-row weighting.
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
        orm.avg_review_score_for_order,
        orm.avg_time_to_review_days_for_order
    from {{ ref('fact_orders') }} as fo
    left join {{ ref('int_order_review_metrics') }} as orm
        on fo.order_id = orm.order_id

),

expected as (

    select
        purchase_date,
        customer_state,
        delivery_delay_bucket,
        avg(avg_review_score_for_order) as avg_review_score,
        avg(avg_time_to_review_days_for_order) as avg_time_to_review_days
    from orders_with_experience
    group by purchase_date, customer_state, delivery_delay_bucket

),

actual as (

    select
        purchase_date,
        customer_state,
        delivery_delay_bucket,
        avg_review_score,
        avg_time_to_review_days
    from {{ ref('mart_customer_experience') }}

)

select
    coalesce(expected.purchase_date, actual.purchase_date) as purchase_date,
    coalesce(expected.customer_state, actual.customer_state) as customer_state,
    coalesce(expected.delivery_delay_bucket, actual.delivery_delay_bucket) as delivery_delay_bucket
from expected
full outer join actual
    on expected.purchase_date = actual.purchase_date
    and expected.customer_state = actual.customer_state
    and expected.delivery_delay_bucket = actual.delivery_delay_bucket
where
    expected.purchase_date is null
    or actual.purchase_date is null
    or not (
        (expected.avg_review_score is null and actual.avg_review_score is null)
        or abs(expected.avg_review_score - actual.avg_review_score) <= 0.000001
    )
    or not (
        (expected.avg_time_to_review_days is null and actual.avg_time_to_review_days is null)
        or abs(expected.avg_time_to_review_days - actual.avg_time_to_review_days) <= 0.000001
    )
