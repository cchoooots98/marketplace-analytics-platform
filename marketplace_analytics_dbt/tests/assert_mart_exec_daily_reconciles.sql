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

select
    coalesce(expected.calendar_date, actual.calendar_date) as calendar_date,
    expected.orders_count as expected_orders_count,
    actual.orders_count as actual_orders_count,
    expected.gmv as expected_gmv,
    actual.gmv as actual_gmv
from expected
full outer join actual
    on expected.calendar_date = actual.calendar_date
where
    expected.calendar_date is null
    or actual.calendar_date is null
    or expected.orders_count != actual.orders_count
    or expected.non_cancelled_orders_count != actual.non_cancelled_orders_count
    or expected.cancelled_orders_count != actual.cancelled_orders_count
    or expected.delivered_orders_count != actual.delivered_orders_count
    or expected.late_orders_count != actual.late_orders_count
    or expected.new_customers_count != actual.new_customers_count
    or abs(expected.gmv - actual.gmv) > 0.01
    or abs(expected.items_value - actual.items_value) > 0.01
    or abs(expected.freight_total - actual.freight_total) > 0.01
    or abs(expected.payment_total - actual.payment_total) > 0.01
    or expected.reviews_count != actual.reviews_count
    or not (
        (expected.avg_review_score is null and actual.avg_review_score is null)
        or abs(expected.avg_review_score - actual.avg_review_score) <= 0.000001
    )
