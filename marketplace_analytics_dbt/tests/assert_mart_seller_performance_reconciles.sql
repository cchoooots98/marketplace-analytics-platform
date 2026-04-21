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

select
    coalesce(expected.seller_id, actual.seller_id) as seller_id,
    coalesce(expected.calendar_date, actual.calendar_date) as calendar_date,
    expected.orders_count as expected_orders_count,
    actual.orders_count as actual_orders_count,
    expected.operational_defect_rate as expected_operational_defect_rate,
    actual.operational_defect_rate as actual_operational_defect_rate
from expected
full outer join actual
    on expected.seller_id = actual.seller_id
    and expected.calendar_date = actual.calendar_date
where
    expected.seller_id is null
    or actual.seller_id is null
    or expected.orders_count != actual.orders_count
    or expected.non_cancelled_orders_count != actual.non_cancelled_orders_count
    or expected.items_count != actual.items_count
    or abs(expected.items_value - actual.items_value) > 0.01
    or abs(expected.freight_total - actual.freight_total) > 0.01
    or abs(expected.gmv - actual.gmv) > 0.01
    or expected.delivered_orders_count != actual.delivered_orders_count
    or expected.cancelled_orders_count != actual.cancelled_orders_count
    or expected.late_orders_count != actual.late_orders_count
    or expected.operational_defect_orders_count
        != actual.operational_defect_orders_count
    or not (
        (expected.aov is null and actual.aov is null)
        or abs(expected.aov - actual.aov) <= 0.000001
    )
    or abs(expected.cancellation_rate - actual.cancellation_rate) > 0.000001
    or not (
        (expected.late_delivery_rate is null and actual.late_delivery_rate is null)
        or abs(expected.late_delivery_rate - actual.late_delivery_rate) <= 0.000001
    )
    or abs(expected.operational_defect_rate - actual.operational_defect_rate) > 0.000001
