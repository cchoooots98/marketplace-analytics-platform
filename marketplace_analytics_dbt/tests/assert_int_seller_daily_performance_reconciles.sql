-- Reconciliation test: int_seller_daily_performance must preserve the
-- seller-date operational contract derived from seller-order rollups. This
-- verifies net GMV, non_cancelled_orders_count, and operational defect logic
-- at the final intermediate grain instead of only comparing grand totals.
with
seller_order_rollup as (

    select
        oi.seller_id,
        d.purchase_date as calendar_date,
        oi.order_id,
        count(*) as items_count,
        sum(oi.item_price) as items_value,
        sum(oi.freight_value) as freight_total,
        sum(oi.item_price + oi.freight_value) as gross_value,
        logical_or(d.is_cancelled) as is_cancelled,
        logical_or(d.is_late) as is_late,
        logical_or(d.is_delivered) as is_delivered
    from {{ ref('stg_order_items') }} as oi
    inner join {{ ref('int_order_delivery') }} as d
        on oi.order_id = d.order_id
    group by
        oi.seller_id,
        d.purchase_date,
        oi.order_id

),

expected as (

    select
        seller_id,
        calendar_date,
        count(*) as orders_count,
        sum(items_count) as items_count,
        sum(items_value) as items_value,
        sum(freight_total) as freight_total,
        sum(case when not is_cancelled then gross_value else 0 end) as gmv,
        countif(not is_cancelled) as non_cancelled_orders_count,
        countif(is_delivered) as delivered_orders_count,
        countif(is_cancelled) as cancelled_orders_count,
        countif(is_late) as late_orders_count,
        countif(
            is_cancelled
            or is_late
        ) as operational_defect_orders_count
    from seller_order_rollup
    group by seller_id, calendar_date

),

actual as (

    select
        seller_id,
        calendar_date,
        orders_count,
        items_count,
        items_value,
        freight_total,
        gmv,
        non_cancelled_orders_count,
        delivered_orders_count,
        cancelled_orders_count,
        late_orders_count,
        operational_defect_orders_count
    from {{ ref('int_seller_daily_performance') }}

)

select
    coalesce(expected.seller_id, actual.seller_id) as seller_id,
    coalesce(expected.calendar_date, actual.calendar_date) as calendar_date,
    expected.orders_count as expected_orders_count,
    actual.orders_count as actual_orders_count,
    expected.gmv as expected_gmv,
    actual.gmv as actual_gmv,
    expected.non_cancelled_orders_count as expected_non_cancelled_orders_count,
    actual.non_cancelled_orders_count as actual_non_cancelled_orders_count,
    expected.operational_defect_orders_count as expected_operational_defect_orders_count,
    actual.operational_defect_orders_count as actual_operational_defect_orders_count
from expected
full outer join actual
    on expected.seller_id = actual.seller_id
    and expected.calendar_date = actual.calendar_date
where
    expected.seller_id is null
    or actual.seller_id is null
    or expected.orders_count != actual.orders_count
    or expected.items_count != actual.items_count
    or abs(expected.items_value - actual.items_value) > 0.01
    or abs(expected.freight_total - actual.freight_total) > 0.01
    or abs(expected.gmv - actual.gmv) > 0.01
    or expected.non_cancelled_orders_count != actual.non_cancelled_orders_count
    or expected.delivered_orders_count != actual.delivered_orders_count
    or expected.cancelled_orders_count != actual.cancelled_orders_count
    or expected.late_orders_count != actual.late_orders_count
    or expected.operational_defect_orders_count
        != actual.operational_defect_orders_count
