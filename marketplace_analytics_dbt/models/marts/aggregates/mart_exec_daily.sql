-- =============================================================================
-- Model: mart_exec_daily
-- Grain: One row per calendar_date (purchase date cohort)
-- Source: fact_orders, fact_reviews, dim_date
-- Purpose: Executive daily KPI feed. All headline metrics (GMV, orders, AOV,
--          cancellation rate, late delivery rate, new customers, review score)
--          are cohorted by the order's purchase_date so every KPI on this row
--          refers to the same set of orders. BI dashboards read this mart
--          directly. Convenience KPI columns are valid at the mart grain; any
--          cross-period rollup must use the published support columns instead
--          of averaging daily rates or averages.
-- Key measures (all numerator/denominator pairs defined once here):
--   orders_count                INT64    Orders placed on the date
--   non_cancelled_orders_count  INT64    Orders placed on the date that are
--                                        not cancelled; denominator for AOV
--   gmv                         NUMERIC  Item + freight, excluding cancelled
--   items_value                 NUMERIC  Sum of item prices (all orders)
--   freight_total               NUMERIC  Sum of freight values (all orders)
--   aov                         NUMERIC  Convenience row-grain KPI:
--                                        gmv / non_cancelled_orders_count
--   cancellation_rate           NUMERIC  Convenience row-grain KPI:
--                                        cancelled / orders_count (0..1)
--   late_delivery_rate          NUMERIC  Convenience row-grain KPI:
--                                        late / delivered (0..1); NULL when no
--                                        deliveries
--   new_customers_count         INT64    First-order customers on the date
--   reviews_count               INT64    Review-row support count on the date
--   review_score_sum            FLOAT64  Additive review-score numerator on the
--                                        purchase-date cohort
--   avg_review_score            FLOAT64  Convenience row-grain average review
--                                        score derived from review_score_sum /
--                                        reviews_count
-- Update frequency: Daily batch rebuild. Full rebuild is intentional because
--                   this mart is small, audit-friendly, and derived from
--                   governed facts. Rows are emitted only for dates with at
--                   least one order so dashboards display a clean series.
-- =============================================================================

{{
    config(
        materialized='table',
        contract={'enforced': true},
        partition_by={
            'field': 'calendar_date',
            'data_type': 'date',
            'granularity': 'day'
        }
    )
}}

with orders_by_date as (

    -- GMV follows the canonical definition (item + freight, excluding
    -- cancelled). items_value and freight_total remain as independent
    -- drill-down measures so operational views that want all freight,
    -- including cancelled, still have the raw inputs.
    select
        purchase_date,
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

reviews_by_date as (

    -- Cohort reviews by the order's purchase_date, not review_created_at.
    -- This keeps the review average on the same cohort as the orders and GMV
    -- on the same row; mixing grains here would make rate cross-checks
    -- meaningless.
    select
        purchase_date,
        count(*) as reviews_count,
        sum(cast(review_score as float64)) as review_score_sum
    from {{ ref('fact_reviews') }}
    group by purchase_date

),

final as (

    -- dim_date is the spine so downstream relationships tests hold, but we
    -- filter to dates that actually have orders because exec dashboards do
    -- not need zero-activity rows cluttering the series.
    select
        d.calendar_date,
        d.year,
        d.quarter,
        d.month,
        d.iso_week,
        d.day_of_week,
        d.is_weekend,
        d.is_holiday,
        d.holiday_name,
        ob.orders_count,
        ob.non_cancelled_orders_count,
        ob.cancelled_orders_count,
        ob.delivered_orders_count,
        ob.late_orders_count,
        ob.new_customers_count,
        ob.gmv,
        ob.items_value,
        ob.freight_total,
        ob.payment_total,
        -- Convenience KPI columns remain useful at the mart grain. BI rollups
        -- should aggregate the published support columns above rather than
        -- averaging these derived values across dates.
        safe_divide(ob.gmv, nullif(ob.non_cancelled_orders_count, 0)) as aov,
        safe_divide(ob.cancelled_orders_count, ob.orders_count) as cancellation_rate,
        -- Denominator is delivered_orders_count, not orders_count. Late is only
        -- defined for delivered orders; using orders_count would understate the
        -- true late rate by counting in-flight and cancelled orders.
        safe_divide(
            ob.late_orders_count,
            nullif(ob.delivered_orders_count, 0)
        ) as late_delivery_rate,
        coalesce(rb.reviews_count, 0) as reviews_count,
        coalesce(rb.review_score_sum, 0) as review_score_sum,
        safe_divide(
            rb.review_score_sum,
            nullif(rb.reviews_count, 0)
        ) as avg_review_score
    from {{ ref('dim_date') }} as d
    inner join orders_by_date as ob
        on d.calendar_date = ob.purchase_date
    left join reviews_by_date as rb
        on d.calendar_date = rb.purchase_date

)

select * from final
