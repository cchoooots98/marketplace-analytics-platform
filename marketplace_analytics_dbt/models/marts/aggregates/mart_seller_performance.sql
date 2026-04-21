-- =============================================================================
-- Model: mart_seller_performance
-- Grain: One row per seller_id and calendar_date (purchase date)
-- Source: int_seller_daily_performance
-- Purpose: Seller-facing operations and commercial mart. Publishes seller-day
--          revenue, item volume, cancellation, lateness, and operational
--          defect metrics for the full seller-order population. Customer
--          experience metrics are intentionally excluded and live in
--          mart_seller_experience.
-- KPI contract (matches docs/metric_definitions.md):
--   gmv                        Item + freight, excluding cancelled.
--   aov                        gmv / non_cancelled_orders_count.
--   cancellation_rate          cancelled_orders_count / orders_count (0..1).
--   late_delivery_rate         late_orders_count / delivered_orders_count
--                              (0..1); NULL when the seller has no deliveries
--                              on the date.
--   operational_defect_rate    operational_defect_orders_count / orders_count
--                              (0..1), where an operational defect is a
--                              cancelled OR late seller order.
-- Update frequency: Daily batch rebuild. Full rebuild is intentional because
--                   this mart is small, audit-friendly, and derived from
--                   governed facts and intermediates.
-- =============================================================================

{{
    config(
        materialized='table',
        contract={'enforced': true},
        partition_by={
            'field': 'calendar_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['seller_id']
    )
}}

with
final as (

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
        -- Mirrors mart_exec_daily: late is a ratio of delivered orders only.
        safe_divide(
            late_orders_count,
            nullif(delivered_orders_count, 0)
        ) as late_delivery_rate,
        safe_divide(
            operational_defect_orders_count,
            orders_count
        ) as operational_defect_rate
    from {{ ref('int_seller_daily_performance') }}

)

select * from final
