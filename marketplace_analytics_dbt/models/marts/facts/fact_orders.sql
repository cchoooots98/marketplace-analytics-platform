-- =============================================================================
-- Model: fact_orders
-- Grain: One row per order_id
-- Source: int_order_delivery, int_order_value, int_customer_order_sequence,
--         stg_customers
-- Purpose: Conformed order fact consolidating delivery SLA flags, financial
--          measures, business-customer identity, and order-time customer
--          geography into a single BI-friendly table. BI dashboards should read
--          this fact instead of rebuilding order-level joins across
--          intermediates.
-- Foreign keys:
--   order_id                        -> PK of this fact
--   customer_unique_id              -> dim_customer.customer_unique_id
--   purchase_date                   -> dim_date.calendar_date
--   delivery_date                   -> dim_date.calendar_date (nullable)
-- Key measures:
--   order_item_value                NUMERIC  Sum of item prices
--   order_freight_total             NUMERIC  Sum of freight values
--   order_payment_total             NUMERIC  Sum of payment values; nullable
--   order_total_with_freight        NUMERIC  order_item_value + order_freight_total
--   is_delivered / is_late / is_cancelled   BOOL    SLA flags
--   late_days                       INT64    Days past estimated delivery; nullable
--   customer_order_number           INT64    Sequence for the physical customer
--   is_first_order                  BOOL     True when customer_order_number = 1
--   customer_*_at_order             *        Order-time customer geography snapshot
--
-- Incremental strategy
-- ---------------------
-- Materialization: incremental, merge strategy on order_id.
--
-- Why incremental: this fact grows linearly with transactions; rebuilding the
-- full table every run wastes BigQuery slot time. Merge (not append) is used
-- because an order's status mutates over its lifecycle (created -> approved
-- -> shipped -> delivered), so a given order_id row must be updated in place
-- rather than appended.
--
-- Lookback window: 30 days behind the current max purchase timestamp. Merge
-- considers only rows within that window, so late-arriving status changes
-- (e.g. a cancellation 14 days post-purchase) are caught without rescanning
-- the full history. The window is wide enough to cover the long tail of the
-- Olist delivery lifecycle while staying small enough that typical runs
-- touch only a few partitions.
--
-- Partition pruning: purchase_date is used as the partition key. Merge
-- touches only the purchase_date partitions within the lookback window, so
-- the merge DML scans kilobytes instead of gigabytes on incremental runs.
--
-- Full refresh: run `dbt build --select fact_orders --full-refresh` after
-- upstream schema or business-logic changes that affect historical rows.
-- =============================================================================

{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        partition_by={
            'field': 'purchase_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['customer_unique_id', 'order_status'],
        on_schema_change='sync_all_columns'
    )
}}

with delivery as (

    select
        order_id,
        customer_id,
        order_status,
        order_purchased_at_utc,
        order_approved_at_utc,
        order_delivered_to_carrier_at_utc,
        order_delivered_to_customer_at_utc,
        order_estimated_delivery_at_utc,
        purchase_date,
        delivery_date,
        is_delivered,
        is_late,
        late_days,
        is_cancelled,
        is_purchase_on_holiday,
        holiday_name_at_purchase,
        delivery_weather_location_key,
        delivery_temperature_max,
        delivery_temperature_min,
        delivery_precipitation_total,
        delivery_humidity_afternoon
    from {{ ref('int_order_delivery') }}

    {% if is_incremental() %}
    -- Incremental lookback filter. On first run this WHERE block is skipped,
    -- so the full history is loaded. On subsequent runs we only scan orders
    -- whose purchase timestamp falls within the last 30 days of the prior
    -- run's max value, which is wide enough to catch late status changes on
    -- in-flight orders. The subquery against {{ this }} is evaluated once by
    -- BigQuery and is effectively free because {{ this }} is partitioned.
    where order_purchased_at_utc >= (
        select timestamp_sub(
            coalesce(max(order_purchased_at_utc), timestamp('1900-01-01')),
            interval 30 day
        )
        from {{ this }}
    )
    {% endif %}

),

order_value as (

    select
        order_id,
        order_item_value,
        order_freight_total,
        order_items_count,
        order_payment_total,
        payment_methods_count
    from {{ ref('int_order_value') }}

),

customer_sequence as (

    -- Grain of int_customer_order_sequence is (customer_id, order_id), but
    -- order_id itself is unique per order in stg_orders, so joining on
    -- order_id alone produces a 1:1 match and cannot fan out.
    select
        order_id,
        customer_unique_id,
        customer_order_number,
        is_first_order
    from {{ ref('int_customer_order_sequence') }}

), 

customer_snapshot as (

    select
        customer_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    from {{ ref('stg_customers') }}

),

final as (

    -- LEFT JOIN on both sides because the incrementally filtered delivery CTE
    -- is the driver. Joining without filtering the sibling CTEs is the
    -- deliberate choice: int_order_value and int_customer_order_sequence are
    -- cheap views, and filtering them with the same lookback would require
    -- duplicating the watermark logic in three places without reducing scan
    -- cost meaningfully. customer_unique_id is sourced only from the sequence
    -- contract: if that model drops an order, the fact should fail its not_null
    -- and relationship tests instead of silently falling back to staging.
    select
        d.order_id,
        d.customer_id,
        cs.customer_unique_id as customer_unique_id,
        c.customer_zip_code_prefix as customer_zip_code_prefix_at_order,
        c.customer_city as customer_city_at_order,
        c.customer_state as customer_state_at_order,

        d.order_status,
        d.order_purchased_at_utc,
        d.order_approved_at_utc,
        d.order_delivered_to_carrier_at_utc,
        d.order_delivered_to_customer_at_utc,
        d.order_estimated_delivery_at_utc,
        d.purchase_date,
        d.delivery_date,

        d.is_delivered,
        d.is_late,
        d.late_days,
        d.is_cancelled,

        cs.customer_order_number,
        cs.is_first_order,

        ov.order_item_value,
        ov.order_freight_total,
        ov.order_items_count,
        ov.order_payment_total,
        ov.payment_methods_count,
        -- Pre-computed so BI dashboards do not reinvent the sum.
        coalesce(ov.order_item_value, 0)
            + coalesce(ov.order_freight_total, 0) as order_total_with_freight,

        d.is_purchase_on_holiday,
        d.holiday_name_at_purchase,
        d.delivery_weather_location_key,
        d.delivery_temperature_max,
        d.delivery_temperature_min,
        d.delivery_precipitation_total,
        d.delivery_humidity_afternoon,

        -- dbt_updated_at is a standard observability column. It records when
        -- this incremental run last wrote the row, which lets dashboards show
        -- data freshness and lets operators trace which run produced which
        -- record during an incident.
        current_timestamp() as dbt_updated_at
    from delivery as d
    left join order_value as ov
        on d.order_id = ov.order_id
    left join customer_sequence as cs
        on d.order_id = cs.order_id
    left join customer_snapshot as c
        on d.customer_id = c.customer_id

)

select * from final
