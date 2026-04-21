-- =============================================================================
-- Model: dim_date
-- Grain: One row per calendar_date
-- Source: stg_orders, stg_reviews, stg_holidays
-- Purpose: Conformed calendar dimension. Provides year/quarter/month/week/day
--          attributes, weekend and configured holiday flags, and month/quarter/year
--          anchors so BI tools can pivot on any time grain without recomputing
--          calendar arithmetic in every dashboard.
-- Key fields:
--   calendar_date           DATE    Primary key of the dimension
--   year                    INT64   Gregorian year
--   quarter                 INT64   1..4
--   month                   INT64   1..12
--   iso_week                INT64   ISO week number
--   day_of_week             INT64   BigQuery DAYOFWEEK: 1=Sunday ... 7=Saturday
--   is_weekend              BOOL    True on Saturday and Sunday
--   is_holiday              BOOL    True when the date is a configured public holiday
--   holiday_name            STRING  Holiday label(s); nullable
-- Update frequency: Daily batch rebuild. Spine is derived from observed order
--                   and review dates so new facts always have a matching row.
-- =============================================================================

with order_bounds as (

    -- Cover every date that could appear as a grain key or measure date in any
    -- downstream fact/mart. Under-coverage would cause relationships tests from
    -- fact_orders.purchase_date to fail.
    select
        min(date(order_purchased_at_utc)) as min_order_date,
        max(greatest(
            coalesce(
                date(order_delivered_to_customer_at_utc),
                date(order_estimated_delivery_at_utc),
                date(order_purchased_at_utc)
            ),
            coalesce(
                date(order_estimated_delivery_at_utc),
                date(order_purchased_at_utc)
            ),
            date(order_purchased_at_utc)
        )) as max_order_date
    from {{ ref('stg_orders') }}

),

review_bounds as (

    select
        min(date(review_created_at_utc)) as min_review_date,
        max(date(review_created_at_utc)) as max_review_date
    from {{ ref('stg_reviews') }}

),

spine_bounds as (

    select
        coalesce(
            least(ob.min_order_date, rb.min_review_date),
            ob.min_order_date,
            rb.min_review_date
        ) as spine_min_date,
        coalesce(
            greatest(ob.max_order_date, rb.max_review_date),
            ob.max_order_date,
            rb.max_review_date
        ) as spine_max_date
    from order_bounds as ob
    cross join review_bounds as rb

),

date_spine as (

    select calendar_date
    from spine_bounds,
        -- Bootstrap safety: when one upstream source is empty, coalesce to the
        -- remaining bound instead of producing a NULL date spine. When both
        -- bounds are missing, emit a one-day spine on current_date() so the
        -- dimension still builds deterministically.
        unnest(generate_date_array(
            coalesce(spine_min_date, current_date()),
            coalesce(
                spine_max_date,
                spine_min_date,
                current_date()
            )
        )) as calendar_date

),

-- Collapse multi-holiday dates to one row before joining to the spine. Without
-- this, a date carrying two holidays (e.g. Carnival Monday + Tuesday block)
-- would fan-out the dimension grain.
holidays_by_date as (

    select
        holiday_date,
        string_agg(holiday_name order by holiday_name) as holiday_names
    from {{ ref('stg_holidays') }}
    where country_code = '{{ holiday_country_code() }}'
    group by holiday_date

),

final as (

    select
        ds.calendar_date,
        extract(year from ds.calendar_date)                     as year,
        extract(quarter from ds.calendar_date)                  as quarter,
        extract(month from ds.calendar_date)                    as month,
        format_date('%B', ds.calendar_date)                     as month_name,
        extract(isoweek from ds.calendar_date)                  as iso_week,
        extract(day from ds.calendar_date)                      as day_of_month,
        extract(dayofweek from ds.calendar_date)                as day_of_week,
        format_date('%A', ds.calendar_date)                     as day_name,
        extract(dayofweek from ds.calendar_date) in (1, 7)      as is_weekend,
        (h.holiday_date is not null)                            as is_holiday,
        h.holiday_names                                         as holiday_name,
        date_trunc(ds.calendar_date, month)                     as first_day_of_month,
        date_trunc(ds.calendar_date, quarter)                   as first_day_of_quarter,
        date_trunc(ds.calendar_date, year)                      as first_day_of_year,
        last_day(ds.calendar_date, month)                       as last_day_of_month
    from date_spine as ds
    left join holidays_by_date as h
        on ds.calendar_date = h.holiday_date

)

select * from final
