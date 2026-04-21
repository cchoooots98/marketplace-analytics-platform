-- Coverage test: dim_date must cover every business date required by orders
-- and reviews so null-safe date-spine logic does not collapse under partial
-- source availability.
with required_dates as (

    select date(order_purchased_at_utc) as calendar_date
    from {{ ref('stg_orders') }}
    where order_purchased_at_utc is not null

    union distinct

    select date(order_delivered_to_customer_at_utc) as calendar_date
    from {{ ref('stg_orders') }}
    where order_delivered_to_customer_at_utc is not null

    union distinct

    select date(order_estimated_delivery_at_utc) as calendar_date
    from {{ ref('stg_orders') }}
    where order_estimated_delivery_at_utc is not null

    union distinct

    select date(review_created_at_utc) as calendar_date
    from {{ ref('stg_reviews') }}
    where review_created_at_utc is not null

),

missing_dates as (

    select
        rd.calendar_date
    from required_dates as rd
    left join {{ ref('dim_date') }} as dd
        on rd.calendar_date = dd.calendar_date
    where dd.calendar_date is null

)

select *
from missing_dates
