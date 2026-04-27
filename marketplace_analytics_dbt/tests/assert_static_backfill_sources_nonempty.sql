-- Static backfill completeness: every raw source required by the published
-- warehouse must contain rows after bootstrap ingestion. Source freshness is
-- reserved for runtime mode; static mode proves the bounded backfill exists.
with source_counts as (

    select 'raw_olist.customers' as source_name, count(*) as row_count
    from {{ source('raw_olist', 'customers') }}

    union all

    select 'raw_olist.geolocation' as source_name, count(*) as row_count
    from {{ source('raw_olist', 'geolocation') }}

    union all

    select 'raw_olist.order_items' as source_name, count(*) as row_count
    from {{ source('raw_olist', 'order_items') }}

    union all

    select 'raw_olist.order_payments' as source_name, count(*) as row_count
    from {{ source('raw_olist', 'order_payments') }}

    union all

    select 'raw_olist.order_reviews' as source_name, count(*) as row_count
    from {{ source('raw_olist', 'order_reviews') }}

    union all

    select 'raw_olist.orders' as source_name, count(*) as row_count
    from {{ source('raw_olist', 'orders') }}

    union all

    select 'raw_olist.products' as source_name, count(*) as row_count
    from {{ source('raw_olist', 'products') }}

    union all

    select 'raw_olist.sellers' as source_name, count(*) as row_count
    from {{ source('raw_olist', 'sellers') }}

    union all

    select 'raw_ext.holidays' as source_name, count(*) as row_count
    from {{ source('raw_ext', 'holidays') }}

    union all

    select 'raw_ext.weather_daily' as source_name, count(*) as row_count
    from {{ source('raw_ext', 'weather_daily') }}

)

select
    source_name,
    row_count
from source_counts
where row_count = 0
