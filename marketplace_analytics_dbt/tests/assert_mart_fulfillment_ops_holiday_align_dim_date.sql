-- Contract test: mart_fulfillment_ops holiday fields must align with the
-- conformed dim_date meaning for the same purchase_date.
select
    mfo.purchase_date,
    mfo.customer_state,
    mfo.delivery_delay_bucket,
    mfo.is_purchase_on_holiday,
    dd.is_holiday,
    mfo.holiday_name_at_purchase,
    dd.holiday_name
from {{ ref('mart_fulfillment_ops') }} as mfo
left join {{ ref('dim_date') }} as dd
    on mfo.purchase_date = dd.calendar_date
where
    dd.calendar_date is null
    or mfo.is_purchase_on_holiday != dd.is_holiday
    or coalesce(mfo.holiday_name_at_purchase, '__NULL__')
        != coalesce(dd.holiday_name, '__NULL__')
