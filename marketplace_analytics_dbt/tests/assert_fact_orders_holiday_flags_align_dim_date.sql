-- Contract test: fact_orders holiday fields must align with dim_date for the
-- same purchase_date. The holiday meaning is owned by the conformed calendar
-- dimension and facts must publish the same boolean and label semantics.
select
    fo.order_id,
    fo.purchase_date,
    fo.is_purchase_on_holiday,
    dd.is_holiday,
    fo.holiday_name_at_purchase,
    dd.holiday_name
from {{ ref('fact_orders') }} as fo
left join {{ ref('dim_date') }} as dd
    on fo.purchase_date = dd.calendar_date
where
    dd.calendar_date is null
    or fo.is_purchase_on_holiday != dd.is_holiday
    or coalesce(fo.holiday_name_at_purchase, '__NULL__')
        != coalesce(dd.holiday_name, '__NULL__')
