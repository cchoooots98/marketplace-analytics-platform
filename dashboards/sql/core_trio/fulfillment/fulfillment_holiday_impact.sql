-- Fulfillment trend: compare holiday and non-holiday purchase cohorts without
-- redefining the warehouse-owned holiday flag.
select
    purchase_date,
    is_purchase_on_holiday,
    sum(orders_count) as orders_count,
    safe_divide(
        sum(late_orders_count),
        sum(delivered_orders_count)
    ) as late_delivery_rate
from `marts.mart_fulfillment_ops`
where 1 = 1
    [[and {{date_range}}]]
    [[and {{customer_state}}]]
    [[and {{delivery_delay_bucket}}]]
    [[and {{holiday_flag}}]]
group by purchase_date, is_purchase_on_holiday
order by purchase_date, is_purchase_on_holiday
