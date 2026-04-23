-- Fulfillment trend: keep cancellation visible beside delivery risk rather
-- than hiding attrition behind a service-only view.
select
    purchase_date,
    sum(cancelled_orders_count) as cancelled_orders_count
from `marts.mart_fulfillment_ops`
where 1 = 1
    [[and {{date_range}}]]
    [[and {{customer_state}}]]
    [[and {{delivery_delay_bucket}}]]
    [[and {{holiday_flag}}]]
group by purchase_date
order by purchase_date
