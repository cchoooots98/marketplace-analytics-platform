-- Fulfillment weekly trend: roll cancellations to week cohorts so the
-- dashboard stays decision-oriented instead of reading like a noisy monitor.
select
    date_trunc(purchase_date, week(monday)) as purchase_date,
    sum(cancelled_orders_count) as cancelled_orders_count
from `marts.mart_fulfillment_ops`
where 1 = 1
    [[and {{date_range}}]]
    [[and {{customer_state}}]]
    [[and {{delivery_delay_bucket}}]]
    [[and {{holiday_flag}}]]
group by 1
order by 1
