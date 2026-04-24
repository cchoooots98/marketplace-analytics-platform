-- Fulfillment cohort comparison: aggregate holiday and non-holiday purchase
-- cohorts so sparse holiday dates do not turn the chart into a noisy daily
-- monitor.
select
    case
        when is_purchase_on_holiday then "Holiday"
        else "Non-Holiday"
    end as holiday_cohort,
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
group by holiday_cohort
order by
    case holiday_cohort
        when "Holiday" then 1
        when "Non-Holiday" then 2
        else 3
    end
