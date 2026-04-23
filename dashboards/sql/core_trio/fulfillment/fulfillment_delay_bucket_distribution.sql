-- Fulfillment bar chart: severity mix of the full order population.
select
    delivery_delay_bucket,
    sum(orders_count) as orders_count
from `marts.mart_fulfillment_ops`
where 1 = 1
    [[and {{date_range}}]]
    [[and {{customer_state}}]]
    [[and {{delivery_delay_bucket}}]]
    [[and {{holiday_flag}}]]
group by delivery_delay_bucket
order by
    case delivery_delay_bucket
        when "not_delivered" then 1
        when "on_time" then 2
        when "1_to_3_days" then 3
        when "4_to_7_days" then 4
        when "8_to_14_days" then 5
        when "15_plus_days" then 6
        else 7
    end
