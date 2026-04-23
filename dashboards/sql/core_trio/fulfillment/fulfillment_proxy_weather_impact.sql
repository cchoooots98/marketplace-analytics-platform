-- Fulfillment scatter plot: expose proxy delivery-weather context and make the
-- order-weighted rollup explicit at the dashboard-serving layer.
select
    purchase_date,
    safe_divide(
        sum(
            case
                when avg_delivery_precipitation_total is not null
                    then avg_delivery_precipitation_total * orders_count
                else 0
            end
        ),
        sum(
            case
                when avg_delivery_precipitation_total is not null then orders_count
                else 0
            end
        )
    ) as avg_delivery_precipitation_total,
    safe_divide(
        sum(
            case
                when avg_delivery_temperature_max is not null
                    then avg_delivery_temperature_max * orders_count
                else 0
            end
        ),
        sum(
            case
                when avg_delivery_temperature_max is not null then orders_count
                else 0
            end
        )
    ) as avg_delivery_temperature_max,
    safe_divide(
        sum(late_orders_count),
        sum(delivered_orders_count)
    ) as late_delivery_rate,
    sum(orders_count) as orders_count
from `marts.mart_fulfillment_ops`
where 1 = 1
    [[and {{date_range}}]]
    [[and {{customer_state}}]]
    [[and {{delivery_delay_bucket}}]]
    [[and {{holiday_flag}}]]
group by purchase_date
order by purchase_date
