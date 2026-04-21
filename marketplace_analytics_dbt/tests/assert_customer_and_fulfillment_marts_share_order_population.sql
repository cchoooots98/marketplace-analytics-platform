-- Shared-grain parity test: fulfillment and customer-experience marts must
-- publish the same order population on their shared cohort keys.
select
    coalesce(fo.purchase_date, cx.purchase_date) as purchase_date,
    coalesce(fo.customer_state, cx.customer_state) as customer_state,
    coalesce(fo.delivery_delay_bucket, cx.delivery_delay_bucket) as delivery_delay_bucket,
    fo.orders_count as fulfillment_orders_count,
    cx.orders_count as customer_experience_orders_count
from {{ ref('mart_fulfillment_ops') }} as fo
full outer join {{ ref('mart_customer_experience') }} as cx
    on fo.purchase_date = cx.purchase_date
    and fo.customer_state = cx.customer_state
    and fo.delivery_delay_bucket = cx.delivery_delay_bucket
where
    fo.purchase_date is null
    or cx.purchase_date is null
    or fo.orders_count != cx.orders_count
