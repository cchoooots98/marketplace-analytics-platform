-- Contract test: is_delivered must stay semantically aligned with the source
-- status and actual delivery timestamp. Source rows can say
-- order_status='delivered' while omitting the delivery timestamp; the model
-- contract is to preserve those rows without promoting them to
-- is_delivered = true.
select
    order_id,
    order_status,
    is_delivered,
    order_delivered_to_customer_at_utc
from {{ ref('int_order_delivery') }}
where is_delivered != (
    order_status = 'delivered'
    and order_delivered_to_customer_at_utc is not null
)
