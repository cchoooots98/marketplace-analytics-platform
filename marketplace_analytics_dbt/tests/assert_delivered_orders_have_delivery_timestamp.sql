-- Singular test: source-delivered orders must carry an actual delivery
-- timestamp. is_delivered is derived from this same rule inside
-- int_order_delivery, so this test asserts the business condition once.
select
    order_id,
    order_status,
    is_delivered,
    order_delivered_to_customer_at_utc
from {{ ref('int_order_delivery') }}
where
    order_status = 'delivered'
    and order_delivered_to_customer_at_utc is null
