-- Singular test: every customer_unique_id must have exactly one first order.
-- A row returned here means the ROW_NUMBER partitioning or filtering in
-- int_customer_order_sequence is broken.
select
    customer_unique_id,
    min(customer_order_number) as first_customer_order_number,
    max(customer_order_number) as last_customer_order_number,
    count(*) as customer_orders_count,
    countif(customer_order_number = 1) as first_order_count
from {{ ref('int_customer_order_sequence') }}
group by customer_unique_id
having
    first_customer_order_number != 1
    or last_customer_order_number != customer_orders_count
    or first_order_count != 1
