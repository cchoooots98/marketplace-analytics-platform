-- Singular test: late_days is only populated for orders marked as late.
-- This protects the order-level SLA contract used by downstream rate metrics.
select order_id
from {{ ref('int_order_delivery') }}
where is_late = false
  and late_days is not null
