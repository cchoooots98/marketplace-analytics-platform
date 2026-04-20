-- Singular test: every order marked is_late = TRUE must have late_days >= 1.
-- A row returned here means the late flag and late_days are inconsistent,
-- which indicates a bug in the delivery SLA logic in int_order_delivery.
select order_id
from {{ ref('int_order_delivery') }}
where is_late = true
  and (late_days is null or late_days < 1)
