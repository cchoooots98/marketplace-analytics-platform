-- Singular test: int_order_value must preserve every order_id from stg_orders.
-- A row returned here means the financial intermediate layer dropped a core
-- order identifier before marts had a chance to decide business inclusion rules.
select o.order_id
from {{ ref('stg_orders') }} as o
left join {{ ref('int_order_value') }} as ov
    on o.order_id = ov.order_id
where ov.order_id is null
