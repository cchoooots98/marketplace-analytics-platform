-- Invariant test: int_order_delivery must retain every order row and still
-- resolve purchase-date calendar context from dim_date. If dim_date coverage
-- regresses, fail here instead of silently dropping orders via an inner join.
select
    order_id,
    purchase_date
from {{ ref('int_order_delivery') }}
where
    purchase_date is not null
    and is_purchase_on_holiday is null
