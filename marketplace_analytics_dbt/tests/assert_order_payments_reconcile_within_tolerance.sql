-- Singular test: item value plus freight should reconcile to aggregated
-- payment totals at order grain within a currency-safe tolerance. Orders
-- without payment rows are excluded because missing optional payments are a
-- known state, while mismatched totals indicate aggregation or join bugs.
{% set reconciliation_amount_tolerance = var('reconciliation_amount_tolerance', 0.01) %}

select
    order_id,
    order_item_value,
    order_freight_total,
    order_payment_total,
    abs(
        (order_item_value + order_freight_total) - order_payment_total
    ) as payment_difference
from {{ ref('int_order_value') }}
where
    order_payment_total is not null
    and abs(
        (order_item_value + order_freight_total) - order_payment_total
    ) > {{ reconciliation_amount_tolerance }}
