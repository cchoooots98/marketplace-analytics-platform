-- Reconciliation test: fact_order_items must preserve the staging item grain
-- and the item-level amounts exactly. Grain-level comparison avoids false
-- passes where positive and negative value drift cancels out at grand total.
with expected as (

    select
        order_id,
        order_item_id,
        item_price,
        freight_value,
        item_price + freight_value as item_total_with_freight
    from {{ ref('stg_order_items') }}

),

actual as (

    select
        order_id,
        order_item_id,
        item_price,
        freight_value,
        item_total_with_freight
    from {{ ref('fact_order_items') }}

)

select
    coalesce(expected.order_id, actual.order_id) as order_id,
    coalesce(expected.order_item_id, actual.order_item_id) as order_item_id,
    expected.item_price as expected_item_price,
    actual.item_price as actual_item_price,
    expected.freight_value as expected_freight_value,
    actual.freight_value as actual_freight_value,
    expected.item_total_with_freight as expected_item_total_with_freight,
    actual.item_total_with_freight as actual_item_total_with_freight
from expected
full outer join actual
    on expected.order_id = actual.order_id
    and expected.order_item_id = actual.order_item_id
where
    expected.order_id is null
    or actual.order_id is null
    or abs(expected.item_price - actual.item_price) > 0.01
    or abs(expected.freight_value - actual.freight_value) > 0.01
    or abs(
        expected.item_total_with_freight - actual.item_total_with_freight
    ) > 0.01
