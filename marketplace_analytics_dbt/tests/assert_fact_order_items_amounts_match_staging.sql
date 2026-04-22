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

{{ reconciliation_mismatch_rows(
    'expected',
    'actual',
    ['order_id', 'order_item_id'],
    required_amount_columns=[
        'item_price',
        'freight_value',
        'item_total_with_freight'
    ],
    diagnostic_columns=[
        'item_price',
        'freight_value',
        'item_total_with_freight'
    ]
) }}
