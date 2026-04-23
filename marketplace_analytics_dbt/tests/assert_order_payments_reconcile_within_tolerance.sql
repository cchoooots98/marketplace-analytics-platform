-- Reconciliation test: int_order_value must preserve the independently
-- roll-up-able staging aggregates at order grain. This catches join fan-out
-- or aggregation drift without assuming that raw payment totals must equal
-- item_value + freight_value exactly; the Olist source contains legitimate
-- financing / discount behavior and orders preserved without item rows.

with orders as (

    select
        order_id
    from {{ ref('stg_orders') }}

),

items_expected as (

    select
        order_id,
        sum(item_price) as order_item_value,
        sum(freight_value) as order_freight_total,
        count(*) as order_items_count
    from {{ ref('stg_order_items') }}
    group by order_id

),

payments_expected as (

    select
        order_id,
        sum(payment_value) as order_payment_total,
        count(distinct payment_type) as payment_methods_count
    from {{ ref('stg_payments') }}
    group by order_id

),

expected as (

    select
        o.order_id,
        coalesce(i.order_item_value, 0) as order_item_value,
        coalesce(i.order_freight_total, 0) as order_freight_total,
        coalesce(i.order_items_count, 0) as order_items_count,
        p.order_payment_total,
        p.payment_methods_count
    from orders as o
    left join items_expected as i
        on o.order_id = i.order_id
    left join payments_expected as p
        on o.order_id = p.order_id

),

actual as (

    select
        order_id,
        order_item_value,
        order_freight_total,
        order_items_count,
        order_payment_total,
        payment_methods_count
    from {{ ref('int_order_value') }}

)

{{ reconciliation_mismatch_rows(
    'expected',
    'actual',
    ['order_id'],
    exact_columns=[
        'order_items_count'
    ],
    nullable_exact_columns=[
        'payment_methods_count'
    ],
    required_amount_columns=[
        'order_item_value',
        'order_freight_total'
    ],
    nullable_amount_columns=[
        'order_payment_total'
    ],
    diagnostic_columns=[
        'order_items_count',
        'order_item_value',
        'order_payment_total'
    ]
) }}
