-- Reconciliation test: int_seller_attributable_experience must include only
-- single-seller orders and must preserve the attributable order-level review
-- metrics used by mart_seller_experience.
with seller_cardinality_by_order as (

    select
        order_id,
        any_value(seller_id) as seller_id,
        count(distinct seller_id) as seller_count
    from {{ ref('fact_order_items') }}
    group by order_id

),

attributable_orders as (

    select
        order_id,
        seller_id
    from seller_cardinality_by_order
    where seller_count = 1

),

expected as (

    select
        ao.seller_id,
        ao.order_id,
        fo.purchase_date as calendar_date,
        rmo.reviews_count,
        rmo.commented_reviews_count,
        rmo.avg_review_score_for_order,
        rmo.avg_time_to_review_days_for_order,
        (rmo.reviews_count is not null) as is_reviewed_order,
        coalesce(
            rmo.avg_review_score_for_order
                <= {{ var('low_review_score_threshold') }},
            false
        ) as is_low_review_order
    from attributable_orders as ao
    inner join {{ ref('fact_orders') }} as fo
        on ao.order_id = fo.order_id
    left join {{ ref('int_order_review_metrics') }} as rmo
        on ao.order_id = rmo.order_id

),

actual as (

    select
        seller_id,
        order_id,
        calendar_date,
        reviews_count,
        commented_reviews_count,
        avg_review_score_for_order,
        avg_time_to_review_days_for_order,
        is_reviewed_order,
        is_low_review_order
    from {{ ref('int_seller_attributable_experience') }}

)

{{ reconciliation_mismatch_rows(
    'expected',
    'actual',
    ['seller_id', 'order_id'],
    exact_columns=[
        'calendar_date',
        'is_reviewed_order',
        'is_low_review_order'
    ],
    nullable_amount_columns=[
        'reviews_count',
        'commented_reviews_count'
    ],
    nullable_rate_columns=[
        'avg_review_score_for_order',
        'avg_time_to_review_days_for_order'
    ],
    diagnostic_columns=[
        'calendar_date',
        'reviews_count',
        'commented_reviews_count',
        'avg_review_score_for_order',
        'avg_time_to_review_days_for_order',
        'is_reviewed_order',
        'is_low_review_order'
    ]
) }}
