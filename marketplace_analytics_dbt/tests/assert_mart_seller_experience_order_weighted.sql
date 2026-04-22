-- Weighting test: mart_seller_experience review averages must match the
-- order-level review contract on attributable orders and must not collapse to
-- item-weighted or raw review-row-weighted averages when those alternatives
-- differ from the contract.
-- This remains a specialized business proof because it guards against
-- alternative but wrong weighting strategies. Shared helper macros are used
-- for the repeated nullable floating-point comparisons.

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
        sco.seller_id,
        fo.order_id,
        fo.purchase_date as calendar_date
    from seller_cardinality_by_order as sco
    inner join {{ ref('fact_orders') }} as fo
        on sco.order_id = fo.order_id
    where sco.seller_count = 1

),

expected as (

    select
        ao.seller_id,
        ao.calendar_date,
        avg(orm.avg_review_score_for_order) as expected_avg_review_score,
        avg(orm.avg_time_to_review_days_for_order)
            as expected_avg_time_to_review_days
    from attributable_orders as ao
    left join {{ ref('int_order_review_metrics') }} as orm
        on ao.order_id = orm.order_id
    group by ao.seller_id, ao.calendar_date

),

item_weighted as (

    select
        ao.seller_id,
        ao.calendar_date,
        avg(orm.avg_review_score_for_order) as item_weighted_avg_review_score,
        avg(orm.avg_time_to_review_days_for_order)
            as item_weighted_avg_time_to_review_days
    from attributable_orders as ao
    inner join {{ ref('fact_order_items') }} as foi
        on ao.order_id = foi.order_id
        and ao.seller_id = foi.seller_id
    left join {{ ref('int_order_review_metrics') }} as orm
        on ao.order_id = orm.order_id
    group by ao.seller_id, ao.calendar_date

),

review_row_weighted as (

    select
        ao.seller_id,
        ao.calendar_date,
        avg(cast(fr.review_score as float64)) as review_row_weighted_avg_review_score,
        avg(cast(fr.time_to_review_days as float64))
            as review_row_weighted_avg_time_to_review_days
    from attributable_orders as ao
    left join {{ ref('fact_reviews') }} as fr
        on ao.order_id = fr.order_id
    group by ao.seller_id, ao.calendar_date

),

actual as (

    select
        seller_id,
        calendar_date,
        avg_review_score,
        avg_time_to_review_days
    from {{ ref('mart_seller_experience') }}

)

select
    coalesce(expected.seller_id, actual.seller_id) as seller_id,
    coalesce(expected.calendar_date, actual.calendar_date) as calendar_date,
    expected.expected_avg_review_score,
    actual.avg_review_score,
    iw.item_weighted_avg_review_score,
    rrw.review_row_weighted_avg_review_score
from expected
full outer join actual
    on expected.seller_id = actual.seller_id
    and expected.calendar_date = actual.calendar_date
left join item_weighted as iw
    on expected.seller_id = iw.seller_id
    and expected.calendar_date = iw.calendar_date
left join review_row_weighted as rrw
    on expected.seller_id = rrw.seller_id
    and expected.calendar_date = rrw.calendar_date
where
    expected.seller_id is null
    or actual.seller_id is null
    or {{ nullable_rate_mismatch(
        'expected.expected_avg_review_score',
        'actual.avg_review_score'
    ) }}
    or {{ nullable_rate_mismatch(
        'expected.expected_avg_time_to_review_days',
        'actual.avg_time_to_review_days'
    ) }}
    or (
        {{ nullable_rate_mismatch(
            'iw.item_weighted_avg_review_score',
            'expected.expected_avg_review_score'
        ) }}
        and not {{ nullable_rate_mismatch(
            'iw.item_weighted_avg_review_score',
            'actual.avg_review_score'
        ) }}
    )
    or (
        {{ nullable_rate_mismatch(
            'rrw.review_row_weighted_avg_review_score',
            'expected.expected_avg_review_score'
        ) }}
        and not {{ nullable_rate_mismatch(
            'rrw.review_row_weighted_avg_review_score',
            'actual.avg_review_score'
        ) }}
    )
    or (
        {{ nullable_rate_mismatch(
            'iw.item_weighted_avg_time_to_review_days',
            'expected.expected_avg_time_to_review_days'
        ) }}
        and not {{ nullable_rate_mismatch(
            'iw.item_weighted_avg_time_to_review_days',
            'actual.avg_time_to_review_days'
        ) }}
    )
    or (
        {{ nullable_rate_mismatch(
            'rrw.review_row_weighted_avg_time_to_review_days',
            'expected.expected_avg_time_to_review_days'
        ) }}
        and not {{ nullable_rate_mismatch(
            'rrw.review_row_weighted_avg_time_to_review_days',
            'actual.avg_time_to_review_days'
        ) }}
    )
