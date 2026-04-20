-- Singular test: seller daily average review score must be weighted by
-- seller-order, not by item rows. A row returned here means multi-item orders
-- are over-weighting the seller review average.
with seller_orders as (

    select
        oi.seller_id,
        oi.order_id,
        d.purchase_date as calendar_date
    from {{ ref('stg_order_items') }} as oi
    inner join {{ ref('int_order_delivery') }} as d
        on oi.order_id = d.order_id
    group by oi.seller_id, oi.order_id, d.purchase_date

),

reviews_by_order as (

    select
        order_id,
        avg(cast(review_score as float64)) as avg_review_score_for_order
    from {{ ref('int_review_enriched') }}
    group by order_id

),

expected as (

    select
        so.seller_id,
        so.calendar_date,
        avg(r.avg_review_score_for_order) as expected_avg_review_score
    from seller_orders as so
    left join reviews_by_order as r
        on so.order_id = r.order_id
    group by so.seller_id, so.calendar_date

),

actual as (

    select
        seller_id,
        calendar_date,
        avg_review_score
    from {{ ref('int_seller_daily_performance') }}

)

select
    actual.seller_id,
    actual.calendar_date,
    actual.avg_review_score,
    expected.expected_avg_review_score
from actual
inner join expected
    on actual.seller_id = expected.seller_id
    and actual.calendar_date = expected.calendar_date
where
    (
        actual.avg_review_score is null
        and expected.expected_avg_review_score is not null
    )
    or (
        actual.avg_review_score is not null
        and expected.expected_avg_review_score is null
    )
    or abs(actual.avg_review_score - expected.expected_avg_review_score) > 0.000001
