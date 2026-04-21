-- Contract test: delivery_delay_bucket must stay semantically aligned with the
-- delivery SLA flags. This is the macro-level contract test for the shared
-- delivery_delay_bucket logic, so downstream mart reconciliations are allowed
-- to reuse the macro without independently re-testing every bucket boundary.
-- It protects the shared bucket contract reused by fact_reviews,
-- mart_fulfillment_ops, and mart_customer_experience.
select
    review_id,
    order_id,
    is_delivered,
    is_late,
    is_cancelled,
    late_days,
    delivery_delay_bucket
from {{ ref('int_review_enriched') }}
where
    ((is_cancelled or not is_delivered) and delivery_delay_bucket != 'not_delivered')
    or (
        delivery_delay_bucket = 'not_delivered'
        and is_delivered = true
        and is_cancelled = false
    )
    or (
        is_delivered = true
        and is_late = false
        and delivery_delay_bucket != 'on_time'
    )
    or (
        is_delivered = true
        and is_late = true
        and late_days between 1 and 3
        and delivery_delay_bucket != '1_to_3_days'
    )
    or (
        is_delivered = true
        and is_late = true
        and late_days between 4 and 7
        and delivery_delay_bucket != '4_to_7_days'
    )
    or (
        is_delivered = true
        and is_late = true
        and late_days between 8 and 14
        and delivery_delay_bucket != '8_to_14_days'
    )
    or (
        is_delivered = true
        and is_late = true
        and late_days >= 15
        and delivery_delay_bucket != '15_plus_days'
    )
