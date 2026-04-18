-- =============================================================================
-- Model: stg_reviews
-- Grain: One row per review_id and order_id
-- Source: raw_olist.order_reviews
-- Purpose: Standardize Olist customer reviews for customer experience modeling.
-- Key fields:
--   review_id                   STRING     Review identifier
--   order_id                    STRING     Order identifier
--   review_score                INTEGER    Customer rating from 1 to 5
--   review_created_at_utc       TIMESTAMP  Review creation timestamp
--   ingested_at_utc             TIMESTAMP  Raw ingestion timestamp
-- Update frequency: Daily batch rebuild
-- =============================================================================
with source_data as (

    select
        *
    from {{ source('raw_olist', 'order_reviews') }}

),

renamed as (

    select
        nullif(trim(review_id), '') as review_id,
        nullif(trim(order_id), '') as order_id,
        safe_cast(review_score as int64) as review_score,

        nullif(trim(review_comment_title), '') as review_comment_title,
        nullif(trim(review_comment_message), '') as review_comment_message,
        safe_cast(nullif(trim(review_creation_date), '') as timestamp)
            as review_created_at_utc,
        safe_cast(nullif(trim(review_answer_timestamp), '') as timestamp)
            as review_answered_at_utc,

        nullif(trim(batch_id), '') as batch_id,
        safe_cast(ingested_at_utc as timestamp) as ingested_at_utc,
        nullif(trim(source_file_name), '') as source_file_name

    from source_data

),

deduplicated as (

    select *
    from renamed
    qualify row_number() over (
        partition by review_id, order_id
        order by ingested_at_utc desc, batch_id desc
    ) = 1

)

select *
from deduplicated
