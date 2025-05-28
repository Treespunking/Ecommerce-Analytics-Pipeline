SELECT
    TRIM(review_id) AS review_id,
    TRIM(order_id) AS order_id,
    review_score::INT AS review_score,
    TRIM(review_comment_title) AS review_comment_title,
    TRIM(review_comment_message) AS review_comment_message,
    review_creation_date::TIMESTAMP AS review_creation_date,
    review_answer_timestamp::TIMESTAMP AS review_answer_timestamp
FROM {{ source('dbt_dev', 'stg_order_reviews') }}
WHERE review_id IS NOT NULL AND review_id <> ''