SELECT
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY review_creation_date DESC) AS row_num
    FROM {{ ref('stg_order_reviews') }}
) ranked
WHERE row_num = 1