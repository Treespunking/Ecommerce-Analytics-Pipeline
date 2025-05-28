WITH date_spine AS (
    SELECT DISTINCT order_purchase_timestamp::DATE AS date_key
    FROM {{ ref('stg_orders') }}
    WHERE order_purchase_timestamp IS NOT NULL

    UNION

    SELECT DISTINCT order_delivered_customer_date::DATE
    FROM {{ ref('stg_orders') }}
    WHERE order_delivered_customer_date IS NOT NULL

    UNION

    SELECT DISTINCT review_creation_date::DATE
    FROM {{ ref('stg_order_reviews') }}
    WHERE review_creation_date IS NOT NULL
),
date_dim AS (
    SELECT 
        date_key,
        EXTRACT(YEAR FROM date_key) AS year,
        EXTRACT(QUARTER FROM date_key) AS quarter,
        EXTRACT(MONTH FROM date_key) AS month,
        TO_CHAR(date_key, 'Month') AS month_name,
        EXTRACT(WEEK FROM date_key) AS week_of_year,
        EXTRACT(DAY FROM date_key) AS day_of_month,
        EXTRACT(ISODOW FROM date_key) AS day_of_week,
        CASE WHEN EXTRACT(ISODOW FROM date_key) IN (6,7) THEN TRUE ELSE FALSE END AS is_weekend
    FROM date_spine
    ORDER BY date_key
)
SELECT * FROM date_dim