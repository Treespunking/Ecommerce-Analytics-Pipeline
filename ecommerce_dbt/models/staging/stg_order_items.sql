-- models/staging/stg_order_items.sql
SELECT
    TRIM(order_id) AS order_id,
    order_item_id::INT AS order_item_id,
    TRIM(product_id) AS product_id,
    TRIM(seller_id) AS seller_id,
    shipping_limit_date::TIMESTAMP AS shipping_limit_date,
    price::NUMERIC AS price,
    freight_value::NUMERIC AS freight_value
FROM {{ source('dbt_dev', 'stg_order_items') }}
WHERE order_id IS NOT NULL AND order_id <> ''