WITH sales AS (
    SELECT 
        o.order_id,
        o.customer_id,
        oi.product_id,
        oi.seller_id,
        oi.order_item_id,
        oi.price AS unit_price,
        oi.freight_value,
        (oi.price + oi.freight_value) AS total_amount,
        oi.shipping_limit_date,
        o.order_status,
        o.order_purchase_timestamp AS order_date,
        o.order_delivered_customer_date AS delivered_date,
        EXTRACT(DAY FROM o.order_delivered_customer_date - o.order_purchase_timestamp) AS delivery_days,
        o.order_estimated_delivery_date
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_order_items') }} oi 
        ON o.order_id = oi.order_id
)
SELECT * FROM sales