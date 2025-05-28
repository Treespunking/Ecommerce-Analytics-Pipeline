SELECT
    TRIM(order_id) AS order_id,
    TRIM(customer_id) AS customer_id,
    TRIM(order_status) AS order_status,
    order_purchase_timestamp::TIMESTAMP AS order_purchase_timestamp,
    order_approved_at::TIMESTAMP AS order_approved_at,
    order_delivered_carrier_date::TIMESTAMP AS order_delivered_carrier_date,
    order_delivered_customer_date::TIMESTAMP AS order_delivered_customer_date,
    order_estimated_delivery_date::TIMESTAMP AS order_estimated_delivery_date
FROM {{ source('dbt_dev', 'stg_orders') }}
WHERE order_id IS NOT NULL AND order_id <> ''