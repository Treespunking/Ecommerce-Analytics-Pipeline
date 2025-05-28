SELECT
    TRIM(order_id) AS order_id,
    payment_sequential::INT AS payment_sequential,
    TRIM(payment_type) AS payment_type,
    payment_installments::INT AS payment_installments,
    payment_value::NUMERIC AS payment_value
FROM {{ source('dbt_dev', 'stg_order_payments') }}
WHERE order_id IS NOT NULL AND order_id <> ''