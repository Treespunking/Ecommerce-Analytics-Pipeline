SELECT
    TRIM(product_id) AS product_id,
    TRIM(product_category_name) AS product_category_name,
    product_name_length::INT AS product_name_length,

    product_description_length::INT AS product_description_length,
    product_photos_qty::INT AS product_photos_qty,
    product_weight_g::INT AS product_weight_g,
    product_length_cm::INT AS product_length_cm,
    product_height_cm::INT AS product_height_cm,
    product_width_cm::INT AS product_width_cm
FROM {{ source('dbt_dev', 'stg_products') }}
WHERE product_id IS NOT NULL AND product_id <> ''