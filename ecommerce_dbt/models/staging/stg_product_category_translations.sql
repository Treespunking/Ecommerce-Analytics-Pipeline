SELECT
    TRIM(product_category_name) AS product_category_name,
    TRIM(product_category_name_english) AS product_category_name_english
FROM {{ source('dbt_dev', 'stg_product_category_translations') }}
WHERE product_category_name IS NOT NULL AND product_category_name <> ''