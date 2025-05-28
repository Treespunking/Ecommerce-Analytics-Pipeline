SELECT
    TRIM(customer_id) AS customer_id,
    TRIM(customer_unique_id) AS customer_unique_id,
    -- Cast zip code prefix to TEXT before trimming since it's a number in source
    TRIM(CAST(customer_zip_code_prefix AS TEXT)) AS customer_zip_code_prefix,
    TRIM(customer_city) AS customer_city,
    TRIM(customer_state) AS customer_state
FROM {{ source('dbt_dev', 'stg_customers') }}
WHERE customer_id IS NOT NULL AND customer_id <> ''