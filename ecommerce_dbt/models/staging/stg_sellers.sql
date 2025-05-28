SELECT
    TRIM(seller_id) AS seller_id,
    -- Cast zip code prefix to TEXT before trimming since it's a number in source
    TRIM(CAST(seller_zip_code_prefix AS TEXT)) AS seller_zip_code_prefix,
    TRIM(seller_city) AS seller_city,
    TRIM(seller_state) AS seller_state
FROM {{ source('dbt_dev', 'stg_sellers') }}
WHERE seller_id IS NOT NULL AND seller_id <> ''