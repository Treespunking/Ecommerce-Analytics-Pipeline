SELECT
    -- Cast to TEXT and filter properly
    geolocation_zip_code_prefix::TEXT AS geolocation_zip_code_prefix,
    geolocation_lat::NUMERIC AS geolocation_lat,
    geolocation_lng::NUMERIC AS geolocation_lng,
    TRIM(geolocation_city) AS geolocation_city,
    TRIM(geolocation_state) AS geolocation_state
FROM {{ source('dbt_dev', 'stg_geolocation') }}
-- Avoid comparing BIGINT with empty string, cast to TEXT
WHERE geolocation_zip_code_prefix IS NOT NULL
  AND geolocation_zip_code_prefix::TEXT <> ''