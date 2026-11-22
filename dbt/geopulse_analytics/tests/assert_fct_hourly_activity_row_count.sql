-- Asserts that fct_hourly_store_activity matches the verified Spark baseline of exactly 81 active store-hours.
WITH row_count AS (
    SELECT COUNT(*) AS total_records
    FROM {{ ref('fct_hourly_store_activity') }}
)
SELECT total_records
FROM row_count
WHERE total_records != 81
