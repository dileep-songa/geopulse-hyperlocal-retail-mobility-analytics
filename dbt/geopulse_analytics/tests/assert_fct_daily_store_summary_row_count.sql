-- Asserts that fct_daily_store_summary contains exactly 8 rows (4 stores x 2 calendar dates).
WITH row_count AS (
    SELECT COUNT(*) AS total_records
    FROM {{ ref('fct_daily_store_summary') }}
)
SELECT total_records
FROM row_count
WHERE total_records != 8
