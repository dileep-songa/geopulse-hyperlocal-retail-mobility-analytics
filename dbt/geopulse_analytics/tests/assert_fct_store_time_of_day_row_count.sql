-- Asserts that fct_store_time_of_day_activity contains exactly 16 rows (4 stores x 4 diurnal periods).
WITH row_count AS (
    SELECT COUNT(*) AS total_records
    FROM {{ ref('fct_store_time_of_day_activity') }}
)
SELECT total_records
FROM row_count
WHERE total_records != 16
