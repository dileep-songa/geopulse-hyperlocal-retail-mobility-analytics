-- Asserts that fct_store_overlap_summary contains exactly 4 rows (1 row per candidate store).
WITH row_count AS (
    SELECT COUNT(*) AS total_records
    FROM {{ ref('fct_store_overlap_summary') }}
)
SELECT total_records
FROM row_count
WHERE total_records != 4
