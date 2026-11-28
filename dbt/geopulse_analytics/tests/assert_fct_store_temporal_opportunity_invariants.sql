-- Asserts that for all store temporal profiles in fct_store_temporal_opportunity:
-- 1. Exactly 4 rows exist
-- 2. operating_volume > 0
-- 3. peak_hour_volume <= operating_volume
-- 4. low_activity_hours_count is between 0 and 14
-- 5. low_activity_hour_share is bounded between 0.0000 and 1.0000
-- 6. If has_zero_median_activity is false, peak_hour_index >= 1.00
-- 7. If has_zero_median_activity is true, peak_hour_index IS NULL
SELECT
    store_id,
    operating_volume,
    peak_hour_volume,
    median_operating_hourly_volume,
    has_zero_median_activity,
    peak_hour_index,
    low_activity_hours_count,
    low_activity_hour_share
FROM {{ ref('fct_store_temporal_opportunity') }}
WHERE operating_volume <= 0
   OR peak_hour_volume > operating_volume
   OR low_activity_hours_count < 0
   OR low_activity_hours_count > 14
   OR low_activity_hour_share < 0.0 OR low_activity_hour_share > 1.0
   OR (median_operating_hourly_volume > 0 AND peak_hour_index < 1.00)
   OR (has_zero_median_activity = TRUE AND peak_hour_index IS NOT NULL)
   OR (has_zero_median_activity = FALSE AND peak_hour_index IS NULL)
