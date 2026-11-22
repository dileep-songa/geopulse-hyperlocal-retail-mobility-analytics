-- Asserts that for every store in fct_store_overlap_summary:
-- 1. exclusive_pings + overlapping_pings equals total_pings
-- 2. exclusive_devices + multi_catchment_devices equals total_devices
SELECT
    store_id,
    total_pings,
    exclusive_pings,
    overlapping_pings,
    total_devices,
    exclusive_devices,
    multi_catchment_devices
FROM {{ ref('fct_store_overlap_summary') }}
WHERE (exclusive_pings + overlapping_pings) != total_pings
   OR (exclusive_devices + multi_catchment_devices) != total_devices
