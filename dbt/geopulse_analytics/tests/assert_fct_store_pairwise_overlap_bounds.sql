-- Asserts that for all store pairs in fct_store_pairwise_overlap:
-- 1. store_id_a is strictly less than store_id_b
-- 2. geometric_overlap_ratio is bounded between 0.0000 and 1.0000
-- 3. device_jaccard_index is bounded between 0.0000 and 1.0000
-- 4. overlap_coefficient is bounded between 0.0000 and 1.0000
-- 5. shared_unique_devices <= union_unique_devices
-- 6. shared_unique_devices <= unique_devices_store_a AND shared_unique_devices <= unique_devices_store_b
-- 7. lens_intersection_area_m2 >= 0.0 AND union_polygon_area_m2 > 0.0
-- 8. total_shared_lens_pings >= 0
SELECT
    store_id_a,
    store_id_b,
    geometric_overlap_ratio,
    device_jaccard_index,
    overlap_coefficient,
    shared_unique_devices,
    union_unique_devices,
    unique_devices_store_a,
    unique_devices_store_b
FROM {{ ref('fct_store_pairwise_overlap') }}
WHERE store_id_a >= store_id_b
   OR geometric_overlap_ratio < 0.0 OR geometric_overlap_ratio > 1.0
   OR device_jaccard_index < 0.0 OR device_jaccard_index > 1.0
   OR overlap_coefficient < 0.0 OR overlap_coefficient > 1.0
   OR shared_unique_devices > union_unique_devices
   OR shared_unique_devices > unique_devices_store_a
   OR shared_unique_devices > unique_devices_store_b
   OR lens_intersection_area_m2 < 0.0
   OR union_polygon_area_m2 <= 0.0
   OR total_shared_lens_pings < 0
