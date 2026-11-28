-- Asserts that for all directed store pairs in fct_store_cross_catchment_transitions:
-- 1. origin_store_id != destination_store_id
-- 2. total_origin_devices > 0
-- 3. 0 <= unique_transitioning_devices_15m <= unique_transitioning_devices_30m
-- 4. unique_transitioning_devices_30m <= unique_transitioning_devices_60m
-- 5. unique_transitioning_devices_60m <= total_origin_devices (Contract Invariant)
-- 6. 0.0000 <= pctr_15m <= pctr_30m <= pctr_60m <= 1.0000
SELECT
    origin_store_id,
    destination_store_id,
    total_origin_devices,
    unique_transitioning_devices_15m,
    unique_transitioning_devices_30m,
    unique_transitioning_devices_60m,
    pctr_15m,
    pctr_30m,
    pctr_60m
FROM {{ ref('fct_store_cross_catchment_transitions') }}
WHERE origin_store_id = destination_store_id
   OR total_origin_devices <= 0
   OR unique_transitioning_devices_15m < 0
   OR unique_transitioning_devices_15m > unique_transitioning_devices_30m
   OR unique_transitioning_devices_30m > unique_transitioning_devices_60m
   OR unique_transitioning_devices_60m > total_origin_devices
   OR pctr_15m < 0.0000
   OR pctr_15m > pctr_30m
   OR pctr_30m > pctr_60m
   OR pctr_60m > 1.0000
