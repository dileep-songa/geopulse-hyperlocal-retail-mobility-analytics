WITH tagged AS (
    SELECT * FROM {{ ref('int_store_memberships_overlap_tagged') }}
),

overlap_summary AS (
    SELECT
        store_id,
        store_name,
        COUNT(ping_id) AS total_pings,
        SUM(is_exclusive_ping) AS exclusive_pings,
        SUM(is_overlapping_ping) AS overlapping_pings,
        COUNT(DISTINCT device_id) AS total_devices,
        COUNT(DISTINCT exclusive_device_id) AS exclusive_devices,
        COUNT(DISTINCT multi_catchment_device_id) AS multi_catchment_devices
    FROM tagged
    GROUP BY
        store_id,
        store_name
)

SELECT *
FROM overlap_summary
ORDER BY
    store_id
