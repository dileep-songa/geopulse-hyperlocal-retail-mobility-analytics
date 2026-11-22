WITH memberships AS (
    SELECT * FROM {{ ref('stg_store_catchment_memberships') }}
),

ping_store_counts AS (
    SELECT
        ping_id,
        COUNT(DISTINCT store_id) AS ping_store_count
    FROM memberships
    GROUP BY ping_id
),

device_store_counts AS (
    SELECT
        device_id,
        COUNT(DISTINCT store_id) AS device_store_count
    FROM memberships
    GROUP BY device_id
),

tagged AS (
    SELECT
        m.ping_id,
        m.device_id,
        m.event_timestamp,
        m.event_timestamp_ist,
        m.calendar_date,
        m.hour_of_day,
        m.time_of_day,
        m.store_id,
        m.store_name,
        m.distance_meters,
        m.movement_mode,
        m.data_provenance,
        p.ping_store_count,
        d.device_store_count,
        -- Ping-level overlap flags
        CASE WHEN p.ping_store_count = 1 THEN 1 ELSE 0 END AS is_exclusive_ping,
        CASE WHEN p.ping_store_count > 1 THEN 1 ELSE 0 END AS is_overlapping_ping,
        -- Device-level overlap identifiers
        -- Device exclusivity: device was observed strictly within this single store catchment across its entire trajectory
        CASE WHEN d.device_store_count = 1 THEN m.device_id ELSE NULL END AS exclusive_device_id,
        -- Multi-catchment device: device was observed across its cumulative trajectory in 2 or more candidate store catchments
        -- Important: Reflects spatial trajectory reach across the observation window, not simultaneous visits or commercial cannibalization
        CASE WHEN d.device_store_count > 1 THEN m.device_id ELSE NULL END AS multi_catchment_device_id,
        m._ingested_at,
        m._staged_file_name,
        m._staged_file_row_number
    FROM memberships m
    INNER JOIN ping_store_counts p ON m.ping_id = p.ping_id
    INNER JOIN device_store_counts d ON m.device_id = d.device_id
)

SELECT * FROM tagged
