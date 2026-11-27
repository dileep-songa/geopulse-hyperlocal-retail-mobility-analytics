{{
    config(
        materialized='view'
    )
}}

/*
    Model: int_store_pairwise_device_transitions
    Layer: Intermediate
    Grain: transition_id

    Purpose:
    Extracts valid directed cross-catchment customer transitions across all 12 directed store pairs
    (origin != destination) preserving full trajectory sequence and kinematic continuity:
    - Departure endpoint must be exclusive ORIGIN_ONLY (last consecutive origin episode ping).
    - Arrival endpoint must be exclusive DEST_ONLY (earliest eligible arrival within 60 minutes).
    - SHARED_PAIR and OUTSIDE_PAIR cannot be endpoints, but are valid intermediate traversal states.
    - Two-tier kinematic plausibility filtering:
        * Step velocity ceiling (15.0 m/s) and simultaneous coordinate conflict detection.
        * Anomaly hard breaks: anomalies abort active episodes and reset trajectory search.
        * Inter-catchment transit speed ceiling (<= 15.0 m/s).
    - Never bridges across omitted, discarded, or anomalous observations.
*/

WITH pings AS (
    SELECT
        ping_id,
        device_id,
        event_timestamp,
        latitude,
        longitude
    FROM {{ ref('stg_mobility_pings') }}
),

memberships AS (
    SELECT
        ping_id,
        store_id
    FROM {{ ref('stg_store_catchment_memberships') }}
),

stores AS (
    SELECT 'STORE_A' AS store_id UNION ALL
    SELECT 'STORE_B' UNION ALL
    SELECT 'STORE_C' UNION ALL
    SELECT 'STORE_D'
),

-- Complete 12 directed pairs grid (origin != destination)
directed_pairs AS (
    SELECT
        s1.store_id AS origin_store_id,
        s2.store_id AS destination_store_id
    FROM stores s1
    CROSS JOIN stores s2
    WHERE s1.store_id != s2.store_id
),

-- Step 1: Detect simultaneous pings and flag coordinate conflicts (teleportation anomalies)
simultaneous_checked AS (
    SELECT
        p.ping_id,
        p.device_id,
        p.event_timestamp,
        p.latitude,
        p.longitude,
        COUNT(*) OVER (PARTITION BY p.device_id, p.event_timestamp) AS sim_count,
        MIN(p.latitude) OVER (PARTITION BY p.device_id, p.event_timestamp) AS min_lat,
        MAX(p.latitude) OVER (PARTITION BY p.device_id, p.event_timestamp) AS max_lat,
        MIN(p.longitude) OVER (PARTITION BY p.device_id, p.event_timestamp) AS min_lon,
        MAX(p.longitude) OVER (PARTITION BY p.device_id, p.event_timestamp) AS max_lon,
        ROW_NUMBER() OVER (PARTITION BY p.device_id, p.event_timestamp ORDER BY p.ping_id) AS sim_rn
    FROM pings p
),

pings_deduped AS (
    SELECT
        ping_id,
        device_id,
        event_timestamp,
        latitude,
        longitude,
        CASE
            WHEN sim_count > 1 AND (min_lat != max_lat OR min_lon != max_lon) THEN TRUE
            ELSE FALSE
        END AS is_sim_anomaly
    FROM simultaneous_checked
    WHERE sim_rn = 1 OR (sim_count > 1 AND (min_lat != max_lat OR min_lon != max_lon))
),

-- Step 2: Step-level velocity filter (v_step > 15.0 m/s is an observation-level anomaly)
pings_velocity AS (
    SELECT
        ping_id,
        device_id,
        event_timestamp,
        latitude,
        longitude,
        is_sim_anomaly,
        LAG(event_timestamp) OVER (PARTITION BY device_id ORDER BY event_timestamp, ping_id) AS prev_ts,
        LAG(latitude) OVER (PARTITION BY device_id ORDER BY event_timestamp, ping_id) AS prev_lat,
        LAG(longitude) OVER (PARTITION BY device_id ORDER BY event_timestamp, ping_id) AS prev_lon
    FROM pings_deduped
),

pings_flagged AS (
    SELECT
        ping_id,
        device_id,
        event_timestamp,
        latitude,
        longitude,
        CASE
            WHEN is_sim_anomaly THEN TRUE
            WHEN prev_ts IS NOT NULL AND DATEDIFF('second', prev_ts, event_timestamp) > 0 THEN
                (SQRT(POW((latitude - prev_lat) * 110600.0, 2) + POW((longitude - prev_lon) * 108477.0, 2)) / DATEDIFF('second', prev_ts, event_timestamp)) > 15.0
            ELSE FALSE
        END AS is_anomaly
    FROM pings_velocity
),

-- Step 3: Classify spatial state relative to each directed pair across full observation universe
pair_pings AS (
    SELECT
        p.ping_id,
        p.device_id,
        p.event_timestamp,
        p.latitude,
        p.longitude,
        p.is_anomaly,
        dp.origin_store_id,
        dp.destination_store_id,
        CASE
            WHEN ma.ping_id IS NOT NULL AND mb.ping_id IS NULL THEN 'ORIGIN_ONLY'
            WHEN ma.ping_id IS NULL AND mb.ping_id IS NOT NULL THEN 'DEST_ONLY'
            WHEN ma.ping_id IS NOT NULL AND mb.ping_id IS NOT NULL THEN 'SHARED_PAIR'
            ELSE 'OUTSIDE_PAIR'
        END AS observation_state
    FROM pings_flagged p
    CROSS JOIN directed_pairs dp
    LEFT JOIN memberships ma
        ON p.ping_id = ma.ping_id AND dp.origin_store_id = ma.store_id
    LEFT JOIN memberships mb
        ON p.ping_id = mb.ping_id AND dp.destination_store_id = mb.store_id
),

-- Step 4: Partition trajectory segments by anomaly events to prevent false bridging
sequenced AS (
    SELECT
        ping_id,
        device_id,
        event_timestamp,
        latitude,
        longitude,
        origin_store_id,
        destination_store_id,
        observation_state,
        is_anomaly,
        SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) OVER (
            PARTITION BY device_id, origin_store_id, destination_store_id
            ORDER BY event_timestamp, ping_id
        ) AS anomaly_segment
    FROM pair_pings
),

-- Step 5: Look ahead to earliest eligible destination arrival within contiguous segment
endpoints AS (
    SELECT
        ping_id,
        device_id,
        event_timestamp,
        latitude,
        longitude,
        origin_store_id,
        destination_store_id,
        observation_state,
        anomaly_segment,
        is_anomaly,
        LEAD(observation_state) OVER (
            PARTITION BY device_id, origin_store_id, destination_store_id
            ORDER BY event_timestamp, ping_id
        ) AS next_immediate_state,
        LEAD(CASE WHEN observation_state IN ('ORIGIN_ONLY', 'DEST_ONLY') THEN observation_state END) IGNORE NULLS OVER (
            PARTITION BY device_id, origin_store_id, destination_store_id
            ORDER BY event_timestamp, ping_id
        ) AS next_endpoint_state,
        LEAD(CASE WHEN observation_state IN ('ORIGIN_ONLY', 'DEST_ONLY') THEN event_timestamp END) IGNORE NULLS OVER (
            PARTITION BY device_id, origin_store_id, destination_store_id
            ORDER BY event_timestamp, ping_id
        ) AS next_endpoint_ts,
        LEAD(CASE WHEN observation_state IN ('ORIGIN_ONLY', 'DEST_ONLY') THEN ping_id END) IGNORE NULLS OVER (
            PARTITION BY device_id, origin_store_id, destination_store_id
            ORDER BY event_timestamp, ping_id
        ) AS next_endpoint_ping_id,
        LEAD(CASE WHEN observation_state IN ('ORIGIN_ONLY', 'DEST_ONLY') THEN latitude END) IGNORE NULLS OVER (
            PARTITION BY device_id, origin_store_id, destination_store_id
            ORDER BY event_timestamp, ping_id
        ) AS next_endpoint_lat,
        LEAD(CASE WHEN observation_state IN ('ORIGIN_ONLY', 'DEST_ONLY') THEN longitude END) IGNORE NULLS OVER (
            PARTITION BY device_id, origin_store_id, destination_store_id
            ORDER BY event_timestamp, ping_id
        ) AS next_endpoint_lon,
        LEAD(CASE WHEN observation_state IN ('ORIGIN_ONLY', 'DEST_ONLY') THEN anomaly_segment END) IGNORE NULLS OVER (
            PARTITION BY device_id, origin_store_id, destination_store_id
            ORDER BY event_timestamp, ping_id
        ) AS next_endpoint_anomaly_segment
    FROM sequenced
),

-- Step 6: Identify valid candidate transitions
candidate_transitions AS (
    SELECT
        device_id,
        origin_store_id,
        destination_store_id,
        ping_id AS exit_ping_id,
        next_endpoint_ping_id AS entry_ping_id,
        event_timestamp AS exit_timestamp,
        next_endpoint_ts AS entry_timestamp,
        DATEDIFF('second', event_timestamp, next_endpoint_ts) / 60.0 AS duration_minutes,
        ROUND(SQRT(POW((next_endpoint_lat - latitude) * 110600.0, 2) + POW((next_endpoint_lon - longitude) * 108477.0, 2)), 2) AS displacement_distance_m
    FROM endpoints
    WHERE observation_state = 'ORIGIN_ONLY'
      AND next_endpoint_state = 'DEST_ONLY'
      -- Enforce departure from last consecutive ORIGIN_ONLY observation
      AND next_immediate_state != 'ORIGIN_ONLY'
      -- Enforce anomaly hard break: zero anomalies permitted between departure and arrival
      AND NOT is_anomaly
      AND anomaly_segment = next_endpoint_anomaly_segment
),

-- Step 7: Apply 60-minute window and transit velocity ceiling (<= 15.0 m/s)
filtered_transitions AS (
    SELECT
        device_id,
        origin_store_id,
        destination_store_id,
        exit_ping_id,
        entry_ping_id,
        exit_timestamp,
        entry_timestamp,
        duration_minutes,
        displacement_distance_m,
        ROUND(displacement_distance_m / NULLIF(duration_minutes * 60.0, 0), 2) AS transit_speed_mps
    FROM candidate_transitions
    WHERE duration_minutes > 0.0
      AND duration_minutes <= 60.0
      AND (displacement_distance_m / NULLIF(duration_minutes * 60.0, 0)) <= 15.0
)

SELECT
    'TR_' || origin_store_id || '_' || destination_store_id || '_' || device_id || '_' || TO_VARCHAR(exit_timestamp, 'YYYYMMDDHH24MISS') AS transition_id,
    device_id,
    origin_store_id,
    destination_store_id,
    exit_ping_id,
    entry_ping_id,
    exit_timestamp,
    entry_timestamp,
    ROUND(duration_minutes, 2)::NUMBER(6, 2) AS duration_minutes,
    displacement_distance_m::NUMBER(12, 2) AS displacement_distance_m,
    transit_speed_mps::NUMBER(6, 2) AS transit_speed_mps,
    TRUE AS is_kinematically_plausible
FROM filtered_transitions
