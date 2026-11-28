{{
    config(
        materialized='table'
    )
}}

/*
    Model: fct_store_cross_catchment_transitions
    Layer: Mart
    Grain: (origin_store_id, destination_store_id)
    Expected Rows: 12 rows (12 directed pairs for 4 stores)

    Purpose:
    Summarizes directional customer mobility flows and transition kinematics between
    store catchments across 15, 30, and 60 minute travel windows:
    - Unique transitioning devices (U_15m, U_30m, U_60m).
    - Total completed transition events.
    - Directional Pairwise Cross-Catchment Transition Rates (PCTR_15m, PCTR_30m, PCTR_60m).
    - Kinematic transit duration summaries (median, minimum, maximum).
    - Average displacement velocity in metres per second.
*/

WITH memberships AS (
    SELECT
        device_id,
        store_id
    FROM {{ ref('stg_store_catchment_memberships') }}
),

transitions AS (
    SELECT
        transition_id,
        device_id,
        origin_store_id,
        destination_store_id,
        duration_minutes,
        displacement_distance_m,
        transit_speed_mps
    FROM {{ ref('int_store_pairwise_device_transitions') }}
),

stores AS (
    SELECT DISTINCT
        store_id,
        store_name
    FROM {{ ref('stg_store_catchment_memberships') }}
),

-- Complete 12 directed pairs grid
directed_pairs AS (
    SELECT
        s_orig.store_id AS origin_store_id,
        s_orig.store_name AS origin_store_name,
        s_dest.store_id AS destination_store_id,
        s_dest.store_name AS destination_store_name
    FROM stores s_orig
    CROSS JOIN stores s_dest
    WHERE s_orig.store_id != s_dest.store_id
),

-- Origin store device baseline (denominator for PCTR)
origin_store_devices AS (
    SELECT
        store_id AS origin_store_id,
        COUNT(DISTINCT device_id) AS total_origin_devices
    FROM memberships
    GROUP BY store_id
),

-- Transition aggregations per directed pair
transition_summary AS (
    SELECT
        origin_store_id,
        destination_store_id,
        COUNT(transition_id) AS total_transition_events,
        COUNT(DISTINCT device_id) AS total_transitioning_devices,
        COUNT(DISTINCT CASE WHEN duration_minutes <= 15.0 THEN device_id END) AS unique_transitioning_devices_15m,
        COUNT(DISTINCT CASE WHEN duration_minutes <= 30.0 THEN device_id END) AS unique_transitioning_devices_30m,
        COUNT(DISTINCT CASE WHEN duration_minutes <= 60.0 THEN device_id END) AS unique_transitioning_devices_60m,
        ROUND(MEDIAN(duration_minutes), 2) AS median_transit_minutes,
        ROUND(MIN(duration_minutes), 2) AS min_transit_minutes,
        ROUND(MAX(duration_minutes), 2) AS max_transit_minutes,
        ROUND(AVG(transit_speed_mps), 2) AS mean_transit_speed_mps
    FROM transitions
    GROUP BY
        origin_store_id,
        destination_store_id
)

SELECT
    dp.origin_store_id,
    dp.origin_store_name,
    dp.destination_store_id,
    dp.destination_store_name,
    osd.total_origin_devices,
    COALESCE(ts.total_transition_events, 0) AS total_transition_events,
    COALESCE(ts.total_transitioning_devices, 0) AS total_transitioning_devices,
    COALESCE(ts.unique_transitioning_devices_15m, 0) AS unique_transitioning_devices_15m,
    COALESCE(ts.unique_transitioning_devices_30m, 0) AS unique_transitioning_devices_30m,
    COALESCE(ts.unique_transitioning_devices_60m, 0) AS unique_transitioning_devices_60m,
    -- PCTR directional rates bounded in [0.0, 1.0]
    ROUND(COALESCE(ts.unique_transitioning_devices_15m, 0)::FLOAT / NULLIF(osd.total_origin_devices, 0), 4)::NUMBER(6,4) AS pctr_15m,
    ROUND(COALESCE(ts.unique_transitioning_devices_30m, 0)::FLOAT / NULLIF(osd.total_origin_devices, 0), 4)::NUMBER(6,4) AS pctr_30m,
    ROUND(COALESCE(ts.unique_transitioning_devices_60m, 0)::FLOAT / NULLIF(osd.total_origin_devices, 0), 4)::NUMBER(6,4) AS pctr_60m,
    ts.median_transit_minutes::NUMBER(6,2) AS median_transit_minutes,
    ts.min_transit_minutes::NUMBER(6,2) AS min_transit_minutes,
    ts.max_transit_minutes::NUMBER(6,2) AS max_transit_minutes,
    ts.mean_transit_speed_mps::NUMBER(6,2) AS mean_transit_speed_mps
FROM directed_pairs dp
INNER JOIN origin_store_devices osd
    ON dp.origin_store_id = osd.origin_store_id
LEFT JOIN transition_summary ts
    ON dp.origin_store_id = ts.origin_store_id
    AND dp.destination_store_id = ts.destination_store_id
ORDER BY
    dp.origin_store_id,
    dp.destination_store_id
