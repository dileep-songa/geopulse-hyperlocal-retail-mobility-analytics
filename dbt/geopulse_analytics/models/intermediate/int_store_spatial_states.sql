{{
    config(
        materialized='view'
    )
}}

/*
    Model: int_store_spatial_states
    Layer: Intermediate
    Grain: (ping_id, store_id_a, store_id_b)
    Expected Rows: 60,000 rows (10,000 observations x 6 unordered store pairs)

    Purpose:
    Evaluates pairwise spatial state classification for every source observation ping
    across all six canonical unordered candidate store pairs (store_id_a < store_id_b).

    Pairwise States:
    - ORIGIN_ONLY:  Ping is inside Catchment A and outside Catchment B.
    - DEST_ONLY:    Ping is inside Catchment B and outside Catchment A.
    - SHARED_PAIR:  Ping is inside both Catchment A and Catchment B.
    - OUTSIDE_PAIR: Ping is outside both Catchment A and Catchment B.

    Design Contract:
    - Full Trajectory Preservation: Sourced from stg_mobility_pings preserving all 10,000 pings.
    - Exact Catchment Semantics: Evaluates containment from canonical Week 2 catchment
      memberships (stg_store_catchment_memberships), avoiding radial approximations.
    - Third-Store Independence: Any observation in Store C or D is classified strictly
      relative to pair (A, B) as OUTSIDE_PAIR, preserving intermediate continuity.
*/

WITH pings AS (
    SELECT
        ping_id,
        device_id,
        event_timestamp,
        event_timestamp_ist,
        calendar_date,
        hour_of_day,
        movement_mode,
        latitude,
        longitude
    FROM {{ ref('stg_mobility_pings') }}
),

memberships AS (
    SELECT
        ping_id,
        store_id,
        store_name
    FROM {{ ref('stg_store_catchment_memberships') }}
),

-- Canonical candidate stores
stores AS (
    SELECT 'STORE_A' AS store_id, 'GeoPulse Coffee A' AS store_name UNION ALL
    SELECT 'STORE_B', 'GeoPulse Coffee B' UNION ALL
    SELECT 'STORE_C', 'GeoPulse Coffee C' UNION ALL
    SELECT 'STORE_D', 'GeoPulse Coffee D'
),

-- Six canonical unordered store pairs (store_id_a < store_id_b)
store_pairs AS (
    SELECT
        s1.store_id AS store_id_a,
        s1.store_name AS store_name_a,
        s2.store_id AS store_id_b,
        s2.store_name AS store_name_b
    FROM stores s1
    CROSS JOIN stores s2
    WHERE s1.store_id < s2.store_id
),

-- Complete cross-product evaluation grid: 10,000 pings x 6 store pairs = 60,000 rows
ping_pairs AS (
    SELECT
        p.ping_id,
        p.device_id,
        p.event_timestamp,
        p.event_timestamp_ist,
        p.calendar_date,
        p.hour_of_day,
        p.movement_mode,
        p.latitude,
        p.longitude,
        sp.store_id_a,
        sp.store_name_a,
        sp.store_id_b,
        sp.store_name_b
    FROM pings p
    CROSS JOIN store_pairs sp
),

-- Classify spatial state relative to store pair (A, B)
classified AS (
    SELECT
        pp.ping_id,
        pp.device_id,
        pp.event_timestamp,
        pp.event_timestamp_ist,
        pp.calendar_date,
        pp.hour_of_day,
        pp.movement_mode,
        pp.latitude,
        pp.longitude,
        pp.store_id_a,
        pp.store_name_a,
        pp.store_id_b,
        pp.store_name_b,
        CASE WHEN ma.ping_id IS NOT NULL THEN 1 ELSE 0 END AS in_store_a,
        CASE WHEN mb.ping_id IS NOT NULL THEN 1 ELSE 0 END AS in_store_b,
        CASE
            WHEN ma.ping_id IS NOT NULL AND mb.ping_id IS NULL THEN 'ORIGIN_ONLY'
            WHEN ma.ping_id IS NULL AND mb.ping_id IS NOT NULL THEN 'DEST_ONLY'
            WHEN ma.ping_id IS NOT NULL AND mb.ping_id IS NOT NULL THEN 'SHARED_PAIR'
            ELSE 'OUTSIDE_PAIR'
        END AS pairwise_state
    FROM ping_pairs pp
    LEFT JOIN memberships ma
        ON pp.ping_id = ma.ping_id
        AND pp.store_id_a = ma.store_id
    LEFT JOIN memberships mb
        ON pp.ping_id = mb.ping_id
        AND pp.store_id_b = mb.store_id
)

SELECT
    ping_id,
    device_id,
    event_timestamp,
    event_timestamp_ist,
    calendar_date,
    hour_of_day,
    movement_mode,
    latitude,
    longitude,
    store_id_a,
    store_name_a,
    store_id_b,
    store_name_b,
    in_store_a,
    in_store_b,
    pairwise_state
FROM classified
