{{
    config(
        materialized='table'
    )
}}

/*
    Model: fct_store_pairwise_hourly_overlap
    Layer: Mart
    Grain: (store_id_a, store_id_b, hour_of_day)
    Expected Rows: 84 rows (6 pairs x 14 operating hours)

    Purpose:
    High-resolution diurnal time-series of cross-catchment activity across the 14 operating hours
    (08:00 to 21:59 local time IST):
    - Hourly unique shared devices: Devices observed in both catchments during hour h.
    - Hourly shared ping volume: Total pings in both catchments during hour h.
    - Hourly diurnal activity share: Fraction of daily shared activity occurring in hour h.
    - Zero-fill complete hour grid: Every operating hour is represented even if volume is zero.
*/

WITH memberships AS (
    SELECT
        ping_id,
        device_id,
        store_id,
        hour_of_day
    FROM {{ ref('stg_store_catchment_memberships') }}
),

spatial_states AS (
    SELECT
        ping_id,
        device_id,
        store_id_a,
        store_name_a,
        store_id_b,
        store_name_b,
        hour_of_day,
        in_store_a,
        in_store_b,
        pairwise_state
    FROM {{ ref('int_store_spatial_states') }}
),

distinct_pairs AS (
    SELECT DISTINCT
        store_id_a,
        store_name_a,
        store_id_b,
        store_name_b
    FROM spatial_states
),

-- Complete 14 operating hours grid (08:00 - 21:59)
operating_hours AS (
    SELECT 8 AS hour_of_day UNION ALL
    SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL
    SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL
    SELECT 15 UNION ALL SELECT 16 UNION ALL SELECT 17 UNION ALL
    SELECT 18 UNION ALL SELECT 19 UNION ALL SELECT 20 UNION ALL
    SELECT 21
),

-- Complete pair-hour skeleton (84 rows)
grid AS (
    SELECT
        p.store_id_a,
        p.store_name_a,
        p.store_id_b,
        p.store_name_b,
        h.hour_of_day
    FROM distinct_pairs p
    CROSS JOIN operating_hours h
),

-- Devices observed in store A during hour h
hourly_devs_a AS (
    SELECT DISTINCT store_id AS store_id_a, device_id, hour_of_day
    FROM memberships
    WHERE hour_of_day BETWEEN 8 AND 21
),

-- Devices observed in store B during hour h
hourly_devs_b AS (
    SELECT DISTINCT store_id AS store_id_b, device_id, hour_of_day
    FROM memberships
    WHERE hour_of_day BETWEEN 8 AND 21
),

-- Count shared devices active in both stores in hour h
hourly_shared_devices AS (
    SELECT
        dp.store_id_a,
        dp.store_id_b,
        hda.hour_of_day,
        COUNT(DISTINCT hda.device_id) AS shared_device_count
    FROM distinct_pairs dp
    INNER JOIN hourly_devs_a hda
        ON dp.store_id_a = hda.store_id_a
    INNER JOIN hourly_devs_b hdb
        ON dp.store_id_b = hdb.store_id_b
        AND hda.device_id = hdb.device_id
        AND hda.hour_of_day = hdb.hour_of_day
    GROUP BY dp.store_id_a, dp.store_id_b, hda.hour_of_day
),

-- Shared lens pings inside both catchments in hour h
hourly_lens_pings AS (
    SELECT
        store_id_a,
        store_id_b,
        hour_of_day,
        COUNT(ping_id) AS shared_ping_volume
    FROM spatial_states
    WHERE pairwise_state = 'SHARED_PAIR'
      AND hour_of_day BETWEEN 8 AND 21
    GROUP BY store_id_a, store_id_b, hour_of_day
),

-- Total shared pings across all operating hours for diurnal denominator
pair_totals AS (
    SELECT
        store_id_a,
        store_id_b,
        COUNT(ping_id) AS total_operating_shared_pings
    FROM spatial_states
    WHERE pairwise_state = 'SHARED_PAIR'
      AND hour_of_day BETWEEN 8 AND 21
    GROUP BY store_id_a, store_id_b
)

SELECT
    g.store_id_a,
    g.store_name_a,
    g.store_id_b,
    g.store_name_b,
    g.hour_of_day,
    LPAD(CAST(g.hour_of_day AS VARCHAR), 2, '0') || ':00' AS hour_window,
    COALESCE(hsd.shared_device_count, 0) AS shared_device_count,
    COALESCE(hlp.shared_ping_volume, 0) AS shared_ping_volume,
    COALESCE(pt.total_operating_shared_pings, 0) AS total_operating_shared_pings,
    ROUND(COALESCE(hlp.shared_ping_volume, 0)::FLOAT / NULLIF(pt.total_operating_shared_pings, 0), 4)::NUMBER(6,4) AS hourly_diurnal_share
FROM grid g
LEFT JOIN hourly_shared_devices hsd
    ON g.store_id_a = hsd.store_id_a
    AND g.store_id_b = hsd.store_id_b
    AND g.hour_of_day = hsd.hour_of_day
LEFT JOIN hourly_lens_pings hlp
    ON g.store_id_a = hlp.store_id_a
    AND g.store_id_b = hlp.store_id_b
    AND g.hour_of_day = hlp.hour_of_day
LEFT JOIN pair_totals pt
    ON g.store_id_a = pt.store_id_a
    AND g.store_id_b = pt.store_id_b
ORDER BY
    g.store_id_a,
    g.store_id_b,
    g.hour_of_day
