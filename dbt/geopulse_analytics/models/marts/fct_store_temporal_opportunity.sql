{{
    config(
        materialized='table'
    )
}}

/*
    Model: fct_store_temporal_opportunity
    Layer: Mart
    Grain: store_id
    Expected Rows: 4 rows (1 row per candidate store)

    Purpose:
    Evaluates store-level traffic temporal profile and utilization opportunity during operating hours
    (08:00 to 21:59 local time IST, 14 operating hours total):
    - Peak Hour Index (PHI): Ratio of peak hourly volume to median operating hourly volume.
    - Peak Hour Window: Earliest operating hour achieving the peak volume (deterministic tie-breaking).
    - Low-Activity Hour Share (LAHS): Proportion of operating hours operating below 50% of store median volume.
    - Zero-Median Safety: PHI is NULL if median volume is 0, flagged with has_zero_median_activity.

    Analytical Assumption:
    The 14-hour operating window (08:00 to 21:59 local IST) is an analytical modeling assumption
    for retail traffic evaluation, rather than an empirically validated store operating schedule.
*/

WITH memberships AS (
    SELECT
        ping_id,
        store_id,
        store_name,
        hour_of_day
    FROM {{ ref('stg_store_catchment_memberships') }}
    WHERE hour_of_day BETWEEN 8 AND 21
),

stores AS (
    SELECT DISTINCT
        store_id,
        store_name
    FROM {{ ref('stg_store_catchment_memberships') }}
),

operating_hours AS (
    SELECT 8 AS hour_of_day UNION ALL
    SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL
    SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL
    SELECT 15 UNION ALL SELECT 16 UNION ALL SELECT 17 UNION ALL
    SELECT 18 UNION ALL SELECT 19 UNION ALL SELECT 20 UNION ALL
    SELECT 21
),

-- Grid of 4 stores x 14 hours = 56 rows
grid AS (
    SELECT
        s.store_id,
        s.store_name,
        h.hour_of_day
    FROM stores s
    CROSS JOIN operating_hours h
),

-- Hourly activity per store (0 filled)
hourly_activity AS (
    SELECT
        g.store_id,
        g.store_name,
        g.hour_of_day,
        COUNT(m.ping_id) AS hourly_pings
    FROM grid g
    LEFT JOIN memberships m
        ON g.store_id = m.store_id
        AND g.hour_of_day = m.hour_of_day
    GROUP BY
        g.store_id,
        g.store_name,
        g.hour_of_day
),

-- Store-level summary stats (median, peak, total volume)
store_stats AS (
    SELECT
        store_id,
        store_name,
        SUM(hourly_pings) AS operating_volume,
        MAX(hourly_pings) AS peak_hour_volume,
        ROUND(MEDIAN(hourly_pings), 2) AS median_operating_hourly_volume
    FROM hourly_activity
    GROUP BY
        store_id,
        store_name
),

-- Deterministic earliest peak hour tie-breaking: MIN(hour_of_day) where hourly_pings = peak_hour_volume
peak_hours AS (
    SELECT
        ha.store_id,
        MIN(ha.hour_of_day) AS peak_hour
    FROM hourly_activity ha
    INNER JOIN store_stats ss
        ON ha.store_id = ss.store_id
        AND ha.hourly_pings = ss.peak_hour_volume
    GROUP BY ha.store_id
),

-- Low activity hours count: hours where hourly_pings < 0.50 * median_hourly_pings
low_activity_counts AS (
    SELECT
        ha.store_id,
        COUNT(CASE WHEN ha.hourly_pings < (0.50 * ss.median_operating_hourly_volume) THEN 1 END) AS low_activity_hours_count
    FROM hourly_activity ha
    INNER JOIN store_stats ss
        ON ha.store_id = ss.store_id
    GROUP BY ha.store_id
)

SELECT
    ss.store_id,
    ss.store_name,
    ss.operating_volume,
    LPAD(CAST(ph.peak_hour AS VARCHAR), 2, '0') || ':00' AS peak_hour_window,
    ss.peak_hour_volume,
    ss.median_operating_hourly_volume::NUMBER(8,2) AS median_operating_hourly_volume,
    CASE
        WHEN ss.median_operating_hourly_volume = 0 THEN TRUE
        ELSE FALSE
    END AS has_zero_median_activity,
    CASE
        WHEN ss.median_operating_hourly_volume = 0 THEN NULL
        ELSE ROUND(ss.peak_hour_volume::FLOAT / ss.median_operating_hourly_volume, 2)::NUMBER(8,2)
    END AS peak_hour_index,
    ROUND(0.50 * ss.median_operating_hourly_volume, 2)::NUMBER(8,2) AS low_activity_threshold,
    lac.low_activity_hours_count,
    ROUND(lac.low_activity_hours_count::FLOAT / 14.0, 4)::NUMBER(6,4) AS low_activity_hour_share
FROM store_stats ss
INNER JOIN peak_hours ph
    ON ss.store_id = ph.store_id
INNER JOIN low_activity_counts lac
    ON ss.store_id = lac.store_id
ORDER BY
    ss.store_id
