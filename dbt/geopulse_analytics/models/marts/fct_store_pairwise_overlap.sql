{{
    config(
        materialized='table'
    )
}}

/*
    Model: fct_store_pairwise_overlap
    Layer: Mart
    Grain: (store_id_a, store_id_b) [where store_id_a < store_id_b]
    Expected Rows: 6 rows (for 4 stores)

    Purpose:
    Computes macro pairwise competition and spatial overlap metrics:
    - Geometric Overlap Ratio (GOR): Exact regular 32-gon intersection area / union area.
    - Device Jaccard Index (DJI): Shared devices observed across both catchments / union devices.
    - Device Overlap Coefficient (DOC): Szymkiewicz-Simpson symmetric overlap coefficient.
    - Pairwise Cross-Catchment Transition Rates (PCTR) at 15, 30, and 60 minutes.
    - Physical shared-lens ping counts (pings located inside both 32-gon catchments).
    - Summary hourly shared-device statistics across the 14 operating hours.
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

transitions AS (
    SELECT
        device_id,
        origin_store_id,
        destination_store_id,
        duration_minutes
    FROM {{ ref('int_store_pairwise_device_transitions') }}
),

-- Device sets per individual store
store_device_counts AS (
    SELECT
        store_id,
        COUNT(DISTINCT device_id) AS total_store_devices
    FROM memberships
    GROUP BY store_id
),

-- Static exact 32-gon geometric polygon intersection and union areas (EPSG:32643)
pair_geometries AS (
    SELECT 'STORE_A' AS store_id_a, 'STORE_B' AS store_id_b, 29518.22::NUMBER(12,2) AS lens_intersection_area_m2, 1531204.36::NUMBER(12,2) AS union_polygon_area_m2, 0.0193::NUMBER(6,4) AS geometric_overlap_ratio UNION ALL
    SELECT 'STORE_A', 'STORE_C', 0.00, 1560722.58, 0.0000 UNION ALL
    SELECT 'STORE_A', 'STORE_D', 0.00, 1560722.58, 0.0000 UNION ALL
    SELECT 'STORE_B', 'STORE_C', 16111.54, 1544611.04, 0.0104 UNION ALL
    SELECT 'STORE_B', 'STORE_D', 23003.02, 1537719.56, 0.0150 UNION ALL
    SELECT 'STORE_C', 'STORE_D', 0.00, 1560722.58, 0.0000
),

distinct_pairs AS (
    SELECT DISTINCT
        store_id_a,
        store_name_a,
        store_id_b,
        store_name_b
    FROM spatial_states
),

-- Distinct devices observed per store
distinct_store_devices AS (
    SELECT DISTINCT
        store_id,
        device_id
    FROM memberships
),

-- Cross-tabulate candidate pairs against devices active in store_id_a or store_id_b
pair_device_cross AS (
    SELECT
        dp.store_id_a,
        dp.store_id_b,
        d.device_id,
        MAX(CASE WHEN d.store_id = dp.store_id_a THEN 1 ELSE 0 END) AS in_a,
        MAX(CASE WHEN d.store_id = dp.store_id_b THEN 1 ELSE 0 END) AS in_b
    FROM distinct_pairs dp
    INNER JOIN distinct_store_devices d
        ON d.store_id IN (dp.store_id_a, dp.store_id_b)
    GROUP BY
        dp.store_id_a,
        dp.store_id_b,
        d.device_id
),

-- Cumulative device-level overlap across catchments
pairwise_devices AS (
    SELECT
        pdc.store_id_a,
        pdc.store_id_b,
        COUNT(CASE WHEN pdc.in_a = 1 AND pdc.in_b = 1 THEN 1 END) AS shared_unique_devices,
        COUNT(*) AS union_unique_devices,
        COALESCE(lens.total_shared_lens_pings, 0) AS total_shared_lens_pings
    FROM pair_device_cross pdc
    LEFT JOIN (
        SELECT
            store_id_a,
            store_id_b,
            COUNT(ping_id) AS total_shared_lens_pings
        FROM spatial_states
        WHERE pairwise_state = 'SHARED_PAIR'
        GROUP BY store_id_a, store_id_b
    ) lens ON pdc.store_id_a = lens.store_id_a AND pdc.store_id_b = lens.store_id_b
    GROUP BY
        pdc.store_id_a,
        pdc.store_id_b,
        lens.total_shared_lens_pings
),

-- Directional transitions: A -> B and B -> A
transition_counts_ab AS (
    SELECT
        origin_store_id AS store_id_a,
        destination_store_id AS store_id_b,
        COUNT(DISTINCT CASE WHEN duration_minutes <= 15.0 THEN device_id END) AS trans_devices_15m_a_to_b,
        COUNT(DISTINCT CASE WHEN duration_minutes <= 30.0 THEN device_id END) AS trans_devices_30m_a_to_b,
        COUNT(DISTINCT CASE WHEN duration_minutes <= 60.0 THEN device_id END) AS trans_devices_60m_a_to_b
    FROM transitions
    GROUP BY origin_store_id, destination_store_id
),

transition_counts_ba AS (
    SELECT
        destination_store_id AS store_id_a,
        origin_store_id AS store_id_b,
        COUNT(DISTINCT CASE WHEN duration_minutes <= 15.0 THEN device_id END) AS trans_devices_15m_b_to_a,
        COUNT(DISTINCT CASE WHEN duration_minutes <= 30.0 THEN device_id END) AS trans_devices_30m_b_to_a,
        COUNT(DISTINCT CASE WHEN duration_minutes <= 60.0 THEN device_id END) AS trans_devices_60m_b_to_a
    FROM transitions
    GROUP BY destination_store_id, origin_store_id
),

-- Hourly shared devices across operating hours (08:00 - 21:59)
-- A device is shared in hour h if it has activity in store A AND store B in hour h
hourly_devs_a AS (
    SELECT DISTINCT store_id AS store_id_a, device_id, hour_of_day
    FROM memberships
    WHERE hour_of_day BETWEEN 8 AND 21
),

hourly_devs_b AS (
    SELECT DISTINCT store_id AS store_id_b, device_id, hour_of_day
    FROM memberships
    WHERE hour_of_day BETWEEN 8 AND 21
),

hourly_shared AS (
    SELECT
        dp.store_id_a,
        dp.store_id_b,
        hda.hour_of_day,
        COUNT(DISTINCT hda.device_id) AS shared_devices_in_hour
    FROM distinct_pairs dp
    INNER JOIN hourly_devs_a hda
        ON dp.store_id_a = hda.store_id_a
    INNER JOIN hourly_devs_b hdb
        ON dp.store_id_b = hdb.store_id_b
        AND hda.device_id = hdb.device_id
        AND hda.hour_of_day = hdb.hour_of_day
    GROUP BY dp.store_id_a, dp.store_id_b, hda.hour_of_day
),

hourly_shared_summary AS (
    SELECT
        store_id_a,
        store_id_b,
        ROUND(AVG(shared_devices_in_hour), 2) AS mean_hourly_shared_devices,
        MAX(shared_devices_in_hour) AS peak_hourly_shared_devices
    FROM hourly_shared
    GROUP BY store_id_a, store_id_b
)

SELECT
    dp.store_id_a,
    dp.store_name_a,
    dp.store_id_b,
    dp.store_name_b,
    pg.lens_intersection_area_m2,
    pg.union_polygon_area_m2,
    pg.geometric_overlap_ratio,
    sda.total_store_devices AS unique_devices_store_a,
    sdb.total_store_devices AS unique_devices_store_b,
    pd.shared_unique_devices,
    pd.union_unique_devices,
    ROUND(pd.shared_unique_devices::FLOAT / NULLIF(pd.union_unique_devices, 0), 4)::NUMBER(6,4) AS device_jaccard_index,
    ROUND(pd.shared_unique_devices::FLOAT / NULLIF(LEAST(sda.total_store_devices, sdb.total_store_devices), 0), 4)::NUMBER(6,4) AS overlap_coefficient,
    pd.total_shared_lens_pings,
    COALESCE(hss.mean_hourly_shared_devices, 0.00)::NUMBER(6,2) AS mean_hourly_shared_devices,
    COALESCE(hss.peak_hourly_shared_devices, 0)::INT AS peak_hourly_shared_devices,
    -- PCTR directional rates (A -> B)
    ROUND(COALESCE(tc_ab.trans_devices_15m_a_to_b, 0)::FLOAT / NULLIF(sda.total_store_devices, 0), 4)::NUMBER(6,4) AS pctr_15m_a_to_b,
    ROUND(COALESCE(tc_ab.trans_devices_30m_a_to_b, 0)::FLOAT / NULLIF(sda.total_store_devices, 0), 4)::NUMBER(6,4) AS pctr_30m_a_to_b,
    ROUND(COALESCE(tc_ab.trans_devices_60m_a_to_b, 0)::FLOAT / NULLIF(sda.total_store_devices, 0), 4)::NUMBER(6,4) AS pctr_60m_a_to_b,
    -- PCTR directional rates (B -> A)
    ROUND(COALESCE(tc_ba.trans_devices_15m_b_to_a, 0)::FLOAT / NULLIF(sdb.total_store_devices, 0), 4)::NUMBER(6,4) AS pctr_15m_b_to_a,
    ROUND(COALESCE(tc_ba.trans_devices_30m_b_to_a, 0)::FLOAT / NULLIF(sdb.total_store_devices, 0), 4)::NUMBER(6,4) AS pctr_30m_b_to_a,
    ROUND(COALESCE(tc_ba.trans_devices_60m_b_to_a, 0)::FLOAT / NULLIF(sdb.total_store_devices, 0), 4)::NUMBER(6,4) AS pctr_60m_b_to_a
FROM distinct_pairs dp
INNER JOIN pair_geometries pg
    ON dp.store_id_a = pg.store_id_a AND dp.store_id_b = pg.store_id_b
INNER JOIN store_device_counts sda
    ON dp.store_id_a = sda.store_id
INNER JOIN store_device_counts sdb
    ON dp.store_id_b = sdb.store_id
INNER JOIN pairwise_devices pd
    ON dp.store_id_a = pd.store_id_a AND dp.store_id_b = pd.store_id_b
LEFT JOIN transition_counts_ab tc_ab
    ON dp.store_id_a = tc_ab.store_id_a AND dp.store_id_b = tc_ab.store_id_b
LEFT JOIN transition_counts_ba tc_ba
    ON dp.store_id_a = tc_ba.store_id_a AND dp.store_id_b = tc_ba.store_id_b
LEFT JOIN hourly_shared_summary hss
    ON dp.store_id_a = hss.store_id_a AND dp.store_id_b = hss.store_id_b
ORDER BY
    dp.store_id_a,
    dp.store_id_b
