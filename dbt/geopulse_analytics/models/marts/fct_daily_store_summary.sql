WITH memberships AS (
    SELECT * FROM {{ ref('stg_store_catchment_memberships') }}
),

daily_summary AS (
    SELECT
        store_id,
        store_name,
        calendar_date,
        COUNT(DISTINCT device_id) AS distinct_devices,
        COUNT(ping_id) AS ping_count,
        COUNT(DISTINCT hour_of_day) AS active_hours_count,
        ROUND(COUNT(ping_id)::FLOAT / NULLIF(COUNT(DISTINCT device_id), 0), 2) AS pings_per_device
    FROM memberships
    GROUP BY
        store_id,
        store_name,
        calendar_date
)

SELECT *
FROM daily_summary
ORDER BY
    store_id,
    calendar_date
