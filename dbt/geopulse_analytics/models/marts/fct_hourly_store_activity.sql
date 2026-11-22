WITH memberships AS (
    SELECT * FROM {{ ref('stg_store_catchment_memberships') }}
),

hourly_activity AS (
    SELECT
        store_id,
        store_name,
        calendar_date,
        hour_of_day,
        COUNT(DISTINCT device_id) AS distinct_device_count,
        COUNT(ping_id) AS ping_count
    FROM memberships
    GROUP BY
        store_id,
        store_name,
        calendar_date,
        hour_of_day
)

SELECT *
FROM hourly_activity
ORDER BY
    store_id,
    calendar_date,
    hour_of_day
