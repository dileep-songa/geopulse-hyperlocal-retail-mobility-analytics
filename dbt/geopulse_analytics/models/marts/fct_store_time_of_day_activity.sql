WITH memberships AS (
    SELECT * FROM {{ ref('stg_store_catchment_memberships') }}
),

time_of_day_activity AS (
    SELECT
        store_id,
        store_name,
        time_of_day,
        COUNT(DISTINCT device_id) AS distinct_device_count,
        COUNT(ping_id) AS ping_count
    FROM memberships
    GROUP BY
        store_id,
        store_name,
        time_of_day
)

SELECT *
FROM time_of_day_activity
ORDER BY
    store_id,
    time_of_day
