WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_store_catchment_memberships') }}
),

staged AS (
    SELECT
        ping_id::VARCHAR(36) AS ping_id,
        device_id::VARCHAR(36) AS device_id,
        event_timestamp::TIMESTAMP_TZ AS event_timestamp,
        CONVERT_TIMEZONE('Asia/Kolkata', event_timestamp)::TIMESTAMP_NTZ AS event_timestamp_ist,
        TO_DATE(CONVERT_TIMEZONE('Asia/Kolkata', event_timestamp)) AS calendar_date,
        DATE_PART('hour', CONVERT_TIMEZONE('Asia/Kolkata', event_timestamp))::INT AS hour_of_day,
        CASE
            WHEN DATE_PART('hour', CONVERT_TIMEZONE('Asia/Kolkata', event_timestamp)) BETWEEN 6 AND 11 THEN 'morning'
            WHEN DATE_PART('hour', CONVERT_TIMEZONE('Asia/Kolkata', event_timestamp)) BETWEEN 12 AND 16 THEN 'afternoon'
            WHEN DATE_PART('hour', CONVERT_TIMEZONE('Asia/Kolkata', event_timestamp)) BETWEEN 17 AND 21 THEN 'evening'
            ELSE 'night'
        END AS time_of_day,
        store_id::VARCHAR(10) AS store_id,
        store_name::VARCHAR(100) AS store_name,
        distance_meters::NUMBER(7, 2) AS distance_meters,
        movement_mode::VARCHAR(30) AS movement_mode,
        data_provenance::VARCHAR(50) AS data_provenance,
        _ingested_at,
        _staged_file_name,
        _staged_file_row_number
    FROM source
)

SELECT * FROM staged
