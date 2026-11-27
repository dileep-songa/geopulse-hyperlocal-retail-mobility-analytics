{{
    config(
        materialized='view'
    )
}}

/*
    Model: stg_mobility_pings
    Layer: Staging
    Grain: ping_id
    Expected Rows: 10,000 rows

    Purpose:
    Canonical staging model for all raw synthetic GPS mobility observations.
    Preserves the complete 10,000-ping observation universe across all simulated devices,
    including points outside all candidate store catchments:
    - Casts timestamps and converts to Asia/Kolkata (IST, UTC+05:30) local time.
    - Extracts calendar_date, hour_of_day, and time_of_day diurnal classification.
    - Preserves exact 6-decimal-place latitude and longitude coordinates.
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_mobility_pings') }}
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
        latitude::NUMBER(9, 6) AS latitude,
        longitude::NUMBER(9, 6) AS longitude,
        accuracy_meters::NUMBER(4, 1) AS accuracy_meters,
        movement_mode::VARCHAR(30) AS movement_mode,
        data_provenance::VARCHAR(50) AS data_provenance,
        _ingested_at,
        _staged_file_name,
        _staged_file_row_number
    FROM source
)

SELECT * FROM staged
