    -- 1. Observation Identifier: RFC 4122 UUIDv4 string
    ping_id VARCHAR(36) NOT NULL
        COMMENT 'Globally unique RFC 4122 UUIDv4 identifier for the discrete GPS observation ping.',

    -- 2. Synthetic Device Identifier: RFC 4122 UUIDv4 string
    device_id VARCHAR(36) NOT NULL
        COMMENT 'Pseudonymous RFC 4122 UUIDv4 identifier of the simulated agent; persistent across trajectory steps.',

    -- 3. Observation Timestamp: Timezone-aware UTC timestamp
    event_timestamp TIMESTAMP_TZ NOT NULL
        COMMENT 'Observation timestamp in timezone-aware UTC. Converted from ISO-8601 UTC string (YYYY-MM-DDTHH:MM:SS[.fff]Z).',

    -- 4. Geodetic Latitude: Decimal degrees (WGS 84 / EPSG:4326)
    latitude NUMBER(9, 6) NOT NULL
        COMMENT 'Geodetic latitude in decimal degrees (WGS 84). Six decimal places provide approximately 0.11 m coordinate resolution near the study area; this is not actual positional accuracy. Valid range: [12.965000, 12.990000] N.',

    -- 5. Geodetic Longitude: Decimal degrees (WGS 84 / EPSG:4326)
    longitude NUMBER(9, 6) NOT NULL
        COMMENT 'Geodetic longitude in decimal degrees (WGS 84). Six decimal places provide approximately 0.11 m coordinate resolution near the study area; this is not actual positional accuracy. Valid range: [77.625000, 77.655000] E.',

    -- 6. Horizontal Accuracy Estimate: Simulated uncertainty field in meters
    accuracy_meters NUMBER(4, 1) NOT NULL
        COMMENT 'Simulated horizontal accuracy estimate in meters. Valid range: [2.0, 35.0] m; not a measured accuracy guarantee.',

    -- 7. Kinematic Movement Mode: Behavioral state enum
    movement_mode VARCHAR(30) NOT NULL
        COMMENT 'Kinematic movement mode at observation time: pedestrian_walking, stationary_dwell, or vehicular_transit.',

    -- 8. Data Provenance Tag: Verification constant
    data_provenance VARCHAR(50) NOT NULL
        COMMENT 'Constant provenance metadata tag confirming synthetic generation origin: geopulse_synthetic_v1.',

    -- ------------------------------------------------------------------------
    -- Warehouse Ingestion Audit Metadata (Standard Data Engineering Practice)
    -- These auxiliary columns track warehouse loading lineage without altering
    -- the 8 core measurement fields.
    -- ------------------------------------------------------------------------
    _ingested_at TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
        COMMENT 'Warehouse ingestion audit timestamp recorded when the record was loaded into Snowflake.',

    _staged_file_name VARCHAR(255)
        COMMENT 'Source filename in Snowflake stage from which this record was loaded.',

    _staged_file_row_number NUMBER(10, 0)
        COMMENT 'Row number of this record within the staged source CSV file.',

    -- ------------------------------------------------------------------------
    -- Table Constraints Documentation
    -- ------------------------------------------------------------------------
    -- IMPORTANT ARCHITECTURAL NOTE ON SNOWFLAKE CONSTRAINT ENFORCEMENT:
    -- On standard Snowflake tables, PRIMARY KEY and UNIQUE constraints are
    -- informational and are not enforced by the database engine during loading
    -- or DML. NOT NULL constraints are enforced.
    -- Uniqueness of ping_id is therefore checked upstream by GeoPulse Python
    -- validation (src/data_quality.py) and the synthetic data generator.
    CONSTRAINT pk_raw_mobility_pings PRIMARY KEY (ping_id)
)
COMMENT = 'Raw landing table storing canonical 8-column synthetic GPS mobility observations for the Indiranagar study area.';

-- ============================================================================
-- Supporting Ingestion Infrastructure (Prepared DDL Templates)
-- The following objects prepare the local CSV ingestion pattern for a future
-- deployment. Creating these objects does not upload or load any CSV records.
-- ============================================================================

-- CSV File Format Definition
CREATE FILE FORMAT IF NOT EXISTS GEOPULSE_DB.RAW.CSV_MOBILITY_PINGS_FORMAT
    TYPE = CSV
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE
    ESCAPE = NONE
    ESCAPE_UNENCLOSED_FIELD = NONE
    TRIM_SPACE = FALSE
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
    VALIDATE_UTF8 = TRUE
    DATE_FORMAT = 'AUTO'
    TIMESTAMP_FORMAT = 'AUTO'
    COMMENT = 'CSV file format for the GeoPulse canonical 8-column synthetic mobility CSV schema.';

-- Internal Staging Area Definition
CREATE STAGE IF NOT EXISTS GEOPULSE_DB.RAW.MOBILITY_RAW_STAGE
    FILE_FORMAT = GEOPULSE_DB.RAW.CSV_MOBILITY_PINGS_FORMAT
    COMMENT = 'Internal stage for landing raw synthetic mobility CSV files prior to table ingestion.';

-- ============================================================================
-- Example Ingestion Command (NOT EXECUTED)
-- This is a future template only. It is intentionally commented out.
-- Uploading a local CSV to the stage and executing COPY INTO are separate steps.
-- ============================================================================
/*
COPY INTO GEOPULSE_DB.RAW.RAW_MOBILITY_PINGS (
    ping_id,
    device_id,
    event_timestamp,
    latitude,
    longitude,
    accuracy_meters,
    movement_mode,
    data_provenance,
    _ingested_at,
    _staged_file_name,
    _staged_file_row_number
)
FROM (
    SELECT
        $1::VARCHAR(36),           -- ping_id (UUIDv4)
        $2::VARCHAR(36),           -- device_id (UUIDv4)
        $3::TIMESTAMP_TZ,          -- event_timestamp (ISO-8601 UTC)
        $4::NUMBER(9, 6),          -- latitude
        $5::NUMBER(9, 6),          -- longitude
        $6::NUMBER(4, 1),          -- accuracy_meters
        $7::VARCHAR(30),           -- movement_mode
        $8::VARCHAR(50),           -- data_provenance
        CURRENT_TIMESTAMP(),       -- _ingested_at
        METADATA$FILENAME,         -- _staged_file_name
        METADATA$FILE_ROW_NUMBER   -- _staged_file_row_number
    FROM @GEOPULSE_DB.RAW.MOBILITY_RAW_STAGE/synthetic_mobility_pings.csv
)
FILE_FORMAT = (FORMAT_NAME = 'GEOPULSE_DB.RAW.CSV_MOBILITY_PINGS_FORMAT')
ON_ERROR = 'ABORT_STATEMENT';
*/
