-- ============================================================================
-- GeoPulse: Hyper-Local Retail Mobility Analytics
-- SQL Deliverable: 002_create_spatial_mobility_table.sql
-- Phase: Week 1 Day 4 — Spatial Data Engineering Preparation
-- Target Platform: Snowflake Data Cloud
-- Status: Prepared DDL & Transformation Script (Offline Development / Local-First)
-- Coordinate Reference System: WGS 84 (EPSG:4326)
-- Spatial Constructor: ST_MAKEPOINT(longitude, latitude) [Longitude First, Latitude Second]
-- ============================================================================

-- Schema Setup for Spatially Enriched Tables
CREATE SCHEMA IF NOT EXISTS GEOPULSE_DB.STAGING
    COMMENT = 'Staging and spatial enrichment schema for clean mobility telemetry';

USE SCHEMA GEOPULSE_DB.STAGING;

-- ============================================================================
-- Table: STG_SPATIAL_MOBILITY_PINGS
-- Description: Spatially enriched mobility table combining canonical tabular
--              attributes with a native Snowflake GEOGRAPHY point object.
--
-- GEODETIC / SPATIAL DESIGN SPECIFICATIONS:
-- 1. Coordinate Reference System (CRS):
--    - Standard: WGS 84 (World Geodetic System 1984), EPSG code 4326.
--    - Snowflake GEOGRAPHY type represents features on the WGS 84 ellipsoid
--      (specifically spherical/ellipsoidal geodetic coordinates).
--
-- 2. Spatial Construction Order Convention (CRITICAL):
--    - ST_MAKEPOINT takes parameters in order: (longitude, latitude).
--    - GeoJSON, WKT (Well-Known Text), and OpenGIS standards require X (longitude)
--      first, followed by Y (latitude) second.
--    - Formula: ST_MAKEPOINT(longitude, latitude)
--
-- 3. Raw Tabular Coordinates vs. Spatially Enriched GEOGRAPHY:
--    - Raw Coordinates (latitude, longitude): Stored as exact NUMBER(9,6) scalars.
--      They provide precision guarantees (exact 6 decimal digits) and human readability
--      for standard relational filters. However, scalar numbers lack spatial intelligence,
--      cannot leverage spatial indexes, and require complex mathematical user-defined
--      functions for distance or polygon containment calculations.
--    - Enriched GEOGRAPHY (coordinates_geo): A native Snowflake binary geospatial
--      data type. GEOGRAPHY natively supports spatial functions:
--      * ST_DISTANCE: Great-circle / ellipsoidal geodesic distance in meters.
--      * ST_CONTAINS: Fast evaluation of retail store catchment polygon containment.
--      * ST_DWITHIN: Radial proximity queries for storefront foot-traffic detection.
--      * Micro-partition spatial clustering and geospatial indexing acceleration.
--
-- 4. Inclusive Study Area Bounding-Box Contract:
--    - Enforces inclusive boundary limits:
--      Longitude: [77.625000, 77.655000] E (inclusive)
--      Latitude:  [12.965000, 12.990000] N (inclusive)
-- ============================================================================
CREATE OR REPLACE TABLE GEOPULSE_DB.STAGING.STG_SPATIAL_MOBILITY_PINGS (
    -- 1. Unique Observation Identifier (RFC 4122 UUIDv4)
    ping_id VARCHAR(36) NOT NULL
        COMMENT 'Globally unique RFC 4122 UUIDv4 identifier for the discrete GPS observation ping.',

    -- 2. Synthetic Device Identifier (RFC 4122 UUIDv4)
    device_id VARCHAR(36) NOT NULL
        COMMENT 'Pseudonymous RFC 4122 UUIDv4 identifier of the simulated agent; persistent across trajectory steps.',

    -- 3. Observation Timestamp (Timezone-Aware UTC)
    event_timestamp TIMESTAMP_TZ NOT NULL
        COMMENT 'Observation timestamp in timezone-aware UTC.',

    -- 4. Scalar Geodetic Latitude (Exact 6-Decimal Storage Precision)
    latitude NUMBER(9, 6) NOT NULL
        COMMENT 'Geodetic latitude in decimal degrees (WGS 84). Preserves exact 6 decimal places (approx. 0.11m precision).',

    -- 5. Scalar Geodetic Longitude (Exact 6-Decimal Storage Precision)
    longitude NUMBER(9, 6) NOT NULL
        COMMENT 'Geodetic longitude in decimal degrees (WGS 84). Preserves exact 6 decimal places (approx. 0.11m precision).',

    -- 6. Spatially Enriched Point Geometry (Native Snowflake GEOGRAPHY)
    coordinates_geo GEOGRAPHY NOT NULL
        COMMENT 'Native Snowflake GEOGRAPHY point feature constructed via ST_MAKEPOINT(longitude, latitude) on WGS 84 (EPSG:4326).',

    -- 7. Positional Uncertainty Radius
    accuracy_meters NUMBER(4, 1) NOT NULL
        COMMENT 'Simulated horizontal positional uncertainty radius in meters (68% confidence interval). Valid range: [2.0, 35.0] m.',

    -- 8. Kinematic Movement Mode
    movement_mode VARCHAR(30) NOT NULL
        COMMENT 'Kinematic movement mode: pedestrian_walking, stationary_dwell, or vehicular_transit.',

    -- 9. Synthetic Provenance Tag
    data_provenance VARCHAR(50) NOT NULL
        COMMENT 'Constant provenance metadata tag confirming synthetic generation origin: geopulse_synthetic_v1.',

    -- 10. Spatial Transformation Timestamp
    _transformed_at TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
        COMMENT 'Timestamp recording when tabular coordinates were transformed into the native GEOGRAPHY point.',

    -- ------------------------------------------------------------------------
    -- Table Constraints Documentation
    -- ------------------------------------------------------------------------
    -- IMPORTANT: In Snowflake, PRIMARY KEY constraints are INFORMATIONAL ONLY.
    -- They provide metadata for query optimizer cost models (RELY/NORELY) but
    -- are not enforced during DML. Uniqueness is validated prior to warehouse loading.
    -- NOT NULL is strictly enforced.
    CONSTRAINT pk_spatial_mobility_pings PRIMARY KEY (ping_id)
)
CLUSTER BY (DATE(event_timestamp), movement_mode)
COMMENT = 'Spatially enriched mobility observation table featuring native WGS 84 GEOGRAPHY points clustered by date and movement mode.';

-- ============================================================================
-- Transformation DML: Raw Tabular to Spatially Enriched GEOGRAPHY
-- Demonstrates the exact transformation logic mapping RAW_MOBILITY_PINGS
-- into STG_SPATIAL_MOBILITY_PINGS while asserting boundary contracts.
-- ============================================================================

-- Idempotent Spatial Population Script (Prepared Template for Week 3 Execution)
/*
INSERT INTO GEOPULSE_DB.STAGING.STG_SPATIAL_MOBILITY_PINGS (
    ping_id,
    device_id,
    event_timestamp,
    latitude,
    longitude,
    coordinates_geo,
    accuracy_meters,
    movement_mode,
    data_provenance,
    _transformed_at
)
SELECT
    raw.ping_id,
    raw.device_id,
    raw.event_timestamp,
    raw.latitude,
    raw.longitude,
    -- CRITICAL SPATIAL CONSTRUCTOR:
    -- Parameter order: ST_MAKEPOINT(longitude, latitude)
    -- Generates a WGS 84 Point feature: POINT(lon lat)
    ST_MAKEPOINT(raw.longitude, raw.latitude) AS coordinates_geo,
    raw.accuracy_meters,
    raw.movement_mode,
    raw.data_provenance,
    CURRENT_TIMESTAMP() AS _transformed_at
FROM GEOPULSE_DB.RAW.RAW_MOBILITY_PINGS raw
WHERE
    -- 1. Inclusive Geographic Bounding-Box Enforcement
    raw.longitude BETWEEN 77.625000 AND 77.655000
    AND raw.latitude BETWEEN 12.965000 AND 12.990000

    -- 2. Accuracy Boundary Enforcement
    AND raw.accuracy_meters BETWEEN 2.0 AND 35.0

    -- 3. Movement Mode Categorical Constraint
    AND raw.movement_mode IN ('pedestrian_walking', 'stationary_dwell', 'vehicular_transit')

    -- 4. Approved Provenance Tag Constraint
    AND raw.data_provenance = 'geopulse_synthetic_v1';
*/

-- ============================================================================
-- Analytical Spatial Verification Queries (Prepared Templates)
-- The following analytical queries illustrate downstream usage of the GEOGRAPHY
-- column for retail corridor footfall and proximity analytics.
-- ============================================================================

-- 1. Study Area Centroid Proximity Query:
-- Calculates geodesic distance in meters from each observation to the
-- Indiranagar commercial corridor centroid (77.640000° E, 12.977500° N).
/*
SELECT
    ping_id,
    device_id,
    movement_mode,
    event_timestamp,
    -- ST_DISTANCE computes ellipsoidal geodesic distance in meters
    ROUND(ST_DISTANCE(
        coordinates_geo,
        ST_MAKEPOINT(77.640000, 12.977500)
    ), 2) AS distance_to_centroid_meters
FROM GEOPULSE_DB.STAGING.STG_SPATIAL_MOBILITY_PINGS
ORDER BY distance_to_centroid_meters ASC
LIMIT 10;
*/

-- 2. Spatial Bounding Envelope Containment Verification:
-- Constructs the analytical Indiranagar study box polygon and verifies
-- that 100% of points fall within the inclusive bounding envelope.
/*
SELECT
    COUNT(*) AS total_points,
    COUNT_IF(ST_CONTAINS(
        ST_MAKELINE(
            ST_MAKEPOINT(77.625000, 12.965000),
            ST_MAKEPOINT(77.655000, 12.965000),
            ST_MAKEPOINT(77.655000, 12.990000),
            ST_MAKEPOINT(77.625000, 12.990000),
            ST_MAKEPOINT(77.625000, 12.965000)
        ),
        coordinates_geo
    )) AS points_inside_bounding_polygon
FROM GEOPULSE_DB.STAGING.STG_SPATIAL_MOBILITY_PINGS;
*/
