# GeoPulse: Technical Architecture and System Overview

This document provides the technical design, architectural patterns, data modeling specifications, and implementation roadmap for **GeoPulse: Hyper-Local Retail Mobility Analytics**.

---

## 1. System Architecture and Component Overview

The GeoPulse platform processes synthetic mobility telemetry through a modern, decoupled data engineering stack:

```mermaid
flowchart TD
    subgraph S1 ["1. Simulation Layer"]
        SG["Synthetic Mobility Generator<br/>(Python / NetworkX / Shapely)"]
        RD["Raw Parquet Telemetry<br/>(WGS 84 Coordinates)"]
        SG --> RD
    end

    subgraph S2 ["2. Distributed Spatial Processing Layer"]
        RD --> SP["Apache Spark & Sedona Engine"]
        POI["Retail POI Polygons<br/>(GeoJSON / Shapefiles)"] --> SP
        SP --> H3["Spatial Discretization & Indexing<br/>(H3 Resolution 8-10)"]
        SP --> ENR["Spatial Containment & Buffer Joins<br/>(ST_Contains / ST_DWithin)"]
    end

    subgraph S3 ["3. Cloud Storage & Warehousing"]
        ENR --> S3Stage["Snowflake External / Internal Stage"]
        S3Stage --> SF_RAW["Raw Ingestion Tables<br/>(GEOGRAPHY Types)"]
    end

    subgraph S4 ["4. Analytical Transformation Layer (dbt)"]
        SF_RAW --> DBT_STG["Staging Models (stg_*)"]
        DBT_STG --> DBT_INT["Intermediate Models (int_*)<br/>Dwell Time & Stay Point Detection"]
        DBT_INT --> DBT_MART["Marts (fct_* / dim_*)<br/>Footfall, Catchment & Overlap"]
    end

    subgraph S5 ["5. Presentation & Visual Analytics"]
        DBT_MART --> API["Analytical Serving Interface"]
        API --> KEP["Kepler.gl / React Dashboard<br/>(3D Heatmaps & Overlap Polygons)"]
    end

    subgraph S6 ["6. Orchestration & Monitoring"]
        AF["Apache Airflow DAGs"]
        AF -.-> SG
        AF -.-> SP
        AF -.-> SF_RAW
        AF -.-> DBT_STG
    end
```

---

## 2. Component Technical Deep-Dives

### 2.1 Synthetic Data Generation
The synthetic mobility generation engine is implemented in Python and designed to produce deterministic, realistic urban movement trajectories without real-world telemetry dependencies:

- **Movement Dynamics**:
  - Simulates agent origins and destinations across commercial clusters using urban road network graphs (via NetworkX / OSMnx topologies).
  - Employs routing algorithms (Dijkstra / A*) with speed profiles representing pedestrian walking (3–5 km/h) and vehicular transit (20–50 km/h).
  - Implements temporal diurnal curves reflecting morning commute, lunchtime retail peaks, evening shopping surges, and nighttime lulls.
- **Stay-Point & Dwell Simulation**:
  - Implements Poisson-distributed dwell durations within retail polygons to simulate shopping, browsing, and dining activities.
- **Sensor Noise Injection**:
  - Adds Gaussian GPS measurement noise ($\sigma \approx 5\text{--}15\text{ meters}$) and simulated multipath canyoning in dense urban environments to mirror real-world mobile device sensor inaccuracies.
- **Schema Contract**:
  - Emits partitioned Apache Parquet files adhering to strict typing:
    - `device_id` (`string` / UUID format)
    - `timestamp` (`timestamp[us]` / UTC)
    - `latitude` (`double` / degrees WGS 84)
    - `longitude` (`double` / degrees WGS 84)
    - `altitude` (`float` / meters above sea level)
    - `horizontal_accuracy` (`float` / estimated radius in meters)
    - `speed` (`float` / meters per second)
    - `bearing` (`float` / degrees 0–360)

### 2.2 PySpark and Apache Sedona Spatial Processing
Large-scale spatial filtering and geometric joins cannot scale efficiently on single-node Python processes. GeoPulse leverages PySpark accelerated by Apache Sedona:

- **Distributed Spatial Partitioning**:
  - Utilizes KDB-Tree and R-Tree spatial partitioners to divide geographical bounding boxes across Spark executors while maintaining spatial locality.
- **Geospatial SQL Acceleration**:
  - Evaluates spatial predicates (`ST_Contains`, `ST_Within`, `ST_DistanceSphere`, `ST_Buffer`) using optimized C++/Java spatial index bindings.
- **Hexagonal Spatial Indexing (H3)**:
  - Bins continuous coordinate pairs into discrete hexagonal cells (Uber H3 Index at resolution 8 [~460m] for corridor overviews, resolution 9 [~174m] for block level, and resolution 10 [~65m] for store-front proximity).
- **Trajectory Aggregation & Map Matching**:
  - Groups sequential breadcrumbs by `device_id` and temporal windows to reconstruct movement vectors and identify trip endpoints.

### 2.3 Snowflake Geospatial Storage
Snowflake serves as the centralized analytical data warehouse:

- **Native Geospatial Datatypes**:
  - Uses Snowflake's native `GEOGRAPHY` data type, modeling features on the WGS 84 ellipsoidal surface and enabling spherical distance calculations without local projection distortion.
  - Selectively uses `GEOMETRY` for planar cadastral boundary operations.
- **Storage & Clustering Strategy**:
  - Organizes raw telemetry tables clustered by `DATE(timestamp)` and `H3_CELL_ID` to optimize micro-partition pruning for spatial-temporal range queries.
- **Ingestion Mechanisms**:
  - Parquet staging via internal/external stages using vectorized `COPY INTO` commands with automated schema validation.

### 2.4 dbt Transformations
The analytical modeling layer is authored in dbt (data build tool), structured around a medallion architecture:

- **Staging (`stg_`)**:
  - Standardizes raw column types, applies UTC normalization, and validates coordinate boundaries.
- **Intermediate (`int_`)**:
  - **Dwell Time Detection**: Groups consecutive observation points within a spatial radius ($\Delta d \le 30\text{m}$) over a minimum time window ($\Delta t \ge 5\text{ min}$) to classify "dwell visits" vs. "drive-by pass-throughs".
  - **Catchment Polygon Generation**: Constructs convex/concave hulls (`ST_CONVEXHULL`) representing the observed spatial distribution of visiting devices.
- **Marts (`fct_` & `dim_`)**:
  - `dim_retail_stores`: Store profile, brand, commercial zone, and store boundary polygon.
  - `fct_hourly_footfall`: Aggregated visit counts, unique device counts, and average dwell times segmented by hour and retail brand.
  - `fct_catchment_overlap`: Pairwise store intersection areas ($\text{Area}(C_A \cap C_B)$) and device cannibalization percentages.
- **Automated Testing**:
  - Enforces schema assertions, non-null guarantees, referential integrity, and custom spatial tests (e.g., verifying latitude $[-90, 90]$ and longitude $[-180, 180]$).

### 2.5 React and Kepler.gl Visualization
The presentation tier provides an interactive, exploratory web interface:

- **Kepler.gl Integration**:
  - Leverages Deck.gl and WebGL to render tens of thousands of spatial points, trajectories, and polygons smoothly at 60 FPS in the browser.
- **Core Visual Layers**:
  - **Hexbin Heatmaps**: Aggregated foot traffic intensity across H3 hexagonal cells.
  - **Trip Playback**: Temporal animation of synthetic device journeys over commercial corridors.
  - **Catchment Overlap Visualizer**: Dynamic alpha-blended polygon overlays highlighting retail cannibalization corridors.
- **User Controls**:
  - Time-range scrubbing, day-of-week filters, brand isolation selectors, and dwell-duration sliders.

### 2.6 Apache Airflow Orchestration
Apache Airflow automates the end-to-end data lifecycle:

- **Pipeline Workflows (DAGs)**:
  - `geopulse_daily_simulation`: Triggers the synthetic telemetry generator for the target date partition.
  - `geopulse_sedona_spatial_etl`: Submits the PySpark/Sedona spatial indexing and boundary-join job.
  - `geopulse_snowflake_stage_and_load`: Executes automated COPY INTO commands to ingest staged Parquet files.
  - `geopulse_dbt_transform_and_test`: Runs dbt models and executes automated test suites.
- **Resilience & Governance**:
  - Configured with retries, exponential backoff, Slack/email alert webhooks, and strict execution SLAs.

---

## 3. Proposed Data Entities and Schemas

```mermaid
erDiagram
    SYNTHETIC_DEVICE ||--o{ MOBILITY_OBSERVATION : emits
    RETAIL_STORE ||--o{ STORE_CATCHMENT : defines
    RETAIL_STORE ||--o{ STORE_VISIT : hosts
    MOBILITY_OBSERVATION }o--o| STORE_VISIT : aggregates_to
    STORE_CATCHMENT ||--o{ CATCHMENT_OVERLAP : intersects

    SYNTHETIC_DEVICE {
        string device_id PK
        string device_type
        string home_h3_cell
        string work_h3_cell
    }

    MOBILITY_OBSERVATION {
        string observation_id PK
        string device_id FK
        timestamp observation_time
        double latitude
        double longitude
        geography coordinates_geo
        string h3_res9
        float speed_mps
        float horizontal_accuracy_m
    }

    RETAIL_STORE {
        string store_id PK
        string brand_name
        string store_category
        geography location_point
        geography footprint_polygon
        float gross_leasable_area_sqm
    }

    STORE_VISIT {
        string visit_id PK
        string store_id FK
        string device_id FK
        timestamp visit_start_time
        timestamp visit_end_time
        int dwell_duration_seconds
        boolean is_dwell_verified
    }

    STORE_CATCHMENT {
        string catchment_id PK
        string store_id FK
        string time_period
        geography catchment_polygon
        float area_sqkm
        int total_unique_visitors
    }

    CATCHMENT_OVERLAP {
        string overlap_id PK
        string primary_store_id FK
        string secondary_store_id FK
        geography overlap_polygon
        float overlap_area_sqkm
        float primary_overlap_pct
        float cannibalization_index
    }
```

---

## 4. Implementation Status: Planned vs. Implemented Components

To maintain transparent engineering governance, all platform components are classified below by operational status:

| Architectural Component | Subsystem / Asset | Status | Notes |
| :--- | :--- | :--- | :--- |
| **Repository Foundation** | Git configuration, `.gitignore`, `.editorconfig` | **Implemented** | Universal standards, clean git tracking setup. |
| **Governance & Documentation** | `PROJECT_CHARTER.md`, `TECHNICAL_OVERVIEW.md`, `README.md` | **Implemented** | Complete scope, data contracts, and architectural specs. |
| **Directory Structure** | `data/` (`raw`, `processed`, `reference`), `src/`, `sql/`, `tests/`, `reports/` | **Implemented** | Directories created and preserved via `.gitkeep`. |
| **Synthetic Simulation Engine** | Trajectory generator, network routing, kinematics, noise injection | *Planned (Week 1)* | Code implementation scheduled for Week 1 Day 1. |
| **Distributed Spatial Engine** | PySpark / Sedona spatial joins, H3 binning jobs | *Planned (Week 2)* | Cluster configuration and Spark jobs to be authored in `src/`. |
| **Snowflake Warehouse Layer** | Stage DDLs, raw/staging table definitions, clustering keys | *Planned (Week 3)* | SQL DDL scripts to be authored in `sql/`. |
| **dbt Transformation Models** | Staging, intermediate dwell detection, gold footfall & overlap marts | *Planned (Week 4)* | dbt project initialization and model authoring. |
| **Kepler.gl / React Frontend** | Interactive web application, layer controls, trajectory playback | *Planned (Week 5)* | Web application development. |
| **Airflow Orchestration** | Production DAGs, sensor operators, SLA monitors | *Planned (Week 6)* | DAG definitions and scheduling infrastructure. |

---

## 5. Security, Credential Management & Quality Assurance

- **Zero Hardcoded Secrets**: All cloud infrastructure endpoints (Snowflake account identifiers, credentials) will be passed exclusively through environment variables and local `.env` files (enforced by `.gitignore`).
- **Data Quality Gates**: Every transformation pipeline incorporates assertions verifying geometry validity (`ST_ISVALID`), non-null spatial coordinates, and referential integrity between visits and retail stores.
- **Deterministic Testing**: Unit and integration test suites in `tests/` will use fixed random seeds and mock reference geometries to ensure rapid, reproducible CI/CD execution without external cloud dependencies.
