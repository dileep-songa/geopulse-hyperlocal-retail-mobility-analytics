# GeoPulse — Week 2 Completion Report: Spatial Analytics & Retail Mobility Modeling

**Project**: GeoPulse — Hyper-Local Retail Mobility Analytics<br/>
**Document**: Week 2 Consolidated Completion & Milestone Closeout Report<br/>
**Phase**: Week 2 (Spatial Analytics, Catchment Modeling & Retail Mobility Analytics)<br/>
**Date**: November 2026<br/>
**Author / Engineering Lead**: Dileep Songa<br/>
**Repository**: `https://github.com/dileep-songa/geopulse-hyperlocal-retail-mobility-analytics`<br/>
**Target Milestone**: Week 2 Closeout — Local Spatial Computing Engine & Retail Mobility Analytics<br/>
**Overall Status**: **COMPLETE & VERIFIED (163 automated tests passed, zero failures and zero errors)**

---

## 1. Executive Summary

This report provides the formal engineering completion review and technical verification for **Week 2** of the **GeoPulse: Hyper-Local Retail Mobility Analytics** project. The primary objective of Week 2 was to implement an offline, local-first spatial data engineering and retail mobility analytics pipeline.

Week 2 comprised two core engineering milestones:
1. **Spatial Catchment Analysis**: Ingesting synthetic mobility telemetry, projecting geodetic coordinates (WGS 84 / `EPSG:4326`) into a planar metric coordinate system (UTM Zone 43N / `EPSG:32643`), generating 500-metre planar buffer polygons around hypothetical retail candidate locations, performing spatial point-in-polygon joins via Apache Sedona, computing Euclidean proximity distances in metres, and evaluating multi-store catchment overlaps.
2. **Retail Mobility Analytics**: Transforming matched catchment memberships into commercial retail intelligence datasets. This includes parsing timestamps into local Indian Standard Time (`Asia/Kolkata`, UTC+05:30), evaluating four diurnal activity windows (`morning`, `afternoon`, `evening`, `night`), aggregating hourly store–ping membership activity across 81 active store-hours, computing daily store activity and visit intensity metrics (`pings_per_device`), and calculating per-store exclusivity and multi-catchment overlap distributions.

The execution and verification workload was structured between two complementary environments:
- **Distributed Spark & Sedona Pipeline Execution**: The PySpark/Sedona end-to-end pipelines (`src.spatial_analysis` and `src.retail_mobility_analytics`) and isolated verification runs were executed in **WSL2 Ubuntu 26.04** with Python 3.12.16, OpenJDK 17.0.21, PySpark 3.5.9, and Apache Sedona 1.9.1.
- **Automated Unit & Integration Test Suite**: The complete 163-test regression suite was executed and verified in the **Windows Python 3.12.9** host environment (`.venv\Scripts\python.exe`), passing all 163 tests with zero failures and zero errors.

| Metric / Objective | Target Specification | Verified Value | Validation Status |
| :--- | :--- | :--- | :--- |
| **Input Mobility Dataset** | `10,000` observations (`500` devices) | `10,000` observations (`500` devices) | Ingested & validated |
| **Candidate Store Locations** | `4` hypothetical coffee shops | `4` candidate locations | Ingested & validated |
| **Source Coordinate System** | Geodetic WGS 84 (`EPSG:4326`) | WGS 84 ellipsoidal coordinates | Verified |
| **Planar Metric Analysis System** | UTM Zone 43N (`EPSG:32643`) | Re-projected planar coordinates | Verified |
| **Store Catchment Geometry** | 500-metre planar buffer polygon | 4 topologically valid polygons | `ST_IsValid = true` |
| **Buffer Catchment Area** | ~`780,361.29 m²` (32-gon approximation) | `780,361.29 m²` per store | Within 0.64% of circle |
| **Total Matched Memberships** | Spatial point-in-polygon join | `4,051` membership records | Exact match |
| **Unique Matched Observations** | Unique pings in $\ge 1$ catchment | `3,956` distinct pings | Exact match |
| **Unique Matched Devices** | Unique devices in $\ge 1$ catchment | `350` distinct devices | Exact match |
| **Pings in Exactly 1 Catchment** | Disjoint overlap category ($N_1$) | `3,861` pings (97.60%) | Exact match |
| **Pings in Exactly 2 Catchments** | Disjoint overlap category ($N_2$) | `95` pings (2.40%) | Exact match |
| **Pings in $\ge 3$ Catchments** | Disjoint overlap category ($N_{\ge 3}$) | `0` pings (0.00%) | Exact match |
| **Total Multi-Catchment Pings** | Overlap sum ($N_2 + N_{\ge 3}$) | `95` pings (2.40%) | Exact match |
| **Network Exclusive Devices** | Devices visiting exactly 1 store | `216` devices (61.71%) | Exact match |
| **Multi-Catchment Devices** | Devices visiting $\ge 2$ stores | `134` devices (38.29%) | Exact match |
| **Hourly Store Activity Mart** | Active store-hour aggregations | `81` data rows across 4 stores | Reconciled & verified |
| **Diurnal Time-of-Day Mart** | 4 diurnal windows per store | `16` data rows across 4 stores | Reconciled & verified |
| **Daily Store Summary Mart** | Store-day visit intensity metrics | `8` data rows (4 stores × 2 dates) | Reconciled & verified |
| **Store Overlap Summary Mart** | Persisted per-store exclusivity | `4` data rows (1 per store) | Reconciled & verified |
| **Automated Test Suite** | Full regression and spatial suite | `163 passed` (0 failures, 0 errors) | Verified in Windows Python 3.12 |
| **Isolated Execution Verification** | Verification output directories | 7 Spark CSV directories + `_SUCCESS` | Verified in WSL2 Spark session |
| **Baseline Output Preservation** | Baseline `data/processed/` | Pre-run and post-run SHA-256 match | Bit-for-bit match across 7 files |

---

## 2. Spatial & Retail Mobility Analytics Architecture

The Week 2 architecture bridges raw synthetic mobility telemetry with commercial retail intelligence through a staged, local-first computing pipeline.

```
┌─────────────────────────────────────────┐   ┌─────────────────────────────────────────┐
│ data/raw/synthetic_mobility_pings.csv   │   │ data/reference/synthetic_store_locations│
│            (10,000 pings)               │   │              (4 stores)                 │
└────────────────────┬────────────────────┘   └────────────────────┬────────────────────┘
                     │                                             │
                     ▼                                             ▼
        Schema & Range Validation                    Schema & Boundary Validation
        (validate_mobility_csv)                      (validate_store_records)
                     │                                             │
                     ▼                                             ▼
             ST_Point(lon, lat)                            ST_Point(lon, lat)
             SetSRID(..., 4326)                            SetSRID(..., 4326)
                     │                                             │
                     ▼                                             ▼
          ST_Transform(..., 32643)                      ST_Transform(..., 32643)
            (Planar Easting/Northing)                     (Planar Easting/Northing)
                     │                                             │
                     │                                             ▼
                     │                                 ST_Buffer(geom, 500.0 m)
                     │                                 (Planar buffer in EPSG:32643)
                     │                                 Area: ~780,361 m², Valid: true
                     │                                             │
                     └──────────────────────┬──────────────────────┘
                                            │
                                            ▼
                    Spatial Membership Join: ST_Intersects(ping, catchment)
                    Metric Distance: ST_Distance(ping, store) in metres
                                            │
                     ┌──────────────────────┴──────────────────────┐
                     ▼                                             ▼
        store_catchments (4 rows)                     store_catchment_memberships
        - Geometry WKT, Area, Radius                   (4,051 matched records)
                     │                                             │
                     └──────────────────────┬──────────────────────┘
                                            ▼
                                store_catchment_summary
                                (4 rows: pings, devices, multi-catchment)
                                            │
                                            ▼
                     ┌─────────────────────────────────────────────┐
                     │     src/retail_mobility_analytics.py        │
                     │  - Strict 8-field membership validation     │
                     │  - Duplicate (ping_id, store_id) rejection  │
                     │  - UTC -> Asia/Kolkata (IST) conversion     │
                     │  - 4 Diurnal windows (morning, aft, eve, nt)│
                     │  - Fail-fast network reconciliation         │
                     └──────────────────────┬──────────────────────┘
                                            │
         ┌───────────────────┬──────────────┴──────┬───────────────────┐
         ▼                   ▼                     ▼                   ▼
hourly_store_activity  store_time_of_day   daily_store_summary   store_overlap_summary
   (81 store-hours)      (16 store-diurnals)    (8 store-days)        (4 stores)
```

### Analytical Transformation Stages

1. **Input Ingestion & Validation (`src/spatial_analysis.py`)**:
   - `data/reference/synthetic_store_locations.csv` is read and validated using `validate_store_records()`, verifying that all coordinates lie strictly inside the Indiranagar study-area boundary ($[77.6250, 77.6550]^\circ\text{E}, [12.9650, 12.9900]^\circ\text{N}$) and carry correct metadata tags (`hypothetical_candidate`, `geopulse_synthetic_v1`).
   - `data/raw/synthetic_mobility_pings.csv` is validated using `validate_mobility_csv()`, checking for non-empty input, exact 8-column canonical schema conformance, finite coordinates, identifier uniqueness, allowed movement modes, and provenance tags.
2. **Coordinate Reference System Transformation**:
   - Geographic coordinates in WGS 84 (`EPSG:4326`) express positions on an ellipsoidal surface in angular degrees.
   - For metric distance and planar area calculations, coordinates are projected to Universal Transverse Mercator (UTM) Zone 43N (`EPSG:32643`), providing a conformal Cartesian coordinate plane measured in metres (Eastings and Northings).
3. **Planar Catchment Buffer Generation**:
   - A planar buffer with radius $r = 500.0\text{ m}$ is constructed around each projected candidate store point using `ST_Buffer`.
   - Apache Sedona/JTS approximates circular buffers using 8 segments per quadrant, creating a 32-sided regular polygon ($N = 32$). The theoretical area of this regular polygon is:
     $$A_{32} = \frac{1}{2} \cdot 32 \cdot r^2 \cdot \sin\left(\frac{2\pi}{32}\right) = 16 \cdot (500)^2 \cdot \sin\left(\frac{\pi}{16}\right) \approx 780,361.29\text{ m}^2$$
   - This polygon area differs from a theoretical circle ($\pi r^2 \approx 785,398.16\text{ m}^2$) by $0.64\%$, well within the pipeline's configured $2\%$ geometric tolerance.
   - Geometry validity is confirmed using `ST_IsValid(catchment_geom)`.
4. **Spatial Intersection & Distance Computation**:
   - Membership is evaluated using `ST_Intersects(m.ping_geom_proj, c.catchment_geom)`.
   - For every matched observation, Euclidean planar distance from the ping to the store centroid is calculated in metres: `ROUND(ST_Distance(m.ping_geom_proj, c.store_geom_proj), 2)`.
5. **Retail Mobility Transformations (`src/retail_mobility_analytics.py`)**:
   - **Local Time Conversion**: UTC timestamps are parsed and converted to `Asia/Kolkata` (Indian Standard Time, UTC+05:30), extracting `calendar_date` and `hour_of_day`.
   - **Diurnal Classification**: Hours are categorized into four retail dayparts:
     - Morning: `06:00`–`11:59` IST
     - Afternoon: `12:00`–`16:59` IST
     - Evening: `17:00`–`21:59` IST
     - Night: `22:00`–`05:59` IST
   - **Multi-Level Aggregations**: Generates hourly profiles, diurnal summaries, store-day visit intensity metrics (`pings_per_device`), and per-store exclusivity and multi-catchment overlap counts.
   - **Pre-Persistence Reconciliation**: Validates that all network-level totals reconcile before creating output directories.

---

## 3. Computing Environment & Dependency Setup

### Environment Specifications

The Week 2 development and verification stack uses a hybrid Linux/Windows environment:

| Component | WSL2 Ubuntu Environment | Windows Host Environment | Role in GeoPulse |
| :--- | :--- | :--- | :--- |
| **Operating System** | Ubuntu 26.04 LTS (WSL2) | Windows 11 Professional | Cross-platform runtime |
| **Python Runtime** | Python 3.12.16 | Python 3.12.9 | Script execution & testing |
| **Virtual Environment**| `/home/dileep/.venvs/geopulse-sedona` | `c:\project\...\.venv` | Isolated dependencies |
| **Java Runtime** | OpenJDK 17.0.21.0.3 (`/usr/lib/jvm`) | Eclipse Adoptium JDK 17.0.21.9 | JVM backend for Spark |
| **Apache Spark** | PySpark 3.5.9 | PySpark 3.5.9 | Distributed SQL & execution |
| **Apache Sedona** | Apache Sedona 1.9.1 | Apache Sedona 1.9.1 | Spatial SQL extensions |
| **Execution Master** | `local[2]` (2 CPU cores) | `local[2]` (2 CPU cores) | Local parallel execution |

### Shaded JAR Resolution

For this project, Apache Sedona's spatial SQL operations utilize two specific JARs:
1. `org.apache.sedona_sedona-spark-shaded-3.5_2.12-1.6.1.jar`: Bundles core Sedona spatial operators, the JTS topology suite (`org.locationtech.jts.geom`), and Kryo spatial serializers.
2. `org.datasyslab_geotools-wrapper-1.6.1-28.2.jar`: Required specifically as part of the CRS transformation setup in this project for `ST_Transform` between geodetic `EPSG:4326` and projected planar `EPSG:32643`. (While base planar geometries like `ST_Point` or `ST_Buffer` do not universally require GeoTools, CRS reprojection in Sedona depends on this wrapper to supply EPSG definitions and coordinate transformation mathematical adapters).

Classpath paths are dynamically located via `find_sedona_jars()` and injected into Spark's `spark.driver.extraClassPath` and `spark.executor.extraClassPath`.

---

## 4. Reference Data Specification

To evaluate retail mobility dynamics without real commercial addresses, Week 2 created four hypothetical candidate retail coffee locations (`data/reference/synthetic_store_locations.csv`) located within the Indiranagar analytical bounding box:

| Store Identifier | Store Name | Longitude (°E) | Latitude (°N) | Projected Easting (X) | Projected Northing (Y) | Location Type | Data Provenance |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`STORE_A`** | GeoPulse Coffee A | 77.632000 | 12.976000 | 785535.98 m | 1435955.05 m | `hypothetical_candidate` | `geopulse_synthetic_v1` |
| **`STORE_B`** | GeoPulse Coffee B | 77.640000 | 12.978000 | 786402.14 m | 1436185.41 m | `hypothetical_candidate` | `geopulse_synthetic_v1` |
| **`STORE_C`** | GeoPulse Coffee C | 77.648000 | 12.981000 | 787267.13 m | 1436526.50 m | `hypothetical_candidate` | `geopulse_synthetic_v1` |
| **`STORE_D`** | GeoPulse Coffee D | 77.638000 | 12.986000 | 786175.86 m | 1437068.70 m | `hypothetical_candidate` | `geopulse_synthetic_v1` |

- All four candidate coordinates fall strictly inside the study area bounds: $[77.6250, 77.6550]^\circ\text{E}$ and $[12.9650, 12.9900]^\circ\text{N}$.
- **Complete Inter-Store Planar Distances** (calculated in projected UTM Zone 43N space):
  - `STORE_A` to `STORE_B`: **896.26 m** (500m catchments overlap by up to **103.74 m**).
  - `STORE_B` to `STORE_C`: **929.81 m** (500m catchments overlap by up to **70.19 m**).
  - `STORE_B` to `STORE_D`: **911.81 m** (500m catchments overlap by up to **88.19 m**).
  - `STORE_C` to `STORE_D`: **1,218.55 m** (disjoint catchments; separation $> 1,000\text{ m}$).
  - `STORE_A` to `STORE_D`: **1,284.39 m** (disjoint catchments; separation $> 1,000\text{ m}$).
  - `STORE_A` to `STORE_C`: **1,823.03 m** (disjoint catchments; separation $> 1,000\text{ m}$).

Because two 500m buffer circles only intersect when their centroids are $< 1,000\text{ m}$ apart, geometric overlap is possible strictly for the three store pairs sharing `STORE_B` as an endpoint (`STORE_A`–`STORE_B`, `STORE_B`–`STORE_C`, and `STORE_B`–`STORE_D`). All other pairs are geographically disjoint.

---

## 5. Defensive Validation, Duplicate Detection & Output Safety Architecture

Both `src/spatial_analysis.py` and `src/retail_mobility_analytics.py` enforce strict defensive engineering practices:

### Spatial Analysis Validation
1. **Empty File Protection**: Wrapped CSV reader initialization in a `try...except StopIteration` handler. Zero-byte files raise a descriptive `ValueError`, and files with only header rows raise an informative error.
2. **Strict Schema Contract**: Validates the sequence and presence of all eight canonical columns (`ping_id`, `device_id`, `event_timestamp`, `latitude`, `longitude`, `accuracy_meters`, `movement_mode`, `data_provenance`).
3. **Coordinate Sanity Checks**: Coordinates are validated using `math.isfinite()`, rejecting `NaN`, `inf`, and non-numeric inputs. Geodetic ranges are validated globally ($[-180, 180]$ and $[-90, 90]$) and verified against `STUDY_AREA.bounds`.
4. **Identifier Integrity**: Missing or blank `ping_id` records and duplicate observation IDs are rejected.
5. **Radius & Buffer Constraints**: Catchment radius must be strictly positive and finite ($r > 0$). Buffer segments must be integers $\ge 1$.

### Retail Analytics Validation & Duplicate Detection
1. **Strict 8-Field Membership Schema**: Validates header sequence (`ping_id`, `device_id`, `event_timestamp`, `store_id`, `store_name`, `distance_meters`, `movement_mode`, `data_provenance`).
2. **Duplicate Membership Detection**: Enforces that no `(ping_id, store_id)` pair can appear more than once. This is validated in both Python preflight audits and the Spark loading pipeline; duplicate keys raise `ValueError` immediately.
3. **Timestamp Validation**: Requires timezone-aware ISO-8601 UTC strings with 'Z' suffix (e.g., `2026-11-16T08:30:00Z`). Null, blank, invalid formats, or non-UTC offsets are rejected.
4. **Distance Sanity**: Rejects null, non-numeric, NaN, positive infinity, negative infinity, and negative distances.
5. **Partition Discovery**: Automatically discovers multi-part partition files (`part-*.csv`) within the membership directory.

### Output Safety Guarantees
1. **Elimination of Destructive Deletion**: Removed `shutil.rmtree()`; output directories are never deleted automatically.
2. **All-Destination Preflight Check**: Before writing any outputs, the pipeline verifies that **none** of the target output directories exist (when `overwrite=False`). If any output directory exists, it fails fast with `FileExistsError` without writing partial results.
3. **Fail-Fast Network Reconciliation**: All network-level reconciliation assertions (pings, memberships, devices) execute **before** invoking Spark output writers. If reconciliation fails, an exception is raised and zero output directories are created.
4. **Native Spark Writer**: Uses `df.write.mode("errorifexists").option("header", "true").csv(...)` by default, or `"overwrite"` when explicitly approved.
5. **Driver Memory Safety**: DataFrames are written directly through Spark's distributed writer without collecting rows or RDD partitions to Python driver memory.

---

## 6. Spatial Catchment Analysis Results & Mathematical Reconciliation

The spatial pipeline was executed against `data/raw/synthetic_mobility_pings.csv` and `data/reference/synthetic_store_locations.csv` in WSL2 Ubuntu.

### Verified Per-Store Spatial Summary

| Candidate Store | Matched Pings | Distinct Devices | Multi-Catchment Pings | Catchment Area | Geometry Validity |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`STORE_A`** (GeoPulse Coffee A) | 1,190 | 146 | 60 | $780,361.29\text{ m}^2$ | `true` |
| **`STORE_B`** (GeoPulse Coffee B) | 1,118 | 146 | 95 | $780,361.29\text{ m}^2$ | `true` |
| **`STORE_C`** (GeoPulse Coffee C) | 930 | 127 | 16 | $780,361.29\text{ m}^2$ | `true` |
| **`STORE_D`** (GeoPulse Coffee D) | 813 | 105 | 19 | $780,361.29\text{ m}^2$ | `true` |
| **Total (Sum of Per-Store)** | **4,051** | *524 (device-store pairs)* | *190 (store overlap instances)* | — | — |

*Note: Per-store distinct device counts sum to 524 because devices may visit multiple catchments. The unique count across all stores is 350.*

### Overlap Distribution & Reconciliation Analysis

Pings were categorized by the number of unique store catchments they intersected:

| Overlap Category | Unique Ping Observations | Share of Matched Pings | Description |
| :--- | :--- | :--- | :--- |
| **Pings in Exactly 1 Catchment ($N_1$)** | **3,861** | 97.60% | Observations exclusive to a single candidate store |
| **Pings in Exactly 2 Catchments ($N_2$)** | **95** | 2.40% | Observations falling within overlapping buffer zones |
| **Pings in $\ge 3$ Catchments ($N_{\ge 3}$)** | **0** | 0.00% | No pings intersected 3 or more store catchments |
| **Total Multi-Catchment Pings** | **95** | 2.40% | Sum of pings intersecting $\ge 2$ catchments ($N_2 + N_{\ge 3}$) |
| **Total Matched Unique Pings** | **3,956** | 100.00% | Distinct pings matched to at least one candidate store |

#### Mathematical Reconciliation
$$\text{Total Matched Unique Pings} = N_1 + N_2 + N_{\ge 3} = 3,861 + 95 + 0 = \mathbf{3,956} \quad (\text{Exact Match — 100\% Reconciled})$$

$$\text{Total Store-Ping Memberships} = (N_1 \times 1) + (N_2 \times 2) + (N_{\ge 3} \times 3) = (3,861 \times 1) + (95 \times 2) + 0 = 3,861 + 190 = \mathbf{4,051} \quad (\text{Exact Match})$$

#### Device Network Exclusivity
- **Exclusive Devices** (visited exactly 1 store): **216** (61.71%)
- **Multi-Catchment Devices** (visited $\ge 2$ stores): **134** (38.29%)
- **Total Unique Network Devices**: $216 + 134 = \mathbf{350}$ ($100.00\%$)

---

## 7. Retail Mobility Analytics Results & Diurnal Profile

`src/retail_mobility_analytics.py` processed the 4,051 spatial membership records into four retail analytics datasets.

### 7.1 Persisted Per-Store Exclusivity & Overlap Summary (`store_overlap_summary`)

*Dataset size: Exactly 4 rows.*

| Store ID | Store Name | Total Pings | Exclusive Pings | Overlapping Pings | Total Devices | Exclusive Devices | Multi-Catchment Devices |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`STORE_A`** | GeoPulse Coffee A | 1,190 | 1,130 | 60 | 146 | 81 | 65 |
| **`STORE_B`** | GeoPulse Coffee B | 1,118 | 1,023 | 95 | 146 | 37 | 109 |
| **`STORE_C`** | GeoPulse Coffee C | 930 | 914 | 16 | 127 | 57 | 70 |
| **`STORE_D`** | GeoPulse Coffee D | 813 | 794 | 19 | 105 | 41 | 64 |
| **Sum / Reconciled** | — | **4,051** | **3,861** | **190** ($95 \times 2$) | **524** (pairs) | **216** | **308** (pairs) |

#### Exact Pairwise Breakdown of Multi-Catchment Pings
An exhaustive query across all 95 multi-catchment pings confirms that every overlap observation involves `STORE_B` pairing with one of the other three stores:
1. **`STORE_A` & `STORE_B`**: **60 pings** (63.16% of multi-catchment pings)
2. **`STORE_B` & `STORE_D`**: **19 pings** (20.00% of multi-catchment pings)
3. **`STORE_B` & `STORE_C`**: **16 pings** (16.84% of multi-catchment pings)
4. **All Other Store Pairs**: **0 pings** (disjoint catchments; inter-store distances $> 1,000\text{ m}$)
- **Reconciliation**: $60 + 19 + 16 = \mathbf{95}$ unique multi-catchment pings.

### 7.2 Diurnal Time-of-Day Activity Profile (`store_time_of_day_activity`)

*Dataset size: Exactly 16 rows (4 stores × 4 diurnal buckets).*

Timestamps were converted from UTC to `Asia/Kolkata` (IST, UTC+05:30):

| Store ID | Diurnal Period | Local IST Window | Distinct Devices | Ping Count | Diurnal Ping Share |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`STORE_A`** | Morning | 06:00–11:59 IST | 49 | 409 | 34.37% |
| | Afternoon | 12:00–16:59 IST | 35 | 304 | 25.55% |
| | Evening | 17:00–21:59 IST | 56 | 430 | 36.13% |
| | Night | 22:00–05:59 IST | 7 | 47 | 3.95% |
| **`STORE_B`** | Morning | 06:00–11:59 IST | 52 | 340 | 30.41% |
| | Afternoon | 12:00–16:59 IST | 38 | 292 | 26.12% |
| | Evening | 17:00–21:59 IST | 51 | 421 | 37.66% |
| | Night | 22:00–05:59 IST | 7 | 65 | 5.81% |
| **`STORE_C`** | Morning | 06:00–11:59 IST | 44 | 338 | 36.34% |
| | Afternoon | 12:00–16:59 IST | 31 | 181 | 19.46% |
| | Evening | 17:00–21:59 IST | 48 | 383 | 41.18% |
| | Night | 22:00–05:59 IST | 7 | 28 | 3.01% |
| **`STORE_D`** | Morning | 06:00–11:59 IST | 33 | 238 | 29.27% |
| | Afternoon | 12:00–16:59 IST | 31 | 260 | 31.98% |
| | Evening | 17:00–21:59 IST | 38 | 282 | 34.69% |
| | Night | 22:00–05:59 IST | 5 | 33 | 4.06% |

#### Network Diurnal Totals & Reconciliation
- **Morning**: 1,325 pings (32.71%) across 178 store-device pairs
- **Afternoon**: 1,037 pings (25.60%) across 135 store-device pairs
- **Evening**: 1,516 pings (37.42%) across 193 store-device pairs — *Peak activity period across the network*
- **Night**: 173 pings (4.27%) across 26 store-device pairs
- **Reconciliation**: $1,325 + 1,037 + 1,516 + 173 = \mathbf{4,051}$ memberships ($100.00\%$).

### 7.3 Daily Store Activity & Visit Intensity (`daily_store_summary`)

*Dataset size: Exactly 8 rows (4 stores × 2 calendar dates).*

| Store ID | Calendar Date (IST) | Distinct Devices | Ping Count | Active Hours | Pings / Device |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`STORE_A`** | 2026-11-16 | 140 | 1,134 | 15 | 8.10 |
| | 2026-11-17 | 6 | 56 | 7 | 9.33 |
| **`STORE_B`** | 2026-11-16 | 141 | 1,060 | 15 | 7.52 |
| | 2026-11-17 | 5 | 58 | 5 | 11.60 |
| **`STORE_C`** | 2026-11-16 | 122 | 909 | 15 | 7.45 |
| | 2026-11-17 | 5 | 21 | 5 | 4.20 |
| **`STORE_D`** | 2026-11-16 | 102 | 798 | 15 | 7.82 |
| | 2026-11-17 | 4 | 15 | 4 | 3.75 |

- **Day 1 (2026-11-16)**: 3,901 pings (96.30%). All 4 stores recorded observations in exactly 15 active hours.
- **Day 2 (2026-11-17)**: 150 pings (3.70%). Generated exclusively during the generator's `nighttime_low` window (`22:00`–`07:00` IST, 540 min duration, 5% trajectory allocation weight), which crosses midnight from 2026-11-16 into the morning of 2026-11-17.
- **Reconciliation**: $3,901 + 150 = \mathbf{4,051}$ memberships ($100.00\%$).
- **Active Hours Reconciled**: $15 \times 4 + (7 + 5 + 5 + 4) = 60 + 21 = \mathbf{81}$ store-hours (matching `hourly_store_activity` record count).

### 7.4 Hourly Activity Profile & Peak Analysis (`hourly_store_activity`)

*Dataset size: Exactly 81 rows.*

The `hourly_store_activity` mart records hourly pings and distinct devices across all active store-hours:
- `STORE_A`: 22 active hours (15 on 2026-11-16 + 7 on 2026-11-17)
- `STORE_B`: 20 active hours (15 on 2026-11-16 + 5 on 2026-11-17)
- `STORE_C`: 20 active hours (15 on 2026-11-16 + 5 on 2026-11-17)
- `STORE_D`: 19 active hours (15 on 2026-11-16 + 4 on 2026-11-17)
- **Total Rows**: $22 + 20 + 20 + 19 = \mathbf{81}$ store-hours.

#### Network Hourly Traffic Highlights (IST)
- **Single Highest Traffic Hour Across Network**: **Hour 09:00–09:59 IST** with **563 pings** and 78 store-device interactions across the 4 stores.
- **Second Highest Traffic Hour**: **Hour 19:00–19:59 IST** with **506 pings** and 75 store-device interactions.
- **Lunch Peak Hour**: **Hour 13:00–13:59 IST** with **486 pings** and 73 store-device interactions.
- **Individual Store Single-Hour Peaks**:
  - `STORE_A`: Peak at 09:00 IST (204 pings, 29 distinct devices)
  - `STORE_B`: Peak at 19:00 IST (173 pings, 21 distinct devices)
  - `STORE_C`: Peak at 09:00 IST (121 pings, 21 distinct devices)
  - `STORE_D`: Peak at 13:00 IST (145 pings, 20 distinct devices)

---

## 8. Output Datasets & Verification Artifacts

All outputs were written using Spark's native CSV writer and verified against exact record counts:

### Baseline Processed Outputs (`data/processed/`)

1. **`store_catchments/`**:
   - `part-00000.csv` (4 data rows, 5,288 bytes)
   - `_SUCCESS` (0 bytes)
   - Schema: `store_id, store_name, radius_meters, area_sq_meters, geometry_wkt, crs`
2. **`store_catchment_memberships/`**:
   - `part-00000.csv` (4,051 data rows, 696,450 bytes)
   - `_SUCCESS` (0 bytes)
   - Schema: `ping_id, device_id, event_timestamp, store_id, store_name, distance_meters, movement_mode, data_provenance`
3. **`store_catchment_summary/`**:
   - `part-00000.csv` (4 data rows, 223 bytes)
   - `_SUCCESS` (0 bytes)
   - Schema: `store_id, store_name, matched_pings, distinct_devices, multi_catchment_pings`
4. **`week2_retail_analytics/daily_store_summary/`**:
   - `part-00000-...csv` (8 data rows, 497 bytes)
   - `_SUCCESS` (0 bytes)
   - Schema: `store_id, store_name, calendar_date, distinct_devices, ping_count, active_hours_count, pings_per_device`
5. **`week2_retail_analytics/hourly_store_activity/`**:
   - `part-00000-...csv` (81 data rows, 4,888 bytes)
   - `_SUCCESS` (0 bytes)
   - Schema: `store_id, store_name, calendar_date, hour_of_day, distinct_device_count, ping_count`
6. **`week2_retail_analytics/store_overlap_summary/`**:
   - `part-00000-...csv` (4 data rows, 381 bytes)
   - `_SUCCESS` (0 bytes)
   - Schema: `store_id, store_name, total_pings, exclusive_pings, overlapping_pings, total_devices, exclusive_devices, multi_catchment_devices`
7. **`week2_retail_analytics/store_time_of_day_activity/`**:
   - `part-00000-...csv` (16 data rows, 908 bytes)
   - `_SUCCESS` (0 bytes)
   - Schema: `store_id, store_name, time_of_day, distinct_device_count, ping_count`

### Isolated Verification Directories
Identical structures and bit-exact outputs were generated under isolated directories during verification runs:
- `data/processed/week2_verification/` (Spatial analysis isolated verification: 3 subdirectories)
- `data/processed/week2_retail_analytics_verification/` (Retail analytics isolated verification: 4 subdirectories)

---

## 9. Baseline Preservation & Integrity Verification

To verify that isolated execution did not modify or overwrite baseline datasets, complete SHA-256 checksums of all pre-existing files under `data/processed/` were recorded before and after execution:

| Existing Output File | Complete SHA-256 Checksum (Pre-Execution) | Complete SHA-256 Checksum (Post-Execution) | Integrity Status |
| :--- | :--- | :--- | :--- |
| `data/processed/.gitkeep` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | Preserved |
| `data/processed/store_catchments/_SUCCESS` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | Preserved |
| `data/processed/store_catchments/part-00000.csv` | `0ec9dfa2850e95dc9748fcf4965626577f44e4bbc5c59813073d7daa646b5ca5` | `0ec9dfa2850e95dc9748fcf4965626577f44e4bbc5c59813073d7daa646b5ca5` | Preserved |
| `data/processed/store_catchment_memberships/_SUCCESS` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | Preserved |
| `data/processed/store_catchment_memberships/part-00000.csv` | `6aba56572bcfc78bf155bc7b3ca96c6e4e9574272b41c64bdecb72b0fe1376e8` | `6aba56572bcfc78bf155bc7b3ca96c6e4e9574272b41c64bdecb72b0fe1376e8` | Preserved |
| `data/processed/store_catchment_summary/_SUCCESS` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | Preserved |
| `data/processed/store_catchment_summary/part-00000.csv` | `a76a4d439d0cb1aaa0730a780304fc2853659619192d2dfae41e2dce23ff9f57` | `a76a4d439d0cb1aaa0730a780304fc2853659619192d2dfae41e2dce23ff9f57` | Preserved |

All seven baseline file checksums matched bit-for-bit, proving that isolated verification runs did not corrupt or modify the primary outputs.

---

## 10. Automated Test Suite Execution

The automated test suite contains **163 unit and integration tests** across **5 test modules** and **23 test classes**.

### Test Execution Command & Verification

```powershell
# Executed in Windows Python 3.12 (.venv)
.venv\Scripts\python.exe -m unittest discover -s tests -v
# Output: Ran 163 tests in 0.939s — OK
```

*(Environment Note: The complete 163-test regression suite was executed and validated in the Windows Python 3.12 environment (`.venv\Scripts\python.exe`), passing with zero failures and zero errors. During earlier Milestone 1 development under commit `0673203`, the 110 spatial analysis tests were also executed in WSL2 Ubuntu in 0.482s. The complete 163-test suite reported here reflects direct execution evidence from the Windows Python environment).*

### Comprehensive Test Suite Breakdown (23 Classes across 5 Modules)

| Module | Test Class | Test Count | Status | Functional Scope Covered |
| :--- | :--- | :--- | :--- | :--- |
| `tests/test_study_area_spec.py` | `TestStudyAreaSpecification` | 6 | Pass | Bounding envelope, centroid, coordinate ranges, CRS |
| | `TestSyntheticGPSSchemaContract` | 3 | Pass | 8 canonical fields, movement mode enums, provenance |
| `tests/test_data_generator.py` | `TestSyntheticGPSGenerator` | 20 | Pass | Seeded PRNG kinematics, diurnal windows, midnight crossing |
| `tests/test_data_quality.py` | `TestDataQualityValidator` | 36 | Pass | Structural, temporal, spatial, and kinematic gates |
| `tests/test_spatial_analysis.py` | `TestOutputDirectoryProtection` | 1 | Pass | Target directory collision prevention (`overwrite=False`) |
| | `TestCatchmentGeometryCalculations` | 7 | Pass | Buffer area formulas, polygon approximation, tolerances |
| | `TestStoreReferenceValidation` | 14 | Pass | Store schema, coordinate bounds, provenance, bad formats |
| | `TestMobilityInputValidation` | 16 | Pass | Empty files, non-numeric coordinates, schema mismatch |
| | `TestOverlapDistributionAndReconciliation` | 3 | Pass | Disjoint category math, membership reconciliation |
| | `TestSpatialAnalysisConfig` | 3 | Pass | Default config, custom parameter injection, assertions |
| | `TestSedonaJarDiscovery` | 1 | Pass | Shaded JAR discovery from Ivy2 cache |
| `tests/test_retail_mobility_analytics.py` | `TestDiurnalTimeOfDayClassification` | 7 | Pass | Diurnal bucket boundaries (morning, afternoon, evening, night) |
| | `TestMembershipsSchemaAndInputValidation` | 11 | Pass | Membership schema, coordinate sanity, partition discovery |
| | `TestTimestampValidation` | 8 | Pass | Strict ISO-8601 UTC parsing, IST timezone offset (+05:30) |
| | `TestInputFieldValidation` | 9 | Pass | Finite non-negative distances, valid modes, provenance |
| | `TestDuplicateMembershipDetection` | 2 | Pass | Strict rejection of duplicate (ping_id, store_id) records |
| | `TestOutputPreflightProtection` | 5 | Pass | Preflight target checks, fail-fast reconciliation assertions |
| | `TestHourlyStoreActivityAggregation` | 3 | Pass | Active hour grouping, distinct devices, ping counts |
| | `TestStoreTimeOfDayAggregation` | 2 | Pass | Diurnal aggregation and network reconciliation |
| | `TestDailyStoreSummaryAggregation` | 1 | Pass | Daily visits, active hours, pings_per_device calculation |
| | `TestStoreOverlapSummaryAggregation` | 2 | Pass | Per-store exclusivity, multi-catchment devices |
| | `TestRetailAnalyticsConfig` | 2 | Pass | Configuration paths, defaults, overwrite safety |
| | `TestActualMembershipsDatasetIfPresent` | 1 | Pass | Direct audit of processed membership dataset |
| **Total Test Suite** | **5 Modules / 23 Classes** | **163** | **Pass** | **163 automated tests passed (0 failures, 0 errors)** |

All tests in `tests/test_spatial_analysis.py` and `tests/test_retail_mobility_analytics.py` run in pure Python without launching an active Spark JVM session, completing in under one second.

---

## 11. Snowflake & dbt Integration Scope Decision

### Strategic Decision & Status
During Week 2, all spatial catchment analysis and retail mobility aggregations were executed locally using Apache Spark 3.5.9, Apache Sedona 1.9.1, and pure Python.

- **Snowflake Infrastructure**: Prepared DDL scripts from Week 1 Day 4 (`001_create_raw_mobility_table.sql` and `002_create_spatial_mobility_table.sql`) remain in the repository. **Zero Snowflake queries were executed during Week 2, and no cloud compute costs were incurred.**
- **dbt Modeling**: **No dbt models or profiles were created or run in Week 2.**
- **Strategic Justification**: Maintaining a local-first paradigm for Week 2 allowed rapid algorithm development, exact mathematical verification of planar metric buffers, and fast regression testing without cloud latency, cloud credentials, or external compute spend.
- **Week 3 Boundary**: Formal cloud data warehousing, store reference DDL, dbt transformation models, and parity validation between Snowflake and Apache Sedona are formally designated as **Week 3** milestones.

---

## 12. Technical Limitations & Analytical Interpretation

To maintain academic and professional standards, the following technical constraints apply to these results:

1. **Synthetic Nature of Mobility Data**:
   - The 10,000 mobility observations and 500 devices were generated using GeoPulse's kinematic generator (`geopulse_synthetic_v1`).
   - The generator uses a fixed random seed (`seed=42`) to drive its seeded pseudo-random number generator (PRNG), including coordinates, kinematics, diurnal window allocation, and seeded UUID generation via `random.Random(seed).getrandbits(128)`.
   - Observations represent simulated trajectories along synthetic corridors; they do not reflect actual human foot-traffic, GPS telemetry from smartphones, or telecom CDR records.
2. **Hypothetical Retail Candidates**:
   - `STORE_A`, `STORE_B`, `STORE_C`, and `STORE_D` are synthetic coordinate pairs within Indiranagar, Bengaluru. They do not correspond to registered commercial entities, real coffee shops, or verified street addresses.
3. **Spatial Proximity vs. Commercial Footfall**:
   - Catchment membership signifies geometric containment within a 500-metre radius of a candidate store point.
   - A membership record indicates spatial proximity only; it does **not** indicate an actual commercial store visit, physical indoor entry, lingering dwell, or retail transaction. Many pings represent pedestrian or vehicular traffic passing along nearby public thoroughfares.
4. **Catchment Overlap vs. Store Cannibalization**:
   - The 95 multi-catchment pings and 134 multi-catchment devices represent geographic overlap between adjacent 500-metre circles (specifically 60 pings between `STORE_A` & `STORE_B`, 19 pings between `STORE_B` & `STORE_D`, and 16 pings between `STORE_B` & `STORE_C`).
   - Spatial overlap indicates shared geographic proximity, not customer switching, shared customer loyalty, or commercial cannibalization.
5. **Planar Projection Geometry**:
   - Catchments are circular buffers in UTM Zone 43N (`EPSG:32643`). While UTM Zone 43N provides low distance distortion for Bengaluru, buffers are planar representations that do not account for physical topography, building footprints, pedestrian barriers, or street network walking times (isochrones).
6. **Local Single-Node Processing**:
   - Processing was executed locally on a single machine using Spark `local[2]`. These results demonstrate algorithmic correctness and spatial SQL functionality, not distributed multi-node cluster performance.
7. **Harmless SLF4J Warnings**:
   - Standard warnings (`SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder"`) appear during Spark startup because no optional SLF4J binding jar was supplied. SLF4J falls back to a NOP (no-operation) logger, which has zero effect on computational correctness.
8. **Cloud Operations Boundary**:
   - Zero Snowflake, cloud warehouse, or paid external operations were performed during Week 2 local spatial processing.

---

## 13. Git Status and Working Tree Verification

```powershell
git status --short --branch
```
```text
## main...origin/main
?? reports/WEEK2_COMPLETION_REPORT.md
```

### Git Repository State
- **Branch**: `main` (synchronized with `origin/main` at commit `e42cda4`).
- **HEAD Commit**: `e42cda4 feat: implement retail mobility analytics`.
- **Tracked Files Modified**: **0** (all prior source and test files remain completely clean).
- **Committed Week 2 Deliverables**:
  - `data/reference/synthetic_store_locations.csv`
  - `src/spatial_analysis.py`
  - `src/retail_mobility_analytics.py`
  - `tests/test_spatial_analysis.py`
  - `tests/test_retail_mobility_analytics.py`
- **Data Protection**: Output directories under `data/processed/` are gitignored per `.gitignore` rule (`data/processed/*` except `.gitkeep`).
- **Preservation**: No staging, commits, or pushes were performed during this documentation finalization.

---

## 14. Conclusion & Week 3 Roadmap

Week 2 successfully developed, tested, and verified the complete local geospatial and retail mobility analytics layer of GeoPulse. The pipeline establishes clean spatial joins and metric buffering between synthetic mobility pings and candidate retail stores, converts UTC timestamps to Indian Standard Time, computes diurnal catchment activity profiles, and calculates network overlap distributions—supported by 163 automated tests and fully reconciled output datasets.

### Next Steps (Week 3 Roadmap):
1. **Snowflake Spatial Infrastructure**: Author and deploy DDL to load candidate store reference geometries and 500-metre catchment buffer polygons into `GEOPULSE_DB.ANALYTICS`.
2. **dbt Transformation Models**: Develop modular dbt models in Snowflake to perform retail mobility joins, dwell-time categorization, and visitor aggregation in the data warehouse.
3. **Parity & Reconciliation Testing**: Verify mathematical parity between Snowflake SQL / dbt analytical tables and the verified PySpark baseline.
4. **Cloud Spatial Analytics Reporting**: Author the Week 3 completion report documenting end-to-end cloud spatial transformations, query performance, and retail intelligence marts.
