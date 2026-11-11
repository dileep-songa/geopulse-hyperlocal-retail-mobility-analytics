# GeoPulse: Data Dictionary & Mobility Telemetry Specification

**Document Version**: 1.0.1<br/>
**Phase**: Week 1 Day 1 — Data Specification & Environment Foundation<br/>
**Domain**: Retail Strategy & Hyper-Local Geospatial Analytics<br/>
**Coordinate Reference System**: WGS 84 (`EPSG:4326`)

---

## 1. Project Purpose and Scope

GeoPulse models and analyzes hyper-local retail foot traffic, consumer dwell times, and commercial catchment overlap using synthetic mobile device location telemetry.

The primary analytical goal is to evaluate commercial viability and store catchment cannibalization for a hypothetical coffee-retail network in a dense Indian urban environment. To support realistic geospatial and temporal analysis without privacy liabilities, all location observations are generated using deterministic mathematical simulations.

This document serves as the formal schema contract, geographic boundary specification, and behavioral assumption baseline for the synthetic telemetry generation engine and all downstream spatial data pipelines.

---

## 2. Geographic Specification (Study Area)

### 2.1 Selected Study Area: Indiranagar, Bengaluru, India

The designated study area is **Indiranagar**, a premier commercial and residential district situated in Bengaluru (Bangalore), Karnataka, India.

- **Focal Commercial Corridors**:
  - **100 Feet Road**: A high-density arterial corridor known for premier apparel flagship stores, specialty dining, and cafe retail.
  - **12th Main Road**: A vibrant commercial spine featuring a dense cluster of specialty coffee houses, coworking spaces, artisanal roasteries, and boutique retail.
- **Hypothetical Retail Domain**: Specialty Coffee Retail Network (simulating competing coffee chains such as Third Wave Coffee, Blue Tokai Coffee Roasters, Starbucks, Araku Coffee, and local artisanal cafes).
- **Urban Context**: Dense grid and organic street layout, high pedestrian activity during daytime and evening hours, frequent mixed-modal transit (walking, auto-rickshaws, two-wheelers, cabs), and distinct commercial vs. residential zones.

### 2.2 Bounding Envelope and Coordinate Bounds

All simulated GPS observations are strictly confined to the following geodetic bounding box:

| Boundary Parameter | Geodetic Coordinate | Description / Landmark Reference |
| :--- | :--- | :--- |
| **Minimum Longitude ($X_{\min}$)** | `77.6250° E` | Western boundary near Domlur / Indiranagar boundary |
| **Maximum Longitude ($X_{\max}$)** | `77.6550° E` | Eastern boundary near Suranjandas Road / Defense Colony |
| **Minimum Latitude ($Y_{\min}$)** | `12.9650° N` | Southern boundary near Old Airport Road corridor |
| **Maximum Latitude ($Y_{\max}$)** | `12.9900° N` | Northern boundary near Swami Vivekananda Road (Old Madras Rd) |
| **Center / Centroid** | `(77.6400° E, 12.9775° N)` | Heart of 100 Feet Road & 12th Main intersection |

```
                       12.9900° N (North Bound)
                +---------------------------------------+
                |                                       |
                |        Swami Vivekananda Road         |
                |                                       |
 77.6250° E     |   [Indiranagar Commercial Corridor]   |     77.6550° E
 (West Bound)   |      100 Feet Rd & 12th Main          |     (East Bound)
                |                                       |
                |           Old Airport Road            |
                |                                       |
                +---------------------------------------+
                       12.9650° N (South Bound)
```

> [!NOTE]
> **Analytical Envelope Disclaimer**: This bounding box is an idealized rectangular study envelope chosen specifically to encompass the dense retail corridors of 100 Feet Road and 12th Main for computational and modeling practicality. It does not represent an exact municipal ward boundary, postal zone, or legal administrative border of Indiranagar.

### 2.3 Spatial Dimensions & Practicality for Local Development

- **Longitudinal Span**: $\Delta \lambda = 0.0300^\circ \approx 3.25\text{ km}$
- **Latitudinal Span**: $\Delta \phi = 0.0250^\circ \approx 2.77\text{ km}$
- **Total Bounding Area**: Approximately $9.0\text{ km}^2$
- **Development Suitability**:
  - Compact footprint ensures that spatial indexing (H3 resolutions 8, 9, and 10) and distributed spatial joins (Apache Sedona / PySpark) run rapidly in local developer workstations and single-cluster cloud environments.
  - Sized appropriately to model 10 to 20 retail POIs (Points of Interest) and realistic pedestrian walking trips between 200 meters and 1.5 kilometers.

### 2.4 Coordinate Conventions

1. **Coordinate Reference System (CRS)**: WGS 84 (World Geodetic System 1984), EPSG code `4326`.
2. **Axis Ordering**:
   - **Spatial Geometry / GIS Construction**: Longitude first ($X$), Latitude second ($Y$). Point representation: `POINT(longitude latitude)` or `[longitude, latitude]`.
   - **Tabular / Storage Representation**: Stored in explicit distinct decimal fields: `longitude` and `latitude`.
3. **Coordinate Storage Precision vs. Simulated GPS Accuracy**:
   - **Coordinate Storage Precision**: Coordinates are formatted and stored to **6 decimal places** ($\approx 0.11\text{ meters}$ ground resolution at this latitude). This represents floating-point storage precision in CSV exports, Parquet files, and database columns.
   - **Simulated Measurement Accuracy**: Storing coordinates to 6 decimal places does *not* imply that mobile GPS hardware achieves 11-centimeter accuracy. Real-world consumer GPS exhibits radial uncertainty due to atmospheric conditions, satellite geometry, and urban canyons. In GeoPulse, physical GPS uncertainty is explicitly and separately modeled via the `accuracy_meters` field ($2.0\text{m} \le r \le 35.0\text{m}$).

---

## 3. Synthetic GPS Observation Schema

Every simulated mobility record represents a discrete GPS location ping emitted by a synthetic virtual agent. Downstream CSV files, Parquet artifacts, and Snowflake raw tables must adhere strictly to this schema:

### 3.1 Field-by-Field Specification

| Column Name | CSV Representation | Snowflake Type | Nullable | Example Value | Description & Constraints |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `ping_id` | `STRING` | `VARCHAR(36)` | **No** | `"550e8400-e29b-41d4-a716-446655440000"` | Globally unique identifier for the observation ping. Must conform to RFC 4122 UUIDv4 format. |
| `device_id` | `STRING` | `VARCHAR(36)` | **No** | `"9f8c1b24-7d5a-4b62-9e81-2c5e4a8f9b10"` | Pseudonymous identifier of the synthetic agent. Persistent across multiple pings from the same agent to form trajectories. RFC 4122 UUIDv4 format. |
| `event_timestamp` | `STRING` | `TIMESTAMP_TZ` | **No** | `"2026-11-16T03:15:30.000Z"` | Observation timestamp in timezone-aware UTC. In CSV files, serialized as an ISO-8601 UTC string (`YYYY-MM-DDTHH:MM:SS.sssZ`). In Snowflake, stored natively as `TIMESTAMP_TZ`. |
| `latitude` | `FLOAT` | `DOUBLE` | **No** | `12.978250` | Geodetic latitude in decimal degrees (WGS 84). Constraint: $12.9650 \le \text{latitude} \le 12.9900$. Formatted to 6 decimal places storage precision. |
| `longitude` | `FLOAT` | `DOUBLE` | **No** | `77.641820` | Geodetic longitude in decimal degrees (WGS 84). Constraint: $77.6250 \le \text{longitude} \le 77.6550$. Formatted to 6 decimal places storage precision. |
| `accuracy_meters` | `FLOAT` | `FLOAT` | **No** | `8.5` | Simulated 68% confidence horizontal positional uncertainty radius in meters. Constraint: $2.0 \le \text{accuracy\_meters} \le 35.0$. |
| `movement_mode` | `STRING` | `VARCHAR(20)` | **No** | `"stationary_dwell"` | Kinematic mode at observation time. Restricted to: `'stationary_dwell'`, `'pedestrian_walking'`, `'vehicular_transit'`. |
| `data_provenance` | `STRING` | `VARCHAR(30)` | **No** | `"geopulse_synthetic_v1"` | Constant metadata tag verifying that the observation was synthetically produced by the GeoPulse simulation engine. |

### 3.2 Timezone and Temporal Conventions

- **Simulation Activity Windows**: Defined in local Indian Standard Time (`Asia/Kolkata`, IST, UTC+05:30) to mirror realistic consumer routines and cafe business hours.
- **Storage Timezone**: Strictly **timezone-aware UTC** (Coordinated Universal Time).
- **Time Conversion & Awareness Requirements**:
  - In Python, observation timestamps must always be created as **timezone-aware** objects (e.g., using `datetime.now(timezone.utc)` or explicit `tzinfo=timezone.utc`), never naive datetimes.
  - Timestamps generated in local IST are converted to timezone-aware UTC before persisting observations:
    $$\text{Timestamp}_{\text{UTC}} = \text{Timestamp}_{\text{IST}} - \text{05:30}$$
  - **CSV Representation**: Formatted explicitly as an ISO-8601 string in UTC, for example: `"2026-11-16T03:15:30.000Z"`.
  - **Snowflake Storage**: Loaded and stored as `TIMESTAMP_TZ`.

---

## 4. Synthetic Movement Assumptions

The simulation models human spatial dynamics via mathematically defined behavioral rules rather than captured user telemetry:

### 4.1 Temporal Activity Windows (Diurnal Curve)

Simulated agents follow an urban commercial activity curve reflective of Bengaluru's retail cafe dynamics:

| Period Name | Local Time (IST) | UTC Window | Traffic Character | Target Simulation Behavior |
| :--- | :--- | :--- | :--- | :--- |
| **Morning Commute** | 08:00 – 11:00 | 02:30 – 05:30 | Commuter & breakfast rush | Fast road transit; high grab-and-go cafe visits (10–20 min dwell). |
| **Midday Work / Lunch** | 11:30 – 15:30 | 06:00 – 10:00 | Moderate commercial volume | Coworking cafe sessions and dining (30–75 min dwell). |
| **Evening Social Peak** | 17:00 – 21:30 | 11:30 – 16:00 | Maximum pedestrian density | High footfall on 100 Feet Rd & 12th Main; multi-store browsing & cafe dwell (20–45 min). |
| **Nighttime Low** | 22:00 – 07:00 | 16:30 – 01:30 | Minimal activity | Nominal transit; minimal retail store entry. |

### 4.2 Agent Trajectory Persistence and Sampling Rates

- **Device Population**: A fixed cohort of synthetic agents is generated per simulated calendar day.
- **Variable Sampling Frequency**:
  - **In-Transit (`pedestrian_walking` or `vehicular_transit`)**: High-frequency breadcrumbs recorded every **15 to 60 seconds** to trace continuous street paths.
  - **Stationary (`stationary_dwell`)**: Low-frequency presence pings recorded every **3 to 10 minutes** to model dwell duration within store boundaries while avoiding unnecessary data ballooning.

### 4.3 Plausible Kinematic Speeds

| Movement Mode | Speed Range ($\text{m/s}$) | Speed Range ($\text{km/h}$) | Kinematic Interpretation |
| :--- | :--- | :--- | :--- |
| `stationary_dwell` | $0.0 \le v \le 0.3\text{ m/s}$ | $0.0 \le v \le 1.1\text{ km/h}$ | Agent lingering inside or directly outside a retail venue; position varies only by simulated GPS sensor noise. |
| `pedestrian_walking` | $0.8 \le v \le 1.6\text{ m/s}$ | $2.9 \le v \le 5.8\text{ km/h}$ | Typical pedestrian walking pace along sidewalks and pedestrian crossings. |
| `vehicular_transit` | $3.5 \le v \le 14.0\text{ m/s}$ | $12.6 \le v \le 50.4\text{ km/h}$ | Urban traffic speeds reflecting two-wheelers, auto-rickshaws, buses, and cars navigating urban congestion. |

### 4.4 Boundary Enforcement & Realistic Movement

- **Strict Boundary Enclosure**: All generated coordinates must strictly remain within the defined bounding box $[77.6250, 77.6550] \times [12.9650, 12.9900]$.
- **Smooth Boundary Redirection**: Trajectory generation algorithms must avoid abrupt, unrealistic spatial jumps or teleportation when encountering boundaries. When a simulated agent approaches a perimeter edge, smooth deceleration and steering deflection or boundary turnaround vectors must be applied to guide the agent back toward internal commercial corridors.
- **Synthetic Movement Rule Disclaimer**: Any reflection, redirection, or velocity damping applied at the bounding perimeter is an algorithmic simulation rule, not actual physical road navigation or vehicle physics.
- **Envelope Scope Disclaimer**: The bounding box represents an analytical study perimeter selected for development tractability; it does not represent an exact municipal, postal, or commercial border.

### 4.5 Deterministic Reproducibility

- **Fixed Seed Determinism**: Setting a fixed random seed (e.g., `seed = 42`) provides deterministic, repeatable output sequences for the **same generator algorithm, identical input parameters, and matching software execution environment** (Python runtime and library versions).
- **Scope Limitation**: This guarantee ensures experimental repeatability during development. It does not promise bitwise reproducibility across arbitrary code refactoring, algorithmic changes, or major dependency version upgrades.

---

## 5. Privacy, Ethical Safeguards, and Synthetic Provenance

1. **Synthetic-Only Data Guarantee**:
   - Zero percent of the mobility observations originate from living human individuals, cellular tower triangulation, mobile applications, or location data brokers.
   - All trajectories are generated via algorithmic random walks, network shortest paths, and statistical dwell distributions.
2. **Absence of Real-World Identifiers**:
   - Identifiers (`ping_id`, `device_id`) are cryptographically random UUIDv4 strings.
   - No phone numbers, IMEIs, MAC addresses, IP addresses, Mobile Advertising IDs (IDFA/AAID), or demographic identifiers are ever generated or stored.
3. **Explicit Provenance Tagging**:
   - The field `data_provenance = 'geopulse_synthetic_v1'` is physically present in every observation row to prevent accidental conflation with real-world mobility datasets.

---

## 6. Known Limitations

- **Geometric Network Approximation**: Trajectories are synthesized using bounding box geometries and primary street corridors; fine-grained building indoor navigation or multi-story elevation differences are not simulated.
- **Heuristic Kinematics**: Simulated agents do not model anomalous human behaviors (e.g., sudden protests, flash crowds, weather disruptions).
- **Hypothetical Commercial POIs**: Retail store footprints and boundaries are modeled using open-source geometries and synthetic parcel polygons for research purposes.
