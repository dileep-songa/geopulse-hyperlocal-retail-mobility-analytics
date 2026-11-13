# GeoPulse — Week 1 Day 2: Synthetic GPS Data Generation Report

**Date of Generation**: 2026-11-12<br/>
**Target Simulation Date**: 2026-11-16<br/>
**Artifact Generated**: `data/raw/synthetic_mobility_pings.csv` (Ignored by Git)<br/>
**SHA-256 Checksum**: `a5070e6a0d94ba6ddc23151564c42d1365d0453cde83c55513f4cda14a124aa8`<br/>
**Status**: Generated, Audited, Validated, and Verified

---

## 1. Executive Summary & Objective

In Week 1 Day 2, the GeoPulse platform developed, audited, and executed its **Synthetic GPS Mobility Telemetry Generator** (`src/data_generator.py`). Built exclusively using Python standard-library modules, the generator synthesizes realistic mobile device location breadcrumbs for hyper-local retail foot-traffic and catchment overlap modeling.

The primary objective of this generation run is to provide an initial development dataset conforming strictly to the schema, geodetic boundaries, activity windows, and kinematic assumptions ratified in Week 1 Day 1.

> [!IMPORTANT]
> **Synthetic Data Disclaimer**: All records, device identifiers, coordinate sequences, and dwell intervals in this dataset are **100% synthetically simulated**. They do not represent observed human customer footfall, measured mobile advertising pings (MAIDs), or real-world cellular carrier telemetry.

---

## 2. Generation Configuration Parameters

| Parameter | Configuration Value | Description |
| :--- | :--- | :--- |
| **Total Records** | `10,000` | Exact count of synthetic observation pings generated |
| **Synthetic Devices** | `500` | Number of distinct simulated mobile agents |
| **Random Seed** | `42` | Explicit PRNG seed for deterministic reproducibility |
| **Simulation Date** | `2026-11-16` | Baseline simulation calendar date |
| **Activity Timezone** | `Asia/Kolkata` (IST, UTC+05:30) | Local time reference for diurnal movement curves |
| **Storage Timezone** | `UTC` | Timezone-aware UTC output with `Z` suffix |
| **Study Area** | Indiranagar, Bengaluru, India | Target metropolitan commercial retail corridor |
| **Bounding Box** | Longitude: `[77.6250, 77.6550]`<br/>Latitude: `[12.9650, 12.9900]` | Analytical study envelope encompassing 100 Feet Rd & 12th Main |
| **Output Destination** | `data/raw/synthetic_mobility_pings.csv` | Git-ignored local raw storage |

### Exact Day 1 Diurnal Activity Windows (IST) & Measured Counts
- **Morning Commute (08:00 – 11:00 IST)**: `3,014` records ($30.14\%$)
- **Midday Work / Lunch (11:30 – 15:30 IST)**: `2,849` records ($28.49\%$)
- **Evening Social Peak (17:00 – 21:30 IST)**: `3,631` records ($36.31\%$)
- **Nighttime Low (22:00 – 07:00 IST, crossing midnight)**: `506` records ($5.06\%$)
- **Excluded Interval Gaps**: `0` records ($0.00\%$, zero spillover into excluded gap hours)

---

## 3. Dataset Validation & Quality Audit

The generated CSV was subjected to automated verification against the Day 1 specification:

| Audit Dimension | Target Specification | Measured Value | Compliance Status |
| :--- | :--- | :--- | :--- |
| **Record Count** | Exactly `10,000` | `10,000` rows (excluding header) | **Passed (100%)** |
| **File Size** | Expected $\approx 1.5\text{--}1.8\text{ MB}$ | `1,644,477 bytes` ($1,605.93\text{ KB}$) | **Passed** |
| **Unique Ping IDs** | Exactly `10,000` unique UUIDv4 | `10,000` | **Passed (Zero collisions)** |
| **Unique Devices** | Exactly `500` unique UUIDv4 | `500` | **Passed** |
| **Pings per Device** | Min $\ge 1$, Avg $= 20.0$ | Min: `3`, Max: `54`, Avg: `20.0` | **Passed** |
| **Null / Missing Values** | `0` across all columns | `0` | **Passed** |
| **Duplicate Records** | `0` duplicate rows | `0` | **Passed** |
| **Latitude Range** | `[12.965000, 12.990000]` | `[12.965000, 12.990000]` | **Passed (Strictly inside)** |
| **Longitude Range** | `[77.625000, 77.655000]` | `[77.625005, 77.654992]` | **Passed (Strictly inside)** |
| **Coordinate Precision**| Exactly `6` decimal places | `6` decimal places on 100% of rows | **Passed** |
| **Accuracy Range** | `[2.0, 35.0]` meters | `[2.0, 35.0]` meters (0 invalid) | **Passed** |
| **Data Provenance** | `"geopulse_synthetic_v1"` | `"geopulse_synthetic_v1"` (10,000 / 10,000) | **Passed** |
| **Timestamp Format** | ISO-8601 UTC with `'Z'` suffix | `YYYY-MM-DDTHH:MM:SS.sssZ` (0 invalid) | **Passed** |
| **Device Chronology** | Monotonically increasing per device | 500 / 500 devices strictly ordered | **Passed** |
| **Walking Speed Limits** | `0.8 – 1.6 m/s` | `0` speed violations across all devices | **Passed** |
| **Vehicle Speed Limits** | `3.5 – 14.0 m/s` | `0` speed violations across all devices | **Passed** |
| **Dwell Displacement** | Apparent speed $\le 0.3\text{ m/s}$ | `0` dwell speed violations across all devices | **Passed** |
| **Maximum Single Step** | Bounded by $v_\max \times \Delta t_\max = 840\text{ m}$ | `806.34 m` (no impossible jumps) | **Passed** |

### Movement Mode Breakdown

| Movement Mode | Observation Count | Percentage | Speed & Interval Rule |
| :--- | :--- | :--- | :--- |
| `pedestrian_walking` | `4,289` | $42.89\%$ | $0.8\text{--}1.6\text{ m/s}$ ($\Delta t \in [15, 60]\text{ s}$) |
| `stationary_dwell` | `3,928` | $39.28\%$ | $\le 0.3\text{ m/s}$ ($\Delta t \in [180, 600]\text{ s}$) |
| `vehicular_transit` | `1,783` | $17.83\%$ | $3.5\text{--}14.0\text{ m/s}$ ($\Delta t \in [15, 60]\text{ s}$) |
| **Total** | **10,000** | **100.00%** | — |

---

## 4. Kinematic and Movement Logic Architecture

The generator models continuous spatial journeys rather than isolated, uncorrelated random points:

1. **Complete Window Adherence**:
   - For every device, its complete trajectory duration is planned prior to simulation, and its start time $T_0$ is chosen such that $T_0 + \sum \Delta t \le T_\text{window\_end}$.
   - Every observation generated strictly falls inside one of the four approved Day 1 windows, with zero points spilling into non-activity gap intervals.
2. **Spherical Great-Circle Distance Conversions**:
   - Distance checks and velocity calculations utilize the Haversine formula, which calculates the **spherical great-circle distance** assuming a mean Earth radius of 6,371,000 meters. (Note: this is a spherical approximation, not an exact ellipsoidal geodesic calculation).
3. **Predictive Inward Steering at Boundaries**:
   - When an agent trajectory step would overshoot a bounding perimeter, heading vectors are predictively reflected inward before displacement, maintaining the exact step distance $d = v \times \Delta t$ and preserving physical speed without step truncation or artificial coordinate jumps.
4. **Variable Sampling Intervals**:
   - Active transit pings are logged every **15 to 60 seconds** to trace street corridors.
   - Dwell pings are recorded every **3 to 10 minutes** to capture sustained retail presence without data bloat.

---

## 5. Reproducibility Verification

Reproducibility was verified by executing independent generator instances using the identical seed (`42`):
- **Run A SHA-256**: `a5070e6a0d94ba6ddc23151564c42d1365d0453cde83c55513f4cda14a124aa8`
- **Run B SHA-256**: `a5070e6a0d94ba6ddc23151564c42d1365d0453cde83c55513f4cda14a124aa8`
- **Result**: **Bitwise identical match (`True`)**.

---

## 6. Known Limitations

- **Simplified Road Topology**: The simulation uses directional persistence and predictive inward boundary steering rather than snapping coordinates to OpenStreetMap (OSM) graph edges (planned for subsequent GIS refinement).
- **Planar Representation**: Elevation and indoor multi-story floor traversal inside malls are not modeled.
- **Simulation Scale**: 10,000 pings across 500 devices was chosen for fast local iteration and unit testing.

---

## 7. How to Reproduce This Dataset

To regenerate this dataset deterministically from the repository root:

```powershell
python -m src.data_generator --records 10000 --devices 500 --seed 42 --date 2026-11-16 --output data/raw/synthetic_mobility_pings.csv --overwrite
```

To run the automated test suite:

```powershell
python -m unittest discover -s tests -v
```
