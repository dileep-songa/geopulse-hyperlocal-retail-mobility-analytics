"""GeoPulse Week 3 temporal mobility, catchment overlap, and cross-store transition engine.

Implements Week 3 advanced spatial and mobility analytics:
- Geodetic (WGS 84 / EPSG:4326) to UTM Zone 43N (EPSG:32643) planar projection.
- Exact regular 32-gon catchment representation matching Apache Sedona ST_Buffer(point, 500, 8).
- Robust point-in-polygon containment matching Sedona ST_Intersects boundary semantics.
- Exact convex polygon-polygon intersection area (Sutherland-Hodgman) for store catchments.
- Pairwise spatial-state classification across unordered store pairs (ORIGIN_ONLY, DEST_ONLY, SHARED_PAIR, OUTSIDE_PAIR).
- Trajectory reconstruction with deterministic sorting and duplicate timestamp handling.
- Two-tier kinematic plausibility filtering:
    * Observation-level: instantaneous teleportation rejection (Δt = 0, Δs > 0) and step speed ceiling (v_step > 15.0 m/s).
    * Hard trajectory breaks: malformed pings and step teleportation anomalies break trajectory continuity and prevent false transitions.
    * Transit-level: inter-catchment transit speed ceiling (v_trans <= 15.0 m/s).
- Directional cross-catchment transitions across all 12 ordered store pairs with windows (15, 30, 60 min).
- Pairwise Cross-Catchment Transition Rate (PCTR) with zero-denominator NULL handling and structural invariant checking.
- Geometric Overlap Ratio (GOR), Device Jaccard Index (DJI), and symmetric Szymkiewicz-Simpson Overlap Coefficient (DOC).
- Store temporal opportunity profiling (08:00-22:00 local time): Peak Hour Index (PHI) with zero-median safety and earliest-hour tie-breaking, and Low-Activity Hour Share (LAHS).
- Hourly shared-device and shared-ping volume cross-tabulation.
- Exact multi-tier numerical precision and deterministic output rounding.

Disclaimer:
Candidate store locations and mobility observations are synthetic constructs.
Catchment overlap and transition metrics measure geographic proximity and trajectory continuity,
not confirmed commercial transactions or physical store cannibalization.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import math
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple
from zoneinfo import ZoneInfo

from src.config import (
    DATA_PROVENANCE_TAG,
    TIMEZONE_LOCAL,
)


# ==============================================================================
# Constants & Configuration
# ==============================================================================

WGS84_A = 6378137.0  # semi-major axis (metres)
WGS84_F = 1.0 / 298.257223563  # flattening
WGS84_E2 = 2.0 * WGS84_F - WGS84_F * WGS84_F  # first eccentricity squared
WGS84_E_PRIME_SQ = WGS84_E2 / (1.0 - WGS84_E2)

UTM_ZONE_43_CENTRAL_MERIDIAN = 75.0  # degrees East
UTM_K0 = 0.9996  # scale factor
UTM_FALSE_EASTING = 500000.0  # metres
UTM_FALSE_NORTHING = 0.0  # metres (Northern Hemisphere)

DEFAULT_CATCHMENT_RADIUS_M = 500.0
CATCHMENT_BUFFER_QUAD_SEGS = 8  # 4 * 8 = 32-gon regular polygon
MAX_KINEMATIC_SPEED_MPS = 15.0  # 54.0 km/h anomaly ceiling

OPERATING_HOURS = tuple(range(8, 22))  # 08:00 to 21:59 (14 hours)
TRANSITION_WINDOWS_MINUTES = (15, 30, 60)


# ==============================================================================
# Numerical Rounding Helpers
# ==============================================================================

def round_decimal(value: Optional[float | Decimal], decimals: int) -> Optional[Decimal]:
    """Deterministically round a numeric value to specified decimal places using ROUND_HALF_UP."""
    if value is None:
        return None
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        val_dec = Decimal(str(value))
    elif isinstance(value, Decimal):
        val_dec = value
    else:
        val_dec = Decimal(str(value))
    
    fmt = "0." + "0" * decimals if decimals > 0 else "0"
    return val_dec.quantize(Decimal(fmt), rounding=ROUND_HALF_UP)


# ==============================================================================
# Geodetic to UTM Zone 43N Coordinate Projection
# ==============================================================================

def wgs84_to_utm43n(longitude: float, latitude: float) -> Tuple[float, float]:
    """Project geodetic coordinates (WGS 84 / EPSG:4326) to UTM Zone 43N (EPSG:32643) in metres.
    
    Implements standard USGS Bulletin 1532 / Redfearn transverse Mercator projection series.
    """
    if not (-180.0 <= longitude <= 180.0 and -90.0 <= latitude <= 90.0):
        raise ValueError(f"Coordinates ({longitude}, {latitude}) out of bounds.")
    
    lat_rad = math.radians(latitude)
    lon_rad = math.radians(longitude)
    lon0_rad = math.radians(UTM_ZONE_43_CENTRAL_MERIDIAN)
    
    sin_lat = math.sin(lat_rad)
    cos_lat = math.cos(lat_rad)
    tan_lat = math.tan(lat_rad)
    
    # Radius of curvature in the prime vertical
    N = WGS84_A / math.sqrt(1.0 - WGS84_E2 * sin_lat * sin_lat)
    T = tan_lat * tan_lat
    C = WGS84_E_PRIME_SQ * cos_lat * cos_lat
    A = (lon_rad - lon0_rad) * cos_lat
    
    # Meridian distance M
    M = WGS84_A * (
        (1.0 - WGS84_E2 / 4.0 - 3.0 * WGS84_E2 * WGS84_E2 / 64.0 - 5.0 * (WGS84_E2 ** 3) / 256.0) * lat_rad
        - (3.0 * WGS84_E2 / 8.0 + 3.0 * WGS84_E2 * WGS84_E2 / 32.0 + 45.0 * (WGS84_E2 ** 3) / 1024.0) * math.sin(2.0 * lat_rad)
        + (15.0 * WGS84_E2 * WGS84_E2 / 256.0 + 45.0 * (WGS84_E2 ** 3) / 1024.0) * math.sin(4.0 * lat_rad)
        - (35.0 * (WGS84_E2 ** 3) / 3072.0) * math.sin(6.0 * lat_rad)
    )
    
    # Easting
    easting = UTM_FALSE_EASTING + UTM_K0 * N * (
        A
        + (1.0 - T + C) * (A ** 3) / 6.0
        + (5.0 - 18.0 * T + T * T + 72.0 * C - 58.0 * WGS84_E_PRIME_SQ) * (A ** 5) / 120.0
    )
    
    # Northing
    northing = UTM_FALSE_NORTHING + UTM_K0 * (
        M
        + N * tan_lat * (
            (A ** 2) / 2.0
            + (5.0 - T + 9.0 * C + 4.0 * C * C) * (A ** 4) / 24.0
            + (61.0 - 58.0 * T + T * T + 600.0 * C - 330.0 * WGS84_E_PRIME_SQ) * (A ** 6) / 720.0
        )
    )
    
    return easting, northing


# ==============================================================================
# Planar Polygon Geometry & Intersection Routines
# ==============================================================================

def generate_regular_32gon_vertices(
    center_easting: float,
    center_northing: float,
    radius: float = DEFAULT_CATCHMENT_RADIUS_M,
) -> List[Tuple[float, float]]:
    """Generate exact 32-gon polygon vertices matching JTS / Apache Sedona ST_Buffer(point, radius, 8).
    
    Starts at (center_x + radius, center_y), proceeds clockwise around the circle,
    and closes with the first vertex (33 coordinates total).
    """
    pts: List[Tuple[float, float]] = []
    n_vertices = 32
    for i in range(n_vertices):
        angle = - (2.0 * math.pi * i) / n_vertices
        x = center_easting + radius * math.cos(angle)
        y = center_northing + radius * math.sin(angle)
        pts.append((x, y))
    pts.append(pts[0])  # Close polygon ring
    return pts


def is_point_on_segment(
    px: float, py: float, x1: float, y1: float, x2: float, y2: float, tol: float = 1e-9
) -> bool:
    """Check if point (px, py) lies on line segment (x1, y1) -> (x2, y2)."""
    # Check bounding box
    if not (min(x1, x2) - tol <= px <= max(x1, x2) + tol and min(y1, y2) - tol <= py <= max(y1, y2) + tol):
        return False
    # Cross product for collinearity
    cross = (px - x1) * (y2 - y1) - (py - y1) * (x2 - x1)
    if abs(cross) > tol:
        return False
    return True


def point_in_polygon_intersects(
    px: float,
    py: float,
    poly: Sequence[Tuple[float, float]],
    boundary_tol: float = 1e-9,
) -> bool:
    """Evaluate point-in-polygon predicate matching Sedona / OGC ST_Intersects semantics.
    
    Returns True if the point is strictly inside the polygon OR on its boundary.
    """
    n = len(poly)
    if n < 3:
        return False
    
    inside = False
    for i in range(n - 1):
        x1, y1 = poly[i]
        x2, y2 = poly[i + 1]
        
        # Boundary check
        if is_point_on_segment(px, py, x1, y1, x2, y2, tol=boundary_tol):
            return True
        
        # Ray casting
        if ((y1 > py) != (y2 > py)):
            x_inters = (py - y1) * (x2 - x1) / (y2 - y1) + x1
            if px < x_inters:
                inside = not inside
                
    return inside


def compute_polygon_area(poly: Sequence[Tuple[float, float]]) -> float:
    """Calculate the planar area of a closed polygon using the shoelace formula."""
    n = len(poly)
    if n < 3:
        return 0.0
    area = 0.0
    for i in range(n - 1):
        area += poly[i][0] * poly[i + 1][1] - poly[i + 1][0] * poly[i][1]
    return abs(area) / 2.0


def ensure_ccw(poly: Sequence[Tuple[float, float]]) -> List[Tuple[float, float]]:
    """Ensure polygon ring vertices are oriented counter-clockwise."""
    clean = list(poly)
    if clean and clean[0] == clean[-1]:
        clean = clean[:-1]
    n = len(clean)
    if n < 3:
        return clean
    signed_area = sum(clean[i][0] * clean[(i + 1) % n][1] - clean[(i + 1) % n][0] * clean[i][1] for i in range(n))
    if signed_area < 0:
        clean = clean[::-1]
    clean.append(clean[0])
    return clean


def clip_polygon_by_line(
    poly: Sequence[Tuple[float, float]],
    p1: Tuple[float, float],
    p2: Tuple[float, float],
) -> List[Tuple[float, float]]:
    """Clip a counter-clockwise polygon by directed line p1 -> p2 (keeps points to the left / on line)."""
    def is_inside(p: Tuple[float, float]) -> bool:
        return (p2[0] - p1[0]) * (p[1] - p1[1]) - (p2[1] - p1[1]) * (p[0] - p1[0]) >= -1e-11

    def line_intersection(s: Tuple[float, float], e: Tuple[float, float]) -> Tuple[float, float]:
        dc = (s[0] - e[0], s[1] - e[1])
        dp = (p1[0] - p2[0], p1[1] - p2[1])
        n1 = s[0] * e[1] - s[1] * e[0]
        n2 = p1[0] * p2[1] - p1[1] * p2[0]
        denom = dc[0] * dp[1] - dc[1] * dp[0]
        if abs(denom) < 1e-14:
            return s
        return ((n1 * dp[0] - n2 * dc[0]) / denom, (n1 * dp[1] - n2 * dc[1]) / denom)

    clean_poly = list(poly)
    if clean_poly and clean_poly[0] == clean_poly[-1]:
        clean_poly = clean_poly[:-1]
    if not clean_poly:
        return []

    clipped: List[Tuple[float, float]] = []
    s = clean_poly[-1]
    for e in clean_poly:
        if is_inside(e):
            if is_inside(s):
                clipped.append(e)
            else:
                clipped.append(line_intersection(s, e))
                clipped.append(e)
        elif is_inside(s):
            clipped.append(line_intersection(s, e))
        s = e
    return clipped


def compute_convex_polygon_intersection(
    poly1: Sequence[Tuple[float, float]],
    poly2: Sequence[Tuple[float, float]],
) -> List[Tuple[float, float]]:
    """Compute exact intersection polygon of two convex polygons using Sutherland-Hodgman clipping."""
    p1_ccw = ensure_ccw(poly1)[:-1]
    p2_ccw = ensure_ccw(poly2)[:-1]
    
    output = list(p1_ccw)
    n2 = len(p2_ccw)
    for i in range(n2):
        edge_start = p2_ccw[i]
        edge_end = p2_ccw[(i + 1) % n2]
        output = clip_polygon_by_line(output, edge_start, edge_end)
        if not output:
            break
    if output:
        output.append(output[0])
    return output


def compute_exact_32gon_intersection_area(
    poly1: Sequence[Tuple[float, float]],
    poly2: Sequence[Tuple[float, float]],
) -> float:
    """Compute exact intersection area in square metres between two regular 32-gon catchment polygons."""
    inter_poly = compute_convex_polygon_intersection(poly1, poly2)
    return compute_polygon_area(inter_poly) if inter_poly else 0.0


# ==============================================================================
# Data Structures
# ==============================================================================

@dataclass
class StoreLocation:
    store_id: str
    store_name: str
    longitude: float
    latitude: float
    easting: float
    northing: float
    radius_meters: float = DEFAULT_CATCHMENT_RADIUS_M
    catchment_polygon: List[Tuple[float, float]] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.catchment_polygon:
            self.catchment_polygon = generate_regular_32gon_vertices(
                self.easting, self.northing, self.radius_meters
            )


@dataclass
class MobilityPing:
    ping_id: str
    device_id: str
    event_timestamp: datetime
    local_timestamp: datetime
    latitude: float
    longitude: float
    easting: float
    northing: float
    movement_mode: str
    data_provenance: str
    intersecting_stores: Set[str] = field(default_factory=set)


@dataclass
class TransitionEvent:
    transition_id: str
    device_id: str
    origin_store_id: str
    destination_store_id: str
    exit_timestamp: datetime
    entry_timestamp: datetime
    duration_minutes: float
    displacement_distance_m: float
    transit_speed_mps: float
    is_kinematically_plausible: bool


# ==============================================================================
# Core Analytic Functions
# ==============================================================================

def parse_iso8601(timestamp_str: str) -> Optional[datetime]:
    """Parse UTC ISO-8601 timestamp string safely."""
    try:
        ts = timestamp_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def classify_pairwise_state(
    ping_store_set: Set[str],
    store_id_a: str,
    store_id_b: str,
) -> str:
    """Classify a ping into one of four mutually exclusive states relative to ordered pair (A, B).
    
    Returns:
        'ORIGIN_ONLY'  if in A and not in B
        'DEST_ONLY'    if in B and not in A
        'SHARED_PAIR'  if in both A and B
        'OUTSIDE_PAIR' if in neither A nor B
    Third store memberships are ignored.
    """
    in_a = store_id_a in ping_store_set
    in_b = store_id_b in ping_store_set
    
    if in_a and not in_b:
        return "ORIGIN_ONLY"
    elif in_b and not in_a:
        return "DEST_ONLY"
    elif in_a and in_b:
        return "SHARED_PAIR"
    else:
        return "OUTSIDE_PAIR"


class TemporalMobilityEngine:
    """GeoPulse Week 3 Python analytical engine."""

    def __init__(
        self,
        stores: Sequence[StoreLocation],
        catchment_radius_m: float = DEFAULT_CATCHMENT_RADIUS_M,
        max_transit_speed_mps: float = MAX_KINEMATIC_SPEED_MPS,
        local_timezone: str = TIMEZONE_LOCAL,
    ) -> None:
        if catchment_radius_m <= 0:
            raise ValueError(f"catchment_radius_m must be strictly positive, got {catchment_radius_m}")
        if max_transit_speed_mps <= 0:
            raise ValueError(f"max_transit_speed_mps must be strictly positive, got {max_transit_speed_mps}")
        self.stores = {s.store_id: s for s in stores}
        self.catchment_radius_m = catchment_radius_m
        self.max_transit_speed_mps = max_transit_speed_mps
        self.local_tz = ZoneInfo(local_timezone)
        
        # Precompute store pairs
        self.sorted_store_ids = sorted(self.stores.keys())
        self.unordered_pairs: List[Tuple[str, str]] = []
        for i in range(len(self.sorted_store_ids)):
            for j in range(i + 1, len(self.sorted_store_ids)):
                self.unordered_pairs.append((self.sorted_store_ids[i], self.sorted_store_ids[j]))
        
        self.directed_pairs: List[Tuple[str, str]] = []
        for a in self.sorted_store_ids:
            for b in self.sorted_store_ids:
                if a != b:
                    self.directed_pairs.append((a, b))

    @classmethod
    def from_store_records(
        cls,
        store_records: Sequence[Dict[str, Any]],
        catchment_radius_m: float = DEFAULT_CATCHMENT_RADIUS_M,
        catchment_wkt_map: Optional[Dict[str, str]] = None,
    ) -> TemporalMobilityEngine:
        """Instantiate engine from raw store location records and optional Sedona catchment WKTs."""
        stores: List[StoreLocation] = []
        for r in store_records:
            s_id = str(r["store_id"])
            lon = float(r["longitude"])
            lat = float(r["latitude"])
            e, n = wgs84_to_utm43n(lon, lat)
            
            # If explicit WKT polygon is supplied (e.g. from Week 2 store_catchments.csv), use it
            poly_vertices: List[Tuple[float, float]] = []
            if catchment_wkt_map and s_id in catchment_wkt_map:
                wkt = catchment_wkt_map[s_id]
                coords_str = wkt.replace("POLYGON ((", "").replace("))", "").split(", ")
                poly_vertices = [tuple(map(float, pt.split())) for pt in coords_str]
            else:
                poly_vertices = generate_regular_32gon_vertices(e, n, catchment_radius_m)
            
            stores.append(
                StoreLocation(
                    store_id=s_id,
                    store_name=str(r.get("store_name", s_id)),
                    longitude=lon,
                    latitude=lat,
                    easting=e,
                    northing=n,
                    radius_meters=catchment_radius_m,
                    catchment_polygon=poly_vertices,
                )
            )
        return cls(stores=stores, catchment_radius_m=catchment_radius_m)

    def load_pings(self, ping_records: Sequence[Dict[str, Any]]) -> List[MobilityPing]:
        """Ingest, project, and evaluate 32-gon catchment polygon intersections for raw pings."""
        pings: List[MobilityPing] = []
        for r in ping_records:
            dt_utc = parse_iso8601(str(r["event_timestamp"]))
            if dt_utc is None:
                continue
            dt_local = dt_utc.astimezone(self.local_tz)
            
            lon = float(r["longitude"])
            lat = float(r["latitude"])
            e, n = wgs84_to_utm43n(lon, lat)
            
            # Exact 32-gon polygon containment join (matching Sedona ST_Intersects)
            intersecting: Set[str] = set()
            for s_id, s in self.stores.items():
                if point_in_polygon_intersects(e, n, s.catchment_polygon):
                    intersecting.add(s_id)
            
            pings.append(
                MobilityPing(
                    ping_id=str(r["ping_id"]),
                    device_id=str(r["device_id"]),
                    event_timestamp=dt_utc,
                    local_timestamp=dt_local,
                    latitude=lat,
                    longitude=lon,
                    easting=e,
                    northing=n,
                    movement_mode=str(r.get("movement_mode", "unknown")),
                    data_provenance=str(r.get("data_provenance", DATA_PROVENANCE_TAG)),
                    intersecting_stores=intersecting,
                )
            )
        return pings

    def extract_transitions_for_pair(
        self,
        pings: Sequence[MobilityPing],
        origin_store_id: str,
        destination_store_id: str,
    ) -> List[TransitionEvent]:
        """Extract directional transitions for an ordered store pair with anomaly sequence breaks."""
        if origin_store_id == destination_store_id:
            raise ValueError("Origin and destination stores must be distinct.")
        if origin_store_id not in self.stores:
            raise ValueError(f"Origin store '{origin_store_id}' not found in registered stores.")
        if destination_store_id not in self.stores:
            raise ValueError(f"Destination store '{destination_store_id}' not found in registered stores.")
        
        # Group pings by device
        devices_pings: Dict[str, List[MobilityPing]] = {}
        for p in pings:
            devices_pings.setdefault(p.device_id, []).append(p)
        
        transitions: List[TransitionEvent] = []
        event_counter = 0

        for dev_id, dev_list in devices_pings.items():
            # Deterministic sorting: timestamp ASC, ping_id ASC
            sorted_pings = sorted(dev_list, key=lambda p: (p.event_timestamp, p.ping_id))
            
            # Step 1: Filter duplicate timestamps & flag step teleportation
            cleaned_pings: List[Tuple[MobilityPing, bool]] = []  # (ping, is_anomaly)
            i = 0
            n = len(sorted_pings)
            
            while i < n:
                curr_p = sorted_pings[i]
                j = i + 1
                
                # Check for simultaneous pings (Δt = 0)
                simultaneous = [curr_p]
                while j < n and sorted_pings[j].event_timestamp == curr_p.event_timestamp:
                    simultaneous.append(sorted_pings[j])
                    j += 1
                
                if len(simultaneous) > 1:
                    # Check coordinates among simultaneous pings
                    first_coord = (simultaneous[0].easting, simultaneous[0].northing)
                    all_same = all(
                        math.isclose(p.easting, first_coord[0], abs_tol=1e-3)
                        and math.isclose(p.northing, first_coord[1], abs_tol=1e-3)
                        for p in simultaneous
                    )
                    if all_same:
                        # Deduplicate: keep first
                        cleaned_pings.append((simultaneous[0], False))
                    else:
                        # Simultaneous distinct coordinates: instantaneous teleportation anomaly
                        for p in simultaneous:
                            cleaned_pings.append((p, True))
                else:
                    cleaned_pings.append((curr_p, False))
                
                i = j

            # Step 2: Observation-level velocity filter (v_step > 15 m/s is anomaly)
            flagged_pings: List[Tuple[MobilityPing, bool]] = []
            for k in range(len(cleaned_pings)):
                p, is_anom = cleaned_pings[k]
                if is_anom:
                    flagged_pings.append((p, True))
                    continue
                
                if k > 0 and not cleaned_pings[k - 1][1]:
                    prev_p = cleaned_pings[k - 1][0]
                    dt_s = (p.event_timestamp - prev_p.event_timestamp).total_seconds()
                    if dt_s > 0:
                        ds_m = math.hypot(p.easting - prev_p.easting, p.northing - prev_p.northing)
                        v_step = ds_m / dt_s
                        if v_step > self.max_transit_speed_mps:
                            flagged_pings.append((p, True))
                            continue
                flagged_pings.append((p, False))

            # Step 3: Transition state machine with anomaly hard breaks
            idx = 0
            total_clean = len(flagged_pings)
            
            while idx < total_clean:
                p, is_anom = flagged_pings[idx]
                if is_anom:
                    idx += 1
                    continue
                
                state = classify_pairwise_state(p.intersecting_stores, origin_store_id, destination_store_id)
                if state != "ORIGIN_ONLY":
                    idx += 1
                    continue
                
                # Active origin episode found: find last consecutive ORIGIN_ONLY ping
                origin_exit_ping = p
                next_idx = idx + 1
                
                while next_idx < total_clean:
                    np, n_anom = flagged_pings[next_idx]
                    if n_anom:
                        # Anomaly encountered inside origin episode -> episode invalid, hard break
                        break
                    n_state = classify_pairwise_state(np.intersecting_stores, origin_store_id, destination_store_id)
                    if n_state == "ORIGIN_ONLY":
                        origin_exit_ping = np
                        next_idx += 1
                    else:
                        break
                
                if next_idx < total_clean and flagged_pings[next_idx][1]:
                    # Hard break: anomaly immediately aborted the episode
                    idx = next_idx + 1
                    continue
                
                # Origin episode closed at origin_exit_ping
                # Scan forward for earliest destination arrival within 60 minutes
                cand_idx = next_idx
                found_transition = False
                
                while cand_idx < total_clean:
                    cp, c_anom = flagged_pings[cand_idx]
                    if c_anom:
                        # Hard break: anomaly encountered during transit resets trajectory
                        break
                    
                    elapsed_min = (cp.event_timestamp - origin_exit_ping.event_timestamp).total_seconds() / 60.0
                    if elapsed_min > 60.0:
                        # Window expired
                        break
                    
                    c_state = classify_pairwise_state(cp.intersecting_stores, origin_store_id, destination_store_id)
                    
                    if c_state == "ORIGIN_ONLY":
                        # Re-entered origin: abort search, start new origin episode here
                        idx = cand_idx
                        found_transition = True
                        break
                    elif c_state == "DEST_ONLY":
                        # Earliest destination arrival!
                        dist_m = math.hypot(
                            cp.easting - origin_exit_ping.easting,
                            cp.northing - origin_exit_ping.northing,
                        )
                        transit_seconds = (cp.event_timestamp - origin_exit_ping.event_timestamp).total_seconds()
                        speed_mps = dist_m / transit_seconds if transit_seconds > 0 else 0.0
                        
                        is_plausible = speed_mps <= self.max_transit_speed_mps
                        
                        if is_plausible:
                            event_counter += 1
                            trans_id = f"TR_{origin_store_id}_{destination_store_id}_{dev_id}_{event_counter:04d}"
                            transitions.append(
                                TransitionEvent(
                                    transition_id=trans_id,
                                    device_id=dev_id,
                                    origin_store_id=origin_store_id,
                                    destination_store_id=destination_store_id,
                                    exit_timestamp=origin_exit_ping.event_timestamp,
                                    entry_timestamp=cp.event_timestamp,
                                    duration_minutes=elapsed_min,
                                    displacement_distance_m=dist_m,
                                    transit_speed_mps=speed_mps,
                                    is_kinematically_plausible=True,
                                )
                            )
                        # Advance pointer to destination arrival ping
                        idx = cand_idx
                        found_transition = True
                        break
                    else:
                        # OUTSIDE_PAIR or SHARED_PAIR: permitted intermediate traversal
                        cand_idx += 1
                
                if not found_transition:
                    idx = next_idx

        return transitions

    def compute_fct_store_pairwise_overlap(
        self,
        pings: Sequence[MobilityPing],
    ) -> List[Dict[str, Any]]:
        """Compute fct_store_pairwise_overlap table (6 symmetric pairs) with exact 32-gon polygon intersection."""
        # Build device catchment sets
        store_devices: Dict[str, Set[str]] = {s_id: set() for s_id in self.sorted_store_ids}
        store_pings: Dict[str, int] = {s_id: 0 for s_id in self.sorted_store_ids}
        
        for p in pings:
            for s_id in p.intersecting_stores:
                store_devices[s_id].add(p.device_id)
                store_pings[s_id] += 1
        
        rows: List[Dict[str, Any]] = []
        
        for a, b in self.unordered_pairs:
            store_a = self.stores[a]
            store_b = self.stores[b]
            
            # Exact 32-gon polygon-polygon intersection area
            inter_area = compute_exact_32gon_intersection_area(
                store_a.catchment_polygon, store_b.catchment_polygon
            )
            area_a = compute_polygon_area(store_a.catchment_polygon)
            area_b = compute_polygon_area(store_b.catchment_polygon)
            union_polygon_area = area_a + area_b - inter_area
            gor = inter_area / union_polygon_area if union_polygon_area > 0 else 0.0
            
            # Shared devices
            devs_a = store_devices[a]
            devs_b = store_devices[b]
            shared_devs = devs_a & devs_b
            union_devs = devs_a | devs_b
            
            dji = len(shared_devs) / len(union_devs) if len(union_devs) > 0 else 0.0
            min_devs = min(len(devs_a), len(devs_b))
            doc = len(shared_devs) / min_devs if min_devs > 0 else 0.0
            
            # Total shared lens pings (pings located inside both catchments)
            shared_lens_pings = sum(1 for p in pings if a in p.intersecting_stores and b in p.intersecting_stores)
            
            # Hourly shared devices across 14 operating hours
            hourly_shared: Dict[int, Set[str]] = {h: set() for h in OPERATING_HOURS}
            for p in pings:
                h = p.local_timestamp.hour
                if h in hourly_shared and a in p.intersecting_stores and b in p.intersecting_stores:
                    hourly_shared[h].add(p.device_id)
            
            hourly_counts = [len(dev_set) for dev_set in hourly_shared.values()]
            mean_hourly_shared = sum(hourly_counts) / len(hourly_counts) if hourly_counts else 0.0
            peak_hourly_shared = max(hourly_counts) if hourly_counts else 0
            
            rows.append({
                "store_id_a": a,
                "store_id_b": b,
                "lens_intersection_area_m2": round_decimal(inter_area, 2),
                "geometric_overlap_ratio": round_decimal(gor, 4),
                "device_jaccard_index": round_decimal(dji, 4),
                "overlap_coefficient": round_decimal(doc, 4),
                "total_shared_lens_pings": shared_lens_pings,
                "mean_hourly_shared_devices": round_decimal(mean_hourly_shared, 2),
                "peak_hourly_shared_devices": peak_hourly_shared,
            })
        
        return rows

    def compute_fct_store_pairwise_hourly_overlap(
        self,
        pings: Sequence[MobilityPing],
    ) -> List[Dict[str, Any]]:
        """Compute fct_store_pairwise_hourly_overlap table (6 pairs x 14 operating hours = 84 rows)."""
        rows: List[Dict[str, Any]] = []
        
        for a, b in self.unordered_pairs:
            # Pre-filter pings in both catchments
            pair_pings = [p for p in pings if a in p.intersecting_stores and b in p.intersecting_stores]
            total_pair_pings = len(pair_pings)
            
            for h in OPERATING_HOURS:
                h_pings = [p for p in pair_pings if p.local_timestamp.hour == h]
                h_devs = len({p.device_id for p in h_pings})
                h_ping_count = len(h_pings)
                diurnal_share = h_ping_count / total_pair_pings if total_pair_pings > 0 else 0.0
                
                rows.append({
                    "store_id_a": a,
                    "store_id_b": b,
                    "hour_of_day": h,
                    "shared_device_count": h_devs,
                    "shared_ping_volume": h_ping_count,
                    "hourly_diurnal_share": round_decimal(diurnal_share, 4),
                })
        
        return rows

    def compute_fct_store_cross_catchment_transitions(
        self,
        pings: Sequence[MobilityPing],
    ) -> List[Dict[str, Any]]:
        """Compute fct_store_cross_catchment_transitions table (12 directed pairs)."""
        # Unique devices per store
        store_devices: Dict[str, Set[str]] = {s_id: set() for s_id in self.sorted_store_ids}
        for p in pings:
            for s_id in p.intersecting_stores:
                store_devices[s_id].add(p.device_id)
        
        rows: List[Dict[str, Any]] = []
        
        for orig, dest in self.directed_pairs:
            orig_dev_count = len(store_devices[orig])
            transitions = self.extract_transitions_for_pair(pings, orig, dest)
            
            # Filter by windows
            u15 = len({t.device_id for t in transitions if t.duration_minutes <= 15.0})
            u30 = len({t.device_id for t in transitions if t.duration_minutes <= 30.0})
            u60 = len({t.device_id for t in transitions if t.duration_minutes <= 60.0})
            
            # Contract Invariant Check
            if u60 > orig_dev_count:
                raise ValueError(
                    f"CONTRACT_INVARIANT_VIOLATION: Transitioning devices ({u60}) exceed origin devices ({orig_dev_count}) for {orig}->{dest}."
                )
            
            pctr15 = round_decimal(u15 / orig_dev_count, 4) if orig_dev_count > 0 else None
            pctr30 = round_decimal(u30 / orig_dev_count, 4) if orig_dev_count > 0 else None
            pctr60 = round_decimal(u60 / orig_dev_count, 4) if orig_dev_count > 0 else None
            
            durations = [t.duration_minutes for t in transitions]
            speeds = [t.transit_speed_mps for t in transitions]
            
            median_dur = round_decimal(float(sorted(durations)[len(durations) // 2]), 2) if durations else None
            mean_speed = round_decimal(sum(speeds) / len(speeds), 2) if speeds else None
            
            rows.append({
                "origin_store_id": orig,
                "destination_store_id": dest,
                "unique_origin_devices": orig_dev_count,
                "unique_transitioning_devices_15m": u15,
                "unique_transitioning_devices_30m": u30,
                "unique_transitioning_devices_60m": u60,
                "pctr_15m": pctr15,
                "pctr_30m": pctr30,
                "pctr_60m": pctr60,
                "total_transition_events": len(transitions),
                "median_transit_minutes": median_dur,
                "mean_transit_speed_mps": mean_speed,
            })
        
        return rows

    def compute_fct_store_temporal_opportunity(
        self,
        pings: Sequence[MobilityPing],
    ) -> List[Dict[str, Any]]:
        """Compute fct_store_temporal_opportunity table (4 stores)."""
        rows: List[Dict[str, Any]] = []
        
        for s_id in self.sorted_store_ids:
            # Filter pings in this store catchment during operating hours (08:00 to 21:59)
            s_pings = [
                p for p in pings
                if s_id in p.intersecting_stores and p.local_timestamp.hour in OPERATING_HOURS
            ]
            total_op_volume = len(s_pings)
            
            # Hourly volumes
            hourly_vols: Dict[int, int] = {h: 0 for h in OPERATING_HOURS}
            for p in s_pings:
                hourly_vols[p.local_timestamp.hour] += 1
            
            vol_values = list(hourly_vols.values())
            sorted_vols = sorted(vol_values)
            
            # Median volume
            n = len(sorted_vols)
            if n % 2 == 1:
                median_vol = float(sorted_vols[n // 2])
            else:
                median_vol = float(sorted_vols[n // 2 - 1] + sorted_vols[n // 2]) / 2.0
            
            # Peak volume and deterministic earliest peak hour tie-breaking
            max_vol = max(vol_values) if vol_values else 0
            peak_hour = None
            for h in OPERATING_HOURS:
                if hourly_vols[h] == max_vol:
                    peak_hour = h
                    break
            
            peak_window = f"{peak_hour:02d}:00" if peak_hour is not None else None
            
            # Zero-median safety
            has_zero_median = math.isclose(median_vol, 0.0)
            if has_zero_median:
                phi = None
            else:
                phi = round_decimal(max_vol / median_vol, 2)
            
            # Low-Activity Hour Share (LAHS) at 50% median
            low_thresh = 0.50 * median_vol
            low_count = sum(1 for v in vol_values if v < low_thresh)
            lahs = round_decimal(low_count / len(OPERATING_HOURS), 4)
            
            rows.append({
                "store_id": s_id,
                "operating_volume": total_op_volume,
                "peak_hour_window": peak_window,
                "peak_hour_volume": max_vol,
                "median_operating_hourly_volume": round_decimal(median_vol, 2),
                "peak_hour_index": phi,
                "has_zero_median_activity": has_zero_median,
                "low_activity_threshold": round_decimal(low_thresh, 2),
                "low_activity_hours_count": low_count,
                "low_activity_hour_share": lahs,
            })
        
        return rows

    # Aliases
    compute_fct_pairwise_overlap = compute_fct_store_pairwise_overlap
    compute_fct_pairwise_hourly_overlap = compute_fct_store_pairwise_hourly_overlap
    compute_fct_cross_catchment_transitions = compute_fct_store_cross_catchment_transitions
    compute_fct_temporal_opportunity = compute_fct_store_temporal_opportunity
