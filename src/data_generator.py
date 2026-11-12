"""GeoPulse synthetic GPS mobility trajectory generator.

Generates realistic, reproducible synthetic GPS mobility telemetry for urban retail
and foot-traffic analytics. Adheres strictly to the schema specification, geographic
boundaries, and kinematic assumptions defined in Week 1 Day 1.

Standard library only: csv, uuid, random, argparse, datetime, zoneinfo, pathlib, math, typing.
"""

from __future__ import annotations

import argparse
import csv
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
import random
from typing import Any, Dict, List, Sequence, Tuple
import uuid
from zoneinfo import ZoneInfo

from src.config import (
    DATA_PROVENANCE_TAG,
    DEFAULT_RANDOM_SEED,
    STUDY_AREA,
    BoundingBox,
    MovementMode,
)

# Mean Earth radius in meters for spherical geodetic conversions (WGS 84 spherical approximation)
EARTH_RADIUS_METERS = 6371000.0

# Canonical CSV column ordering adhering to Day 1 specification
CSV_COLUMNS: Tuple[str, ...] = (
    "ping_id",
    "device_id",
    "event_timestamp",
    "latitude",
    "longitude",
    "accuracy_meters",
    "movement_mode",
    "data_provenance",
)

# Exact Day 1 diurnal activity period configuration in local time (Asia/Kolkata)
# Format: (period_name, start_hour, start_minute, duration_minutes, weight)
DIURNAL_WINDOWS: Tuple[Tuple[str, int, int, int, float], ...] = (
    ("morning_commute", 8, 0, 180, 0.35),       # 08:00 - 11:00 IST (180 min)
    ("midday_work_lunch", 11, 30, 240, 0.25),   # 11:30 - 15:30 IST (240 min)
    ("evening_social_peak", 17, 0, 270, 0.35),  # 17:00 - 21:30 IST (270 min)
    ("nighttime_low", 22, 0, 540, 0.05),        # 22:00 - 07:00 IST (540 min, crossing midnight)
)


def get_local_timezone() -> timezone | ZoneInfo:
    """Return timezone object for Asia/Kolkata (IST), robust across all environments.

    Falls back to a standard-library fixed offset (UTC+05:30) if system tzdata
    is not installed on bare Windows environments.
    """
    try:
        return ZoneInfo("Asia/Kolkata")
    except Exception:
        return timezone(timedelta(hours=5, minutes=30), name="Asia/Kolkata")


def meters_to_degrees_lat(meters: float) -> float:
    """Convert distance in meters to delta degrees latitude on a spherical Earth."""
    return (meters / EARTH_RADIUS_METERS) * (180.0 / math.pi)


def meters_to_degrees_lon(meters: float, latitude: float) -> float:
    """Convert distance in meters to delta degrees longitude accounting for latitude cosine scaling."""
    lat_rad = math.radians(latitude)
    cos_lat = math.cos(lat_rad)
    if cos_lat < 1e-6:
        cos_lat = 1e-6
    return (meters / (EARTH_RADIUS_METERS * cos_lat)) * (180.0 / math.pi)


def haversine_distance_meters(
    lat1: float, lon1: float, lat2: float, lon2: float
) -> float:
    """Calculate spherical great-circle distance in meters between two geodetic coordinates.

    Note:
        Uses the Haversine formula assuming a spherical Earth model with mean radius
        6,371,000 meters. It is a spherical great-circle approximation, not an exact
        ellipsoidal geodesic calculation (such as Vincenty or Karney).
    """
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
    )
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return EARTH_RADIUS_METERS * c


class SyntheticGPSGenerator:
    """Deterministic, kinematic synthetic mobility observation generator."""

    def __init__(
        self,
        bounds: BoundingBox = STUDY_AREA.bounds,
        seed: int = DEFAULT_RANDOM_SEED,
    ) -> None:
        """Initialize generator with spatial boundary envelope and random seed.

        Args:
            bounds: BoundingBox defining the valid spatial envelope.
            seed: Integer random seed ensuring deterministic reproducibility.
        """
        self.bounds = bounds
        self.seed = seed
        self.rng = random.Random(seed)
        self.local_tz = get_local_timezone()

    def _generate_seeded_uuid(self) -> str:
        """Generate a deterministic RFC 4122 UUIDv4 using the seeded PRNG."""
        random_int = self.rng.getrandbits(128)
        return str(uuid.UUID(int=random_int, version=4))

    def _allocate_device_counts(
        self,
        total_records: int,
        total_devices: int,
    ) -> List[int]:
        """Partition total records across devices deterministically.

        Ensures every device receives at least 1 observation, and the sum
        equals exactly total_records.
        """
        if total_records < total_devices:
            raise ValueError(
                f"total_records ({total_records}) cannot be less than total_devices ({total_devices})."
            )

        # Base allocation: 1 record per device
        counts = [1] * total_devices
        remaining = total_records - total_devices

        if remaining > 0:
            # Generate random weights using seeded PRNG to simulate varied device activity
            weights = [self.rng.random() ** 1.5 for _ in range(total_devices)]
            total_weight = sum(weights)

            allocated = 0
            for i in range(total_devices):
                share = int((weights[i] / total_weight) * remaining)
                counts[i] += share
                allocated += share

            # Distribute any rounding remainder
            unallocated = remaining - allocated
            for _ in range(unallocated):
                idx = self.rng.randrange(total_devices)
                counts[idx] += 1

        return counts

    def _plan_trajectory_timestamps(
        self,
        record_count: int,
        sim_date: str,
    ) -> Tuple[List[datetime], List[MovementMode], List[int]]:
        """Pre-plan device modes, time intervals, and timestamps strictly within a Day 1 window.

        Guarantees that every observation timestamp from the first to the last
        remains completely within the assigned Day 1 window, without spilling into gaps.
        """
        year, month, day = map(int, sim_date.split("-"))

        # Select window according to distribution weights
        periods = [p[0] for p in DIURNAL_WINDOWS]
        weights = [p[4] for p in DIURNAL_WINDOWS]
        selected_period = self.rng.choices(periods, weights=weights, k=1)[0]
        window = next(w for w in DIURNAL_WINDOWS if w[0] == selected_period)

        start_hour, start_minute, duration_minutes = window[1], window[2], window[3]
        total_window_seconds = duration_minutes * 60

        base_window_start = datetime(
            year, month, day, start_hour, start_minute, 0, tzinfo=self.local_tz
        )

        # Plan movement mode and delta_seconds for each step
        modes: List[MovementMode] = []
        delta_seconds_list: List[int] = [0]  # First step has 0 offset from t0

        # Initial mode selection
        mode_roll = self.rng.random()
        if mode_roll < 0.50:
            cur_mode = MovementMode.PEDESTRIAN_WALKING
        elif mode_roll < 0.80:
            cur_mode = MovementMode.VEHICULAR_TRANSIT
        else:
            cur_mode = MovementMode.STATIONARY_DWELL
        modes.append(cur_mode)

        for step in range(1, record_count):
            # Mode transitions
            if cur_mode == MovementMode.STATIONARY_DWELL:
                if self.rng.random() < 0.30:
                    cur_mode = (
                        MovementMode.PEDESTRIAN_WALKING
                        if self.rng.random() < 0.70
                        else MovementMode.VEHICULAR_TRANSIT
                    )
            else:
                if self.rng.random() < 0.20:
                    cur_mode = MovementMode.STATIONARY_DWELL

            modes.append(cur_mode)

            # Interval based on Day 1 specification
            if cur_mode == MovementMode.STATIONARY_DWELL:
                dt = self.rng.randint(180, 600)  # 3 to 10 minutes
            else:
                dt = self.rng.randint(15, 60)    # 15 to 60 seconds
            delta_seconds_list.append(dt)

        total_trajectory_span = sum(delta_seconds_list)

        # If trajectory span exceeds window, scale deltas proportionally
        if total_trajectory_span >= total_window_seconds - 60:
            scale_factor = (total_window_seconds - 120) / total_trajectory_span
            delta_seconds_list = [0] + [
                max(
                    15 if modes[i] != MovementMode.STATIONARY_DWELL else 180,
                    int(delta_seconds_list[i] * scale_factor),
                )
                for i in range(1, record_count)
            ]
            total_trajectory_span = sum(delta_seconds_list)

        # Select initial start time so that t_last = t0 + total_trajectory_span < window_end
        max_start_offset = max(0, total_window_seconds - total_trajectory_span - 1)
        start_offset = self.rng.randint(0, max_start_offset)
        microsecond = self.rng.randint(0, 999) * 1000

        t0 = base_window_start + timedelta(
            seconds=start_offset, microseconds=microsecond
        )

        timestamps: List[datetime] = [t0]
        cur_t = t0
        for dt in delta_seconds_list[1:]:
            cur_t = cur_t + timedelta(seconds=dt)
            timestamps.append(cur_t)

        return timestamps, modes, delta_seconds_list

    def _generate_device_trajectory(
        self,
        device_id: str,
        record_count: int,
        sim_date: str,
    ) -> List[Dict[str, Any]]:
        """Generate a coherent sequence of observations for a single synthetic device."""
        observations: List[Dict[str, Any]] = []

        timestamps, modes, deltas = self._plan_trajectory_timestamps(
            record_count=record_count,
            sim_date=sim_date,
        )

        # Start position inside bounding envelope (with safe internal margin)
        margin_x = self.bounds.longitude_span * 0.10
        margin_y = self.bounds.latitude_span * 0.10

        cur_lon = self.rng.uniform(
            self.bounds.min_longitude + margin_x,
            self.bounds.max_longitude - margin_x,
        )
        cur_lat = self.rng.uniform(
            self.bounds.min_latitude + margin_y,
            self.bounds.max_latitude - margin_y,
        )

        heading = self.rng.uniform(0.0, 2.0 * math.pi)
        anchor_lon = cur_lon
        anchor_lat = cur_lat

        for step in range(record_count):
            cur_mode = modes[step]
            cur_time = timestamps[step]

            if step > 0:
                dt_seconds = deltas[step]

                if cur_mode == MovementMode.STATIONARY_DWELL:
                    # Update anchor if just entering dwell
                    if modes[step - 1] != MovementMode.STATIONARY_DWELL:
                        anchor_lon = cur_lon
                        anchor_lat = cur_lat

                    # Realistic sensor jitter around anchor (0.5 to 3.0 meters)
                    # Ensures apparent speed between consecutive dwell pings is <= 0.3 m/s
                    jitter_meters = self.rng.uniform(0.5, 3.0)
                    jitter_angle = self.rng.uniform(0.0, 2.0 * math.pi)
                    d_lon = meters_to_degrees_lon(
                        jitter_meters * math.sin(jitter_angle), anchor_lat
                    )
                    d_lat = meters_to_degrees_lat(
                        jitter_meters * math.cos(jitter_angle)
                    )
                    cur_lon = anchor_lon + d_lon
                    cur_lat = anchor_lat + d_lat
                else:
                    # Target speed strictly adhering to Day 1 approved ranges:
                    # Walking: 0.8 to 1.6 m/s
                    # Vehicle: 3.5 to 14.0 m/s
                    if cur_mode == MovementMode.PEDESTRIAN_WALKING:
                        speed = self.rng.uniform(0.85, 1.55)
                        heading_noise = self.rng.gauss(0.0, 0.12)
                    else:  # VEHICULAR_TRANSIT
                        speed = self.rng.uniform(3.8, 13.5)
                        heading_noise = self.rng.gauss(0.0, 0.06)

                    # Update heading smoothly (directional persistence)
                    heading = (heading + heading_noise) % (2.0 * math.pi)
                    distance_moved = speed * dt_seconds

                    # Project candidate displacement onto geodetic coordinates
                    disp_lon = meters_to_degrees_lon(
                        distance_moved * math.sin(heading), cur_lat
                    )
                    disp_lat = meters_to_degrees_lat(
                        distance_moved * math.cos(heading)
                    )

                    cand_lon = cur_lon + disp_lon
                    cand_lat = cur_lat + disp_lat

                    # Predictive boundary steering: steer inward to preserve exact speed and distance
                    # without folding back or truncating the step vector
                    vx = math.sin(heading)
                    vy = math.cos(heading)
                    steered = False

                    if cand_lon < self.bounds.min_longitude and vx < 0:
                        vx = abs(vx)
                        steered = True
                    elif cand_lon > self.bounds.max_longitude and vx > 0:
                        vx = -abs(vx)
                        steered = True

                    if cand_lat < self.bounds.min_latitude and vy < 0:
                        vy = abs(vy)
                        steered = True
                    elif cand_lat > self.bounds.max_latitude and vy > 0:
                        vy = -abs(vy)
                        steered = True

                    if steered:
                        heading = math.atan2(vx, vy)
                        disp_lon = meters_to_degrees_lon(
                            distance_moved * math.sin(heading), cur_lat
                        )
                        disp_lat = meters_to_degrees_lat(
                            distance_moved * math.cos(heading)
                        )
                        next_lon = cur_lon + disp_lon
                        next_lat = cur_lat + disp_lat
                    else:
                        next_lon = cand_lon
                        next_lat = cand_lat

                    cur_lon = next_lon
                    cur_lat = next_lat

            # Enforce strict clamp to guaranteed bounds
            cur_lon = max(
                self.bounds.min_longitude,
                min(cur_lon, self.bounds.max_longitude),
            )
            cur_lat = max(
                self.bounds.min_latitude,
                min(cur_lat, self.bounds.max_latitude),
            )

            # Simulated horizontal GPS uncertainty (meters) adhering to Day 1 [2.0, 35.0]
            if cur_mode == MovementMode.STATIONARY_DWELL:
                accuracy = round(self.rng.uniform(2.0, 10.0), 1)
            elif cur_mode == MovementMode.PEDESTRIAN_WALKING:
                accuracy = round(self.rng.uniform(4.0, 18.0), 1)
            else:
                accuracy = round(self.rng.uniform(6.0, 35.0), 1)

            # Convert timezone-aware local timestamp to timezone-aware UTC
            utc_time = cur_time.astimezone(timezone.utc)
            utc_iso_string = (
                utc_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            )

            # Assemble observation record adhering to Day 1 schema
            record = {
                "ping_id": self._generate_seeded_uuid(),
                "device_id": device_id,
                "event_timestamp": utc_iso_string,
                "latitude": f"{cur_lat:.6f}",
                "longitude": f"{cur_lon:.6f}",
                "accuracy_meters": f"{accuracy:.1f}",
                "movement_mode": cur_mode.value,
                "data_provenance": DATA_PROVENANCE_TAG,
                # Retain raw datetime for sorting
                "_raw_utc_dt": utc_time,
            }
            observations.append(record)

        return observations

    def generate_observations(
        self,
        total_records: int = 10000,
        total_devices: int = 500,
        sim_date: str = "2026-11-16",
    ) -> List[Dict[str, Any]]:
        """Generate exactly total_records synthetic GPS mobility observations.

        Args:
            total_records: Exact number of observations to generate.
            total_devices: Number of synthetic devices to simulate.
            sim_date: Target simulation calendar date (YYYY-MM-DD).

        Returns:
            List of dictionary records adhering to Day 1 schema.
        """
        if total_records <= 0:
            raise ValueError(
                f"total_records must be a positive integer, got {total_records}."
            )
        if total_devices <= 0:
            raise ValueError(
                f"total_devices must be a positive integer, got {total_devices}."
            )
        if total_devices > total_records:
            raise ValueError(
                f"total_devices ({total_devices}) cannot exceed total_records ({total_records})."
            )

        # Allocate counts per device
        device_counts = self._allocate_device_counts(
            total_records, total_devices
        )

        # Generate unique, deterministic device IDs
        device_ids = [self._generate_seeded_uuid() for _ in range(total_devices)]

        all_records: List[Dict[str, Any]] = []

        # Generate trajectory for each device
        for i, dev_id in enumerate(device_ids):
            dev_records = self._generate_device_trajectory(
                device_id=dev_id,
                record_count=device_counts[i],
                sim_date=sim_date,
            )
            all_records.extend(dev_records)

        # Sort all observations chronologically by UTC timestamp
        # In case of identical timestamps, tie-break by device_id and ping_id
        all_records.sort(
            key=lambda r: (r["_raw_utc_dt"], r["device_id"], r["ping_id"])
        )

        # Remove temporary sorting metadata
        for record in all_records:
            del record["_raw_utc_dt"]

        return all_records

    def write_csv(
        self,
        records: Sequence[Dict[str, Any]],
        output_path: str | Path,
        overwrite: bool = False,
    ) -> Path:
        """Write records to CSV adhering strictly to Day 1 schema column order.

        Args:
            records: Generated list of observation records.
            output_path: Destination filesystem path.
            overwrite: If False, raises FileExistsError if file already exists.

        Returns:
            Resolved Path of the written CSV file.
        """
        path = Path(output_path).resolve()

        if path.exists() and not overwrite:
            raise FileExistsError(
                f"Output file '{path}' already exists. Pass overwrite=True or --overwrite to replace it."
            )

        # Ensure parent directory exists
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=list(CSV_COLUMNS),
                quoting=csv.QUOTE_MINIMAL,
            )
            writer.writeheader()
            for record in records:
                # Ensure only canonical columns are written in exact order
                row = {col: record[col] for col in CSV_COLUMNS}
                writer.writerow(row)

        return path


def parse_args(args: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse and validate command line arguments."""
    parser = argparse.ArgumentParser(
        prog="python -m src.data_generator",
        description="GeoPulse Synthetic GPS Mobility Trajectory Generator.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--records",
        type=int,
        default=10000,
        help="Total number of synthetic GPS observations to generate.",
    )
    parser.add_argument(
        "--devices",
        type=int,
        default=500,
        help="Number of synthetic mobile devices to simulate.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_RANDOM_SEED,
        help="Random seed for deterministic reproducibility.",
    )
    parser.add_argument(
        "--date",
        type=str,
        default="2026-11-16",
        help="Simulation calendar date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/raw/synthetic_mobility_pings.csv"),
        help="Output CSV file path.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow overwriting destination file if it already exists.",
    )

    parsed = parser.parse_args(args)

    # Argument validation
    if parsed.records <= 0:
        parser.error(f"--records must be a positive integer, got {parsed.records}.")
    if parsed.devices <= 0:
        parser.error(f"--devices must be a positive integer, got {parsed.devices}.")
    if parsed.devices > parsed.records:
        parser.error(
            f"--devices ({parsed.devices}) cannot exceed --records ({parsed.records})."
        )

    return parsed


def main() -> None:
    """CLI execution entrypoint."""
    args = parse_args()

    print(
        f"[GeoPulse Generator] Starting generation: {args.records:,} records, "
        f"{args.devices:,} devices, seed={args.seed}, date={args.date}"
    )

    generator = SyntheticGPSGenerator(seed=args.seed)
    records = generator.generate_observations(
        total_records=args.records,
        total_devices=args.devices,
        sim_date=args.date,
    )

    output_path = generator.write_csv(
        records=records,
        output_path=args.output,
        overwrite=args.overwrite,
    )

    print(f"[GeoPulse Generator] Successfully written {len(records):,} records to: {output_path}")


if __name__ == "__main__":
    main()
