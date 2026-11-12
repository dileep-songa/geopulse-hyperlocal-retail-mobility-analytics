"""Automated unit tests for GeoPulse synthetic GPS data generator."""

import csv
from datetime import datetime, timezone
from pathlib import Path
import re
import tempfile
import unittest

from src.config import (
    DATA_PROVENANCE_TAG,
    STUDY_AREA,
    MovementMode,
)
from src.data_generator import (
    CSV_COLUMNS,
    SyntheticGPSGenerator,
    get_local_timezone,
    haversine_distance_meters,
)


class TestSyntheticGPSGenerator(unittest.TestCase):
    """Test suite validating synthetic GPS generator logic, schema, and kinematics."""

    def setUp(self) -> None:
        """Initialize generator with fixed seed for test reproducibility."""
        self.generator = SyntheticGPSGenerator(seed=42)
        self.local_tz = get_local_timezone()

    def test_exact_requested_record_count(self) -> None:
        """Verify generator returns exactly the requested record count."""
        records = self.generator.generate_observations(total_records=100, total_devices=10)
        self.assertEqual(len(records), 100)

        small_records = self.generator.generate_observations(total_records=25, total_devices=5)
        self.assertEqual(len(small_records), 25)

    def test_canonical_csv_column_order(self) -> None:
        """Verify written CSV has exact canonical headers and column ordering."""
        records = self.generator.generate_observations(total_records=20, total_devices=4)
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test_output.csv"
            self.generator.write_csv(records, csv_path)

            with open(csv_path, mode="r", newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                header = next(reader)
                self.assertEqual(tuple(header), CSV_COLUMNS)

    def test_unique_ping_ids(self) -> None:
        """Verify all ping_id values are strictly unique across generated observations."""
        records = self.generator.generate_observations(total_records=250, total_devices=20)
        ping_ids = [r["ping_id"] for r in records]
        self.assertEqual(len(ping_ids), len(set(ping_ids)))

    def test_device_id_reuse_across_records(self) -> None:
        """Verify synthetic device IDs are reused across sequential observations."""
        total_records = 300
        total_devices = 15
        records = self.generator.generate_observations(
            total_records=total_records, total_devices=total_devices
        )

        unique_devices = {r["device_id"] for r in records}
        self.assertEqual(len(unique_devices), total_devices)

        device_counts = {}
        for r in records:
            dev = r["device_id"]
            device_counts[dev] = device_counts.get(dev, 0) + 1

        for dev, count in device_counts.items():
            self.assertGreaterEqual(count, 1, f"Device {dev} has no observations")

    def test_data_provenance_consistency(self) -> None:
        """Verify all records carry the exact approved data provenance tag."""
        records = self.generator.generate_observations(total_records=50, total_devices=5)
        for r in records:
            self.assertEqual(r["data_provenance"], DATA_PROVENANCE_TAG)

    def test_valid_movement_modes(self) -> None:
        """Verify all movement_mode values adhere to the approved enum values."""
        allowed_modes = {mode.value for mode in MovementMode}
        records = self.generator.generate_observations(total_records=150, total_devices=10)
        for r in records:
            self.assertIn(r["movement_mode"], allowed_modes)

    def test_coordinate_bounds_enforcement(self) -> None:
        """Verify all generated coordinates fall strictly inside the study area bounds."""
        bounds = STUDY_AREA.bounds
        records = self.generator.generate_observations(total_records=500, total_devices=25)

        for r in records:
            lat = float(r["latitude"])
            lon = float(r["longitude"])

            self.assertGreaterEqual(
                lat,
                bounds.min_latitude,
                f"Latitude {lat} is below min bound {bounds.min_latitude}",
            )
            self.assertLessEqual(
                lat,
                bounds.max_latitude,
                f"Latitude {lat} is above max bound {bounds.max_latitude}",
            )
            self.assertGreaterEqual(
                lon,
                bounds.min_longitude,
                f"Longitude {lon} is below min bound {bounds.min_longitude}",
            )
            self.assertLessEqual(
                lon,
                bounds.max_longitude,
                f"Longitude {lon} is above max bound {bounds.max_longitude}",
            )

    def test_coordinate_precision_format(self) -> None:
        """Verify coordinates are formatted with exactly 6 decimal places."""
        records = self.generator.generate_observations(total_records=50, total_devices=5)
        pattern = re.compile(r"^\d+\.\d{6}$")
        for r in records:
            self.assertTrue(pattern.match(r["latitude"]), f"Latitude {r['latitude']} not 6 decimals")
            self.assertTrue(pattern.match(r["longitude"]), f"Longitude {r['longitude']} not 6 decimals")

    def test_accuracy_meters_range(self) -> None:
        """Verify simulated accuracy falls within the allowed 2.0 to 35.0 range."""
        records = self.generator.generate_observations(total_records=300, total_devices=15)
        for r in records:
            acc = float(r["accuracy_meters"])
            self.assertGreaterEqual(acc, 2.0, f"Accuracy {acc} is below minimum 2.0m")
            self.assertLessEqual(acc, 35.0, f"Accuracy {acc} exceeds maximum 35.0m")

    def test_utc_iso8601_timestamp_format(self) -> None:
        """Verify timestamps are formatted as ISO-8601 UTC with 'Z' suffix."""
        iso_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
        records = self.generator.generate_observations(total_records=50, total_devices=5)
        for r in records:
            self.assertTrue(
                iso_pattern.match(r["event_timestamp"]),
                f"Timestamp {r['event_timestamp']} does not match UTC ISO-8601 format",
            )

    def test_per_device_chronological_ordering(self) -> None:
        """Verify timestamps for any single synthetic device strictly increase."""
        records = self.generator.generate_observations(total_records=400, total_devices=20)

        device_timestamps = {}
        for r in records:
            dev = r["device_id"]
            ts = datetime.fromisoformat(r["event_timestamp"].replace("Z", "+00:00"))
            device_timestamps.setdefault(dev, []).append(ts)

        for dev, timestamps in device_timestamps.items():
            for i in range(1, len(timestamps)):
                self.assertGreater(
                    timestamps[i],
                    timestamps[i - 1],
                    f"Timestamp sequence for device {dev} not strictly increasing at index {i}",
                )

    def test_deterministic_reproducibility(self) -> None:
        """Verify identical seed produces identical observations and CSV output."""
        gen1 = SyntheticGPSGenerator(seed=42)
        gen2 = SyntheticGPSGenerator(seed=42)

        records1 = gen1.generate_observations(total_records=100, total_devices=10)
        records2 = gen2.generate_observations(total_records=100, total_devices=10)

        self.assertEqual(records1, records2)

    def test_divergence_with_different_seeds(self) -> None:
        """Verify different seeds produce divergent trajectories and ping IDs."""
        gen1 = SyntheticGPSGenerator(seed=42)
        gen2 = SyntheticGPSGenerator(seed=99)

        records1 = gen1.generate_observations(total_records=50, total_devices=5)
        records2 = gen2.generate_observations(total_records=50, total_devices=5)

        self.assertNotEqual(records1, records2)

    def test_invalid_configuration_handling(self) -> None:
        """Verify generator raises ValueError for invalid input configurations."""
        with self.assertRaises(ValueError):
            self.generator.generate_observations(total_records=0, total_devices=10)

        with self.assertRaises(ValueError):
            self.generator.generate_observations(total_records=-100, total_devices=10)

        with self.assertRaises(ValueError):
            self.generator.generate_observations(total_records=50, total_devices=0)

        with self.assertRaises(ValueError):
            self.generator.generate_observations(total_records=10, total_devices=50)

    def test_file_exists_overwrite_protection(self) -> None:
        """Verify write_csv raises FileExistsError unless overwrite=True is passed."""
        records = self.generator.generate_observations(total_records=10, total_devices=2)

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "output.csv"
            self.generator.write_csv(records, csv_path)
            self.assertTrue(csv_path.exists())

            with self.assertRaises(FileExistsError):
                self.generator.write_csv(records, csv_path, overwrite=False)

            self.generator.write_csv(records, csv_path, overwrite=True)
            self.assertTrue(csv_path.exists())

    def test_exact_day1_activity_windows_and_midnight_crossing(self) -> None:
        """Verify EVERY observation timestamp strictly adheres to Day 1 windows including midnight crossing."""
        # Generate enough records to thoroughly test window adherence across all observations
        records = self.generator.generate_observations(total_records=1000, total_devices=100, sim_date="2026-11-16")

        night_crossed_midnight = False
        for r in records:
            utc_ts = datetime.fromisoformat(r["event_timestamp"].replace("Z", "+00:00"))
            ist_ts = utc_ts.astimezone(self.local_tz)
            hour = ist_ts.hour
            minute = ist_ts.minute
            day = ist_ts.day

            # Check adherence to one of the 4 exact Day 1 windows:
            # 1. Morning Commute: 08:00 - 11:00 IST
            in_morning = (8 <= hour < 11) and (day == 16)

            # 2. Midday Work/Lunch: 11:30 - 15:30 IST
            in_midday = (
                ((hour == 11 and minute >= 30) or (12 <= hour < 15) or (hour == 15 and minute < 30))
                and (day == 16)
            )

            # 3. Evening Social Peak: 17:00 - 21:30 IST
            in_evening = (
                ((17 <= hour < 21) or (hour == 21 and minute < 30))
                and (day == 16)
            )

            # 4. Nighttime Low: 22:00 - 07:00 IST (crossing midnight into day 17)
            in_night_pre_midnight = (22 <= hour <= 23) and (day == 16)
            in_night_post_midnight = (0 <= hour < 7) and (day == 17)
            in_night = in_night_pre_midnight or in_night_post_midnight

            if in_night_post_midnight:
                night_crossed_midnight = True

            # Assert observation is inside an allowed window
            self.assertTrue(
                in_morning or in_midday or in_evening or in_night,
                f"Observation {r['ping_id']} fell in excluded window: {ist_ts.strftime('%Y-%m-%d %H:%M:%S IST')}",
            )

            # Explicitly assert observation does NOT fall in excluded gap windows
            is_in_morning_midday_gap = (hour == 11 and minute < 30) and (day == 16)
            is_in_midday_evening_gap = ((hour == 15 and minute >= 30) or (hour == 16)) and (day == 16)
            is_in_evening_night_gap = ((hour == 21 and minute >= 30)) and (day == 16)
            is_in_morning_pre_gap = (hour >= 7 and hour < 8) and (day == 17)

            self.assertFalse(is_in_morning_midday_gap, f"Spilled into Morning-Midday gap: {ist_ts}")
            self.assertFalse(is_in_midday_evening_gap, f"Spilled into Midday-Evening gap: {ist_ts}")
            self.assertFalse(is_in_evening_night_gap, f"Spilled into Evening-Night gap: {ist_ts}")
            self.assertFalse(is_in_morning_pre_gap, f"Spilled past night end at 07:00: {ist_ts}")

        # Confirm that the midnight crossing actually occurred for the nighttime window
        self.assertTrue(night_crossed_midnight, "Nighttime window did not cross midnight into day 17")

    def test_movement_speed_and_distance_consistency(self) -> None:
        """Verify movement speeds strictly adhere to Day 1 approved ranges (walking: 0.8-1.6 m/s, vehicle: 3.5-14.0 m/s)."""
        records = self.generator.generate_observations(total_records=1000, total_devices=40)

        by_device = {}
        for r in records:
            by_device.setdefault(r["device_id"], []).append(r)

        for dev, pings in by_device.items():
            for i in range(1, len(pings)):
                prev = pings[i - 1]
                curr = pings[i]

                t_prev = datetime.fromisoformat(prev["event_timestamp"].replace("Z", "+00:00"))
                t_curr = datetime.fromisoformat(curr["event_timestamp"].replace("Z", "+00:00"))
                dt_seconds = (t_curr - t_prev).total_seconds()

                dist_meters = haversine_distance_meters(
                    float(prev["latitude"]), float(prev["longitude"]),
                    float(curr["latitude"]), float(curr["longitude"]),
                )
                speed_mps = dist_meters / dt_seconds

                prev_mode = prev["movement_mode"]
                curr_mode = curr["movement_mode"]

                # When both observations are walking, verify exact 0.8 to 1.6 m/s speed limit
                # (allowing 0.02 m/s buffer for 6-decimal geodetic coordinate rounding)
                if prev_mode == MovementMode.PEDESTRIAN_WALKING.value and curr_mode == MovementMode.PEDESTRIAN_WALKING.value:
                    self.assertGreaterEqual(speed_mps, 0.78, f"Walking speed {speed_mps:.3f} m/s < 0.8 m/s")
                    self.assertLessEqual(speed_mps, 1.62, f"Walking speed {speed_mps:.3f} m/s > 1.6 m/s")

                # When both observations are vehicular, verify exact 3.5 to 14.0 m/s speed limit
                # (allowing 0.05 m/s buffer for 6-decimal geodetic coordinate rounding)
                elif prev_mode == MovementMode.VEHICULAR_TRANSIT.value and curr_mode == MovementMode.VEHICULAR_TRANSIT.value:
                    self.assertGreaterEqual(speed_mps, 3.45, f"Vehicle speed {speed_mps:.3f} m/s < 3.5 m/s")
                    self.assertLessEqual(speed_mps, 14.05, f"Vehicle speed {speed_mps:.3f} m/s > 14.0 m/s")

                # Absolute bound: no single step can ever exceed vehicle max distance (14.0 m/s * 60s = 840m)
                self.assertLessEqual(dist_meters, 850.0, f"Physically impossible jump: {dist_meters:.1f}m in {dt_seconds}s")

    def test_stationary_dwell_displacement_and_speed(self) -> None:
        """Verify stationary dwell observations exhibit minimal displacement and apparent speed <= 0.3 m/s."""
        records = self.generator.generate_observations(total_records=1000, total_devices=40)

        by_device = {}
        for r in records:
            by_device.setdefault(r["device_id"], []).append(r)

        for dev, pings in by_device.items():
            for i in range(1, len(pings)):
                prev = pings[i - 1]
                curr = pings[i]

                # If both consecutive observations are stationary dwell
                if (
                    prev["movement_mode"] == MovementMode.STATIONARY_DWELL.value
                    and curr["movement_mode"] == MovementMode.STATIONARY_DWELL.value
                ):
                    t_prev = datetime.fromisoformat(prev["event_timestamp"].replace("Z", "+00:00"))
                    t_curr = datetime.fromisoformat(curr["event_timestamp"].replace("Z", "+00:00"))
                    dt_seconds = (t_curr - t_prev).total_seconds()

                    dist_meters = haversine_distance_meters(
                        float(prev["latitude"]), float(prev["longitude"]),
                        float(curr["latitude"]), float(curr["longitude"]),
                    )
                    speed_mps = dist_meters / dt_seconds

                    # Apparent speed must be <= 0.30 m/s as strictly documented in Day 1 specification
                    self.assertLessEqual(
                        speed_mps,
                        0.30,
                        f"Dwell apparent speed {speed_mps:.3f} m/s exceeds 0.30 m/s limit",
                    )
                    # Displacement between consecutive jittered pings must not exceed 7.0 meters
                    self.assertLessEqual(
                        dist_meters,
                        7.0,
                        f"Dwell jitter displacement {dist_meters:.2f}m exceeds 7.0m limit",
                    )

    def test_movement_mode_observation_intervals(self) -> None:
        """Verify observation time deltas strictly follow Day 1 movement interval rules."""
        records = self.generator.generate_observations(total_records=800, total_devices=30)

        by_device = {}
        for r in records:
            by_device.setdefault(r["device_id"], []).append(r)

        for dev, pings in by_device.items():
            for i in range(1, len(pings)):
                prev = pings[i - 1]
                curr = pings[i]

                t_prev = datetime.fromisoformat(prev["event_timestamp"].replace("Z", "+00:00"))
                t_curr = datetime.fromisoformat(curr["event_timestamp"].replace("Z", "+00:00"))
                dt_seconds = (t_curr - t_prev).total_seconds()

                mode = curr["movement_mode"]
                if mode in (MovementMode.PEDESTRIAN_WALKING.value, MovementMode.VEHICULAR_TRANSIT.value):
                    # Active transit: 15 to 60 seconds
                    self.assertGreaterEqual(dt_seconds, 15, f"Transit interval {dt_seconds}s < 15s")
                    self.assertLessEqual(dt_seconds, 60, f"Transit interval {dt_seconds}s > 60s")
                elif mode == MovementMode.STATIONARY_DWELL.value:
                    # Dwell: 3 to 10 minutes (180 to 600 seconds)
                    self.assertGreaterEqual(dt_seconds, 180, f"Dwell interval {dt_seconds}s < 180s")
                    self.assertLessEqual(dt_seconds, 600, f"Dwell interval {dt_seconds}s > 600s")

    def test_predictive_boundary_steering_continuity(self) -> None:
        """Verify predictive inward steering keeps coordinates inside bounds without teleportation."""
        bounds = STUDY_AREA.bounds
        for test_seed in (42, 101, 777):
            gen = SyntheticGPSGenerator(bounds=bounds, seed=test_seed)
            records = gen.generate_observations(total_records=500, total_devices=20)

            for r in records:
                lat = float(r["latitude"])
                lon = float(r["longitude"])
                self.assertTrue(
                    bounds.contains(lon, lat),
                    f"Coordinate ({lon}, {lat}) escaped bounding box",
                )


if __name__ == "__main__":
    unittest.main()
