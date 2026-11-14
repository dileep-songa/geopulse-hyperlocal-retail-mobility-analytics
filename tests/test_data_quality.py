"""Unit and integration tests for the GeoPulse data quality and validation engine.

Tests all structural, record integrity, geographic, temporal, and mobility kinematic
validation rules against both valid synthetic data and controlled invalid fixtures.
"""

from __future__ import annotations

import csv
from datetime import datetime, time, timedelta, timezone
import hashlib
import math
from pathlib import Path
import tempfile
from typing import Any, Dict, List, Sequence
import unittest
import uuid

from src.config import (
    DATA_PROVENANCE_TAG,
    STUDY_AREA,
    BoundingBox,
    MovementMode,
)
from src.data_generator import (
    CSV_COLUMNS,
    get_local_timezone,
    haversine_distance_meters,
)
from src.data_quality import (
    VALID_MOVEMENT_TRANSITIONS,
    ValidationFailure,
    classify_diurnal_window,
    compute_file_sha256,
    format_validation_report,
    get_diurnal_window_boundaries,
    load_and_validate_csv_structure,
    parse_iso8601_utc,
    validate_dataset,
    validate_geography,
    validate_mobility,
    validate_record_integrity,
    validate_temporal,
)


def _make_valid_ping(
    ping_id: str | None = None,
    device_id: str | None = None,
    event_timestamp: str = "2026-11-16T03:00:00.000Z",
    latitude: str = "12.975000",
    longitude: str = "77.640000",
    accuracy_meters: str = "10.0",
    movement_mode: str = MovementMode.PEDESTRIAN_WALKING.value,
    data_provenance: str = DATA_PROVENANCE_TAG,
) -> Dict[str, str]:
    """Helper to construct a valid dictionary record adhering to Day 1 specification."""
    return {
        "ping_id": ping_id or str(uuid.uuid4()),
        "device_id": device_id or str(uuid.uuid4()),
        "event_timestamp": event_timestamp,
        "latitude": latitude,
        "longitude": longitude,
        "accuracy_meters": accuracy_meters,
        "movement_mode": movement_mode,
        "data_provenance": data_provenance,
    }


class TestDataQualityValidator(unittest.TestCase):
    """Test suite for data quality, schema integrity, and kinematic validation."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_dir_path = Path(self.temp_dir.name)
        self.local_tz = get_local_timezone()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _write_csv(
        self,
        rows: Sequence[Dict[str, Any]],
        columns: Sequence[str] = CSV_COLUMNS,
        filename: str = "test_pings.csv",
    ) -> Path:
        """Write a sequence of dictionary rows to a temporary CSV file."""
        file_path = self.temp_dir_path / filename
        with open(file_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(columns))
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
        return file_path

    def _generate_valid_small_trajectory(
        self, sim_date_str: str = "2026-11-16"
    ) -> List[Dict[str, str]]:
        """Construct a minimal valid multi-step trajectory covering transit and dwell on a given date."""
        dev_id = str(uuid.uuid4())
        year, month, day = map(int, sim_date_str.split("-"))
        # 08:30 IST = 03:00 UTC
        base_t = datetime(year, month, day, 3, 0, 0, tzinfo=timezone.utc)

        # Step 0: walking
        p0 = _make_valid_ping(
            device_id=dev_id,
            event_timestamp=base_t.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            latitude="12.975000",
            longitude="77.640000",
            movement_mode=MovementMode.PEDESTRIAN_WALKING.value,
        )
        # Step 1: walking 30 seconds later (speed = 1.0 m/s -> dist = 30m ~ 0.00027 deg lat)
        t1 = base_t + timedelta(seconds=30)
        p1 = _make_valid_ping(
            device_id=dev_id,
            event_timestamp=t1.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            latitude="12.975270",
            longitude="77.640000",
            movement_mode=MovementMode.PEDESTRIAN_WALKING.value,
        )
        # Step 2: arrival at cafe, dwell for 300s (5 min), small jitter displacement (1.5m)
        t2 = t1 + timedelta(seconds=300)
        p2 = _make_valid_ping(
            device_id=dev_id,
            event_timestamp=t2.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            latitude="12.975280",
            longitude="77.640010",
            movement_mode=MovementMode.STATIONARY_DWELL.value,
        )
        # Step 3: continue dwell for 240s, jitter displacement
        t3 = t2 + timedelta(seconds=240)
        p3 = _make_valid_ping(
            device_id=dev_id,
            event_timestamp=t3.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            latitude="12.975275",
            longitude="77.640005",
            movement_mode=MovementMode.STATIONARY_DWELL.value,
        )
        # Step 4: departure to vehicle 20s later (speed = 10 m/s -> dist = 200m ~ 0.0018 deg lat)
        t4 = t3 + timedelta(seconds=20)
        p4 = _make_valid_ping(
            device_id=dev_id,
            event_timestamp=t4.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            latitude="12.977075",
            longitude="77.640005",
            movement_mode=MovementMode.VEHICULAR_TRANSIT.value,
        )

        return [p0, p1, p2, p3, p4]

    # --- Structural Validation Tests ---

    def test_valid_dataset_passes_all_checks(self) -> None:
        """Verify that a compliant synthetic trajectory dataset passes 100% of checks."""
        rows = self._generate_valid_small_trajectory()
        csv_file = self._write_csv(rows)

        result = validate_dataset(csv_file)
        self.assertTrue(result.is_valid, f"Validation failed unexpectedly: {result.failures}")
        self.assertEqual(len(result.failures), 0)
        self.assertEqual(result.total_records, 5)
        self.assertIsNotNone(result.metrics)
        self.assertEqual(result.metrics.unique_devices, 1)

    def test_missing_or_blank_values_detected(self) -> None:
        """Verify that missing, empty, or whitespace-only field values are caught."""
        rows = self._generate_valid_small_trajectory()
        rows[1]["latitude"] = ""
        rows[2]["movement_mode"] = "   "
        csv_file = self._write_csv(rows)

        result = validate_dataset(csv_file)
        self.assertFalse(result.is_valid)
        missing_failures = [f for f in result.failures if f.check_name == "missing_value"]
        self.assertGreaterEqual(len(missing_failures), 2)

    def test_missing_csv_header_fails_structurally(self) -> None:
        """Verify that an empty file missing headers fails structurally and aborts downstream checks."""
        file_path = self.temp_dir_path / "empty.csv"
        file_path.write_text("", encoding="utf-8")

        result = validate_dataset(file_path)
        self.assertFalse(result.is_valid)
        self.assertEqual(result.failures[0].check_name, "empty_file")
        self.assertIsNone(result.metrics)

    def test_duplicate_csv_headers_fail_structurally(self) -> None:
        """Verify that duplicate headers fail structurally and prevent misleading semantic errors."""
        file_path = self.temp_dir_path / "dup_header.csv"
        # Duplicate 'latitude'
        header = "ping_id,device_id,event_timestamp,latitude,latitude,accuracy_meters,movement_mode,data_provenance\n"
        row = f"{uuid.uuid4()},{uuid.uuid4()},2026-11-16T03:00:00.000Z,12.975000,12.975000,10.0,pedestrian_walking,geopulse_synthetic_v1\n"
        file_path.write_text(header + row, encoding="utf-8")

        result = validate_dataset(file_path)
        self.assertFalse(result.is_valid)
        self.assertEqual(result.failures[0].check_name, "duplicate_column_headers")
        self.assertIsNone(result.metrics)

    def test_reordered_csv_headers_fail_structurally(self) -> None:
        """Verify that reordered columns fail structurally without misinterpreting columns."""
        file_path = self.temp_dir_path / "reordered.csv"
        # Swapped latitude and longitude
        reordered = [
            "ping_id", "device_id", "event_timestamp",
            "longitude", "latitude",  # SWAPPED!
            "accuracy_meters", "movement_mode", "data_provenance"
        ]
        rows = self._generate_valid_small_trajectory()
        self._write_csv(rows, columns=reordered, filename="reordered.csv")

        result = validate_dataset(file_path)
        self.assertFalse(result.is_valid)
        self.assertEqual(result.failures[0].check_name, "column_order_mismatch")
        self.assertIsNone(result.metrics)

    def test_unexpected_or_missing_columns_fail_structurally(self) -> None:
        """Verify that extra or missing columns are caught at the structural level."""
        file_path = self.temp_dir_path / "bad_cols.csv"
        bad_cols = ["ping_id", "device_id", "event_timestamp", "extra_col"]
        file_path.write_text(",".join(bad_cols) + "\n1,2,3,4\n", encoding="utf-8")

        result = validate_dataset(file_path)
        self.assertFalse(result.is_valid)
        self.assertEqual(result.failures[0].check_name, "column_set_mismatch")
        self.assertIsNone(result.metrics)

    # --- Record Integrity Tests ---

    def test_duplicate_ping_ids_detected(self) -> None:
        """Verify that non-unique ping IDs across records are flagged as violations."""
        rows = self._generate_valid_small_trajectory()
        duplicate_id = rows[0]["ping_id"]
        rows[2]["ping_id"] = duplicate_id
        csv_file = self._write_csv(rows)

        result = validate_dataset(csv_file)
        self.assertFalse(result.is_valid)
        dup_failures = [f for f in result.failures if f.check_name == "ping_id_uniqueness"]
        self.assertEqual(len(dup_failures), 1)
        self.assertEqual(dup_failures[0].value, duplicate_id)

    def test_invalid_uuidv4_format_detected(self) -> None:
        """Verify that malformed or non-UUIDv4 identifiers are flagged."""
        rows = self._generate_valid_small_trajectory()
        rows[0]["ping_id"] = "not-a-valid-uuid"
        rows[1]["device_id"] = "12345-abcde"
        csv_file = self._write_csv(rows)

        result = validate_dataset(csv_file)
        self.assertFalse(result.is_valid)
        uuid_failures = [f for f in result.failures if "uuidv4_format" in f.check_name]
        self.assertEqual(len(uuid_failures), 2)

    def test_expected_device_count_validation(self) -> None:
        """Verify that explicit expected device count is verified accurately."""
        rows = self._generate_valid_small_trajectory()  # 1 unique device
        csv_file = self._write_csv(rows)

        # Expected 1 device -> PASS
        result_pass = validate_dataset(csv_file, expected_devices=1)
        self.assertTrue(result_pass.is_valid)

        # Expected 500 devices -> FAIL with clear message
        result_fail = validate_dataset(csv_file, expected_devices=500)
        self.assertFalse(result_fail.is_valid)
        dev_fails = [f for f in result_fail.failures if f.check_name == "expected_device_count"]
        self.assertEqual(len(dev_fails), 1)

    def test_invalid_movement_mode_enum_detected(self) -> None:
        """Verify that invalid movement mode strings are rejected."""
        rows = self._generate_valid_small_trajectory()
        rows[0]["movement_mode"] = "flying_drone"
        csv_file = self._write_csv(rows)

        result = validate_dataset(csv_file)
        self.assertFalse(result.is_valid)
        mode_fails = [f for f in result.failures if f.check_name == "movement_mode_enum"]
        self.assertEqual(len(mode_fails), 1)

    # --- Geographic Validation Tests ---

    def test_invalid_and_out_of_bounds_coordinates_detected(self) -> None:
        """Verify that out-of-bounds and unparseable coordinates trigger failures."""
        rows = self._generate_valid_small_trajectory()
        rows[0]["latitude"] = "13.050000"  # North of 12.9900 bounds
        rows[1]["longitude"] = "77.600000"  # West of 77.6250 bounds
        rows[2]["latitude"] = "invalid_lat"  # Non-numeric
        csv_file = self._write_csv(rows)

        result = validate_dataset(csv_file)
        self.assertFalse(result.is_valid)
        out_of_bounds = [f for f in result.failures if f.check_name == "study_area_containment"]
        numeric_fails = [f for f in result.failures if f.check_name == "latitude_numeric"]
        self.assertEqual(len(out_of_bounds), 2)
        self.assertEqual(len(numeric_fails), 1)

    def test_nan_and_infinity_in_numeric_fields_rejected(self) -> None:
        """Verify that NaN and Infinity in latitude, longitude, or accuracy are rejected."""
        rows = self._generate_valid_small_trajectory()
        rows[0]["latitude"] = "NaN"
        rows[1]["longitude"] = "Infinity"
        rows[2]["accuracy_meters"] = "-inf"
        csv_file = self._write_csv(rows)

        result = validate_dataset(csv_file)
        self.assertFalse(result.is_valid)
        nan_fails = [f for f in result.failures if "_finite" in f.check_name]
        self.assertEqual(len(nan_fails), 3)

    def test_coordinate_precision_format_enforcement(self) -> None:
        """Verify coordinates without exact 6-decimal places are flagged."""
        rows = self._generate_valid_small_trajectory()
        rows[0]["latitude"] = "12.975"  # 3 decimal places
        rows[1]["longitude"] = "77.6400001"  # 7 decimal places
        csv_file = self._write_csv(rows)

        result = validate_dataset(csv_file)
        self.assertFalse(result.is_valid)
        precision_fails = [f for f in result.failures if "precision" in f.check_name]
        self.assertEqual(len(precision_fails), 2)

    # --- Temporal & Activity Window Tests ---

    def test_dynamic_simulation_dates_supported(self) -> None:
        """Verify that activity windows work on any valid calendar date, not just 2026-11-16."""
        # Generate valid trajectory on a different date: 2027-03-25
        rows = self._generate_valid_small_trajectory(sim_date_str="2027-03-25")
        csv_file = self._write_csv(rows)

        # Validate with explicit date
        result_explicit = validate_dataset(csv_file, simulation_date="2027-03-25")
        self.assertTrue(result_explicit.is_valid, f"Failures: {result_explicit.failures}")

        # Validate with auto-detected date
        result_autodetect = validate_dataset(csv_file)
        self.assertTrue(result_autodetect.is_valid, f"Failures: {result_autodetect.failures}")
        self.assertEqual(result_autodetect.metrics.base_simulation_date, "2027-03-25")

    def test_nighttime_window_crossing_midnight_verified(self) -> None:
        """Verify nighttime window (22:00 IST on date D to 07:00 IST on date D+1) passes without gap error."""
        dev_id = str(uuid.uuid4())
        # Date: 2026-11-16
        # 23:58:00 IST on 2026-11-16 = 18:28:00 UTC (pre-midnight)
        t_pre = datetime(2026, 11, 16, 18, 28, 0, tzinfo=timezone.utc)
        # 00:03:00 IST on 2026-11-17 = 18:33:00 UTC (post-midnight, 300s later)
        t_post = datetime(2026, 11, 16, 18, 33, 0, tzinfo=timezone.utc)

        p0 = _make_valid_ping(
            device_id=dev_id,
            event_timestamp=t_pre.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            latitude="12.975000",
            longitude="77.640000",
            movement_mode=MovementMode.STATIONARY_DWELL.value,
        )
        p1 = _make_valid_ping(
            device_id=dev_id,
            event_timestamp=t_post.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            latitude="12.975010",
            longitude="77.640005",
            movement_mode=MovementMode.STATIONARY_DWELL.value,
        )
        csv_file = self._write_csv([p0, p1])

        result = validate_dataset(csv_file, simulation_date="2026-11-16")
        self.assertTrue(result.is_valid, f"Failures: {result.failures}")
        self.assertEqual(result.metrics.diurnal_counts["nighttime_low"], 2)
        self.assertEqual(result.metrics.diurnal_counts["unassigned_gap"], 0)

    def test_exact_activity_window_boundaries(self) -> None:
        """Verify exact inclusive boundaries (08:00, 11:00, 11:30, 15:30, 17:00, 21:30, 22:00, 07:00) and gaps."""
        base_d = datetime(2026, 11, 16).date()
        next_d = base_d + timedelta(days=1)

        # Inclusive points (must pass)
        inclusive_pts = [
            datetime.combine(base_d, time(8, 0, 0), tzinfo=self.local_tz),   # Morning start
            datetime.combine(base_d, time(11, 0, 0), tzinfo=self.local_tz),  # Morning end
            datetime.combine(base_d, time(11, 30, 0), tzinfo=self.local_tz), # Midday start
            datetime.combine(base_d, time(15, 30, 0), tzinfo=self.local_tz), # Midday end
            datetime.combine(base_d, time(17, 0, 0), tzinfo=self.local_tz),  # Evening start
            datetime.combine(base_d, time(21, 30, 0), tzinfo=self.local_tz), # Evening end
            datetime.combine(base_d, time(22, 0, 0), tzinfo=self.local_tz),  # Night start
            datetime.combine(next_d, time(7, 0, 0), tzinfo=self.local_tz),   # Night end
        ]
        for pt in inclusive_pts:
            w_name, gap = classify_diurnal_window(pt, base_d, self.local_tz)
            self.assertIsNotNone(w_name, f"Boundary point {pt} failed: {gap}")

        # Gap points (must be rejected)
        gap_pts = [
            datetime.combine(base_d, time(11, 0, 1), tzinfo=self.local_tz),   # Just past morning end
            datetime.combine(base_d, time(11, 29, 59), tzinfo=self.local_tz), # Just before midday start
            datetime.combine(base_d, time(15, 30, 1), tzinfo=self.local_tz),  # Just past midday end
            datetime.combine(base_d, time(16, 59, 59), tzinfo=self.local_tz), # Just before evening start
            datetime.combine(base_d, time(21, 30, 1), tzinfo=self.local_tz),  # Just past evening end
            datetime.combine(base_d, time(21, 59, 59), tzinfo=self.local_tz), # Just before night start
            datetime.combine(next_d, time(7, 0, 1), tzinfo=self.local_tz),    # Just past night end
            datetime.combine(next_d, time(7, 59, 59), tzinfo=self.local_tz),  # Just before morning next day
        ]
        for pt in gap_pts:
            w_name, gap = classify_diurnal_window(pt, base_d, self.local_tz)
            self.assertIsNone(w_name, f"Gap point {pt} incorrectly passed into {w_name}")
            self.assertIsNotNone(gap)

    def test_out_of_order_csv_device_chronology_detected(self) -> None:
        """Verify that out-of-order records in CSV sequence are reported without sorting."""
        rows = self._generate_valid_small_trajectory()
        # Invert rows 1 and 2 in CSV appearance
        rows[1], rows[2] = rows[2], rows[1]
        csv_file = self._write_csv(rows)

        result = validate_dataset(csv_file)
        self.assertFalse(result.is_valid)
        chrono_fails = [f for f in result.failures if f.check_name == "device_chronology_monotonic"]
        self.assertGreaterEqual(len(chrono_fails), 1)

    def test_duplicate_device_timestamps_detected(self) -> None:
        """Verify that identical timestamps for a single device are flagged as violations."""
        rows = self._generate_valid_small_trajectory()
        rows[1]["event_timestamp"] = rows[0]["event_timestamp"]
        csv_file = self._write_csv(rows)

        result = validate_dataset(csv_file)
        self.assertFalse(result.is_valid)
        chrono_fails = [f for f in result.failures if f.check_name == "device_chronology_monotonic"]
        self.assertGreaterEqual(len(chrono_fails), 1)

    # --- Mobility Kinematic & Transition Tests ---

    def test_unsupported_movement_transitions_rejected(self) -> None:
        """Verify that direct walking->vehicle or vehicle->walking transitions without dwell are rejected."""
        dev_id = str(uuid.uuid4())
        base_t = datetime(2026, 11, 16, 3, 0, 0, tzinfo=timezone.utc)

        # Step 0: walking
        p0 = _make_valid_ping(
            device_id=dev_id,
            event_timestamp=base_t.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            movement_mode=MovementMode.PEDESTRIAN_WALKING.value,
        )
        # Step 1: direct vehicle transition (unsupported without dwell stop)
        t1 = base_t + timedelta(seconds=30)
        p1 = _make_valid_ping(
            device_id=dev_id,
            event_timestamp=t1.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            movement_mode=MovementMode.VEHICULAR_TRANSIT.value,
        )
        csv_file = self._write_csv([p0, p1])

        result = validate_dataset(csv_file)
        self.assertFalse(result.is_valid)
        unsupported = [f for f in result.failures if f.check_name == "unsupported_movement_transition"]
        self.assertEqual(len(unsupported), 1)
        self.assertIn("pedestrian_walking", unsupported[0].message)
        self.assertIn("vehicular_transit", unsupported[0].message)

    def test_both_supported_arrival_transitions(self) -> None:
        """Verify that walking->dwell and vehicle->dwell arrivals are explicitly evaluated and pass."""
        dev_id = str(uuid.uuid4())
        base_t = datetime(2026, 11, 16, 3, 0, 0, tzinfo=timezone.utc)

        # 1. Walking -> Dwell arrival
        p0 = _make_valid_ping(
            device_id=dev_id,
            event_timestamp=base_t.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            latitude="12.975000",
            longitude="77.640000",
            movement_mode=MovementMode.PEDESTRIAN_WALKING.value,
        )
        t1 = base_t + timedelta(seconds=200)
        p1 = _make_valid_ping(
            device_id=dev_id,
            event_timestamp=t1.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            latitude="12.975010",
            longitude="77.640005",
            movement_mode=MovementMode.STATIONARY_DWELL.value,
        )
        # 2. Dwell -> Vehicle departure
        t2 = t1 + timedelta(seconds=30)
        p2 = _make_valid_ping(
            device_id=dev_id,
            event_timestamp=t2.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            latitude="12.976800",
            longitude="77.640005",
            movement_mode=MovementMode.VEHICULAR_TRANSIT.value,
        )
        # 3. Vehicle -> Dwell arrival
        t3 = t2 + timedelta(seconds=250)
        p3 = _make_valid_ping(
            device_id=dev_id,
            event_timestamp=t3.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            latitude="12.976810",
            longitude="77.640010",
            movement_mode=MovementMode.STATIONARY_DWELL.value,
        )
        csv_file = self._write_csv([p0, p1, p2, p3])

        result = validate_dataset(csv_file)
        self.assertTrue(result.is_valid, f"Failures: {result.failures}")
        self.assertIn("pedestrian_walking -> stationary_dwell", result.metrics.pair_transition_counts)
        self.assertIn("vehicular_transit -> stationary_dwell", result.metrics.pair_transition_counts)

    def test_both_supported_departure_transitions(self) -> None:
        """Verify that dwell->walking and dwell->vehicle departures are explicitly evaluated and pass."""
        dev_id = str(uuid.uuid4())
        base_t = datetime(2026, 11, 16, 3, 0, 0, tzinfo=timezone.utc)

        # Dwell ping
        p0 = _make_valid_ping(
            device_id=dev_id,
            event_timestamp=base_t.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            latitude="12.975000",
            longitude="77.640000",
            movement_mode=MovementMode.STATIONARY_DWELL.value,
        )
        # Departure to walk (speed ~ 1.0 m/s)
        t1 = base_t + timedelta(seconds=30)
        p1 = _make_valid_ping(
            device_id=dev_id,
            event_timestamp=t1.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            latitude="12.975270",
            longitude="77.640000",
            movement_mode=MovementMode.PEDESTRIAN_WALKING.value,
        )
        csv_file = self._write_csv([p0, p1])

        result = validate_dataset(csv_file)
        self.assertTrue(result.is_valid, f"Failures: {result.failures}")
        self.assertIn("stationary_dwell -> pedestrian_walking", result.metrics.pair_transition_counts)

    def test_speed_limit_violations_detected(self) -> None:
        """Verify excessive speed conditions are caught."""
        rows = self._generate_valid_small_trajectory()
        # Teleport step 1 coordinate 1 km away in 30 seconds
        rows[1]["latitude"] = "12.984000"
        csv_file = self._write_csv(rows)

        result = validate_dataset(csv_file)
        self.assertFalse(result.is_valid)
        speed_fails = [f for f in result.failures if "speed" in f.check_name]
        self.assertGreaterEqual(len(speed_fails), 1)

    # --- Summary Metrics & Non-Mutation Tests ---

    def test_invalid_records_excluded_from_descriptive_summaries(self) -> None:
        """Verify malformed records do not contaminate valid measurement counts or spatial bounds."""
        rows = self._generate_valid_small_trajectory()
        # Add a malformed row with invalid latitude
        malformed = _make_valid_ping(latitude="not_a_num")
        rows.append(malformed)
        csv_file = self._write_csv(rows)

        result = validate_dataset(csv_file)
        self.assertFalse(result.is_valid)
        self.assertEqual(result.metrics.total_records, 6)
        self.assertEqual(result.metrics.valid_measurement_records, 5)
        self.assertEqual(result.metrics.malformed_records, 1)

    def test_malformed_csv_quoting_handled_safely(self) -> None:
        """Verify that a CSV with corrupt or unclosed quotes is handled gracefully without crashing."""
        file_path = self.temp_dir_path / "corrupt_quote.csv"
        file_path.write_text(
            'ping_id,device_id,event_timestamp,latitude,longitude,accuracy_meters,movement_mode,data_provenance\n"unclosed_quote,1,2,3,4,5,6,7\n',
            encoding="utf-8",
        )
        result = validate_dataset(file_path)
        self.assertFalse(result.is_valid)
        self.assertTrue(any(f.category == "structural" for f in result.failures))

    def test_strict_utc_iso8601_parser(self) -> None:
        """Verify standalone strict ISO-8601 parser enforces 'Z' suffix and timezone-awareness."""
        valid_ts = "2026-11-16T03:00:00.000Z"
        dt = parse_iso8601_utc(valid_ts)
        self.assertIsNotNone(dt)
        self.assertIsNotNone(dt.tzinfo)
        self.assertEqual(dt.tzinfo, timezone.utc)

        self.assertIsNone(parse_iso8601_utc("2026-11-16T03:00:00.000"))
        self.assertIsNone(parse_iso8601_utc("2026-11-16 03:00:00"))
        self.assertIsNone(parse_iso8601_utc("2026-11-16T08:30:00+05:30"))
        self.assertIsNone(parse_iso8601_utc("invalid-timestamp"))
        self.assertIsNone(parse_iso8601_utc(""))

    def test_spatial_quadrants_sum_to_valid_records(self) -> None:
        """Verify spatial quadrants are mutually exclusive and sum exactly to valid measurement count."""
        rows = self._generate_valid_small_trajectory()
        csv_file = self._write_csv(rows)
        result = validate_dataset(csv_file)
        self.assertTrue(result.is_valid)
        self.assertIsNotNone(result.metrics)
        quad_sum = sum(result.metrics.quadrant_counts.values())
        self.assertEqual(quad_sum, result.metrics.valid_measurement_records)
        pct_sum = sum(result.metrics.quadrant_percentages.values())
        self.assertAlmostEqual(pct_sum, 100.0, places=3)

    def test_consecutive_pair_count_and_no_double_counting(self) -> None:
        """Verify eligible consecutive pairs are counted exactly once with no double-counting."""
        rows = self._generate_valid_small_trajectory()
        csv_file = self._write_csv(rows)
        result = validate_dataset(csv_file)
        self.assertTrue(result.is_valid)
        self.assertIsNotNone(result.metrics)
        self.assertEqual(result.metrics.consecutive_pairs_evaluated, 4)
        total_transitions = sum(result.metrics.pair_transition_counts.values())
        self.assertEqual(total_transitions, 4)

    def test_input_dataset_is_never_modified(self) -> None:
        """Verify that running the validator guarantees read-only access and never alters input."""
        rows = self._generate_valid_small_trajectory()
        csv_file = self._write_csv(rows)

        hash_before = compute_file_sha256(csv_file)
        size_before = csv_file.stat().st_size

        result1 = validate_dataset(csv_file)
        result2 = validate_dataset(csv_file)
        _ = format_validation_report(result1)

        hash_after = compute_file_sha256(csv_file)
        size_after = csv_file.stat().st_size

        self.assertEqual(hash_before, hash_after)
        self.assertEqual(size_before, size_after)
        self.assertTrue(result1.is_valid)
        self.assertTrue(result2.is_valid)

    def test_empty_csv_with_header_only_is_rejected(self) -> None:
        """Verify that a CSV containing only a valid header and zero observations is rejected."""
        file_path = self.temp_dir_path / "header_only.csv"
        file_path.write_text(",".join(CSV_COLUMNS) + "\n", encoding="utf-8")

        result = validate_dataset(file_path)
        self.assertFalse(result.is_valid)
        self.assertTrue(any(f.check_name == "zero_records" for f in result.failures))
        self.assertIsNone(result.metrics)

    def test_strict_csv_reader_error_handling(self) -> None:
        """Verify strict CSV reader catches unclosed quotes, blank rows, and incorrect field counts."""
        # 1. Blank row in CSV
        blank_row_file = self.temp_dir_path / "blank_row.csv"
        rows = self._generate_valid_small_trajectory()
        csv_content = ",".join(CSV_COLUMNS) + "\n\n"
        for r in rows:
            csv_content += ",".join(str(r[c]) for c in CSV_COLUMNS) + "\n"
        blank_row_file.write_text(csv_content, encoding="utf-8")

        result_blank = validate_dataset(blank_row_file)
        self.assertFalse(result_blank.is_valid)
        self.assertTrue(any(f.check_name == "blank_record" for f in result_blank.failures))

        # 2. Row with incorrect field count (7 fields instead of 8)
        bad_field_file = self.temp_dir_path / "bad_fields.csv"
        bad_content = ",".join(CSV_COLUMNS) + "\n" + "val1,val2,val3,val4,val5,val6,val7\n"
        bad_field_file.write_text(bad_content, encoding="utf-8")

        result_bad = validate_dataset(bad_field_file)
        self.assertFalse(result_bad.is_valid)
        self.assertTrue(any(f.check_name == "row_field_count" for f in result_bad.failures))

    def test_valid_measurement_records_strict_definition(self) -> None:
        """Verify invalid precision, global limits, duplicate ping IDs, invalid timestamps, and out-of-calendar records are excluded from valid records."""
        base_rows = self._generate_valid_small_trajectory()

        # Row with invalid precision (5 decimals)
        bad_prec = _make_valid_ping(latitude="12.97500")
        # Row with global lat out of bounds
        bad_global = _make_valid_ping(latitude="95.000000")
        # Row with duplicate ping_id (duplicate of base_rows[0])
        dup_ping = _make_valid_ping(ping_id=base_rows[0]["ping_id"])
        # Row with invalid timestamp format
        bad_ts = _make_valid_ping(event_timestamp="2026-11-16 03:00:00")
        # Row with out-of-calendar timestamp (11:15 IST gap)
        t_gap = datetime(2026, 11, 16, 5, 45, 0, tzinfo=timezone.utc)
        bad_cal = _make_valid_ping(event_timestamp=t_gap.strftime("%Y-%m-%dT%H:%M:%S.000Z"))

        all_rows = base_rows + [bad_prec, bad_global, dup_ping, bad_ts, bad_cal]
        csv_file = self._write_csv(all_rows)

        result = validate_dataset(csv_file, simulation_date="2026-11-16")
        self.assertFalse(result.is_valid)
        self.assertIsNotNone(result.metrics)
        self.assertEqual(result.metrics.total_records, 10)
        self.assertEqual(result.metrics.valid_measurement_records, 4)
        self.assertEqual(result.metrics.malformed_records, 6)

    def test_temporal_calendar_exclusion_from_valid_measurement_records(self) -> None:
        """Verify records outside the configured simulation calendar or in excluded diurnal gaps are excluded from valid measurement records."""
        base_rows = self._generate_valid_small_trajectory()
        # Create a record falling in the morning-to-midday gap: 11:15 IST = 05:45 UTC
        t_gap = datetime(2026, 11, 16, 5, 45, 0, tzinfo=timezone.utc)
        gap_record = _make_valid_ping(
            event_timestamp=t_gap.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        )
        # Create a record on D+2: outside simulation calendar
        t_future = datetime(2026, 11, 18, 4, 0, 0, tzinfo=timezone.utc)
        future_record = _make_valid_ping(
            event_timestamp=t_future.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        )
        all_rows = base_rows + [gap_record, future_record]
        csv_file = self._write_csv(all_rows)

        result = validate_dataset(csv_file, simulation_date="2026-11-16")
        self.assertFalse(result.is_valid)
        self.assertIsNotNone(result.metrics)
        self.assertEqual(result.metrics.total_records, 7)
        self.assertEqual(result.metrics.valid_measurement_records, 5)
        self.assertEqual(result.metrics.malformed_records, 2)

    def test_coordinate_precision_exclusion_from_mobility_pair_evaluation(self) -> None:
        """Verify coordinates with invalid precision do not qualify adjacent pairs for mobility evaluation."""
        dev_id = str(uuid.uuid4())
        base_t = datetime(2026, 11, 16, 3, 0, 0, tzinfo=timezone.utc)
        # 3-step trajectory for a single device
        p0 = _make_valid_ping(
            device_id=dev_id,
            event_timestamp=base_t.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            latitude="12.975000",
            longitude="77.640000",
            movement_mode=MovementMode.PEDESTRIAN_WALKING.value,
        )
        # p1 has invalid 5-decimal precision
        t1 = base_t + timedelta(seconds=30)
        p1 = _make_valid_ping(
            device_id=dev_id,
            event_timestamp=t1.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            latitude="12.97527",  # 5 decimals instead of 6
            longitude="77.640000",
            movement_mode=MovementMode.PEDESTRIAN_WALKING.value,
        )
        t2 = t1 + timedelta(seconds=30)
        p2 = _make_valid_ping(
            device_id=dev_id,
            event_timestamp=t2.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            latitude="12.975540",
            longitude="77.640000",
            movement_mode=MovementMode.PEDESTRIAN_WALKING.value,
        )
        csv_file = self._write_csv([p0, p1, p2])

        result = validate_dataset(csv_file)
        self.assertFalse(result.is_valid)
        self.assertIsNotNone(result.metrics)
        # Pairs (p0, p1) and (p1, p2) both involve p1 which has invalid precision (is_valid_coord=False).
        # Thus, zero eligible consecutive pairs qualify for mobility evaluation.
        self.assertEqual(result.metrics.consecutive_pairs_evaluated, 0)
        # Confirm precision failure is recorded
        prec_fails = [f for f in result.failures if "precision" in f.check_name]
        self.assertEqual(len(prec_fails), 1)

    def test_observations_outside_simulation_calendar_rejected(self) -> None:
        """Verify observations on subsequent date D+1 outside nighttime window (e.g. 10:00 IST) are rejected."""
        dev_id = str(uuid.uuid4())
        # 10:00 IST on 2026-11-17 = 04:30 UTC on 2026-11-17
        t_late = datetime(2026, 11, 17, 4, 30, 0, tzinfo=timezone.utc)
        p_late = _make_valid_ping(
            device_id=dev_id,
            event_timestamp=t_late.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        )
        csv_file = self._write_csv([p_late])

        result = validate_dataset(csv_file, simulation_date="2026-11-16")
        self.assertFalse(result.is_valid)
        gap_fails = [f for f in result.failures if f.check_name == "activity_window_compliance"]
        self.assertEqual(len(gap_fails), 1)
        self.assertIn("Outside simulation calendar", gap_fails[0].message)

    def test_maximum_displacement_checked_on_unsupported_transitions(self) -> None:
        """Verify maximum single-step displacement (>850m) is checked even when the transition is unsupported."""
        dev_id = str(uuid.uuid4())
        base_t = datetime(2026, 11, 16, 3, 0, 0, tzinfo=timezone.utc)
        p0 = _make_valid_ping(
            device_id=dev_id,
            event_timestamp=base_t.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            latitude="12.966000",
            longitude="77.626000",
            movement_mode=MovementMode.PEDESTRIAN_WALKING.value,
        )
        t1 = base_t + timedelta(seconds=30)
        p1 = _make_valid_ping(
            device_id=dev_id,
            event_timestamp=t1.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            latitude="12.988000",
            longitude="77.645000",
            movement_mode=MovementMode.VEHICULAR_TRANSIT.value,
        )
        csv_file = self._write_csv([p0, p1])

        result = validate_dataset(csv_file)
        self.assertFalse(result.is_valid)
        unsupported = [f for f in result.failures if f.check_name == "unsupported_movement_transition"]
        teleport = [f for f in result.failures if f.check_name == "maximum_step_displacement"]
        self.assertEqual(len(unsupported), 1)
        self.assertEqual(len(teleport), 1)

    def test_actual_synthetic_dataset_if_present(self) -> None:
        """If data/raw/synthetic_mobility_pings.csv is present, verify it passes 100% of checks."""
        dataset_path = Path("data/raw/synthetic_mobility_pings.csv")
        if not dataset_path.exists():
            self.skipTest("Local dataset data/raw/synthetic_mobility_pings.csv not found.")

        result = validate_dataset(
            dataset_path,
            simulation_date="2026-11-16",
            expected_devices=500,
        )
        self.assertTrue(
            result.is_valid,
            f"Validation failed with {len(result.failures)} errors: {result.failures[:5]}",
        )
        self.assertEqual(result.total_records, 10000)
        self.assertIsNotNone(result.metrics)
        self.assertEqual(result.metrics.valid_measurement_records, 10000)
        self.assertEqual(result.metrics.malformed_records, 0)
        self.assertEqual(result.metrics.unique_devices, 500)
        self.assertEqual(result.metrics.unique_ping_ids, 10000)
        self.assertEqual(result.metrics.consecutive_pairs_evaluated, 9500)


if __name__ == "__main__":
    unittest.main()
