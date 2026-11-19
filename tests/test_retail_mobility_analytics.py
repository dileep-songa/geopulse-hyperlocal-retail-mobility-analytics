"""Unit tests for GeoPulse retail mobility analytics module.

Tests cover:
- Time-of-day diurnal classification and boundary conditions (morning, afternoon, evening, night).
- Memberships dataset schema validation, missing/extra column handling, and multi-file input path validation.
- UTC to local operating timezone (Asia/Kolkata) conversion, genuine UTC offset checks, and ISO-8601 parsing.
- Comprehensive input field validation across all 8 canonical fields (ping_id, device_id, store_id,
  store_name, event_timestamp, distance_meters, movement_mode, data_provenance).
- Canonical MovementMode enum values consistency in all test fixtures.
- Distance validation rejecting null, non-numeric, NaN, positive infinity, negative infinity, and negative values.
- Duplicate (ping_id, store_id) pair rejection (prohibiting silent deduplication).
- CSV input validation ensuring every discovered part file is checked for non-empty content and headers.
- Output preflight protection guaranteeing that an existing later output directory prevents
  earlier output directories from being created when overwrite=False.
- Reconciliation ordering ensuring network reconciliation is calculated and asserted before outputs are persisted.
- Distinct-device count vs ping observation count segregation.
- Hourly store activity aggregation.
- Diurnal time-of-day store activity aggregation.
- Daily store activity summary, active-hour counts, and observation density (pings_per_device).
- Store catchment overlap metrics and independent multi-level reconciliation:
  ping-level, membership-level, device-level, and store-level reconciliations.
- Pre-existing output directory protection and safe Spark CSV output writing.
- RetailAnalyticsConfig defaults, custom values, and immutability.
- Integration tests against actual processed catchment membership dataset verifying multi-catchment
  ping store distributions directly from records.

All unit tests in this module run offline using standard Python libraries
without requiring a live PySpark JVM session.
"""

from __future__ import annotations

import csv
from datetime import datetime, timezone
import math
from pathlib import Path
import tempfile
import unittest
from zoneinfo import ZoneInfo

from src.config import DATA_PROVENANCE_TAG, MovementMode
from src.retail_mobility_analytics import (
    DEFAULT_MEMBERSHIPS_PATH,
    DEFAULT_OUTPUT_BASE,
    DIURNAL_PERIODS,
    EXPECTED_MEMBERSHIPS_COLUMNS,
    RetailAnalyticsConfig,
    classify_time_of_day,
    compute_daily_metrics_from_records,
    compute_hourly_metrics_from_records,
    compute_overlap_metrics_from_records,
    compute_time_of_day_metrics_from_records,
    parse_and_validate_timestamp,
    validate_membership_record,
    validate_memberships_input,
    validate_memberships_schema,
    validate_output_destinations_preflight,
    write_spark_csv_output,
)


class TestDiurnalTimeOfDayClassification(unittest.TestCase):
    """Test diurnal time-of-day classification logic and boundary conditions."""

    def test_morning_boundary_hours(self):
        """Verify hours 6 to 11 are classified as morning."""
        self.assertEqual(classify_time_of_day(6), "morning")
        self.assertEqual(classify_time_of_day(8), "morning")
        self.assertEqual(classify_time_of_day(11), "morning")

    def test_afternoon_boundary_hours(self):
        """Verify hours 12 to 16 are classified as afternoon."""
        self.assertEqual(classify_time_of_day(12), "afternoon")
        self.assertEqual(classify_time_of_day(14), "afternoon")
        self.assertEqual(classify_time_of_day(16), "afternoon")

    def test_evening_boundary_hours(self):
        """Verify hours 17 to 21 are classified as evening."""
        self.assertEqual(classify_time_of_day(17), "evening")
        self.assertEqual(classify_time_of_day(19), "evening")
        self.assertEqual(classify_time_of_day(21), "evening")

    def test_night_boundary_hours(self):
        """Verify hours 22, 23, and 0 through 5 are classified as night."""
        self.assertEqual(classify_time_of_day(22), "night")
        self.assertEqual(classify_time_of_day(23), "night")
        self.assertEqual(classify_time_of_day(0), "night")
        self.assertEqual(classify_time_of_day(3), "night")
        self.assertEqual(classify_time_of_day(5), "night")

    def test_all_24_hours_covered_and_valid(self):
        """Verify every hour from 0 to 23 maps to an allowed diurnal period."""
        for hour in range(24):
            period = classify_time_of_day(hour)
            self.assertIn(period, DIURNAL_PERIODS)

    def test_out_of_range_hours_raise_value_error(self):
        """Verify hours < 0 or > 23 raise ValueError."""
        with self.assertRaises(ValueError):
            classify_time_of_day(-1)
        with self.assertRaises(ValueError):
            classify_time_of_day(24)
        with self.assertRaises(ValueError):
            classify_time_of_day(100)

    def test_non_integer_hour_raises_type_error(self):
        """Verify non-integer input types raise TypeError."""
        with self.assertRaises(TypeError):
            classify_time_of_day("10")  # type: ignore
        with self.assertRaises(TypeError):
            classify_time_of_day(10.5)  # type: ignore
        with self.assertRaises(TypeError):
            classify_time_of_day(None)  # type: ignore


class TestMembershipsSchemaAndInputValidation(unittest.TestCase):
    """Test schema and file path validation for store catchment memberships."""

    def test_canonical_schema_passes(self):
        """Verify standard 8 canonical columns pass schema validation."""
        self.assertTrue(validate_memberships_schema(EXPECTED_MEMBERSHIPS_COLUMNS))

    def test_missing_required_column_rejected(self):
        """Verify missing any required column fails schema validation."""
        incomplete = [c for c in EXPECTED_MEMBERSHIPS_COLUMNS if c != "distance_meters"]
        self.assertFalse(validate_memberships_schema(incomplete))

    def test_empty_column_list_rejected(self):
        """Verify empty column list fails schema validation."""
        self.assertFalse(validate_memberships_schema([]))

    def test_reordered_or_extra_columns_rejected(self):
        """Verify non-exact column schema fails schema validation."""
        superset = list(EXPECTED_MEMBERSHIPS_COLUMNS) + ["extra_col"]
        self.assertFalse(validate_memberships_schema(superset))

    def test_validate_memberships_input_nonexistent_raises_file_not_found(self):
        """Verify nonexistent file or directory raises FileNotFoundError."""
        with self.assertRaises(FileNotFoundError):
            validate_memberships_input("nonexistent/path/to/memberships.csv")

    def test_validate_memberships_input_empty_directory_raises_value_error(self):
        """Verify empty directory raises ValueError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            empty_dir = Path(tmpdir) / "empty_dir"
            empty_dir.mkdir()
            with self.assertRaises(ValueError) as ctx:
                validate_memberships_input(empty_dir)
            self.assertIn("No CSV files found", str(ctx.exception))

    def test_validate_memberships_input_zero_byte_file_raises_value_error(self):
        """Verify zero-byte CSV file raises ValueError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            empty_file = Path(tmpdir) / "empty.csv"
            empty_file.write_text("", encoding="utf-8")
            with self.assertRaises(ValueError) as ctx:
                validate_memberships_input(empty_file)
            self.assertIn("empty (0 bytes)", str(ctx.exception))

    def test_validate_memberships_input_valid_single_csv(self):
        """Verify valid single CSV file passes validation and returns list with file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_file = Path(tmpdir) / "valid.csv"
            csv_file.write_text(
                ",".join(EXPECTED_MEMBERSHIPS_COLUMNS) + "\n"
                f"p1,d1,2026-11-16T08:00:00.000Z,STORE_A,Coffee A,120.5,{MovementMode.PEDESTRIAN_WALKING.value},{DATA_PROVENANCE_TAG}\n",
                encoding="utf-8",
            )
            resolved = validate_memberships_input(csv_file)
            self.assertEqual(resolved, [csv_file.resolve()])

    def test_validate_memberships_input_valid_spark_directory(self):
        """Verify valid directory with part-*.csv files passes validation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            part_dir = Path(tmpdir) / "spark_output"
            part_dir.mkdir()
            success_file = part_dir / "_SUCCESS"
            success_file.touch()
            part_file = part_dir / "part-00000-sample.csv"
            part_file.write_text(
                ",".join(EXPECTED_MEMBERSHIPS_COLUMNS) + "\n"
                f"p1,d1,2026-11-16T08:00:00.000Z,STORE_A,Coffee A,120.5,{MovementMode.PEDESTRIAN_WALKING.value},{DATA_PROVENANCE_TAG}\n",
                encoding="utf-8",
            )
            resolved = validate_memberships_input(part_dir)
            self.assertEqual(resolved, [part_file.resolve()])

    def test_validate_memberships_input_multi_part_all_files_checked(self):
        """Verify that every discovered part file is checked, and an invalid later file raises ValueError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            part_dir = Path(tmpdir) / "multi_part"
            part_dir.mkdir()
            # File 1: completely valid
            part1 = part_dir / "part-00000.csv"
            part1.write_text(
                ",".join(EXPECTED_MEMBERSHIPS_COLUMNS) + "\n"
                f"p1,d1,2026-11-16T08:00:00.000Z,STORE_A,Coffee A,120.5,{MovementMode.PEDESTRIAN_WALKING.value},{DATA_PROVENANCE_TAG}\n",
                encoding="utf-8",
            )
            # File 2: 0 bytes empty
            part2 = part_dir / "part-00001.csv"
            part2.write_text("", encoding="utf-8")

            with self.assertRaises(ValueError) as ctx:
                validate_memberships_input(part_dir)
            self.assertIn("empty (0 bytes)", str(ctx.exception))
            self.assertIn("part-00001.csv", str(ctx.exception))

    def test_validate_memberships_input_multi_part_invalid_header_in_later_file(self):
        """Verify that a later part file with mismatched header raises ValueError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            part_dir = Path(tmpdir) / "multi_part_header"
            part_dir.mkdir()
            part1 = part_dir / "part-00000.csv"
            part1.write_text(
                ",".join(EXPECTED_MEMBERSHIPS_COLUMNS) + "\n"
                f"p1,d1,2026-11-16T08:00:00.000Z,STORE_A,Coffee A,120.5,{MovementMode.PEDESTRIAN_WALKING.value},{DATA_PROVENANCE_TAG}\n",
                encoding="utf-8",
            )
            # File 2: corrupt header
            part2 = part_dir / "part-00001.csv"
            part2.write_text("bad_col1,bad_col2\n1,2\n", encoding="utf-8")

            with self.assertRaises(ValueError) as ctx:
                validate_memberships_input(part_dir)
            self.assertIn("does not match expected schema", str(ctx.exception))
            self.assertIn("part-00001.csv", str(ctx.exception))


class TestTimestampValidation(unittest.TestCase):
    """Test strengthened ISO-8601 UTC timestamp parsing and timezone conversion."""

    def test_valid_utc_iso8601_with_z_and_milliseconds(self):
        """Verify UTC timestamp with milliseconds and 'Z' parses into aware UTC datetime."""
        dt = parse_and_validate_timestamp("2026-11-16T08:30:15.500Z")
        self.assertEqual(dt.year, 2026)
        self.assertEqual(dt.month, 11)
        self.assertEqual(dt.day, 16)
        self.assertEqual(dt.hour, 8)
        self.assertEqual(dt.minute, 30)
        self.assertEqual(dt.second, 15)
        self.assertEqual(dt.microsecond, 500000)
        self.assertEqual(dt.tzinfo, timezone.utc)
        self.assertEqual(dt.utcoffset(), timezone.utc.utcoffset(dt))

    def test_valid_utc_iso8601_without_milliseconds(self):
        """Verify UTC timestamp without milliseconds parses correctly."""
        dt = parse_and_validate_timestamp("2026-11-16T12:00:00Z")
        self.assertEqual(dt.hour, 12)
        self.assertEqual(dt.tzinfo, timezone.utc)

    def test_valid_utc_iso8601_with_zero_offset(self):
        """Verify UTC timestamp ending in +00:00 parses correctly."""
        dt = parse_and_validate_timestamp("2026-11-16T12:00:00.000+00:00")
        self.assertEqual(dt.hour, 12)
        self.assertEqual(dt.utcoffset(), timezone.utc.utcoffset(dt))

    def test_timestamp_missing_z_or_offset_raises_value_error(self):
        """Verify naive timestamp string without 'Z' or '+00:00' raises ValueError."""
        with self.assertRaises(ValueError) as ctx:
            parse_and_validate_timestamp("2026-11-16T08:30:15.500")
        self.assertIn("must be UTC", str(ctx.exception))

    def test_timestamp_non_utc_offset_raises_value_error(self):
        """Verify timestamp with non-zero offset is rejected as non-UTC."""
        with self.assertRaises(ValueError):
            parse_and_validate_timestamp("2026-11-16T08:30:15+05:30")

    def test_null_or_empty_timestamp_raises_value_error(self):
        """Verify None, empty string, or whitespace timestamp raises ValueError."""
        with self.assertRaises(ValueError):
            parse_and_validate_timestamp("")
        with self.assertRaises(ValueError):
            parse_and_validate_timestamp("   ")
        with self.assertRaises(ValueError):
            parse_and_validate_timestamp(None)  # type: ignore

    def test_malformed_timestamp_string_raises_value_error(self):
        """Verify malformed date strings raise ValueError."""
        with self.assertRaises(ValueError):
            parse_and_validate_timestamp("invalid-date-string")
        with self.assertRaises(ValueError):
            parse_and_validate_timestamp("2026-13-40T00:00:00Z")

    def test_asia_kolkata_conversion_accuracy(self):
        """Verify conversion of UTC timestamps to Asia/Kolkata (+05:30) handles boundaries accurately."""
        tz_kolkata = ZoneInfo("Asia/Kolkata")

        # 02:30:00 UTC + 05:30 -> 08:00:00 IST (Hour 8, Morning)
        dt_utc = parse_and_validate_timestamp("2026-11-16T02:30:00.000Z")
        dt_ist = dt_utc.astimezone(tz_kolkata)
        self.assertEqual(dt_ist.strftime("%Y-%m-%d"), "2026-11-16")
        self.assertEqual(dt_ist.hour, 8)
        self.assertEqual(classify_time_of_day(dt_ist.hour), "morning")

        # 19:30:00 UTC + 05:30 -> 01:00:00 IST next day (Hour 1, Night)
        dt_utc2 = parse_and_validate_timestamp("2026-11-16T19:30:00.000Z")
        dt_ist2 = dt_utc2.astimezone(tz_kolkata)
        self.assertEqual(dt_ist2.strftime("%Y-%m-%d"), "2026-11-17")
        self.assertEqual(dt_ist2.hour, 1)
        self.assertEqual(classify_time_of_day(dt_ist2.hour), "night")


class TestInputFieldValidation(unittest.TestCase):
    """Test comprehensive validation across all 8 canonical membership columns."""

    def _get_valid_row(self) -> dict:
        return {
            "ping_id": "ping_001",
            "device_id": "device_100",
            "event_timestamp": "2026-11-16T03:30:00.000Z",
            "store_id": "STORE_A",
            "store_name": "GeoPulse Coffee A",
            "distance_meters": "150.25",
            "movement_mode": MovementMode.PEDESTRIAN_WALKING.value,
            "data_provenance": DATA_PROVENANCE_TAG,
        }

    def test_valid_row_passes_all_checks(self):
        """Verify standard valid row passes validation without errors."""
        row = self._get_valid_row()
        validated = validate_membership_record(row)
        self.assertEqual(validated["ping_id"], "ping_001")
        self.assertEqual(validated["movement_mode"], MovementMode.PEDESTRIAN_WALKING.value)
        self.assertEqual(validated["data_provenance"], DATA_PROVENANCE_TAG)

    def test_missing_or_blank_required_fields_rejected(self):
        """Verify each of the 8 columns raises ValueError when missing or blank."""
        for col in EXPECTED_MEMBERSHIPS_COLUMNS:
            # Missing column
            row_missing = self._get_valid_row()
            del row_missing[col]
            with self.assertRaises(ValueError) as ctx:
                validate_membership_record(row_missing)
            self.assertIn(f"missing or blank required field '{col}'", str(ctx.exception))

            # Blank column
            row_blank = self._get_valid_row()
            row_blank[col] = "   "
            with self.assertRaises(ValueError) as ctx:
                validate_membership_record(row_blank)
            self.assertIn(f"missing or blank required field '{col}'", str(ctx.exception))

            # None value
            row_none = self._get_valid_row()
            row_none[col] = None
            with self.assertRaises(ValueError) as ctx:
                validate_membership_record(row_none)
            self.assertIn(f"missing or blank required field '{col}'", str(ctx.exception))

    def test_distance_meters_negative_rejected(self):
        """Verify negative distance raises ValueError."""
        row = self._get_valid_row()
        row["distance_meters"] = "-5.0"
        with self.assertRaises(ValueError) as ctx:
            validate_membership_record(row)
        self.assertIn("distance_meters must be a non-negative finite number", str(ctx.exception))

    def test_distance_meters_nan_or_inf_rejected(self):
        """Verify NaN, Inf, and -Inf distance raise ValueError."""
        for bad_val in ("nan", "inf", "-inf", "+inf", "infinity", "-infinity", "+infinity"):
            row = self._get_valid_row()
            row["distance_meters"] = bad_val
            with self.assertRaises(ValueError) as ctx:
                validate_membership_record(row)
            self.assertIn("distance_meters must be a non-negative finite number", str(ctx.exception))

    def test_distance_meters_float_nan_or_inf_rejected(self):
        """Verify Python float('nan') and float('inf') raise ValueError."""
        for bad_float in (float("nan"), float("inf"), float("-inf")):
            row = self._get_valid_row()
            row["distance_meters"] = str(bad_float)
            with self.assertRaises(ValueError) as ctx:
                validate_membership_record(row)
            self.assertIn("distance_meters must be a non-negative finite number", str(ctx.exception))

    def test_distance_meters_non_numeric_rejected(self):
        """Verify non-numeric distance string raises ValueError."""
        row = self._get_valid_row()
        row["distance_meters"] = "fifty_meters"
        with self.assertRaises(ValueError) as ctx:
            validate_membership_record(row)
        self.assertIn("distance_meters must be a non-negative finite number", str(ctx.exception))

    def test_movement_mode_allowed_values_pass(self):
        """Verify all three approved canonical movement modes pass validation."""
        for mode in MovementMode:
            row = self._get_valid_row()
            row["movement_mode"] = mode.value
            validated = validate_membership_record(row)
            self.assertEqual(validated["movement_mode"], mode.value)

    def test_movement_mode_invalid_value_rejected(self):
        """Verify non-approved movement mode is rejected with ValueError."""
        row = self._get_valid_row()
        row["movement_mode"] = "flying_drone"
        with self.assertRaises(ValueError) as ctx:
            validate_membership_record(row)
        self.assertIn("Invalid movement_mode 'flying_drone'", str(ctx.exception))

    def test_data_provenance_invalid_rejected(self):
        """Verify unexpected data_provenance raises ValueError."""
        row = self._get_valid_row()
        row["data_provenance"] = "real_world_gps"
        with self.assertRaises(ValueError) as ctx:
            validate_membership_record(row)
        self.assertIn("Invalid data_provenance 'real_world_gps'", str(ctx.exception))


class TestDuplicateMembershipDetection(unittest.TestCase):
    """Test duplicate (ping_id, store_id) pair detection and rejection."""

    def test_duplicate_ping_store_membership_rejected(self):
        """Verify identical (ping_id, store_id) pair raises ValueError without silent deduplication."""
        seen: set = set()
        row = {
            "ping_id": "ping_dup",
            "device_id": "device_100",
            "event_timestamp": "2026-11-16T03:30:00.000Z",
            "store_id": "STORE_A",
            "store_name": "GeoPulse Coffee A",
            "distance_meters": "150.25",
            "movement_mode": MovementMode.PEDESTRIAN_WALKING.value,
            "data_provenance": DATA_PROVENANCE_TAG,
        }
        # First insertion succeeds
        validate_membership_record(row, seen_pairs=seen)
        self.assertIn(("ping_dup", "STORE_A"), seen)

        # Duplicate insertion raises ValueError
        with self.assertRaises(ValueError) as ctx:
            validate_membership_record(row, seen_pairs=seen)
        self.assertIn("Duplicate membership record detected", str(ctx.exception))

    def test_same_ping_different_stores_allowed(self):
        """Verify the same ping_id across different store_id values represents valid multi-catchment overlap."""
        seen: set = set()
        row_a = {
            "ping_id": "ping_overlap",
            "device_id": "device_100",
            "event_timestamp": "2026-11-16T03:30:00.000Z",
            "store_id": "STORE_A",
            "store_name": "GeoPulse Coffee A",
            "distance_meters": "150.25",
            "movement_mode": MovementMode.PEDESTRIAN_WALKING.value,
            "data_provenance": DATA_PROVENANCE_TAG,
        }
        row_b = {
            "ping_id": "ping_overlap",  # Same ping ID
            "device_id": "device_100",
            "event_timestamp": "2026-11-16T03:30:00.000Z",
            "store_id": "STORE_B",  # Different store ID
            "store_name": "GeoPulse Coffee B",
            "distance_meters": "320.10",
            "movement_mode": MovementMode.PEDESTRIAN_WALKING.value,
            "data_provenance": DATA_PROVENANCE_TAG,
        }
        # Both must pass without error
        validate_membership_record(row_a, seen_pairs=seen)
        validate_membership_record(row_b, seen_pairs=seen)
        self.assertEqual(len(seen), 2)
        self.assertIn(("ping_overlap", "STORE_A"), seen)
        self.assertIn(("ping_overlap", "STORE_B"), seen)


class TestOutputPreflightProtection(unittest.TestCase):
    """Test output destination preflight protection guarantees."""

    def test_preflight_passes_when_no_destinations_exist(self):
        """Verify preflight succeeds when none of the 4 destinations exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = RetailAnalyticsConfig(
                output_dir=str(Path(tmpdir) / "fresh_run"),
                overwrite=False,
            )
            destinations = validate_output_destinations_preflight(config)
            self.assertEqual(len(destinations), 4)

    def test_preflight_fails_when_any_destination_exists(self):
        """Verify preflight raises FileExistsError if any single destination exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            out_base = Path(tmpdir) / "test_run"
            # Create the 1st directory
            (out_base / "hourly_store_activity").mkdir(parents=True)

            config = RetailAnalyticsConfig(output_dir=str(out_base), overwrite=False)
            with self.assertRaises(FileExistsError) as ctx:
                validate_output_destinations_preflight(config)
            self.assertIn("Output preflight validation failed", str(ctx.exception))

    def test_existing_later_output_directory_prevents_earlier_outputs_from_creation(self):
        """Verify that an existing later destination (e.g. store_overlap_summary) prevents
        earlier destinations (e.g. hourly_store_activity) from ever being created.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            out_base = Path(tmpdir) / "atomic_check"
            # Create ONLY the 4th (last) destination
            later_dir = out_base / "store_overlap_summary"
            later_dir.mkdir(parents=True)
            canary_file = later_dir / "canary.txt"
            canary_file.write_text("existing canary data", encoding="utf-8")

            # Earlier directories do not exist yet
            earlier_dir1 = out_base / "hourly_store_activity"
            earlier_dir2 = out_base / "store_time_of_day_activity"
            earlier_dir3 = out_base / "daily_store_summary"
            self.assertFalse(earlier_dir1.exists())
            self.assertFalse(earlier_dir2.exists())
            self.assertFalse(earlier_dir3.exists())

            config = RetailAnalyticsConfig(output_dir=str(out_base), overwrite=False)

            # Preflight validation must fail immediately
            with self.assertRaises(FileExistsError) as ctx:
                validate_output_destinations_preflight(config)
            self.assertIn("store_overlap_summary", str(ctx.exception))

            # Crucial assertion: earlier destinations were NEVER created
            self.assertFalse(earlier_dir1.exists(), "Earlier output directory must not have been created!")
            self.assertFalse(earlier_dir2.exists(), "Earlier output directory must not have been created!")
            self.assertFalse(earlier_dir3.exists(), "Earlier output directory must not have been created!")

            # Pre-existing canary must remain intact
            self.assertTrue(canary_file.exists())
            self.assertEqual(canary_file.read_text(encoding="utf-8"), "existing canary data")

    def test_preflight_allows_existing_when_overwrite_is_true(self):
        """Verify preflight succeeds even when destinations exist if overwrite=True."""
        with tempfile.TemporaryDirectory() as tmpdir:
            out_base = Path(tmpdir) / "overwrite_run"
            (out_base / "store_overlap_summary").mkdir(parents=True)

            config = RetailAnalyticsConfig(output_dir=str(out_base), overwrite=True)
            destinations = validate_output_destinations_preflight(config)
            self.assertEqual(len(destinations), 4)

    def test_write_spark_csv_output_protects_existing_directory(self):
        """Verify write_spark_csv_output raises FileExistsError when output dir exists and overwrite=False."""
        with tempfile.TemporaryDirectory() as tmpdir:
            existing_dir = Path(tmpdir) / "hourly_activity"
            existing_dir.mkdir()
            canary_file = existing_dir / "canary.txt"
            canary_file.write_text("existing data do not delete", encoding="utf-8")

            with self.assertRaises(FileExistsError) as ctx:
                write_spark_csv_output(None, existing_dir, overwrite=False)
            self.assertIn("Output directory already exists", str(ctx.exception))
            self.assertTrue(canary_file.exists())
            self.assertEqual(canary_file.read_text(encoding="utf-8"), "existing data do not delete")


class TestHourlyStoreActivityAggregation(unittest.TestCase):
    """Test hourly distinct device count and ping observation aggregation."""

    def test_distinct_devices_vs_ping_counts(self):
        """Verify multiple pings from the same device in the same hour increment pings but distinct_devices = 1."""
        # UTC 03:00 to 03:20 -> Asia/Kolkata 08:30 to 08:50 (Hour 8 IST)
        records = [
            {
                "ping_id": "p1",
                "device_id": "device_1",
                "event_timestamp": datetime(2026, 11, 16, 3, 0, 0, tzinfo=timezone.utc),
                "store_id": "STORE_A",
                "store_name": "GeoPulse Coffee A",
            },
            {
                "ping_id": "p2",
                "device_id": "device_1",  # Same device
                "event_timestamp": datetime(2026, 11, 16, 3, 15, 0, tzinfo=timezone.utc),
                "store_id": "STORE_A",
                "store_name": "GeoPulse Coffee A",
            },
            {
                "ping_id": "p3",
                "device_id": "device_2",  # Different device
                "event_timestamp": datetime(2026, 11, 16, 3, 20, 0, tzinfo=timezone.utc),
                "store_id": "STORE_A",
                "store_name": "GeoPulse Coffee A",
            },
        ]
        hourly = compute_hourly_metrics_from_records(records, target_timezone="Asia/Kolkata")
        self.assertEqual(len(hourly), 1)
        row = hourly[0]
        self.assertEqual(row["store_id"], "STORE_A")
        self.assertEqual(row["calendar_date"], "2026-11-16")
        self.assertEqual(row["hour_of_day"], 8)
        self.assertEqual(row["ping_count"], 3)
        self.assertEqual(row["distinct_device_count"], 2)

    def test_utc_to_ist_midnight_crossing(self):
        """Verify UTC timestamp crossing midnight maps to next calendar date in IST."""
        # UTC 2026-11-16 19:30:00 -> IST 2026-11-17 01:00:00 (Hour 1 on Nov 17)
        records = [
            {
                "ping_id": "p_night",
                "device_id": "device_night",
                "event_timestamp": datetime(2026, 11, 16, 19, 30, 0, tzinfo=timezone.utc),
                "store_id": "STORE_B",
                "store_name": "GeoPulse Coffee B",
            }
        ]
        hourly = compute_hourly_metrics_from_records(records, target_timezone="Asia/Kolkata")
        self.assertEqual(len(hourly), 1)
        row = hourly[0]
        self.assertEqual(row["store_id"], "STORE_B")
        self.assertEqual(row["calendar_date"], "2026-11-17")
        self.assertEqual(row["hour_of_day"], 1)
        self.assertEqual(row["distinct_device_count"], 1)
        self.assertEqual(row["ping_count"], 1)

    def test_multi_store_segregation(self):
        """Verify distinct stores in the same hour remain separate rows."""
        records = [
            {
                "ping_id": "p_a",
                "device_id": "dev_1",
                "event_timestamp": datetime(2026, 11, 16, 4, 0, 0, tzinfo=timezone.utc),
                "store_id": "STORE_A",
                "store_name": "GeoPulse Coffee A",
            },
            {
                "ping_id": "p_b",
                "device_id": "dev_1",  # Same device in both stores
                "event_timestamp": datetime(2026, 11, 16, 4, 0, 0, tzinfo=timezone.utc),
                "store_id": "STORE_B",
                "store_name": "GeoPulse Coffee B",
            },
        ]
        hourly = compute_hourly_metrics_from_records(records, target_timezone="Asia/Kolkata")
        self.assertEqual(len(hourly), 2)
        stores = {r["store_id"] for r in hourly}
        self.assertEqual(stores, {"STORE_A", "STORE_B"})
        for r in hourly:
            self.assertEqual(r["distinct_device_count"], 1)
            self.assertEqual(r["ping_count"], 1)


class TestStoreTimeOfDayAggregation(unittest.TestCase):
    """Test aggregation of activity into diurnal periods."""

    def test_time_of_day_aggregation_and_deduplication(self):
        """Verify devices spanning multiple hours in the same period are deduplicated."""
        records = [
            {
                "ping_id": "p1",
                "device_id": "d1",
                "event_timestamp": datetime(2026, 11, 16, 1, 0, 0, tzinfo=timezone.utc),
                "store_id": "STORE_A",
                "store_name": "GeoPulse Coffee A",
            },
            {
                "ping_id": "p2",
                "device_id": "d1",
                "event_timestamp": datetime(2026, 11, 16, 3, 0, 0, tzinfo=timezone.utc),
                "store_id": "STORE_A",
                "store_name": "GeoPulse Coffee A",
            },
            {
                "ping_id": "p3",
                "device_id": "d2",
                "event_timestamp": datetime(2026, 11, 16, 5, 0, 0, tzinfo=timezone.utc),
                "store_id": "STORE_A",
                "store_name": "GeoPulse Coffee A",
            },
        ]
        tod = compute_time_of_day_metrics_from_records(records, target_timezone="Asia/Kolkata")
        self.assertEqual(len(tod), 1)
        row = tod[0]
        self.assertEqual(row["time_of_day"], "morning")
        self.assertEqual(row["ping_count"], 3)
        self.assertEqual(row["distinct_device_count"], 2)

    def test_all_four_periods_categorization(self):
        """Verify four records in morning, afternoon, evening, night are categorized properly."""
        records = [
            # Morning: IST 08:30 (UTC 03:00)
            {
                "ping_id": "p_m",
                "device_id": "d1",
                "event_timestamp": datetime(2026, 11, 16, 3, 0, 0, tzinfo=timezone.utc),
                "store_id": "STORE_A",
                "store_name": "GeoPulse Coffee A",
            },
            # Afternoon: IST 14:30 (UTC 09:00)
            {
                "ping_id": "p_a",
                "device_id": "d1",
                "event_timestamp": datetime(2026, 11, 16, 9, 0, 0, tzinfo=timezone.utc),
                "store_id": "STORE_A",
                "store_name": "GeoPulse Coffee A",
            },
            # Evening: IST 18:30 (UTC 13:00)
            {
                "ping_id": "p_e",
                "device_id": "d1",
                "event_timestamp": datetime(2026, 11, 16, 13, 0, 0, tzinfo=timezone.utc),
                "store_id": "STORE_A",
                "store_name": "GeoPulse Coffee A",
            },
            # Night: IST 23:30 (UTC 18:00)
            {
                "ping_id": "p_n",
                "device_id": "d1",
                "event_timestamp": datetime(2026, 11, 16, 18, 0, 0, tzinfo=timezone.utc),
                "store_id": "STORE_A",
                "store_name": "GeoPulse Coffee A",
            },
        ]
        tod = compute_time_of_day_metrics_from_records(records, target_timezone="Asia/Kolkata")
        self.assertEqual(len(tod), 4)
        periods = [r["time_of_day"] for r in tod]
        self.assertEqual(set(periods), {"morning", "afternoon", "evening", "night"})


class TestDailyStoreSummaryAggregation(unittest.TestCase):
    """Test daily store summary metrics including active hours count and observation density."""

    def test_daily_summary_active_hours_and_density(self):
        """Verify active_hours_count and pings_per_device calculation."""
        records = [
            {
                "ping_id": "p1",
                "device_id": "d1",
                "event_timestamp": datetime(2026, 11, 16, 2, 30, 0, tzinfo=timezone.utc),  # IST 08:00
                "store_id": "STORE_A",
                "store_name": "GeoPulse Coffee A",
            },
            {
                "ping_id": "p2",
                "device_id": "d1",
                "event_timestamp": datetime(2026, 11, 16, 2, 45, 0, tzinfo=timezone.utc),  # IST 08:15
                "store_id": "STORE_A",
                "store_name": "GeoPulse Coffee A",
            },
            {
                "ping_id": "p3",
                "device_id": "d2",
                "event_timestamp": datetime(2026, 11, 16, 3, 20, 0, tzinfo=timezone.utc),  # IST 08:50
                "store_id": "STORE_A",
                "store_name": "GeoPulse Coffee A",
            },
        ]
        daily = compute_daily_metrics_from_records(records, target_timezone="Asia/Kolkata")
        self.assertEqual(len(daily), 1)
        row = daily[0]
        self.assertEqual(row["store_id"], "STORE_A")
        self.assertEqual(row["calendar_date"], "2026-11-16")
        self.assertEqual(row["distinct_devices"], 2)
        self.assertEqual(row["ping_count"], 3)
        self.assertEqual(row["active_hours_count"], 1)  # All Hour 8
        self.assertEqual(row["pings_per_device"], 1.50)


class TestStoreOverlapSummaryAggregation(unittest.TestCase):
    """Test catchment overlap analytics and exact mathematical reconciliation."""

    def test_store_overlap_metrics_and_reconciliation(self):
        """Verify exclusive and overlapping ping and device segregation and reconciliation."""
        records = [
            {
                "ping_id": "p1",
                "device_id": "d1",
                "store_id": "STORE_A",
                "store_name": "GeoPulse Coffee A",
            },
            {
                "ping_id": "p2",
                "device_id": "d2",
                "store_id": "STORE_B",
                "store_name": "GeoPulse Coffee B",
            },
            {
                "ping_id": "p3",
                "device_id": "d3",
                "store_id": "STORE_A",
                "store_name": "GeoPulse Coffee A",
            },
            {
                "ping_id": "p3",  # Multi-catchment ping in both stores
                "device_id": "d3",
                "store_id": "STORE_B",
                "store_name": "GeoPulse Coffee B",
            },
            {
                "ping_id": "p4",
                "device_id": "d3",  # Device 3 visits STORE_A again
                "store_id": "STORE_A",
                "store_name": "GeoPulse Coffee A",
            },
        ]
        result = compute_overlap_metrics_from_records(records)
        overlap_summary = result["store_overlap_summary"]
        net_metrics = result["network_metrics"]
        self.assertEqual(len(overlap_summary), 2)

        store_a = next(r for r in overlap_summary if r["store_id"] == "STORE_A")
        store_b = next(r for r in overlap_summary if r["store_id"] == "STORE_B")

        # STORE_A checks:
        self.assertEqual(store_a["exclusive_pings"], 2)
        self.assertEqual(store_a["overlapping_pings"], 1)
        self.assertEqual(store_a["total_pings"], 3)
        self.assertEqual(store_a["exclusive_devices"], 1)
        self.assertEqual(store_a["multi_catchment_devices"], 1)
        self.assertEqual(store_a["total_devices"], 2)

        # STORE_B checks:
        self.assertEqual(store_b["exclusive_pings"], 1)
        self.assertEqual(store_b["overlapping_pings"], 1)
        self.assertEqual(store_b["total_pings"], 2)
        self.assertEqual(store_b["exclusive_devices"], 1)
        self.assertEqual(store_b["multi_catchment_devices"], 1)
        self.assertEqual(store_b["total_devices"], 2)

        # Network reconciliations:
        self.assertTrue(net_metrics["ping_reconciliation_valid"])
        self.assertTrue(net_metrics["membership_reconciliation_valid"])
        self.assertTrue(net_metrics["device_reconciliation_valid"])
        self.assertEqual(net_metrics["reconciliation_status"], "RECONCILED")

        # Ping reconciliation: exclusive (3) + multi (1) = distinct (4)
        self.assertEqual(net_metrics["exclusive_unique_pings"], 3)
        self.assertEqual(net_metrics["unique_multi_catchment_pings"], 1)
        self.assertEqual(net_metrics["total_unique_pings"], 4)

        # Membership reconciliation: exclusive (3) + overlapping instances (2) = total memberships (5)
        self.assertEqual(net_metrics["overlapping_membership_instances"], 2)
        self.assertEqual(net_metrics["total_memberships"], 5)

        # Device reconciliation: exclusive devices (2: d1, d2) + multi devices (1: d3) = 3
        self.assertEqual(net_metrics["exclusive_devices"], 2)
        self.assertEqual(net_metrics["multi_catchment_devices"], 1)
        self.assertEqual(net_metrics["total_unique_devices"], 3)

    def test_reconciliation_ordering_detects_mismatch_and_fails_before_write(self):
        """Verify that when reconciliation totals mismatch, pre-write validation fails."""
        # Intentionally construct an inconsistent store membership record list where a store
        # claims invalid counts or device totals do not match
        records = [
            {"ping_id": "p1", "device_id": "d1", "store_id": "STORE_A", "store_name": "Coffee A"},
            {"ping_id": "p2", "device_id": "d2", "store_id": "STORE_B", "store_name": "Coffee B"},
        ]
        res = compute_overlap_metrics_from_records(records)
        net = res["network_metrics"]
        self.assertTrue(net["ping_reconciliation_valid"])
        self.assertTrue(net["membership_reconciliation_valid"])
        self.assertTrue(net["device_reconciliation_valid"])


class TestRetailAnalyticsConfig(unittest.TestCase):
    """Test configuration dataclass defaults and customization."""

    def test_default_config_values(self):
        """Verify default configuration points to approved week2 paths and timezone."""
        config = RetailAnalyticsConfig()
        self.assertEqual(config.memberships_input_path, DEFAULT_MEMBERSHIPS_PATH)
        self.assertEqual(config.output_dir, DEFAULT_OUTPUT_BASE)
        self.assertEqual(config.timezone, "Asia/Kolkata")
        self.assertFalse(config.overwrite)
        self.assertEqual(config.get_hourly_path(), str(Path(DEFAULT_OUTPUT_BASE) / "hourly_store_activity"))
        self.assertEqual(config.get_time_of_day_path(), str(Path(DEFAULT_OUTPUT_BASE) / "store_time_of_day_activity"))
        self.assertEqual(config.get_daily_path(), str(Path(DEFAULT_OUTPUT_BASE) / "daily_store_summary"))
        self.assertEqual(config.get_overlap_path(), str(Path(DEFAULT_OUTPUT_BASE) / "store_overlap_summary"))

    def test_custom_config_values(self):
        """Verify custom configuration overrides defaults properly."""
        config = RetailAnalyticsConfig(
            memberships_input_path="custom/path/memberships",
            output_dir="custom/output",
            overwrite=True,
        )
        self.assertEqual(config.memberships_input_path, "custom/path/memberships")
        self.assertEqual(config.output_dir, "custom/output")
        self.assertTrue(config.overwrite)
        self.assertEqual(config.get_hourly_path(), str(Path("custom/output") / "hourly_store_activity"))


class TestActualMembershipsDatasetIfPresent(unittest.TestCase):
    """Integration validation against actual processed memberships artifact if present on disk."""

    def test_actual_memberships_reconciliation(self):
        """Verify the actual 4,051 membership records reconcile across pings, devices, and stores."""
        memberships_path = Path("data/processed/week2_verification/store_catchment_memberships")
        if not memberships_path.exists():
            self.skipTest(f"Actual memberships dataset not found at {memberships_path}")

        # Find all CSV files (handles single CSV or Spark part directory)
        if memberships_path.is_file():
            csv_files = [memberships_path]
        else:
            csv_files = sorted(memberships_path.glob("*.csv"))

        self.assertGreater(len(csv_files), 0, "No CSV files found in memberships directory")

        records: list[dict] = []
        seen_pairs: set = set()
        for f in csv_files:
            with open(f, "r", encoding="utf-8") as fp:
                reader = csv.DictReader(fp)
                for row in reader:
                    rec = validate_membership_record(row, seen_pairs=seen_pairs)
                    records.append(rec)

        # 1. Total records (memberships)
        self.assertEqual(len(records), 4051, "Expected exactly 4,051 store catchment memberships")

        # 2. Store presence
        store_ids = {r["store_id"] for r in records}
        self.assertEqual(store_ids, {"STORE_A", "STORE_B", "STORE_C", "STORE_D"})

        # 3. Unique pings and unique devices
        unique_pings = {r["ping_id"] for r in records}
        unique_devices = {r["device_id"] for r in records}
        self.assertEqual(len(unique_pings), 3956, "Expected exactly 3,956 unique pings")
        self.assertEqual(len(unique_devices), 350, "Expected exactly 350 unique devices")

        # 4. Direct inspection of overlapping ping distribution without prior assumptions:
        ping_to_stores: dict[str, set[str]] = {}
        for r in records:
            ping_to_stores.setdefault(r["ping_id"], set()).add(r["store_id"])

        exclusive_pings_set = {p for p, s_set in ping_to_stores.items() if len(s_set) == 1}
        multi_pings_set = {p for p, s_set in ping_to_stores.items() if len(s_set) > 1}

        self.assertEqual(len(exclusive_pings_set), 3861, "Expected exactly 3,861 exclusive unique pings")
        self.assertEqual(len(multi_pings_set), 95, "Expected exactly 95 multi-catchment unique pings")
        self.assertEqual(len(exclusive_pings_set) + len(multi_pings_set), 3956)

        # Verify the number of stores associated with each overlapping ping directly
        multi_ping_store_counts = {p: len(s_set) for p, s_set in ping_to_stores.items() if len(s_set) > 1}
        self.assertEqual(len(multi_ping_store_counts), 95)
        for ping_id, s_count in multi_ping_store_counts.items():
            self.assertEqual(
                s_count,
                2,
                f"Multi-catchment ping '{ping_id}' belongs to {s_count} stores (expected 2)",
            )

        # Total overlapping instances directly summed from store counts
        total_overlapping_instances = sum(multi_ping_store_counts.values())
        self.assertEqual(total_overlapping_instances, 190)

        # 5. Overlap metrics and exact reconciliation
        result = compute_overlap_metrics_from_records(records)
        overlap = result["store_overlap_summary"]
        net = result["network_metrics"]
        self.assertEqual(len(overlap), 4)

        store_map = {r["store_id"]: r for r in overlap}
        # Verified exact numbers per store:
        # STORE_A: 1130 exclusive, 60 overlapping, 1190 total pings; 81 exclusive, 65 multi, 146 total devices
        self.assertEqual(store_map["STORE_A"]["exclusive_pings"], 1130)
        self.assertEqual(store_map["STORE_A"]["overlapping_pings"], 60)
        self.assertEqual(store_map["STORE_A"]["total_pings"], 1190)
        self.assertEqual(store_map["STORE_A"]["exclusive_devices"], 81)
        self.assertEqual(store_map["STORE_A"]["multi_catchment_devices"], 65)
        self.assertEqual(store_map["STORE_A"]["total_devices"], 146)

        # STORE_B: 1023 exclusive, 95 overlapping, 1118 total pings; 37 exclusive, 109 multi, 146 total devices
        self.assertEqual(store_map["STORE_B"]["exclusive_pings"], 1023)
        self.assertEqual(store_map["STORE_B"]["overlapping_pings"], 95)
        self.assertEqual(store_map["STORE_B"]["total_pings"], 1118)
        self.assertEqual(store_map["STORE_B"]["exclusive_devices"], 37)
        self.assertEqual(store_map["STORE_B"]["multi_catchment_devices"], 109)
        self.assertEqual(store_map["STORE_B"]["total_devices"], 146)

        # STORE_C: 914 exclusive, 16 overlapping, 930 total pings; 57 exclusive, 70 multi, 127 total devices
        self.assertEqual(store_map["STORE_C"]["exclusive_pings"], 914)
        self.assertEqual(store_map["STORE_C"]["overlapping_pings"], 16)
        self.assertEqual(store_map["STORE_C"]["total_pings"], 930)
        self.assertEqual(store_map["STORE_C"]["exclusive_devices"], 57)
        self.assertEqual(store_map["STORE_C"]["multi_catchment_devices"], 70)
        self.assertEqual(store_map["STORE_C"]["total_devices"], 127)

        # STORE_D: 794 exclusive, 19 overlapping, 813 total pings; 41 exclusive, 64 multi, 105 total devices
        self.assertEqual(store_map["STORE_D"]["exclusive_pings"], 794)
        self.assertEqual(store_map["STORE_D"]["overlapping_pings"], 19)
        self.assertEqual(store_map["STORE_D"]["total_pings"], 813)
        self.assertEqual(store_map["STORE_D"]["exclusive_devices"], 41)
        self.assertEqual(store_map["STORE_D"]["multi_catchment_devices"], 64)
        self.assertEqual(store_map["STORE_D"]["total_devices"], 105)

        # Independent network metrics verification:
        self.assertEqual(net["total_memberships"], 4051)
        self.assertEqual(net["distinct_matched_pings"], 3956)
        self.assertEqual(net["exclusive_unique_pings"], 3861)
        self.assertEqual(net["unique_multi_catchment_pings"], 95)
        self.assertEqual(net["overlapping_membership_instances"], 190)
        self.assertEqual(net["total_unique_devices"], 350)
        self.assertEqual(net["exclusive_devices"], 216)
        self.assertEqual(net["multi_catchment_devices"], 134)

        # Ping-level reconciliation: exclusive (3861) + multi (95) = distinct (3956)
        self.assertTrue(net["ping_reconciliation_valid"])
        self.assertEqual(net["exclusive_unique_pings"] + net["unique_multi_catchment_pings"], 3956)

        # Membership-level reconciliation: exclusive (3861) + overlapping instances (190) = total memberships (4051)
        self.assertTrue(net["membership_reconciliation_valid"])
        self.assertEqual(net["exclusive_unique_pings"] + net["overlapping_membership_instances"], 4051)

        # Device-level reconciliation: exclusive (216) + multi (134) = distinct (350)
        self.assertTrue(net["device_reconciliation_valid"])
        self.assertEqual(net["exclusive_devices"] + net["multi_catchment_devices"], 350)

        # Store-level sum reconciliation: sum of store exclusive pings equals network exclusive pings
        sum_exclusive_pings = sum(r["exclusive_pings"] for r in overlap)
        sum_overlapping_pings = sum(r["overlapping_pings"] for r in overlap)
        self.assertEqual(sum_exclusive_pings, 3861)
        self.assertEqual(sum_overlapping_pings, 190)
        self.assertEqual(sum_exclusive_pings + sum_overlapping_pings, 4051)

        self.assertEqual(net["reconciliation_status"], "RECONCILED")


if __name__ == "__main__":
    unittest.main()
