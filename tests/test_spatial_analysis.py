"""Unit tests for GeoPulse spatial analysis module.

Tests cover:
- Output directory protection and native Spark writer safety.
- Planar geometric area calculations (circle and regular polygon buffer approximation in EPSG:32643).
- Catchment radius validation (positive, finite numbers).
- Polygon segment parameter configuration validation.
- Catchment area validation against theoretical tolerances.
- Store reference data schema, coordinate bounds, metadata (location_type, data_provenance), and uniqueness.
- Mobility ping schema validation, empty file handling, and coordinate/identifier validation.
- Catchment overlap category aggregation and exact reconciliation.
- Spatial analysis configuration defaults, post-init validation, and immutability.
- Sedona environment helper functions.

All unit tests in this module run offline using standard Python libraries
without requiring a live PySpark or Sedona JVM session.
"""

import csv
import hashlib
import math
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from src.config import DATA_PROVENANCE_TAG, MovementMode, STUDY_AREA
from src.spatial_analysis import (
    DEFAULT_CATCHMENT_RADIUS_METERS,
    DEFAULT_METRIC_CRS,
    DEFAULT_SOURCE_CRS,
    EXPECTED_STORE_COUNT,
    MOBILITY_CSV_COLUMNS,
    SEDONA_SHADED_JAR_NAME,
    SEDONA_SHADED_JAR_SHA256,
    STORE_CSV_COLUMNS,
    SpatialAnalysisConfig,
    calculate_overlap_distribution,
    calculate_polygon_buffer_area,
    calculate_theoretical_circle_area,
    compute_file_sha256,
    find_sedona_jars,
    validate_catchment_area,
    validate_catchment_radius,
    validate_mobility_csv,
    validate_mobility_record,
    validate_mobility_schema,
    validate_sedona_jar_checksum,
    validate_store_records,
    write_spark_csv_output,
)


class TestOutputDirectoryProtection(unittest.TestCase):
    """Test safety guarantees for output directory persistence."""

    def test_write_spark_csv_output_protects_existing_directory(self):
        """Verify write_spark_csv_output raises FileExistsError and preserves pre-existing contents."""
        with tempfile.TemporaryDirectory() as tmpdir:
            existing_dir = Path(tmpdir) / "store_catchments"
            existing_dir.mkdir()
            canary_file = existing_dir / "valid_result.txt"
            canary_file.write_text("pre-existing valid data", encoding="utf-8")

            # Calling write with overwrite=False must raise FileExistsError immediately
            with self.assertRaises(FileExistsError) as ctx:
                write_spark_csv_output(None, existing_dir, overwrite=False)

            self.assertIn("already exists", str(ctx.exception))
            self.assertIn("overwrite=True", str(ctx.exception))

            # Verify that the directory was NOT deleted and the canary file remains intact
            self.assertTrue(existing_dir.exists())
            self.assertTrue(canary_file.exists())
            self.assertEqual(canary_file.read_text(encoding="utf-8"), "pre-existing valid data")


class TestCatchmentGeometryCalculations(unittest.TestCase):
    """Test geometric area and buffer calculation helper functions."""

    def test_theoretical_circle_area_calculation(self):
        """Verify pi * r^2 formula for 500m radius gives ~785,398.16 m^2."""
        radius = 500.0
        expected = math.pi * (500.0 ** 2)
        actual = calculate_theoretical_circle_area(radius)
        self.assertAlmostEqual(actual, expected, places=4)
        self.assertAlmostEqual(actual, 785398.163397, places=2)

    def test_theoretical_circle_area_invalid_radius(self):
        """Verify zero, negative, infinite, or NaN radius raises ValueError."""
        with self.assertRaises(ValueError):
            calculate_theoretical_circle_area(0.0)
        with self.assertRaises(ValueError):
            calculate_theoretical_circle_area(-100.0)
        with self.assertRaises(ValueError):
            calculate_theoretical_circle_area(float("inf"))
        with self.assertRaises(ValueError):
            calculate_theoretical_circle_area(float("nan"))
        with self.assertRaises(TypeError):
            calculate_theoretical_circle_area("500")

    def test_polygon_buffer_area_calculation(self):
        """Verify 32-sided polygon approximation formula yields ~780,361.29 m^2."""
        radius = 500.0
        # For a 32-sided regular polygon: Area = 16 * r^2 * sin(pi / 16)
        expected_32gon = 16.0 * (500.0 ** 2) * math.sin(math.pi / 16.0)
        actual = calculate_polygon_buffer_area(radius, segments_per_quadrant=8)
        self.assertAlmostEqual(actual, expected_32gon, places=4)
        self.assertAlmostEqual(actual, 780361.29, delta=0.1)

    def test_polygon_segment_configuration_validation(self):
        """Verify invalid polygon segment configurations are strictly rejected."""
        radius = 500.0
        # Zero or negative segments
        with self.assertRaises(ValueError):
            calculate_polygon_buffer_area(radius, segments_per_quadrant=0)
        with self.assertRaises(ValueError):
            calculate_polygon_buffer_area(radius, segments_per_quadrant=-4)
        # Non-integer segments
        with self.assertRaises(TypeError):
            calculate_polygon_buffer_area(radius, segments_per_quadrant=8.5)
        with self.assertRaises(TypeError):
            calculate_polygon_buffer_area(radius, segments_per_quadrant="8")
        with self.assertRaises(TypeError):
            calculate_polygon_buffer_area(radius, segments_per_quadrant=True)

    def test_validate_catchment_radius_helper(self):
        """Verify standalone catchment radius validator catches all invalid forms."""
        # Valid
        validate_catchment_radius(500.0)
        validate_catchment_radius(100)

        # Invalid values
        with self.assertRaises(ValueError):
            validate_catchment_radius(0.0)
        with self.assertRaises(ValueError):
            validate_catchment_radius(-50.0)
        with self.assertRaises(ValueError):
            validate_catchment_radius(float("inf"))
        with self.assertRaises(ValueError):
            validate_catchment_radius(float("-inf"))
        with self.assertRaises(ValueError):
            validate_catchment_radius(float("nan"))

        # Invalid types
        with self.assertRaises(TypeError):
            validate_catchment_radius("500")
        with self.assertRaises(TypeError):
            validate_catchment_radius(None)
        with self.assertRaises(TypeError):
            validate_catchment_radius(True)

    def test_validate_catchment_area_within_tolerance(self):
        """Verify area tolerance validation accepts values within 2% and rejects extremes."""
        radius = 500.0
        base_area = calculate_polygon_buffer_area(radius)

        # Exact expected area passes
        self.assertTrue(validate_catchment_area(base_area, radius, tolerance_pct=0.02))

        # +1% and -1% pass within 2% tolerance
        self.assertTrue(validate_catchment_area(base_area * 1.01, radius, tolerance_pct=0.02))
        self.assertTrue(validate_catchment_area(base_area * 0.99, radius, tolerance_pct=0.02))

        # +5% and -5% must fail
        self.assertFalse(validate_catchment_area(base_area * 1.05, radius, tolerance_pct=0.02))
        self.assertFalse(validate_catchment_area(base_area * 0.95, radius, tolerance_pct=0.02))

    def test_validate_catchment_area_non_positive_inputs(self):
        """Verify non-positive areas or radii return False gracefully."""
        self.assertFalse(validate_catchment_area(0.0, 500.0))
        self.assertFalse(validate_catchment_area(-500.0, 500.0))
        self.assertFalse(validate_catchment_area(780000.0, 0.0))
        self.assertFalse(validate_catchment_area(780000.0, -100.0))


class TestStoreReferenceValidation(unittest.TestCase):
    """Test store reference dataset validation logic."""

    def setUp(self):
        """Prepare sample valid store records for testing."""
        self.valid_records = [
            {
                "store_id": "STORE_A",
                "store_name": "GeoPulse Coffee A",
                "longitude": "77.6320",
                "latitude": "12.9760",
                "location_type": "hypothetical_candidate",
                "data_provenance": DATA_PROVENANCE_TAG,
            },
            {
                "store_id": "STORE_B",
                "store_name": "GeoPulse Coffee B",
                "longitude": "77.6400",
                "latitude": "12.9780",
                "location_type": "hypothetical_candidate",
                "data_provenance": DATA_PROVENANCE_TAG,
            },
            {
                "store_id": "STORE_C",
                "store_name": "GeoPulse Coffee C",
                "longitude": "77.6480",
                "latitude": "12.9810",
                "location_type": "hypothetical_candidate",
                "data_provenance": DATA_PROVENANCE_TAG,
            },
            {
                "store_id": "STORE_D",
                "store_name": "GeoPulse Coffee D",
                "longitude": "77.6380",
                "latitude": "12.9860",
                "location_type": "hypothetical_candidate",
                "data_provenance": DATA_PROVENANCE_TAG,
            },
        ]

    def test_valid_store_records_pass(self):
        """Verify standard 4 candidate stores pass validation."""
        result = validate_store_records(self.valid_records)
        self.assertTrue(result["valid"])
        self.assertEqual(result["count"], 4)
        self.assertEqual(len(result["stores"]), 4)

    def test_empty_store_records_rejected(self):
        """Verify empty records list raises ValueError."""
        with self.assertRaises(ValueError) as ctx:
            validate_store_records([])
        self.assertIn("must not be empty", str(ctx.exception))

    def test_unexpected_store_count_rejected(self):
        """Verify non-4 store count raises ValueError."""
        with self.assertRaises(ValueError) as ctx:
            validate_store_records(self.valid_records[:3])
        self.assertIn("Expected exactly 4", str(ctx.exception))

    def test_missing_required_column_rejected(self):
        """Verify record with missing column raises ValueError."""
        corrupted = [dict(r) for r in self.valid_records]
        del corrupted[0]["latitude"]
        with self.assertRaises(ValueError) as ctx:
            validate_store_records(corrupted)
        self.assertIn("missing required field 'latitude'", str(ctx.exception))

    def test_missing_location_type_metadata_rejected(self):
        """Verify record with missing location_type raises ValueError."""
        corrupted = [dict(r) for r in self.valid_records]
        del corrupted[0]["location_type"]
        with self.assertRaises(ValueError) as ctx:
            validate_store_records(corrupted)
        self.assertIn("missing required field 'location_type'", str(ctx.exception))

    def test_invalid_location_type_rejected(self):
        """Verify unexpected location_type value raises ValueError."""
        corrupted = [dict(r) for r in self.valid_records]
        corrupted[0]["location_type"] = "actual_retail_branch"
        with self.assertRaises(ValueError) as ctx:
            validate_store_records(corrupted)
        self.assertIn("invalid location_type", str(ctx.exception))

    def test_missing_data_provenance_rejected(self):
        """Verify record with missing data_provenance raises ValueError."""
        corrupted = [dict(r) for r in self.valid_records]
        del corrupted[0]["data_provenance"]
        with self.assertRaises(ValueError) as ctx:
            validate_store_records(corrupted)
        self.assertIn("missing required field 'data_provenance'", str(ctx.exception))

    def test_invalid_data_provenance_rejected(self):
        """Verify unexpected data_provenance value raises ValueError."""
        corrupted = [dict(r) for r in self.valid_records]
        corrupted[1]["data_provenance"] = "unapproved_source_v2"
        with self.assertRaises(ValueError) as ctx:
            validate_store_records(corrupted)
        self.assertIn("invalid data_provenance", str(ctx.exception))

    def test_blank_field_value_rejected(self):
        """Verify record with whitespace-only field raises ValueError."""
        corrupted = [dict(r) for r in self.valid_records]
        corrupted[1]["store_name"] = "   "
        with self.assertRaises(ValueError) as ctx:
            validate_store_records(corrupted)
        self.assertIn("missing required field 'store_name'", str(ctx.exception))

    def test_duplicate_store_id_rejected(self):
        """Verify duplicate store_id values are rejected."""
        corrupted = [dict(r) for r in self.valid_records]
        corrupted[1]["store_id"] = "STORE_A"
        with self.assertRaises(ValueError) as ctx:
            validate_store_records(corrupted)
        self.assertIn("Duplicate store_id detected", str(ctx.exception))

    def test_store_coordinate_outside_bounds_rejected(self):
        """Verify store located outside Indiranagar boundary is rejected."""
        corrupted = [dict(r) for r in self.valid_records]
        corrupted[2]["latitude"] = "13.0500"  # Well north of Indiranagar max 12.9900
        with self.assertRaises(ValueError) as ctx:
            validate_store_records(corrupted)
        self.assertIn("outside study area bounding box", str(ctx.exception))

    def test_store_infinite_or_nan_coordinate_rejected(self):
        """Verify infinite or NaN coordinates raise ValueError."""
        corrupted = [dict(r) for r in self.valid_records]
        corrupted[0]["longitude"] = "inf"
        with self.assertRaises(ValueError) as ctx:
            validate_store_records(corrupted)
        self.assertIn("must be finite", str(ctx.exception))

    def test_invalid_coordinate_format_rejected(self):
        """Verify non-numeric coordinate string raises ValueError."""
        corrupted = [dict(r) for r in self.valid_records]
        corrupted[3]["longitude"] = "invalid_coord"
        with self.assertRaises(ValueError) as ctx:
            validate_store_records(corrupted)
        self.assertIn("Invalid coordinate format", str(ctx.exception))

    def test_actual_reference_csv_file_if_present(self):
        """Verify data/reference/synthetic_store_locations.csv passes validation directly."""
        ref_path = Path("data/reference/synthetic_store_locations.csv")
        if ref_path.exists():
            with open(ref_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                records = list(reader)
            result = validate_store_records(records)
            self.assertTrue(result["valid"])
            self.assertEqual(result["count"], 4)
            for store in result["stores"]:
                self.assertTrue(STUDY_AREA.bounds.contains(store["longitude"], store["latitude"]))
                self.assertEqual(store["location_type"], "hypothetical_candidate")
                self.assertEqual(store["data_provenance"], DATA_PROVENANCE_TAG)


class TestMobilityInputValidation(unittest.TestCase):
    """Test mobility pings schema, empty CSV handling, and record validation."""

    def setUp(self):
        """Prepare sample valid mobility record for record-level tests."""
        self.valid_mobility_record = {
            "ping_id": "6f7bc14c-c11c-4825-8977-760ea6776b0d",
            "device_id": "ca55e38b-3c9a-4e10-a38d-6f44cac4d0eb",
            "event_timestamp": "2026-11-16T02:30:07.600Z",
            "latitude": "12.976371",
            "longitude": "77.642056",
            "accuracy_meters": "14.5",
            "movement_mode": "pedestrian_walking",
            "data_provenance": DATA_PROVENANCE_TAG,
        }

    def test_canonical_mobility_columns_pass(self):
        """Verify exact canonical 8 columns pass validation."""
        self.assertTrue(validate_mobility_schema(MOBILITY_CSV_COLUMNS))

    def test_missing_column_fails(self):
        """Verify omitting any column fails validation."""
        incomplete = MOBILITY_CSV_COLUMNS[:-1]
        self.assertFalse(validate_mobility_schema(incomplete))

    def test_reordered_columns_fail(self):
        """Verify reordered columns fail schema validation."""
        reordered = list(reversed(MOBILITY_CSV_COLUMNS))
        self.assertFalse(validate_mobility_schema(reordered))

    def test_empty_columns_fail(self):
        """Verify empty column list fails validation."""
        self.assertFalse(validate_mobility_schema([]))

    def test_empty_mobility_csv_handled_gracefully(self):
        """Verify empty CSV file raises ValueError without unhandled StopIteration."""
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".csv") as tmp:
            tmp_path = Path(tmp.name)
        try:
            with self.assertRaises(ValueError) as ctx:
                validate_mobility_csv(tmp_path)
            self.assertIn("completely empty", str(ctx.exception))
        finally:
            tmp_path.unlink()

    def test_mobility_csv_with_headers_only_rejected(self):
        """Verify CSV containing headers but zero records raises ValueError."""
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".csv") as tmp:
            tmp.write(",".join(MOBILITY_CSV_COLUMNS) + "\n")
            tmp_path = Path(tmp.name)
        try:
            with self.assertRaises(ValueError) as ctx:
                validate_mobility_csv(tmp_path)
            self.assertIn("zero data records", str(ctx.exception))
        finally:
            tmp_path.unlink()

    def test_valid_mobility_record_passes(self):
        """Verify standard valid mobility record passes validation."""
        validate_mobility_record(self.valid_mobility_record)

    def test_mobility_duplicate_ping_id_rejected(self):
        """Verify duplicate ping_id raises ValueError."""
        seen = {"6f7bc14c-c11c-4825-8977-760ea6776b0d"}
        with self.assertRaises(ValueError) as ctx:
            validate_mobility_record(self.valid_mobility_record, seen_ping_ids=seen)
        self.assertIn("Duplicate ping_id detected", str(ctx.exception))

    def test_mobility_blank_ping_id_rejected(self):
        """Verify missing or blank ping_id raises ValueError."""
        corrupted = dict(self.valid_mobility_record)
        corrupted["ping_id"] = "   "
        with self.assertRaises(ValueError) as ctx:
            validate_mobility_record(corrupted)
        self.assertIn("Missing or blank 'ping_id'", str(ctx.exception))

    def test_mobility_non_numeric_coordinates_rejected(self):
        """Verify non-numeric latitude or longitude raises ValueError."""
        corrupted = dict(self.valid_mobility_record)
        corrupted["latitude"] = "not_a_latitude"
        with self.assertRaises(ValueError) as ctx:
            validate_mobility_record(corrupted)
        self.assertIn("non-numeric coordinates", str(ctx.exception))

    def test_mobility_infinite_coordinates_rejected(self):
        """Verify infinite coordinates raise ValueError."""
        corrupted = dict(self.valid_mobility_record)
        corrupted["longitude"] = "inf"
        with self.assertRaises(ValueError) as ctx:
            validate_mobility_record(corrupted)
        self.assertIn("not a finite number", str(ctx.exception))

    def test_mobility_nan_coordinates_rejected(self):
        """Verify NaN coordinates raise ValueError."""
        corrupted = dict(self.valid_mobility_record)
        corrupted["latitude"] = "nan"
        with self.assertRaises(ValueError) as ctx:
            validate_mobility_record(corrupted)
        self.assertIn("not a finite number", str(ctx.exception))

    def test_mobility_out_of_bounds_coordinates_rejected(self):
        """Verify coordinates outside Indiranagar study-area boundary are rejected."""
        corrupted = dict(self.valid_mobility_record)
        corrupted["latitude"] = "13.0500"  # North of Indiranagar max latitude 12.9900
        with self.assertRaises(ValueError) as ctx:
            validate_mobility_record(corrupted)
        self.assertIn("outside study area bounding box", str(ctx.exception))

    def test_mobility_invalid_movement_mode_rejected(self):
        """Verify unsupported movement mode raises ValueError."""
        corrupted = dict(self.valid_mobility_record)
        corrupted["movement_mode"] = "hoverboard_gliding"
        with self.assertRaises(ValueError) as ctx:
            validate_mobility_record(corrupted)
        self.assertIn("not in allowed modes", str(ctx.exception))

    def test_mobility_invalid_data_provenance_rejected(self):
        """Verify unapproved data provenance raises ValueError."""
        corrupted = dict(self.valid_mobility_record)
        corrupted["data_provenance"] = "unverified_source"
        with self.assertRaises(ValueError) as ctx:
            validate_mobility_record(corrupted)
        self.assertIn("does not match expected", str(ctx.exception))

    def test_actual_mobility_csv_sample_validation(self):
        """Verify actual raw mobility dataset passes validation (first 500 records)."""
        raw_path = Path("data/raw/synthetic_mobility_pings.csv")
        if raw_path.exists():
            res = validate_mobility_csv(raw_path, max_records=500)
            self.assertTrue(res["valid"])
            self.assertEqual(res["record_count"], 500)
            self.assertEqual(res["unique_pings"], 500)


class TestOverlapDistributionAndReconciliation(unittest.TestCase):
    """Test catchment overlap calculation and mathematical reconciliation."""

    def test_overlap_distribution_single_catchment_only(self):
        """Verify distribution when all pings belong to exactly one store catchment."""
        memberships = [
            {"ping_id": "p1", "store_id": "STORE_A"},
            {"ping_id": "p2", "store_id": "STORE_B"},
            {"ping_id": "p3", "store_id": "STORE_C"},
        ]
        dist = calculate_overlap_distribution(memberships)
        self.assertEqual(dist["pings_exactly_one_catchment"], 3)
        self.assertEqual(dist["pings_exactly_two_catchments"], 0)
        self.assertEqual(dist["pings_three_or_more_catchments"], 0)
        self.assertEqual(dist["pings_multiple_catchments"], 0)
        self.assertEqual(dist["total_unique_matched_pings"], 3)
        self.assertTrue(dist["reconciled"])

    def test_overlap_distribution_mixed_memberships(self):
        """Verify distribution and reconciliation with pings in 1, 2, and 3 catchments."""
        memberships = [
            # p1: in 1 catchment
            {"ping_id": "p1", "store_id": "STORE_A"},
            # p2: in 2 catchments
            {"ping_id": "p2", "store_id": "STORE_A"},
            {"ping_id": "p2", "store_id": "STORE_B"},
            # p3: in 3 catchments
            {"ping_id": "p3", "store_id": "STORE_A"},
            {"ping_id": "p3", "store_id": "STORE_B"},
            {"ping_id": "p3", "store_id": "STORE_C"},
            # p4: in 1 catchment
            {"ping_id": "p4", "store_id": "STORE_D"},
        ]
        dist = calculate_overlap_distribution(memberships)
        self.assertEqual(dist["pings_exactly_one_catchment"], 2)  # p1, p4
        self.assertEqual(dist["pings_exactly_two_catchments"], 1)  # p2
        self.assertEqual(dist["pings_three_or_more_catchments"], 1)  # p3
        self.assertEqual(dist["pings_multiple_catchments"], 2)  # p2, p3
        self.assertEqual(dist["total_unique_matched_pings"], 4)
        self.assertTrue(dist["reconciled"])

    def test_actual_memberships_csv_overlap_reconciliation(self):
        """Verify actual generated memberships CSV reconciles disjoint overlap categories."""
        csv_path = Path("data/processed/store_catchment_memberships/part-00000.csv")
        if csv_path.exists():
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                memberships = list(reader)

            dist = calculate_overlap_distribution(memberships)
            self.assertEqual(dist["total_unique_matched_pings"], 3956)
            self.assertEqual(dist["pings_exactly_one_catchment"], 3861)
            self.assertEqual(dist["pings_exactly_two_catchments"], 95)
            self.assertEqual(dist["pings_three_or_more_catchments"], 0)
            self.assertEqual(dist["pings_multiple_catchments"], 95)
            self.assertTrue(dist["reconciled"])

            # Verify sum of categories matches total
            reconciled_sum = (
                dist["pings_exactly_one_catchment"]
                + dist["pings_exactly_two_catchments"]
                + dist["pings_three_or_more_catchments"]
            )
            self.assertEqual(reconciled_sum, dist["total_unique_matched_pings"])


class TestSpatialAnalysisConfig(unittest.TestCase):
    """Test configuration dataclass defaults and validation behaviors."""

    def test_default_configuration_values(self):
        """Verify default spatial analytics configuration parameters."""
        cfg = SpatialAnalysisConfig()
        self.assertEqual(cfg.source_crs, "EPSG:4326")
        self.assertEqual(cfg.metric_crs, "EPSG:32643")
        self.assertEqual(cfg.catchment_radius_meters, 500.0)
        self.assertEqual(cfg.expected_store_count, 4)
        self.assertEqual(cfg.area_tolerance_pct, 0.02)
        self.assertEqual(cfg.overwrite, False)
        self.assertEqual(cfg.mobility_input_path, "data/raw/synthetic_mobility_pings.csv")
        self.assertEqual(cfg.store_input_path, "data/reference/synthetic_store_locations.csv")

    def test_custom_configuration_values(self):
        """Verify custom radius, overwrite flag, and output paths can be configured."""
        cfg = SpatialAnalysisConfig(
            catchment_radius_meters=750.0,
            catchments_output_path="custom/catchments",
            overwrite=True,
        )
        self.assertEqual(cfg.catchment_radius_meters, 750.0)
        self.assertEqual(cfg.catchments_output_path, "custom/catchments")
        self.assertTrue(cfg.overwrite)

    def test_config_post_init_rejects_invalid_radius(self):
        """Verify dataclass __post_init__ catches invalid catchment radius immediately."""
        with self.assertRaises(ValueError):
            SpatialAnalysisConfig(catchment_radius_meters=-200.0)
        with self.assertRaises(ValueError):
            SpatialAnalysisConfig(catchment_radius_meters=0.0)
        with self.assertRaises(ValueError):
            SpatialAnalysisConfig(catchment_radius_meters=float("inf"))


class TestSedonaJarDiscovery(unittest.TestCase):
    """Test Sedona JAR discovery helper."""

    def test_find_sedona_jars_constants(self):
        """Verify Sedona shaded JAR constants match verified artifact specification."""
        self.assertEqual(SEDONA_SHADED_JAR_NAME, "sedona-spark-shaded-3.5_2.12-1.9.1.jar")
        self.assertEqual(
            SEDONA_SHADED_JAR_SHA256,
            "0EE2A6886D864C7E52D33B1A08208EFFC89E0778CE03FBE8B35F32E9C235611A",
        )

    def test_find_sedona_jars_selects_only_exact_1_9_1_shaded_jar(self):
        """Verify discovery selects only the 1.9.1 shaded JAR and rejects 1.6.1 or component JARs."""
        with tempfile.TemporaryDirectory() as tmp_home:
            ivy_jars = Path(tmp_home) / ".ivy2" / "jars"
            ivy_jars.mkdir(parents=True)

            # Create various dummy jar files in simulated ivy cache
            target_jar = ivy_jars / SEDONA_SHADED_JAR_NAME
            target_jar.write_bytes(b"dummy 1.9.1 shaded content")

            old_jar = ivy_jars / "org.apache.sedona_sedona-spark-shaded-3.5_2.12-1.6.1.jar"
            old_jar.write_bytes(b"dummy 1.6.1 shaded content")

            component_jar = ivy_jars / "org.apache.sedona_sedona-common-1.9.1.jar"
            component_jar.write_bytes(b"dummy component content")

            geotools_jar = ivy_jars / "geotools-wrapper-1.6.1-28.2.jar"
            geotools_jar.write_bytes(b"dummy geotools content")

            with mock.patch.dict(os.environ, {}, clear=False):
                os.environ.pop("SEDONA_JARS", None)
                with mock.patch("pathlib.Path.home", return_value=Path(tmp_home)):
                    with mock.patch("src.spatial_analysis.compute_file_sha256", return_value=SEDONA_SHADED_JAR_SHA256):
                        jars = find_sedona_jars()
                        self.assertEqual(len(jars), 1)
                        self.assertEqual(jars[0], str(target_jar.resolve()))
                        self.assertNotIn(str(old_jar.resolve()), jars)
                        self.assertNotIn(str(component_jar.resolve()), jars)
                        self.assertNotIn(str(geotools_jar.resolve()), jars)

    def test_find_sedona_jars_rejects_incorrect_checksum_in_ivy_cache(self):
        """Verify discovery rejects Ivy cache JAR if SHA-256 integrity check fails."""
        with tempfile.TemporaryDirectory() as tmp_home:
            ivy_jars = Path(tmp_home) / ".ivy2" / "jars"
            ivy_jars.mkdir(parents=True)

            target_jar = ivy_jars / SEDONA_SHADED_JAR_NAME
            target_jar.write_bytes(b"corrupted or wrong jar content")

            with mock.patch.dict(os.environ, {}, clear=False):
                os.environ.pop("SEDONA_JARS", None)
                with mock.patch("pathlib.Path.home", return_value=Path(tmp_home)):
                    with self.assertRaises(ValueError) as ctx:
                        find_sedona_jars()
                    self.assertIn("Sedona JAR integrity check failed", str(ctx.exception))

    def test_find_sedona_jars_raises_when_target_missing(self):
        """Verify discovery raises FileNotFoundError when 1.9.1 shaded JAR is missing."""
        with tempfile.TemporaryDirectory() as tmp_home:
            ivy_jars = Path(tmp_home) / ".ivy2" / "jars"
            ivy_jars.mkdir(parents=True)

            # Only old and component jars exist
            old_jar = ivy_jars / "org.apache.sedona_sedona-spark-shaded-3.5_2.12-1.6.1.jar"
            old_jar.write_bytes(b"dummy 1.6.1 shaded content")

            component_jar = ivy_jars / "org.apache.sedona_sedona-common-1.9.1.jar"
            component_jar.write_bytes(b"dummy component content")

            with mock.patch.dict(os.environ, {}, clear=False):
                os.environ.pop("SEDONA_JARS", None)
                with mock.patch("pathlib.Path.home", return_value=Path(tmp_home)):
                    with self.assertRaises(FileNotFoundError) as ctx:
                        find_sedona_jars()
                    self.assertIn(SEDONA_SHADED_JAR_NAME, str(ctx.exception))

    def test_find_sedona_jars_valid_env_override(self):
        """Verify find_sedona_jars accepts a valid SEDONA_JARS override pointing to the exact JAR with matching checksum."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            valid_jar = Path(tmp_dir) / SEDONA_SHADED_JAR_NAME
            valid_jar.write_bytes(b"valid jar bytes")

            with mock.patch.dict(os.environ, {"SEDONA_JARS": str(valid_jar)}):
                with mock.patch("src.spatial_analysis.compute_file_sha256", return_value=SEDONA_SHADED_JAR_SHA256):
                    jars = find_sedona_jars()
                    self.assertEqual(jars, [str(valid_jar.resolve())])

    def test_find_sedona_jars_invalid_env_override_wrong_checksum(self):
        """Verify find_sedona_jars rejects SEDONA_JARS override if file contents do not match expected SHA-256."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tampered_jar = Path(tmp_dir) / SEDONA_SHADED_JAR_NAME
            tampered_jar.write_bytes(b"tampered content with bad hash")

            with mock.patch.dict(os.environ, {"SEDONA_JARS": str(tampered_jar)}):
                with self.assertRaises(ValueError) as ctx:
                    find_sedona_jars()
                self.assertIn("Sedona JAR integrity check failed", str(ctx.exception))

    def test_find_sedona_jars_invalid_env_override_wrong_filename(self):
        """Verify find_sedona_jars rejects SEDONA_JARS override with incorrect artifact name."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            wrong_jar = Path(tmp_dir) / "sedona-spark-shaded-3.5_2.12-1.6.1.jar"
            wrong_jar.write_bytes(b"dummy 1.6.1 content")

            with mock.patch.dict(os.environ, {"SEDONA_JARS": str(wrong_jar)}):
                with self.assertRaises(ValueError) as ctx:
                    find_sedona_jars()
                self.assertIn("file name must be exactly", str(ctx.exception))

    def test_find_sedona_jars_invalid_env_override_multiple_files(self):
        """Verify find_sedona_jars rejects SEDONA_JARS override with multiple files."""
        multiple = f"file1.jar{os.pathsep}file2.jar"
        with mock.patch.dict(os.environ, {"SEDONA_JARS": multiple}):
            with self.assertRaises(ValueError) as ctx:
                find_sedona_jars()
            self.assertIn("must specify exactly one JAR file", str(ctx.exception))

    def test_find_sedona_jars_invalid_env_override_missing_file(self):
        """Verify find_sedona_jars rejects SEDONA_JARS override when file does not exist on disk."""
        nonexistent = str(Path(tempfile.gettempdir()) / "nonexistent_dir" / SEDONA_SHADED_JAR_NAME)
        with mock.patch.dict(os.environ, {"SEDONA_JARS": nonexistent}):
            with self.assertRaises(FileNotFoundError) as ctx:
                find_sedona_jars()
            self.assertIn("not found or not a regular file", str(ctx.exception))

    def test_validate_sedona_jar_checksum_helper(self):
        """Verify compute_file_sha256 and validate_sedona_jar_checksum helpers."""
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            tf.write(b"Hello GeoPulse")
            tf_path = Path(tf.name)
        try:
            expected = hashlib.sha256(b"Hello GeoPulse").hexdigest()
            # Matching digest should succeed
            verified = validate_sedona_jar_checksum(tf_path, expected)
            self.assertEqual(verified.lower(), expected.lower())

            # Mismatched digest should raise ValueError
            with self.assertRaises(ValueError) as ctx:
                validate_sedona_jar_checksum(tf_path, "0000000000000000000000000000000000000000000000000000000000000000")
            self.assertIn("Sedona JAR integrity check failed", str(ctx.exception))
        finally:
            if tf_path.exists():
                tf_path.unlink()


class TestBenchmarkPreLaunchEnvironment(unittest.TestCase):
    """Test Spark pre-launch environment configuration in benchmark harness."""

    def test_configure_spark_pre_launch_converts_jars_to_file_uris(self):
        """Verify --jars in PYSPARK_SUBMIT_ARGS uses file:/// URIs, avoiding Hadoop scheme 'C' error."""
        from scripts.benchmark_100k import configure_spark_pre_launch_environment

        test_jars = [r"C:\Users\test\.ivy2\jars\test-jar.jar"]
        with mock.patch.dict(os.environ, {}, clear=False):
            configure_spark_pre_launch_environment(extra_jars=test_jars)
            submit_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "")
            self.assertIn("--jars", submit_args)
            self.assertIn("file:///", submit_args)
            self.assertNotIn("--jars C:", submit_args)

    def test_configure_spark_pre_launch_configures_java_11(self):
        """Verify process-level JAVA_HOME is configured to Java 11 and prepended to PATH."""
        from scripts.benchmark_100k import configure_spark_pre_launch_environment

        test_jars = [r"C:\test\sedona-spark-shaded-3.5_2.12-1.9.1.jar"]
        with mock.patch.dict(os.environ, {"JAVA_HOME": "C:\\global\\jdk-17", "PATH": "C:\\Windows\\system32"}, clear=False):
            configure_spark_pre_launch_environment(extra_jars=test_jars)
            java_home = os.environ.get("JAVA_HOME", "")
            path = os.environ.get("PATH", "")
            self.assertIn("jdk-11", java_home)
            self.assertTrue(path.startswith(str(Path(java_home) / "bin")))

    def test_configure_spark_pre_launch_raises_if_java_11_missing(self):
        """Verify configure_spark_pre_launch_environment raises FileNotFoundError if Java 11 or java.exe is missing."""
        from scripts.benchmark_100k import configure_spark_pre_launch_environment

        with mock.patch("pathlib.Path.is_file", return_value=False):
            with self.assertRaises(FileNotFoundError) as ctx:
                configure_spark_pre_launch_environment(extra_jars=["test.jar"])
            self.assertIn("Required Java 11 installation not found", str(ctx.exception))

    def test_configure_spark_pre_launch_raises_if_no_jars(self):
        """Verify configure_spark_pre_launch_environment raises RuntimeError if no jars provided."""
        from scripts.benchmark_100k import configure_spark_pre_launch_environment

        with mock.patch("src.spatial_analysis.find_sedona_jars", return_value=[]):
            with self.assertRaises(RuntimeError) as ctx:
                configure_spark_pre_launch_environment(extra_jars=[])
            self.assertIn("no verified Sedona JAR was provided or located", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
