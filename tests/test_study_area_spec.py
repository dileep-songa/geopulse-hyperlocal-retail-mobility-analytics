"""Unit tests for GeoPulse study area specifications and schema contract."""

import unittest
import sys
from pathlib import Path

# Add project root to sys.path so tests can import from src
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import (
    STUDY_AREA,
    SYNTHETIC_GPS_SCHEMA,
    MovementMode,
    validate_coordinate,
)


class TestStudyAreaSpecification(unittest.TestCase):
    """Test suite validating geographic study area bounds and spatial conventions."""

    def test_bounding_box_ordering_and_ranges(self):
        """Verify min coordinates are strictly less than max coordinates and within global ranges."""
        bounds = STUDY_AREA.bounds

        # Longitudinal checks
        self.assertLess(bounds.min_longitude, bounds.max_longitude)
        self.assertGreaterEqual(bounds.min_longitude, -180.0)
        self.assertLessEqual(bounds.max_longitude, 180.0)

        # Latitudinal checks
        self.assertLess(bounds.min_latitude, bounds.max_latitude)
        self.assertGreaterEqual(bounds.min_latitude, -90.0)
        self.assertLessEqual(bounds.max_latitude, 90.0)

    def test_compact_dimensions_for_local_development(self):
        """Ensure study area footprint is compact enough for rapid local processing."""
        bounds = STUDY_AREA.bounds

        # Spans should be positive and compact (< 0.10 degrees, ~11 km)
        self.assertGreater(bounds.longitude_span, 0.0)
        self.assertLess(bounds.longitude_span, 0.10)

        self.assertGreater(bounds.latitude_span, 0.0)
        self.assertLess(bounds.latitude_span, 0.10)

    def test_centroid_calculation(self):
        """Verify centroid lies strictly within the bounding box."""
        bounds = STUDY_AREA.bounds
        c_lon, c_lat = bounds.centroid

        self.assertTrue(bounds.contains(c_lon, c_lat))
        self.assertAlmostEqual(c_lon, 77.6400, places=4)
        self.assertAlmostEqual(c_lat, 12.9775, places=4)

    def test_bounding_box_contains_predicate(self):
        """Test spatial containment method with inside and outside points."""
        bounds = STUDY_AREA.bounds

        # Known point inside Indiranagar 100 Feet Rd corridor
        self.assertTrue(bounds.contains(77.6418, 12.9782))

        # Points outside the bounding box
        self.assertFalse(bounds.contains(77.5946, 12.9716))  # MG Road (West of bounds)
        self.assertFalse(bounds.contains(77.6600, 12.9782))  # East of bounds
        self.assertFalse(bounds.contains(77.6418, 12.9600))  # South of bounds
        self.assertFalse(bounds.contains(77.6418, 13.0000))  # North of bounds

    def test_coordinate_system_conventions(self):
        """Validate standard CRS and axis ordering convention."""
        self.assertEqual(STUDY_AREA.crs, "EPSG:4326")
        self.assertEqual(STUDY_AREA.coordinate_order, "longitude_first")

    def test_global_coordinate_validator(self):
        """Validate boundary checking function for geodetic values."""
        self.assertIsNone(validate_coordinate(77.6400, 12.9775))
        self.assertIsNotNone(validate_coordinate(195.0, 12.0))
        self.assertIsNotNone(validate_coordinate(77.0, -95.0))


class TestSyntheticGPSSchemaContract(unittest.TestCase):
    """Test suite validating the schema dictionary and constraints."""

    def test_required_schema_fields(self):
        """Verify that all 8 core fields exist and are marked required."""
        expected_fields = [
            "ping_id",
            "device_id",
            "event_timestamp",
            "latitude",
            "longitude",
            "accuracy_meters",
            "movement_mode",
            "data_provenance",
        ]
        for field in expected_fields:
            self.assertIn(field, SYNTHETIC_GPS_SCHEMA)
            self.assertTrue(
                SYNTHETIC_GPS_SCHEMA[field]["required"],
                f"Field {field} must be marked required",
            )

    def test_movement_mode_allowed_values(self):
        """Verify movement modes match enum values."""
        allowed = SYNTHETIC_GPS_SCHEMA["movement_mode"]["allowed_values"]
        expected = [
            MovementMode.STATIONARY_DWELL.value,
            MovementMode.PEDESTRIAN_WALKING.value,
            MovementMode.VEHICULAR_TRANSIT.value,
        ]
        self.assertEqual(sorted(allowed), sorted(expected))

    def test_data_provenance_tag(self):
        """Verify provenance tag adheres to constant contract."""
        self.assertEqual(
            SYNTHETIC_GPS_SCHEMA["data_provenance"]["expected_value"],
            "geopulse_synthetic_v1",
        )


if __name__ == "__main__":
    unittest.main()
