"""GeoPulse configuration module.

Defines the geographic study area boundaries, coordinate reference conventions,
synthetic GPS schema specifications, and kinematic constants for the GeoPulse platform.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional


class MovementMode(str, Enum):
    """Allowed movement modes for synthetic mobility observations."""

    STATIONARY_DWELL = "stationary_dwell"
    PEDESTRIAN_WALKING = "pedestrian_walking"
    VEHICULAR_TRANSIT = "vehicular_transit"


@dataclass(frozen=True)
class BoundingBox:
    """Geodetic bounding box in WGS 84 (EPSG:4326) coordinates.

    Convention:
        - Longitude corresponds to the X axis (Eastings).
        - Latitude corresponds to the Y axis (Northings).
        - Points are constructed as (longitude, latitude).
    """

    min_longitude: float
    max_longitude: float
    min_latitude: float
    max_latitude: float

    def contains(self, longitude: float, latitude: float) -> bool:
        """Check whether a given (longitude, latitude) coordinate falls within the bounds."""
        return (
            self.min_longitude <= longitude <= self.max_longitude
            and self.min_latitude <= latitude <= self.max_latitude
        )

    @property
    def longitude_span(self) -> float:
        """Span in decimal degrees longitude."""
        return self.max_longitude - self.min_longitude

    @property
    def latitude_span(self) -> float:
        """Span in decimal degrees latitude."""
        return self.max_latitude - self.min_latitude

    @property
    def centroid(self) -> tuple[float, float]:
        """Centroid of the bounding box as (longitude, latitude)."""
        return (
            (self.min_longitude + self.max_longitude) / 2.0,
            (self.min_latitude + self.max_latitude) / 2.0,
        )


@dataclass(frozen=True)
class StudyAreaConfig:
    """Study area geographic metadata and boundary configuration."""

    city: str = "Bengaluru"
    locality: str = "Indiranagar"
    state: str = "Karnataka"
    country: str = "India"
    crs: str = "EPSG:4326"
    coordinate_order: str = "longitude_first"

    bounds: BoundingBox = BoundingBox(
        min_longitude=77.6250,
        max_longitude=77.6550,
        min_latitude=12.9650,
        max_latitude=12.9900,
    )


# Default study area configuration
STUDY_AREA = StudyAreaConfig()

# Provenance and reproducibility constants
DATA_PROVENANCE_TAG = "geopulse_synthetic_v1"
DEFAULT_RANDOM_SEED = 42

# Timezone convention
TIMEZONE_STORAGE = "UTC"
TIMEZONE_LOCAL = "Asia/Kolkata"

# Field-by-field schema contract for synthetic GPS pings
SYNTHETIC_GPS_SCHEMA: Dict[str, Dict[str, Any]] = {
    "ping_id": {
        "type": "string",
        "format": "uuidv4",
        "required": True,
        "description": "Unique RFC 4122 UUIDv4 identifier for the observation ping.",
    },
    "device_id": {
        "type": "string",
        "format": "uuidv4",
        "required": True,
        "description": "Pseudonymous synthetic agent UUIDv4 identifier.",
    },
    "event_timestamp": {
        "type": "string",
        "format": "iso8601_utc",
        "required": True,
        "description": "Observation timestamp in UTC (YYYY-MM-DDTHH:MM:SS.sssZ).",
    },
    "latitude": {
        "type": "float",
        "min": STUDY_AREA.bounds.min_latitude,
        "max": STUDY_AREA.bounds.max_latitude,
        "required": True,
        "description": "WGS 84 latitude in decimal degrees.",
    },
    "longitude": {
        "type": "float",
        "min": STUDY_AREA.bounds.min_longitude,
        "max": STUDY_AREA.bounds.max_longitude,
        "required": True,
        "description": "WGS 84 longitude in decimal degrees.",
    },
    "accuracy_meters": {
        "type": "float",
        "min": 2.0,
        "max": 35.0,
        "required": True,
        "description": "Simulated horizontal positional uncertainty radius in meters.",
    },
    "movement_mode": {
        "type": "enum",
        "allowed_values": [mode.value for mode in MovementMode],
        "required": True,
        "description": "Kinematic movement mode at observation time.",
    },
    "data_provenance": {
        "type": "string",
        "expected_value": DATA_PROVENANCE_TAG,
        "required": True,
        "description": "Synthetic simulation provenance identifier.",
    },
}


def validate_coordinate(longitude: float, latitude: float) -> Optional[str]:
    """Validate that coordinates are within legitimate global geodetic ranges.

    Returns:
        Error message string if invalid, None if valid.
    """
    if not (-180.0 <= longitude <= 180.0):
        return f"Longitude {longitude} is out of global range [-180.0, 180.0]."
    if not (-90.0 <= latitude <= 90.0):
        return f"Latitude {latitude} is out of global range [-90.0, 90.0]."
    return None
