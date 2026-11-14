"""GeoPulse data quality and mobility trajectory validation engine.

Provides independent, reproducible verification and validation for synthetic GPS mobility
telemetry datasets. Validates CSV structural integrity, schema compliance, record uniqueness,
geographic bounding box adherence, temporal consistency, and kinematic trajectory rules.

Built strictly with Python's standard library.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import math
from pathlib import Path
import re
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple
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

# UUIDv4 regex pattern for strict string format checking
UUIDV4_REGEX = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)

# Coordinate precision regex for exact 6 decimal places storage format
COORD_6DEC_REGEX = re.compile(r"^-?\d+\.\d{6}$")

# Strict ISO-8601 UTC timestamp regex ending in literal 'Z': YYYY-MM-DDTHH:MM:SS[.fraction]Z
ISO8601_UTC_REGEX = re.compile(
    r"^\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+)?Z$"
)

# Approved movement transitions (7 valid combinations)
VALID_MOVEMENT_TRANSITIONS: Dict[Tuple[str, str], str] = {
    (MovementMode.PEDESTRIAN_WALKING.value, MovementMode.PEDESTRIAN_WALKING.value): "intra_walking",
    (MovementMode.VEHICULAR_TRANSIT.value, MovementMode.VEHICULAR_TRANSIT.value): "intra_vehicular",
    (MovementMode.STATIONARY_DWELL.value, MovementMode.STATIONARY_DWELL.value): "intra_dwell",
    (MovementMode.PEDESTRIAN_WALKING.value, MovementMode.STATIONARY_DWELL.value): "arrival_walking_to_dwell",
    (MovementMode.VEHICULAR_TRANSIT.value, MovementMode.STATIONARY_DWELL.value): "arrival_vehicular_to_dwell",
    (MovementMode.STATIONARY_DWELL.value, MovementMode.PEDESTRIAN_WALKING.value): "departure_dwell_to_walking",
    (MovementMode.STATIONARY_DWELL.value, MovementMode.VEHICULAR_TRANSIT.value): "departure_dwell_to_vehicular",
}


@dataclass(frozen=True)
class ValidationFailure:
    """Represents a discrete validation check failure."""

    category: str
    check_name: str
    identifier: str
    message: str
    value: Optional[Any] = None

    def __str__(self) -> str:
        val_str = f" (observed: {self.value!r})" if self.value is not None else ""
        return f"[{self.category.upper()}] {self.check_name} | ID: {self.identifier} | {self.message}{val_str}"


@dataclass
class ValidationMetrics:
    """Descriptive statistics and validation metrics computed across valid records."""

    total_records: int = 0
    valid_measurement_records: int = 0
    malformed_records: int = 0
    unique_devices: int = 0
    unique_ping_ids: int = 0
    base_simulation_date: Optional[str] = None
    min_latitude: float = 0.0
    max_latitude: float = 0.0
    min_longitude: float = 0.0
    max_longitude: float = 0.0
    latitude_span: float = 0.0
    longitude_span: float = 0.0
    min_accuracy: float = 0.0
    max_accuracy: float = 0.0
    mode_counts: Dict[str, int] = field(default_factory=dict)
    mode_percentages: Dict[str, float] = field(default_factory=dict)
    diurnal_counts: Dict[str, int] = field(default_factory=dict)
    diurnal_percentages: Dict[str, float] = field(default_factory=dict)
    quadrant_counts: Dict[str, int] = field(default_factory=dict)
    quadrant_percentages: Dict[str, float] = field(default_factory=dict)
    pings_per_device_min: int = 0
    pings_per_device_max: int = 0
    pings_per_device_avg: float = 0.0
    consecutive_pairs_evaluated: int = 0
    pair_transition_counts: Dict[str, int] = field(default_factory=dict)
    measured_speed_ranges: Dict[str, Tuple[float, float]] = field(default_factory=dict)


@dataclass
class ValidationResult:
    """Overall outcome of the dataset quality and mobility validation audit."""

    file_path: str
    file_sha256: str
    total_records: int
    is_valid: bool
    failures: List[ValidationFailure] = field(default_factory=list)
    metrics: Optional[ValidationMetrics] = None

    @property
    def total_failures(self) -> int:
        return len(self.failures)

    @property
    def failures_by_category(self) -> Dict[str, List[ValidationFailure]]:
        categorized: Dict[str, List[ValidationFailure]] = {}
        for f in self.failures:
            categorized.setdefault(f.category, []).append(f)
        return categorized


def compute_file_sha256(path: Path | str) -> str:
    """Compute SHA-256 hash of a file without reading the entire file into memory."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def is_finite_number(val: float) -> bool:
    """Check if a floating-point number is finite (neither NaN nor Infinity)."""
    return not (math.isnan(val) or math.isinf(val))


def check_finite_float(val_str: Any) -> Tuple[Optional[float], Optional[str]]:
    """Convert input to float and check for finiteness.

    Returns:
        (float_value, None) on success.
        (None, "non_numeric") if conversion fails.
        (None, "non_finite") if value is NaN, +Inf, or -Inf.
    """
    if val_str is None:
        return None, "non_numeric"
    try:
        val = float(val_str)
        if not is_finite_number(val):
            return None, "non_finite"
        return val, None
    except (ValueError, TypeError):
        return None, "non_numeric"


def parse_finite_float(val_str: Any) -> Optional[float]:
    """Parse string or numeric value to float, returning None if non-numeric, NaN, or infinite."""
    val, _ = check_finite_float(val_str)
    return val


def parse_iso8601_utc(ts_str: str) -> Optional[datetime]:
    """Strict ISO-8601 UTC timestamp parser.

    Requirements:
    - String must strictly conform to YYYY-MM-DDTHH:MM:SS[.fraction]Z ending in literal 'Z'.
    - Must represent a valid calendar date and time.
    - Result must be a timezone-aware datetime in UTC.

    Returns:
        Timezone-aware datetime in UTC, or None if invalid.
    """
    if not isinstance(ts_str, str) or not ISO8601_UTC_REGEX.match(ts_str):
        return None
    try:
        dt_parsed = datetime.fromisoformat(ts_str[:-1] + "+00:00")
        if dt_parsed.tzinfo is None:
            return None
        return dt_parsed.astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None


def get_diurnal_window_boundaries(
    base_date: date,
    local_tz: Any,
) -> Dict[str, Tuple[datetime, datetime]]:
    """Define exact, unambiguous diurnal window boundaries for a given simulation date in local time.

    Activity Windows (Asia/Kolkata):
        - Morning Commute: 08:00:00 to 11:00:00 IST (inclusive)
        - Midday Work / Lunch: 11:30:00 to 15:30:00 IST (inclusive)
        - Evening Social Peak: 17:00:00 to 21:30:00 IST (inclusive)
        - Nighttime Low: 22:00:00 IST on base_date to 07:00:00 IST on base_date + 1 day (inclusive)

    Any timestamp outside these four inclusive ranges falls into an excluded non-activity gap.
    """
    next_date = base_date + timedelta(days=1)
    return {
        "morning_commute": (
            datetime.combine(base_date, time(8, 0, 0), tzinfo=local_tz),
            datetime.combine(base_date, time(11, 0, 0), tzinfo=local_tz),
        ),
        "midday_work_lunch": (
            datetime.combine(base_date, time(11, 30, 0), tzinfo=local_tz),
            datetime.combine(base_date, time(15, 30, 0), tzinfo=local_tz),
        ),
        "evening_social_peak": (
            datetime.combine(base_date, time(17, 0, 0), tzinfo=local_tz),
            datetime.combine(base_date, time(21, 30, 0), tzinfo=local_tz),
        ),
        "nighttime_low": (
            datetime.combine(base_date, time(22, 0, 0), tzinfo=local_tz),
            datetime.combine(next_date, time(7, 0, 0), tzinfo=local_tz),
        ),
    }


def classify_diurnal_window(
    dt_local: datetime,
    base_date: date,
    local_tz: Any,
) -> Tuple[Optional[str], Optional[str]]:
    """Classify local timestamp into an activity window or identify the excluded gap.

    The simulation model is designed for a single diurnal run:
    - Base date D covers morning (08:00-11:00), midday (11:30-15:30), evening (17:00-21:30),
      and night start (22:00-23:59:59).
    - Date D+1 covers night end (00:00-07:00).
    - Timestamps after 07:00:00 IST on date D+1, or before 08:00:00 IST on date D,
      fall outside the simulation calendar and are rejected.

    Returns:
        (window_name, None) if inside an approved window.
        (None, gap_description) if inside an excluded gap period or outside simulation calendar.
    """
    windows = get_diurnal_window_boundaries(base_date, local_tz)

    for w_name, (w_start, w_end) in windows.items():
        if w_start <= dt_local <= w_end:
            return w_name, None

    # Excluded gap and boundary identification
    m_start = windows["morning_commute"][0]
    m_end = windows["morning_commute"][1]
    l_start = windows["midday_work_lunch"][0]
    l_end = windows["midday_work_lunch"][1]
    e_start = windows["evening_social_peak"][0]
    e_end = windows["evening_social_peak"][1]
    n_start = windows["nighttime_low"][0]
    n_end = windows["nighttime_low"][1]

    if dt_local < m_start:
        return None, f"Prior to simulation start (before 08:00:00 IST on {base_date})"
    elif m_end < dt_local < l_start:
        return None, "Morning-to-Midday Gap (11:00:00 to 11:30:00 IST)"
    elif l_end < dt_local < e_start:
        return None, "Midday-to-Evening Gap (15:30:00 to 17:00:00 IST)"
    elif e_end < dt_local < n_start:
        return None, "Evening-to-Night Gap (21:30:00 to 22:00:00 IST)"
    elif n_end < dt_local < datetime.combine(base_date + timedelta(days=1), time(8, 0, 0), tzinfo=local_tz):
        return None, "Night-to-Morning Gap (07:00:00 to 08:00:00 IST)"
    else:
        return None, f"Outside simulation calendar window (after 07:00:00 IST on {base_date + timedelta(days=1)})"


def load_and_validate_csv_structure(
    csv_path: Path | str,
    expected_columns: Sequence[str] = CSV_COLUMNS,
) -> Tuple[List[Dict[str, str]], List[ValidationFailure], str]:
    """Load raw CSV records and validate structural header conformity.

    Uses strict CSV parsing (csv.reader with strict=True) to reject malformed quoting,
    unescaped delimiters, and invalid row structure. Handles file access, encoding,
    and parsing exceptions explicitly.

    If header is missing, duplicated, reordered, contains unexpected/missing columns,
    or the file contains zero records, reports structural failure(s).

    Returns:
        Tuple of (loaded rows, list of structural failures, file SHA-256 checksum).
    """
    path = Path(csv_path)
    failures: List[ValidationFailure] = []

    if not path.exists():
        failures.append(
            ValidationFailure(
                category="structural",
                check_name="file_existence",
                identifier=str(path),
                message="Target dataset file does not exist",
                value=str(path),
            )
        )
        return [], failures, ""

    if not path.is_file():
        failures.append(
            ValidationFailure(
                category="structural",
                check_name="file_type",
                identifier=str(path),
                message="Target path is not a regular file",
                value=str(path),
            )
        )
        return [], failures, ""

    try:
        file_sha256 = compute_file_sha256(path)
    except (OSError, PermissionError) as exc:
        failures.append(
            ValidationFailure(
                category="structural",
                check_name="file_read_error",
                identifier=str(path),
                message=f"Failed to read file for checksum computation: {exc}",
                value=str(exc),
            )
        )
        return [], failures, ""

    rows: List[Dict[str, str]] = []

    try:
        with open(path, mode="r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f, strict=True)
            try:
                header = next(reader, None)
            except csv.Error as exc:
                failures.append(
                    ValidationFailure(
                        category="structural",
                        check_name="csv_parse_exception",
                        identifier=str(path),
                        message=f"Strict CSV header parse error: {exc}",
                        value=str(exc),
                    )
                )
                return [], failures, file_sha256

            if header is None:
                failures.append(
                    ValidationFailure(
                        category="structural",
                        check_name="empty_file",
                        identifier=str(path),
                        message="CSV file is empty (missing header row)",
                    )
                )
                return [], failures, file_sha256

            # 1. Check duplicate columns in header
            if len(header) != len(set(header)):
                failures.append(
                    ValidationFailure(
                        category="structural",
                        check_name="duplicate_column_headers",
                        identifier="header_row",
                        message=f"CSV header contains duplicate column names: {header}",
                        value=header,
                    )
                )
                return [], failures, file_sha256

            # 2. Check missing or unexpected columns
            missing_cols = set(expected_columns) - set(header)
            unexpected_cols = set(header) - set(expected_columns)
            if missing_cols or unexpected_cols:
                failures.append(
                    ValidationFailure(
                        category="structural",
                        check_name="column_set_mismatch",
                        identifier="header_row",
                        message=f"CSV header columns mismatch. Missing: {sorted(missing_cols)}, Unexpected: {sorted(unexpected_cols)}",
                        value=header,
                    )
                )
                return [], failures, file_sha256

            # 3. Check exact column order
            if list(header) != list(expected_columns):
                failures.append(
                    ValidationFailure(
                        category="structural",
                        check_name="column_order_mismatch",
                        identifier="header_row",
                        message=f"CSV columns are reordered. Expected canonical order: {list(expected_columns)}, got: {list(header)}",
                        value=header,
                    )
                )
                return [], failures, file_sha256

            # Read data rows under strict mode
            expected_col_count = len(expected_columns)
            row_idx = 1
            for row in reader:
                row_idx += 1
                # Check for blank / empty rows
                if len(row) == 0 or (len(row) == 1 and row[0].strip() == ""):
                    failures.append(
                        ValidationFailure(
                            category="structural",
                            check_name="blank_record",
                            identifier=f"row_{row_idx}",
                            message="CSV contains a blank or empty record",
                            value="",
                        )
                    )
                    continue

                if len(row) != expected_col_count:
                    failures.append(
                        ValidationFailure(
                            category="structural",
                            check_name="row_field_count",
                            identifier=f"row_{row_idx}",
                            message=f"Row has {len(row)} fields, expected {expected_col_count}",
                            value=len(row),
                        )
                    )
                    continue

                row_dict = dict(zip(header, row))

                # Check for empty or whitespace-only values
                for col, val in row_dict.items():
                    if val is None or val.strip() == "":
                        failures.append(
                            ValidationFailure(
                                category="structural",
                                check_name="missing_value",
                                identifier=f"row_{row_idx}:{col}",
                                message=f"Field '{col}' is missing or blank",
                                value=val,
                            )
                        )

                rows.append(row_dict)

            # Check for header-only file (zero observation records)
            if len(rows) == 0:
                failures.append(
                    ValidationFailure(
                        category="structural",
                        check_name="zero_records",
                        identifier=str(path),
                        message="CSV contains only a header row with zero observation records",
                        value=0,
                    )
                )

    except UnicodeDecodeError as exc:
        failures.append(
            ValidationFailure(
                category="structural",
                check_name="encoding_error",
                identifier=str(path),
                message=f"CSV file is not valid UTF-8: {exc}",
                value=str(exc),
            )
        )
    except csv.Error as exc:
        failures.append(
            ValidationFailure(
                category="structural",
                check_name="csv_parse_exception",
                identifier=str(path),
                message=f"Strict CSV parsing error: {exc}",
                value=str(exc),
            )
        )
    except (OSError, PermissionError) as exc:
        failures.append(
            ValidationFailure(
                category="structural",
                check_name="file_read_error",
                identifier=str(path),
                message=f"I/O error accessing CSV file: {exc}",
                value=str(exc),
            )
        )

    return rows, failures, file_sha256


def validate_record_integrity(
    rows: List[Dict[str, str]],
    expected_devices: Optional[int] = None,
) -> List[ValidationFailure]:
    """Validate uniqueness, UUID formatting, provenance tags, and categorical enums."""
    failures: List[ValidationFailure] = []
    seen_ping_ids: set[str] = set()
    device_ping_counts: Dict[str, int] = {}
    valid_modes = {mode.value for mode in MovementMode}

    for row_idx, r in enumerate(rows, start=2):
        row_id = f"row_{row_idx}"
        ping_id = r.get("ping_id", "")
        device_id = r.get("device_id", "")
        mode = r.get("movement_mode", "")
        provenance = r.get("data_provenance", "")

        # Ping ID UUIDv4 format check
        if not UUIDV4_REGEX.match(ping_id):
            failures.append(
                ValidationFailure(
                    category="record_integrity",
                    check_name="ping_id_uuidv4_format",
                    identifier=f"{row_id}:ping_id",
                    message="ping_id is not a valid RFC 4122 UUIDv4",
                    value=ping_id,
                )
            )

        # Ping ID uniqueness check
        if ping_id in seen_ping_ids:
            failures.append(
                ValidationFailure(
                    category="record_integrity",
                    check_name="ping_id_uniqueness",
                    identifier=f"{row_id}:ping_id",
                    message=f"Duplicate ping_id detected: {ping_id}",
                    value=ping_id,
                )
            )
        else:
            seen_ping_ids.add(ping_id)

        # Device ID UUIDv4 format check
        if not UUIDV4_REGEX.match(device_id):
            failures.append(
                ValidationFailure(
                    category="record_integrity",
                    check_name="device_id_uuidv4_format",
                    identifier=f"{row_id}:device_id",
                    message="device_id is not a valid RFC 4122 UUIDv4",
                    value=device_id,
                )
            )
        else:
            device_ping_counts[device_id] = device_ping_counts.get(device_id, 0) + 1

        # Provenance constant check
        if provenance != DATA_PROVENANCE_TAG:
            failures.append(
                ValidationFailure(
                    category="record_integrity",
                    check_name="data_provenance_tag",
                    identifier=f"{row_id}:data_provenance",
                    message=f"data_provenance does not match approved tag '{DATA_PROVENANCE_TAG}'",
                    value=provenance,
                )
            )

        # Movement mode enum check
        if mode not in valid_modes:
            failures.append(
                ValidationFailure(
                    category="record_integrity",
                    check_name="movement_mode_enum",
                    identifier=f"{row_id}:movement_mode",
                    message=f"movement_mode '{mode}' is not in allowed enum values {valid_modes}",
                    value=mode,
                )
            )

    # Device count validation (if explicit count is configured)
    if expected_devices is not None and rows:
        if len(device_ping_counts) != expected_devices:
            failures.append(
                ValidationFailure(
                    category="record_integrity",
                    check_name="expected_device_count",
                    identifier="dataset",
                    message=f"Unique device count mismatch: expected {expected_devices}, found {len(device_ping_counts)}",
                    value=len(device_ping_counts),
                )
            )

    # General multi-ping device trajectory reuse check
    if rows and len(rows) > 1 and len(device_ping_counts) == len(rows):
        failures.append(
            ValidationFailure(
                category="record_integrity",
                check_name="device_id_reuse",
                identifier="dataset",
                message="Every observation belongs to a unique device; no multi-point device trajectories detected",
                value=f"{len(device_ping_counts)} devices across {len(rows)} records",
            )
        )

    return failures


def validate_geography(
    rows: List[Dict[str, str]],
    bounds: BoundingBox = STUDY_AREA.bounds,
) -> List[ValidationFailure]:
    """Validate numeric ranges, global geodetic validity, precision, and bounding box enclosure.

    Explicitly rejects NaN, infinity, non-numeric strings, and precision violations.
    """
    failures: List[ValidationFailure] = []

    for row_idx, r in enumerate(rows, start=2):
        row_id = f"row_{row_idx}"
        lat_str = r.get("latitude", "")
        lon_str = r.get("longitude", "")
        acc_str = r.get("accuracy_meters", "")

        # 1. Latitude finite float check
        lat, lat_err = check_finite_float(lat_str)
        if lat_err == "non_numeric":
            failures.append(
                ValidationFailure(
                    category="geographic",
                    check_name="latitude_numeric",
                    identifier=f"{row_id}:latitude",
                    message="Latitude is not a valid floating-point number",
                    value=lat_str,
                )
            )
        elif lat_err == "non_finite":
            failures.append(
                ValidationFailure(
                    category="geographic",
                    check_name="latitude_finite",
                    identifier=f"{row_id}:latitude",
                    message=f"Latitude must be a finite real number, got '{lat_str}'",
                    value=lat_str,
                )
            )

        # 2. Longitude finite float check
        lon, lon_err = check_finite_float(lon_str)
        if lon_err == "non_numeric":
            failures.append(
                ValidationFailure(
                    category="geographic",
                    check_name="longitude_numeric",
                    identifier=f"{row_id}:longitude",
                    message="Longitude is not a valid floating-point number",
                    value=lon_str,
                )
            )
        elif lon_err == "non_finite":
            failures.append(
                ValidationFailure(
                    category="geographic",
                    check_name="longitude_finite",
                    identifier=f"{row_id}:longitude",
                    message=f"Longitude must be a finite real number, got '{lon_str}'",
                    value=lon_str,
                )
            )

        # 3. Accuracy meters finite float check
        acc, acc_err = check_finite_float(acc_str)
        if acc_err == "non_numeric":
            failures.append(
                ValidationFailure(
                    category="geographic",
                    check_name="accuracy_numeric",
                    identifier=f"{row_id}:accuracy_meters",
                    message="accuracy_meters is not a valid floating-point number",
                    value=acc_str,
                )
            )
        elif acc_err == "non_finite":
            failures.append(
                ValidationFailure(
                    category="geographic",
                    check_name="accuracy_finite",
                    identifier=f"{row_id}:accuracy_meters",
                    message=f"accuracy_meters must be a finite real number, got '{acc_str}'",
                    value=acc_str,
                )
            )

        # 4. Precision format check (exactly 6 decimal places per Day 1 contract)
        if lat is not None and not COORD_6DEC_REGEX.match(lat_str):
            failures.append(
                ValidationFailure(
                    category="geographic",
                    check_name="latitude_precision",
                    identifier=f"{row_id}:latitude",
                    message="Latitude does not match exact 6-decimal storage specification",
                    value=lat_str,
                )
            )

        if lon is not None and not COORD_6DEC_REGEX.match(lon_str):
            failures.append(
                ValidationFailure(
                    category="geographic",
                    check_name="longitude_precision",
                    identifier=f"{row_id}:longitude",
                    message="Longitude does not match exact 6-decimal storage specification",
                    value=lon_str,
                )
            )

        # 5. Global geodetic validity
        if lat is not None and not (-90.0 <= lat <= 90.0):
            failures.append(
                ValidationFailure(
                    category="geographic",
                    check_name="global_latitude_bounds",
                    identifier=f"{row_id}:latitude",
                    message="Latitude exceeds global geodetic range [-90.0, 90.0]",
                    value=lat,
                )
            )

        if lon is not None and not (-180.0 <= lon <= 180.0):
            failures.append(
                ValidationFailure(
                    category="geographic",
                    check_name="global_longitude_bounds",
                    identifier=f"{row_id}:longitude",
                    message="Longitude exceeds global geodetic range [-180.0, 180.0]",
                    value=lon,
                )
            )

        # 6. Study-area bounding box enclosure (within configured bounding box)
        if lat is not None and lon is not None:
            if not bounds.contains(lon, lat):
                failures.append(
                    ValidationFailure(
                        category="geographic",
                        check_name="study_area_containment",
                        identifier=f"{row_id}:coordinates",
                        message=f"Coordinate ({lon}, {lat}) escaped study area bounding box [{bounds.min_longitude}, {bounds.max_longitude}] x [{bounds.min_latitude}, {bounds.max_latitude}]",
                        value=(lon, lat),
                    )
                )

        # 7. Accuracy meters range [2.0, 35.0]
        if acc is not None:
            if not (2.0 <= acc <= 35.0):
                failures.append(
                    ValidationFailure(
                        category="geographic",
                        check_name="accuracy_meters_range",
                        identifier=f"{row_id}:accuracy_meters",
                        message="accuracy_meters is outside allowed range [2.0, 35.0]",
                        value=acc,
                    )
                )

    return failures


def validate_temporal(
    rows: List[Dict[str, str]],
    bounds: BoundingBox = STUDY_AREA.bounds,
    simulation_date: Optional[str] = None,
    local_tz: Any = None,
) -> Tuple[List[ValidationFailure], Dict[str, List[Dict[str, Any]]], Optional[date]]:
    """Validate ISO-8601 UTC timestamp format, chronological ordering, and Day 1 activity windows.

    Preserves CSV observation order for each device. Reports out-of-order or duplicate timestamps.
    Derives base simulation date dynamically from earliest valid local timestamp if not explicitly supplied.
    Evaluates coordinate precision and bounds against configured bounds to determine mobility pair eligibility.

    Returns:
        Tuple of (validation failures, per-device observation dictionary in CSV order, base_date).
    """
    failures: List[ValidationFailure] = []
    device_observations: Dict[str, List[Dict[str, Any]]] = {}

    if local_tz is None:
        local_tz = get_local_timezone()

    # Determine base simulation date
    base_sim_date: Optional[date] = None
    if simulation_date is not None:
        try:
            base_sim_date = datetime.strptime(simulation_date, "%Y-%m-%d").date()
        except ValueError:
            failures.append(
                ValidationFailure(
                    category="temporal",
                    check_name="simulation_date_format",
                    identifier="config",
                    message=f"Invalid simulation_date format '{simulation_date}'. Expected 'YYYY-MM-DD'",
                    value=simulation_date,
                )
            )

    # First pass: parse timestamps with strict parse_iso8601_utc and collect valid local datetimes
    parsed_timestamps: List[Optional[datetime]] = []
    valid_local_dts: List[datetime] = []

    for row_idx, r in enumerate(rows, start=2):
        row_id = f"row_{row_idx}"
        ts_str = r.get("event_timestamp", "")

        # Must end with literal 'Z'
        if not ts_str.endswith("Z"):
            failures.append(
                ValidationFailure(
                    category="temporal",
                    check_name="utc_z_suffix",
                    identifier=f"{row_id}:event_timestamp",
                    message="Timestamp must be serialized as ISO-8601 UTC ending with 'Z'",
                    value=ts_str,
                )
            )
            parsed_timestamps.append(None)
            continue

        dt_utc = parse_iso8601_utc(ts_str)
        if dt_utc is None:
            failures.append(
                ValidationFailure(
                    category="temporal",
                    check_name="timestamp_iso8601_parse",
                    identifier=f"{row_id}:event_timestamp",
                    message="Cannot parse timestamp as valid strict ISO-8601 UTC (expected YYYY-MM-DDTHH:MM:SS[.fraction]Z)",
                    value=ts_str,
                )
            )
            parsed_timestamps.append(None)
            continue

        parsed_timestamps.append(dt_utc)
        valid_local_dts.append(dt_utc.astimezone(local_tz))

    # Dynamic date inference: earliest valid local timestamp
    if base_sim_date is None and valid_local_dts:
        base_sim_date = min(dt.date() for dt in valid_local_dts)

    # Second pass: validate activity window compliance and group by device in CSV order
    for row_idx, (r, dt_utc) in enumerate(zip(rows, parsed_timestamps), start=2):
        row_id = f"row_{row_idx}"
        ts_str = r.get("event_timestamp", "")

        if dt_utc is not None and base_sim_date is not None:
            dt_local = dt_utc.astimezone(local_tz)
            window_name, gap_reason = classify_diurnal_window(dt_local, base_sim_date, local_tz)

            if window_name is None:
                failures.append(
                    ValidationFailure(
                        category="temporal",
                        check_name="activity_window_compliance",
                        identifier=f"{row_id}:{r.get('ping_id')}",
                        message=f"Observation falls in excluded period: {gap_reason} at {dt_local.strftime('%Y-%m-%d %H:%M:%S IST')}",
                        value=dt_local.isoformat(),
                    )
                )

        lat_str = r.get("latitude", "")
        lon_str = r.get("longitude", "")
        lat = parse_finite_float(lat_str)
        lon = parse_finite_float(lon_str)
        acc = parse_finite_float(r.get("accuracy_meters"))
        dev_id = r.get("device_id", "")

        # Coordinate validity for mobility evaluation: finite, exact 6 decimals, global bounds, within configured bounds
        is_valid_coord = (
            lat is not None
            and lon is not None
            and bool(COORD_6DEC_REGEX.match(lat_str))
            and bool(COORD_6DEC_REGEX.match(lon_str))
            and (-90.0 <= lat <= 90.0)
            and (-180.0 <= lon <= 180.0)
            and bounds.contains(lon, lat)
        )

        parsed_record = {
            "row_idx": row_idx,
            "ping_id": r.get("ping_id"),
            "device_id": dev_id,
            "timestamp": dt_utc,
            "timestamp_str": ts_str,
            "latitude": lat,
            "longitude": lon,
            "accuracy_meters": acc,
            "movement_mode": r.get("movement_mode"),
            "data_provenance": r.get("data_provenance"),
            "is_valid_coord": is_valid_coord,
            "is_valid_timestamp": dt_utc is not None,
        }
        device_observations.setdefault(dev_id, []).append(parsed_record)

    # Validate per-device chronology in exact CSV appearance order (DO NOT sort!)
    for dev_id, pings in device_observations.items():
        for i in range(1, len(pings)):
            prev = pings[i - 1]
            curr = pings[i]

            if prev["timestamp"] is None or curr["timestamp"] is None:
                continue

            if curr["timestamp"] <= prev["timestamp"]:
                failures.append(
                    ValidationFailure(
                        category="temporal",
                        check_name="device_chronology_monotonic",
                        identifier=f"device:{dev_id}:step_{i}",
                        message=f"Device timestamp out-of-order or duplicate in CSV sequence: {prev['timestamp_str']} -> {curr['timestamp_str']}",
                        value=(prev["timestamp_str"], curr["timestamp_str"]),
                    )
                )

    return failures, device_observations, base_sim_date


def validate_mobility(
    device_trajectories: Dict[str, List[Dict[str, Any]]],
) -> Tuple[List[ValidationFailure], Dict[str, Any]]:
    """Validate kinematic speed limits, dwell displacement, intervals, and mode transitions.

    Definitions:
    - consecutive_pairs_evaluated: Total count of eligible adjacent observation pairs (P_{i-1}, P_i)
      in CSV order with valid coordinates, valid timestamps, and elapsed time dt > 0.
      Pairs with unsupported mode transitions or subsequent kinematic violations are explicitly
      included in this evaluation count, while pairs with invalid timestamps, invalid coordinates
      (including precision defects or boundary escapes), or non-positive elapsed time are excluded.
    - Numerical Tolerances:
      * Walking speed allowed: [0.78, 1.62] m/s (nominal contract: 0.8-1.6 m/s, tolerance: +-0.02 m/s
        to accommodate floating-point geodetic projection and 6-decimal precision rounding).
      * Vehicular speed allowed: [3.45, 14.05] m/s (nominal contract: 3.5-14.0 m/s, tolerance: +-0.05 m/s).
      * Stationary dwell apparent speed: <= 0.30 m/s.
      * Transit sampling interval: [15, 60] seconds.
      * Dwell sampling interval: [180, 600] seconds.
      * Dwell jitter displacement: <= 7.0 meters.
      * Maximum single-step displacement envelope: <= 850.0 meters.

    Critical Integrity Rules:
    - The maximum single-step displacement check (<= 850m) is evaluated on EVERY eligible pair,
      including pairs with unsupported mode transitions.
    - Invalid pairs (e.g. speed violations, interval violations, unsupported transitions) are
      excluded from measured_speed_ranges and valid transition counts.
    """
    failures: List[ValidationFailure] = []
    consecutive_pairs_count = 0
    transition_counts: Dict[str, int] = {}
    speed_ranges: Dict[str, Tuple[float, float]] = {}

    for dev_id, pings in device_trajectories.items():
        for i in range(1, len(pings)):
            prev = pings[i - 1]
            curr = pings[i]

            # Skip pairs where timestamp or coordinates are invalid/missing
            if not prev["is_valid_timestamp"] or not curr["is_valid_timestamp"]:
                continue
            if not prev["is_valid_coord"] or not curr["is_valid_coord"]:
                continue

            dt_seconds = (curr["timestamp"] - prev["timestamp"]).total_seconds()
            if dt_seconds <= 0:
                # Chronology reversal or duplicate timestamp already flagged in temporal check;
                # cannot compute meaningful physical velocity
                continue

            # Eligible consecutive pair
            consecutive_pairs_count += 1

            dist_meters = haversine_distance_meters(
                prev["latitude"], prev["longitude"],
                curr["latitude"], curr["longitude"],
            )

            # Check finite distance
            if not is_finite_number(dist_meters):
                failures.append(
                    ValidationFailure(
                        category="mobility",
                        check_name="distance_finite",
                        identifier=f"device:{dev_id}:step_{i}",
                        message="Calculated Haversine distance is non-finite (NaN or Inf)",
                        value=dist_meters,
                    )
                )
                continue

            speed_mps = dist_meters / dt_seconds

            # Check finite speed
            if not is_finite_number(speed_mps):
                failures.append(
                    ValidationFailure(
                        category="mobility",
                        check_name="speed_finite",
                        identifier=f"device:{dev_id}:step_{i}",
                        message="Calculated speed is non-finite (NaN or Inf)",
                        value=speed_mps,
                    )
                )
                continue

            p_mode = prev["movement_mode"]
            c_mode = curr["movement_mode"]
            transition_pair = (p_mode, c_mode)
            transition_key = f"{p_mode} -> {c_mode}"
            pair_id = f"device:{dev_id}:step_{i}:{transition_key}"

            pair_has_violation = False

            # 1. Absolute maximum displacement check (evaluated on ALL eligible pairs,
            # even if the mode transition is unsupported)
            if dist_meters > 850.0:
                pair_has_violation = True
                failures.append(
                    ValidationFailure(
                        category="mobility",
                        check_name="maximum_step_displacement",
                        identifier=pair_id,
                        message=f"Physically impossible spatial step: {dist_meters:.1f}m in {dt_seconds}s exceeds 850m envelope",
                        value=dist_meters,
                    )
                )

            # 2. Enforce supported transitions using VALID_MOVEMENT_TRANSITIONS as single source of truth
            if transition_pair not in VALID_MOVEMENT_TRANSITIONS:
                failures.append(
                    ValidationFailure(
                        category="mobility",
                        check_name="unsupported_movement_transition",
                        identifier=pair_id,
                        message=f"Unsupported movement transition '{p_mode}' -> '{c_mode}'. Permitted combinations are the 7 approved intra-mode, arrival, and departure transitions.",
                        value=transition_key,
                    )
                )
                continue

            transition_type = VALID_MOVEMENT_TRANSITIONS[transition_pair]

            # 3. Evaluate transition-specific speed, interval, and displacement rules
            if transition_type == "intra_walking":
                if not (0.78 <= speed_mps <= 1.62):
                    pair_has_violation = True
                    failures.append(
                        ValidationFailure(
                            category="mobility",
                            check_name="walking_speed_range",
                            identifier=pair_id,
                            message=f"Walking speed {speed_mps:.3f} m/s outside approved range [0.78, 1.62] m/s (nominal 0.8–1.6 m/s with ±0.02 m/s tolerance)",
                            value=speed_mps,
                        )
                    )
                if not (15.0 <= dt_seconds <= 60.0):
                    pair_has_violation = True
                    failures.append(
                        ValidationFailure(
                            category="mobility",
                            check_name="walking_interval_range",
                            identifier=pair_id,
                            message=f"Walking interval {dt_seconds}s outside approved range [15, 60]s",
                            value=dt_seconds,
                        )
                    )

            elif transition_type == "intra_vehicular":
                if not (3.45 <= speed_mps <= 14.05):
                    pair_has_violation = True
                    failures.append(
                        ValidationFailure(
                            category="mobility",
                            check_name="vehicular_speed_range",
                            identifier=pair_id,
                            message=f"Vehicular speed {speed_mps:.3f} m/s outside approved range [3.45, 14.05] m/s (nominal 3.5–14.0 m/s with ±0.05 m/s tolerance)",
                            value=speed_mps,
                        )
                    )
                if not (15.0 <= dt_seconds <= 60.0):
                    pair_has_violation = True
                    failures.append(
                        ValidationFailure(
                            category="mobility",
                            check_name="vehicular_interval_range",
                            identifier=pair_id,
                            message=f"Vehicular interval {dt_seconds}s outside approved range [15, 60]s",
                            value=dt_seconds,
                        )
                    )

            elif transition_type == "intra_dwell":
                if speed_mps > 0.30:
                    pair_has_violation = True
                    failures.append(
                        ValidationFailure(
                            category="mobility",
                            check_name="dwell_apparent_speed",
                            identifier=pair_id,
                            message=f"Dwell apparent speed {speed_mps:.3f} m/s exceeds 0.30 m/s limit",
                            value=speed_mps,
                        )
                    )
                if not (180.0 <= dt_seconds <= 600.0):
                    pair_has_violation = True
                    failures.append(
                        ValidationFailure(
                            category="mobility",
                            check_name="dwell_interval_range",
                            identifier=pair_id,
                            message=f"Dwell interval {dt_seconds}s outside approved range [180, 600]s",
                            value=dt_seconds,
                        )
                    )
                if dist_meters > 7.0:
                    pair_has_violation = True
                    failures.append(
                        ValidationFailure(
                            category="mobility",
                            check_name="dwell_jitter_displacement",
                            identifier=pair_id,
                            message=f"Dwell jitter displacement {dist_meters:.2f}m exceeds 7.0m threshold",
                            value=dist_meters,
                        )
                    )

            elif transition_type == "arrival_walking_to_dwell":
                if speed_mps > 0.30:
                    pair_has_violation = True
                    failures.append(
                        ValidationFailure(
                            category="mobility",
                            check_name="arrival_walking_speed",
                            identifier=pair_id,
                            message=f"Walking-to-dwell arrival apparent speed {speed_mps:.3f} m/s exceeds 0.30 m/s limit",
                            value=speed_mps,
                        )
                    )
                if not (180.0 <= dt_seconds <= 600.0):
                    pair_has_violation = True
                    failures.append(
                        ValidationFailure(
                            category="mobility",
                            check_name="arrival_walking_interval",
                            identifier=pair_id,
                            message=f"Walking-to-dwell arrival interval {dt_seconds}s outside approved dwell range [180, 600]s",
                            value=dt_seconds,
                        )
                    )
                if dist_meters > 7.0:
                    pair_has_violation = True
                    failures.append(
                        ValidationFailure(
                            category="mobility",
                            check_name="arrival_walking_displacement",
                            identifier=pair_id,
                            message=f"Walking-to-dwell arrival settling displacement {dist_meters:.2f}m exceeds 7.0m threshold",
                            value=dist_meters,
                        )
                    )

            elif transition_type == "arrival_vehicular_to_dwell":
                if speed_mps > 0.30:
                    pair_has_violation = True
                    failures.append(
                        ValidationFailure(
                            category="mobility",
                            check_name="arrival_vehicular_speed",
                            identifier=pair_id,
                            message=f"Vehicle-to-dwell arrival apparent speed {speed_mps:.3f} m/s exceeds 0.30 m/s limit",
                            value=speed_mps,
                        )
                    )
                if not (180.0 <= dt_seconds <= 600.0):
                    pair_has_violation = True
                    failures.append(
                        ValidationFailure(
                            category="mobility",
                            check_name="arrival_vehicular_interval",
                            identifier=pair_id,
                            message=f"Vehicle-to-dwell arrival interval {dt_seconds}s outside approved dwell range [180, 600]s",
                            value=dt_seconds,
                        )
                    )
                if dist_meters > 7.0:
                    pair_has_violation = True
                    failures.append(
                        ValidationFailure(
                            category="mobility",
                            check_name="arrival_vehicular_displacement",
                            identifier=pair_id,
                            message=f"Vehicle-to-dwell arrival settling displacement {dist_meters:.2f}m exceeds 7.0m threshold",
                            value=dist_meters,
                        )
                    )

            elif transition_type == "departure_dwell_to_walking":
                if not (0.78 <= speed_mps <= 1.62):
                    pair_has_violation = True
                    failures.append(
                        ValidationFailure(
                            category="mobility",
                            check_name="departure_walking_speed",
                            identifier=pair_id,
                            message=f"Dwell-to-walking departure speed {speed_mps:.3f} m/s outside approved range [0.78, 1.62] m/s (nominal 0.8–1.6 m/s with ±0.02 m/s tolerance)",
                            value=speed_mps,
                        )
                    )
                if not (15.0 <= dt_seconds <= 60.0):
                    pair_has_violation = True
                    failures.append(
                        ValidationFailure(
                            category="mobility",
                            check_name="departure_walking_interval",
                            identifier=pair_id,
                            message=f"Dwell-to-walking departure interval {dt_seconds}s outside approved range [15, 60]s",
                            value=dt_seconds,
                        )
                    )

            elif transition_type == "departure_dwell_to_vehicular":
                if not (3.45 <= speed_mps <= 14.05):
                    pair_has_violation = True
                    failures.append(
                        ValidationFailure(
                            category="mobility",
                            check_name="departure_vehicular_speed",
                            identifier=pair_id,
                            message=f"Dwell-to-vehicle departure speed {speed_mps:.3f} m/s outside approved range [3.45, 14.05] m/s (nominal 3.5–14.0 m/s with ±0.05 m/s tolerance)",
                            value=speed_mps,
                        )
                    )
                if not (15.0 <= dt_seconds <= 60.0):
                    pair_has_violation = True
                    failures.append(
                        ValidationFailure(
                            category="mobility",
                            check_name="departure_vehicular_interval",
                            identifier=pair_id,
                            message=f"Dwell-to-vehicle departure interval {dt_seconds}s outside approved range [15, 60]s",
                            value=dt_seconds,
                        )
                    )

            # Exclude invalid pairs from speed summaries and transition counts
            if not pair_has_violation:
                transition_counts[transition_key] = transition_counts.get(transition_key, 0) + 1
                if transition_key not in speed_ranges:
                    speed_ranges[transition_key] = (speed_mps, speed_mps)
                else:
                    curr_min, curr_max = speed_ranges[transition_key]
                    speed_ranges[transition_key] = (min(curr_min, speed_mps), max(curr_max, speed_mps))

    mobility_stats = {
        "consecutive_pairs_count": consecutive_pairs_count,
        "transition_counts": transition_counts,
        "speed_ranges": speed_ranges,
    }
    return failures, mobility_stats


def compute_spatial_summary(
    rows: List[Dict[str, str]],
    bounds: BoundingBox = STUDY_AREA.bounds,
    base_date: Optional[date] = None,
    local_tz: Any = None,
) -> ValidationMetrics:
    """Compute comprehensive descriptive metrics across fully valid measurement records only.

    A record is classified as a valid_measurement_record if and only if it satisfies all
    field-level contract specifications AND activity-calendar membership:
    - ping_id: valid RFC 4122 UUIDv4 AND unique across all dataset rows (no duplicate ping_ids)
    - device_id: valid RFC 4122 UUIDv4
    - data_provenance: matches DATA_PROVENANCE_TAG ('geopulse_synthetic_v1')
    - movement_mode: matches approved MovementMode enum
    - latitude: finite float, exact 6-decimal string format, within global bounds [-90, 90],
      and within configured bounding box
    - longitude: finite float, exact 6-decimal string format, within global bounds [-180, 180],
      and within configured bounding box
    - accuracy_meters: finite float within [2.0, 35.0]
    - event_timestamp: valid strict ISO-8601 UTC timestamp ending in 'Z'
    - activity_calendar: timestamp must fall within an approved diurnal activity window
      for the simulation calendar date (records in excluded gaps or outside simulation
      calendar are excluded from valid measurement summaries).

    Partially validated, malformed, or out-of-calendar records are excluded from spatial,
    temporal, and numeric statistics and counted under malformed_records.
    """
    metrics = ValidationMetrics()
    metrics.total_records = len(rows)
    if not rows:
        return metrics

    if local_tz is None:
        local_tz = get_local_timezone()

    # Pre-compute ping_id frequencies to detect duplicates
    ping_id_counts: Dict[str, int] = {}
    for r in rows:
        pid = r.get("ping_id", "").strip()
        ping_id_counts[pid] = ping_id_counts.get(pid, 0) + 1

    # Pre-determine base_date if not passed in, by finding earliest valid local timestamp
    if base_date is None:
        valid_local_dts = []
        for r in rows:
            ts = parse_iso8601_utc(r.get("event_timestamp", "").strip())
            if ts is not None:
                valid_local_dts.append(ts.astimezone(local_tz))
        if valid_local_dts:
            base_date = min(dt.date() for dt in valid_local_dts)

    lats: List[float] = []
    lons: List[float] = []
    accs: List[float] = []
    device_counts: Dict[str, int] = {}
    ping_ids: set[str] = set()

    mid_lon, mid_lat = bounds.centroid
    quadrants = {"NW": 0, "NE": 0, "SW": 0, "SE": 0}
    diurnal_counts = {
        "morning_commute": 0,
        "midday_work_lunch": 0,
        "evening_social_peak": 0,
        "nighttime_low": 0,
        "unassigned_gap": 0,
    }
    mode_counts = {
        MovementMode.PEDESTRIAN_WALKING.value: 0,
        MovementMode.STATIONARY_DWELL.value: 0,
        MovementMode.VEHICULAR_TRANSIT.value: 0,
    }

    valid_modes = set(mode_counts.keys())
    valid_count = 0
    malformed_count = 0

    for r in rows:
        lat_str = r.get("latitude", "")
        lon_str = r.get("longitude", "")
        acc_str = r.get("accuracy_meters", "")
        dev_id = r.get("device_id", "").strip()
        ping_id = r.get("ping_id", "").strip()
        mode = r.get("movement_mode", "").strip()
        ts_str = r.get("event_timestamp", "").strip()
        prov = r.get("data_provenance", "").strip()

        lat = parse_finite_float(lat_str)
        lon = parse_finite_float(lon_str)
        acc = parse_finite_float(acc_str)
        dt_utc = parse_iso8601_utc(ts_str)

        # Diurnal window classification for activity calendar check
        w_name = None
        if dt_utc is not None and base_date is not None:
            dt_local = dt_utc.astimezone(local_tz)
            w_name, _ = classify_diurnal_window(dt_local, base_date, local_tz)

        # Check all validation criteria strictly
        is_valid = (
            # 1. Latitude: finite, 6-decimals, global bounds, within bounding box
            lat is not None
            and bool(COORD_6DEC_REGEX.match(lat_str))
            and (-90.0 <= lat <= 90.0)
            # 2. Longitude: finite, 6-decimals, global bounds, within bounding box
            and lon is not None
            and bool(COORD_6DEC_REGEX.match(lon_str))
            and (-180.0 <= lon <= 180.0)
            and bounds.contains(lon, lat)
            # 3. Accuracy: finite, within [2.0, 35.0]
            and acc is not None and (2.0 <= acc <= 35.0)
            # 4. Identity & Uniqueness
            and bool(UUIDV4_REGEX.match(dev_id))
            and bool(UUIDV4_REGEX.match(ping_id))
            and ping_id_counts.get(ping_id, 0) == 1
            # 5. Provenance & Enum
            and mode in valid_modes
            and prov == DATA_PROVENANCE_TAG
            # 6. Timestamp: strict UTC ending in 'Z'
            and dt_utc is not None
            # 7. Diurnal activity-calendar membership
            and w_name is not None
        )

        if not is_valid:
            malformed_count += 1
            continue

        valid_count += 1
        lats.append(lat)
        lons.append(lon)
        accs.append(acc)
        ping_ids.add(ping_id)
        device_counts[dev_id] = device_counts.get(dev_id, 0) + 1
        mode_counts[mode] += 1

        # Spatial quadrant allocation (mutually exclusive)
        if lat >= mid_lat:
            if lon < mid_lon:
                quadrants["NW"] += 1
            else:
                quadrants["NE"] += 1
        else:
            if lon < mid_lon:
                quadrants["SW"] += 1
            else:
                quadrants["SE"] += 1

        # Diurnal window allocation
        diurnal_counts[w_name] += 1

    metrics.valid_measurement_records = valid_count
    metrics.malformed_records = malformed_count
    metrics.unique_devices = len(device_counts)
    metrics.unique_ping_ids = len(ping_ids)
    if base_date:
        metrics.base_simulation_date = base_date.strftime("%Y-%m-%d")

    if lats:
        metrics.min_latitude = min(lats)
        metrics.max_latitude = max(lats)
        metrics.min_longitude = min(lons)
        metrics.max_longitude = max(lons)
        metrics.latitude_span = metrics.max_latitude - metrics.min_latitude
        metrics.longitude_span = metrics.max_longitude - metrics.min_longitude
        metrics.min_accuracy = min(accs)
        metrics.max_accuracy = max(accs)

    # Denominator for percentages is valid measurement count
    denom = valid_count if valid_count > 0 else 1
    metrics.mode_counts = mode_counts
    metrics.mode_percentages = {m: (c / denom) * 100.0 for m, c in mode_counts.items()}

    metrics.diurnal_counts = diurnal_counts
    metrics.diurnal_percentages = {p: (c / denom) * 100.0 for p, c in diurnal_counts.items()}

    metrics.quadrant_counts = quadrants
    metrics.quadrant_percentages = {q: (c / denom) * 100.0 for q, c in quadrants.items()}

    pings_per_dev = list(device_counts.values())
    metrics.pings_per_device_min = min(pings_per_dev) if pings_per_dev else 0
    metrics.pings_per_device_max = max(pings_per_dev) if pings_per_dev else 0
    metrics.pings_per_device_avg = (sum(pings_per_dev) / len(pings_per_dev)) if pings_per_dev else 0.0

    return metrics


def validate_dataset(
    csv_path: Path | str,
    bounds: BoundingBox = STUDY_AREA.bounds,
    simulation_date: Optional[str] = None,
    expected_devices: Optional[int] = None,
) -> ValidationResult:
    """Execute complete end-to-end data quality and mobility trajectory audit."""
    path = Path(csv_path)
    rows, structural_failures, file_sha256 = load_and_validate_csv_structure(path)

    # If structural validation failed completely (missing file, bad headers, zero records, etc.)
    # abort semantic checks immediately to prevent misleading downstream errors
    if structural_failures and not rows:
        return ValidationResult(
            file_path=str(path),
            file_sha256=file_sha256,
            total_records=0,
            is_valid=False,
            failures=structural_failures,
            metrics=None,
        )

    all_failures: List[ValidationFailure] = list(structural_failures)

    # 1. Record integrity validation
    integrity_failures = validate_record_integrity(rows, expected_devices=expected_devices)
    all_failures.extend(integrity_failures)

    # 2. Geographic boundary validation
    geo_failures = validate_geography(rows, bounds=bounds)
    all_failures.extend(geo_failures)

    # 3. Temporal validation & trajectory extraction
    local_tz = get_local_timezone()
    temporal_failures, device_trajectories, base_date = validate_temporal(
        rows, bounds=bounds, simulation_date=simulation_date, local_tz=local_tz
    )
    all_failures.extend(temporal_failures)

    # 4. Mobility kinematics validation
    mobility_failures, mobility_stats = validate_mobility(device_trajectories)
    all_failures.extend(mobility_failures)

    # 5. Compute summary metrics across valid records
    metrics = compute_spatial_summary(
        rows, bounds=bounds, base_date=base_date, local_tz=local_tz
    )
    metrics.consecutive_pairs_evaluated = mobility_stats.get("consecutive_pairs_count", 0)
    metrics.pair_transition_counts = mobility_stats.get("transition_counts", {})
    metrics.measured_speed_ranges = mobility_stats.get("speed_ranges", {})

    is_valid = len(all_failures) == 0

    return ValidationResult(
        file_path=str(path),
        file_sha256=file_sha256,
        total_records=len(rows),
        is_valid=is_valid,
        failures=all_failures,
        metrics=metrics,
    )


def format_validation_report(result: ValidationResult) -> str:
    """Format structured validation result into human-readable report."""
    lines: List[str] = []
    lines.append("=" * 80)
    lines.append("GeoPulse: Mobility Data Quality & Kinematic Validation Report")
    lines.append("=" * 80)
    lines.append(f"Target File: {result.file_path}")
    lines.append(f"File SHA-256: {result.file_sha256}")
    lines.append(f"Total Observations in CSV: {result.total_records:,}")
    status_str = "PASSED (100% COMPLIANT)" if result.is_valid else f"FAILED ({result.total_failures} violations)"
    lines.append(f"Overall Dataset Validity Status: {status_str}")
    lines.append("-" * 80)

    if result.metrics:
        m = result.metrics
        lines.append("1. Record-Level Validity & Entity Summary:")
        lines.append(f"   - Valid Measurement Records: {m.valid_measurement_records:,}")
        if m.malformed_records > 0:
            lines.append(f"   - Excluded Malformed / Out-of-Calendar Records: {m.malformed_records:,}")
        lines.append(f"   - Unique Devices: {m.unique_devices:,}")
        lines.append(f"   - Unique Ping IDs: {m.unique_ping_ids:,}")
        lines.append(f"   - Base Simulation Date: {m.base_simulation_date or 'Auto-detected'}")
        lines.append(f"   - Pings Per Device: min={m.pings_per_device_min}, max={m.pings_per_device_max}, avg={m.pings_per_device_avg:.1f}")
        lines.append("")

        lines.append("2. Geographic & Bounding Box Conformity:")
        lines.append(f"   - Latitude Range:  [{m.min_latitude:.6f}, {m.max_latitude:.6f}] (Span: {m.latitude_span:.6f} deg)")
        lines.append(f"   - Longitude Range: [{m.min_longitude:.6f}, {m.max_longitude:.6f}] (Span: {m.longitude_span:.6f} deg)")
        lines.append(f"   - Accuracy Range:  [{m.min_accuracy:.1f}, {m.max_accuracy:.1f}] meters")
        lines.append("   - Spatial Quadrants (relative to centroid):")
        for q, pct in m.quadrant_percentages.items():
            lines.append(f"     * {q}: {m.quadrant_counts[q]:,} ({pct:.2f}%)")
        lines.append("")

        lines.append("3. Diurnal Activity Windows (Asia/Kolkata):")
        for p, pct in m.diurnal_percentages.items():
            lines.append(f"   - {p}: {m.diurnal_counts[p]:,} ({pct:.2f}%)")
        lines.append("")

        lines.append("4. Movement Mode Breakdown:")
        for mode, pct in m.mode_percentages.items():
            lines.append(f"   - {mode}: {m.mode_counts[mode]:,} ({pct:.2f}%)")
        lines.append("")

        lines.append(f"5. Consecutive Observation Pairs Evaluated (Eligible Adjacent Pairs): {m.consecutive_pairs_evaluated:,}")
        for t_key, count in sorted(m.pair_transition_counts.items()):
            min_s, max_s = m.measured_speed_ranges.get(t_key, (0.0, 0.0))
            lines.append(f"   - {t_key}: {count:,} pairs | Speeds: [{min_s:.3f}, {max_s:.3f}] m/s")
        lines.append("-" * 80)

    if result.failures:
        lines.append(f"VIOLATIONS DETECTED ({len(result.failures)} total):")
        for i, failure in enumerate(result.failures[:50], start=1):
            lines.append(f"  {i}. {failure}")
        if len(result.failures) > 50:
            lines.append(f"  ... and {len(result.failures) - 50} additional violations.")
        lines.append("-" * 80)
    else:
        lines.append("ALL QUALITY AUDIT ASSERTIONS PASSED:")
        lines.append("  [x] Canonical 8-column CSV schema and order verified")
        lines.append("  [x] No null, empty, non-finite (NaN/Inf), or unparseable fields")
        lines.append("  [x] 100% unique UUIDv4 ping IDs and persistent device IDs")
        lines.append("  [x] Approved data provenance tag verified")
        lines.append("  [x] All geodetic coordinates within the configured bounding box")
        lines.append("  [x] Exactly 6 decimal places coordinate precision")
        lines.append("  [x] ISO-8601 UTC timestamps with 'Z' suffix")
        lines.append("  [x] Strict per-device chronological ordering (CSV sequence)")
        lines.append("  [x] Zero observations in excluded diurnal window gaps")
        lines.append("  [x] Walking speeds within inclusive range [0.78, 1.62] m/s (nominal 0.8–1.6 m/s with ±0.02 m/s tolerance)")
        lines.append("  [x] Vehicular speeds within inclusive range [3.45, 14.05] m/s (nominal 3.5–14.0 m/s with ±0.05 m/s tolerance)")
        lines.append("  [x] Stationary dwell speeds <= 0.30 m/s and displacement <= 7.0m")
        lines.append("  [x] Transit intervals [15, 60]s and dwell intervals [180, 600]s")
        lines.append("  [x] Mode transitions restricted to 7 approved combinations")
        lines.append("  [x] Explicit walking and vehicular arrival & departure transitions verified")
        lines.append("=" * 80)

    lines.append("Note: Summaries reflect synthetic simulation telemetry, not real-world footfall.")
    return "\n".join(lines)


def main() -> int:
    """CLI entrypoint for data quality validation."""
    parser = argparse.ArgumentParser(
        description="GeoPulse: Validate synthetic GPS mobility telemetry dataset."
    )
    parser.add_argument(
        "--input",
        type=str,
        default="data/raw/synthetic_mobility_pings.csv",
        help="Path to input synthetic GPS CSV file (default: data/raw/synthetic_mobility_pings.csv)",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Optional simulation date (YYYY-MM-DD) for activity window checks. Defaults to auto-detect from dataset.",
    )
    parser.add_argument(
        "--expected-devices",
        type=int,
        default=None,
        help="Optional expected unique device count (e.g. 500)",
    )
    parser.add_argument(
        "--report",
        type=str,
        default=None,
        help="Optional path to output text report file",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress console output and return exit code only",
    )
    args = parser.parse_args()

    result = validate_dataset(
        args.input,
        simulation_date=args.date,
        expected_devices=args.expected_devices,
    )
    report_text = format_validation_report(result)

    if not args.quiet:
        print(report_text)

    if args.report:
        report_path = Path(args.report)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report_text)

    return 0 if result.is_valid else 1


if __name__ == "__main__":
    sys.exit(main())
