"""GeoPulse retail mobility analytics module.

Implements Week 2 retail mobility and catchment footfall dynamics:
- Ingestion and schema validation of catchment membership datasets.
- Explicit UTC to local operating timezone (Asia/Kolkata) conversion.
- Hourly unique-device and ping observation volume analytics.
- Diurnal time-of-day distribution analytics (morning, afternoon, evening, night).
- Daily store activity summaries and active-hour metrics.
- Multi-catchment network overlap analytics and reconciliation.
- Safe Spark CSV output directory persistence with strict overwrite guards.

Disclaimer:
All mobility telemetry and candidate storefronts are synthetic constructs.
Catchment membership measures geometric spatial proximity, not confirmed storefront
visits or commercial transactions. Catchment overlap reflects geographic proximity,
not commercial customer cannibalization or revenue loss.
"""

from __future__ import annotations

import argparse
import csv
import logging
import math
import os
from pathlib import Path
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

from src.config import (
    DATA_PROVENANCE_TAG,
    MovementMode,
    TIMEZONE_LOCAL,
)

logger = logging.getLogger("geopulse.retail_mobility_analytics")


# ==============================================================================
# Configuration & Constants
# ==============================================================================

DEFAULT_MEMBERSHIPS_PATH = "data/processed/week2_verification/store_catchment_memberships"
DEFAULT_OUTPUT_BASE = "data/processed/week2_retail_analytics"
DEFAULT_TIMEZONE = TIMEZONE_LOCAL  # "Asia/Kolkata" (UTC+05:30)

EXPECTED_MEMBERSHIPS_COLUMNS = [
    "ping_id",
    "device_id",
    "event_timestamp",
    "store_id",
    "store_name",
    "distance_meters",
    "movement_mode",
    "data_provenance",
]

# Time of day diurnal period boundaries (in local operating hours 0-23)
# Morning:   06:00 - 11:59 (hours 6, 7, 8, 9, 10, 11)
# Afternoon: 12:00 - 16:59 (hours 12, 13, 14, 15, 16)
# Evening:   17:00 - 21:59 (hours 17, 18, 19, 20, 21)
# Night:     22:00 - 05:59 (hours 22, 23, 0, 1, 2, 3, 4, 5)
DIURNAL_PERIODS = ("morning", "afternoon", "evening", "night")


@dataclass(frozen=True)
class RetailAnalyticsConfig:
    """Configuration container for GeoPulse retail mobility analytics."""

    memberships_input_path: str = DEFAULT_MEMBERSHIPS_PATH
    output_dir: str = DEFAULT_OUTPUT_BASE
    hourly_output_path: Optional[str] = None
    time_of_day_output_path: Optional[str] = None
    daily_output_path: Optional[str] = None
    overlap_output_path: Optional[str] = None
    timezone: str = DEFAULT_TIMEZONE
    overwrite: bool = False

    def get_hourly_path(self) -> str:
        return self.hourly_output_path or str(Path(self.output_dir) / "hourly_store_activity")

    def get_time_of_day_path(self) -> str:
        return self.time_of_day_output_path or str(Path(self.output_dir) / "store_time_of_day_activity")

    def get_daily_path(self) -> str:
        return self.daily_output_path or str(Path(self.output_dir) / "daily_store_summary")

    def get_overlap_path(self) -> str:
        return self.overlap_output_path or str(Path(self.output_dir) / "store_overlap_summary")


# ==============================================================================
# Pure Python Validation & Classification Helpers (No Spark Required)
# ==============================================================================

def classify_time_of_day(hour: int) -> str:
    """Classify an integer hour (0-23) into one of four diurnal time-of-day periods.

    Periods:
    - morning:   06:00 - 11:59 (hours 6, 7, 8, 9, 10, 11)
    - afternoon: 12:00 - 16:59 (hours 12, 13, 14, 15, 16)
    - evening:   17:00 - 21:59 (hours 17, 18, 19, 20, 21)
    - night:     22:00 - 05:59 (hours 22, 23, 0, 1, 2, 3, 4, 5)

    Raises:
        TypeError: If hour is not an integer.
        ValueError: If hour is outside [0, 23].
    """
    if not isinstance(hour, int) or isinstance(hour, bool):
        raise TypeError(f"Hour must be an integer, got {type(hour).__name__}.")
    if not (0 <= hour <= 23):
        raise ValueError(f"Hour must be between 0 and 23 inclusive, got {hour}.")

    if 6 <= hour <= 11:
        return "morning"
    elif 12 <= hour <= 16:
        return "afternoon"
    elif 17 <= hour <= 21:
        return "evening"
    else:
        return "night"


def validate_memberships_schema(columns: List[str]) -> bool:
    """Validate that input columns contain the required membership fields."""
    if not columns:
        return False
    return list(columns) == EXPECTED_MEMBERSHIPS_COLUMNS


def parse_and_validate_timestamp(ts_str: str) -> datetime:
    """Parse and validate an ISO-8601 UTC timestamp string.

    Requires non-empty string with valid format and 'Z' or '+00:00' UTC indicator.
    Verifies that the parsed timezone offset is strictly UTC (+00:00).
    """
    if not isinstance(ts_str, str) or not ts_str.strip():
        raise ValueError("Timestamp must be a non-empty string.")
    cleaned = ts_str.strip()
    if not (cleaned.endswith("Z") or "+00:00" in cleaned):
        raise ValueError(f"Timestamp '{ts_str}' must be UTC (ending in 'Z' or '+00:00').")
    try:
        dt = datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
    except Exception as exc:
        raise ValueError(f"Invalid ISO-8601 timestamp '{ts_str}': {exc}") from exc

    if dt.tzinfo is None:
        raise ValueError(f"Timestamp '{ts_str}' parsed without timezone information.")
    if dt.utcoffset() != timezone.utc.utcoffset(dt):
        raise ValueError(f"Timestamp '{ts_str}' has non-zero UTC offset: {dt.utcoffset()}.")

    return dt


def validate_membership_record(
    row: Dict[str, Any],
    seen_pairs: Optional[Set[Tuple[str, str]]] = None,
    seen_pings: Optional[Set[Tuple[str, str]]] = None,
) -> Dict[str, Any]:
    """Validate a single catchment membership record against schema and data constraints.

    Validates:
    - All 8 canonical fields present, non-null, and non-blank.
    - ping_id: non-empty string.
    - device_id: non-empty string.
    - store_id: non-empty string.
    - store_name: non-empty string.
    - event_timestamp: valid UTC ISO-8601 format.
    - distance_meters: non-negative finite numeric float.
    - movement_mode: allowed MovementMode enum values.
    - data_provenance: exact match with DATA_PROVENANCE_TAG.
    - Duplicate detection: (ping_id, store_id) uniqueness if seen_pairs is provided.

    Returns:
        The validated record dict.
    """
    if seen_pairs is None and seen_pings is not None:
        seen_pairs = seen_pings

    # 1. Validate all required columns presence and non-blank
    for col in EXPECTED_MEMBERSHIPS_COLUMNS:
        if col not in row or row[col] is None or not str(row[col]).strip():
            raise ValueError(f"Membership record missing or blank required field '{col}'.")

    ping_id = str(row["ping_id"]).strip()
    store_id = str(row["store_id"]).strip()

    # 2. Duplicate detection
    if seen_pairs is not None:
        pair = (ping_id, store_id)
        if pair in seen_pairs:
            raise ValueError(
                f"Duplicate membership record detected for ping '{ping_id}' and store '{store_id}'."
            )
        seen_pairs.add(pair)

    # 3. Timestamp parsing & UTC check
    parse_and_validate_timestamp(str(row["event_timestamp"]))

    # 4. Distance meters check (must be non-negative, finite number; reject null, NaN, +/-inf, negative)
    val_str = str(row["distance_meters"]).strip()
    try:
        dist = float(val_str)
    except (ValueError, TypeError) as exc:
        raise ValueError(
            f"distance_meters must be a non-negative finite number, got '{row['distance_meters']}'."
        ) from exc
    if not math.isfinite(dist) or math.isnan(dist) or math.isinf(dist) or dist < 0.0:
        raise ValueError(
            f"distance_meters must be a non-negative finite number, got {dist}."
        )

    # 5. Movement mode check
    mode = str(row["movement_mode"]).strip()
    allowed_modes = {m.value for m in MovementMode}
    if mode not in allowed_modes:
        raise ValueError(
            f"Invalid movement_mode '{mode}'. Allowed values: {sorted(allowed_modes)}."
        )

    # 6. Data provenance check
    prov = str(row["data_provenance"]).strip()
    if prov != DATA_PROVENANCE_TAG:
        raise ValueError(
            f"Invalid data_provenance '{prov}'. Expected '{DATA_PROVENANCE_TAG}'."
        )

    return dict(row)


def validate_memberships_input(file_or_dir_path: str | Path) -> List[Path]:
    """Validate membership input path and locate CSV files.

    Handles single CSV file or Spark CSV part-file directory.
    Validates:
    - Path existence.
    - Non-empty content (avoids unhandled StopIteration).
    - Canonical 8-column header.

    Returns:
        List of resolved CSV file Paths.
    """
    target = Path(file_or_dir_path).resolve()
    if not target.exists():
        raise FileNotFoundError(f"Catchment memberships input path not found: {target}")

    csv_files: List[Path] = []
    if target.is_file():
        if target.suffix.lower() == ".csv":
            csv_files.append(target)
    else:
        csv_files = sorted(target.glob("part-*.csv"))
        if not csv_files:
            csv_files = sorted(target.glob("*.csv"))

    if not csv_files:
        raise ValueError(f"No CSV files found in memberships path: {target}")

    # Validate every discovered CSV part file for non-empty content and canonical headers
    for csv_file in csv_files:
        if csv_file.stat().st_size == 0:
            raise ValueError(f"Memberships CSV file is completely empty (0 bytes): {csv_file}")

        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
            except StopIteration:
                raise ValueError(f"Memberships CSV file contains zero lines: {csv_file}")

        if not validate_memberships_schema(header):
            raise ValueError(
                f"Memberships CSV file '{csv_file.name}' header {header} does not match expected schema {EXPECTED_MEMBERSHIPS_COLUMNS}."
            )

    return csv_files


# ==============================================================================
# Pure Python Analytics Implementations (Testable Without Spark)
# ==============================================================================

def compute_hourly_metrics_from_records(
    records: List[Dict[str, Any]],
    target_timezone: str = DEFAULT_TIMEZONE,
    timezone_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Compute hourly distinct devices and ping counts from in-memory records.

    Used for unit testing and independent validation.
    """
    tz_str = timezone_name if timezone_name is not None else target_timezone
    tz = ZoneInfo(tz_str)
    hourly_buckets: Dict[Tuple[str, str, str, int], Dict[str, Any]] = {}

    for row in records:
        dt_utc = parse_and_validate_timestamp(str(row["event_timestamp"]))
        dt_local = dt_utc.astimezone(tz)
        cal_date = dt_local.strftime("%Y-%m-%d")
        hour_of_day = dt_local.hour
        store_id = str(row["store_id"]).strip()
        store_name = str(row.get("store_name", store_id)).strip()
        device_id = str(row["device_id"]).strip()
        ping_id = str(row["ping_id"]).strip()

        key = (store_id, store_name, cal_date, hour_of_day)
        if key not in hourly_buckets:
            hourly_buckets[key] = {"devices": set(), "pings": 0}
        hourly_buckets[key]["devices"].add(device_id)
        hourly_buckets[key]["pings"] += 1

    results = []
    for (s_id, s_name, c_date, hour), val in sorted(hourly_buckets.items()):
        results.append({
            "store_id": s_id,
            "store_name": s_name,
            "calendar_date": c_date,
            "hour_of_day": hour,
            "distinct_device_count": len(val["devices"]),
            "ping_count": val["pings"],
        })
    return results


def compute_time_of_day_metrics_from_records(
    records: List[Dict[str, Any]],
    target_timezone: str = DEFAULT_TIMEZONE,
    timezone_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Compute diurnal time-of-day metrics from in-memory records."""
    tz_str = timezone_name if timezone_name is not None else target_timezone
    tz = ZoneInfo(tz_str)
    tod_buckets: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

    for row in records:
        dt_utc = parse_and_validate_timestamp(str(row["event_timestamp"]))
        dt_local = dt_utc.astimezone(tz)
        tod = classify_time_of_day(dt_local.hour)
        store_id = str(row["store_id"]).strip()
        store_name = str(row.get("store_name", store_id)).strip()
        device_id = str(row["device_id"]).strip()

        key = (store_id, store_name, tod)
        if key not in tod_buckets:
            tod_buckets[key] = {"devices": set(), "pings": 0}
        tod_buckets[key]["devices"].add(device_id)
        tod_buckets[key]["pings"] += 1

    results = []
    for (s_id, s_name, tod), val in sorted(tod_buckets.items()):
        results.append({
            "store_id": s_id,
            "store_name": s_name,
            "time_of_day": tod,
            "distinct_device_count": len(val["devices"]),
            "ping_count": val["pings"],
        })
    return results


def compute_daily_metrics_from_records(
    records: List[Dict[str, Any]],
    target_timezone: str = DEFAULT_TIMEZONE,
    timezone_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Compute daily store summaries and active hours from in-memory records."""
    tz_str = timezone_name if timezone_name is not None else target_timezone
    tz = ZoneInfo(tz_str)
    daily_buckets: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

    for row in records:
        dt_utc = parse_and_validate_timestamp(str(row["event_timestamp"]))
        dt_local = dt_utc.astimezone(tz)
        cal_date = dt_local.strftime("%Y-%m-%d")
        store_id = str(row["store_id"]).strip()
        store_name = str(row.get("store_name", store_id)).strip()
        device_id = str(row["device_id"]).strip()
        hour = dt_local.hour

        key = (store_id, store_name, cal_date)
        if key not in daily_buckets:
            daily_buckets[key] = {"devices": set(), "pings": 0, "hours": set()}
        daily_buckets[key]["devices"].add(device_id)
        daily_buckets[key]["pings"] += 1
        daily_buckets[key]["hours"].add(hour)

    results = []
    for (s_id, s_name, c_date), val in sorted(daily_buckets.items()):
        dev_count = len(val["devices"])
        p_count = val["pings"]
        active_hours = len(val["hours"])
        pings_per_dev = round(p_count / dev_count, 2) if dev_count > 0 else 0.0

        results.append({
            "store_id": s_id,
            "store_name": s_name,
            "calendar_date": c_date,
            "distinct_devices": dev_count,
            "ping_count": p_count,
            "active_hours_count": active_hours,
            "pings_per_device": pings_per_dev,
        })
    return results


def compute_overlap_metrics_from_records(
    records: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Compute store overlap summary and network reconciliation from in-memory records."""
    ping_to_stores: Dict[str, Set[str]] = {}
    device_to_stores: Dict[str, Set[str]] = {}
    stores_present: Set[str] = set()
    store_names: Dict[str, str] = {}

    for row in records:
        p_id = str(row["ping_id"]).strip()
        d_id = str(row["device_id"]).strip()
        s_id = str(row["store_id"]).strip()
        s_name = str(row.get("store_name", s_id)).strip()

        stores_present.add(s_id)
        store_names[s_id] = s_name

        if p_id not in ping_to_stores:
            ping_to_stores[p_id] = set()
        ping_to_stores[p_id].add(s_id)

        if d_id not in device_to_stores:
            device_to_stores[d_id] = set()
        device_to_stores[d_id].add(s_id)

    store_rows = []
    sum_exclusive_pings = 0
    sum_overlapping_pings = 0
    sum_total_pings = 0

    for s_id in sorted(stores_present):
        s_rows = [r for r in records if str(r["store_id"]).strip() == s_id]
        tot_pings = len(s_rows)
        excl_pings = sum(1 for r in s_rows if len(ping_to_stores[str(r["ping_id"]).strip()]) == 1)
        over_pings = sum(1 for r in s_rows if len(ping_to_stores[str(r["ping_id"]).strip()]) > 1)

        s_devices = {str(r["device_id"]).strip() for r in s_rows}
        tot_devices = len(s_devices)
        excl_devices = sum(1 for d in s_devices if len(device_to_stores[d]) == 1)
        multi_devices = sum(1 for d in s_devices if len(device_to_stores[d]) > 1)

        sum_exclusive_pings += excl_pings
        sum_overlapping_pings += over_pings
        sum_total_pings += tot_pings

        # Internal consistency check per store
        if (excl_pings + over_pings) != tot_pings:
            raise ValueError(f"Store {s_id} pings mismatch: excl({excl_pings}) + over({over_pings}) != tot({tot_pings})")
        if (excl_devices + multi_devices) != tot_devices:
            raise ValueError(f"Store {s_id} devices mismatch: excl({excl_devices}) + multi({multi_devices}) != tot({tot_devices})")

        store_rows.append({
            "store_id": s_id,
            "store_name": store_names[s_id],
            "total_pings": tot_pings,
            "exclusive_pings": excl_pings,
            "overlapping_pings": over_pings,
            "ping_overlap_pct": round((over_pings / tot_pings) * 100.0, 2) if tot_pings > 0 else 0.0,
            "total_devices": tot_devices,
            "exclusive_devices": excl_devices,
            "multi_catchment_devices": multi_devices,
            "device_overlap_pct": round((multi_devices / tot_devices) * 100.0, 2) if tot_devices > 0 else 0.0,
        })

    # Network-wide unique counts (calculated independently)
    total_unique_pings = len(ping_to_stores)
    total_unique_devices = len(device_to_stores)
    network_exclusive_pings = sum(1 for s_set in ping_to_stores.values() if len(s_set) == 1)
    network_multi_pings = sum(1 for s_set in ping_to_stores.values() if len(s_set) > 1)
    network_exclusive_devices = sum(1 for s_set in device_to_stores.values() if len(s_set) == 1)
    network_multi_devices = sum(1 for s_set in device_to_stores.values() if len(s_set) > 1)

    # Reconciliation assertions
    reconciled_pings = (network_exclusive_pings + network_multi_pings) == total_unique_pings
    reconciled_memberships = (sum_exclusive_pings + sum_overlapping_pings) == sum_total_pings
    reconciled_devices = (network_exclusive_devices + network_multi_devices) == total_unique_devices
    reconciled_store_pings = (sum_exclusive_pings == network_exclusive_pings)

    all_reconciled = (
        reconciled_pings
        and reconciled_memberships
        and reconciled_devices
        and reconciled_store_pings
    )

    return {
        "store_overlap_summary": store_rows,
        "network_metrics": {
            "total_memberships": sum_total_pings,
            "total_unique_pings": total_unique_pings,
            "distinct_matched_pings": total_unique_pings,
            "total_unique_devices": total_unique_devices,
            "distinct_matched_devices": total_unique_devices,
            "exclusive_unique_pings": network_exclusive_pings,
            "unique_multi_catchment_pings": network_multi_pings,
            "overlapping_membership_instances": sum_overlapping_pings,
            "exclusive_devices": network_exclusive_devices,
            "multi_catchment_devices": network_multi_devices,
            "ping_reconciliation_valid": reconciled_pings,
            "membership_reconciliation_valid": reconciled_memberships,
            "device_reconciliation_valid": reconciled_devices,
            "reconciliation_status": "RECONCILED" if all_reconciled else "FAILED",
        },
    }


# ==============================================================================
# Spark Session Management
# ==============================================================================

def create_spark_session(app_name: str = "GeoPulse-RetailMobilityAnalytics", master: str = "local[2]"):
    """Create a PySpark session for retail mobility analytics."""
    if "JAVA_HOME" not in os.environ:
        default_jdk = Path(r"C:\Program Files\Eclipse Adoptium\jdk-17.0.21.9-hotspot")
        if default_jdk.exists():
            os.environ["JAVA_HOME"] = str(default_jdk)
            os.environ["PATH"] = f"{default_jdk / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}"

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
    )
    return builder.getOrCreate()


# ==============================================================================
# PySpark DataFrame Analytics Implementations
# ==============================================================================

def load_and_prepare_memberships(spark_session, config: RetailAnalyticsConfig):
    """Load membership CSV dataset, validate, and compute temporal classification columns.

    Validates:
    - Path existence, non-empty files, and header match.
    - Columns match canonical EXPECTED_MEMBERSHIPS_COLUMNS.
    - No null or blank values in any of the 8 canonical columns.
    - No duplicate (ping_id, store_id) pairs (rejects duplicates; does not silently drop them).
    - movement_mode values adhere strictly to MovementMode enum.
    - data_provenance matches DATA_PROVENANCE_TAG.
    - distance_meters is non-negative and finite.
    - event_timestamp matches ISO-8601 UTC regex and parses to valid UTC timestamp without nulls.
    - Re-projected local timestamp in target timezone does not produce nulls.

    Transforms:
    - Parses event_timestamp as UTC timestamp (ts_utc).
    - Re-projects timestamp to local business timezone (ts_local).
    - Extracts calendar_date ('yyyy-MM-dd') and hour_of_day (0-23).
    - Classifies time_of_day into morning, afternoon, evening, night.
    """
    import pyspark.sql.functions as F

    input_path = config.memberships_input_path
    validate_memberships_input(input_path)

    df = spark_session.read.option("header", "true").csv(input_path)

    # 1. Validate schema
    actual_columns = df.columns
    if not validate_memberships_schema(actual_columns):
        raise ValueError(
            f"Input DataFrame columns {actual_columns} do not match expected schema {EXPECTED_MEMBERSHIPS_COLUMNS}."
        )

    # 2. Validate non-null and non-blank constraints on ALL 8 canonical columns
    null_checks = [
        F.count(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), c)).alias(c)
        for c in EXPECTED_MEMBERSHIPS_COLUMNS
    ]
    null_row = df.select(null_checks).collect()[0]
    for c in EXPECTED_MEMBERSHIPS_COLUMNS:
        if null_row[c] > 0:
            raise ValueError(
                f"Input validation failed: found {null_row[c]} null or blank values in required column '{c}'."
            )

    # 3. Validate duplicate (ping_id, store_id) pairs (reject; do NOT silently deduplicate)
    dup_pairs = (
        df.groupBy("ping_id", "store_id")
        .agg(F.count("*").alias("pair_count"))
        .filter(F.col("pair_count") > 1)
    )
    dup_count = dup_pairs.count()
    if dup_count > 0:
        sample_dups = dup_pairs.limit(5).collect()
        sample_str = ", ".join(
            f"(ping_id={r['ping_id']}, store_id={r['store_id']}, count={r['pair_count']})"
            for r in sample_dups
        )
        raise ValueError(
            f"Duplicate membership validation failed: found {dup_count} duplicate (ping_id, store_id) "
            f"membership pairs in input dataset. Duplicates are rejected and must not be silently deduplicated. "
            f"Sample duplicates: {sample_str}"
        )

    # 4. Validate movement_mode values
    allowed_modes = [mode.value for mode in MovementMode]
    invalid_modes = df.filter(~F.col("movement_mode").isin(allowed_modes))
    invalid_modes_count = invalid_modes.count()
    if invalid_modes_count > 0:
        sample_modes = [r["movement_mode"] for r in invalid_modes.limit(5).collect()]
        raise ValueError(
            f"Input validation failed: found {invalid_modes_count} records with invalid movement_mode. "
            f"Allowed values: {allowed_modes}. Sample invalid values: {sample_modes}"
        )

    # 5. Validate data_provenance value
    invalid_prov = df.filter(F.col("data_provenance") != DATA_PROVENANCE_TAG)
    invalid_prov_count = invalid_prov.count()
    if invalid_prov_count > 0:
        sample_prov = [r["data_provenance"] for r in invalid_prov.limit(5).collect()]
        raise ValueError(
            f"Input validation failed: found {invalid_prov_count} records with unexpected data_provenance "
            f"(expected '{DATA_PROVENANCE_TAG}'). Sample invalid values: {sample_prov}"
        )

    # 6. Validate distance_meters (must be non-negative, finite number; reject null, non-numeric, NaN, +/-inf, negative)
    dist_col = F.col("distance_meters").cast("double")
    invalid_dist = df.filter(
        dist_col.isNull()
        | F.isnan(dist_col)
        | (dist_col < 0.0)
        | (dist_col == float("inf"))
        | (dist_col == float("-inf"))
        | F.lower(F.trim(F.col("distance_meters"))).isin(
            "nan", "inf", "-inf", "+inf", "infinity", "-infinity", "+infinity"
        )
    )
    invalid_dist_count = invalid_dist.count()
    if invalid_dist_count > 0:
        sample_bad = [r["distance_meters"] for r in invalid_dist.limit(5).collect()]
        raise ValueError(
            f"Input validation failed: found {invalid_dist_count} records with null, non-numeric, NaN, "
            f"infinite, or negative distance_meters. Sample invalid values: {sample_bad}"
        )

    # 7. Validate event_timestamp format (must be UTC ending in 'Z' or '+00:00')
    utc_pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|\+00:00)$"
    invalid_ts = df.filter(~F.col("event_timestamp").rlike(utc_pattern))
    invalid_ts_count = invalid_ts.count()
    if invalid_ts_count > 0:
        sample_ts = [r["event_timestamp"] for r in invalid_ts.limit(5).collect()]
        raise ValueError(
            f"Timestamp validation failed: found {invalid_ts_count} records with non-UTC or malformed "
            f"event_timestamp (must be ISO-8601 UTC ending in 'Z' or '+00:00'). Sample invalid: {sample_ts}"
        )

    # 8. Parse UTC timestamp and re-project to local business timezone
    ts_utc = F.to_timestamp(F.col("event_timestamp"))
    unparseable_utc = df.filter(ts_utc.isNull()).count()
    if unparseable_utc > 0:
        raise ValueError(
            f"Timestamp validation failed: found {unparseable_utc} records where event_timestamp "
            f"could not be parsed to timestamp."
        )

    ts_local = F.from_utc_timestamp(ts_utc, config.timezone)
    unparseable_local = df.filter(ts_local.isNull()).count()
    if unparseable_local > 0:
        raise ValueError(
            f"Timezone conversion failed: found {unparseable_local} records where conversion to "
            f"timezone '{config.timezone}' produced null."
        )

    enriched_df = (
        df.withColumn("ts_utc", ts_utc)
        .withColumn("ts_local", ts_local)
        .withColumn("calendar_date", F.date_format(ts_local, "yyyy-MM-dd"))
        .withColumn("hour_of_day", F.hour(ts_local))
        .withColumn(
            "time_of_day",
            F.when((F.col("hour_of_day") >= 6) & (F.col("hour_of_day") <= 11), "morning")
            .when((F.col("hour_of_day") >= 12) & (F.col("hour_of_day") <= 16), "afternoon")
            .when((F.col("hour_of_day") >= 17) & (F.col("hour_of_day") <= 21), "evening")
            .otherwise("night"),
        )
    )

    return enriched_df


def compute_hourly_store_activity(memberships_df):
    """Aggregate distinct devices and ping counts by store, date, and hour.

    Produces:
        store_id, store_name, calendar_date, hour_of_day, distinct_device_count, ping_count
    """
    import pyspark.sql.functions as F

    return (
        memberships_df.groupBy("store_id", "store_name", "calendar_date", "hour_of_day")
        .agg(
            F.countDistinct("device_id").alias("distinct_device_count"),
            F.count("ping_id").alias("ping_count"),
        )
        .orderBy("store_id", "calendar_date", "hour_of_day")
    )


def compute_store_time_of_day_activity(memberships_df):
    """Aggregate distinct devices and ping counts by store and diurnal period.

    Produces:
        store_id, store_name, time_of_day, distinct_device_count, ping_count
    """
    import pyspark.sql.functions as F

    return (
        memberships_df.groupBy("store_id", "store_name", "time_of_day")
        .agg(
            F.countDistinct("device_id").alias("distinct_device_count"),
            F.count("ping_id").alias("ping_count"),
        )
        .orderBy("store_id", "time_of_day")
    )


def compute_daily_store_summary(memberships_df):
    """Aggregate daily store summary metrics including active hours and observation density.

    Produces:
        store_id, store_name, calendar_date, distinct_devices, ping_count, active_hours_count, pings_per_device
    """
    import pyspark.sql.functions as F

    return (
        memberships_df.groupBy("store_id", "store_name", "calendar_date")
        .agg(
            F.countDistinct("device_id").alias("distinct_devices"),
            F.count("ping_id").alias("ping_count"),
            F.countDistinct("hour_of_day").alias("active_hours_count"),
            F.round(F.count("ping_id") / F.countDistinct("device_id"), 2).alias("pings_per_device"),
        )
        .orderBy("store_id", "calendar_date")
    )


def compute_store_overlap_summary(memberships_df):
    """Compute store overlap summary distinguishing exclusive from overlapping pings and devices.

    Produces:
        store_id, store_name, total_pings, exclusive_pings, overlapping_pings,
        total_devices, exclusive_devices, multi_catchment_devices
    """
    memberships_df.createOrReplaceTempView("mem_view")

    sql = """
    WITH ping_counts AS (
        SELECT ping_id, COUNT(DISTINCT store_id) AS ping_store_count
        FROM mem_view
        GROUP BY ping_id
    ),
    device_counts AS (
        SELECT device_id, COUNT(DISTINCT store_id) AS device_store_count
        FROM mem_view
        GROUP BY device_id
    ),
    tagged AS (
        SELECT
            m.store_id,
            m.store_name,
            m.ping_id,
            m.device_id,
            CASE WHEN p.ping_store_count = 1 THEN 1 ELSE 0 END AS is_exclusive_ping,
            CASE WHEN p.ping_store_count > 1 THEN 1 ELSE 0 END AS is_overlapping_ping,
            CASE WHEN d.device_store_count = 1 THEN m.device_id ELSE NULL END AS excl_device,
            CASE WHEN d.device_store_count > 1 THEN m.device_id ELSE NULL END AS multi_device
        FROM mem_view m
        INNER JOIN ping_counts p ON m.ping_id = p.ping_id
        INNER JOIN device_counts d ON m.device_id = d.device_id
    )
    SELECT
        store_id,
        store_name,
        COUNT(ping_id) AS total_pings,
        SUM(is_exclusive_ping) AS exclusive_pings,
        SUM(is_overlapping_ping) AS overlapping_pings,
        COUNT(DISTINCT device_id) AS total_devices,
        COUNT(DISTINCT excl_device) AS exclusive_devices,
        COUNT(DISTINCT multi_device) AS multi_catchment_devices
    FROM tagged
    GROUP BY store_id, store_name
    ORDER BY store_id
    """
    return memberships_df.sparkSession.sql(sql)


# ==============================================================================
# Safe Output Persistence & Preflight Protection
# ==============================================================================

def validate_output_destinations_preflight(config: RetailAnalyticsConfig) -> List[Path]:
    """Validate all four output destinations before initiating any output writing.

    Safety considerations and write semantics:
    - Resolves all four target destinations upfront.
    - If overwrite is False, checks if ANY of the four destinations already exist.
    - If any destination exists, raises FileExistsError immediately before writing any output.
    - Ensures that an existing later output directory (e.g. store_overlap_summary) prevents
      earlier output directories (e.g. hourly_store_activity) from being created.
    - Note on partial outputs: Spark writes datasets to disk sequentially directory-by-directory
      without distributed multi-directory transactions. If an unhandled failure (e.g., process termination
      or disk exhaustion) occurs midway through writing, previously written directories will remain on disk.
      To minimize this risk, all data validation, metric computation, and reconciliation checks execute
      strictly before any write operations begin.
    - Overwrite remains disabled by default (overwrite=False).

    Returns:
        List of resolved output Paths [hourly, time_of_day, daily, overlap].
    """
    destinations = [
        Path(config.get_hourly_path()).resolve(),
        Path(config.get_time_of_day_path()).resolve(),
        Path(config.get_daily_path()).resolve(),
        Path(config.get_overlap_path()).resolve(),
    ]

    if not config.overwrite:
        existing = [str(p) for p in destinations if p.exists()]
        if existing:
            raise FileExistsError(
                f"Output preflight validation failed: {len(existing)} target destination(s) already exist: "
                f"{existing}. Specify overwrite=True to allow replacement, or remove existing outputs."
            )

    return destinations


def write_spark_csv_output(
    df,
    output_dir: str | Path,
    selected_columns: Optional[List[str]] = None,
    overwrite: bool = False,
) -> Path:
    """Safely write DataFrame to Spark CSV directory with headers.

    Enforces safety guarantees:
    - Never deletes or replaces existing output directories automatically.
    - If output_dir exists and overwrite is False, raises FileExistsError immediately.
    - Uses Spark's native CSV writer with mode('errorifexists') by default.
    - Only if overwrite is explicitly True, allows Spark write with mode('overwrite').
    """
    out_path = Path(output_dir).resolve()

    if out_path.exists() and not overwrite:
        raise FileExistsError(
            f"Output directory already exists: '{out_path}'. "
            "To prevent accidental data loss, existing outputs are not replaced automatically. "
            "Specify overwrite=True to allow replacement."
        )

    if selected_columns:
        df = df.select(*selected_columns)

    write_mode = "overwrite" if overwrite else "errorifexists"
    df.write.mode(write_mode).option("header", "true").csv(str(out_path))

    logger.info("Successfully wrote Spark CSV output: %s", out_path)
    return out_path


# ==============================================================================
# Pipeline Orchestrator
# ==============================================================================

def run_retail_mobility_analytics(
    config: Optional[RetailAnalyticsConfig] = None,
    spark_session=None,
) -> Dict[str, Any]:
    """Execute end-to-end GeoPulse Week 2 retail mobility analytics pipeline.

    Steps:
    1. Preflight destination check (ensures no destination exists if overwrite=False).
    2. Validate input path existence, schema, and non-empty status.
    3. Initialize/use SparkSession.
    4. Ingest, validate schema/fields/timestamps/duplicates, and temporally enrich DataFrame.
    5. Compute hourly store activity.
    6. Compute diurnal time-of-day store activity.
    7. Compute daily store summary.
    8. Compute store overlap summary.
    9. Perform preflight output destination verification.
    10. Write the 4 output datasets safely.
    11. Compute and validate full independent overlap reconciliations.
    12. Return execution summary and metrics.
    """
    cfg = config or RetailAnalyticsConfig()

    # Preflight check on all destinations before any computation
    validate_output_destinations_preflight(cfg)

    close_session = False
    if spark_session is None:
        spark_session = create_spark_session()
        close_session = True

    try:
        import pyspark.sql.functions as F

        # Load & validate memberships
        memberships_df = load_and_prepare_memberships(spark_session, cfg)
        total_memberships = memberships_df.count()

        if total_memberships == 0:
            raise ValueError(f"Memberships dataset is empty: {cfg.memberships_input_path}")

        # 1. Hourly activity
        hourly_df = compute_hourly_store_activity(memberships_df)

        # 2. Time-of-day activity
        tod_df = compute_store_time_of_day_activity(memberships_df)

        # 3. Daily store summary
        daily_df = compute_daily_store_summary(memberships_df)

        # 4. Store overlap summary
        overlap_df = compute_store_overlap_summary(memberships_df)

        # Collect scalar summary metrics for execution report and validation
        overlap_rows = overlap_df.collect()
        store_overlap_metrics = [
            {
                "store_id": r["store_id"],
                "store_name": r["store_name"],
                "total_pings": int(r["total_pings"]),
                "exclusive_pings": int(r["exclusive_pings"]),
                "overlapping_pings": int(r["overlapping_pings"]),
                "total_devices": int(r["total_devices"]),
                "exclusive_devices": int(r["exclusive_devices"]),
                "multi_catchment_devices": int(r["multi_catchment_devices"]),
            }
            for r in overlap_rows
        ]

        # Independent calculations for full network reconciliation:
        distinct_pings = memberships_df.select("ping_id").distinct().count()
        distinct_devices = memberships_df.select("device_id").distinct().count()

        ping_store_counts = memberships_df.groupBy("ping_id").agg(F.countDistinct("store_id").alias("store_count"))
        exclusive_unique_pings = ping_store_counts.filter(F.col("store_count") == 1).count()
        unique_multi_catchment_pings = ping_store_counts.filter(F.col("store_count") > 1).count()

        device_store_counts = memberships_df.groupBy("device_id").agg(F.countDistinct("store_id").alias("store_count"))
        network_exclusive_devices = device_store_counts.filter(F.col("store_count") == 1).count()
        network_multi_catchment_devices = device_store_counts.filter(F.col("store_count") > 1).count()

        sum_exclusive_pings = sum(m["exclusive_pings"] for m in store_overlap_metrics)
        sum_overlapping_instances = sum(m["overlapping_pings"] for m in store_overlap_metrics)

        # Reconciliation validations:
        ping_reconciliation = (exclusive_unique_pings + unique_multi_catchment_pings == distinct_pings)
        membership_reconciliation = (sum_exclusive_pings + sum_overlapping_instances == total_memberships)
        store_ping_reconciliation = (sum_exclusive_pings == exclusive_unique_pings)
        device_reconciliation = (network_exclusive_devices + network_multi_catchment_devices == distinct_devices)

        all_reconciled = (
            ping_reconciliation
            and membership_reconciliation
            and store_ping_reconciliation
            and device_reconciliation
        )

        if not all_reconciled:
            reconciliation_failures = []
            if not ping_reconciliation:
                reconciliation_failures.append(
                    f"Ping mismatch: exclusive({exclusive_unique_pings}) + multi({unique_multi_catchment_pings}) != distinct({distinct_pings})"
                )
            if not membership_reconciliation:
                reconciliation_failures.append(
                    f"Membership mismatch: sum_exclusive({sum_exclusive_pings}) + overlapping_instances({sum_overlapping_instances}) != total({total_memberships})"
                )
            if not store_ping_reconciliation:
                reconciliation_failures.append(
                    f"Store sum mismatch: sum_exclusive({sum_exclusive_pings}) != network_exclusive({exclusive_unique_pings})"
                )
            if not device_reconciliation:
                reconciliation_failures.append(
                    f"Device mismatch: exclusive({network_exclusive_devices}) + multi({network_multi_catchment_devices}) != distinct({distinct_devices})"
                )
            fail_msg = "; ".join(reconciliation_failures)
            raise ValueError(
                f"Network reconciliation failed before output persistence: {fail_msg}. "
                f"No output datasets have been created."
            )

        # Preflight destination check again before writing outputs
        validate_output_destinations_preflight(cfg)

        # Persist the 4 artifacts safely only after all reconciliations have succeeded
        write_spark_csv_output(hourly_df, cfg.get_hourly_path(), overwrite=cfg.overwrite)
        write_spark_csv_output(tod_df, cfg.get_time_of_day_path(), overwrite=cfg.overwrite)
        write_spark_csv_output(daily_df, cfg.get_daily_path(), overwrite=cfg.overwrite)
        write_spark_csv_output(overlap_df, cfg.get_overlap_path(), overwrite=cfg.overwrite)

        return {
            "status": "SUCCESS",
            "timezone": cfg.timezone,
            "total_memberships": total_memberships,
            "distinct_matched_pings": distinct_pings,
            "distinct_matched_devices": distinct_devices,
            "exclusive_pings_total": exclusive_unique_pings,
            "multi_catchment_pings_total": unique_multi_catchment_pings,
            "overlapping_membership_instances": sum_overlapping_instances,
            "exclusive_devices_total": network_exclusive_devices,
            "multi_catchment_devices_total": network_multi_catchment_devices,
            "ping_reconciliation_valid": ping_reconciliation,
            "membership_reconciliation_valid": membership_reconciliation,
            "device_reconciliation_valid": device_reconciliation,
            "reconciliation_status": "RECONCILED" if all_reconciled else "FAILED",
            "store_overlap_summary": store_overlap_metrics,
            "outputs": {
                "hourly_store_activity": cfg.get_hourly_path(),
                "store_time_of_day_activity": cfg.get_time_of_day_path(),
                "daily_store_summary": cfg.get_daily_path(),
                "store_overlap_summary": cfg.get_overlap_path(),
            },
        }

    finally:
        if close_session:
            spark_session.stop()


# ==============================================================================
# CLI Entrypoint
# ==============================================================================

def main():
    """Command-line interface for GeoPulse retail mobility analytics."""
    parser = argparse.ArgumentParser(
        description="GeoPulse Hyper-Local Retail Mobility Analytics Pipeline (PySpark)"
    )
    parser.add_argument(
        "--memberships-input",
        default=DEFAULT_MEMBERSHIPS_PATH,
        help="Path to store catchment memberships CSV directory or file",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_BASE,
        help="Base output directory for retail mobility analytics",
    )
    parser.add_argument(
        "--timezone",
        default=DEFAULT_TIMEZONE,
        help=f"Target local timezone for diurnal analytics (default: {DEFAULT_TIMEZONE})",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Explicitly permit overwriting pre-existing output directories",
    )

    args = parser.parse_args()

    config = RetailAnalyticsConfig(
        memberships_input_path=args.memberships_input,
        output_dir=args.output_dir,
        timezone=args.timezone,
        overwrite=args.overwrite,
    )

    print("================================================================================")
    print(" GeoPulse Retail Mobility Analytics Pipeline — Week 2 Execution")
    print("================================================================================")
    print(f"Memberships Input : {config.memberships_input_path}")
    print(f"Output Base Dir   : {config.output_dir}")
    print(f"Operating Timezone: {config.timezone}")
    print(f"Overwrite Mode    : {config.overwrite}")
    print("--------------------------------------------------------------------------------")

    results = run_retail_mobility_analytics(config)

    print(f"\nExecution Status : {results['status']}")
    print(f"Total Memberships: {results['total_memberships']:,}")
    print(f"Distinct Pings   : {results['distinct_matched_pings']:,}")
    print(f"Distinct Devices : {results['distinct_matched_devices']:,} (across all stores)")
    print(f"Exclusive Pings  : {results['exclusive_pings_total']:,} (pings in exactly 1 catchment)")
    print(f"Multi-Catchment  : {results['multi_catchment_pings_total']:,} (unique pings in >=2 catchments)")
    print(f"Overlapping Recs : {results['overlapping_membership_instances']:,} (membership instances)")
    print(f"Exclusive Devs   : {results['exclusive_devices_total']:,} (devices in exactly 1 catchment)")
    print(f"Multi-Catch Devs : {results['multi_catchment_devices_total']:,} (devices in >=2 catchments)")
    print(f"Reconciliation   : {results['reconciliation_status']}")

    print("\nStore Overlap Summary Breakdown:")
    print(f"{'Store ID':<10} {'Store Name':<22} {'Total Pings':<13} {'Excl Pings':<12} {'Over Pings':<12} {'Total Devs':<12} {'Excl Devs':<11} {'Multi Devs':<11}")
    print("-" * 105)
    for s in results["store_overlap_summary"]:
        print(
            f"{s['store_id']:<10} "
            f"{s['store_name']:<22} "
            f"{s['total_pings']:<13} "
            f"{s['exclusive_pings']:<12} "
            f"{s['overlapping_pings']:<12} "
            f"{s['total_devices']:<12} "
            f"{s['exclusive_devices']:<11} "
            f"{s['multi_catchment_devices']:<11}"
        )
    print("--------------------------------------------------------------------------------")
    print("Generated Artifacts:")
    for key, path in results["outputs"].items():
        print(f"  - {key}: {path}")
    print("================================================================================\n")


if __name__ == "__main__":
    main()
