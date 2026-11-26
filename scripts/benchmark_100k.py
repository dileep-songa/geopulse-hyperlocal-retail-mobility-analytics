"""GeoPulse 100K Spatial Benchmark Harness.

Standalone, reproducible benchmark runner for the GeoPulse Week 2 spatial
analytics pipeline executing on 100,000 synthetic mobility observations.

Key Architectural Guarantees:
- Enforces strict preflight host RAM safety check (minimum 4 GiB available) at benchmark start
  and repeats this check immediately before JVM initialization.
- Directly builds SedonaContext with 1536 MB driver heap and 4 shuffle partitions.
- Configures PYSPARK_SUBMIT_ARGS pre-launch to guarantee JVM launches with -Xmx1536m.
- Verifies JVM runtime maxMemory and startup arguments (-Xmx1536m) prior to dataset ingestion.
- Passes external Sedona session to `src.spatial_analysis.run_spatial_analysis()`,
  bypassing the hardcoded 2g session creation without modifying production source code.
- Writes outputs strictly into isolated run directories (benchmark_100k_run1, benchmark_100k_run2).
- Pre-checks all destination output directories before Spark or Java initializes.
- Never overwrites or touches existing Week 1 / Week 2 baseline artifacts (overwrite=False).
- Requires comprehensive Run 1 completion with matching input hashes, verified metrics,
  physical artifact recounts, and explicit Boolean reconciliation validation before permitting Run 2.
- Samples process and host memory telemetry continuously throughout execution with reliable psutil and Win32 fallback discovery.
- Attempts to generate a comprehensive JSON benchmark manifest inside the run directory for both success and post-initialization failures (including secondary preflight SystemExit safety aborts) after the run directory has been created. Pre-run preflight checks (initial RAM check, input validation, Run 2 prerequisites, destination collision checks) abort cleanly prior to directory creation and do not produce a manifest.
- Requires all five lifecycle conditions (execution, validation, reconciliation, session shutdown, monitor shutdown) AND successful manifest persistence on disk for overall success.
- Independent SparkSession termination and memory monitor cleanup via separate try...finally blocks, with explicit thread-join timeout enforcement and accurate failure reporting (does not claim guaranteed termination without confirmation).

Disclaimer:
This benchmark evaluates execution performance, memory footprint, and reproducibility
for a 100,000-observation workload on a local developer workstation. It does NOT prove
or claim linear scalability to multi-million or billion-observation distributed workloads.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
from pathlib import Path
import platform
import sys
import threading
import time
import traceback
from typing import Any, Dict, List, Optional, Tuple

# Try importing psutil; if not available, native Windows Win32 API fallback will be loaded on demand
try:
    import psutil  # type: ignore
except ImportError:
    psutil = None

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.spatial_analysis import (
    SpatialAnalysisConfig,
    find_sedona_jars,
    run_spatial_analysis,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("geopulse.benchmark_100k")

# ==============================================================================
# Constants & Configuration
# ==============================================================================

EXPECTED_100K_SHA256 = "0559637e8455ba2a06aeaa7710b2cc4184be832a19ea4f752a1064e6073a4374"
EXPECTED_RECORD_COUNT = 100_000
EXPECTED_STORE_COUNT = 4
MIN_AVAILABLE_RAM_BYTES = 4 * 1024 * 1024 * 1024  # Exactly 4 GiB

DEFAULT_MOBILITY_PATH = PROJECT_ROOT / "data" / "raw" / "synthetic_mobility_pings_100k.csv"
DEFAULT_STORE_PATH = PROJECT_ROOT / "data" / "reference" / "synthetic_store_locations.csv"
BASE_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

TARGET_DRIVER_MEMORY = "1536m"
TARGET_SHUFFLE_PARTITIONS = 4
TARGET_MASTER = "local[2]"
TARGET_TIMEZONE = "UTC"

WINDOWS_HADOOP_SHIM_JAR = PROJECT_ROOT / "tools" / "shims" / "windows-hadoop-shim.jar"
WINDOWS_HADOOP_FS_IMPL = "com.geopulse.hadoop.WindowsLocalFileSystem"


# ==============================================================================
# Lazy-Loaded Native Windows Win32 Memory API Fallback
# ==============================================================================

class WindowsMemoryApi:
    """Encapsulates Win32 memory APIs, loaded only on Windows when needed."""

    _initialized: bool = False
    _kernel32: Any = None
    _psapi: Any = None
    _MEMORYSTATUSEX: Any = None
    _PROCESS_MEMORY_COUNTERS_EX: Any = None
    _PROCESSENTRY32: Any = None

    @classmethod
    def _initialize(cls) -> None:
        if cls._initialized:
            return

        current_os = platform.system()
        if current_os != "Windows":
            raise RuntimeError(
                f"Native memory API is only supported on Windows (detected: '{current_os}'). "
                "Please install the 'psutil' package for cross-platform memory monitoring."
            )

        import ctypes
        import ctypes.wintypes

        class MEMORYSTATUSEX(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.wintypes.DWORD),
                ("dwMemoryLoad", ctypes.wintypes.DWORD),
                ("ullTotalPhys", ctypes.c_uint64),
                ("ullAvailPhys", ctypes.c_uint64),
                ("ullTotalPageFile", ctypes.c_uint64),
                ("ullAvailPageFile", ctypes.c_uint64),
                ("ullTotalVirtual", ctypes.c_uint64),
                ("ullAvailVirtual", ctypes.c_uint64),
                ("ullAvailExtendedVirtual", ctypes.c_uint64),
            ]

        class PROCESS_MEMORY_COUNTERS_EX(ctypes.Structure):
            _fields_ = [
                ("cb", ctypes.wintypes.DWORD),
                ("PageFaultCount", ctypes.wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
                ("PrivateUsage", ctypes.c_size_t),
            ]

        class PROCESSENTRY32(ctypes.Structure):
            _fields_ = [
                ("dwSize", ctypes.wintypes.DWORD),
                ("cntUsage", ctypes.wintypes.DWORD),
                ("th32ProcessID", ctypes.wintypes.DWORD),
                ("th32DefaultHeapID", ctypes.c_size_t),
                ("th32ModuleID", ctypes.wintypes.DWORD),
                ("cntThreads", ctypes.wintypes.DWORD),
                ("th32ParentProcessID", ctypes.wintypes.DWORD),
                ("pcPriClassBase", ctypes.c_long),
                ("dwFlags", ctypes.wintypes.DWORD),
                ("szExeFile", ctypes.c_char * 260),
            ]

        cls._MEMORYSTATUSEX = MEMORYSTATUSEX
        cls._PROCESS_MEMORY_COUNTERS_EX = PROCESS_MEMORY_COUNTERS_EX
        cls._PROCESSENTRY32 = PROCESSENTRY32

        cls._kernel32 = ctypes.windll.kernel32
        cls._psapi = ctypes.windll.psapi

        cls._kernel32.GlobalMemoryStatusEx.argtypes = [ctypes.POINTER(cls._MEMORYSTATUSEX)]
        cls._kernel32.GlobalMemoryStatusEx.restype = ctypes.wintypes.BOOL

        cls._kernel32.OpenProcess.argtypes = [
            ctypes.wintypes.DWORD,
            ctypes.wintypes.BOOL,
            ctypes.wintypes.DWORD,
        ]
        cls._kernel32.OpenProcess.restype = ctypes.wintypes.HANDLE

        cls._kernel32.CloseHandle.argtypes = [ctypes.wintypes.HANDLE]
        cls._kernel32.CloseHandle.restype = ctypes.wintypes.BOOL

        cls._psapi.GetProcessMemoryInfo.argtypes = [
            ctypes.wintypes.HANDLE,
            ctypes.POINTER(cls._PROCESS_MEMORY_COUNTERS_EX),
            ctypes.wintypes.DWORD,
        ]
        cls._psapi.GetProcessMemoryInfo.restype = ctypes.wintypes.BOOL

        cls._kernel32.CreateToolhelp32Snapshot.argtypes = [
            ctypes.wintypes.DWORD,
            ctypes.wintypes.DWORD,
        ]
        cls._kernel32.CreateToolhelp32Snapshot.restype = ctypes.wintypes.HANDLE

        cls._kernel32.Process32First.argtypes = [
            ctypes.wintypes.HANDLE,
            ctypes.POINTER(cls._PROCESSENTRY32),
        ]
        cls._kernel32.Process32First.restype = ctypes.wintypes.BOOL

        cls._kernel32.Process32Next.argtypes = [
            ctypes.wintypes.HANDLE,
            ctypes.POINTER(cls._PROCESSENTRY32),
        ]
        cls._kernel32.Process32Next.restype = ctypes.wintypes.BOOL

        cls._initialized = True

    @classmethod
    def get_host_memory(cls) -> Dict[str, int]:
        cls._initialize()
        import ctypes
        status = cls._MEMORYSTATUSEX()
        status.dwLength = ctypes.sizeof(cls._MEMORYSTATUSEX)
        if not cls._kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            raise OSError("Win32 GlobalMemoryStatusEx failed.")
        return {
            "total": int(status.ullTotalPhys),
            "available": int(status.ullAvailPhys),
            "used": int(status.ullTotalPhys - status.ullAvailPhys),
            "percent": int(status.dwMemoryLoad),
        }

    @classmethod
    def get_process_memory(cls, pid: int) -> Optional[Dict[str, int]]:
        cls._initialize()
        import ctypes
        invalid_handles = {None, 0, -1, ctypes.c_void_p(-1).value}
        PROCESS_QUERY_INFORMATION = 0x0400
        PROCESS_VM_READ = 0x0010
        h = cls._kernel32.OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, False, pid)
        if h in invalid_handles or not h:
            return None
        try:
            pmc = cls._PROCESS_MEMORY_COUNTERS_EX()
            pmc.cb = ctypes.sizeof(cls._PROCESS_MEMORY_COUNTERS_EX)
            if cls._psapi.GetProcessMemoryInfo(h, ctypes.byref(pmc), pmc.cb):
                return {
                    "rss": int(pmc.WorkingSetSize),
                    "peak_wset": int(pmc.PeakWorkingSetSize),
                }
        finally:
            if h not in invalid_handles and h:
                cls._kernel32.CloseHandle(h)
        return None

    @classmethod
    def find_child_pids(cls, parent_pid: int) -> List[Tuple[int, str]]:
        cls._initialize()
        import ctypes
        invalid_handles = {None, 0, -1, ctypes.c_void_p(-1).value}
        TH32CS_SNAPPROCESS = 0x00000002
        h = cls._kernel32.CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0)
        if h in invalid_handles or not h:
            return []
        children: List[Tuple[int, str]] = []
        pe = cls._PROCESSENTRY32()
        pe.dwSize = ctypes.sizeof(cls._PROCESSENTRY32)
        try:
            if cls._kernel32.Process32First(h, ctypes.byref(pe)):
                while True:
                    if pe.th32ParentProcessID == parent_pid:
                        exe_name = pe.szExeFile.decode("latin1", "ignore").strip()
                        children.append((int(pe.th32ProcessID), exe_name))
                    if not cls._kernel32.Process32Next(h, ctypes.byref(pe)):
                        break
        finally:
            if h not in invalid_handles and h:
                cls._kernel32.CloseHandle(h)
        return children


def query_host_available_memory_bytes() -> int:
    """Query available physical memory in bytes using psutil or native Windows fallback."""
    if psutil is not None:
        return int(psutil.virtual_memory().available)
    return WindowsMemoryApi.get_host_memory()["available"]


def query_host_memory_stats() -> Dict[str, Any]:
    """Query total, used, available, and percent memory using psutil or native Windows fallback."""
    if psutil is not None:
        mem = psutil.virtual_memory()
        return {
            "total": int(mem.total),
            "available": int(mem.available),
            "used": int(mem.used),
            "percent": float(mem.percent),
        }
    return WindowsMemoryApi.get_host_memory()


# ==============================================================================
# Preflight Validation Helpers (No Spark / JVM Required)
# ==============================================================================

def check_host_memory_safety(min_bytes: int = MIN_AVAILABLE_RAM_BYTES) -> int:
    """Verify that current host available physical memory meets or exceeds the required threshold.

    This threshold is non-negotiable and cannot be bypassed via CLI arguments.
    Raises SystemExit if available memory is below threshold to abort execution safely.
    When invoked during pre-run checks (prior to run directory creation), the benchmark aborts
    cleanly without creating output directories or generating a manifest.
    When invoked as a secondary check immediately prior to JVM launch (after run directory creation),
    the failure is caught within the protected lifecycle and recorded in the failure manifest.
    """
    mem_stats = query_host_memory_stats()
    available_bytes = mem_stats["available"]
    total_bytes = mem_stats["total"]
    used_bytes = mem_stats["used"]
    percent = mem_stats["percent"]

    available_gib = available_bytes / (1024 ** 3)
    required_gib = min_bytes / (1024 ** 3)

    logger.info(
        "Host RAM Preflight: Total=%.2f GiB, Used=%.2f GiB (%.1f%%), Available=%.2f GiB (Threshold: %.2f GiB)",
        total_bytes / (1024 ** 3),
        used_bytes / (1024 ** 3),
        float(percent),
        available_gib,
        required_gib,
    )

    if available_bytes < min_bytes:
        raise SystemExit(
            f"PREFLIGHT SAFETY CHECK FAILED: Available host RAM is {available_gib:.2f} GiB, "
            f"which is below the required {required_gib:.2f} GiB safety threshold. "
            "Execution aborted to prevent Windows memory pressure and pagefile thrashing. "
            "Please close memory-intensive applications before re-running."
        )

    return available_bytes


def compute_file_sha256(file_path: Path) -> str:
    """Compute cryptographic SHA-256 hash of a file using streaming reads."""
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


def validate_input_artifacts(mobility_path: Path, store_path: Path) -> Dict[str, Any]:
    """Validate input dataset existence and cryptographic checksums.

    Executed during pre-run preflight checks before run directory creation; any validation
    failure aborts execution cleanly without creating output directories or generating a manifest.
    Verifies mobility against EXPECTED_100K_SHA256.
    Computes and records the store reference hash without inventing an unverified expected hash.
    """
    if not mobility_path.is_file():
        raise FileNotFoundError(f"Target 100K mobility dataset not found: {mobility_path}")
    if not store_path.is_file():
        raise FileNotFoundError(f"Candidate store locations reference file not found: {store_path}")

    logger.info("Computing SHA-256 checksum for mobility dataset: %s ...", mobility_path.name)
    actual_mobility_hash = compute_file_sha256(mobility_path)
    if actual_mobility_hash != EXPECTED_100K_SHA256:
        raise ValueError(
            f"Input mobility dataset SHA-256 mismatch!\n"
            f"  Expected: {EXPECTED_100K_SHA256}\n"
            f"  Actual:   {actual_mobility_hash}\n"
            "Aborting benchmark to prevent testing against modified dataset."
        )

    mobility_size_bytes = mobility_path.stat().st_size
    logger.info(
        "Mobility dataset verified: SHA-256=%s, Size=%.2f MB (%d bytes)",
        actual_mobility_hash,
        mobility_size_bytes / (1024 * 1024),
        mobility_size_bytes,
    )

    logger.info("Computing SHA-256 checksum for store reference: %s ...", store_path.name)
    store_hash = compute_file_sha256(store_path)
    store_size_bytes = store_path.stat().st_size
    logger.info("Store reference checksum recorded: SHA-256=%s, Size=%d bytes", store_hash, store_size_bytes)

    return {
        "mobility": {
            "file_name": mobility_path.name,
            "file_path": str(mobility_path.resolve()),
            "file_size_bytes": mobility_size_bytes,
            "sha256": actual_mobility_hash,
            "expected_sha256": EXPECTED_100K_SHA256,
            "verified": True,
        },
        "store_reference": {
            "file_name": store_path.name,
            "file_path": str(store_path.resolve()),
            "file_size_bytes": store_size_bytes,
            "sha256": store_hash,
            "verified": True,
        },
    }


def validate_run2_prerequisites(
    run_id: str,
    base_processed_dir: Path,
    current_input_meta: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Validate that Run 1 completed successfully and produced valid outputs before permitting Run 2.

    Executed during pre-run preflight checks before Run 2 directory creation; any prerequisite check
    failure aborts execution cleanly without creating Run 2 output directories or generating a manifest.

    Asserts:
    1. Readable, valid JSON Run 1 manifest with status == "SUCCESS".
    2. Strict lifecycle fields: execution_status == "SUCCESS", validation_status == "SUCCESS", reconciliation_status == "SUCCESS".
    3. Run 1 mobility SHA-256 matches current mobility SHA-256 and EXPECTED_100K_SHA256.
    4. Run 1 store reference SHA-256 matches current store reference SHA-256.
    5. Matching Spark driver-memory and shuffle-partitions configurations.
    6. Run 1 pipeline metrics match expected values (input_pings=100000, stores=4, total_memberships>0, overlap reconciled).
    7. Physical CSV row counts and part-files exist for all three Run 1 output directories.
    8. Cross-check against recorded Run 1 reconciliation metadata (all_reconciled is True, explicit Boolean checks,
       and persisted/expected row counts match physical recounts).
    """
    if run_id != "run2":
        return None

    run1_dir = base_processed_dir / "benchmark_100k_run1"
    manifest_path = run1_dir / "benchmark_manifest.json"

    logger.info("Verifying Run 2 prerequisites: inspecting Run 1 artifacts at %s ...", run1_dir)

    if not run1_dir.is_dir():
        raise SystemExit(
            f"PREREQUISITE CHECK FAILED FOR RUN 2: Run 1 output directory '{run1_dir}' does not exist. "
            "Run 2 cannot be executed until Run 1 has completed successfully."
        )

    if not manifest_path.is_file():
        raise SystemExit(
            f"PREREQUISITE CHECK FAILED FOR RUN 2: Run 1 manifest '{manifest_path}' does not exist. "
            "Run 2 cannot be executed until Run 1 has completed and persisted its manifest."
        )

    # 1. Readable, valid manifest
    try:
        with open(manifest_path, "r", encoding="utf-8") as f:
            run1_manifest = json.load(f)
    except Exception as exc:
        raise SystemExit(f"PREREQUISITE CHECK FAILED FOR RUN 2: Corrupted or unreadable Run 1 manifest: {exc}")

    if not isinstance(run1_manifest, dict):
        raise SystemExit("PREREQUISITE CHECK FAILED FOR RUN 2: Run 1 manifest root must be a JSON object.")

    # 2. Lifecycle status validations
    for lifecycle_field in ("status", "execution_status", "validation_status", "reconciliation_status"):
        field_val = run1_manifest.get(lifecycle_field)
        if field_val != "SUCCESS":
            raise SystemExit(
                f"PREREQUISITE CHECK FAILED FOR RUN 2: Run 1 manifest field '{lifecycle_field}' is '{field_val}', "
                "expected 'SUCCESS'."
            )

    # 3. Input mobility SHA-256 matching
    run1_mobility_hash = run1_manifest.get("inputs", {}).get("mobility", {}).get("sha256")
    curr_mobility_hash = current_input_meta.get("mobility", {}).get("sha256")
    if not run1_mobility_hash:
        raise SystemExit("PREREQUISITE CHECK FAILED FOR RUN 2: Run 1 manifest missing mobility SHA-256 metadata.")
    if run1_mobility_hash != EXPECTED_100K_SHA256:
        raise SystemExit(
            f"PREREQUISITE CHECK FAILED FOR RUN 2: Run 1 mobility hash ({run1_mobility_hash}) does not match expected ({EXPECTED_100K_SHA256})."
        )
    if run1_mobility_hash != curr_mobility_hash:
        raise SystemExit(
            f"PREREQUISITE CHECK FAILED FOR RUN 2: Run 1 mobility SHA-256 ({run1_mobility_hash}) differs from current input ({curr_mobility_hash})."
        )

    # 4. Input store reference SHA-256 matching
    run1_store_hash = run1_manifest.get("inputs", {}).get("store_reference", {}).get("sha256")
    curr_store_hash = current_input_meta.get("store_reference", {}).get("sha256")
    if not run1_store_hash:
        raise SystemExit("PREREQUISITE CHECK FAILED FOR RUN 2: Run 1 manifest missing store-reference SHA-256 metadata.")
    if not curr_store_hash:
        raise SystemExit("PREREQUISITE CHECK FAILED FOR RUN 2: Current input missing store-reference SHA-256 metadata.")
    if run1_store_hash != curr_store_hash:
        raise SystemExit(
            f"PREREQUISITE CHECK FAILED FOR RUN 2: Run 1 store SHA-256 ({run1_store_hash}) differs from current ({curr_store_hash})."
        )

    # 5. Matching Spark driver-memory and shuffle-partitions
    spark_cfg = run1_manifest.get("spark_configuration", {})
    if spark_cfg.get("driver_memory") != TARGET_DRIVER_MEMORY:
        raise SystemExit(
            f"PREREQUISITE CHECK FAILED FOR RUN 2: Run 1 driver_memory was '{spark_cfg.get('driver_memory')}', "
            f"expected '{TARGET_DRIVER_MEMORY}'."
        )
    if spark_cfg.get("shuffle_partitions") != TARGET_SHUFFLE_PARTITIONS:
        raise SystemExit(
            f"PREREQUISITE CHECK FAILED FOR RUN 2: Run 1 shuffle_partitions was '{spark_cfg.get('shuffle_partitions')}', "
            f"expected '{TARGET_SHUFFLE_PARTITIONS}'."
        )

    # 6. Verify pipeline metrics integrity in Run 1 manifest
    metrics = run1_manifest.get("pipeline_metrics", {})
    if not isinstance(metrics, dict):
        raise SystemExit("PREREQUISITE CHECK FAILED FOR RUN 2: Run 1 pipeline_metrics missing or invalid.")

    if metrics.get("input_pings_count") != EXPECTED_RECORD_COUNT:
        raise SystemExit(
            f"PREREQUISITE CHECK FAILED FOR RUN 2: Run 1 input_pings_count={metrics.get('input_pings_count')}, "
            f"expected {EXPECTED_RECORD_COUNT}."
        )
    if metrics.get("input_stores_count") != EXPECTED_STORE_COUNT:
        raise SystemExit(
            f"PREREQUISITE CHECK FAILED FOR RUN 2: Run 1 input_stores_count={metrics.get('input_stores_count')}, "
            f"expected {EXPECTED_STORE_COUNT}."
        )
    total_m = metrics.get("total_memberships")
    if not isinstance(total_m, int) or total_m <= 0:
        raise SystemExit(
            f"PREREQUISITE CHECK FAILED FOR RUN 2: Run 1 total_memberships must be a positive integer, got {total_m}."
        )
    if not metrics.get("overlap_distribution", {}).get("reconciled", False):
        raise SystemExit("PREREQUISITE CHECK FAILED FOR RUN 2: Run 1 overlap_distribution is not reconciled.")
    per_store = metrics.get("per_store_summary", [])
    if not isinstance(per_store, list) or len(per_store) != EXPECTED_STORE_COUNT:
        raise SystemExit(
            f"PREREQUISITE CHECK FAILED FOR RUN 2: Run 1 per_store_summary must contain exactly {EXPECTED_STORE_COUNT} store records."
        )

    # 7. Physical Revalidation of Run 1 Output Contents on Disk
    r1_catchments = run1_dir / "store_catchments"
    r1_memberships = run1_dir / "store_catchment_memberships"
    r1_summary = run1_dir / "store_catchment_summary"

    r1_c_rows, r1_c_parts = count_csv_records_in_directory(r1_catchments)
    r1_m_rows, r1_m_parts = count_csv_records_in_directory(r1_memberships)
    r1_s_rows, r1_s_parts = count_csv_records_in_directory(r1_summary)

    if r1_c_parts <= 0 or r1_c_rows != EXPECTED_STORE_COUNT:
        raise SystemExit(
            f"PREREQUISITE CHECK FAILED FOR RUN 2: Run 1 catchments recount mismatch: {r1_c_rows} rows in {r1_c_parts} parts "
            f"(expected {EXPECTED_STORE_COUNT})."
        )
    if r1_m_parts <= 0 or r1_m_rows != total_m:
        raise SystemExit(
            f"PREREQUISITE CHECK FAILED FOR RUN 2: Run 1 memberships recount mismatch: {r1_m_rows} rows in {r1_m_parts} parts "
            f"(expected {total_m})."
        )
    if r1_s_parts <= 0 or r1_s_rows != EXPECTED_STORE_COUNT:
        raise SystemExit(
            f"PREREQUISITE CHECK FAILED FOR RUN 2: Run 1 summary recount mismatch: {r1_s_rows} rows in {r1_s_parts} parts "
            f"(expected {EXPECTED_STORE_COUNT})."
        )

    # 8. Cross-check against recorded Run 1 reconciliation metadata (Explicit Boolean checks)
    recon = run1_manifest.get("reconciliation")
    if not isinstance(recon, dict):
        raise SystemExit(
            "PREREQUISITE CHECK FAILED FOR RUN 2: Run 1 manifest missing or malformed 'reconciliation' object."
        )
    if recon.get("all_reconciled") is not True:
        raise SystemExit(
            "PREREQUISITE CHECK FAILED FOR RUN 2: Run 1 reconciliation.all_reconciled must be explicitly True."
        )

    expected_recounts = {
        "store_catchments": r1_c_rows,
        "store_catchment_memberships": r1_m_rows,
        "store_catchment_summary": r1_s_rows,
    }

    for entry_name, physical_recount in expected_recounts.items():
        entry = recon.get(entry_name)
        if not isinstance(entry, dict):
            raise SystemExit(
                f"PREREQUISITE CHECK FAILED FOR RUN 2: Run 1 reconciliation entry '{entry_name}' must be a dictionary."
            )
        if entry.get("reconciled") is not True:
            raise SystemExit(
                f"PREREQUISITE CHECK FAILED FOR RUN 2: Run 1 reconciliation entry '{entry_name}.reconciled' must be explicitly True."
            )
        persisted_val = entry.get("persisted_rows")
        if not isinstance(persisted_val, int) or persisted_val != physical_recount:
            raise SystemExit(
                f"PREREQUISITE CHECK FAILED FOR RUN 2: Run 1 reconciliation entry '{entry_name}.persisted_rows' ({persisted_val}) "
                f"does not match physical recount ({physical_recount})."
            )
        expected_val = entry.get("expected_rows")
        if not isinstance(expected_val, int) or expected_val != physical_recount:
            raise SystemExit(
                f"PREREQUISITE CHECK FAILED FOR RUN 2: Run 1 reconciliation entry '{entry_name}.expected_rows' ({expected_val}) "
                f"does not match physical recount ({physical_recount})."
            )

    logger.info(
        "Run 2 prerequisite verified: Comprehensive Run 1 evidence and physical outputs revalidated at %s",
        manifest_path,
    )
    return run1_manifest


# ==============================================================================
# Asynchronous Background Memory Monitor
# ==============================================================================

class ProcessMemoryMonitor:
    """Background sampling thread tracking Python RSS, Java RSS, and Host Available RAM."""

    def __init__(self, sample_interval_seconds: float = 0.1):
        self.interval = sample_interval_seconds
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self._started: bool = False

        self.python_pid = os.getpid()
        self.java_pid: Optional[int] = None
        self.java_pid_discovered: bool = False

        self.peak_python_rss_bytes = 0
        self.peak_java_rss_bytes = 0

        initial_available = query_host_available_memory_bytes()
        self.initial_host_available_bytes = initial_available
        self.min_host_available_bytes = initial_available

        self._samples_count = 0
        self._sampling_consecutive_errors = 0
        self._max_logged_errors = 0

    def attach_java_process(self, explicit_java_pid: Optional[int] = None) -> None:
        """Attach to child Java process using explicitly discovered PID or process tree search."""
        if explicit_java_pid:
            if not self.java_pid_discovered or self.java_pid != explicit_java_pid:
                self.java_pid = explicit_java_pid
                self.java_pid_discovered = True
                logger.info("Directly attached to Java process: PID=%d", explicit_java_pid)
            return

        if self.java_pid_discovered and self.java_pid is not None:
            return  # Already discovered, avoid duplicate assignment

        # 1. Attempt child process discovery via psutil if available
        if psutil is not None:
            try:
                py_proc = psutil.Process(self.python_pid)
                for child in py_proc.children(recursive=True):
                    if "java" in child.name().lower():
                        self.java_pid = child.pid
                        self.java_pid_discovered = True
                        logger.info("Discovered child Java process (psutil): PID=%d", child.pid)
                        return
            except Exception as exc:
                logger.debug("psutil child process search error: %s", exc)

        # 2. If no Java process discovered yet and OS is Windows, attempt native Win32 process-tree lookup
        if not self.java_pid_discovered and platform.system() == "Windows":
            try:
                children = WindowsMemoryApi.find_child_pids(self.python_pid)
                for child_pid, child_exe in children:
                    if "java" in child_exe.lower():
                        self.java_pid = child_pid
                        self.java_pid_discovered = True
                        logger.info("Discovered child Java process (Win32): PID=%d (%s)", child_pid, child_exe)
                        return
                    # Look one level deeper (cmd -> java)
                    subchildren = WindowsMemoryApi.find_child_pids(child_pid)
                    for sub_pid, sub_exe in subchildren:
                        if "java" in sub_exe.lower():
                            self.java_pid = sub_pid
                            self.java_pid_discovered = True
                            logger.info("Discovered grandchild Java process (Win32): PID=%d (%s)", sub_pid, sub_exe)
                            return
            except Exception as exc:
                logger.debug("Win32 child process search error: %s", exc)

    def start(self) -> None:
        """Start the background sampling thread."""
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()
        self._started = True

    def _monitor_loop(self) -> None:
        while not self.stop_event.is_set():
            iteration_error = False

            # 1. Sample Python memory
            try:
                if psutil is not None:
                    py_rss = psutil.Process(self.python_pid).memory_info().rss
                else:
                    py_mem = WindowsMemoryApi.get_process_memory(self.python_pid)
                    py_rss = py_mem["rss"] if py_mem else None

                if py_rss is not None:
                    if py_rss > self.peak_python_rss_bytes:
                        self.peak_python_rss_bytes = py_rss
                else:
                    iteration_error = True
            except Exception:
                iteration_error = True

            # 2. Sample Java memory
            if self.java_pid is not None:
                try:
                    if psutil is not None:
                        j_rss = psutil.Process(self.java_pid).memory_info().rss
                    else:
                        j_mem = WindowsMemoryApi.get_process_memory(self.java_pid)
                        j_rss = j_mem["rss"] if j_mem else None

                    if j_rss is not None:
                        if j_rss > self.peak_java_rss_bytes:
                            self.peak_java_rss_bytes = j_rss
                    else:
                        iteration_error = True
                except Exception:
                    iteration_error = True
            elif not self.java_pid_discovered:
                self.attach_java_process()
                if not self.java_pid_discovered:
                    iteration_error = True

            # 3. Sample Host available RAM
            try:
                avail = query_host_available_memory_bytes()
                if avail < self.min_host_available_bytes:
                    self.min_host_available_bytes = avail
            except Exception:
                iteration_error = True

            if iteration_error:
                self._sampling_consecutive_errors += 1
                if self._sampling_consecutive_errors >= 5 and self._max_logged_errors < 3:
                    logger.warning(
                        "Repeated sampling exceptions in memory monitor (consecutive errors: %d)",
                        self._sampling_consecutive_errors,
                    )
                    self._max_logged_errors += 1
            else:
                self._sampling_consecutive_errors = 0
                self._samples_count += 1

            time.sleep(self.interval)

    def stop(self) -> Dict[str, Any]:
        """Stop sampling and return telemetry summary.

        Signals the monitor loop to stop and waits up to 2.0 seconds for the thread to terminate.
        If the thread remains alive after the timeout, raises RuntimeError.
        Does not claim guaranteed thread termination without confirmation.
        """
        if not self._started:
            raise RuntimeError("Cannot stop memory monitor: monitor was not started.")

        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=2.0)
            if self.thread.is_alive():
                raise RuntimeError("Memory monitor thread failed to terminate within 2.0-second timeout.")

        # Query native peak working set for Python
        if platform.system() == "Windows":
            try:
                py_mem = WindowsMemoryApi.get_process_memory(self.python_pid)
                if py_mem and py_mem.get("peak_wset", 0) > self.peak_python_rss_bytes:
                    self.peak_python_rss_bytes = py_mem["peak_wset"]
            except Exception:
                pass

        # Query native peak working set for Java
        if self.java_pid and platform.system() == "Windows":
            try:
                j_mem = WindowsMemoryApi.get_process_memory(self.java_pid)
                if j_mem and j_mem.get("peak_wset", 0) > self.peak_java_rss_bytes:
                    self.peak_java_rss_bytes = j_mem["peak_wset"]
            except Exception:
                pass

        final_available = query_host_available_memory_bytes()

        # Format Python telemetry
        python_available = self.peak_python_rss_bytes > 0
        peak_py_mb = round(self.peak_python_rss_bytes / (1024 * 1024), 2) if python_available else None
        peak_py_bytes = self.peak_python_rss_bytes if python_available else None

        # Format Java telemetry: if discovery failed or no readings, record explicitly as unavailable
        java_available = self.java_pid_discovered and (self.peak_java_rss_bytes > 0)
        peak_java_mb = round(self.peak_java_rss_bytes / (1024 * 1024), 2) if java_available else None
        peak_java_bytes = self.peak_java_rss_bytes if java_available else None

        return {
            "sample_count": self._samples_count,
            "consecutive_sampling_errors": self._sampling_consecutive_errors,
            "java_process_detected": self.java_pid_discovered,
            "java_pid": self.java_pid,
            "java_telemetry_status": "AVAILABLE" if java_available else "UNAVAILABLE",
            "peak_python_rss_mb": peak_py_mb,
            "peak_python_rss_bytes": peak_py_bytes,
            "peak_java_rss_mb": peak_java_mb,
            "peak_java_rss_bytes": peak_java_bytes,
            "initial_host_available_mb": round(self.initial_host_available_bytes / (1024 * 1024), 2),
            "min_host_available_mb": round(self.min_host_available_bytes / (1024 * 1024), 2),
            "final_host_available_mb": round(final_available / (1024 * 1024), 2),
            "host_ram_drawdown_mb": round(
                (self.initial_host_available_bytes - self.min_host_available_bytes) / (1024 * 1024), 2
            ),
        }


# ==============================================================================
# Sedona Spark Session Creation & JVM Pre-Execution Verification
# ==============================================================================

def configure_spark_pre_launch_environment(extra_jars: Optional[List[str]] = None) -> None:
    """Configure environment variables prior to PySpark Java gateway launch.

    Configures:
    - Process-level JAVA_HOME strictly to Java 11 (jdk-11.0.33.10-hotspot)
    - Prepends Java 11 bin directory to process-level PATH (takes precedence)
    - PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON
    - PYSPARK_SUBMIT_ARGS to enforce --driver-memory 1536m, Sedona JARs,
      and Windows Hadoop filesystem shim configuration at JVM launch
    """
    java_11_home = Path(r"C:\Program Files\Eclipse Adoptium\jdk-11.0.33.10-hotspot")
    java_11_bin = java_11_home / "bin"
    java_11_exe = java_11_bin / "java.exe"

    if not java_11_home.is_dir() or not java_11_exe.is_file():
        raise FileNotFoundError(
            f"Required Java 11 installation not found: '{java_11_exe}' does not exist. "
            "The benchmark strictly requires Eclipse Adoptium JDK 11 and will not fall back to Java 17."
        )

    # Configure strictly process-level environment
    os.environ["JAVA_HOME"] = str(java_11_home)
    java_11_bin_str = str(java_11_bin)
    current_path = os.environ.get("PATH", "")
    path_parts = current_path.split(os.pathsep)
    cleaned_parts = [p for p in path_parts if p and p != java_11_bin_str]
    os.environ["PATH"] = f"{java_11_bin_str}{os.pathsep}{os.pathsep.join(cleaned_parts)}"
    logger.info("Configured process-level JAVA_HOME for Java 11: %s", java_11_home)

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Pre-launch JVM driver memory and configuration via PYSPARK_SUBMIT_ARGS
    # This guarantees that spark-submit launches java.exe with -Xmx1536m before
    # any user code or Python gateway objects are created.
    submit_parts = [
        f"--driver-memory {TARGET_DRIVER_MEMORY}",
        f"--conf spark.driver.memory={TARGET_DRIVER_MEMORY}",
        f"--conf spark.sql.shuffle.partitions={TARGET_SHUFFLE_PARTITIONS}",
        f"--conf spark.sql.session.timeZone={TARGET_TIMEZONE}",
        "--conf spark.sql.execution.arrow.pyspark.enabled=false",
    ]

    jars = list(extra_jars) if extra_jars is not None else find_sedona_jars()
    if not jars:
        raise RuntimeError(
            "Cannot configure Spark launch arguments: no verified Sedona JAR was provided or located."
        )

    # If running on Windows, verify and attach the pure-Java Windows Hadoop filesystem shim JAR
    if platform.system() == "Windows":
        if not WINDOWS_HADOOP_SHIM_JAR.is_file():
            raise FileNotFoundError(
                f"Required Windows Hadoop filesystem shim JAR not found at '{WINDOWS_HADOOP_SHIM_JAR}'. "
                "Compile tools/shims/WindowsLocalFileSystem.java before launching Spark."
            )
        shim_jar_str = str(WINDOWS_HADOOP_SHIM_JAR.resolve())
        if shim_jar_str not in jars:
            jars.append(shim_jar_str)
        submit_parts.append(f"--conf spark.hadoop.fs.file.impl={WINDOWS_HADOOP_FS_IMPL}")
        logger.info("Configured Windows Hadoop filesystem shim: %s", WINDOWS_HADOOP_FS_IMPL)

    # Convert JAR paths to file URIs for Hadoop/Spark --jars argument
    jar_uris = [Path(j).resolve().as_uri() for j in jars]
    jars_arg = ",".join(jar_uris)
    submit_parts.append(f"--jars {jars_arg}")

    submit_parts.append("pyspark-shell")
    os.environ["PYSPARK_SUBMIT_ARGS"] = " ".join(submit_parts)
    logger.info("Configured pre-launch PYSPARK_SUBMIT_ARGS: %s", os.environ["PYSPARK_SUBMIT_ARGS"])


def initialize_benchmark_sedona_session(app_name: str = "GeoPulse-SpatialAnalysis-100k-Benchmark"):
    """Instantiate external Sedona SparkSession configuring 1536m heap and 4 shuffle partitions.

    Guarantees environment is configured before Java gateway startup.
    Strictly asserts the verified Sedona JAR is located before session creation.
    """
    jars = find_sedona_jars()
    if not jars:
        raise RuntimeError(
            "Cannot initialize benchmark Sedona session: verified Sedona JAR was not located."
        )

    all_jars = list(jars)
    if platform.system() == "Windows":
        if not WINDOWS_HADOOP_SHIM_JAR.is_file():
            raise FileNotFoundError(
                f"Required Windows Hadoop filesystem shim JAR not found at '{WINDOWS_HADOOP_SHIM_JAR}'. "
                "Compile tools/shims/WindowsLocalFileSystem.java before launching Spark."
            )
        all_jars.append(str(WINDOWS_HADOOP_SHIM_JAR.resolve()))

    configure_spark_pre_launch_environment(extra_jars=all_jars)

    from sedona.spark import SedonaContext

    # Native Windows paths joined with os.pathsep for driver and executor extraClassPath
    classpath = os.pathsep.join(str(Path(j).resolve()) for j in all_jars)
    if not classpath:
        raise RuntimeError(
            "Cannot initialize benchmark Sedona session: classpath is empty."
        )

    builder = (
        SedonaContext.builder()
        .master(TARGET_MASTER)
        .appName(app_name)
        .config("spark.driver.memory", TARGET_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", str(TARGET_SHUFFLE_PARTITIONS))
        .config("spark.sql.session.timeZone", TARGET_TIMEZONE)
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.driver.extraClassPath", classpath)
        .config("spark.executor.extraClassPath", classpath)
    )

    if platform.system() == "Windows":
        builder = builder.config("spark.hadoop.fs.file.impl", WINDOWS_HADOOP_FS_IMPL)

    spark = builder.getOrCreate()
    session = SedonaContext.create(spark)
    logger.info("SedonaContext created successfully with Spark version %s", session.version)
    return session


def verify_jvm_runtime_arguments(session) -> Dict[str, Any]:
    """Inspect and verify live JVM startup arguments and maximum heap memory ceiling.

    Raises RuntimeError if the JVM did not boot with -Xmx1536m or configuration fails assertions.
    """
    jvm = session.sparkContext._jvm
    if jvm is None:
        raise RuntimeError("PySpark JVM reference is None; cannot inspect runtime parameters.")

    # 1. Query command line arguments passed to java.exe
    runtime_mx = jvm.java.lang.management.ManagementFactory.getRuntimeMXBean()
    jvm_input_args = list(runtime_mx.getInputArguments())

    # Check for -Xmx1536m
    has_target_xmx = any("-Xmx1536m" in arg for arg in jvm_input_args)
    if not has_target_xmx:
        raise RuntimeError(
            f"JVM STARTUP VERIFICATION FAILED: '-Xmx1536m' was not found in JVM arguments. "
            f"Actual arguments: {jvm_input_args}"
        )

    # 2. Query maximum available heap from java.lang.Runtime
    max_memory_bytes = jvm.java.lang.Runtime.getRuntime().maxMemory()
    max_memory_mb = max_memory_bytes / (1024 * 1024)

    # For -Xmx1536m, Runtime.maxMemory() reports ~1380 to 1536 MB (less GC survivor spaces)
    if not (1300.0 <= max_memory_mb <= 1550.0):
        raise RuntimeError(
            f"JVM RUNTIME MAX HEAP VERIFICATION FAILED: Runtime.getRuntime().maxMemory() reported "
            f"{max_memory_mb:.2f} MB, which is outside expected range [1300 MB, 1550 MB] for -Xmx1536m."
        )

    # 3. Query Spark configuration properties
    driver_mem_conf = session.conf.get("spark.driver.memory", None)
    shuffle_partitions_conf = session.conf.get("spark.sql.shuffle.partitions", None)

    if driver_mem_conf != TARGET_DRIVER_MEMORY:
        raise RuntimeError(
            f"Spark conf 'spark.driver.memory' is '{driver_mem_conf}', expected '{TARGET_DRIVER_MEMORY}'."
        )
    if str(shuffle_partitions_conf) != str(TARGET_SHUFFLE_PARTITIONS):
        raise RuntimeError(
            f"Spark conf 'spark.sql.shuffle.partitions' is '{shuffle_partitions_conf}', "
            f"expected '{TARGET_SHUFFLE_PARTITIONS}'."
        )

    # 4. Verify Windows Hadoop filesystem shim configuration
    hadoop_fs_file_impl = None
    if platform.system() == "Windows":
        hadoop_fs_file_impl = session.sparkContext._jsc.hadoopConfiguration().get("fs.file.impl")
        logger.info("Verified Hadoop fs.file.impl configuration: %s", hadoop_fs_file_impl)
        if hadoop_fs_file_impl != WINDOWS_HADOOP_FS_IMPL:
            raise RuntimeError(
                f"Hadoop fs.file.impl is '{hadoop_fs_file_impl}', expected '{WINDOWS_HADOOP_FS_IMPL}'."
            )

    # 5. Discover JVM PID via Java 9+ ProcessHandle API
    java_pid = None
    try:
        java_pid = int(jvm.java.lang.ProcessHandle.current().pid())
        logger.info("Discovered Java PID via ProcessHandle: %d", java_pid)
    except Exception as exc:
        logger.debug("Java ProcessHandle.current().pid() query error: %s", exc)

    logger.info(
        "JVM Runtime Verified: -Xmx1536m confirmed, Runtime.maxMemory=%.2f MB (%d bytes), Java PID=%s",
        max_memory_mb,
        max_memory_bytes,
        java_pid,
    )

    result = {
        "verified": True,
        "jvm_arguments": jvm_input_args,
        "max_memory_bytes": max_memory_bytes,
        "max_memory_mb": round(max_memory_mb, 2),
        "spark_driver_memory_conf": driver_mem_conf,
        "spark_shuffle_partitions_conf": int(shuffle_partitions_conf),
        "discovered_java_pid": java_pid,
    }
    if hadoop_fs_file_impl is not None:
        result["hadoop_fs_file_impl"] = hadoop_fs_file_impl
    return result


# ==============================================================================
# Output Artifact Verification & Reconciliation
# ==============================================================================

def count_csv_records_in_directory(dir_path: Path) -> Tuple[int, int]:
    """Count data rows and part-files across all part-*.csv files in an output directory using streaming reads.

    Returns:
        (total_data_rows, valid_part_files_count)
    """
    if not dir_path.is_dir():
        return 0, 0

    total_rows = 0
    part_files = sorted(dir_path.glob("*.csv"))
    valid_parts = 0

    for pf in part_files:
        with open(pf, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
            except StopIteration:
                continue
            if not header or not any(field.strip() for field in header):
                continue
            valid_parts += 1
            for _ in reader:
                total_rows += 1

    return total_rows, valid_parts


def reconcile_outputs_against_pipeline_results(
    pipeline_results: Dict[str, Any],
    catchments_dir: Path,
    memberships_dir: Path,
    summary_dir: Path,
) -> Dict[str, Any]:
    """Reconcile persisted CSV part-files against pipeline returned metrics.

    Strictly asserts that:
    1. Each output directory exists and contains at least one valid CSV part file with header.
    2. Persisted row counts match pipeline metrics exactly without fallbacks.
    """
    c_rows, c_parts = count_csv_records_in_directory(catchments_dir)
    m_rows, m_parts = count_csv_records_in_directory(memberships_dir)
    s_rows, s_parts = count_csv_records_in_directory(summary_dir)

    # Strictly extract from pipeline_results without default fallbacks
    if "input_stores_count" not in pipeline_results:
        raise KeyError("Missing 'input_stores_count' in pipeline_results")
    if "total_memberships" not in pipeline_results:
        raise KeyError("Missing 'total_memberships' in pipeline_results")
    if "per_store_summary" not in pipeline_results:
        raise KeyError("Missing 'per_store_summary' in pipeline_results")

    expected_catchments = pipeline_results["input_stores_count"]
    expected_memberships = pipeline_results["total_memberships"]
    per_store = pipeline_results["per_store_summary"]
    expected_summary = len(per_store)

    if not isinstance(expected_catchments, int) or expected_catchments != EXPECTED_STORE_COUNT:
        raise ValueError(f"Invalid expected catchments: {expected_catchments}")
    if not isinstance(expected_memberships, int) or expected_memberships <= 0:
        raise ValueError(f"Invalid expected memberships: {expected_memberships}")
    if not isinstance(per_store, list) or expected_summary != EXPECTED_STORE_COUNT:
        raise ValueError(f"Invalid per_store_summary count: {expected_summary}")

    c_match = (c_rows == expected_catchments) and (c_parts > 0)
    m_match = (m_rows == expected_memberships) and (m_parts > 0)
    s_match = (s_rows == expected_summary) and (s_parts > 0)

    all_reconciled = c_match and m_match and s_match

    reconciliation = {
        "all_reconciled": all_reconciled,
        "store_catchments": {
            "path": str(catchments_dir.resolve()),
            "persisted_rows": c_rows,
            "expected_rows": expected_catchments,
            "part_files_count": c_parts,
            "reconciled": c_match,
        },
        "store_catchment_memberships": {
            "path": str(memberships_dir.resolve()),
            "persisted_rows": m_rows,
            "expected_rows": expected_memberships,
            "part_files_count": m_parts,
            "reconciled": m_match,
        },
        "store_catchment_summary": {
            "path": str(summary_dir.resolve()),
            "persisted_rows": s_rows,
            "expected_rows": expected_summary,
            "part_files_count": s_parts,
            "reconciled": s_match,
        },
    }

    if not all_reconciled:
        raise ValueError(
            "Output artifact reconciliation failed! Persisted CSV counts do not match pipeline metrics:\n"
            f"  - Catchments : persisted={c_rows}, expected={expected_catchments}, part_files={c_parts}, match={c_match}\n"
            f"  - Memberships: persisted={m_rows}, expected={expected_memberships}, part_files={m_parts}, match={m_match}\n"
            f"  - Summary    : persisted={s_rows}, expected={expected_summary}, part_files={s_parts}, match={s_match}"
        )

    return reconciliation


def write_atomic_json(target_path: Path, data: Dict[str, Any]) -> None:
    """Atomically write JSON data to target_path using a temporary sibling file."""
    temp_path = target_path.with_suffix(".tmp")
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(temp_path, target_path)


# ==============================================================================
# Main Benchmark Execution Flow
# ==============================================================================

def run_100k_benchmark(run_id: str = "run1") -> Dict[str, Any]:
    """Execute complete GeoPulse 100K spatial benchmark workflow.

    Workflow:
    1. Validate run_id strictly ('run1' or 'run2').
    2. Initial Preflight RAM check (>= 4 GiB available). Pre-run check aborts without directory or manifest.
    3. Input existence and cryptographic SHA-256 verification (mobility + store reference). Pre-run check.
    4. Run 2 prerequisite check (if run_id == 'run2'). Pre-run check.
    5. Destination isolation and pre-checking of all destination output paths before Spark starts. Pre-run check.
    6. Run directory creation and secondary host RAM safety check immediately before JVM launch.
    7. Sedona session instantiation with 1536m driver heap and pre-launch PYSPARK_SUBMIT_ARGS.
    8. JVM runtime maxMemory verification prior to reading dataset.
    9. Background memory monitoring with reliable Java PID discovery (psutil + Win32 fallback).
    10. Pipeline invocation via `run_spatial_analysis(config, sedona_session=session)`.
    11. Strict result validation and artifact reconciliation with explicit RUNNING/SUCCESS/FAILED/SKIPPED lifecycle tracking.
    12. Explicit handling of SystemExit and Exception to persist failure manifests for post-creation failures.
    13. Overall composite success strictly requires all 5 lifecycle conditions AND successful manifest persistence on disk.
    14. Independent session shutdown and monitor cleanup via separate try...finally blocks with explicit thread join timeout enforcement.
    """
    # 1. Strict run_id validation
    if run_id not in ("run1", "run2"):
        raise ValueError(f"Invalid run_id '{run_id}'. Only 'run1' and 'run2' are permitted.")

    logger.info("================================================================================")
    logger.info(" GeoPulse 100K Spatial Analytics Benchmark — Target: %s", run_id.upper())
    logger.info("================================================================================")

    # 2. Initial Host Memory Preflight (Mandatory >= 4 GiB)
    check_host_memory_safety(MIN_AVAILABLE_RAM_BYTES)

    # 3. Input Integrity Verification
    input_meta = validate_input_artifacts(DEFAULT_MOBILITY_PATH, DEFAULT_STORE_PATH)

    # 4. Run 2 Prerequisite Check (verifies Run 1 manifest, inputs, physical outputs, and explicit Booleans)
    validate_run2_prerequisites(run_id, BASE_PROCESSED_DIR, input_meta)

    # 5. Target Directory Setup with Strict Collision Protection BEFORE Spark Starts
    target_run_dir = BASE_PROCESSED_DIR / f"benchmark_100k_{run_id}"
    target_catchments_dir = target_run_dir / "store_catchments"
    target_memberships_dir = target_run_dir / "store_catchment_memberships"
    target_summary_dir = target_run_dir / "store_catchment_summary"

    if target_run_dir.exists():
        raise FileExistsError(
            f"Destination run directory already exists: '{target_run_dir}'. "
            "To prevent accidental data loss or mixing runs, overwrite=False is strictly enforced. "
            "Please remove or rename the existing directory to proceed."
        )

    for dest_dir in (target_catchments_dir, target_memberships_dir, target_summary_dir):
        if dest_dir.exists():
            raise FileExistsError(
                f"Destination dataset directory already exists: '{dest_dir}'. "
                "Execution aborted before Spark initialization."
            )

    # Safely create run directory before starting processing
    target_run_dir.mkdir(parents=True, exist_ok=False)
    run_dir_created = True

    session = None
    monitor: Optional[ProcessMemoryMonitor] = None

    benchmark_start_time = time.perf_counter()
    pipeline_exec_duration = 0.0

    # Independent Lifecycle Statuses
    execution_status = "NOT_STARTED"
    validation_status = "NOT_STARTED"
    reconciliation_status = "NOT_STARTED"
    session_shutdown_status = "NOT_STARTED"
    memory_monitor_status = "NOT_STARTED"
    manifest_persisted = False

    pipeline_results: Any = None
    error_details: Optional[Dict[str, Any]] = None
    jvm_meta: Dict[str, Any] = {}
    reconciliation_meta: Dict[str, Any] = {}
    manifest: Dict[str, Any] = {}

    try:
        # Protected monitor construction inside try block
        monitor = ProcessMemoryMonitor(sample_interval_seconds=0.1)

        # REPEAT Host Memory Safety Check immediately before JVM initialization
        logger.info("Performing secondary host RAM safety check immediately before JVM launch...")
        check_host_memory_safety(MIN_AVAILABLE_RAM_BYTES)

        # 6. Initialize Sedona Spark Session with pre-launch environment
        session = initialize_benchmark_sedona_session(
            app_name=f"GeoPulse-SpatialAnalysis-100k-Benchmark-{run_id}"
        )

        # 7. Verify JVM Runtime Arguments & Heap Ceiling
        jvm_meta = verify_jvm_runtime_arguments(session)

        # 8. Start Asynchronous Memory Monitor with Discovered Java PID
        monitor.attach_java_process(jvm_meta.get("discovered_java_pid"))
        monitor.start()

        # 9. Configure Spatial Analysis Pipeline
        config = SpatialAnalysisConfig(
            mobility_input_path=str(DEFAULT_MOBILITY_PATH.resolve()),
            store_input_path=str(DEFAULT_STORE_PATH.resolve()),
            catchments_output_path=str(target_catchments_dir.resolve()),
            memberships_output_path=str(target_memberships_dir.resolve()),
            summary_output_path=str(target_summary_dir.resolve()),
            overwrite=False,
        )

        logger.info("Executing spatial analysis pipeline against 100K dataset ...")
        pipeline_exec_start = time.perf_counter()
        execution_status = "RUNNING"

        # Execute pipeline using externally injected session
        pipeline_results = run_spatial_analysis(config=config, sedona_session=session)
        pipeline_exec_duration = time.perf_counter() - pipeline_exec_start
        execution_status = "SUCCESS"

        # 10. Strict Pipeline Return Contract Validation
        validation_status = "RUNNING"
        if not isinstance(pipeline_results, dict):
            validation_status = "FAILED"
            raise TypeError(f"Pipeline returned unexpected type: {type(pipeline_results).__name__}")

        raw_status = pipeline_results.get("status")
        if raw_status != "SUCCESS":
            validation_status = "FAILED"
            raise ValueError(f"Pipeline reported non-success status: {raw_status!r}")

        actual_input_pings = pipeline_results.get("input_pings_count")
        if actual_input_pings != EXPECTED_RECORD_COUNT:
            validation_status = "FAILED"
            raise ValueError(
                f"Input pings count mismatch: expected {EXPECTED_RECORD_COUNT}, got {actual_input_pings}"
            )

        actual_input_stores = pipeline_results.get("input_stores_count")
        if actual_input_stores != EXPECTED_STORE_COUNT:
            validation_status = "FAILED"
            raise ValueError(
                f"Input stores count mismatch: expected {EXPECTED_STORE_COUNT}, got {actual_input_stores}"
            )

        total_memberships = pipeline_results.get("total_memberships")
        if not isinstance(total_memberships, int) or total_memberships <= 0:
            validation_status = "FAILED"
            raise ValueError(f"Pipeline reported non-positive total memberships: {total_memberships}")

        overlap_dist = pipeline_results.get("overlap_distribution")
        if not isinstance(overlap_dist, dict) or not overlap_dist.get("reconciled", False):
            validation_status = "FAILED"
            raise ValueError("Pipeline overlap distribution reconciliation reported False or invalid.")

        per_store_summary = pipeline_results.get("per_store_summary")
        if not isinstance(per_store_summary, list) or len(per_store_summary) != EXPECTED_STORE_COUNT:
            validation_status = "FAILED"
            raise ValueError(
                f"Pipeline per_store_summary expected {EXPECTED_STORE_COUNT} records, got {len(per_store_summary) if isinstance(per_store_summary, list) else type(per_store_summary).__name__}"
            )

        validation_status = "SUCCESS"

        # 11. Reconcile Persisted Output Files Against Pipeline Metrics
        reconciliation_status = "RUNNING"
        reconciliation_meta = reconcile_outputs_against_pipeline_results(
            pipeline_results=pipeline_results,
            catchments_dir=target_catchments_dir,
            memberships_dir=target_memberships_dir,
            summary_dir=target_summary_dir,
        )
        reconciliation_status = "SUCCESS"
        logger.info(
            "Pipeline execution and artifact reconciliation completed successfully in %.2f seconds.",
            pipeline_exec_duration,
        )

    except SystemExit as exc:
        if execution_status == "RUNNING" or execution_status != "SUCCESS":
            execution_status = "FAILED"
        if validation_status == "RUNNING":
            validation_status = "FAILED"
        elif validation_status == "NOT_STARTED":
            validation_status = "SKIPPED"
        if reconciliation_status == "RUNNING":
            reconciliation_status = "FAILED"
        elif reconciliation_status == "NOT_STARTED":
            reconciliation_status = "SKIPPED"

        error_details = {
            "exception_type": type(exc).__name__,
            "exception_message": str(exc),
            "traceback": traceback.format_exc(),
        }
        logger.error(
            "Benchmark execution halted by SystemExit: %s\n%s",
            exc,
            traceback.format_exc(),
        )
        raise
    except Exception as exc:
        if execution_status == "RUNNING" or execution_status != "SUCCESS":
            execution_status = "FAILED"
        if validation_status == "RUNNING":
            validation_status = "FAILED"
        elif validation_status == "NOT_STARTED":
            validation_status = "SKIPPED"
        if reconciliation_status == "RUNNING":
            reconciliation_status = "FAILED"
        elif reconciliation_status == "NOT_STARTED":
            reconciliation_status = "SKIPPED"

        error_details = {
            "exception_type": type(exc).__name__,
            "exception_message": str(exc),
            "traceback": traceback.format_exc(),
        }
        logger.error(
            "Benchmark execution encountered an exception (%s): %s\n%s",
            type(exc).__name__,
            exc,
            traceback.format_exc(),
        )
        raise
    finally:
        # Stop monitor independently; failure in monitor must not prevent session shutdown
        memory_telemetry: Dict[str, Any] = {}
        if monitor is not None and monitor._started:
            try:
                memory_telemetry = monitor.stop()
                if monitor.thread is not None and monitor.thread.is_alive():
                    memory_monitor_status = "FAILED"
                    logger.error("Memory monitor thread remains alive after stop() timeout.")
                else:
                    memory_monitor_status = "CLEAN"
            except Exception as mon_exc:
                memory_monitor_status = "FAILED"
                logger.warning("Error stopping memory monitor: %s", mon_exc)
                thread_alive = monitor.thread.is_alive() if monitor.thread is not None else False
                memory_telemetry = {
                    "monitor_stopped_with_error": str(mon_exc),
                    "thread_alive_after_timeout": thread_alive,
                    "sample_count": monitor._samples_count,
                    "java_telemetry_status": "UNAVAILABLE",
                    "peak_python_rss_mb": round(monitor.peak_python_rss_bytes / (1024 * 1024), 2) if monitor.peak_python_rss_bytes > 0 else None,
                    "peak_java_rss_mb": round(monitor.peak_java_rss_bytes / (1024 * 1024), 2) if monitor.peak_java_rss_bytes > 0 else None,
                }
        else:
            memory_monitor_status = "NOT_STARTED"
            memory_telemetry = {
                "java_telemetry_status": "NOT_STARTED",
                "peak_python_rss_mb": None,
                "peak_java_rss_mb": None,
            }

        # Stop SparkSession independently; preserve original exception if stop fails
        if session is not None:
            try:
                session.stop()
                session_shutdown_status = "CLEAN"
                logger.info("Sedona SparkSession stopped cleanly.")
            except Exception as stop_exc:
                session_shutdown_status = "FAILED"
                logger.warning("Error stopping SparkSession: %s", stop_exc)
        else:
            session_shutdown_status = "NOT_STARTED"

        total_wall_clock_duration = time.perf_counter() - benchmark_start_time

        # Composite lifecycle success check
        lifecycle_success = (
            execution_status == "SUCCESS"
            and validation_status == "SUCCESS"
            and reconciliation_status == "SUCCESS"
            and session_shutdown_status == "CLEAN"
            and memory_monitor_status == "CLEAN"
        )
        provisional_status = "SUCCESS" if lifecycle_success else "FAILED"

        # Persist Manifest (Only attempted once run directory has been created)
        manifest_persisted = False
        if run_dir_created:
            manifest_path = target_run_dir / "benchmark_manifest.json"
            try:
                metrics_payload = (
                    pipeline_results
                    if isinstance(pipeline_results, dict)
                    else {
                        "input_pings_count": 0,
                        "input_stores_count": 0,
                        "total_memberships": 0,
                        "overall_distinct_matched_pings": 0,
                        "overall_distinct_matched_devices": 0,
                        "overlap_distribution": {},
                        "per_store_summary": [],
                    }
                )

                manifest = {
                    "benchmark_id": f"geopulse_100k_spatial_benchmark_{run_id}",
                    "run_id": run_id,
                    "status": provisional_status,
                    "execution_status": execution_status,
                    "validation_status": validation_status,
                    "reconciliation_status": reconciliation_status,
                    "session_shutdown_status": session_shutdown_status,
                    "memory_monitor_status": memory_monitor_status,
                    "manifest_persisted": True,
                    "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "environment": {
                        "os": platform.system(),
                        "os_release": platform.release(),
                        "architecture": platform.machine(),
                        "python_executable": sys.executable,
                        "python_version": sys.version,
                        "cpu_logical_cores": os.cpu_count(),
                    },
                    "spark_configuration": {
                        "master": TARGET_MASTER,
                        "driver_memory": TARGET_DRIVER_MEMORY,
                        "shuffle_partitions": TARGET_SHUFFLE_PARTITIONS,
                        "timezone": TARGET_TIMEZONE,
                        "arrow_enabled": False,
                    },
                    "jvm_verification": jvm_meta,
                    "inputs": input_meta,
                    "timing_seconds": {
                        "total_wall_clock": round(total_wall_clock_duration, 3),
                        "pipeline_execution": round(pipeline_exec_duration, 3),
                    },
                    "memory_telemetry": memory_telemetry,
                    "pipeline_metrics": {
                        "input_pings_count": metrics_payload.get("input_pings_count", 0),
                        "input_stores_count": metrics_payload.get("input_stores_count", 0),
                        "total_memberships": metrics_payload.get("total_memberships", 0),
                        "overall_distinct_matched_pings": metrics_payload.get("overall_distinct_matched_pings", 0),
                        "overall_distinct_matched_devices": metrics_payload.get("overall_distinct_matched_devices", 0),
                        "overlap_distribution": metrics_payload.get("overlap_distribution", {}),
                        "per_store_summary": metrics_payload.get("per_store_summary", []),
                    },
                    "reconciliation": reconciliation_meta,
                    "disclaimer": (
                        "This benchmark evaluates execution performance, memory footprint, and reproducibility "
                        "for a 100,000-observation workload on a local developer workstation. It does NOT prove "
                        "or claim linear scalability to multi-million or billion-observation distributed workloads."
                    ),
                }

                if error_details:
                    manifest["error"] = error_details

                write_atomic_json(manifest_path, manifest)
                manifest_persisted = True
                logger.info("Benchmark manifest saved (%s): %s", provisional_status, manifest_path)
            except Exception as manifest_exc:
                manifest_persisted = False
                logger.error(
                    "Failed to write benchmark manifest to '%s': %s\n%s",
                    manifest_path,
                    manifest_exc,
                    traceback.format_exc(),
                )
                if manifest:
                    manifest["status"] = "FAILED"
                    manifest["manifest_persisted"] = False

        overall_status = "SUCCESS" if (lifecycle_success and manifest_persisted) else "FAILED"

    logger.info("================================================================================")
    logger.info(
        " Benchmark Complete (%s): Overall=%s, Execution=%s, Validation=%s, Reconciliation=%s, SessionShutdown=%s, Monitor=%s, ManifestPersisted=%s, Elapsed=%.2fs",
        run_id.upper(),
        overall_status,
        execution_status,
        validation_status,
        reconciliation_status,
        session_shutdown_status,
        memory_monitor_status,
        manifest_persisted,
        total_wall_clock_duration,
    )
    logger.info("================================================================================")

    return manifest


# ==============================================================================
# CLI Entrypoint
# ==============================================================================

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command line arguments for the benchmark harness.

    Note: The 4 GiB memory threshold cannot be bypassed via any argument.
    """
    parser = argparse.ArgumentParser(
        description="GeoPulse Hyper-Local Retail Mobility 100K Spatial Analytics Benchmark Harness"
    )
    parser.add_argument(
        "--run",
        choices=["run1", "run2", "1", "2"],
        default="run1",
        help="Benchmark execution slice ('run1' or 'run2'). Default is 'run1'. Run 2 requires completed Run 1.",
    )
    args = parser.parse_args(argv)

    # Normalize run identifier
    if args.run in ("1", "run1"):
        args.run = "run1"
    elif args.run in ("2", "run2"):
        args.run = "run2"

    return args


def main():
    """CLI entrypoint."""
    args = parse_args()
    try:
        manifest = run_100k_benchmark(run_id=args.run)
        if manifest.get("status") != "SUCCESS":
            sys.exit(1)
    except SystemExit as exc:
        logger.error("Preflight check halted benchmark: %s", exc)
        sys.exit(1)
    except Exception as exc:
        logger.error("Benchmark failed with error: %s", exc)
        sys.exit(2)


if __name__ == "__main__":
    main()
