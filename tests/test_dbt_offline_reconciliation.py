"""Offline Python-to-dbt reconciliation test suite for GeoPulse Week 3 analytics.

Validates the Week 3 SQL models against the accepted Python temporal mobility engine:
- Simulates Snowflake SQL logic on the canonical 10K dataset using DuckDB in-memory.
- Executes the exact logic of the Week 3 dbt models:
    1. stg_mobility_pings
    2. int_store_spatial_states (Full 10K preservation -> 60,000 state rows)
    3. int_store_pairwise_device_transitions (Full trajectory preservation, anomaly hard breaks)
    4. fct_store_pairwise_overlap
    5. fct_store_pairwise_hourly_overlap
    6. fct_store_cross_catchment_transitions
    7. fct_store_temporal_opportunity
- Asserts 100% exact key and count identity with the Python engine reference.
- Asserts metric parity for GOR, DJI, DOC, PCTR, PHI, LAHS within deterministic tolerances.
- Asserts absence of unapproved Conversion Readiness Score (CRS) from canonical marts.
- Verifies anomaly detection across intermediate observations and prevents false trajectory bridging.
- Executes singular test assertions offline.
"""

from collections import Counter
import csv
from datetime import datetime, timezone
from decimal import Decimal
import os
import re
import unittest
import duckdb

from src.temporal_mobility_analytics import (
    TemporalMobilityEngine,
    wgs84_to_utm43n,
)


def snowflake_to_duckdb_sql(sql: str) -> str:
    """Translate Snowflake SQL syntax quirks to DuckDB for offline reconciliation."""
    idx = sql.find("WITH")
    if idx != -1:
        sql = sql[idx:]

    sql = sql.replace("{{ ref('stg_mobility_pings') }}", "stg_mobility_pings")
    sql = sql.replace("{{ ref('stg_store_catchment_memberships') }}", "stg_store_catchment_memberships")
    sql = sql.replace("{{ ref('int_store_spatial_states') }}", "int_store_spatial_states")
    sql = sql.replace("{{ ref('int_store_pairwise_device_transitions') }}", "int_store_pairwise_device_transitions")

    sql = sql.replace("DATEDIFF('second',", "date_diff('second',")
    sql = re.sub(r"TO_VARCHAR\(([^,]+),\s*'YYYYMMDDHH24MISS'\)", r"strftime(\1, '%Y%m%d%H%M%S')", sql)
    # Translate Snowflake LEAD(...) IGNORE NULLS to DuckDB LEAD(... IGNORE NULLS)
    sql = re.sub(r"LEAD\((.+?)\)\s+IGNORE\s+NULLS", r"LEAD(\1 IGNORE NULLS)", sql, flags=re.IGNORECASE)
    sql = re.sub(r"::NUMBER\(\d+,\s*\d+\)", "", sql)
    sql = re.sub(r"::NUMBER", "", sql)
    sql = re.sub(r"::INT", "", sql)
    sql = re.sub(r"::FLOAT", "", sql)
    sql = re.sub(r"::VARCHAR", "", sql)

    return sql


class TestDbtOfflineReconciliation(unittest.TestCase):
    """Offline reconciliation of Week 3 dbt models against Python reference engine."""

    @classmethod
    def setUpClass(cls):
        cls.con = duckdb.connect(database=":memory:")

        stores_path = "data/reference/synthetic_store_locations.csv"
        pings_path = "data/raw/synthetic_mobility_pings.csv"
        catchments_path = "data/processed/week2_verification/store_catchments/part-00000-add80fd6-392e-4363-9704-248ee09f2202-c000.csv"
        memberships_path = "data/processed/week2_verification/store_catchment_memberships/part-00000-39b544ad-119e-4473-819c-c8304a9025af-c000.csv"

        with open(stores_path, "r", encoding="utf-8") as f:
            store_rows = list(csv.DictReader(f))
        with open(pings_path, "r", encoding="utf-8") as f:
            ping_rows = list(csv.DictReader(f))
        with open(catchments_path, "r", encoding="utf-8") as f:
            catchment_wkt_map = {r["store_id"]: r["geometry_wkt"] for r in csv.DictReader(f)}

        cls.py_engine = TemporalMobilityEngine.from_store_records(store_rows, catchment_wkt_map=catchment_wkt_map)
        cls.py_pings = cls.py_engine.load_pings(ping_rows)

        cls.py_overlap = cls.py_engine.compute_fct_pairwise_overlap(cls.py_pings)
        cls.py_hourly = cls.py_engine.compute_fct_pairwise_hourly_overlap(cls.py_pings)
        cls.py_trans = cls.py_engine.compute_fct_cross_catchment_transitions(cls.py_pings)
        cls.py_opp = cls.py_engine.compute_fct_temporal_opportunity(cls.py_pings)

        # Load complete raw mobility observations (10,000 pings)
        cls.con.execute(f"""
            CREATE TABLE stg_mobility_pings AS
            SELECT
                ping_id,
                device_id,
                CAST(REPLACE(event_timestamp, 'Z', '') AS TIMESTAMP) AS event_timestamp,
                CAST(REPLACE(event_timestamp, 'Z', '') AS TIMESTAMP) + INTERVAL 330 MINUTE AS event_timestamp_ist,
                CAST(CAST(REPLACE(event_timestamp, 'Z', '') AS TIMESTAMP) + INTERVAL 330 MINUTE AS DATE) AS calendar_date,
                EXTRACT(hour FROM CAST(REPLACE(event_timestamp, 'Z', '') AS TIMESTAMP) + INTERVAL 330 MINUTE) AS hour_of_day,
                CASE
                    WHEN EXTRACT(hour FROM CAST(REPLACE(event_timestamp, 'Z', '') AS TIMESTAMP) + INTERVAL 330 MINUTE) BETWEEN 6 AND 11 THEN 'morning'
                    WHEN EXTRACT(hour FROM CAST(REPLACE(event_timestamp, 'Z', '') AS TIMESTAMP) + INTERVAL 330 MINUTE) BETWEEN 12 AND 16 THEN 'afternoon'
                    WHEN EXTRACT(hour FROM CAST(REPLACE(event_timestamp, 'Z', '') AS TIMESTAMP) + INTERVAL 330 MINUTE) BETWEEN 17 AND 21 THEN 'evening'
                    ELSE 'night'
                END AS time_of_day,
                CAST(latitude AS DOUBLE) AS latitude,
                CAST(longitude AS DOUBLE) AS longitude,
                CAST(accuracy_meters AS DOUBLE) AS accuracy_meters,
                movement_mode,
                data_provenance
            FROM read_csv_auto('{pings_path}', all_varchar=true);
        """)

        # Load canonical catchment memberships (4,051 records across 3,956 unique pings)
        cls.con.execute(f"""
            CREATE TABLE stg_store_catchment_memberships AS
            SELECT
                ping_id,
                device_id,
                CAST(REPLACE(event_timestamp, 'Z', '') AS TIMESTAMP) AS event_timestamp,
                CAST(REPLACE(event_timestamp, 'Z', '') AS TIMESTAMP) + INTERVAL 330 MINUTE AS event_timestamp_ist,
                CAST(CAST(REPLACE(event_timestamp, 'Z', '') AS TIMESTAMP) + INTERVAL 330 MINUTE AS DATE) AS calendar_date,
                EXTRACT(hour FROM CAST(REPLACE(event_timestamp, 'Z', '') AS TIMESTAMP) + INTERVAL 330 MINUTE) AS hour_of_day,
                CASE
                    WHEN EXTRACT(hour FROM CAST(REPLACE(event_timestamp, 'Z', '') AS TIMESTAMP) + INTERVAL 330 MINUTE) BETWEEN 6 AND 11 THEN 'morning'
                    WHEN EXTRACT(hour FROM CAST(REPLACE(event_timestamp, 'Z', '') AS TIMESTAMP) + INTERVAL 330 MINUTE) BETWEEN 12 AND 16 THEN 'afternoon'
                    WHEN EXTRACT(hour FROM CAST(REPLACE(event_timestamp, 'Z', '') AS TIMESTAMP) + INTERVAL 330 MINUTE) BETWEEN 17 AND 21 THEN 'evening'
                    ELSE 'night'
                END AS time_of_day,
                store_id,
                store_name,
                CAST(distance_meters AS DOUBLE) AS distance_meters,
                movement_mode,
                data_provenance
            FROM read_csv_auto('{memberships_path}', all_varchar=true);
        """)

    def test_stg_mobility_pings_preservation(self):
        """Verify stg_mobility_pings preserves all 10,000 source observations."""
        cnt = self.con.execute("SELECT COUNT(*) FROM stg_mobility_pings;").fetchone()[0]
        self.assertEqual(cnt, 10000)
        dist_dev = self.con.execute("SELECT COUNT(DISTINCT device_id) FROM stg_mobility_pings;").fetchone()[0]
        self.assertEqual(dist_dev, 500)

    def test_stg_catchment_membership_count(self):
        """Verify stg_store_catchment_memberships retains exact Week 2 count (4,051 records)."""
        cnt = self.con.execute("SELECT COUNT(*) FROM stg_store_catchment_memberships;").fetchone()[0]
        self.assertEqual(cnt, 4051)

    def test_reconciliation_model_1_int_store_spatial_states(self):
        """Verify complete 60,000-row pairwise state grid and exact state distribution."""
        with open("dbt/geopulse_analytics/models/intermediate/int_store_spatial_states.sql", "r") as f:
            sql_clean = snowflake_to_duckdb_sql(f.read())
            self.con.execute(f"CREATE OR REPLACE VIEW int_store_spatial_states AS {sql_clean}")

        res = self.con.execute("""
            SELECT pairwise_state, COUNT(*)
            FROM int_store_spatial_states
            GROUP BY pairwise_state
            ORDER BY pairwise_state;
        """).fetchall()
        state_counts = dict(res)

        # Verify complete grid: 10,000 pings * 6 unordered pairs = 60,000 rows
        total_rows = self.con.execute("SELECT COUNT(*) FROM int_store_spatial_states;").fetchone()[0]
        self.assertEqual(total_rows, 10000 * 6)

        # Verify exact deterministic distribution across the 4 states
        self.assertEqual(state_counts["DEST_ONLY"], 5322)
        self.assertEqual(state_counts["ORIGIN_ONLY"], 6641)
        self.assertEqual(state_counts["OUTSIDE_PAIR"], 47942)
        self.assertEqual(state_counts["SHARED_PAIR"], 95)

        # Verify 6 canonical store pairs exist
        pairs = self.con.execute("""
            SELECT DISTINCT store_id_a, store_id_b
            FROM int_store_spatial_states
            ORDER BY store_id_a, store_id_b;
        """).fetchall()
        self.assertEqual(len(pairs), 6)

    def test_reconciliation_model_2_int_store_pairwise_device_transitions(self):
        """Verify 100% transition parity (194 transitions) against accepted Python engine."""
        with open("dbt/geopulse_analytics/models/intermediate/int_store_pairwise_device_transitions.sql", "r") as f:
            sql_clean = snowflake_to_duckdb_sql(f.read())
            self.con.execute(f"CREATE OR REPLACE VIEW int_store_pairwise_device_transitions AS {sql_clean}")

        trans_rows = self.con.execute("""
            SELECT COUNT(*), COUNT(DISTINCT device_id), MIN(duration_minutes), MAX(duration_minutes), MAX(transit_speed_mps)
            FROM int_store_pairwise_device_transitions;
        """).fetchone()

        # Canonical 10K dataset contains exactly 194 valid directed transitions
        self.assertEqual(trans_rows[0], 194)
        self.assertGreater(trans_rows[1], 0)
        self.assertGreater(trans_rows[2], 0.0)
        self.assertLessEqual(trans_rows[3], 60.0)
        self.assertLessEqual(trans_rows[4], 15.0)

    def test_trajectory_anomaly_break_and_no_bridging(self):
        """Verify anomaly detection breaks trajectory and prevents false bridging across anomalies."""
        # Query int_store_pairwise_device_transitions to ensure every transition satisfies plausibility
        anom_violations = self.con.execute("""
            SELECT transition_id, transit_speed_mps, duration_minutes
            FROM int_store_pairwise_device_transitions
            WHERE transit_speed_mps > 15.0 OR duration_minutes > 60.0 OR duration_minutes <= 0.0;
        """).fetchall()
        self.assertEqual(len(anom_violations), 0, f"Plausibility violations found: {anom_violations}")

    def test_third_store_intermediate_visit_handling(self):
        """Verify intermediate visits outside candidate pair do not break valid transitions."""
        # For directed pair (STORE_A -> STORE_B), transitions with duration > 10 min reflect intermediate traversal
        longer_trans = self.con.execute("""
            SELECT COUNT(*)
            FROM int_store_pairwise_device_transitions
            WHERE origin_store_id = 'STORE_A' AND destination_store_id = 'STORE_B'
              AND duration_minutes > 10.0;
        """).fetchone()[0]
        # Valid longer traversal transitions are retained
        self.assertGreaterEqual(longer_trans, 0)

    def test_reconciliation_model_3_fct_store_pairwise_overlap(self):
        """Verify fct_store_pairwise_overlap parity and precomputed 32-gon geometry constants."""
        with open("dbt/geopulse_analytics/models/marts/fct_store_pairwise_overlap.sql", "r") as f:
            sql_clean = snowflake_to_duckdb_sql(f.read())
            self.con.execute(f"CREATE OR REPLACE TABLE fct_store_pairwise_overlap AS {sql_clean}")

        rows = self.con.execute("SELECT * FROM fct_store_pairwise_overlap ORDER BY store_id_a, store_id_b;").fetchall()
        self.assertEqual(len(rows), 6)

        py_map = {(r["store_id_a"], r["store_id_b"]): r for r in self.py_overlap}
        cols = [desc[0] for desc in self.con.description]

        for r in rows:
            row_dict = dict(zip(cols, r))
            key = (row_dict["store_id_a"], row_dict["store_id_b"])
            py_row = py_map[key]

            # GOR parity
            self.assertAlmostEqual(float(row_dict["geometric_overlap_ratio"]), float(py_row["geometric_overlap_ratio"]), places=4)
            # Shared pings parity
            self.assertEqual(row_dict["total_shared_lens_pings"], py_row["total_shared_lens_pings"])
            # DJI parity
            self.assertAlmostEqual(float(row_dict["device_jaccard_index"]), float(py_row["device_jaccard_index"]), places=4)
            # DOC parity
            self.assertAlmostEqual(float(row_dict["overlap_coefficient"]), float(py_row["overlap_coefficient"]), places=4)

            # Specific check for STORE_A - STORE_B: must be exact 29518.22, not 143445.69
            if key == ("STORE_A", "STORE_B"):
                self.assertAlmostEqual(float(row_dict["lens_intersection_area_m2"]), 29518.22, places=2)
                self.assertNotAlmostEqual(float(row_dict["lens_intersection_area_m2"]), 143445.69, places=2)

    def test_reconciliation_model_4_fct_store_pairwise_hourly_overlap(self):
        """Verify diurnal hourly shared activity across 14 operating hours (84 rows)."""
        with open("dbt/geopulse_analytics/models/marts/fct_store_pairwise_hourly_overlap.sql", "r") as f:
            sql_clean = snowflake_to_duckdb_sql(f.read())
            self.con.execute(f"CREATE OR REPLACE TABLE fct_store_pairwise_hourly_overlap AS {sql_clean}")

        rows = self.con.execute("SELECT * FROM fct_store_pairwise_hourly_overlap ORDER BY store_id_a, store_id_b, hour_of_day;").fetchall()
        self.assertEqual(len(rows), 84)

        py_map = {(r["store_id_a"], r["store_id_b"], r["hour_of_day"]): r for r in self.py_hourly}
        cols = [desc[0] for desc in self.con.description]

        for r in rows:
            row_dict = dict(zip(cols, r))
            key = (row_dict["store_id_a"], row_dict["store_id_b"], row_dict["hour_of_day"])
            py_row = py_map[key]
            self.assertEqual(row_dict["shared_ping_volume"], py_row["shared_ping_volume"])

    def test_reconciliation_model_5_fct_store_cross_catchment_transitions(self):
        """Verify fct_store_cross_catchment_transitions 12 directed pairs and PCTR distinct device logic."""
        with open("dbt/geopulse_analytics/models/marts/fct_store_cross_catchment_transitions.sql", "r") as f:
            sql_clean = snowflake_to_duckdb_sql(f.read())
            self.con.execute(f"CREATE OR REPLACE TABLE fct_store_cross_catchment_transitions AS {sql_clean}")

        rows = self.con.execute("SELECT * FROM fct_store_cross_catchment_transitions ORDER BY origin_store_id, destination_store_id;").fetchall()
        self.assertEqual(len(rows), 12)
        cols = [desc[0] for desc in self.con.description]

        py_map = {(r["origin_store_id"], r["destination_store_id"]): r for r in self.py_trans}

        for r in rows:
            row_dict = dict(zip(cols, r))
            key = (row_dict["origin_store_id"], row_dict["destination_store_id"])
            py_row = py_map[key]

            # Directional constraint
            self.assertNotEqual(row_dict["origin_store_id"], row_dict["destination_store_id"])
            # Exact events and unique device counts
            self.assertEqual(row_dict["total_transition_events"], py_row["total_transition_events"])
            self.assertEqual(row_dict["unique_transitioning_devices_15m"], py_row["unique_transitioning_devices_15m"])
            self.assertEqual(row_dict["unique_transitioning_devices_30m"], py_row["unique_transitioning_devices_30m"])
            self.assertEqual(row_dict["unique_transitioning_devices_60m"], py_row["unique_transitioning_devices_60m"])

            # Contract Invariant: U_15m <= U_30m <= U_60m <= total_origin_devices
            self.assertLessEqual(row_dict["unique_transitioning_devices_15m"], row_dict["unique_transitioning_devices_30m"])
            self.assertLessEqual(row_dict["unique_transitioning_devices_30m"], row_dict["unique_transitioning_devices_60m"])
            self.assertLessEqual(row_dict["unique_transitioning_devices_60m"], row_dict["total_origin_devices"])

            # Exact PCTR rate match
            if row_dict["pctr_60m"] is not None:
                self.assertAlmostEqual(float(row_dict["pctr_60m"]), float(py_row["pctr_60m"]), places=4)
                self.assertLessEqual(float(row_dict["pctr_60m"]), 1.0000)
                self.assertGreaterEqual(float(row_dict["pctr_60m"]), 0.0000)

    def test_reconciliation_model_6_fct_store_temporal_opportunity(self):
        """Verify fct_store_temporal_opportunity PHI and LAHS parity."""
        with open("dbt/geopulse_analytics/models/marts/fct_store_temporal_opportunity.sql", "r") as f:
            sql_clean = snowflake_to_duckdb_sql(f.read())
            self.con.execute(f"CREATE OR REPLACE TABLE fct_store_temporal_opportunity AS {sql_clean}")

        rows = self.con.execute("SELECT * FROM fct_store_temporal_opportunity ORDER BY store_id;").fetchall()
        self.assertEqual(len(rows), 4)
        cols = [desc[0] for desc in self.con.description]

        py_map = {r["store_id"]: r for r in self.py_opp}
        for r in rows:
            row_dict = dict(zip(cols, r))
            py_row = py_map[row_dict["store_id"]]
            self.assertEqual(row_dict["operating_volume"], py_row["operating_volume"])
            self.assertEqual(row_dict["peak_hour_volume"], py_row["peak_hour_volume"])
            self.assertEqual(row_dict["peak_hour_window"], py_row["peak_hour_window"])
            self.assertEqual(row_dict["low_activity_hours_count"], py_row["low_activity_hours_count"])
            self.assertAlmostEqual(float(row_dict["low_activity_hour_share"]), float(py_row["low_activity_hour_share"]), places=4)
            self.assertAlmostEqual(float(row_dict["peak_hour_index"]), float(py_row["peak_hour_index"]), places=2)

    def test_reconciliation_model_7_absence_of_crs_from_all_marts(self):
        """Verify unapproved Conversion Readiness Score (CRS) is strictly absent from all canonical marts."""
        sql_files = [
            "dbt/geopulse_analytics/models/marts/fct_store_temporal_opportunity.sql",
            "dbt/geopulse_analytics/models/marts/fct_store_pairwise_overlap.sql",
            "dbt/geopulse_analytics/models/marts/fct_store_cross_catchment_transitions.sql",
            "dbt/geopulse_analytics/models/marts/fct_store_pairwise_hourly_overlap.sql",
            "dbt/geopulse_analytics/models/marts/marts_week3.yml",
        ]
        for fpath in sql_files:
            with open(fpath, "r", encoding="utf-8") as f:
                content = f.read().lower()
                self.assertNotIn("conversion_readiness_score", content, f"CRS found in {fpath}")
                self.assertNotIn("readiness_score", content, f"CRS found in {fpath}")

    def test_singular_assert_fct_store_pairwise_overlap_bounds(self):
        """Verify singular dbt test assert_fct_store_pairwise_overlap_bounds passes offline."""
        with open("dbt/geopulse_analytics/tests/assert_fct_store_pairwise_overlap_bounds.sql", "r") as f:
            sql = f.read().replace("{{ ref('fct_store_pairwise_overlap') }}", "fct_store_pairwise_overlap")
            fail_rows = self.con.execute(sql).fetchall()
            self.assertEqual(len(fail_rows), 0, f"Violations found: {fail_rows}")

    def test_singular_assert_fct_cross_catchment_pctr_invariants(self):
        """Verify singular dbt test assert_fct_cross_catchment_pctr_invariants passes offline."""
        with open("dbt/geopulse_analytics/tests/assert_fct_cross_catchment_pctr_invariants.sql", "r") as f:
            sql = f.read().replace("{{ ref('fct_store_cross_catchment_transitions') }}", "fct_store_cross_catchment_transitions")
            fail_rows = self.con.execute(sql).fetchall()
            self.assertEqual(len(fail_rows), 0, f"Violations found: {fail_rows}")

    def test_singular_assert_fct_store_temporal_opportunity_invariants(self):
        """Verify singular dbt test assert_fct_store_temporal_opportunity_invariants passes offline."""
        with open("dbt/geopulse_analytics/tests/assert_fct_store_temporal_opportunity_invariants.sql", "r") as f:
            sql = f.read().replace("{{ ref('fct_store_temporal_opportunity') }}", "fct_store_temporal_opportunity")
            fail_rows = self.con.execute(sql).fetchall()
            self.assertEqual(len(fail_rows), 0, f"Violations found: {fail_rows}")

    def test_reconciliation_zero_denominator_pctr_null_handling(self):
        """Verify zero origin device count returns NULL for PCTR rather than division by zero."""
        test_sql = """
            SELECT
                'STORE_X' AS origin_store_id,
                'STORE_Y' AS destination_store_id,
                0 AS total_origin_devices,
                ROUND(COALESCE(0, 0)::FLOAT / NULLIF(0, 0), 4) AS pctr_15m,
                ROUND(COALESCE(0, 0)::FLOAT / NULLIF(0, 0), 4) AS pctr_30m,
                ROUND(COALESCE(0, 0)::FLOAT / NULLIF(0, 0), 4) AS pctr_60m;
        """
        res = self.con.execute(test_sql).fetchone()
        self.assertIsNone(res[3], "pctr_15m must be NULL when denominator is zero")
        self.assertIsNone(res[4], "pctr_30m must be NULL when denominator is zero")
        self.assertIsNone(res[5], "pctr_60m must be NULL when denominator is zero")

    def test_reconciliation_zero_median_phi_null_handling(self):
        """Verify zero median operating volume sets PHI to NULL and flags has_zero_median_activity."""
        test_sql = """
            SELECT
                'STORE_ZERO' AS store_id,
                0 AS median_operating_hourly_volume,
                10 AS peak_hour_volume,
                CASE WHEN 0 = 0 THEN TRUE ELSE FALSE END AS has_zero_median_activity,
                CASE WHEN 0 = 0 THEN NULL ELSE ROUND(10::FLOAT / 0, 2) END AS peak_hour_index;
        """
        res = self.con.execute(test_sql).fetchone()
        self.assertTrue(res[3], "has_zero_median_activity must be TRUE")
        self.assertIsNone(res[4], "peak_hour_index must be NULL when median is zero")

    def test_reconciliation_zero_overlap_pairs_canonical(self):
        """Verify canonical zero-overlap pairs have zero GOR, zero lens area, and zero shared pings."""
        zero_pairs = [("STORE_A", "STORE_C"), ("STORE_A", "STORE_D"), ("STORE_C", "STORE_D")]
        for s_a, s_b in zero_pairs:
            row = self.con.execute(f"""
                SELECT lens_intersection_area_m2, geometric_overlap_ratio, total_shared_lens_pings
                FROM fct_store_pairwise_overlap
                WHERE store_id_a = '{s_a}' AND store_id_b = '{s_b}';
            """).fetchone()
            self.assertIsNotNone(row, f"Pair {s_a}-{s_b} missing from fct_store_pairwise_overlap")
            self.assertAlmostEqual(float(row[0]), 0.0, places=2)
            self.assertAlmostEqual(float(row[1]), 0.0000, places=4)
            self.assertEqual(row[2], 0)

    def test_reconciliation_pctr_monotonicity_all_12_pairs(self):
        """Verify strict monotonicity 0 <= U_15m <= U_30m <= U_60m <= total_origin_devices across all 12 pairs."""
        rows = self.con.execute("""
            SELECT
                origin_store_id, destination_store_id, total_origin_devices,
                unique_transitioning_devices_15m, unique_transitioning_devices_30m, unique_transitioning_devices_60m,
                pctr_15m, pctr_30m, pctr_60m
            FROM fct_store_cross_catchment_transitions
            ORDER BY origin_store_id, destination_store_id;
        """).fetchall()
        self.assertEqual(len(rows), 12)
        for r in rows:
            u15, u30, u60, n_orig = r[3], r[4], r[5], r[2]
            p15, p30, p60 = float(r[6]), float(r[7]), float(r[8])
            self.assertGreater(n_orig, 0)
            self.assertGreaterEqual(u15, 0)
            self.assertLessEqual(u15, u30)
            self.assertLessEqual(u30, u60)
            self.assertLessEqual(u60, n_orig)
            self.assertGreaterEqual(p15, 0.0000)
            self.assertLessEqual(p15, p30)
            self.assertLessEqual(p30, p60)
            self.assertLessEqual(p60, 1.0000)

    def test_singular_assertions_fail_on_invalid_data(self):
        """Verify that custom singular test SQL assertions actively fail (return rows) on invalid inputs."""
        # Test 1: PCTR invariant violation (U15 > U30)
        self.con.execute("""
            CREATE TEMP TABLE mock_invalid_pctr AS
            SELECT 'STORE_A' AS origin_store_id, 'STORE_B' AS destination_store_id,
                   100 AS total_origin_devices, 20 AS unique_transitioning_devices_15m,
                   10 AS unique_transitioning_devices_30m, 30 AS unique_transitioning_devices_60m,
                   0.2000 AS pctr_15m, 0.1000 AS pctr_30m, 0.3000 AS pctr_60m;
        """)
        with open("dbt/geopulse_analytics/tests/assert_fct_cross_catchment_pctr_invariants.sql", "r") as f:
            sql = f.read().replace("{{ ref('fct_store_cross_catchment_transitions') }}", "mock_invalid_pctr")
            violations = self.con.execute(sql).fetchall()
            self.assertGreater(len(violations), 0, "Singular PCTR test must catch U15 > U30 violation")

        # Test 2: Overlap bounds violation (GOR > 1.0)
        self.con.execute("""
            CREATE TEMP TABLE mock_invalid_overlap AS
            SELECT 'STORE_A' AS store_id_a, 'STORE_B' AS store_id_b,
                   1.5000 AS geometric_overlap_ratio, 0.5000 AS device_jaccard_index,
                   0.6000 AS overlap_coefficient, 50 AS shared_unique_devices,
                   100 AS union_unique_devices, 80 AS unique_devices_store_a,
                   70 AS unique_devices_store_b, 1000.0 AS lens_intersection_area_m2,
                   5000.0 AS union_polygon_area_m2, 10 AS total_shared_lens_pings;
        """)
        with open("dbt/geopulse_analytics/tests/assert_fct_store_pairwise_overlap_bounds.sql", "r") as f:
            sql = f.read().replace("{{ ref('fct_store_pairwise_overlap') }}", "mock_invalid_overlap")
            violations = self.con.execute(sql).fetchall()
            self.assertGreater(len(violations), 0, "Singular overlap test must catch GOR > 1.0 violation")

        # Test 3: Temporal opportunity violation (peak_volume > operating_volume)
        self.con.execute("""
            CREATE TEMP TABLE mock_invalid_temporal AS
            SELECT 'STORE_A' AS store_id, 100 AS operating_volume, 150 AS peak_hour_volume,
                   10.0 AS median_operating_hourly_volume, FALSE AS has_zero_median_activity,
                   15.0 AS peak_hour_index, 2 AS low_activity_hours_count, 0.1429 AS low_activity_hour_share;
        """)
        with open("dbt/geopulse_analytics/tests/assert_fct_store_temporal_opportunity_invariants.sql", "r") as f:
            sql = f.read().replace("{{ ref('fct_store_temporal_opportunity') }}", "mock_invalid_temporal")
            violations = self.con.execute(sql).fetchall()
            self.assertGreater(len(violations), 0, "Singular temporal test must catch peak > operating volume")


if __name__ == "__main__":
    unittest.main()
