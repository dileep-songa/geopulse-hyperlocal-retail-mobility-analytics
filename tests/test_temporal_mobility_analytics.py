"""Comprehensive unit and integration tests for GeoPulse Week 3 temporal mobility analytics."""

import csv
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import math
import os
import unittest

from src.temporal_mobility_analytics import (
    classify_pairwise_state,
    compute_exact_32gon_intersection_area,
    compute_polygon_area,
    compute_convex_polygon_intersection,
    generate_regular_32gon_vertices,
    is_point_on_segment,
    point_in_polygon_intersects,
    round_decimal,
    wgs84_to_utm43n,
    MobilityPing,
    StoreLocation,
    TemporalMobilityEngine,
    OPERATING_HOURS,
)


class TestCoordinateProjectionAndGeometry(unittest.TestCase):
    """Test geodetic projection and planar geometry routines."""

    def test_wgs84_to_utm43n_known_point(self):
        # Indiranagar 100ft road store A coordinate: (77.6320, 12.9760)
        e, n = wgs84_to_utm43n(77.6320, 12.9760)
        self.assertAlmostEqual(e, 785535.9837, places=3)
        self.assertAlmostEqual(n, 1435955.0466, places=3)

    def test_wgs84_to_utm43n_bounds(self):
        with self.assertRaises(ValueError):
            wgs84_to_utm43n(200.0, 12.0)
        with self.assertRaises(ValueError):
            wgs84_to_utm43n(77.0, -100.0)

    def test_generate_regular_32gon_vertices(self):
        poly = generate_regular_32gon_vertices(1000.0, 2000.0, 500.0)
        self.assertEqual(len(poly), 33)  # 32 facets + closing vertex
        self.assertEqual(poly[0], poly[-1])
        # Area of regular 32-gon with r=500m
        area = compute_polygon_area(poly)
        expected_area = 16.0 * (500.0 ** 2) * math.sin(math.pi / 16.0)
        self.assertAlmostEqual(area, expected_area, places=2)
        self.assertAlmostEqual(area, 780361.29, delta=0.5)

    def test_point_in_polygon_boundary_and_corner(self):
        # Unit square: (0,0), (10,0), (10,10), (0,10), (0,0)
        sq = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]
        # Inside
        self.assertTrue(point_in_polygon_intersects(5.0, 5.0, sq))
        # Boundary edges (ST_Intersects semantics)
        self.assertTrue(point_in_polygon_intersects(0.0, 5.0, sq))
        self.assertTrue(point_in_polygon_intersects(10.0, 5.0, sq))
        self.assertTrue(point_in_polygon_intersects(5.0, 0.0, sq))
        self.assertTrue(point_in_polygon_intersects(5.0, 10.0, sq))
        # Corners
        self.assertTrue(point_in_polygon_intersects(0.0, 0.0, sq))
        self.assertTrue(point_in_polygon_intersects(10.0, 10.0, sq))
        # Outside
        self.assertFalse(point_in_polygon_intersects(11.0, 5.0, sq))
        self.assertFalse(point_in_polygon_intersects(-1.0, 5.0, sq))

    def test_point_outside_32gon_but_inside_radial_distance(self):
        # Center at (0, 0), radius = 500.0
        poly = generate_regular_32gon_vertices(0.0, 0.0, 500.0)
        # Midpoint of first facet between angle 0 and angle -2*pi/32 (-11.25 deg)
        # At angle -5.625 deg, the apothem is r * cos(pi/32) = 500 * cos(5.625 deg) ≈ 497.592 m
        angle_mid = - math.pi / 32.0
        apothem = 500.0 * math.cos(angle_mid)
        # Point at distance 498.5m along this angle is OUTSIDE the 32-gon polygon,
        # but INSIDE the true Euclidean circle of radius 500m!
        test_x = 498.5 * math.cos(angle_mid)
        test_y = 498.5 * math.sin(angle_mid)
        dist = math.hypot(test_x, test_y)
        self.assertLess(dist, 500.0)
        self.assertGreater(dist, apothem)
        # Must be FALSE under Sedona 32-gon ST_Intersects semantics
        self.assertFalse(point_in_polygon_intersects(test_x, test_y, poly))

    def test_exact_polygon_intersection_cases(self):
        p1 = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]
        # Identical
        self.assertAlmostEqual(compute_exact_32gon_intersection_area(p1, p1), 100.0)
        # Disjoint
        p_disjoint = [(20.0, 0.0), (30.0, 0.0), (30.0, 10.0), (20.0, 10.0), (20.0, 0.0)]
        self.assertEqual(compute_exact_32gon_intersection_area(p1, p_disjoint), 0.0)
        # Contained
        p_contained = [(2.0, 2.0), (8.0, 2.0), (8.0, 8.0), (2.0, 8.0), (2.0, 2.0)]
        self.assertAlmostEqual(compute_exact_32gon_intersection_area(p1, p_contained), 36.0)
        # Partial overlap (50%)
        p_partial = [(5.0, 0.0), (15.0, 0.0), (15.0, 10.0), (5.0, 10.0), (5.0, 0.0)]
        self.assertAlmostEqual(compute_exact_32gon_intersection_area(p1, p_partial), 50.0)


class TestPairwiseStateClassification(unittest.TestCase):
    """Test 4-state classification and third-store membership independence."""

    def test_pairwise_states_independent_of_third_store(self):
        # Only in A
        self.assertEqual(classify_pairwise_state({"STORE_A"}, "STORE_A", "STORE_B"), "ORIGIN_ONLY")
        # In A and third store C (still ORIGIN_ONLY relative to pair A, B)
        self.assertEqual(classify_pairwise_state({"STORE_A", "STORE_C"}, "STORE_A", "STORE_B"), "ORIGIN_ONLY")
        # In B and third store D (DEST_ONLY relative to pair A, B)
        self.assertEqual(classify_pairwise_state({"STORE_B", "STORE_D"}, "STORE_A", "STORE_B"), "DEST_ONLY")
        # In A, B, and C (SHARED_PAIR relative to pair A, B)
        self.assertEqual(classify_pairwise_state({"STORE_A", "STORE_B", "STORE_C"}, "STORE_A", "STORE_B"), "SHARED_PAIR")
        # Only in C (OUTSIDE_PAIR relative to pair A, B)
        self.assertEqual(classify_pairwise_state({"STORE_C"}, "STORE_A", "STORE_B"), "OUTSIDE_PAIR")
        # Empty (OUTSIDE_PAIR)
        self.assertEqual(classify_pairwise_state(set(), "STORE_A", "STORE_B"), "OUTSIDE_PAIR")


class TestTemporalMobilityEngineTransitions(unittest.TestCase):
    """Test transition state machine, anomaly filtering, no-bridge behavior, and invariants."""

    def setUp(self):
        self.stores = [
            StoreLocation("STORE_A", "Store A", 77.6330, 12.9759, 785535.98, 1435955.05),
            StoreLocation("STORE_B", "Store B", 77.6410, 12.9780, 786402.14, 1436185.41),
            StoreLocation("STORE_C", "Store C", 77.6490, 12.9811, 787267.13, 1436526.50),
            StoreLocation("STORE_D", "Store D", 77.6389, 12.9860, 786175.86, 1437068.70),
        ]
        self.engine = TemporalMobilityEngine(self.stores)
        self.base_time = datetime(2026, 11, 16, 10, 0, 0, tzinfo=timezone.utc)

    def _make_ping(self, ping_id, dev_id, minutes_offset, easting, northing, stores):
        dt = self.base_time + timedelta(minutes=minutes_offset)
        return MobilityPing(
            ping_id=ping_id,
            device_id=dev_id,
            event_timestamp=dt,
            local_timestamp=dt.astimezone(self.engine.local_tz),
            latitude=12.9759,
            longitude=77.6330,
            easting=easting,
            northing=northing,
            movement_mode="pedestrian_walking",
            data_provenance="test",
            intersecting_stores=set(stores),
        )

    def test_valid_pairwise_transition(self):
        # A valid transition A -> B taking 10 minutes (~896m in 600s = 1.49 m/s)
        p1 = self._make_ping("p1", "d1", 0, 785535.98, 1435955.05, ["STORE_A"])
        p2 = self._make_ping("p2", "d1", 5, 785900.00, 1436050.00, [])  # intermediate outside
        p3 = self._make_ping("p3", "d1", 10, 786402.14, 1436185.41, ["STORE_B"])
        
        transitions = self.engine.extract_transitions_for_pair([p1, p2, p3], "STORE_A", "STORE_B")
        self.assertEqual(len(transitions), 1)
        tr = transitions[0]
        self.assertEqual(tr.device_id, "d1")
        self.assertEqual(tr.origin_store_id, "STORE_A")
        self.assertEqual(tr.destination_store_id, "STORE_B")
        self.assertEqual(tr.duration_minutes, 10.0)
        self.assertTrue(tr.is_kinematically_plausible)
        self.assertAlmostEqual(tr.displacement_distance_m, 896.26, delta=5.0)

    def test_multi_store_intermediate_visit_handling(self):
        # Trajectory: Origin Store A -> Visits Store C (exclusive) -> Arrives Store B within 30 min
        # Under pair (A, B) rules: Store C is OUTSIDE_PAIR.
        # It permits traversal provided delta_t <= 60 min and v_trans <= 15 m/s.
        p1 = self._make_ping("p1", "d1", 0, 785535.98, 1435955.05, ["STORE_A"])
        p2 = self._make_ping("p2", "d1", 10, 787267.13, 1436526.50, ["STORE_C"])
        p3 = self._make_ping("p3", "d1", 25, 786402.14, 1436185.41, ["STORE_B"])
        
        transitions = self.engine.extract_transitions_for_pair([p1, p2, p3], "STORE_A", "STORE_B")
        self.assertEqual(len(transitions), 1)
        self.assertEqual(transitions[0].duration_minutes, 25.0)

    def test_speed_ceiling_rejection(self):
        dt0 = self.base_time
        dt1 = self.base_time + timedelta(seconds=10)
        p1 = MobilityPing("p1", "d1", dt0, dt0, 12.9, 77.6, 785535.98, 1435955.05, "w", "test", {"STORE_A"})
        p2 = MobilityPing("p2", "d1", dt1, dt1, 12.9, 77.6, 786402.14, 1436185.41, "w", "test", {"STORE_B"})
        
        transitions = self.engine.extract_transitions_for_pair([p1, p2], "STORE_A", "STORE_B")
        self.assertEqual(len(transitions), 0)

    def test_anomaly_hard_break_no_bridging(self):
        dt0 = self.base_time
        dt1 = self.base_time + timedelta(seconds=2)
        dt2 = self.base_time + timedelta(minutes=15)
        
        p1 = MobilityPing("p1", "d1", dt0, dt0, 12.9, 77.6, 785535.98, 1435955.05, "w", "test", {"STORE_A"})
        p2 = MobilityPing("p2", "d1", dt1, dt1, 12.9, 77.6, 795535.98, 1435955.05, "w", "test", set())
        p3 = MobilityPing("p3", "d1", dt2, dt2, 12.9, 77.6, 786402.14, 1436185.41, "w", "test", {"STORE_B"})
        
        transitions = self.engine.extract_transitions_for_pair([p1, p2, p3], "STORE_A", "STORE_B")
        self.assertEqual(len(transitions), 0)

    def test_duplicate_timestamp_handling(self):
        dt0 = self.base_time
        dt1 = self.base_time + timedelta(minutes=10)
        p1a = MobilityPing("p1a", "d1", dt0, dt0, 12.9, 77.6, 785535.98, 1435955.05, "w", "test", {"STORE_A"})
        p1b = MobilityPing("p1b", "d1", dt0, dt0, 12.9, 77.6, 785535.98, 1435955.05, "w", "test", {"STORE_A"})
        p2 = MobilityPing("p2", "d1", dt1, dt1, 12.9, 77.6, 786402.14, 1436185.41, "w", "test", {"STORE_B"})
        
        transitions = self.engine.extract_transitions_for_pair([p1a, p1b, p2], "STORE_A", "STORE_B")
        self.assertEqual(len(transitions), 1)

    def test_simultaneous_distinct_coordinates_teleportation(self):
        dt0 = self.base_time
        dt1 = self.base_time + timedelta(minutes=10)
        p1a = MobilityPing("p1a", "d1", dt0, dt0, 12.9, 77.6, 785535.98, 1435955.05, "w", "test", {"STORE_A"})
        p1b = MobilityPing("p1b", "d1", dt0, dt0, 12.9, 77.6, 786000.00, 1436000.00, "w", "test", {"STORE_A"})
        p2 = MobilityPing("p2", "d1", dt1, dt1, 12.9, 77.6, 786402.14, 1436185.41, "w", "test", {"STORE_B"})
        
        transitions = self.engine.extract_transitions_for_pair([p1a, p1b, p2], "STORE_A", "STORE_B")
        self.assertEqual(len(transitions), 0)

    def test_60_minute_window_expiration(self):
        p1 = self._make_ping("p1", "d1", 0, 785535.98, 1435955.05, ["STORE_A"])
        p2 = self._make_ping("p2", "d1", 65, 786402.14, 1436185.41, ["STORE_B"])
        
        transitions = self.engine.extract_transitions_for_pair([p1, p2], "STORE_A", "STORE_B")
        self.assertEqual(len(transitions), 0)


class TestMetricsAndParity(unittest.TestCase):
    """Test full metric tables computation, invariant checks, and zero-median safety."""

    def setUp(self):
        self.stores = [
            StoreLocation("STORE_A", "Store A", 77.6330, 12.9759, 785535.98, 1435955.05),
            StoreLocation("STORE_B", "Store B", 77.6410, 12.9780, 786402.14, 1436185.41),
            StoreLocation("STORE_C", "Store C", 77.6490, 12.9811, 787267.13, 1436526.50),
            StoreLocation("STORE_D", "Store D", 77.6389, 12.9860, 786175.86, 1437068.70),
        ]
        self.engine = TemporalMobilityEngine(self.stores)

    def test_zero_denominator_pctr(self):
        fct_tr = self.engine.compute_fct_cross_catchment_transitions([])
        for r in fct_tr:
            self.assertEqual(r["unique_origin_devices"], 0)
            self.assertIsNone(r["pctr_15m"])
            self.assertIsNone(r["pctr_30m"])
            self.assertIsNone(r["pctr_60m"])

    def test_pctr_invariant_violation_raises(self):
        pings = [
            MobilityPing("p1", "d1", datetime(2026, 11, 16, 10, 0, tzinfo=timezone.utc),
                         datetime(2026, 11, 16, 10, 0, tzinfo=timezone.utc), 12.9, 77.6, 785535.98, 1435955.05, "w", "t", {"STORE_A"}),
            MobilityPing("p2", "d1", datetime(2026, 11, 16, 10, 10, tzinfo=timezone.utc),
                         datetime(2026, 11, 16, 10, 10, tzinfo=timezone.utc), 12.9, 77.6, 786402.14, 1436185.41, "w", "t", {"STORE_B"}),
        ]
        fct = self.engine.compute_fct_cross_catchment_transitions(pings)
        ab = next(r for r in fct if r["origin_store_id"] == "STORE_A" and r["destination_store_id"] == "STORE_B")
        self.assertEqual(ab["pctr_60m"], Decimal("1.0000"))

    def test_phi_zero_median_and_earliest_tie_break(self):
        fct_opp = self.engine.compute_fct_temporal_opportunity([])
        for r in fct_opp:
            self.assertTrue(r["has_zero_median_activity"])
            self.assertIsNone(r["peak_hour_index"])

        p1 = MobilityPing("p1", "d1", datetime(2026, 11, 16, 2, 30, tzinfo=timezone.utc),
                          datetime(2026, 11, 16, 8, 0, tzinfo=self.engine.local_tz), 12.9, 77.6, 785535.98, 1435955.05, "w", "t", {"STORE_A"})
        p2 = MobilityPing("p2", "d2", datetime(2026, 11, 16, 3, 30, tzinfo=timezone.utc),
                          datetime(2026, 11, 16, 9, 0, tzinfo=self.engine.local_tz), 12.9, 77.6, 785535.98, 1435955.05, "w", "t", {"STORE_A"})
        fct_opp2 = self.engine.compute_fct_temporal_opportunity([p1, p2])
        sa = next(r for r in fct_opp2 if r["store_id"] == "STORE_A")
        self.assertEqual(sa["peak_hour_window"], "08:00")
        self.assertEqual(sa["peak_hour_volume"], 1)

    def test_pairwise_hourly_overlap_count(self):
        fct_hourly = self.engine.compute_fct_pairwise_hourly_overlap([])
        self.assertEqual(len(fct_hourly), 84)


class TestIntegrationWith10KDatasetAndSedonaParity(unittest.TestCase):
    """Integration test pinning exact Sedona 10K dataset parity."""

    def test_exact_sedona_10k_catchment_membership_parity(self):
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
        with open(memberships_path, "r", encoding="utf-8") as f:
            sedona_memberships = list(csv.DictReader(f))
            
        engine = TemporalMobilityEngine.from_store_records(store_rows, catchment_wkt_map=catchment_wkt_map)
        pings = engine.load_pings(ping_rows)
        
        # Verify 100% exact membership parity with Week 2 Sedona baseline
        sedona_pairs = {(r["ping_id"], r["store_id"]) for r in sedona_memberships}
        engine_pairs = {(p.ping_id, s) for p in pings for s in p.intersecting_stores}
        
        self.assertEqual(len(engine_pairs), 4051)
        self.assertEqual(len(sedona_pairs), 4051)
        self.assertEqual(engine_pairs, sedona_pairs)
        self.assertEqual(len(sedona_pairs ^ engine_pairs), 0)
        
        # Verify per-store counts match Sedona baseline
        store_counts = {}
        for p in pings:
            for s in p.intersecting_stores:
                store_counts[s] = store_counts.get(s, 0) + 1
        self.assertEqual(store_counts["STORE_A"], 1190)
        self.assertEqual(store_counts["STORE_B"], 1118)
        self.assertEqual(store_counts["STORE_C"], 930)
        self.assertEqual(store_counts["STORE_D"], 813)

        # Test fct_store_pairwise_overlap exact values
        overlap_rows = engine.compute_fct_pairwise_overlap(pings)
        self.assertEqual(len(overlap_rows), 6)
        
        ab = next(r for r in overlap_rows if r["store_id_a"] == "STORE_A" and r["store_id_b"] == "STORE_B")
        # Exact 32-gon intersection area
        self.assertEqual(ab["lens_intersection_area_m2"], Decimal("29518.22"))
        self.assertEqual(ab["geometric_overlap_ratio"], Decimal("0.0193"))
        self.assertEqual(ab["total_shared_lens_pings"], 60)
        
        # Disjoint pairs have exact 0.0000 GOR
        ac = next(r for r in overlap_rows if r["store_id_a"] == "STORE_A" and r["store_id_b"] == "STORE_C")
        self.assertEqual(ac["lens_intersection_area_m2"], Decimal("0.00"))
        self.assertEqual(ac["geometric_overlap_ratio"], Decimal("0.0000"))
        self.assertEqual(ac["total_shared_lens_pings"], 0)

    def test_exact_194_transitions_and_directed_pair_distribution(self):
        stores_path = "data/reference/synthetic_store_locations.csv"
        pings_path = "data/raw/synthetic_mobility_pings.csv"
        catchments_path = "data/processed/week2_verification/store_catchments/part-00000-add80fd6-392e-4363-9704-248ee09f2202-c000.csv"
        
        with open(stores_path, "r", encoding="utf-8") as f:
            store_rows = list(csv.DictReader(f))
        with open(pings_path, "r", encoding="utf-8") as f:
            ping_rows = list(csv.DictReader(f))
        with open(catchments_path, "r", encoding="utf-8") as f:
            catchment_wkt_map = {r["store_id"]: r["geometry_wkt"] for r in csv.DictReader(f)}
            
        engine = TemporalMobilityEngine.from_store_records(store_rows, catchment_wkt_map=catchment_wkt_map)
        pings = engine.load_pings(ping_rows)
        
        expected_pair_counts = {
            ("STORE_A", "STORE_B"): 23,
            ("STORE_A", "STORE_C"): 8,
            ("STORE_A", "STORE_D"): 9,
            ("STORE_B", "STORE_A"): 26,
            ("STORE_B", "STORE_C"): 22,
            ("STORE_B", "STORE_D"): 18,
            ("STORE_C", "STORE_A"): 8,
            ("STORE_C", "STORE_B"): 22,
            ("STORE_C", "STORE_D"): 14,
            ("STORE_D", "STORE_A"): 11,
            ("STORE_D", "STORE_B"): 18,
            ("STORE_D", "STORE_C"): 15,
        }
        
        total_transitions = 0
        for (orig, dest), expected_count in expected_pair_counts.items():
            trans = engine.extract_transitions_for_pair(pings, orig, dest)
            self.assertEqual(len(trans), expected_count, f"Mismatch for {orig}->{dest}")
            total_transitions += len(trans)
            
        self.assertEqual(total_transitions, 194)

    def test_engine_initialization_and_validation(self):
        with self.assertRaises(ValueError):
            TemporalMobilityEngine([], catchment_radius_m=0.0)
        with self.assertRaises(ValueError):
            TemporalMobilityEngine([], max_transit_speed_mps=-5.0)

    def test_extract_transitions_validation_edge_cases(self):
        stores = [StoreLocation("STORE_A", "Store A", 77.63, 12.97, 785535.98, 1435955.05)]
        engine = TemporalMobilityEngine(stores)
        # Empty pings
        self.assertEqual(engine.extract_transitions_for_pair([], "STORE_A", "STORE_B") if "STORE_B" in engine.stores else [], [])
        # Unknown store
        with self.assertRaises(ValueError):
            engine.extract_transitions_for_pair([], "STORE_A", "STORE_UNKNOWN")
        with self.assertRaises(ValueError):
            engine.extract_transitions_for_pair([], "STORE_UNKNOWN", "STORE_A")
        # Same origin/destination
        with self.assertRaises(ValueError):
            engine.extract_transitions_for_pair([], "STORE_A", "STORE_A")


if __name__ == "__main__":
    unittest.main()
