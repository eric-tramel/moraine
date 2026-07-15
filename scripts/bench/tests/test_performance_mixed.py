from __future__ import annotations

import sys
import threading
import unittest
from pathlib import Path
from unittest import mock

BENCH_DIR = Path(__file__).resolve().parents[1]
if str(BENCH_DIR) not in sys.path:
    sys.path.insert(0, str(BENCH_DIR))

import performance_scenarios as scenarios


QUERY_SCHEDULE = [
    {"kind": "query", "sequence": 0, "scheduled_offset_ns": 0, "case_id": "q0"},
    {"kind": "query", "sequence": 1, "scheduled_offset_ns": 1_000_000, "case_id": "q1"},
]
EVENT_SCHEDULE = [
    {"kind": "event", "sequence": 0, "scheduled_offset_ns": 0, "case_id": "e0"},
    {"kind": "event", "sequence": 1, "scheduled_offset_ns": 1_000_000, "case_id": "e1"},
]


def query_evidence(count: int = 2) -> dict:
    return {
        "metrics": {"goodput_qps": 100.0, "p95_ms": 100.0, "p99_ms": 150.0},
        "planned": count,
        "started": count,
        "completed": count,
        "scheduler_p99_slip_ms": 1.0,
        "failures": {
            "dropped": 0,
            "timeout": 0,
            "admission_rejection": 0,
            "semantic": 0,
            "malformed": 0,
            "other": 0,
        },
        "drained": True,
        "backlog": 0,
        "first_started_ns": 100,
        "last_completed_ns": 300,
        "samples": [{"sequence": index, "outcome": "correct"} for index in range(count)],
        "semantic_failures": 0,
    }


def valid_etd_sample(index: int) -> dict:
    return {
        "event_identity_sha256": f"sha256:{index:064x}",
        "term_sha256": f"sha256:{index + 100:064x}",
        "batch_sequence": index + 1,
        "publication_durable_ms": 1.0,
        "db_ack_ms": 2.0,
        "last_miss_ms": 3.0,
        "first_hit_ms": 4.0,
        "first_valid_ms": 4.0,
        "source_interval": {
            "lower_ms": 3.0,
            "upper_ms": 4.0,
            "censoring": "interval",
        },
        "db_ack_interval": {
            "lower_ms": 1.0,
            "upper_ms": 2.0,
            "censoring": "interval",
        },
        "term_use_count": 1,
        "cache_bypass": {
            "result": True,
            "document_frequency": True,
            "posting": True,
            "corpus": True,
            "hydration": True,
        },
        "valid": True,
        "error_code": None,
    }


def ingest_evidence(count: int = 2) -> dict:
    interval = {"lower_ms": 80.0, "upper_ms": 100.0, "censoring": "interval"}
    return {
        "status": "pass",
        "metrics": {
            "source_etd_p95": dict(interval),
            "db_ack_etd_p95": dict(interval),
        },
        "planned": count,
        "started": count,
        "completed": count,
        "scheduler_p99_slip_ms": 1.0,
        "drained": True,
        "backlog": 0,
        "lost_events": 0,
        "duplicate_events": 0,
        "first_started_ns": 150,
        "last_completed_ns": 350,
        "samples": [valid_etd_sample(index) for index in range(count)],
        "semantic_failures": 0,
    }


class FakeMixedArm:
    def __init__(self, label: str, owner: "ArmFactory") -> None:
        self.label = label
        self.owner = owner
        self.reset_id = owner.reset_id_for(label)
        self.recipe_fingerprint = owner.recipe_fingerprint_for(label)
        self.stream_barrier = threading.Barrier(2) if label == "combined" else None
        self.closed = False

    def run_query(self, schedule):
        self.owner.calls.append((self.label, "query"))
        if self.stream_barrier is not None:
            self.stream_barrier.wait(timeout=1.0)
        evidence = query_evidence(len(schedule))
        self.owner.mutate(self.label, "query", evidence)
        return evidence

    def run_ingest(self, schedule):
        self.owner.calls.append((self.label, "ingest"))
        if self.stream_barrier is not None:
            self.stream_barrier.wait(timeout=1.0)
        evidence = ingest_evidence(len(schedule))
        self.owner.mutate(self.label, "ingest", evidence)
        return evidence

    def queue_depth(self) -> int:
        return self.owner.queue_depths.get(self.label, 0)

    def close(self) -> None:
        self.closed = True
        if self.owner.cleanup_failure_label == self.label:
            raise OSError(f"{self.label} cleanup failed")


class ArmFactory:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.mutations: dict[tuple[str, str], object] = {}
        self.queue_depths: dict[str, int] = {}
        self.same_reset = False
        self.recipe_mismatch = False
        self.cleanup_failure_label: str | None = None
        self.arms: list[FakeMixedArm] = []

    def reset_id_for(self, label: str) -> str:
        return "one-reset" if self.same_reset else f"reset-{label}"

    def recipe_fingerprint_for(self, label: str) -> str:
        return f"recipe-{label}" if self.recipe_mismatch else "recipe-fixed"

    def mutate(self, label: str, stream: str, evidence: dict) -> None:
        mutation = self.mutations.get((label, stream))
        if callable(mutation):
            mutation(evidence)

    def __call__(self, label: str) -> FakeMixedArm:
        arm = FakeMixedArm(label, self)
        self.arms.append(arm)
        return arm


class MixedScenarioTests(unittest.TestCase):
    def run_mixed(self, factory: ArmFactory):
        return scenarios.run_mixed_scenario(
            QUERY_SCHEDULE,
            EVENT_SCHEDULE,
            factory,
            combined_timeout_s=1.0,
        )

    def test_owned_query_stream_prepares_route_before_measurement(self) -> None:
        sandbox = mock.Mock(sandbox_id="sb-123456")
        runner = mock.Mock(return_value=query_evidence())
        with mock.patch.object(
            scenarios,
            "make_owned_sandbox_query_load",
            return_value=runner,
        ):
            arm = scenarios.OwnedSandboxMixedArm(
                sandbox,
                {"q0": {"case_id": "q0"}},
                {"e0": {"case_id": "e0"}},
                query_rate_qps=10.0,
                recipe_fingerprint="fixture",
            )
            result = arm.run_query(QUERY_SCHEDULE[:1])
        runner.prepare.assert_called_once_with()
        runner.assert_called_once_with(10.0)
        self.assertEqual(result["metrics"]["goodput_qps"], 100.0)

    def test_controls_run_before_one_barrier_started_overlapping_combined_arm(self) -> None:
        factory = ArmFactory()
        result = self.run_mixed(factory)
        self.assertEqual(result.status, "pass")
        self.assertEqual(factory.calls[:2], [("query_only", "query"), ("ingest_only", "ingest")])
        self.assertEqual(set(factory.calls[2:]), {("combined", "query"), ("combined", "ingest")})
        self.assertTrue(result.metrics["mixed_gates"]["overlap"])
        self.assertTrue(result.metrics["mixed_gates"]["drained"])
        self.assertTrue(result.metrics["mixed_gates"]["exact_events"])
        self.assertEqual(set(result.samples), {"query_records", "ingest"})

    def test_cleanup_failure_is_propagated_after_every_arm_is_closed(self) -> None:
        factory = ArmFactory()
        factory.cleanup_failure_label = "combined"
        with self.assertRaisesRegex(scenarios.ScenarioError, "combined cleanup failed"):
            self.run_mixed(factory)
        self.assertEqual(len(factory.arms), 3)
        self.assertTrue(all(arm.closed for arm in factory.arms))


    def test_query_only_control_starvation_fails_independently(self) -> None:
        factory = ArmFactory()

        def starve(evidence: dict) -> None:
            evidence["metrics"]["goodput_qps"] = 0.0
            evidence["started"] = 0
            evidence["completed"] = 0

        factory.mutations[("query_only", "query")] = starve
        result = self.run_mixed(factory)
        self.assertEqual(result.status, "fail")

    def test_ingest_only_control_starvation_fails_independently(self) -> None:
        factory = ArmFactory()

        def starve(evidence: dict) -> None:
            evidence["status"] = "fail"
            evidence["started"] = 0
            evidence["completed"] = 0
            evidence["lost_events"] = 2

        factory.mutations[("ingest_only", "ingest")] = starve
        result = self.run_mixed(factory)
        self.assertEqual(result.status, "fail")

    def test_right_censored_ingest_control_emits_failed_result(self) -> None:
        factory = ArmFactory()

        def censor(evidence: dict) -> None:
            evidence["status"] = "fail"
            for name in ("source_etd_p95", "db_ack_etd_p95"):
                evidence["metrics"][name] = {
                    "lower_ms": 5_000.0,
                    "upper_ms": None,
                    "censoring": "right",
                }
            for sample in evidence["samples"]:
                sample["valid"] = False

        factory.mutations[("ingest_only", "ingest")] = censor
        result = self.run_mixed(factory)
        self.assertEqual(result.status, "fail")
        self.assertFalse(result.metrics["mixed_gates"]["controls_valid"])
        self.assertIsNone(
            result.metrics["ingest_control"]["source_etd_p95"]["upper_ms"]
        )

    def test_severe_degradation_fails_even_while_absolute_slos_pass(self) -> None:
        factory = ArmFactory()

        def degrade_query(evidence: dict) -> None:
            evidence["metrics"]["goodput_qps"] = 80.0
            evidence["metrics"]["p95_ms"] = 120.0
            evidence["metrics"]["p99_ms"] = 180.0

        factory.mutations[("combined", "query")] = degrade_query
        result = self.run_mixed(factory)
        self.assertTrue(result.metrics["mixed_gates"]["query_slo"])
        self.assertFalse(result.metrics["mixed_gates"]["query_degradation"])
        self.assertEqual(result.status, "fail")

        factory = ArmFactory()

        def degrade_ingest(evidence: dict) -> None:
            evidence["metrics"]["source_etd_p95"]["upper_ms"] = 200.0
            evidence["metrics"]["db_ack_etd_p95"]["upper_ms"] = 200.0

        factory.mutations[("combined", "ingest")] = degrade_ingest
        result = self.run_mixed(factory)
        self.assertTrue(result.metrics["mixed_gates"]["ingest_slo"])
        self.assertFalse(result.metrics["mixed_gates"]["ingest_degradation"])
        self.assertEqual(result.status, "fail")

    def test_serialized_stream_evidence_fails_overlap_gate(self) -> None:
        factory = ArmFactory()

        def query_bounds(evidence: dict) -> None:
            evidence["first_started_ns"] = 100
            evidence["last_completed_ns"] = 150

        def ingest_bounds(evidence: dict) -> None:
            evidence["first_started_ns"] = 150
            evidence["last_completed_ns"] = 200

        factory.mutations[("combined", "query")] = query_bounds
        factory.mutations[("combined", "ingest")] = ingest_bounds
        result = self.run_mixed(factory)
        self.assertFalse(result.metrics["mixed_gates"]["overlap"])
        self.assertEqual(result.status, "fail")

    def test_schedule_slip_loss_duplication_and_backlog_each_fail(self) -> None:
        cases = []

        def slip(evidence: dict) -> None:
            evidence["scheduler_p99_slip_ms"] = 10.001

        cases.append(("query slip", ("combined", "query"), slip, {}))

        def lost(evidence: dict) -> None:
            evidence["lost_events"] = 1
            evidence["completed"] = 1

        cases.append(("lost", ("combined", "ingest"), lost, {}))

        def duplicate(evidence: dict) -> None:
            evidence["duplicate_events"] = 1

        cases.append(("duplicate", ("combined", "ingest"), duplicate, {}))
        cases.append(("backlog", None, None, {"combined": 1}))

        for name, key, mutation, depths in cases:
            with self.subTest(name=name):
                factory = ArmFactory()
                if key is not None:
                    factory.mutations[key] = mutation
                factory.queue_depths.update(depths)
                result = self.run_mixed(factory)
                self.assertEqual(result.status, "fail")

    def test_fresh_equivalent_physical_resets_are_mandatory(self) -> None:
        factory = ArmFactory()
        factory.same_reset = True
        with self.assertRaisesRegex(scenarios.ScenarioError, "reused"):
            self.run_mixed(factory)

        factory = ArmFactory()
        factory.recipe_mismatch = True
        with self.assertRaisesRegex(scenarios.ScenarioError, "logical recipe"):
            self.run_mixed(factory)


if __name__ == "__main__":
    unittest.main()
