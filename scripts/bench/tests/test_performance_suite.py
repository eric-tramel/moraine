#!/usr/bin/env python3
from __future__ import annotations

import tempfile
import unittest
from unittest import mock
import sys
from pathlib import Path
BENCH = Path(__file__).resolve().parents[1]
if str(BENCH) not in sys.path:
    sys.path.insert(0, str(BENCH))


import performance_suite as suite
from performance_fixtures import build_recipe
from performance_protocol import create_build_identity, create_build_recipe, sha256_json
from performance_scenarios import ScenarioResult
from performance_suite import (
    EvidenceCollector,
    PreparedBuild,
    _definition,
    _ordered_usage,
    _physical_reset_sha256,
    _schedule_evidence,
    _schedule_hashes,
    _semantic_evidence,
    freeze,
    validate_path,
)


def digest(label: str) -> str:
    return sha256_json({"label": label})


def protocol_build(arm: str = "baseline") -> dict:
    return create_build_identity(
        arm=arm,
        git_commit=("a" if arm == "baseline" else "b") * 40,
        image_digest=digest(f"image:{arm}"),
        build_environment_sha256=digest("environment"),
        binaries=[
            {"role": "moraine", "sha256": digest(f"{arm}:moraine")},
            {"role": "moraine-mcp", "sha256": digest(f"{arm}:mcp")},
        ],
    )


def build_recipe_document() -> dict:
    return create_build_recipe(
        toolchain_sha256=digest("toolchain"),
        target="x86_64-unknown-linux-gnu",
        linker="cc",
        environment_allowlist=[],
        build_environment_sha256=digest("environment"),
        image_recipe_sha256=digest("image-recipe"),
    )


def resource(identity: str, *, cpu: int = 10, peak: int = 20) -> dict:
    return {
        "authoritative": True,
        "cgroup_version": 2,
        "cgroup_driver": "cgroupfs",
        "cgroup_identity_sha256": digest(f"cgroup:{identity}"),
        "role_membership_sha256": digest(f"roles:{identity}"),
        "controllers_enabled_proven": True,
        "effective_limits_proven": True,
        "host_headroom_proven": True,
        "cpuset_cpus_effective": "0-3",
        "cpu_max_quota_us": 100_000,
        "cpu_max_period_us": 100_000,
        "cpu_usage_usec_delta": cpu,
        "cpu_nr_throttled_delta": 1,
        "throttled_usec_delta": 2,
        "memory_max_bytes": 8 * 1024**3,
        "memory_current_bytes": peak - 1,
        "memory_peak_bytes": peak,
        "memory_event_high_delta": 0,
        "memory_event_max_delta": 0,
        "swap_max_bytes": 0,
        "swap_current_bytes": 0,
        "oom_kill_delta": 0,
        "server_descendants_proven": True,
        "loadgen_excluded_proven": True,
    }


class DefinitionTests(unittest.TestCase):
    def test_actual_fixture_shape_builds_protocol_definition(self) -> None:
        recipe = build_recipe("smoke")
        build = protocol_build()
        prepared = {"baseline": PreparedBuild(None, build)}  # type: ignore[arg-type]
        definition = _definition("smoke", recipe, prepared, build_recipe_document())
        self.assertEqual(definition["fixture"]["seed"], recipe["seed"]["value"])
        self.assertEqual(set(definition["schedules"]), {
            f"{scenario}:{split}"
            for scenario, splits in recipe["split_matrix"].items()
            for split in splits
        })

    def test_schedule_hashes_are_fixture_sensitive_and_complete(self) -> None:
        smoke = _schedule_hashes(build_recipe("smoke"))
        full = _schedule_hashes(build_recipe("full"))
        self.assertEqual(len(smoke), 9)
        self.assertEqual(set(smoke), set(full))
        self.assertNotEqual(smoke, full)

    def test_comparison_usage_runs_all_research_before_holdout(self) -> None:
        recipe = build_recipe("full")
        usage = _ordered_usage(recipe, "comparison")
        self.assertEqual(usage[:4], tuple((name, "research") for name in ("qps", "ttr", "etd_idle", "etd_loaded")))
        self.assertEqual(usage[4:8], tuple((name, "holdout") for name in ("qps", "ttr", "etd_idle", "etd_loaded")))
        self.assertEqual(usage[-1], ("mixed", "stress"))


class EvidenceTests(unittest.TestCase):
    def test_resource_aggregation_preserves_one_shared_envelope_contract(self) -> None:
        collector = EvidenceCollector(resources=[resource("a", cpu=10, peak=100), resource("b", cpu=20, peak=200)])
        aggregate = collector.resource_artifact(authoritative=True)
        self.assertEqual(aggregate["cpu_usage_usec_delta"], 30)
        self.assertEqual(aggregate["memory_peak_bytes"], 200)
        self.assertEqual(aggregate["memory_event_max_delta"], 0)
        self.assertNotEqual(aggregate["cgroup_identity_sha256"], collector.resources[0]["cgroup_identity_sha256"])

    def test_binary_cache_and_reset_evidence_publish_only_hashes(self) -> None:
        build = protocol_build()
        collector = EvidenceCollector()
        for item in build["binaries"]:
            collector.record_binary(item["role"], item["sha256"])
        collector.cache_generations.extend(["generation-a", "generation-b"])
        collector.record_reset("scenario", "sb-private")
        binary = collector.binary_artifact(build)
        cache = collector.cache_artifact(build_recipe("smoke"), "ttr", "research")
        self.assertTrue(all(item["verified"] for item in binary["running_binaries"]))
        self.assertNotIn("generation-a", str(cache))
        self.assertEqual(collector.physical_resets["scenario"], _physical_reset_sha256("sb-private"))

    def test_qps_semantic_evidence_does_not_treat_expected_overload_as_wrong_answer(self) -> None:
        outcomes = {
            "planned": 2,
            "started": 2,
            "correct": 1,
            "rejected": 1,
            "timed_out": 0,
            "semantic_error": 0,
            "protocol_error": 0,
            "malformed": 0,
            "late": 0,
            "dropped": 0,
            "other_error": 0,
        }
        sample = {
            "outcomes": outcomes,
            "scheduler_p99_start_slip_ms": 1.0,
            "drained": True,
            "drain_ms": 1.0,
            "reset_sha256": digest("reset"),
        }
        result = ScenarioResult("pass", {"capacity_censoring": "none"}, (sample,))
        semantic = _semantic_evidence(result, "qps", "research", build_recipe("smoke"))
        self.assertTrue(semantic["passed"])
        self.assertEqual(semantic["expected_count"], 1)
        self.assertEqual(semantic["observed_count"], 1)

    def test_qps_schedule_binds_exact_sample_reset(self) -> None:
        recipe = build_recipe("smoke")
        build = protocol_build()
        definition = _definition(
            "smoke",
            recipe,
            {"baseline": PreparedBuild(None, build)},  # type: ignore[arg-type]
            build_recipe_document(),
        )
        reset = digest("trial")
        sample = {
            "outcomes": {
                "planned": 1,
                "started": 1,
                "correct": 1,
                "rejected": 0,
                "timed_out": 0,
                "semantic_error": 0,
                "protocol_error": 0,
                "malformed": 0,
                "late": 0,
                "dropped": 0,
                "other_error": 0,
            },
            "scheduler_p99_start_slip_ms": 1.0,
            "drained": True,
            "drain_ms": 1.0,
            "reset_sha256": reset,
        }
        result = ScenarioResult("pass", {"capacity_censoring": "none"}, (sample,))
        schedule = _schedule_evidence(result, "qps", "research", definition, recipe, EvidenceCollector())
        self.assertEqual(schedule["physical_resets"], [{"role": "trial", "reset_sha256": reset}])


class CliArtifactTests(unittest.TestCase):
    def test_freeze_writes_canonical_fixture_accepted_by_validate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "frozen"
            freeze("smoke", root)
            validate_path(root / "fixture-smoke.json")
            self.assertTrue((root / "policy-smoke.json").is_file())

    def test_baseline_runner_builds_fixture_before_preparing_binaries(self) -> None:
        class StopAfterFixture(Exception):
            pass

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with mock.patch.object(
                suite,
                "_prepare_builds",
                side_effect=StopAfterFixture,
            ) as prepare:
                with self.assertRaises(StopAfterFixture):
                    suite.run_baseline(
                        {"baseline": root},
                        "smoke",
                        root / "results",
                    )
            prepare.assert_called_once()

    def test_validate_rejects_untyped_json(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "unknown.json"
            path.write_text("{}\n")
            with self.assertRaisesRegex(Exception, "neither a protocol document nor a fixture recipe"):
                validate_path(path)


if __name__ == "__main__":
    unittest.main()
