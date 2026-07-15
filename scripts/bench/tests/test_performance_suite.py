#!/usr/bin/env python3
from __future__ import annotations

import copy
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
from performance_runtime import LocalEnvelope


def digest(label: str) -> str:
    return sha256_json({"label": label})


def autoresearch_artifacts(pair_results: list[dict]) -> dict[str, dict]:
    artifacts: dict[str, dict] = {}
    for pair_index, pair in enumerate(pair_results, 1):
        candidate = pair["candidate"]
        common = {
            "document_type": "scenario_result",
            "split": "research",
            "status": "inconclusive",
            "run": {
                "arm": "candidate",
                "pair_id": pair_index,
                "authoritative": False,
            },
            "gates": {
                "correctness": True,
                "schedule": True,
                "scenario": True,
            },
        }
        for scenario, metrics in (
            (
                "qps",
                {
                    "capacity_censoring": "none",
                    "sustainable_qps": candidate["qps"],
                },
            ),
            ("ttr", {"p95_ms": candidate["ttr_p95_ms"]}),
            (
                "etd_loaded",
                {
                    "source_etd_p95": {
                        "lower_ms": candidate["source_etd_p95_midpoint_ms"],
                        "upper_ms": candidate["source_etd_p95_midpoint_ms"],
                    }
                },
            ),
        ):
            artifacts[f"candidate/pair-{pair_index}/artifacts/{scenario}-research.json"] = {
                **common,
                "scenario": scenario,
                "metrics": metrics,
            }
    return artifacts


def bind_autoresearch_artifacts(document: dict, artifacts: dict[str, dict]) -> None:
    suite_identity = digest("local-suite")
    candidate_identity = digest("candidate-build")
    document["suite_definition_sha256"] = suite_identity
    document["builds"] = {"candidate": candidate_identity}
    document["artifact_bindings"] = {}
    for path, artifact in artifacts.items():
        oracle_identity = digest(f"oracle:{path}")
        artifact["suite_definition_sha256"] = suite_identity
        artifact["binary"] = {"build_identity_sha256": candidate_identity}
        artifact["semantic"] = {"oracle_sha256": oracle_identity}
        document["artifact_bindings"][path] = {
            "suite_definition_sha256": suite_identity,
            "build_identity_sha256": candidate_identity,
            "semantic_oracle_sha256": oracle_identity,
        }


def protocol_build(arm: str = "baseline") -> dict:
    return create_build_identity(
        arm=arm,
        git_commit=("a" if arm == "baseline" else "b") * 40,
        image_digest=digest(f"image:{arm}"),
        build_environment_sha256=digest("environment"),
        binaries=[
            {"role": "moraine-ingest", "sha256": digest(f"{arm}:ingest")},
            {"role": "moraine-mcp", "sha256": digest(f"{arm}:mcp")},
        ],
    )


def build_recipe_document() -> dict:
    return create_build_recipe(
        toolchain_sha256=digest("toolchain"),
        target="x86_64-unknown-linux-gnu",
        linker_sha256=digest("linker"),
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

    def test_qps_scenario_gate_is_independent_from_schedule_failure(self) -> None:
        result = ScenarioResult("fail", {}, (), 0)
        self.assertTrue(suite._scenario_pass(result, "qps"))

    def test_local_qps_goodput_calibrates_follow_on_load_without_passing_gate(self) -> None:
        result = ScenarioResult(
            "fail",
            {"sustainable_qps": 0},
            ({"achieved_goodput_qps": 16.0},),
            0,
        )
        self.assertEqual(
            suite._qps_capacity_for_follow_on_load(result, authoritative=False),
            16.0,
        )
        self.assertEqual(
            suite._qps_capacity_for_follow_on_load(result, authoritative=True),
            0.0,
        )

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

    def test_local_resource_evidence_is_explicitly_non_authoritative(self) -> None:
        snapshot = LocalEnvelope("perf-0123456789ab").reset_measurement((), ())
        resources = snapshot.artifact(snapshot)
        self.assertFalse(resources["authoritative"])
        self.assertFalse(resources["effective_limits_proven"])

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
        schedule = _schedule_evidence(
            result,
            "qps",
            "research",
            definition,
            recipe,
            EvidenceCollector(),
            expanded_schedule={"scenario": "qps", "split": "research"},
        )
        self.assertEqual(schedule["physical_resets"], [{"role": "trial", "reset_sha256": reset}])


class LifecycleTests(unittest.TestCase):
    def test_setup_failure_reports_each_owned_cleanup_failure(self) -> None:
        envelope = mock.Mock()
        envelope.owned_id = "perf-0123456789abcdef"
        envelope.create.return_value = "owned-parent"
        envelope.remove.side_effect = OSError("cgroup remains busy")
        sandbox = mock.Mock()
        sandbox.sandbox_id = "sb-123456"
        sandbox.down.side_effect = OSError("compose down failed")
        sandbox.status.return_value.image_ids = {"moraine": "sha256:runtime"}

        with (
            mock.patch.object(suite, "FixedEnvelope", return_value=envelope),
            mock.patch.object(suite, "start_owned_sandbox", return_value=sandbox),
            mock.patch.object(
                suite,
                "_seed_owned_sandbox",
                side_effect=OSError("seed failed"),
            ),
        ):
            with self.assertRaisesRegex(
                suite.SuiteFailure,
                "sandbox sb-123456 cleanup failed.*"
                "cgroup perf-0123456789abcdef cleanup failed",
            ):
                suite._start_measured_sandbox(
                    Path("."),
                    mock.Mock(),
                    sha256_json({"moraine": "sha256:runtime"}),
                    {},
                    EvidenceCollector(),
                    reset_role="trial",
                )
        sandbox.down.assert_called_once()
        envelope.remove.assert_called_once()


    def test_local_envelope_checks_docker_without_claiming_cgroup_control(self) -> None:
        process = mock.Mock(returncode=0, stderr="")
        with mock.patch("performance_runtime._run", return_value=process) as run:
            envelope = LocalEnvelope("perf-0123456789ab")
            self.assertEqual(envelope.create(), "local-comparative")
            evidence = envelope.inspect((), ()).artifact(envelope.reset_measurement((), ()))
        run.assert_called_once_with(["docker", "info"], timeout=30)
        self.assertFalse(evidence["authoritative"])
        self.assertFalse(evidence["effective_limits_proven"])

    def test_build_image_is_prebuilt_once_and_both_arms_use_it(self) -> None:
        runtime_build = mock.Mock(
            toolchain_sha256=digest("toolchain"),
            target="x86_64-unknown-linux-gnu",
            build_environment={},
            binary_sha256={
                "moraine-ingest": digest("ingest"),
                "moraine-mcp": digest("mcp"),
            },
        )
        runtime_build.artifact.return_value = {
            "recipe": {
                "build_environment_sha256": digest("environment"),
                "linker_sha256": digest("linker"),
            }
        }
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repositories = {
                "baseline": root / "baseline",
                "candidate": root / "candidate",
            }
            for repository in repositories.values():
                repository.mkdir()
            with (
                mock.patch.object(suite, "_require_clean"),
                mock.patch.object(suite, "ensure_runtime_build_image") as ensure,
                mock.patch.object(
                    suite,
                    "build_release_binaries_in_docker",
                    return_value=runtime_build,
                ) as build,
                mock.patch.object(
                    suite,
                    "_discover_image_digest",
                    return_value=digest("runtime-image"),
                ),
                mock.patch.object(
                    suite,
                    "_git_commit",
                    side_effect=("a" * 40, "b" * 40),
                ),
            ):
                prepared, _ = suite._prepare_builds(
                    repositories,
                    root / "output",
                    authoritative=True,
                )
        ensure.assert_called_once_with(suite.SUITE_ROOT)
        self.assertEqual(build.call_count, 2)
        self.assertEqual(set(prepared), {"baseline", "candidate"})


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
            self.assertFalse(prepare.call_args.kwargs["authoritative"])

    def test_local_cli_exposes_practical_defaults(self) -> None:
        args = suite._parse_args(
            [
                "run",
                "--mode",
                "local",
                "--baseline",
                "/baseline",
                "--candidate",
                "/candidate",
                "--output",
                "/output",
            ]
        )
        self.assertEqual(args.mode, "local")
        self.assertIsNone(args.profile)
        self.assertIsNone(args.pairs)

    def test_local_comparison_validator_requires_non_authoritative_relative_evidence(
        self,
    ) -> None:
        artifact_path = "candidate/pair-1/artifacts/qps-research.json"
        suite_identity = digest("local-suite")
        candidate_identity = digest("candidate-build")
        document = {
            "schema_version": "moraine-local-comparison-v1",
            "mode": "local_comparative",
            "authoritative": False,
            "pairs": 1,
            "pair_results": [{}],
            "suite_definition_sha256": suite_identity,
            "builds": {"candidate": candidate_identity},
            "artifacts": [artifact_path],
            "artifact_bindings": {
                artifact_path: {
                    "suite_definition_sha256": suite_identity,
                    "build_identity_sha256": candidate_identity,
                    "semantic_oracle_sha256": digest("oracle"),
                }
            },
        }
        suite._validate_local_comparison(document)
        document["authoritative"] = True
        with self.assertRaisesRegex(suite.SuiteFailure, "non-authoritative"):
            suite._validate_local_comparison(document)

    def test_autoresearch_metrics_use_candidate_end_to_end_evidence(self) -> None:
        pair_results = [
            {
                "pair_id": 1,
                "candidate": {
                    "qps": 100.0,
                    "ttr_p95_ms": 25.0,
                    "source_etd_p95_midpoint_ms": 80.0,
                },
            },
            {
                "pair_id": 2,
                "candidate": {
                    "qps": 400.0,
                    "ttr_p95_ms": 100.0,
                    "source_etd_p95_midpoint_ms": 320.0,
                },
            },
        ]
        document = {
            "schema_version": "moraine-local-comparison-v1",
            "mode": "local_comparative",
            "authoritative": False,
            "pairs": 2,
            "pair_results": pair_results,
            "artifacts": [
                f"candidate/pair-{pair}/artifacts/{scenario}-research.json"
                for pair in (1, 2)
                for scenario in ("qps", "ttr", "etd_loaded")
            ],
        }
        artifacts = autoresearch_artifacts(pair_results)
        bind_autoresearch_artifacts(document, artifacts)
        metrics = dict(
            suite.autoresearch_metrics(document, artifact_loader=artifacts.__getitem__)
        )
        self.assertEqual(metrics["retrieval_operational_ns_per_query"], 5_000_000)
        self.assertAlmostEqual(metrics["retrieval_sustainable_qps"], 200.0)
        self.assertAlmostEqual(metrics["retrieval_ttr_p95_ms"], 50.0)
        self.assertAlmostEqual(
            metrics["retrieval_loaded_etd_p95_midpoint_ms"],
            160.0,
        )
        artifact_path = "candidate/pair-1/artifacts/qps-research.json"
        for section, field in (
            (None, "suite_definition_sha256"),
            ("binary", "build_identity_sha256"),
            ("semantic", "oracle_sha256"),
        ):
            with self.subTest(identity=field):
                tampered = copy.deepcopy(artifacts)
                target = tampered[artifact_path]
                if section is None:
                    target[field] = digest(f"wrong:{field}")
                else:
                    target[section][field] = digest(f"wrong:{field}")
                with self.assertRaisesRegex(suite.SuiteFailure, "did not pass"):
                    suite.autoresearch_metrics(
                        document,
                        artifact_loader=tampered.__getitem__,
                    )

    def test_autoresearch_metrics_fail_closed_on_zero_candidate_capacity(self) -> None:
        pair_results = [
            {
                "pair_id": 1,
                "candidate": {
                    "qps": 0.0,
                    "ttr_p95_ms": 25.0,
                    "source_etd_p95_midpoint_ms": 80.0,
                },
            }
        ]
        document = {
            "schema_version": "moraine-local-comparison-v1",
            "mode": "local_comparative",
            "authoritative": False,
            "pairs": 1,
            "pair_results": pair_results,
            "artifacts": [
                f"candidate/pair-1/artifacts/{scenario}-research.json"
                for scenario in ("qps", "ttr", "etd_loaded")
            ],
        }
        artifacts = autoresearch_artifacts(pair_results)
        bind_autoresearch_artifacts(document, artifacts)
        with self.assertRaisesRegex(suite.SuiteFailure, "finite and positive"):
            suite.autoresearch_metrics(
                document,
                artifact_loader=artifacts.__getitem__,
            )

    def test_validate_rejects_untyped_json(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "unknown.json"
            path.write_text("{}\n")
            with self.assertRaisesRegex(
                Exception,
                "neither a protocol document nor a fixture recipe",
            ):
                validate_path(path)


if __name__ == "__main__":
    unittest.main()
