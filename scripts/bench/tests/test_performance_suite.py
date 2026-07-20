#!/usr/bin/env python3
from __future__ import annotations

import copy
import json
import os
import re
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
from performance_runtime import LocalEnvelope, OwnedSandbox


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
    def test_prepare_builds_canonicalizes_relative_output_for_sandbox_mount(self) -> None:
        class StopAfterBuildPath(Exception):
            pass

        observed: dict[str, Path] = {}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repository = root / "candidate"
            repository.mkdir()
            absolute_output = root / "output"
            relative_output = Path(os.path.relpath(absolute_output, Path.cwd()))

            def stop_after_path(_repo, build_output, **_kwargs):
                observed["output"] = build_output
                raise StopAfterBuildPath

            with (
                mock.patch.object(suite, "_require_clean"),
                mock.patch.object(suite, "ensure_runtime_build_image"),
                mock.patch.object(
                    suite,
                    "build_release_binaries_in_docker",
                    side_effect=stop_after_path,
                ),
            ):
                with self.assertRaises(StopAfterBuildPath):
                    suite._prepare_builds(
                        {"candidate": repository},
                        relative_output,
                        authoritative=False,
                    )

        self.assertTrue(observed["output"].is_absolute())
        self.assertEqual(observed["output"], absolute_output.resolve() / "candidate")

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

    def test_seed_reconciles_projection_before_measurement(self) -> None:
        recipe = build_recipe("smoke")
        expected = str(recipe["corpus"]["document_count"])
        search_counts = iter(("0", expected))
        queries: list[str] = []

        def query(_url, sql, **_kwargs):
            queries.append(sql)
            if sql == "SELECT count() FROM moraine.search_documents":
                return next(search_counts)
            if "FROM system.tables" in sql:
                return "1"
            if "mcp_open_projection_state" in sql:
                return "1"
            if "countIf(dirty.dirty_revision" in sql:
                return "0"
            return ""

        sandbox = mock.Mock(
            clickhouse_port=8123,
            sandbox_id="sb-123abc",
        )
        with mock.patch.object(suite, "_clickhouse_query", side_effect=query):
            suite._seed_owned_sandbox(sandbox, recipe)

        sandbox.reconcile_seeded_read_model.assert_called_once_with()
        sandbox.checkpoint.assert_called_once_with("seeded")
        self.assertTrue(any("mcp_open_projection_state" in sql for sql in queries))
        self.assertTrue(any("countIf(dirty.dirty_revision" in sql for sql in queries))

    def test_owned_sandbox_reconciles_with_frozen_binary(self) -> None:
        sandbox = OwnedSandbox(
            Path("/repo"),
            Path("/sandbox-tool"),
            "sb-123abc",
            "project",
            8873,
            8123,
            Path("/tmp/sandbox-config"),
            "sha256:" + "a" * 64,
            mock.Mock(),
        )
        process = mock.Mock(returncode=0, stderr="")
        with mock.patch("performance_runtime._run", return_value=process) as run:
            sandbox.reconcile_seeded_read_model()

        run.assert_called_once_with(
            [
                "/sandbox-tool",
                "exec-loadgen",
                "sb-123abc",
                "--cwd",
                "/home/moraine",
                "--",
                "/opt/moraine/bin/moraine",
                "db",
                "migrate",
                "--config",
                "/sandbox/moraine.toml",
            ],
            timeout=600,
        )


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

    def test_local_comparison_aggregates_baseline_and_candidate_arms(self) -> None:
        definition = {"suite": "local-comparison-test"}
        suite_identity = sha256_json(definition)
        identities = {
            arm: digest(f"{arm}-build") for arm in ("baseline", "candidate")
        }
        prepared = {
            arm: PreparedBuild(
                mock.Mock(),
                {"identity_sha256": identities[arm]},
            )
            for arm in ("baseline", "candidate")
        }
        metrics = {
            "baseline": {
                "qps": {"sustainable_qps": 100.0},
                "ttr": {"p95_ms": 50.0},
                "etd_loaded": {
                    "source_etd_p95": {"lower_ms": 90.0, "upper_ms": 110.0}
                },
                "mixed": {"ratios": {"query_goodput": 1.0}},
            },
            "candidate": {
                "qps": {"sustainable_qps": 125.0},
                "ttr": {"p95_ms": 40.0},
                "etd_loaded": {
                    "source_etd_p95": {"lower_ms": 70.0, "upper_ms": 90.0}
                },
                "mixed": {"ratios": {"query_goodput": 1.1}},
            },
        }
        documents = {}

        def run_arm(_repo, _prepared, _recipe, _definition, **kwargs):
            arm = kwargs["arm"]
            paths = []
            for scenario, scenario_metrics in metrics[arm].items():
                path = kwargs["output"] / f"{scenario}-research.json"
                documents[path] = {
                    "scenario": scenario,
                    "metrics": scenario_metrics,
                    "suite_definition_sha256": suite_identity,
                    "binary": {"build_identity_sha256": identities[arm]},
                    "semantic": {"oracle_sha256": digest(f"{arm}:{scenario}")},
                }
                paths.append(path)
            return paths, float(metrics[arm]["qps"]["sustainable_qps"])

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repositories = {
                "baseline": root / "baseline",
                "candidate": root / "candidate",
            }
            with (
                mock.patch.object(
                    suite,
                    "_prepare_builds",
                    return_value=(prepared, {}),
                ),
                mock.patch.object(suite, "_definition", return_value=definition),
                mock.patch.object(suite, "_run_logical_arm", side_effect=run_arm),
                mock.patch.object(
                    suite,
                    "load_document",
                    side_effect=documents.__getitem__,
                ),
                mock.patch.object(
                    suite,
                    "_local_docker_platform",
                    return_value="test-platform",
                ),
            ):
                result = suite.run_local_comparison(
                    repositories,
                    profile="smoke",
                    pairs=1,
                    output=root / "output",
                )
            summary = json.loads(result.read_text(encoding="utf-8"))

        pair = summary["pair_results"][0]
        self.assertEqual(pair["baseline"]["qps"], 100.0)
        self.assertEqual(pair["candidate"]["qps"], 125.0)
        self.assertEqual(pair["baseline"]["source_etd_p95_midpoint_ms"], 100.0)
        self.assertEqual(pair["candidate"]["source_etd_p95_midpoint_ms"], 80.0)
        self.assertEqual(
            pair["ratios"],
            {"qps": 1.25, "ttr": 1.25, "source_etd": 1.25},
        )

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

    def test_validate_path_accepts_local_comparison(self) -> None:
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
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "local-comparison.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            validate_path(path)

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

    def test_publication_capture_scale_seed_is_insert_only_and_exact(self) -> None:
        statements = []
        snapshots = iter(("1\t1", "10000\t10000"))

        def query(_url, sql, **_kwargs):
            statements.append(sql)
            if "v_current_published_source_generations" in sql:
                return next(snapshots)
            return ""

        with mock.patch.object(suite, "_clickhouse_query", side_effect=query):
            observed = suite._seed_publication_heads_to(
                "http://127.0.0.1:8123",
                current_count=1,
                target_count=10_000,
            )

        self.assertEqual(
            observed, {"head_count": 10_000, "max_publication_revision": 10_000}
        )
        insert = next(sql for sql in statements if sql.startswith("INSERT INTO"))
        self.assertIn("FROM numbers(9999)", insert)
        self.assertNotRegex(insert, r"(?i)\b(?:DELETE|TRUNCATE|DROP)\b")
        capture = suite._publication_capture_sql()
        self.assertEqual(
            capture,
            """/* moraine:publication_snapshot:capture */
SELECT
  toUInt64(ifNull(max(publication_revision), 0)) AS publication_revision
FROM `moraine`.`published_source_generations`
FORMAT JSONEachRow""",
        )
        self.assertNotIn("v_current_published_source_generations", capture)
        self.assertNotIn("head_fingerprint", capture)
        self.assertNotIn("head_count", capture)

    def test_local_publication_capture_result_has_only_the_revision_token(self) -> None:
        suite._validate_publication_capture_result(
            '{"publication_revision":"42"}', publication_revision=42
        )
        with self.assertRaisesRegex(suite.SuiteFailure, "capture result is malformed"):
            suite._validate_publication_capture_result(
                '{"publication_revision":"42","head_count":"1"}',
                publication_revision=42,
            )

    def test_publication_capture_query_log_requires_exact_finished_rows(self) -> None:
        rows = "\n".join(
            json.dumps(
                {
                    "query_id": query_id,
                    "type": "QueryFinish",
                    "query_duration_ms": index + 0.5,
                    "read_rows": "10000",
                    "read_bytes": "640000",
                    "result_rows": "1",
                    "memory_usage_bytes": "8192",
                }
            )
            for index, query_id in enumerate(("capture-a", "capture-b"))
        )
        with mock.patch.object(suite, "_clickhouse_query", side_effect=("", rows)):
            samples = suite._publication_capture_query_log_samples(
                "http://127.0.0.1:8123", ("capture-a", "capture-b")
            )

        self.assertEqual(len(samples), 2)
        self.assertEqual(samples[0]["read_rows"], 10_000)
        summary = suite._publication_capture_query_log_summary(samples)
        self.assertEqual(summary["read_rows_total"], 20_000)
        self.assertEqual(summary["max_memory_bytes"], 8192)

    def test_publication_capture_storage_allows_physical_history_rows(self) -> None:
        self.assertIsNotNone(
            re.search(
                suite.PUBLICATION_CONTROL_TABLE_PATTERN,
                "published_source_generations",
            )
        )
        suite._validate_publication_capture_storage(
            {"published_source_generations": {"rows": 3}},
            logical_head_count=2,
        )
        with self.assertRaisesRegex(
            suite.SuiteFailure, "fewer than logical head count"
        ):
            suite._validate_publication_capture_storage(
                {"published_source_generations": {"rows": 1}},
                logical_head_count=2,
            )

    def test_source_publication_probe_validates_samples_head_and_control_deltas(self) -> None:
        before_controls = {
            "published_source_generations": {
                "rows": 2,
                "active_parts": 1,
                "compressed_bytes": 128,
            }
        }
        after_controls = {
            "published_source_generations": {
                "rows": 2,
                "active_parts": 1,
                "compressed_bytes": 128,
            },
            "ingest_checkpoints": {
                "rows": 100,
                "active_parts": 4,
                "compressed_bytes": 4096,
            },
        }
        raw = [float(value) for value in range(100)]
        source_host_before = {
            table: {
                "rows": 0,
                "active_parts": 0,
                "compressed_bytes": 0,
                "uncompressed_bytes": 0,
                "bytes_on_disk": 0,
            }
            for table in suite.SOURCE_HOST_PHYSICAL_TABLES
        }
        source_host_after = copy.deepcopy(source_host_before)
        source_host_after["events"] = {
            "rows": 101,
            "active_parts": 2,
            "compressed_bytes": 256,
            "uncompressed_bytes": 808,
            "bytes_on_disk": 512,
        }
        scaling_points = []
        for head_count in suite.PUBLICATION_CAPTURE_HEAD_COUNTS:
            query_samples = [
                {
                    "query_duration_ms": float(value),
                    "read_rows": head_count,
                    "read_bytes": head_count * 64,
                    "result_rows": 1,
                    "memory_usage_bytes": head_count * 8,
                }
                for value in range(suite.PUBLICATION_CAPTURE_REPETITIONS)
            ]
            scaling_points.append(
                {
                    "head_count": head_count,
                    "max_publication_revision": head_count,
                    "client_latency": suite._latency_summary(
                        [
                            float(value)
                            for value in range(
                                suite.PUBLICATION_CAPTURE_REPETITIONS
                            )
                        ]
                    ),
                    "query_log": suite._publication_capture_query_log_summary(
                        query_samples
                    ),
                    "control_tables": {
                        "published_source_generations": {
                            "rows": head_count + 1,
                            "active_parts": 1,
                            "compressed_bytes": head_count * 32,
                        }
                    },
                }
            )
        document = {
            "document_type": "source_publication_append_probe",
            "schema_version": "moraine.source-publication-probe.v1",
            "status": "pass",
            "git_commit": "a" * 40,
            "run": {
                "authoritative": False,
                "minimum_samples": 100,
                "p95_limit_ms": 2_000.0,
                "timeout_seconds": 5.0,
            },
            "append": {
                "sample_count": 100,
                "fsync_to_live_p50_ms": 49.0,
                "fsync_to_live_p95_ms": 94.0,
                "fsync_to_live_max_ms": 99.0,
                "raw_fsync_to_live_ms": raw,
            },
            "publication_head": {
                "before": {"row_count": 2, "max_publication_revision": 2},
                "after": {"row_count": 2, "max_publication_revision": 2},
                "head_write_count": 0,
                "publication_revision_unchanged": True,
            },
            "control_tables": {
                "before": before_controls,
                "after": after_controls,
                "delta": suite._control_resource_delta(
                    before_controls, after_controls
                ),
            },
            "control_capture_scaling": {
                "publication_mode": "local",
                "capture_query": "raw_history_max_publication_revision",
                "head_counts": list(suite.PUBLICATION_CAPTURE_HEAD_COUNTS),
                "warmup_repetitions": suite.PUBLICATION_CAPTURE_WARMUP_REPETITIONS,
                "measured_repetitions": suite.PUBLICATION_CAPTURE_REPETITIONS,
                "points": scaling_points,
            },
            "source_host_columns": {
                "before": source_host_before,
                "after": source_host_after,
                "delta": suite._source_host_column_resource_delta(
                    source_host_before, source_host_after
                ),
            },
            "resources": suite.non_authoritative_resource_evidence(),
            "artifact_sha256": "",
        }
        document["artifact_sha256"] = sha256_json(
            {
                key: value
                for key, value in document.items()
                if key != "artifact_sha256"
            }
        )

        suite.validate_source_publication_probe(document)
        args = suite._parse_args(
            [
                "source-publication-append-probe",
                "--repo",
                ".",
                "--output",
                "target/publication-probe",
            ]
        )
        self.assertEqual(args.samples, 100)
        self.assertEqual(args.p95_limit_ms, 2_000.0)

        changed = copy.deepcopy(document)
        changed["publication_head"]["head_write_count"] = 1
        with self.assertRaisesRegex(suite.SuiteFailure, "control evidence"):
            suite.validate_source_publication_probe(changed)

        changed = copy.deepcopy(document)
        changed["resources"]["memory_peak_bytes"] = -1
        with self.assertRaisesRegex(suite.SuiteFailure, "memory_peak_bytes"):
            suite.validate_source_publication_probe(changed)


if __name__ == "__main__":
    unittest.main()
