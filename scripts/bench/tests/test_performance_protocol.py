from __future__ import annotations

import copy
import json
import math
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

BENCH_DIR = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).resolve().parent / "fixtures"
sys.path.insert(0, str(BENCH_DIR))

import performance_protocol as protocol


def digest(label: str) -> str:
    return protocol.sha256_json(label)


def fixture_identity(profile: str = "full") -> dict:
    names = (
        "corpus_sha256", "query_research_sha256", "query_holdout_sha256",
        "query_stress_sha256", "event_research_sha256", "event_holdout_sha256",
        "event_stress_sha256", "split_matrix_sha256", "schedule_templates_sha256",
    )
    return {
        "recipe_version": "fixed-search-v1",
        "profile": profile,
        "seed": {"algorithm": "closed-form", "value": 480},
        "fixture_sha256": digest(f"fixture:{profile}"),
        "fingerprints": {name: digest(f"{profile}:{name}") for name in names},
        "split_matrix": {name: list(splits) for name, splits in protocol.COMPARISON_MATRIX.items()},
    }


def make_definition(profile: str = "full", *, both_arms: bool = True) -> tuple[dict, dict[str, dict]]:
    build_environment = digest("build-environment")
    recipe = protocol.create_build_recipe(
        toolchain_sha256=digest("rust-toolchain"),
        target="x86_64-unknown-linux-gnu",
        linker_sha256=digest("linker"),
        environment_allowlist=["CARGO_HOME", "RUSTFLAGS", "SOURCE_DATE_EPOCH"],
        build_environment_sha256=build_environment,
        image_recipe_sha256=digest("image-recipe"),
    )
    builds: dict[str, dict] = {}
    for arm in (("baseline", "candidate") if both_arms else ("baseline",)):
        builds[arm] = protocol.create_build_identity(
            arm=arm,
            git_commit=("a" if arm == "baseline" else "b") * 40,
            image_digest=digest(f"image:{arm}"),
            build_environment_sha256=build_environment,
            binaries=[
                {
                    "role": role,
                    "sha256": digest(f"binary:{arm}:{role}"),
                }
                for role in sorted(protocol.MEASURED_BINARY_ROLES)
            ],
        )
    schedules = {
        f"{scenario}:{split}": digest(f"schedule:{profile}:{scenario}:{split}")
        for scenario, splits in protocol.COMPARISON_MATRIX.items()
        for split in splits
    }
    definition = protocol.create_suite_definition(
        profile=profile,
        fixture=fixture_identity(profile),
        build_recipe=recipe,
        builds=list(builds.values()),
        schedules=schedules,
    )
    return definition, builds


def resources(authoritative: bool = True) -> dict:
    return {
        "authoritative": authoritative,
        "cgroup_version": 2 if authoritative else 1,
        "cgroup_driver": "systemd" if authoritative else "unavailable",
        "cgroup_identity_sha256": digest("cgroup"),
        "role_membership_sha256": digest("roles"),
        "controllers_enabled_proven": authoritative,
        "effective_limits_proven": authoritative,
        "host_headroom_proven": authoritative,
        "cpuset_cpus_effective": "0-3",
        "cpu_max_quota_us": 100_000,
        "cpu_max_period_us": 100_000,
        "cpu_usage_usec_delta": 1_000,
        "cpu_nr_throttled_delta": 1,
        "throttled_usec_delta": 10,
        "memory_max_bytes": 8 * 1024**3,
        "memory_current_bytes": 1024,
        "memory_peak_bytes": 2048,
        "memory_event_high_delta": 0,
        "memory_event_max_delta": 0,
        "swap_max_bytes": 0,
        "swap_current_bytes": 0,
        "oom_kill_delta": 0,
        "server_descendants_proven": authoritative,
        "loadgen_excluded_proven": authoritative,
    }


def cache() -> dict:
    return {
        "label": "fresh_moraine_existing_clickhouse",
        "moraine_process": "fresh",
        "mcp_result_cache": "fresh",
        "mcp_posting_cache": "fresh",
        "mcp_document_frequency_cache": "fresh",
        "mcp_document_cache": "fresh",
        "mcp_hydration_cache": "fresh",
        "target_query_prewarmed": False,
        "clickhouse_cache": "seed_warmed",
        "os_page_cache": "uncontrolled",
        "generation_sha256": digest("cache-generation"),
        "fingerprint_sha256": digest("cache-fingerprint"),
    }


def binary(build: dict) -> dict:
    entries = [
        {"role": item["role"], "sha256": item["sha256"], "proc_exe_sha256": item["sha256"], "verified": True}
        for item in build["binaries"]
    ]
    return {
        "build_identity_sha256": build["identity_sha256"],
        "image_digest": build["image_digest"],
        "running_binaries": entries,
    }


def schedule(definition: dict, scenario: str, split: str, count: int,
             physical_resets: list[dict], *, overlap: bool | None = None) -> dict:
    expanded = {"scenario": scenario, "split": split, "planned": count}
    return {
        "schedule_sha256": definition["schedules"][f"{scenario}:{split}"],
        "expanded_schedule": expanded,
        "expanded_schedule_sha256": protocol.sha256_json(expanded),
        "seed": 480,
        "planned": count,
        "started": count,
        "completed": count,
        "unfinished": 0,
        "dropped": 0,
        "p99_start_slip_ms": 1.0,
        "drained": True,
        "drain_ms": 1.0,
        "streams_overlap": overlap,
        "physical_resets": physical_resets,
    }


def semantic(count: int) -> dict:
    return {
        "passed": True,
        "oracle_sha256": digest("independent-oracle"),
        "expected_count": count,
        "observed_count": count,
        "missing_count": 0,
        "duplicate_count": 0,
        "stale_count": 0,
        "malformed_count": 0,
        "other_error_count": 0,
    }


def run(arm: str, pair: int, order: str, reset_id: str, *, profile: str = "full", authoritative: bool = True) -> dict:
    return {
        "run_id": f"run-{arm}-{pair}-{reset_id}",
        "reset_id": reset_id,
        "arm": arm,
        "pair_id": pair,
        "order": order,
        "profile": profile,
        "authoritative": authoritative,
    }


def trial_telemetry() -> dict:
    return {
        "cpu_usage_usec_delta": 100,
        "cpu_nr_throttled_delta": 0,
        "throttled_usec_delta": 0,
        "memory_current_bytes": 1024,
        "memory_peak_bytes": 2048,
        "memory_event_high_delta": 0,
        "memory_event_max_delta": 0,
        "swap_current_bytes": 0,
        "oom_kill_delta": 0,
    }


def qps_trials(capacity: int, reset_prefix: str = "qps") -> tuple[list[dict], dict]:
    rates: list[int] = []
    low = 0
    rate = 1
    high: int | None = None
    while True:
        rates.append(rate)
        if rate > capacity:
            high = rate
            break
        low = rate
        if rate == 512:
            break
        rate = min(rate * 2, 512)
    if low == 0:
        censoring, lower, upper = "left", 0, 1
    elif low == 512:
        censoring, lower, upper = "right", 512, 512
    else:
        assert high is not None
        while high - low > 1:
            middle = (low + high) // 2
            rates.append(middle)
            if middle <= capacity:
                low = middle
            else:
                high = middle
        censoring, lower, upper = "none", low, high
    samples: list[dict] = []
    for offered in rates:
        passed = offered <= capacity
        for replicate in range(1, 4):
            planned = offered * 30
            correct = planned if passed else planned - 1
            samples.append({
                "offered_qps": offered,
                "replicate": replicate,
                "duration_s": 30,
                "reset_sha256": digest(f"{reset_prefix}:{offered}:{replicate}"),
                "achieved_goodput_qps": correct / 30,
                "p95_ms": 100.0,
                "p99_ms": 200.0,
                "max_ms": 300.0,
                "scheduler_p99_start_slip_ms": 1.0,
                "drain_ms": 1.0,
                "drained": True,
                "passed": passed,
                "outcomes": {
                    "planned": planned,
                    "started": planned,
                    "correct": correct,
                    "rejected": 0 if passed else 1,
                    "timed_out": 0,
                    "semantic_error": 0,
                    "protocol_error": 0,
                    "malformed": 0,
                    "late": 0,
                    "dropped": 0,
                    "other_error": 0,
                },
                "telemetry": trial_telemetry(),
            })
    return samples, {
        "direction": "higher",
        "sustainable_qps": low,
        "capacity_lower_qps": lower,
        "capacity_upper_qps": upper,
        "capacity_censoring": censoring,
    }


def ttr_sample(total: float = 100.0) -> dict:
    phases = {
        "spawn_exec": total * 0.2,
        "central_readiness": total * 0.2,
        "route_connect": total * 0.1,
        "initialize_tools": total * 0.1,
        "request_write": total * 0.1,
        "response_read": total * 0.2,
        "oracle_validation": total * 0.1,
    }
    return {
        "sample_id": "ttr-1",
        "total_ms": total,
        "phases_ms": phases,
        "pid_start_sha256": digest("pid-start"),
        "cache_generation_sha256": digest("ttr-cache"),
        "route_marker_sha256": digest("central-route"),
        "searches_before_write": 0,
        "central_request_count": 1,
        "embedded_fallback": False,
        "valid": True,
        "error_code": None,
    }


def etd_sample(lower: float = 100.0, upper: float | None = None, *, left: bool = False, failed: bool = False, suffix: str = "1") -> dict:
    upper = lower if upper is None else upper
    if failed:
        return {
            "event_identity_sha256": digest(f"event:{suffix}"),
            "term_sha256": None,
            "batch_sequence": None,
            "publication_durable_ms": None,
            "db_ack_ms": None,
            "last_miss_ms": None,
            "first_hit_ms": None,
            "first_valid_ms": None,
            "source_interval": {"lower_ms": 0.0, "upper_ms": None, "censoring": "right"},
            "db_ack_interval": {"lower_ms": 0.0, "upper_ms": None, "censoring": "right"},
            "term_use_count": 0,
            "cache_bypass": {name: True for name in protocol.CACHE_BYPASS_KEYS},
            "valid": False,
            "error_code": "publication_failed",
        }
    last_miss = None if left else lower
    return {
        "event_identity_sha256": digest(f"event:{suffix}"),
        "term_sha256": digest(f"term:{suffix}"),
        "batch_sequence": 1,
        "publication_durable_ms": 0.5,
        "db_ack_ms": 1.0,
        "last_miss_ms": last_miss,
        "first_hit_ms": upper,
        "first_valid_ms": upper,
        "source_interval": {"lower_ms": 0.0 if left else lower, "upper_ms": upper, "censoring": "left" if left else "interval"},
        "db_ack_interval": {"lower_ms": 0.0 if left or lower <= 1 else lower - 1.0, "upper_ms": max(0.0, upper - 1.0), "censoring": "left" if left or lower <= 1 else "interval"},
        "term_use_count": 1,
        "cache_bypass": {name: True for name in protocol.CACHE_BYPASS_KEYS},
        "valid": True,
        "error_code": None,
    }


def etd_metrics(sample: dict, *, loaded: bool = False) -> dict:
    source = sample["source_interval"]
    db_ack = sample["db_ack_interval"]
    def aggregate(interval: dict) -> dict:
        if interval["censoring"] == "right":
            return dict(interval)
        censoring = "left" if interval["censoring"] == "left" else ("none" if interval["lower_ms"] == interval["upper_ms"] else "interval")
        return {"lower_ms": interval["lower_ms"], "upper_ms": interval["upper_ms"], "censoring": censoring}
    loaded_query = None
    if loaded:
        loaded_query = {
            "offered_qps": 75.0,
            "planned": 1,
            "started": 1,
            "completed": 1,
            "scheduler_p99_slip_ms": 1.0,
            "schedule_delivered": True,
            "drained": True,
            "backlog": 0,
            "first_start_slip_ms": 1.0,
            "coverage_ns": 1,
            "failure_count": 0,
            "semantic_failures": 0,
        }
    return {
        "direction": "lower",
        "event_count": 1,
        "source_etd_p95": aggregate(source),
        "db_ack_etd_p95": aggregate(db_ack),
        "loaded_query": loaded_query,
        "operational": {
            "planned": 1,
            "started": 1,
            "completed": 1,
            "scheduler_p99_slip_ms": 1.0,
            "first_started_ns": 1,
            "last_completed_ns": 2,
        },
    }


def mixed_metrics(sample: dict, degradation: float = 1.0) -> dict:
    query_control = {"goodput_qps": 100.0, "p95_ms": 100.0, "p99_ms": 200.0}
    combined_query = {"goodput_qps": 100.0, "p95_ms": 100.0 * degradation, "p99_ms": 200.0 * degradation}
    ingest_control = {
        "source_etd_p95": {"lower_ms": 10.0, "upper_ms": 12.0, "censoring": "interval"},
        "db_ack_etd_p95": {"lower_ms": 9.0, "upper_ms": 11.0, "censoring": "interval"},
    }
    combined_ingest = {
        "source_etd_p95": {"lower_ms": 10.0, "upper_ms": 12.0 * degradation, "censoring": "interval"},
        "db_ack_etd_p95": {"lower_ms": 9.0, "upper_ms": 11.0 * degradation, "censoring": "interval"},
    }
    ratios = {
        "query_goodput": 1.0,
        "query_p95": degradation,
        "query_p99": degradation,
        "source_etd": degradation,
        "db_ack_etd": degradation,
    }
    passing = degradation <= 1.25
    return {
        "direction": "gates",
        "query_control": query_control,
        "ingest_control": ingest_control,
        "combined_query": combined_query,
        "combined_ingest": combined_ingest,
        "control_evidence": {
            "query_schedule_delivered": True,
            "ingest_schedule_delivered": True,
            "query_queue_depth": 0,
            "ingest_queue_depth": 0,
            "ingest_status_pass": True,
            "ingest_lost_events": 0,
            "ingest_duplicate_events": 0,
        },
        "ratios": ratios,
        "mixed_gates": {
            "query_slo": combined_query["p95_ms"] <= 750 and combined_query["p99_ms"] <= 2000,
            "query_degradation": passing,
            "ingest_slo": True,
            "ingest_degradation": passing,
            "controls_valid": True,
            "schedules_delivered": True,
            "overlap": True,
            "drained": True,
            "exact_events": True,
        },
        "lost_events": 0,
        "duplicate_events": 0,
    }


def make_result(definition: dict, builds: dict[str, dict], scenario: str, split: str, arm: str,
                pair: int, reset_id: str, *, qps_capacity: int = 100, ttr_ms: float = 100.0,
                etd_lower: float = 100.0, etd_upper: float | None = None,
                etd_left: bool = False, etd_failed: bool = False,
                mixed_degradation: float = 1.0, profile: str = "full", authoritative: bool = True) -> dict:
    order = protocol.PAIR_ORDER[pair - 1] if profile == "full" and pair <= 7 else "AB"
    if scenario == "qps":
        samples, metrics = qps_trials(qps_capacity, f"{reset_id}:{scenario}:{split}")
        count = sum(item["outcomes"]["planned"] for item in samples)
        scenario_ok = True
        conclusive = metrics["capacity_censoring"] == "none"
        physical_resets = [
            {"role": "trial", "reset_sha256": item["reset_sha256"]}
            for item in samples
        ]
    elif scenario == "ttr":
        samples = [ttr_sample(ttr_ms)]
        metrics = {"direction": "lower", "p95_ms": ttr_ms, "sample_count": 1}
        count = 1
        scenario_ok = True
        conclusive = True
        physical_resets = [{
            "role": "scenario",
            "reset_sha256": digest(f"physical:{reset_id}:{scenario}:{split}"),
        }]
    elif scenario in {"etd_idle", "etd_loaded"}:
        event = etd_sample(etd_lower, etd_upper, left=etd_left, failed=etd_failed, suffix=f"{arm}:{pair}:{scenario}:{split}")
        samples = [event]
        metrics = etd_metrics(event, loaded=scenario == "etd_loaded")
        count = 1
        scenario_ok = not etd_failed
        conclusive = True
        physical_resets = [{
            "role": "scenario",
            "reset_sha256": digest(f"physical:{reset_id}:{scenario}:{split}"),
        }]
    else:
        event = etd_sample(10.0, 12.0, suffix=f"{arm}:{pair}:mixed")
        samples = {"query_records": [], "ingest": [event]}
        metrics = mixed_metrics(event, mixed_degradation)
        count = 2
        scenario_ok = mixed_degradation <= 1.25
        conclusive = True
        physical_resets = [
            {
                "role": role,
                "reset_sha256": digest(f"physical:{reset_id}:{scenario}:{split}:{role}"),
            }
            for role in ("query_control", "ingest_control", "combined")
        ]
    resource_evidence = resources(authoritative)
    resource_ok = authoritative
    common_schedule = schedule(
        definition, scenario, split, count, physical_resets,
        overlap=True if scenario == "mixed" else None,
    )
    gates = {"correctness": True, "resources": resource_ok, "schedule": True, "scenario": scenario_ok}
    if not scenario_ok:
        status = "fail"
    elif not authoritative or profile == "smoke" or not conclusive:
        status = "inconclusive"
    else:
        status = "pass"
    return protocol.create_scenario_result(
        scenario=scenario,
        split=split,
        suite_definition_sha256=protocol.sha256_json(definition),
        run=run(arm, pair, order, reset_id, profile=profile, authoritative=authoritative),
        status=status,
        cache=cache(),
        binary=binary(builds[arm]),
        resources=resource_evidence,
        schedule=common_schedule,
        metrics=metrics,
        samples=samples,
        semantic=semantic(count),
        gates=gates,
    )


def write_document(path: Path, document: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(protocol.canonical_json_bytes(document))


def comparison_bundle(root: Path, *, qps_ratios: list[float] | None = None,
                      ttr_ratios: list[float] | None = None,
                      etd_ratios: list[float] | None = None,
                      censored_candidate: bool = False,
                      left_censored_etd_candidate: bool = False,
                      mixed_candidate_degradation: float = 1.0,
                      duplicate_first_reset: bool = False) -> tuple[Path, Path, dict]:
    definition, builds = make_definition()
    qps_ratios = qps_ratios or [1.10] * 7
    ttr_ratios = ttr_ratios or [1.10] * 7
    etd_ratios = etd_ratios or [1.10] * 7
    manifests: dict[str, Path] = {}
    for arm in ("baseline", "candidate"):
        parent = root / arm
        paths: list[Path] = []
        for pair, order in enumerate(protocol.PAIR_ORDER, 1):
            reset_id = (
                "reset-baseline-1"
                if duplicate_first_reset and arm == "candidate" and pair == 1
                else f"reset-{arm}-{pair}"
            )
            for scenario, splits in protocol.COMPARISON_MATRIX.items():
                for split in splits:
                    kwargs = {}
                    if scenario == "qps":
                        kwargs["qps_capacity"] = 100 if arm == "baseline" else (512 if censored_candidate else round(100 * qps_ratios[pair - 1]))
                    elif scenario == "ttr":
                        kwargs["ttr_ms"] = 100.0 if arm == "baseline" else 100.0 / ttr_ratios[pair - 1]
                    elif scenario == "etd_loaded":
                        value = 100.0 if arm == "baseline" else 100.0 / etd_ratios[pair - 1]
                        kwargs.update(
                            etd_lower=0.0 if arm == "candidate" and left_censored_etd_candidate else value,
                            etd_upper=value,
                            etd_left=arm == "candidate" and left_censored_etd_candidate,
                        )
                    elif scenario == "etd_idle":
                        kwargs.update(etd_lower=80.0, etd_upper=80.0)
                    elif scenario == "mixed" and arm == "candidate":
                        kwargs["mixed_degradation"] = mixed_candidate_degradation
                    document = make_result(definition, builds, scenario, split, arm, pair, reset_id, **kwargs)
                    path = parent / f"pair-{pair}-{scenario}-{split}.json"
                    write_document(path, document)
                    paths.append(path)
        manifest_path = parent / "manifest.json"
        manifest = protocol.create_suite_manifest(
            purpose="comparison",
            arm=arm,
            suite_definition=definition,
            artifact_paths=paths,
            manifest_path=manifest_path,
        )
        write_document(manifest_path, manifest)
        manifests[arm] = manifest_path
    return manifests["baseline"], manifests["candidate"], definition


def repeatability_bundles(root: Path, capacities: list[int]) -> list[Path]:
    definition, builds = make_definition(both_arms=False)
    manifests: list[Path] = []
    for index, capacity in enumerate(capacities, 1):
        parent = root / f"repeat-{index}"
        paths: list[Path] = []
        reset_id = f"repeat-reset-{index}"
        for scenario, splits in protocol.REPEATABILITY_MATRIX.items():
            split = splits[0]
            kwargs = {"qps_capacity": capacity} if scenario == "qps" else {}
            if scenario in {"etd_idle", "etd_loaded"}:
                kwargs.update(etd_lower=100.0, etd_upper=101.0)
            document = make_result(definition, builds, scenario, split, "baseline", 1, reset_id, **kwargs)
            path = parent / f"{scenario}-{split}.json"
            write_document(path, document)
            paths.append(path)
        manifest_path = parent / "manifest.json"
        manifest = protocol.create_suite_manifest(
            purpose="repeatability",
            arm="baseline",
            suite_definition=definition,
            artifact_paths=paths,
            manifest_path=manifest_path,
        )
        write_document(manifest_path, manifest)
        manifests.append(manifest_path)
    return manifests


class DocumentValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.definition, self.builds = make_definition()

    def result(self, scenario: str = "ttr", split: str = "research", **kwargs) -> dict:
        return make_result(self.definition, self.builds, scenario, split, "baseline", 1, "reset-one", **kwargs)

    def assert_mutation_rejected(self, document: dict, mutate, message: str | None = None) -> None:
        changed = copy.deepcopy(document)
        mutate(changed)
        changed["artifact_sha256"] = protocol.sha256_json({key: value for key, value in changed.items() if key != "artifact_sha256"})
        context = self.assertRaisesRegex(protocol.ProtocolError, message) if message else self.assertRaises(protocol.ProtocolError)
        with context:
            protocol.validate_document(changed)

    def test_public_constructor_and_validator_apis(self) -> None:
        expected = {
            "create_scenario_result", "create_build_recipe", "create_build_identity",
            "create_suite_definition", "create_suite_manifest", "validate_scenario",
            "validate_suite_definition", "validate_comparison", "validate_repeatability",
            "validate_document", "load_document", "load_suite_manifest", "compare_manifests",
            "evaluate_repeatability", "write_json_atomic",
        }
        self.assertTrue(all(callable(getattr(protocol, name, None)) for name in expected))
        self.assertEqual(protocol.validate_document(self.result())["document_type"], "scenario_result")

    def test_schemas_parse_and_explicitly_dispatch_documents(self) -> None:
        performance = json.loads((BENCH_DIR / "schema" / "moraine-performance-v1.schema.json").read_text())
        suite = json.loads((BENCH_DIR / "schema" / "moraine-performance-suite-v1.schema.json").read_text())
        refs = {item["$ref"] for item in performance["oneOf"]}
        self.assertEqual(refs, {"#/$defs/scenario_result", "#/$defs/comparison", "#/$defs/repeatability"})
        self.assertEqual(suite["properties"]["schema_version"]["const"], protocol.SUITE_VERSION)
        self.assertFalse(suite["additionalProperties"])

    def test_checked_in_schema_fixtures_have_explicit_outcomes(self) -> None:
        expected = {
            "performance-valid-ttr.json": True,
            "performance-invalid-document-type.json": False,
            "performance-invalid-nonfinite.json": False,
        }
        actual = {path.name for path in FIXTURES.glob("performance-*.json")}
        self.assertEqual(actual, set(expected))
        for name, valid in expected.items():
            with self.subTest(name=name):
                if valid:
                    protocol.load_document(FIXTURES / name)
                else:
                    with self.assertRaises(protocol.ProtocolError):
                        protocol.load_document(FIXTURES / name)

    def test_dispatch_rejects_unknown_extra_and_missing_documents(self) -> None:
        with self.assertRaisesRegex(protocol.ProtocolError, "unsupported document type"):
            protocol.validate_document({"document_type": "benchmark"})
        document = self.result()
        document["unexpected"] = True
        with self.assertRaisesRegex(protocol.ProtocolError, "extra=.*unexpected"):
            protocol.validate_document(document)
        document = self.result()
        del document["cache"]
        with self.assertRaisesRegex(protocol.ProtocolError, "missing=.*cache"):
            protocol.validate_document(document)

    def test_qps_direction_zero_and_exact_bracket(self) -> None:
        normal = self.result("qps", qps_capacity=100)
        self.assertEqual(normal["metrics"]["direction"], "higher")
        self.assertEqual((normal["metrics"]["capacity_lower_qps"], normal["metrics"]["capacity_upper_qps"]), (100, 101))
        left = self.result("qps", qps_capacity=0)
        self.assertEqual(left["status"], "inconclusive")
        self.assertEqual((left["metrics"]["sustainable_qps"], left["metrics"]["capacity_lower_qps"], left["metrics"]["capacity_upper_qps"]), (0, 0, 1))

    def test_every_qps_threshold_and_error_class_is_enforced(self) -> None:
        cases = {
            "p95_ms": 750.0001,
            "p99_ms": 2000.0001,
            "max_ms": 5000.0001,
            "scheduler_p99_start_slip_ms": 10.0001,
            "drain_ms": 5000.0001,
        }
        base = self.result("qps", qps_capacity=100)
        passing = next(index for index, item in enumerate(base["samples"]) if item["passed"])
        for field, value in cases.items():
            with self.subTest(field=field):
                self.assert_mutation_rejected(base, lambda doc, f=field, v=value: doc["samples"][passing].__setitem__(f, v), "contradicts QPS")
        equality_cases = {
            "p95_ms": 750.0,
            "p99_ms": 2000.0,
            "max_ms": 5000.0,
            "scheduler_p99_start_slip_ms": 10.0,
            "drain_ms": 5000.0,
        }
        for field, value in equality_cases.items():
            equality = copy.deepcopy(base)
            equality["samples"][passing][field] = value
            equality["artifact_sha256"] = protocol.sha256_json({
                key: child for key, child in equality.items() if key != "artifact_sha256"
            })
            with self.subTest(equality=field):
                protocol.validate_document(equality)
        for outcome in ("rejected", "timed_out", "semantic_error", "protocol_error", "malformed", "late", "other_error"):
            def mutate(doc, name=outcome):
                sample = doc["samples"][passing]
                sample["outcomes"]["correct"] -= 1
                sample["outcomes"][name] = 1
                sample["achieved_goodput_qps"] = sample["outcomes"]["correct"] / sample["duration_s"]
            with self.subTest(outcome=outcome):
                self.assert_mutation_rejected(base, mutate, "contradicts QPS")
        for telemetry_name in ("swap_current_bytes", "oom_kill_delta"):
            with self.subTest(telemetry=telemetry_name):
                self.assert_mutation_rejected(base, lambda doc, name=telemetry_name: doc["samples"][passing]["telemetry"].__setitem__(name, 1), "contradicts QPS")

    def test_qps_count_duplicate_order_and_goodput_invariants(self) -> None:
        base = self.result("qps", qps_capacity=100)
        self.assert_mutation_rejected(base, lambda doc: doc["samples"][0]["outcomes"].__setitem__("planned", 31), "planned")
        self.assert_mutation_rejected(base, lambda doc: doc["samples"].append(copy.deepcopy(doc["samples"][0])), "duplicate")
        self.assert_mutation_rejected(base, lambda doc: doc["samples"][0].__setitem__("achieved_goodput_qps", 0), "must equal correct")
        self.assert_mutation_rejected(base, lambda doc: doc["samples"].pop(), "replicates|bracket")
        self.assert_mutation_rejected(
            base,
            lambda doc: doc["samples"][1].__setitem__(
                "reset_sha256", doc["samples"][0]["reset_sha256"]
            ),
            "unique physical reset",
        )
        self.assert_mutation_rejected(
            base,
            lambda doc: doc["schedule"]["physical_resets"][0].__setitem__(
                "reset_sha256", digest("unmatched-reset")
            ),
            "exactly match",
        )

    def test_ttr_lower_direction_zero_and_route_invariants(self) -> None:
        zero = self.result("ttr", ttr_ms=0.0)
        self.assertEqual(zero["metrics"], {"direction": "lower", "p95_ms": 0.0, "sample_count": 1})
        base = self.result("ttr")
        cases = {
            "searches_before_write": 1,
            "central_request_count": 2,
            "embedded_fallback": True,
            "error_code": "tool_error",
        }
        for field, value in cases.items():
            with self.subTest(field=field):
                self.assert_mutation_rejected(base, lambda doc, f=field, v=value: doc["samples"][0].__setitem__(f, v), "valid")
        self.assert_mutation_rejected(base, lambda doc: doc["samples"][0]["phases_ms"].__setitem__("spawn_exec", 0), "reconcile")
        self.assert_mutation_rejected(base, lambda doc: doc["metrics"].__setitem__("p95_ms", 99), "nearest-rank")

    def test_etd_interval_censoring_cache_and_identity_boundaries(self) -> None:
        normal = self.result("etd_loaded", etd_lower=100, etd_upper=101)
        self.assertEqual(normal["metrics"]["direction"], "lower")
        left = self.result("etd_loaded", etd_lower=0, etd_upper=1, etd_left=True)
        self.assertEqual(left["metrics"]["source_etd_p95"]["censoring"], "left")
        failed = self.result("etd_loaded", etd_failed=True)
        self.assertEqual(failed["metrics"]["source_etd_p95"]["censoring"], "right")
        self.assertEqual(failed["status"], "fail")
        multiple_terms = copy.deepcopy(normal)
        multiple_terms["samples"][0]["term_use_count"] = 2
        multiple_terms["artifact_sha256"] = protocol.sha256_json({
            key: value for key, value in multiple_terms.items() if key != "artifact_sha256"
        })
        protocol.validate_document(multiple_terms)
        early_ack = copy.deepcopy(normal)
        early_ack["samples"][0]["db_ack_ms"] = 0.25
        early_ack["samples"][0]["db_ack_interval"] = {
            "lower_ms": 99.75, "upper_ms": 100.75, "censoring": "interval"
        }
        early_ack["metrics"]["db_ack_etd_p95"] = {
            "lower_ms": 99.75, "upper_ms": 100.75, "censoring": "interval"
        }
        early_ack["artifact_sha256"] = protocol.sha256_json({
            key: value for key, value in early_ack.items() if key != "artifact_sha256"
        })
        protocol.validate_document(early_ack)
        self.assert_mutation_rejected(normal, lambda doc: doc["samples"][0]["cache_bypass"].__setitem__("posting", False), "valid")
        self.assert_mutation_rejected(normal, lambda doc: doc["samples"].append(copy.deepcopy(doc["samples"][0])), "event_count|duplicate")
        self.assert_mutation_rejected(normal, lambda doc: doc["samples"][0].__setitem__("first_valid_ms", 99), "precede|polling")

    def test_mixed_exact_thresholds_loss_duplicate_and_overlap(self) -> None:
        exact = self.result("mixed", "stress", mixed_degradation=1.25)
        self.assertEqual(exact["status"], "pass")
        above = self.result("mixed", "stress", mixed_degradation=1.2501)
        self.assertEqual(above["status"], "fail")
        self.assert_mutation_rejected(exact, lambda doc: doc["metrics"].__setitem__("lost_events", 1), "gates|status")
        self.assert_mutation_rejected(exact, lambda doc: doc["metrics"].__setitem__("duplicate_events", 1), "gates|status")
        self.assert_mutation_rejected(exact, lambda doc: doc["schedule"].__setitem__("streams_overlap", False), "gates|status")

    def test_resource_binary_cache_and_schedule_evidence_are_fail_closed(self) -> None:
        base = self.result()
        self.assert_mutation_rejected(base, lambda doc: doc["resources"].__setitem__("memory_max_bytes", 1024), "gates")
        self.assert_mutation_rejected(base, lambda doc: doc["binary"]["running_binaries"][0].__setitem__("proc_exe_sha256", digest("mutation")), "checksum")
        self.assert_mutation_rejected(base, lambda doc: doc["cache"].__setitem__("target_query_prewarmed", True), "must be false")
        self.assert_mutation_rejected(base, lambda doc: doc["schedule"].__setitem__("dropped", 1), "planned|gates")

    def test_redaction_rejects_credentials_environment_paths_oracles_and_cgroup_identity(self) -> None:
        cases = {
            "credential": lambda doc: doc["run"].__setitem__("run_id", "https://alice:secret@example.invalid/run"),
            "authorization": lambda doc: doc["run"].__setitem__("run_id", "Bearer abcdefgh"),
            "environment": lambda doc: doc.__setitem__("environment", {"RUSTFLAGS": "secret"}),
            "home_path": lambda doc: doc["run"].__setitem__("run_id", "/Users/alice/private/run"),
            "oracle": lambda doc: doc.__setitem__("oracle_payload", ["private answer"]),
            "cgroup": lambda doc: doc.__setitem__("cgroup_path", "/sys/fs/cgroup/private-host"),
            "user_host": lambda doc: doc["run"].__setitem__("run_id", "alice@runner17"),
        }
        for name, mutate in cases.items():
            document = self.result()
            mutate(document)
            with self.subTest(name=name):
                with self.assertRaises(protocol.ProtocolError):
                    protocol.validate_document(document)

    def test_nonfinite_values_are_rejected_in_memory_and_on_load(self) -> None:
        document = self.result()
        document["metrics"]["p95_ms"] = math.inf
        with self.assertRaisesRegex(protocol.ProtocolError, "finite"):
            protocol.validate_document(document)
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "nonfinite.json"
            path.write_text('{"document_type":"scenario_result","value":NaN}\n')
            with self.assertRaisesRegex(protocol.ProtocolError, "non-finite"):
                protocol.load_document(path)

    def test_atomic_writer_preserves_destination_on_pre_replace_failure(self) -> None:
        document = self.result()
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary) / "nested" / "result.json"
            protocol.write_json_atomic(destination, document)
            self.assertEqual(protocol.load_document(destination), document)
            with mock.patch.object(protocol.os, "replace", side_effect=OSError("injected")):
                with self.assertRaisesRegex(OSError, "injected"):
                    protocol.write_json_atomic(destination, document)
            self.assertEqual(protocol.load_document(destination), document)
            self.assertEqual([item.name for item in destination.parent.iterdir()], ["result.json"])


class ManifestValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.baseline, self.candidate, self.definition = comparison_bundle(self.root)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def manifest(self, path: Path | None = None) -> dict:
        return json.loads((path or self.baseline).read_text())

    def test_comparison_manifest_has_exact_seven_pair_nine_artifact_matrix(self) -> None:
        manifest, loaded = protocol.load_suite_manifest(self.baseline)
        self.assertEqual(len(manifest["artifacts"]), 63)
        self.assertEqual(len(loaded), 63)
        self.assertEqual({item["pair_id"] for item in manifest["artifacts"]}, set(range(1, 8)))
        for pair, order in enumerate(protocol.PAIR_ORDER, 1):
            self.assertEqual({item["order"] for item in manifest["artifacts"] if item["pair_id"] == pair}, {order})

    def test_missing_extra_duplicate_order_and_mismatched_arms_are_rejected(self) -> None:
        mutations = {
            "missing": lambda doc: doc["artifacts"].pop(),
            "extra": lambda doc: doc["artifacts"].append(copy.deepcopy(doc["artifacts"][0])),
            "duplicate": lambda doc: doc["artifacts"][1].update(doc["artifacts"][0]),
            "order": lambda doc: doc["artifacts"][0].__setitem__("order", "BA"),
            "arm": lambda doc: doc["artifacts"][0].__setitem__("arm", "candidate"),
            "pair": lambda doc: doc["artifacts"][0].__setitem__("pair_id", 8),
        }
        for name, mutate in mutations.items():
            document = self.manifest()
            mutate(document)
            with self.subTest(name=name):
                with self.assertRaises(protocol.ProtocolError):
                    protocol.validate_document(document)

    def test_duplicate_and_cross_arm_reset_id_are_rejected(self) -> None:
        baseline = self.manifest()
        pair_two = next(item for item in baseline["artifacts"] if item["pair_id"] == 2)
        pair_two["run_id"] = baseline["artifacts"][0]["run_id"]
        pair_two["reset_id"] = baseline["artifacts"][0]["reset_id"]
        with self.assertRaisesRegex(protocol.ProtocolError, "unique|share"):
            protocol.validate_document(baseline)
        with tempfile.TemporaryDirectory() as temporary:
            baseline_path, candidate_path, _ = comparison_bundle(
                Path(temporary), duplicate_first_reset=True
            )
            with self.assertRaisesRegex(protocol.ProtocolError, "fourteen.*unique"):
                protocol.compare_manifests(baseline_path, candidate_path)

    def test_missing_file_checksum_and_manifest_mutation_are_rejected(self) -> None:
        manifest = self.manifest()
        target = self.baseline.parent / manifest["artifacts"][0]["relative_path"]
        original = target.read_bytes()
        target.write_bytes(original + b" ")
        with self.assertRaisesRegex(protocol.ProtocolError, "checksum mismatch"):
            protocol.load_suite_manifest(self.baseline)
        target.write_bytes(original)
        target.unlink()
        with self.assertRaisesRegex(protocol.ProtocolError, "missing artifact"):
            protocol.load_suite_manifest(self.baseline)
        manifest = self.manifest(self.candidate)
        manifest["suite_definition"]["fixture"]["fingerprints"]["corpus_sha256"] = digest("mutated-corpus")
        manifest["suite_definition_sha256"] = protocol.sha256_json(manifest["suite_definition"])
        manifest["manifest_sha256"] = protocol.sha256_json({key: value for key, value in manifest.items() if key != "manifest_sha256"})
        write_document(self.candidate, manifest)
        with self.assertRaisesRegex(protocol.ProtocolError, "suite definition mismatch"):
            protocol.load_suite_manifest(self.candidate)

    def test_artifact_reference_identity_mutation_is_rejected(self) -> None:
        manifest = self.manifest()
        manifest["artifacts"][0]["artifact_sha256"] = digest("mutated-artifact")
        manifest["manifest_sha256"] = protocol.sha256_json({key: value for key, value in manifest.items() if key != "manifest_sha256"})
        write_document(self.baseline, manifest)
        with self.assertRaisesRegex(protocol.ProtocolError, "artifact_sha256 does not match"):
            protocol.load_suite_manifest(self.baseline)


class ComparisonPolicyTests(unittest.TestCase):
    def compare(self, **kwargs) -> dict:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        baseline, candidate, _ = comparison_bundle(Path(temporary.name), **kwargs)
        return protocol.compare_manifests(baseline, candidate)

    def test_candidate_over_baseline_qps_and_baseline_over_candidate_latency(self) -> None:
        result = self.compare()
        self.assertEqual(result["status"], "pass")
        self.assertAlmostEqual(result["metrics"]["ratios"]["qps"], 1.1)
        self.assertAlmostEqual(result["metrics"]["ratios"]["ttr"], 1.1)
        self.assertGreater(result["metrics"]["ratios"]["source_etd"], 1.09)
        self.assertEqual(result["metrics"]["direction_pairs"], {"qps": 7, "ttr": 7, "source_etd": 7})
        self.assertGreater(result["metrics"]["suite_score_ci95"]["lower"], 1.0)

    def test_summary_validator_recomputes_metrics_and_status(self) -> None:
        result = self.compare()
        protocol.validate_comparison(result)
        for field in ("status", "ratios"):
            mutated = copy.deepcopy(result)
            if field == "status":
                mutated["status"] = "fail"
            else:
                mutated["metrics"]["ratios"]["qps"] *= 2
            mutated["document_sha256"] = protocol.sha256_json(
                {
                    key: value
                    for key, value in mutated.items()
                    if key != "document_sha256"
                }
            )
            with self.subTest(field=field), self.assertRaises(protocol.ProtocolError):
                protocol.validate_comparison(mutated)

    def test_manifest_argument_order_and_labels_do_not_change_result(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        baseline, candidate, _ = comparison_bundle(Path(temporary.name))
        forward = protocol.compare_manifests(baseline, candidate)
        reverse = protocol.compare_manifests(candidate, baseline)
        self.assertEqual(forward, reverse)

    def test_exact_score_threshold_passes_and_just_below_fails(self) -> None:
        exact = self.compare(qps_ratios=[1.05] * 7, ttr_ratios=[1.05] * 7, etd_ratios=[1.05] * 7)
        self.assertEqual(exact["status"], "pass")
        below = self.compare(qps_ratios=[1.04] * 7, ttr_ratios=[1.04] * 7, etd_ratios=[1.04] * 7)
        self.assertEqual(below["status"], "fail")

    def test_constituent_and_five_of_seven_direction_boundaries(self) -> None:
        four = self.compare(qps_ratios=[1.06] * 4 + [0.99] * 3, ttr_ratios=[1.20] * 7, etd_ratios=[1.20] * 7)
        self.assertEqual(four["metrics"]["direction_pairs"]["qps"], 4)
        self.assertEqual(four["status"], "fail")
        five = self.compare(qps_ratios=[1.06] * 5 + [0.99] * 2, ttr_ratios=[1.20] * 7, etd_ratios=[1.20] * 7)
        self.assertEqual(five["metrics"]["direction_pairs"]["qps"], 5)
        self.assertEqual(five["status"], "pass")
        constituent = self.compare(qps_ratios=[0.97] * 7, ttr_ratios=[1.30] * 7, etd_ratios=[1.30] * 7)
        self.assertAlmostEqual(constituent["metrics"]["ratios"]["qps"], 0.97)
        self.assertEqual(constituent["status"], "fail")

    def test_high_variability_complete_arms_are_inconclusive(self) -> None:
        result = self.compare(qps_ratios=[0.8, 1.4, 0.8, 1.4, 0.8, 1.4, 1.4], ttr_ratios=[1.2] * 7, etd_ratios=[1.2] * 7)
        self.assertEqual(result["status"], "inconclusive")
        self.assertIn("comparison:high-geometric-variability", result["diagnostics"])

    def test_geometric_variability_exact_limits_and_just_over(self) -> None:
        for limit in (0.10, 0.15):
            span = math.log1p(limit) * math.sqrt(3)
            exact = [math.exp(-span), 1.0, 1.0, 1.0, 1.0, 1.0, math.exp(span)]
            over_span = span * 1.000001
            over = [math.exp(-over_span), 1.0, 1.0, 1.0, 1.0, 1.0, math.exp(over_span)]
            with self.subTest(limit=limit):
                self.assertAlmostEqual(protocol.geometric_rsd(exact), limit)
                self.assertGreater(protocol.geometric_rsd(over), limit)

    def test_censored_capacity_is_inconclusive_not_failure(self) -> None:
        result = self.compare(censored_candidate=True)
        self.assertEqual(result["status"], "inconclusive")
        self.assertTrue(any("capacity-censored" in item for item in result["diagnostics"]))

    def test_etd_threshold_overlap_and_left_censoring_are_inconclusive(self) -> None:
        overlap = self.compare(etd_ratios=[1.0] * 7, qps_ratios=[1.2] * 7, ttr_ratios=[1.2] * 7)
        self.assertEqual(overlap["status"], "inconclusive")
        self.assertTrue(any("threshold-overlap" in item for item in overlap["diagnostics"]))
        left = self.compare(
            left_censored_etd_candidate=True,
            qps_ratios=[1.2] * 7,
            ttr_ratios=[1.2] * 7,
            etd_ratios=[1.1] * 7,
        )
        self.assertEqual(left["status"], "inconclusive")
        self.assertTrue(any("source-etd:censored" in item for item in left["diagnostics"]))

    def test_failed_mixed_gate_takes_failure_precedence(self) -> None:
        result = self.compare(
            mixed_candidate_degradation=1.2501,
            qps_ratios=[1.2] * 7,
            ttr_ratios=[1.2] * 7,
            etd_ratios=[1.2] * 7,
        )
        self.assertEqual(result["status"], "fail")
        self.assertTrue(any("mixed:stress:gate-failed" in item for item in result["diagnostics"]))

    def test_fast_error_capacity_cannot_raise_goodput_or_pass(self) -> None:
        result = self.compare(qps_ratios=[0.9] * 7, ttr_ratios=[1.2] * 7, etd_ratios=[1.2] * 7)
        self.assertLess(result["metrics"]["ratios"]["qps"], 1.0)
        self.assertEqual(result["status"], "fail")

    def test_deterministic_bootstrap_is_stable_and_strictly_above_one(self) -> None:
        values = [1.05] * 7
        first = protocol.bootstrap_interval(values)
        second = protocol.bootstrap_interval(values)
        self.assertEqual(first, second)
        self.assertEqual(first, (1.05, 1.05))
        self.assertEqual(protocol.bootstrap_interval([1.0] * 7)[0], 1.0)


class RepeatabilityPolicyTests(unittest.TestCase):
    def evaluate(self, capacities: list[int]) -> dict:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        paths = repeatability_bundles(Path(temporary.name), capacities)
        return protocol.evaluate_repeatability(paths)

    def test_exact_mad_threshold_passes_and_just_over_fails(self) -> None:
        exact = self.evaluate([95, 95, 95, 100, 105, 105, 105])
        self.assertEqual(exact["status"], "pass")
        self.assertEqual(exact["metrics"]["mad_ratios"]["qps"], 0.05)
        over = self.evaluate([94, 94, 94, 100, 106, 106, 106])
        self.assertEqual(over["status"], "fail")
        self.assertGreater(over["metrics"]["mad_ratios"]["qps"], 0.05)

    def test_summary_validator_recomputes_mad_and_status(self) -> None:
        result = self.evaluate([100] * 7)
        protocol.validate_repeatability(result)
        for field in ("status", "mad"):
            mutated = copy.deepcopy(result)
            if field == "status":
                mutated["status"] = "fail"
            else:
                mutated["metrics"]["mad_ratios"]["qps"] = 1.0
            mutated["document_sha256"] = protocol.sha256_json(
                {
                    key: value
                    for key, value in mutated.items()
                    if key != "document_sha256"
                }
            )
            with self.subTest(field=field), self.assertRaises(protocol.ProtocolError):
                protocol.validate_repeatability(mutated)

    def test_all_mad_threshold_boundaries_are_inclusive(self) -> None:
        for name, limit in {
            "qps": 0.05,
            "ttr": 0.10,
            "source_etd": 0.15,
            "db_ack_etd": 0.15,
        }.items():
            exact = [100 * (1 - limit)] * 3 + [100.0] + [100 * (1 + limit)] * 3
            over = [100 * (1 - limit - 0.0001)] * 3 + [100.0] + [100 * (1 + limit + 0.0001)] * 3
            with self.subTest(metric=name):
                self.assertAlmostEqual(protocol.mad_ratio(exact), limit)
                self.assertGreater(protocol.mad_ratio(over), limit)

    def test_input_order_is_invariant(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            paths = repeatability_bundles(Path(temporary), [98, 99, 100, 100, 100, 101, 102])
            forward = protocol.evaluate_repeatability(paths)
            reverse = protocol.evaluate_repeatability(list(reversed(paths)))
            self.assertEqual(forward, reverse)

    def test_six_or_eight_runs_fail_validation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            paths = repeatability_bundles(Path(temporary), [100] * 7)
            with self.assertRaisesRegex(protocol.ProtocolError, "exactly seven"):
                protocol.evaluate_repeatability(paths[:6])
            with self.assertRaisesRegex(protocol.ProtocolError, "exactly seven"):
                protocol.evaluate_repeatability(paths + [paths[0]])

    def test_duplicate_resets_censored_capacity_and_failed_gate_fail_validation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            paths = repeatability_bundles(Path(temporary), [100] * 7)
            duplicate = json.loads(paths[1].read_text())
            first_reset = json.loads(paths[0].read_text())["artifacts"][0]["reset_id"]
            for item in duplicate["artifacts"]:
                item["reset_id"] = first_reset
            duplicate["manifest_sha256"] = protocol.sha256_json({key: value for key, value in duplicate.items() if key != "manifest_sha256"})
            write_document(paths[1], duplicate)
            with self.assertRaises(protocol.ProtocolError):
                protocol.evaluate_repeatability(paths)
        with tempfile.TemporaryDirectory() as temporary:
            paths = repeatability_bundles(Path(temporary), [0] + [100] * 6)
            with self.assertRaisesRegex(protocol.ProtocolError, "uncensored"):
                protocol.evaluate_repeatability(paths)


if __name__ == "__main__":
    unittest.main()
