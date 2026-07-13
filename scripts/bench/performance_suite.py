#!/usr/bin/env python3
"""Run Moraine's fixed-resource end-to-end search performance suite."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence
from urllib.request import Request, urlopen

from performance_fixtures import (
    FreshSeedTarget,
    build_recipe,
    mixed_control_schedules,
    open_event_schedule,
    open_query_schedule,
    required_split_usage,
    seed_search_sql,
    validate_recipe,
    validate_split_usage,
)
from performance_protocol import (
    PAIR_ORDER,
    ProtocolError,
    canonical_json_bytes,
    compare_manifests,
    create_build_identity,
    create_build_recipe,
    create_scenario_result,
    create_suite_definition,
    create_suite_manifest,
    evaluate_repeatability,
    load_document,
    policy_document,
    sha256_bytes,
    sha256_json,
    validate_document,
    write_json_atomic,
)
from performance_runtime import (
    BuildIdentity,
    FixedEnvelope,
    RuntimeFailure,
    build_release_binaries,
    non_authoritative_resource_evidence,
    run_busy_child_proof,
    run_id,
    start_owned_sandbox,
    verify_process_binary,
)
from performance_scenarios import (
    ScenarioError,
    ScenarioResult,
    make_owned_sandbox_mixed_arm_factory,
    make_owned_sandbox_qps_runtime_factory,
    make_owned_sandbox_query_load,
    make_owned_sandbox_ttr_runtime_factory,
    run_mixed_scenario,
    run_owned_sandbox_etd_scenario,
    run_qps_scenario,
    run_ttr_scenario,
)

SCENARIOS = ("qps", "ttr", "etd_idle", "etd_loaded", "mixed")
SERVER_BINARY_NAMES = ("moraine", "moraine-ingest", "moraine-monitor")
IMAGE_RECIPE_PATHS = (
    "scripts/dev/sandbox/Dockerfile",
    "scripts/dev/sandbox/compose.yaml",
    "scripts/bench/compose.performance.yaml",
    "scripts/dev/sandbox/entrypoint.sh",
    "scripts/dev/sandbox/performance-entrypoint.sh",
)


class SuiteFailure(RuntimeError):
    """The suite cannot produce a truthful, complete artifact."""


def _git_commit(repo: Path) -> str:
    process = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
    )
    commit = process.stdout.strip()
    if process.returncode or len(commit) != 40:
        raise SuiteFailure(f"cannot resolve immutable commit for {repo}")
    return commit


def _require_clean(repo: Path) -> None:
    process = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=repo,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
    )
    if process.returncode:
        raise SuiteFailure(f"cannot inspect worktree {repo}")
    if process.stdout.strip():
        raise SuiteFailure(f"benchmark worktree is dirty: {repo}")


def _clickhouse_query(url: str, sql: str, *, timeout_s: float = 600.0) -> str:
    request = Request(url, data=sql.encode("utf-8"), headers={"Content-Type": "text/plain"}, method="POST")
    try:
        with urlopen(request, timeout=timeout_s) as response:
            return response.read().decode("utf-8").strip()
    except OSError as error:
        raise SuiteFailure(f"ClickHouse request failed: {type(error).__name__}") from error


def _seed_owned_sandbox(sandbox: Any, recipe: Mapping[str, Any]) -> None:
    url = f"http://127.0.0.1:{sandbox.clickhouse_port}"
    database = "moraine"
    existing = _clickhouse_query(url, f"SELECT count() FROM {database}.search_documents")
    if existing != "0":
        raise SuiteFailure(f"fresh owned volume is not empty: observed {existing} documents")
    target = FreshSeedTarget(
        database=database,
        reset_id=sandbox.sandbox_id,
        sandbox_owned=True,
        fresh_volume=True,
        empty_database=True,
    )
    _clickhouse_query(url, seed_search_sql(target, recipe))
    expected = recipe["corpus"]["document_count"]
    observed = _clickhouse_query(url, f"SELECT count() FROM {database}.search_documents")
    if observed != str(expected):
        raise SuiteFailure(f"seed cardinality mismatch: expected {expected}, observed {observed}")
    sandbox.checkpoint("seeded")


def _image_recipe_sha256(repo: Path) -> str:
    inputs: dict[str, str] = {}
    for relative in IMAGE_RECIPE_PATHS:
        path = repo / relative
        if not path.is_file():
            raise SuiteFailure(f"image recipe input is missing: {relative}")
        inputs[relative] = sha256_bytes(path.read_bytes())
    return sha256_json(inputs)


def _schedule_hashes(recipe: Mapping[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    templates = recipe["schedule_templates"]
    for scenario, splits in recipe["split_matrix"].items():
        template_name = "etd" if scenario in {"etd_idle", "etd_loaded"} else scenario
        for split in splits:
            result[f"{scenario}:{split}"] = sha256_json(
                {
                    "fixture_sha256": recipe["fixture_sha256"],
                    "scenario": scenario,
                    "split": split,
                    "template": templates[template_name],
                }
            )
    return result


def _physical_reset_sha256(reset_id: str) -> str:
    return "sha256:" + hashlib.sha256(reset_id.encode("utf-8")).hexdigest()


@dataclass
class EvidenceCollector:
    resources: list[dict[str, Any]] = field(default_factory=list)
    binary_hashes: dict[str, str] = field(default_factory=dict)
    cache_generations: list[str] = field(default_factory=list)
    physical_resets: dict[str, str] = field(default_factory=dict)

    def record_binary(self, name: str, digest: str) -> None:
        previous = self.binary_hashes.setdefault(name, digest)
        if previous != digest:
            raise SuiteFailure(f"running binary changed within scenario: {name}")

    def record_reset(self, role: str, reset_id: str) -> None:
        digest = _physical_reset_sha256(reset_id)
        if digest in self.physical_resets.values():
            raise SuiteFailure("physical reset was reused within a scenario")
        self.physical_resets[role] = digest

    def resource_artifact(self, *, authoritative: bool) -> dict[str, Any]:
        if not self.resources:
            if authoritative:
                raise SuiteFailure("authoritative scenario has no cgroup evidence")
            return non_authoritative_resource_evidence()
        constants = (
            "cgroup_version",
            "cgroup_driver",
            "controllers_enabled_proven",
            "effective_limits_proven",
            "host_headroom_proven",
            "cpuset_cpus_effective",
            "cpu_max_quota_us",
            "cpu_max_period_us",
            "memory_max_bytes",
            "swap_max_bytes",
            "server_descendants_proven",
            "loadgen_excluded_proven",
        )
        for name in constants:
            values = {json.dumps(item[name], sort_keys=True) for item in self.resources}
            if len(values) != 1:
                raise SuiteFailure(f"resource evidence changed across physical resets: {name}")
        first = self.resources[0]
        aggregate = {name: first[name] for name in constants}
        aggregate.update(
            {
                "authoritative": all(item["authoritative"] for item in self.resources),
                "cgroup_identity_sha256": sha256_json([item["cgroup_identity_sha256"] for item in self.resources]),
                "role_membership_sha256": sha256_json([item["role_membership_sha256"] for item in self.resources]),
                "cpu_usage_usec_delta": sum(item["cpu_usage_usec_delta"] for item in self.resources),
                "cpu_nr_throttled_delta": sum(item["cpu_nr_throttled_delta"] for item in self.resources),
                "throttled_usec_delta": sum(item["throttled_usec_delta"] for item in self.resources),
                "memory_current_bytes": max(item["memory_current_bytes"] for item in self.resources),
                "memory_peak_bytes": max(item["memory_peak_bytes"] for item in self.resources),
                "memory_event_high_delta": sum(item["memory_event_high_delta"] for item in self.resources),
                "memory_event_max_delta": sum(item["memory_event_max_delta"] for item in self.resources),
                "swap_current_bytes": max(item["swap_current_bytes"] for item in self.resources),
                "oom_kill_delta": sum(item["oom_kill_delta"] for item in self.resources),
            }
        )
        if authoritative and not aggregate["authoritative"]:
            raise SuiteFailure("authoritative scenario contains non-authoritative resource evidence")
        return aggregate

    def binary_artifact(self, build: Mapping[str, Any]) -> dict[str, Any]:
        if not self.binary_hashes:
            raise SuiteFailure("scenario has no verified running binary evidence")
        expected = {item["role"]: item["sha256"] for item in build["binaries"]}
        running = []
        for role, observed in sorted(self.binary_hashes.items()):
            if expected.get(role) != observed:
                raise SuiteFailure(f"running binary differs from immutable build: {role}")
            running.append({"role": role, "sha256": expected[role], "proc_exe_sha256": observed, "verified": True})
        return {
            "build_identity_sha256": build["identity_sha256"],
            "image_digest": build["image_digest"],
            "running_binaries": running,
        }

    def cache_artifact(self, recipe: Mapping[str, Any], scenario: str, split: str) -> dict[str, Any]:
        if not self.cache_generations:
            raise SuiteFailure("scenario has no observed cache generation")
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
            "generation_sha256": sha256_json(sorted(self.cache_generations)),
            "fingerprint_sha256": sha256_json(
                {"fixture": recipe["fixture_sha256"], "scenario": scenario, "split": split}
            ),
        }


class ManagedSandbox:
    """Couple one sandbox with its owned aggregate cgroup and evidence."""

    def __init__(
        self,
        sandbox: Any,
        envelope: FixedEnvelope,
        build: BuildIdentity,
        collector: EvidenceCollector,
        *,
        reset_role: str,
    ) -> None:
        self._sandbox = sandbox
        self._envelope = envelope
        self._collector = collector
        self._closed = False
        self._captured: Optional[Any] = None
        self.sandbox_id = sandbox.sandbox_id
        self.project = sandbox.project
        self.monitor_port = sandbox.monitor_port
        self.clickhouse_port = sandbox.clickhouse_port
        self.config_dir = sandbox.config_dir
        self.build = sandbox.build
        self._collector.record_reset(reset_role, self.sandbox_id)
        status = sandbox.status()
        self._server_pids = status.server_pids
        self._loadgen_pids = status.loadgen_pids
        for item in envelope.verify_running_binaries(build.binary_sha256):
            self._collector.record_binary(item.name, item.sha256)
        self._before = envelope.reset_measurement(self._server_pids, self._loadgen_pids)
        central = sandbox.central_status()
        self._collector.cache_generations.append(str(central["cache_generation"]))

    @property
    def watched_source_dir(self) -> Path:
        return self._sandbox.watched_source_dir

    def __getattr__(self, name: str) -> Any:
        return getattr(self._sandbox, name)

    def spawn_stdio_route(self, **kwargs: Any) -> subprocess.Popen[bytes]:
        process = self._sandbox.spawn_stdio_route(**kwargs)
        deadline = __import__("time").monotonic() + 5.0
        while __import__("time").monotonic() < deadline:
            status = self._sandbox.central_status()
            routes = status.get("route_processes", [])
            if routes:
                evidence = verify_process_binary(
                    int(routes[-1]["pid"]), "moraine-mcp", self.build.binary_sha256["moraine-mcp"]
                )
                self._collector.record_binary(evidence.name, evidence.sha256)
                return process
            __import__("time").sleep(0.01)
        process.kill()
        process.wait(timeout=2)
        raise SuiteFailure("MCP route process was not observable for binary verification")

    def _capture(self) -> Any:
        if self._captured is None:
            evidence = self._envelope.inspect(self._server_pids, self._loadgen_pids)
            evidence.assert_clean(self._before)
            self._captured = evidence
        return self._captured

    def trial_telemetry(self) -> dict[str, Any]:
        artifact = self._capture().artifact(self._before)
        return {
            name: artifact[name]
            for name in (
                "cpu_usage_usec_delta",
                "cpu_nr_throttled_delta",
                "throttled_usec_delta",
                "memory_current_bytes",
                "memory_peak_bytes",
                "memory_event_high_delta",
                "memory_event_max_delta",
                "swap_current_bytes",
                "oom_kill_delta",
            )
        }

    def down(self) -> None:
        if self._closed:
            return
        self._closed = True
        failure: Optional[BaseException] = None
        try:
            evidence = self._capture()
            self._collector.resources.append(evidence.artifact(self._before))
            self._sandbox.checkpoint("artifact-created")
        except BaseException as error:
            failure = error
        try:
            self._sandbox.down()
        except BaseException as error:
            failure = failure or error
        try:
            self._envelope.remove()
        except BaseException as error:
            failure = failure or error
        if failure is not None:
            raise SuiteFailure(f"owned resource cleanup or evidence capture failed: {failure}") from failure


def _start_measured_sandbox(
    repo: Path,
    build: BuildIdentity,
    recipe: Mapping[str, Any],
    collector: EvidenceCollector,
    *,
    reset_role: str,
) -> ManagedSandbox:
    envelope = FixedEnvelope(run_id())
    sandbox = None
    try:
        parent = envelope.create()
        sandbox = start_owned_sandbox(repo, cgroup_parent=parent, build=build)
        _seed_owned_sandbox(sandbox, recipe)
        return ManagedSandbox(sandbox, envelope, build, collector, reset_role=reset_role)
    except BaseException:
        if sandbox is not None:
            try:
                sandbox.down()
            except BaseException:
                pass
        try:
            envelope.remove(ignore_nonempty=True)
        except BaseException:
            pass
        raise


def _discover_image_digest(repo: Path, build: BuildIdentity, *, prove_cpu: bool) -> str:
    envelope = FixedEnvelope(run_id())
    sandbox = None
    try:
        parent = envelope.create()
        if prove_cpu:
            proof = run_busy_child_proof(envelope)
            if proof.cpu_to_wall_ratio < 0.85 or proof.cpu_to_wall_ratio > 1.15:
                raise SuiteFailure("aggregate busy-child CPU proof fell outside the declared tolerance")
        sandbox = start_owned_sandbox(repo, cgroup_parent=parent, build=build)
        status = sandbox.status()
        return sha256_json(status.image_ids)
    finally:
        failure: Optional[BaseException] = None
        if sandbox is not None:
            try:
                sandbox.down()
            except BaseException as error:
                failure = error
        try:
            envelope.remove()
        except BaseException as error:
            failure = failure or error
        if failure is not None:
            raise SuiteFailure(f"image-identity sandbox cleanup failed: {failure}") from failure


@dataclass(frozen=True)
class PreparedBuild:
    runtime: BuildIdentity
    protocol: Mapping[str, Any]


def _prepare_builds(repositories: Mapping[str, Path], output: Path) -> tuple[dict[str, PreparedBuild], Mapping[str, Any]]:
    prepared: dict[str, PreparedBuild] = {}
    common_recipe: Optional[Mapping[str, Any]] = None
    output.mkdir(parents=True, exist_ok=False)
    for index, (arm, repo) in enumerate(repositories.items()):
        _require_clean(repo)
        runtime_build = build_release_binaries(repo, output / arm)
        build_environment_sha256 = sha256_json(dict(sorted(runtime_build.build_environment.items())))
        runtime_recipe = runtime_build.artifact()["recipe"]
        recipe = create_build_recipe(
            toolchain_sha256=runtime_build.toolchain_sha256,
            target=runtime_build.target,
            linker=runtime_recipe["linker"],
            environment_allowlist=sorted(runtime_build.build_environment),
            build_environment_sha256=build_environment_sha256,
            image_recipe_sha256=_image_recipe_sha256(repo),
        )
        if common_recipe is None:
            common_recipe = recipe
        elif recipe != common_recipe:
            raise SuiteFailure("baseline and candidate build recipes differ")
        image_digest = _discover_image_digest(repo, runtime_build, prove_cpu=index == 0)
        protocol_build = create_build_identity(
            arm=arm,
            git_commit=_git_commit(repo),
            image_digest=image_digest,
            build_environment_sha256=build_environment_sha256,
            binaries=[
                {"role": name, "sha256": digest}
                for name, digest in sorted(runtime_build.binary_sha256.items())
            ],
        )
        prepared[arm] = PreparedBuild(runtime_build, protocol_build)
    if common_recipe is None:
        raise SuiteFailure("no build repositories were supplied")
    return prepared, common_recipe


def _semantic_evidence(
    result: ScenarioResult,
    scenario: str,
    split: str,
    recipe: Mapping[str, Any],
) -> dict[str, Any]:
    malformed = 0
    missing = 0
    duplicates = 0
    stale = 0
    other = result.semantic_failures
    if scenario == "qps":
        outcomes = [sample["outcomes"] for sample in result.samples]  # type: ignore[index]
        observed = sum(item["correct"] for item in outcomes)
        malformed = sum(item["malformed"] for item in outcomes)
        other = sum(item[name] for item in outcomes for name in ("semantic_error", "protocol_error", "other_error"))
        expected = observed + malformed + other
    elif scenario == "ttr":
        samples = list(result.samples)  # type: ignore[arg-type]
        observed = sum(bool(item["valid"]) for item in samples)
        expected = len(samples)
        other = expected - observed
    elif scenario in {"etd_idle", "etd_loaded"}:
        samples = list(result.samples)  # type: ignore[arg-type]
        observed = sum(bool(item["valid"]) for item in samples)
        expected = len(samples)
        other = expected - observed
    else:
        metrics = result.metrics
        expected = len(result.samples["query_records"]) + len(result.samples["ingest"])  # type: ignore[index]
        missing = int(metrics["lost_events"])
        duplicates = int(metrics["duplicate_events"])
        observed = max(0, expected - missing) + duplicates
        other = result.semantic_failures
    passed = expected == observed and not any((missing, duplicates, stale, malformed, other))
    fingerprint_name = f"{'query' if scenario in {'qps', 'ttr'} else 'event'}_{split}_sha256"
    if scenario == "mixed":
        fingerprint_name = "event_stress_sha256"
    return {
        "passed": passed,
        "oracle_sha256": recipe["fingerprints"][fingerprint_name],
        "expected_count": expected,
        "observed_count": observed,
        "missing_count": missing,
        "duplicate_count": duplicates,
        "stale_count": stale,
        "malformed_count": malformed,
        "other_error_count": other,
    }


def _scenario_pass(result: ScenarioResult, scenario: str) -> bool:
    if scenario == "qps":
        return True
    if scenario == "ttr":
        return all(bool(sample["valid"]) for sample in result.samples)  # type: ignore[arg-type]
    if scenario in {"etd_idle", "etd_loaded"}:
        return all(bool(sample["valid"]) for sample in result.samples)  # type: ignore[arg-type]
    gates = result.metrics["mixed_gates"]
    return all(bool(value) for value in gates.values()) and result.metrics["lost_events"] == 0 and result.metrics["duplicate_events"] == 0


def _schedule_evidence(
    result: ScenarioResult,
    scenario: str,
    split: str,
    definition: Mapping[str, Any],
    recipe: Mapping[str, Any],
    collector: EvidenceCollector,
    *,
    query_count: int = 0,
    event_count: int = 0,
) -> dict[str, Any]:
    if scenario == "qps":
        samples = list(result.samples)  # type: ignore[arg-type]
        planned = sum(item["outcomes"]["planned"] for item in samples)
        started = sum(item["outcomes"]["started"] for item in samples)
        completed = started
        dropped = sum(item["outcomes"]["dropped"] for item in samples)
        slip = max(item["scheduler_p99_start_slip_ms"] for item in samples)
        drained = all(item["drained"] for item in samples)
        drain_ms = max(item["drain_ms"] for item in samples)
        physical = [{"role": "trial", "reset_sha256": item["reset_sha256"]} for item in samples]
    elif scenario == "mixed":
        planned = 2 * (query_count + event_count)
        started = completed = planned
        dropped = 0
        slip = 0.0
        drained = bool(result.metrics["mixed_gates"]["drained"])
        drain_ms = 0.0
        physical = [
            {"role": role, "reset_sha256": collector.physical_resets[role]}
            for role in ("query_control", "ingest_control", "combined")
        ]
    else:
        samples = list(result.samples)  # type: ignore[arg-type]
        planned = started = completed = len(samples)
        dropped = 0
        slip = 0.0
        drained = True
        drain_ms = 0.0
        physical = [{"role": "scenario", "reset_sha256": collector.physical_resets["scenario"]}]
    return {
        "schedule_sha256": definition["schedules"][f"{scenario}:{split}"],
        "seed": recipe["seed"]["value"],
        "planned": planned,
        "started": started,
        "completed": completed,
        "unfinished": started - completed,
        "dropped": dropped,
        "p99_start_slip_ms": slip,
        "drained": drained,
        "drain_ms": drain_ms,
        "streams_overlap": bool(result.metrics["mixed_gates"]["overlap"]) if scenario == "mixed" else None,
        "physical_resets": physical,
    }


def _run_scenario(
    repo: Path,
    prepared: PreparedBuild,
    recipe: Mapping[str, Any],
    definition: Mapping[str, Any],
    *,
    scenario: str,
    split: str,
    run: Mapping[str, Any],
    baseline_sustainable_qps: Optional[float],
    output: Path,
) -> tuple[Path, ScenarioResult]:
    collector = EvidenceCollector()
    query_cases = recipe["query_splits"][split if scenario != "mixed" else "stress"]
    event_cases = recipe["event_splits"][split if scenario != "mixed" else "stress"]
    query_count = 0
    event_count = 0
    if scenario == "qps":
        def sandbox_factory(_spec: Any) -> ManagedSandbox:
            return _start_measured_sandbox(repo, prepared.runtime, recipe, collector, reset_role="trial")

        runtime_factory = make_owned_sandbox_qps_runtime_factory(
            sandbox_factory, lambda sandbox: sandbox.trial_telemetry()
        )
        result = run_qps_scenario(query_cases, runtime_factory, profile=run["profile"])
    elif scenario == "ttr":
        sandbox = _start_measured_sandbox(repo, prepared.runtime, recipe, collector, reset_role="scenario")
        try:
            samples = 3 if run["profile"] == "smoke" else 15
            result = run_ttr_scenario(
                query_cases,
                make_owned_sandbox_ttr_runtime_factory(sandbox),
                samples=samples,
            )
        finally:
            sandbox.down()
    elif scenario in {"etd_idle", "etd_loaded"}:
        sandbox = _start_measured_sandbox(repo, prepared.runtime, recipe, collector, reset_role="scenario")
        event_schedule = open_event_schedule(recipe, split)
        event_count = len(event_schedule)
        timeout_s = recipe["schedule_templates"]["etd"]["visibility_timeout_ns"] / 1_000_000_000
        try:
            query_load = None
            if scenario == "etd_loaded":
                if baseline_sustainable_qps is None or baseline_sustainable_qps <= 0:
                    raise SuiteFailure("loaded ETD requires a frozen positive baseline capacity")
                offered = 0.75 * baseline_sustainable_qps
                load_cases = {case["case_id"]: case for case in recipe["query_splits"]["stress"]}
                load_schedule = open_query_schedule(recipe, "stress", offered, stream="mixed")
                query_load = make_owned_sandbox_query_load(
                    sandbox,
                    load_cases,
                    load_schedule,
                    offered_qps=offered,
                    timeout_s=5.0,
                )
            result = run_owned_sandbox_etd_scenario(
                sandbox,
                event_cases,
                event_schedule,
                mode="idle" if scenario == "etd_idle" else "loaded",
                timeout_s=timeout_s,
                poll_interval_s=recipe["schedule_templates"]["etd"]["poll_interval_ns"] / 1_000_000_000,
                baseline_sustainable_qps=baseline_sustainable_qps,
                query_load=query_load,
            )
        finally:
            sandbox.down()
    else:
        if baseline_sustainable_qps is None or baseline_sustainable_qps <= 0:
            raise SuiteFailure("mixed scenario requires a frozen positive baseline capacity")
        offered = 0.75 * baseline_sustainable_qps
        schedules = mixed_control_schedules(recipe, offered)
        query_schedule = schedules["combined"]["queries"]
        event_schedule = schedules["combined"]["events"]
        query_count = len(query_schedule)
        event_count = len(event_schedule)
        mixed_query_cases = {case["case_id"]: case for case in recipe["query_splits"]["stress"]}
        mixed_event_cases = {event["case_id"]: event for event in recipe["event_splits"]["stress"]}
        role_map = {"query_only": "query_control", "ingest_only": "ingest_control", "combined": "combined"}

        def sandbox_factory(label: str) -> ManagedSandbox:
            return _start_measured_sandbox(
                repo,
                prepared.runtime,
                recipe,
                collector,
                reset_role=role_map[label],
            )

        arm_factory = make_owned_sandbox_mixed_arm_factory(
            sandbox_factory,
            mixed_query_cases,
            mixed_event_cases,
            query_rate_qps=offered,
            recipe_fingerprint=recipe["fixture_sha256"],
            request_timeout_s=5.0,
            poll_interval_s=recipe["schedule_templates"]["etd"]["poll_interval_ns"] / 1_000_000_000,
        )
        result = run_mixed_scenario(query_schedule, event_schedule, arm_factory)
    resources = collector.resource_artifact(authoritative=bool(run["authoritative"]))
    binary = collector.binary_artifact(prepared.protocol)
    cache = collector.cache_artifact(recipe, scenario, split)
    schedule = _schedule_evidence(
        result,
        scenario,
        split,
        definition,
        recipe,
        collector,
        query_count=query_count,
        event_count=event_count,
    )
    semantic = _semantic_evidence(result, scenario, split, recipe)
    schedule_pass = not schedule["dropped"] and not schedule["unfinished"] and schedule["drained"] and schedule["p99_start_slip_ms"] <= 10 and schedule["drain_ms"] <= 5_000
    resource_pass = resources["authoritative"] and resources["memory_peak_bytes"] < resources["memory_max_bytes"] and resources["swap_current_bytes"] == 0 and resources["oom_kill_delta"] == 0 and resources["memory_event_high_delta"] == 0 and resources["memory_event_max_delta"] == 0 and all(resources[name] for name in ("controllers_enabled_proven", "effective_limits_proven", "host_headroom_proven", "server_descendants_proven", "loadgen_excluded_proven"))
    scenario_pass = _scenario_pass(result, scenario)
    gates = {
        "correctness": semantic["passed"],
        "resources": resource_pass,
        "schedule": bool(schedule_pass),
        "scenario": scenario_pass,
    }
    failed = not semantic["passed"] or not schedule_pass or not scenario_pass or (run["authoritative"] and not resource_pass)
    conclusive = not (scenario == "qps" and result.metrics["capacity_censoring"] != "none")
    status = "fail" if failed else ("inconclusive" if not run["authoritative"] or run["profile"] == "smoke" or not conclusive else "pass")
    document = create_scenario_result(
        scenario=scenario,
        split=split,
        suite_definition_sha256=sha256_json(definition),
        run=run,
        status=status,
        cache=cache,
        binary=binary,
        resources=resources,
        schedule=schedule,
        metrics=result.metrics,
        samples=result.samples,
        semantic=semantic,
        gates=gates,
    )
    path = output / f"{scenario}-{split}.json"
    write_json_atomic(path, document)
    return path, result


def _ordered_usage(recipe: Mapping[str, Any], purpose: str) -> tuple[tuple[str, str], ...]:
    if purpose == "comparison":
        usage = tuple((scenario, "research") for scenario in SCENARIOS[:-1]) + tuple(
            (scenario, "holdout") for scenario in SCENARIOS[:-1]
        ) + (("mixed", "stress"),)
        validate_split_usage(recipe, usage, "comparison")
        return usage
    usage = required_split_usage(recipe, "baseline")
    validate_split_usage(recipe, usage, "baseline")
    return usage


def _run_logical_arm(
    repo: Path,
    prepared: PreparedBuild,
    recipe: Mapping[str, Any],
    definition: Mapping[str, Any],
    *,
    arm: str,
    pair_id: int,
    order: str,
    purpose: str,
    authoritative: bool,
    baseline_sustainable_qps: Optional[float],
    output: Path,
) -> tuple[list[Path], float]:
    logical_reset = run_id()
    run = {
        "run_id": f"run-{arm}-{pair_id}-{logical_reset}",
        "reset_id": logical_reset,
        "arm": arm,
        "pair_id": pair_id,
        "order": order,
        "profile": recipe["profile"],
        "authoritative": authoritative,
    }
    paths: list[Path] = []
    measured_capacity = baseline_sustainable_qps
    research_passed = True
    for scenario, split in _ordered_usage(recipe, purpose):
        if purpose == "comparison" and split == "holdout" and not research_passed:
            raise SuiteFailure("holdout is prohibited because the research trigger failed")
        path, result = _run_scenario(
            repo,
            prepared,
            recipe,
            definition,
            scenario=scenario,
            split=split,
            run=run,
            baseline_sustainable_qps=measured_capacity,
            output=output,
        )
        paths.append(path)
        document = load_document(path)
        if split == "research" and document["status"] != "pass":
            research_passed = False
        if scenario == "qps" and split == "research" and measured_capacity is None:
            measured_capacity = float(result.metrics["sustainable_qps"])
    if measured_capacity is None or measured_capacity <= 0:
        raise SuiteFailure("arm did not produce a positive baseline capacity")
    return paths, measured_capacity


def _definition(
    profile: str,
    recipe: Mapping[str, Any],
    prepared: Mapping[str, PreparedBuild],
    build_recipe: Mapping[str, Any],
) -> Mapping[str, Any]:
    return create_suite_definition(
        profile=profile,
        fixture=recipe,
        build_recipe=build_recipe,
        builds=[item.protocol for item in prepared.values()],
        schedules=_schedule_hashes(recipe),
    )


def freeze(profile: str, output: Path) -> None:
    recipe = build_recipe(profile)
    validate_recipe(recipe)
    output.mkdir(parents=True, exist_ok=False)
    (output / f"fixture-{profile}.json").write_bytes(canonical_json_bytes(recipe))
    (output / f"policy-{profile}.json").write_bytes(
        canonical_json_bytes(
            {
                "profile": profile,
                "fixture_sha256": recipe["fixture_sha256"],
                "policy": policy_document(),
                "schedules": _schedule_hashes(recipe),
            }
        )
    )


def validate_path(path: Path) -> None:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SuiteFailure(f"cannot load {path}: {error}") from error
    if isinstance(document, dict) and "document_type" in document:
        validate_document(document)
    elif isinstance(document, dict) and "recipe_version" in document:
        validate_recipe(document)
    else:
        raise SuiteFailure(f"{path} is neither a protocol document nor a fixture recipe")


def run_baseline(repositories: Mapping[str, Path], profile: str, output: Path) -> list[Path]:
    if set(repositories) != {"baseline"}:
        raise SuiteFailure("baseline workflow requires exactly one baseline repository")
    recipe = build_recipe(profile)
    validate_recipe(recipe)
    output.mkdir(parents=True, exist_ok=False)
    prepared, build_recipe_document = _prepare_builds(repositories, output / "builds")
    definition = _definition(profile, recipe, prepared, build_recipe_document)
    manifest_paths: list[Path] = []
    count = 1 if profile == "smoke" else 7
    purpose = "smoke" if profile == "smoke" else "repeatability"
    for index in range(1, count + 1):
        run_root = output / f"baseline-{index:02d}"
        artifacts_root = run_root / "artifacts"
        artifacts_root.mkdir(parents=True)
        paths, _ = _run_logical_arm(
            repositories["baseline"],
            prepared["baseline"],
            recipe,
            definition,
            arm="baseline",
            pair_id=1,
            order="AB",
            purpose=purpose,
            authoritative=profile == "full",
            baseline_sustainable_qps=None,
            output=artifacts_root,
        )
        manifest_path = run_root / "manifest.json"
        manifest = create_suite_manifest(
            purpose=purpose,
            arm="baseline",
            suite_definition=definition,
            artifact_paths=paths,
            manifest_path=manifest_path,
        )
        write_json_atomic(manifest_path, manifest)
        manifest_paths.append(manifest_path)
    if profile == "full":
        write_json_atomic(output / "repeatability.json", evaluate_repeatability(manifest_paths))
    return manifest_paths


def run_comparison(
    repositories: Mapping[str, Path],
    baseline_manifests: Sequence[Path],
    output: Path,
) -> tuple[Path, Path, Path]:
    if set(repositories) != {"baseline", "candidate"}:
        raise SuiteFailure("comparison requires baseline and candidate repositories")
    repeatability = evaluate_repeatability(baseline_manifests)
    if repeatability["status"] != "pass":
        raise SuiteFailure("comparison requires a passing seven-run baseline repeatability study")
    baseline_capacity = statistics.median(repeatability["metrics"]["values"]["qps"])
    recipe = build_recipe("full")
    validate_recipe(recipe)
    output.mkdir(parents=True, exist_ok=False)
    prepared, build_recipe_document = _prepare_builds(repositories, output / "builds")
    definition = _definition("full", recipe, prepared, build_recipe_document)
    all_paths: dict[str, list[Path]] = {"baseline": [], "candidate": []}
    for pair_id, order in enumerate(PAIR_ORDER, 1):
        sequence = ("baseline", "candidate") if order == "AB" else ("candidate", "baseline")
        for arm in sequence:
            artifacts_root = output / arm / f"pair-{pair_id}" / "artifacts"
            artifacts_root.mkdir(parents=True)
            paths, _ = _run_logical_arm(
                repositories[arm],
                prepared[arm],
                recipe,
                definition,
                arm=arm,
                pair_id=pair_id,
                order=order,
                purpose="comparison",
                authoritative=True,
                baseline_sustainable_qps=baseline_capacity,
                output=artifacts_root,
            )
            all_paths[arm].extend(paths)
    manifests: dict[str, Path] = {}
    for arm in ("baseline", "candidate"):
        manifest_path = output / arm / "manifest.json"
        manifest = create_suite_manifest(
            purpose="comparison",
            arm=arm,
            suite_definition=definition,
            artifact_paths=all_paths[arm],
            manifest_path=manifest_path,
        )
        write_json_atomic(manifest_path, manifest)
        manifests[arm] = manifest_path
    comparison_path = output / "comparison.json"
    write_json_atomic(comparison_path, compare_manifests(manifests["baseline"], manifests["candidate"]))
    return manifests["baseline"], manifests["candidate"], comparison_path


def _parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    freeze_parser = commands.add_parser("freeze")
    freeze_parser.add_argument("--profile", choices=("smoke", "full"), default="full")
    freeze_parser.add_argument("--output", type=Path, required=True)
    validate_parser = commands.add_parser("validate")
    validate_parser.add_argument("paths", type=Path, nargs="+")
    compare_parser = commands.add_parser("compare")
    compare_parser.add_argument("baseline", type=Path)
    compare_parser.add_argument("candidate", type=Path)
    compare_parser.add_argument("--output", type=Path, required=True)
    repeatability_parser = commands.add_parser("repeatability")
    repeatability_parser.add_argument("manifests", type=Path, nargs=7)
    repeatability_parser.add_argument("--output", type=Path, required=True)
    smoke_parser = commands.add_parser("smoke")
    smoke_parser.add_argument("--repo", type=Path, default=Path.cwd())
    smoke_parser.add_argument("--output", type=Path, required=True)
    run_parser = commands.add_parser("run")
    run_parser.add_argument("--baseline", type=Path, required=True)
    run_parser.add_argument("--candidate", type=Path)
    run_parser.add_argument("--baseline-manifests", type=Path, nargs=7)
    run_parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        if args.command == "freeze":
            freeze(args.profile, args.output)
        elif args.command == "validate":
            for path in args.paths:
                validate_path(path)
        elif args.command == "compare":
            write_json_atomic(args.output, compare_manifests(args.baseline, args.candidate))
        elif args.command == "repeatability":
            write_json_atomic(args.output, evaluate_repeatability(args.manifests))
        elif args.command == "smoke":
            run_baseline({"baseline": args.repo.resolve()}, "smoke", args.output)
        elif args.candidate is None:
            if args.baseline_manifests is not None:
                raise SuiteFailure("baseline-only run does not accept --baseline-manifests")
            run_baseline({"baseline": args.baseline.resolve()}, "full", args.output)
        else:
            if args.baseline_manifests is None:
                raise SuiteFailure("comparison run requires seven --baseline-manifests")
            run_comparison(
                {"baseline": args.baseline.resolve(), "candidate": args.candidate.resolve()},
                args.baseline_manifests,
                args.output,
            )
        return 0
    except (
        ProtocolError,
        RuntimeFailure,
        ScenarioError,
        SuiteFailure,
        OSError,
        ValueError,
        json.JSONDecodeError,
    ) as error:
        print(f"performance-suite: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
