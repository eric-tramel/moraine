#!/usr/bin/env python3
"""Import-only fixed workload scenarios for the performance suite.

The suite owns physical sandbox lifecycle.  This module owns the frozen workload
algorithms and consumes narrow runtime protocols so deterministic tests and the
owned production sandbox exercise exactly the same policy.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import queue
import subprocess
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Optional, Protocol, Sequence
from performance_fixtures import validate_query_result


class ScenarioError(RuntimeError):
    """The workload or its fail-closed evidence is invalid."""


class AdmissionRejected(ScenarioError):
    """The central MCP admission controller rejected a request."""


class RequestTimeout(ScenarioError):
    """An MCP request exceeded its absolute deadline."""


class MalformedResult(ScenarioError):
    """An MCP result did not satisfy the public response contract."""


class McpProtocolError(ScenarioError):
    """The JSON-RPC transport returned an invalid protocol response."""


class McpToolError(ScenarioError):
    """The MCP tool returned a non-admission error."""


class CentralCrashed(ScenarioError):
    """The measured central process exited before the sample completed."""


@dataclass(frozen=True)
class ScenarioResult:
    status: str
    metrics: Mapping[str, Any]
    samples: Sequence[Mapping[str, Any]] | Mapping[str, Any]
    semantic_failures: int = 0
    diagnostics: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.status not in {"pass", "fail", "inconclusive"}:
            raise ValueError(f"invalid scenario status {self.status!r}")
        if self.semantic_failures < 0:
            raise ValueError("semantic_failures cannot be negative")


# ---------------------------------------------------------------------------
# Exact independent query oracle




def validate_query_oracle(structured: Mapping[str, Any], case: Mapping[str, Any]) -> None:
    """Delegate to the fixture-owned count/order/digest oracle."""


    validate_query_result(structured, case)


# ---------------------------------------------------------------------------
# Sustainable QPS


@dataclass(frozen=True)
class QpsPolicy:
    profile: str
    duration_s: float
    replicates: int
    maximum_qps: int
    p95_limit_ms: float = 750.0
    p99_limit_ms: float = 2_000.0
    hard_deadline_ms: float = 5_000.0
    scheduler_p99_slip_limit_ms: float = 10.0
    drain_limit_s: float = 5.0
    max_scheduler_workers: int = 4_096

    def __post_init__(self) -> None:
        if self.profile not in {"smoke", "full", "test"}:
            raise ValueError(f"invalid QPS policy profile {self.profile!r}")
        if self.duration_s <= 0 or self.replicates < 1 or self.maximum_qps < 1:
            raise ValueError("QPS policy duration, replicates, and cap must be positive")
        if self.max_scheduler_workers < 1 or self.drain_limit_s < 0:
            raise ValueError("QPS scheduler policy is invalid")


SMOKE_QPS_POLICY = QpsPolicy(profile="smoke", duration_s=3, replicates=1, maximum_qps=16)
FULL_QPS_POLICY = QpsPolicy(profile="full", duration_s=30, replicates=3, maximum_qps=512)


@dataclass(frozen=True)
class QpsTrialSpec:
    profile: str
    offered_qps: int
    replicate: int
    duration_s: float


class QpsTrialRuntime(Protocol):
    """One physically reset, thread-safe MCP trial runtime."""

    reset_id: str

    def search(self, case: Mapping[str, Any], timeout_s: float) -> Mapping[str, Any]: ...
    def telemetry(self) -> Mapping[str, Any]: ...
    def abort(self) -> None: ...
    def close(self) -> None: ...
    def leaked_work(self) -> bool: ...


@dataclass
class _QpsTrialExecution:
    sample: dict[str, Any]
    reset_id: str
    infrastructure_errors: list[str] = field(default_factory=list)


@dataclass
class _QpsRateExecution:
    offered_qps: int
    passed: bool
    trials: list[_QpsTrialExecution]

    @property
    def infrastructure_errors(self) -> list[str]:
        return [error for trial in self.trials for error in trial.infrastructure_errors]
QPS_TELEMETRY_KEYS = (
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


def _empty_qps_telemetry() -> dict[str, int]:
    return {key: 0 for key in QPS_TELEMETRY_KEYS}


def _qps_telemetry(value: Mapping[str, Any]) -> dict[str, int]:
    if set(value) != set(QPS_TELEMETRY_KEYS):
        raise ScenarioError("QPS cgroup telemetry keys are incomplete")
    telemetry: dict[str, int] = {}
    for key in QPS_TELEMETRY_KEYS:
        observed = value[key]
        if isinstance(observed, bool) or not isinstance(observed, int) or observed < 0:
            raise ScenarioError(f"QPS cgroup telemetry {key} is invalid")
        telemetry[key] = observed
    return telemetry


def _nearest_rank(values: Sequence[float], percent: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, math.ceil(percent * len(ordered) / 100.0) - 1)
    return float(ordered[index])


def _planned_arrivals(offered_qps: int, duration_s: float) -> int:
    exact = offered_qps * duration_s
    rounded = round(exact)
    if not math.isclose(exact, rounded, rel_tol=0.0, abs_tol=1e-9):
        raise ScenarioError("arrival schedule must contain an integral request count")
    return max(1, int(rounded))

def _qps_reset_sha256(reset_id: str, spec: QpsTrialSpec) -> str:
    identity = reset_id or (
        f"missing:{spec.profile}:{spec.offered_qps}:{spec.replicate}"
    )
    return "sha256:" + hashlib.sha256(identity.encode()).hexdigest()


def _empty_qps_sample(spec: QpsTrialSpec, planned: int) -> dict[str, Any]:
    return {
        "offered_qps": spec.offered_qps,
        "replicate": spec.replicate,
        "duration_s": spec.duration_s,
        "reset_sha256": _qps_reset_sha256("", spec),
        "achieved_goodput_qps": 0.0,
        "p95_ms": 0.0,
        "p99_ms": 0.0,
        "max_ms": 0.0,
        "scheduler_p99_start_slip_ms": 0.0,
        "drain_ms": 0.0,
        "drained": False,
        "passed": False,
        "outcomes": {
            "planned": planned,
            "started": 0,
            "correct": 0,
            "rejected": 0,
            "timed_out": 0,
            "semantic_error": 0,
            "protocol_error": 0,
            "malformed": 0,
            "late": 0,
            "dropped": planned,
            "other_error": 0,
        },
        "telemetry": _empty_qps_telemetry(),
    }


def _execute_open_arrival_trial(
    spec: QpsTrialSpec,
    runtime: QpsTrialRuntime,
    cases: Sequence[Mapping[str, Any]],
    oracle: Callable[[Mapping[str, Any], Mapping[str, Any]], None],
    policy: QpsPolicy,
    *,
    clock_ns: Callable[[], int] = time.perf_counter_ns,
    sleep: Callable[[float], None] = time.sleep,
) -> _QpsTrialExecution:
    planned = _planned_arrivals(spec.offered_qps, spec.duration_s)
    sample = _empty_qps_sample(spec, planned)
    reset_id = getattr(runtime, "reset_id", "")
    infrastructure_errors: list[str] = []
    outcomes = sample["outcomes"]
    correct_latencies: list[float] = []
    scheduler_slips: list[float] = []
    state_lock = threading.Lock()
    active = 0
    max_active = 0

    def execute(index: int, scheduled_ns: int) -> None:
        nonlocal active, max_active
        actual_start_ns = clock_ns()
        slip_ms = max(0.0, (actual_start_ns - scheduled_ns) / 1_000_000.0)
        with state_lock:
            outcomes["started"] += 1
            scheduler_slips.append(slip_ms)
            active += 1
            max_active = max(max_active, active)
        case = cases[index % len(cases)]
        outcome = "other_error"
        latency_ms = 0.0
        try:
            structured = runtime.search(case, policy.hard_deadline_ms / 1_000.0)
            latency_ms = (clock_ns() - actual_start_ns) / 1_000_000.0
            if latency_ms > policy.hard_deadline_ms:
                outcome = "late"
            else:
                try:
                    oracle(structured, case)
                except MalformedResult:
                    outcome = "malformed"
                except BaseException:
                    outcome = "semantic_error"
                else:
                    outcome = "correct"
        except AdmissionRejected:
            outcome = "rejected"
        except RequestTimeout:
            outcome = "timed_out"
        except MalformedResult:
            outcome = "malformed"
        except (McpProtocolError, McpToolError):
            outcome = "protocol_error"
        except BaseException:
            outcome = "other_error"
        finally:
            with state_lock:
                outcomes[outcome] += 1
                if outcome == "correct":
                    correct_latencies.append(latency_ms)
                active -= 1

    executor = ThreadPoolExecutor(max_workers=policy.max_scheduler_workers, thread_name_prefix="moraine-qps")
    futures: list[Future[None]] = []
    started_ns = clock_ns()
    interval_end_ns = started_ns + int(spec.duration_s * 1_000_000_000)
    initial_pending: set[Future[None]] = set()
    try:
        for index in range(planned):
            scheduled_ns = started_ns + int(index * 1_000_000_000 / spec.offered_qps)
            delay_s = (scheduled_ns - clock_ns()) / 1_000_000_000.0
            if delay_s > 0:
                sleep(delay_s)
            if clock_ns() >= interval_end_ns:
                break
            futures.append(executor.submit(execute, index, scheduled_ns))

        until_end_s = (interval_end_ns - clock_ns()) / 1_000_000_000.0
        if until_end_s > 0:
            sleep(until_end_s)
        backlog_at_end = sum(not future.done() for future in futures)
        _, initial_pending = wait(futures, timeout=policy.drain_limit_s)
        observed_drain_ms = max(0.0, (clock_ns() - interval_end_ns) / 1_000_000.0)
        sample["drain_ms"] = min(observed_drain_ms, policy.drain_limit_s * 1_000.0)
        sample["drained"] = not initial_pending
        if initial_pending:
            try:
                runtime.abort()
            except BaseException:
                infrastructure_errors.append("abort_failed")
            _, lingering = wait(initial_pending, timeout=1.0)
            if lingering:
                infrastructure_errors.append("worker_leak")
        else:
            lingering = set()

        try:
            sample["telemetry"] = _qps_telemetry(runtime.telemetry())
        except BaseException:
            infrastructure_errors.append("telemetry_failed")
            sample["telemetry"] = _empty_qps_telemetry()

        with state_lock:
            outcomes["dropped"] = planned - outcomes["started"]
            latencies = tuple(correct_latencies)
            slips = tuple(scheduler_slips)
            observed_max_active = max_active
        sample["p95_ms"] = _nearest_rank(latencies, 95.0)
        sample["p99_ms"] = _nearest_rank(latencies, 99.0)
        sample["max_ms"] = max(latencies, default=0.0)
        sample["scheduler_p99_start_slip_ms"] = _nearest_rank(slips, 99.0)
        sample["achieved_goodput_qps"] = outcomes["correct"] / spec.duration_s
        sample["backlog_at_end"] = backlog_at_end
        sample["max_in_flight"] = observed_max_active
    finally:
        executor.shutdown(wait=not initial_pending, cancel_futures=True)
        try:
            runtime.close()
        except BaseException:
            infrastructure_errors.append("cleanup_failed")
        try:
            if runtime.leaked_work():
                infrastructure_errors.append("runtime_leak")
        except BaseException:
            infrastructure_errors.append("leak_evidence_missing")

    adverse = sum(
        outcomes[name]
        for name in (
            "rejected",
            "timed_out",
            "semantic_error",
            "protocol_error",
            "malformed",
            "late",
            "dropped",
            "other_error",
        )
    )
    terminal = sum(
        outcomes[name]
        for name in outcomes
        if name not in {"planned", "started", "dropped"}
    )
    if outcomes["started"] != terminal or planned != outcomes["started"] + outcomes["dropped"]:
        infrastructure_errors.append("outcome_count_mismatch")
    sample["passed"] = bool(
        not infrastructure_errors
        and outcomes["started"] == planned
        and adverse == 0
        and sample["p95_ms"] <= policy.p95_limit_ms
        and sample["p99_ms"] <= policy.p99_limit_ms
        and sample["max_ms"] <= policy.hard_deadline_ms
        and sample["scheduler_p99_start_slip_ms"] <= policy.scheduler_p99_slip_limit_ms
        and sample["drained"]
        and sample["drain_ms"] <= policy.drain_limit_s * 1_000.0
        and sample["telemetry"]["swap_current_bytes"] == 0
        and sample["telemetry"]["oom_kill_delta"] == 0
        and sample["telemetry"]["memory_peak_bytes"] < 8 * 1024**3
    )
    return _QpsTrialExecution(sample, reset_id, infrastructure_errors)


def _failed_qps_trial(spec: QpsTrialSpec, policy: QpsPolicy, error: str) -> _QpsTrialExecution:
    planned = _planned_arrivals(spec.offered_qps, spec.duration_s)
    sample = _empty_qps_sample(spec, planned)
    sample["outcomes"]["other_error"] = 1
    return _QpsTrialExecution(sample, "", [error])


def run_qps_sweep(
    cases: Sequence[Mapping[str, Any]],
    runtime_factory: Callable[[QpsTrialSpec], QpsTrialRuntime],
    oracle: Callable[[Mapping[str, Any], Mapping[str, Any]], None],
    policy: QpsPolicy,
    *,
    trial_executor: Optional[
        Callable[
            [
                QpsTrialSpec,
                QpsTrialRuntime,
                Sequence[Mapping[str, Any]],
                Callable[[Mapping[str, Any], Mapping[str, Any]], None],
                QpsPolicy,
            ],
            _QpsTrialExecution,
        ]
    ] = None,
) -> ScenarioResult:
    """Run the open-arrival doubling/binary sweep under an explicit policy.

    The public suite wrapper below supplies only the frozen smoke/full policies;
    this lower-level entry point exists so tests can exercise every branch without
    weakening production constants.
    """

    if not cases:
        raise ScenarioError("QPS scenario requires at least one query case")
    attempted: list[_QpsRateExecution] = []
    reset_ids: set[str] = set()
    diagnostics: list[str] = []
    fatal = False
    execute_trial = trial_executor or _execute_open_arrival_trial

    def measure(offered_qps: int) -> _QpsRateExecution:
        nonlocal fatal
        trials: list[_QpsTrialExecution] = []
        for replicate in range(1, policy.replicates + 1):
            spec = QpsTrialSpec(policy.profile, offered_qps, replicate, policy.duration_s)
            if fatal:
                trial = _failed_qps_trial(spec, policy, "replicate_not_run_after_uncontained_leak")
            else:
                runtime: Optional[QpsTrialRuntime] = None
                try:
                    runtime = runtime_factory(spec)
                    trial = execute_trial(spec, runtime, cases, oracle, policy)
                except BaseException:
                    if runtime is not None:
                        try:
                            runtime.abort()
                            runtime.close()
                        except BaseException:
                            fatal = True
                    trial = _failed_qps_trial(spec, policy, "replicate_failed")
            if not trial.reset_id:
                trial.infrastructure_errors.append("reset_identity_missing")
            elif trial.reset_id in reset_ids:
                trial.infrastructure_errors.append("reset_identity_reused")
            else:
                reset_ids.add(trial.reset_id)
            trial.sample["reset_sha256"] = _qps_reset_sha256(trial.reset_id, spec)
            if any(error in {"worker_leak", "runtime_leak", "cleanup_failed", "leak_evidence_missing"} for error in trial.infrastructure_errors):
                fatal = True
            trials.append(trial)
        rate = _QpsRateExecution(offered_qps, all(trial.sample["passed"] for trial in trials), trials)
        attempted.append(rate)
        return rate

    if policy.profile == "smoke":
        smoke = measure(policy.maximum_qps)
        lower = policy.maximum_qps if smoke.passed else 0
        upper = policy.maximum_qps
        censoring = "right" if smoke.passed else "none"
    else:
        bottom = measure(1)
        lower = 0
        upper = 1
        censoring = "left"
        if bottom.passed:
            lower = 1
            current = 1
            first_failure: Optional[int] = None
            while current < policy.maximum_qps and not fatal:
                next_rate = min(policy.maximum_qps, current * 2)
                measured = measure(next_rate)
                if measured.passed:
                    lower = next_rate
                    current = next_rate
                else:
                    first_failure = next_rate
                    break
            if first_failure is None and lower == policy.maximum_qps:
                upper = lower
                censoring = "right"
            elif first_failure is not None:
                upper = first_failure
                while upper - lower > 1 and not fatal:
                    midpoint = (lower + upper) // 2
                    measured = measure(midpoint)
                    if measured.passed:
                        lower = midpoint
                    else:
                        upper = midpoint
                censoring = "none"

    samples: list[dict[str, Any]] = []
    correctness_failures = 0
    infrastructure_failures = 0
    for rate in attempted:
        for trial in rate.trials:
            sample = dict(trial.sample)
            # Internal-only scheduling diagnostics are useful to focused tests but
            # are removed before the protocol constructor by ScenarioResult callers.
            sample.pop("backlog_at_end", None)
            sample.pop("max_in_flight", None)
            samples.append(sample)
            correctness_failures += sum(
                sample["outcomes"][name]
                for name in ("semantic_error", "protocol_error", "malformed", "other_error")
            )
            infrastructure_failures += len(trial.infrastructure_errors)
            diagnostics.extend(trial.infrastructure_errors)

    metrics = {
        "direction": "higher",
        "sustainable_qps": lower,
        "capacity_lower_qps": lower,
        "capacity_upper_qps": upper,
        "capacity_censoring": censoring,
    }
    if correctness_failures or infrastructure_failures or fatal:
        status = "fail"
    elif policy.profile == "smoke":
        status = "pass" if lower > 0 else "fail"
    elif censoring != "none":
        status = "inconclusive"
    else:
        status = "pass"
    return ScenarioResult(
        status=status,
        metrics=metrics,
        samples=tuple(samples),
        semantic_failures=correctness_failures,
        diagnostics=tuple(sorted(set(diagnostics))),
    )


def run_qps_scenario(
    cases: Sequence[Mapping[str, Any]],
    runtime_factory: Callable[[QpsTrialSpec], QpsTrialRuntime],
    oracle: Callable[[Mapping[str, Any], Mapping[str, Any]], None] = validate_query_oracle,
    *,
    profile: str,
) -> ScenarioResult:
    try:
        policy = {"smoke": SMOKE_QPS_POLICY, "full": FULL_QPS_POLICY}[profile]
    except KeyError as exc:
        raise ScenarioError(f"unsupported QPS profile {profile!r}") from exc
    return run_qps_sweep(cases, runtime_factory, oracle, policy)


# ---------------------------------------------------------------------------
# Fresh-central-process first-valid-result TTR


@dataclass(frozen=True)
class CentralIdentity:
    pid: int
    starttime_ticks: int
    cache_generation: str


@dataclass(frozen=True)
class CacheState:
    label: str
    central_process: str
    mcp_caches: str
    target_query_prewarmed: bool
    clickhouse: str
    os_page_cache: str
    generation: str


@dataclass(frozen=True)
class RouteEvidence:
    benchmark_searches: int
    central_requests: int
    route_marker: str
    central_proxy_observed: bool
    embedded_fallback: bool


@dataclass(frozen=True)
class TtrSampleSpec:
    sample_id: str
    case: Mapping[str, Any]


class TtrRuntime(Protocol):
    reset_id: str

    def previous_identity(self) -> CentralIdentity: ...
    def spawn_central(self) -> None: ...
    def wait_exec(self, deadline_ns: int) -> CentralIdentity: ...
    def wait_ready(self, deadline_ns: int) -> None: ...
    def connect_route(self, deadline_ns: int) -> None: ...
    def initialize(self, deadline_ns: int) -> None: ...
    def list_tools(self, deadline_ns: int) -> Sequence[str]: ...
    def cache_state(self, deadline_ns: int) -> CacheState: ...
    def route_evidence(self, deadline_ns: int) -> RouteEvidence: ...
    def write_search(self, case: Mapping[str, Any], deadline_ns: int) -> None: ...
    def read_search(self, deadline_ns: int) -> Mapping[str, Any]: ...
    def central_alive(self) -> bool: ...
    def close(self) -> None: ...
    def surviving_children(self) -> Sequence[int]: ...


_TTR_PHASES = (
    "spawn_exec",
    "central_readiness",
    "route_connect",
    "initialize_tools",
    "request_write",
    "response_read",
    "oracle_validation",
)


def _hash_private(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode()).hexdigest()


def _fresh_identity(previous: CentralIdentity, current: CentralIdentity) -> bool:
    return bool(
        previous.pid > 0
        and current.pid > 0
        and previous.pid != current.pid
        and previous.starttime_ticks > 0
        and current.starttime_ticks > 0
        and previous.starttime_ticks != current.starttime_ticks
        and previous.cache_generation
        and current.cache_generation
        and previous.cache_generation != current.cache_generation
    )


def _valid_cache_state(state: CacheState, identity: CentralIdentity) -> bool:
    return bool(
        state.label == "fresh_moraine_existing_clickhouse"
        and state.central_process == "fresh"
        and state.mcp_caches == "fresh"
        and state.target_query_prewarmed is False
        and state.clickhouse == "seed_warmed"
        and state.os_page_cache in {"uncontrolled", "privileged_reset"}
        and state.generation == identity.cache_generation
    )


def _empty_ttr_sample(sample_id: str) -> dict[str, Any]:
    return {
        "sample_id": sample_id,
        "total_ms": 0.0,
        "phases_ms": {phase: 0.0 for phase in _TTR_PHASES},
        "pid_start_sha256": _hash_private("missing"),
        "cache_generation_sha256": _hash_private("missing"),
        "route_marker_sha256": _hash_private("missing"),
        "searches_before_write": 0,
        "central_request_count": 0,
        "embedded_fallback": True,
        "valid": False,
        "error_code": "runtime_not_started",
    }


def _deadline_check(clock_ns: Callable[[], int], deadline_ns: int) -> None:
    if clock_ns() > deadline_ns:
        raise RequestTimeout("TTR sample timed out")


def _run_ttr_sample(
    spec: TtrSampleSpec,
    runtime: TtrRuntime,
    oracle: Callable[[Mapping[str, Any], Mapping[str, Any]], None],
    timeout_s: float,
    clock_ns: Callable[[], int],
    seen_identities: set[tuple[int, int, str]],
) -> tuple[dict[str, Any], int, list[str], bool]:
    sample = _empty_ttr_sample(spec.sample_id)
    phase_ns = {phase: 0 for phase in _TTR_PHASES}
    phase_index = 0
    error_code = ""
    semantic_failures = 0
    diagnostics: list[str] = []
    unsafe_cleanup = False
    current: Optional[CentralIdentity] = None
    before: Optional[RouteEvidence] = None
    phase_started_ns = 0
    started_ns = 0
    deadline_ns = 0
    endpoint_ns = 0

    def finish_phase() -> None:
        nonlocal phase_index, phase_started_ns, endpoint_ns
        now = clock_ns()
        phase_ns[_TTR_PHASES[phase_index]] += max(0, now - phase_started_ns)
        endpoint_ns = now
        phase_started_ns = now
        phase_index += 1
        _deadline_check(clock_ns, deadline_ns)

    previous: Optional[CentralIdentity] = None
    previous_error: Optional[BaseException] = None
    try:
        previous = runtime.previous_identity()
        if (
            previous.pid <= 0
            or previous.starttime_ticks <= 0
            or not previous.cache_generation
        ):
            raise ScenarioError("previous central identity is invalid")
    except BaseException as exc:
        previous_error = exc

    phase_started_ns = clock_ns()
    started_ns = phase_started_ns
    deadline_ns = started_ns + int(timeout_s * 1_000_000_000)
    endpoint_ns = started_ns
    try:
        if previous_error is not None or previous is None:
            raise ScenarioError("previous central identity is unavailable") from previous_error
        runtime.spawn_central()
        current = runtime.wait_exec(deadline_ns)
        finish_phase()
        if not _fresh_identity(previous, current):
            raise ScenarioError("central process identity was not fresh")
        identity_key = (current.pid, current.starttime_ticks, current.cache_generation)
        if identity_key in seen_identities:
            raise ScenarioError("central process identity was reused")
        seen_identities.add(identity_key)

        runtime.wait_ready(deadline_ns)
        finish_phase()
        if not runtime.central_alive():
            raise CentralCrashed("central exited during readiness")

        runtime.connect_route(deadline_ns)
        finish_phase()

        runtime.initialize(deadline_ns)
        tools = runtime.list_tools(deadline_ns)
        if "search_sessions" not in tools:
            raise McpProtocolError("search_sessions tool missing")
        cache_state = runtime.cache_state(deadline_ns)
        if not _valid_cache_state(cache_state, current):
            raise ScenarioError("layered cache evidence is invalid")
        before = runtime.route_evidence(deadline_ns)
        if (
            before.benchmark_searches != 0
            or before.central_requests != 0
            or not before.central_proxy_observed
            or not before.route_marker
            or before.embedded_fallback
        ):
            raise ScenarioError("pre-search central-route evidence is invalid")
        finish_phase()

        runtime.write_search(spec.case, deadline_ns)
        finish_phase()

        structured = runtime.read_search(deadline_ns)
        finish_phase()

        try:
            oracle(structured, spec.case)
        except MalformedResult:
            raise
        except BaseException as exc:
            semantic_failures += 1
            raise ScenarioError("independent oracle rejected first result") from exc
        finish_phase()
        endpoint_ns = clock_ns()

        if not runtime.central_alive():
            raise CentralCrashed("central exited before route proof")
        after = runtime.route_evidence(deadline_ns)
        central_request_count = after.central_requests - before.central_requests
        searches_before_write = before.benchmark_searches
        embedded_fallback = before.embedded_fallback or after.embedded_fallback
        route_marker = after.route_marker or before.route_marker
        if (
            searches_before_write != 0
            or after.benchmark_searches - before.benchmark_searches != 1
            or central_request_count != 1
            or embedded_fallback
            or not after.central_proxy_observed
            or not route_marker
            or route_marker != before.route_marker
        ):
            raise ScenarioError("post-search central-route evidence is invalid")

        sample.update(
            {
                "pid_start_sha256": _hash_private(f"{current.pid}:{current.starttime_ticks}"),
                "cache_generation_sha256": _hash_private(current.cache_generation),
                "route_marker_sha256": _hash_private(route_marker),
                "searches_before_write": searches_before_write,
                "central_request_count": central_request_count,
                "embedded_fallback": embedded_fallback,
                "valid": True,
                "error_code": None,
            }
        )
    except RequestTimeout:
        error_code = "timeout"
    except CentralCrashed:
        error_code = "central_crash"
    except MalformedResult:
        error_code = "malformed_result"
    except (McpProtocolError, McpToolError, AdmissionRejected):
        error_code = "mcp_error"
    except ScenarioError as exc:
        text = str(exc)
        if "identity" in text:
            error_code = "freshness"
        elif "cache" in text:
            error_code = "cache_state"
        elif "route" in text:
            error_code = "route_proof"
        elif "oracle" in text:
            error_code = "oracle"
        else:
            error_code = "scenario_error"
    except BaseException:
        error_code = "runtime_error"
    finally:
        if phase_index < len(_TTR_PHASES) and not sample["valid"]:
            now = clock_ns()
            phase_ns[_TTR_PHASES[phase_index]] += max(0, now - phase_started_ns)
            endpoint_ns = now
        try:
            runtime.close()
        except BaseException:
            diagnostics.append("cleanup_failed")
            error_code = error_code or "cleanup"
            sample["valid"] = False
        try:
            survivors = tuple(runtime.surviving_children())
        except BaseException:
            survivors = (-1,)
            diagnostics.append("child_evidence_missing")
        if survivors:
            diagnostics.append("surviving_child")
            error_code = "surviving_child"
            sample["valid"] = False
            unsafe_cleanup = True

    phases_ms = {phase: phase_ns[phase] / 1_000_000.0 for phase in _TTR_PHASES}
    sample["phases_ms"] = phases_ms
    sample["total_ms"] = sum(phases_ms.values())
    if current is not None:
        sample["pid_start_sha256"] = _hash_private(f"{current.pid}:{current.starttime_ticks}")
        sample["cache_generation_sha256"] = _hash_private(current.cache_generation)
    if not sample["valid"]:
        sample["error_code"] = error_code or "invalid"
    # The endpoint intentionally excludes route-proof reads and cleanup.  The
    # phase sum is the reconciled spawn-through-oracle interval by construction.
    if abs(sample["total_ms"] - (endpoint_ns - started_ns) / 1_000_000.0) > 1e-6:
        sample["valid"] = False
        sample["error_code"] = "phase_reconciliation"
        diagnostics.append("phase_reconciliation")
    return sample, semantic_failures, diagnostics, unsafe_cleanup


def run_ttr_scenario(
    cases: Sequence[Mapping[str, Any]],
    runtime_factory: Callable[[TtrSampleSpec], TtrRuntime],
    oracle: Callable[[Mapping[str, Any], Mapping[str, Any]], None] = validate_query_oracle,
    *,
    samples: int,
    timeout_s: float = 30.0,
    clock_ns: Callable[[], int] = time.perf_counter_ns,
) -> ScenarioResult:
    if not cases or samples < 1 or timeout_s <= 0:
        raise ScenarioError("TTR scenario requires cases, samples, and a positive timeout")
    raw: list[dict[str, Any]] = []
    diagnostics: list[str] = []
    semantic_failures = 0
    seen_identities: set[tuple[int, int, str]] = set()
    unsafe_cleanup = False
    for index in range(samples):
        sample_id = f"ttr-{index + 1:03d}"
        spec = TtrSampleSpec(sample_id, cases[index % len(cases)])
        if unsafe_cleanup:
            sample = _empty_ttr_sample(sample_id)
            sample["error_code"] = "not_run_after_child_leak"
            raw.append(sample)
            diagnostics.append("sample_not_run_after_child_leak")
            continue
        runtime: Optional[TtrRuntime] = None
        try:
            runtime = runtime_factory(spec)
            sample, failures, sample_diagnostics, unsafe_cleanup = _run_ttr_sample(
                spec, runtime, oracle, timeout_s, clock_ns, seen_identities
            )
            semantic_failures += failures
            diagnostics.extend(sample_diagnostics)
        except BaseException:
            sample = _empty_ttr_sample(sample_id)
            sample["error_code"] = "runtime_factory"
            diagnostics.append("runtime_factory_failed")
        raw.append(sample)

    totals = [sample["total_ms"] for sample in raw]
    metrics = {
        "direction": "lower",
        "p95_ms": _nearest_rank(totals, 95.0),
        "sample_count": len(raw),
    }
    status = "pass" if all(sample["valid"] for sample in raw) else "fail"
    return ScenarioResult(status, metrics, tuple(raw), semantic_failures, tuple(sorted(set(diagnostics))))


# ---------------------------------------------------------------------------
# Production stdio MCP adapters


@dataclass
class _PendingRpc:
    response: "queue.Queue[object]"


class _StdioJsonRpcClient:
    """Thread-safe, multiplexed newline-delimited JSON-RPC client."""

    ROUTE_MARKER = "proxying stdio to central MCP server"

    def __init__(self, proc: subprocess.Popen[bytes]) -> None:
        if proc.stdin is None or proc.stdout is None or proc.stderr is None:
            raise ScenarioError("stdio route must expose stdin, stdout, and stderr")
        self.proc = proc
        self._write_lock = threading.Lock()
        self._pending_lock = threading.Lock()
        self._pending: dict[int, _PendingRpc] = {}
        self._next_id = 1
        self._closed = False
        self._route_marker = threading.Event()
        self._reader = threading.Thread(target=self._read_stdout, name="moraine-mcp-reader", daemon=True)
        self._stderr_reader = threading.Thread(target=self._read_stderr, name="moraine-mcp-stderr", daemon=True)
        self._reader.start()
        self._stderr_reader.start()

    def _fail_pending(self, error: BaseException) -> None:
        with self._pending_lock:
            pending = tuple(self._pending.values())
            self._pending.clear()
        for item in pending:
            item.response.put(error)

    def _read_stdout(self) -> None:
        assert self.proc.stdout is not None
        try:
            for raw in iter(self.proc.stdout.readline, b""):
                try:
                    response = json.loads(raw)
                    request_id = response.get("id")
                    if not isinstance(request_id, int):
                        raise ValueError("response id missing")
                except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
                    self._fail_pending(McpProtocolError("MCP response was malformed"))
                    continue
                with self._pending_lock:
                    pending = self._pending.pop(request_id, None)
                if pending is not None:
                    pending.response.put(response)
        finally:
            self._fail_pending(McpProtocolError("MCP route closed before response"))

    def _read_stderr(self) -> None:
        assert self.proc.stderr is not None
        for raw in iter(self.proc.stderr.readline, b""):
            if self.ROUTE_MARKER.encode() in raw:
                self._route_marker.set()

    def wait_for_central_route(self, timeout_s: float) -> str:
        if not self._route_marker.wait(timeout_s):
            raise ScenarioError("central proxy route marker was not observed")
        return self.ROUTE_MARKER

    def begin_call(self, method: str, params: Mapping[str, Any]) -> tuple[int, _PendingRpc]:
        if self._closed or self.proc.poll() is not None:
            raise McpProtocolError("MCP route is not running")
        with self._pending_lock:
            request_id = self._next_id
            self._next_id += 1
            pending = _PendingRpc(queue.Queue(maxsize=1))
            self._pending[request_id] = pending
        request = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": dict(params)}
        encoded = json.dumps(request, separators=(",", ":")).encode() + b"\n"
        try:
            with self._write_lock:
                assert self.proc.stdin is not None
                self.proc.stdin.write(encoded)
                self.proc.stdin.flush()
        except BaseException as exc:
            with self._pending_lock:
                self._pending.pop(request_id, None)
            raise McpProtocolError("MCP request write failed") from exc
        return request_id, pending

    def finish_call(self, request_id: int, pending: _PendingRpc, timeout_s: float) -> Mapping[str, Any]:
        try:
            response = pending.response.get(timeout=max(0.0, timeout_s))
        except queue.Empty as exc:
            with self._pending_lock:
                self._pending.pop(request_id, None)
            self.notify_cancel(request_id)
            raise RequestTimeout("MCP request timed out") from exc
        if isinstance(response, BaseException):
            raise response
        if not isinstance(response, Mapping) or response.get("id") != request_id:
            raise McpProtocolError("MCP response id mismatch")
        error = response.get("error")
        if isinstance(error, Mapping):
            if error.get("code") == -32000 and error.get("message") == "server busy: concurrent request limit reached":
                raise AdmissionRejected("central MCP admission rejected the request")
            raise McpProtocolError("MCP JSON-RPC error response")
        result = response.get("result")
        if not isinstance(result, Mapping):
            raise MalformedResult("MCP response omitted result")
        return result

    def call(self, method: str, params: Mapping[str, Any], timeout_s: float) -> Mapping[str, Any]:
        request_id, pending = self.begin_call(method, params)
        return self.finish_call(request_id, pending, timeout_s)

    def notify_cancel(self, request_id: int) -> None:
        if self._closed or self.proc.poll() is not None:
            return
        payload = {
            "jsonrpc": "2.0",
            "method": "notifications/cancelled",
            "params": {"requestId": request_id, "reason": "performance deadline"},
        }
        try:
            with self._write_lock:
                assert self.proc.stdin is not None
                self.proc.stdin.write(json.dumps(payload, separators=(",", ":")).encode() + b"\n")
                self.proc.stdin.flush()
        except (BrokenPipeError, OSError):
            pass

    def initialize(self, timeout_s: float) -> None:
        result = self.call(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "moraine-performance", "version": "1"},
            },
            timeout_s,
        )
        if not isinstance(result.get("serverInfo", {}), Mapping):
            raise McpProtocolError("MCP initialize result malformed")

    def list_tools(self, timeout_s: float) -> tuple[str, ...]:
        result = self.call("tools/list", {}, timeout_s)
        tools = result.get("tools")
        if not isinstance(tools, list):
            raise MalformedResult("MCP tools/list result malformed")
        names = tuple(tool.get("name") for tool in tools if isinstance(tool, Mapping) and isinstance(tool.get("name"), str))
        return names

    @staticmethod
    def decode_search_result(result: Mapping[str, Any]) -> Mapping[str, Any]:
        if result.get("isError") is True:
            raise McpToolError("search_sessions returned a tool error")
        structured = result.get("structuredContent")
        if not isinstance(structured, Mapping):
            raise MalformedResult("search_sessions omitted structuredContent")
        return structured

    def search(self, case: Mapping[str, Any], timeout_s: float) -> Mapping[str, Any]:
        arguments = case.get("arguments")
        if not isinstance(arguments, Mapping):
            raise ScenarioError("query case omitted frozen search arguments")
        result = self.call(
            "tools/call",
            {"name": "search_sessions", "arguments": dict(arguments)},
            timeout_s,
        )
        return self.decode_search_result(result)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            if self.proc.stdin is not None:
                self.proc.stdin.close()
        except OSError:
            pass
        try:
            self.proc.wait(timeout=1.0)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(self.proc.pid, 15)
            except ProcessLookupError:
                pass
            try:
                self.proc.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(self.proc.pid, 9)
                except ProcessLookupError:
                    pass
                self.proc.wait(timeout=1.0)
        self._fail_pending(McpProtocolError("MCP route closed"))


class QpsResetControl(Protocol):
    reset_id: str

    def spawn_stdio_route(self) -> subprocess.Popen[bytes]: ...
    def telemetry(self) -> Mapping[str, Any]: ...
    def close(self) -> None: ...
    def leaked_work(self) -> bool: ...


class StdioQpsTrialRuntime:
    """Concrete production QPS runtime over the owned central stdio proxy."""

    def __init__(self, control: QpsResetControl, route_timeout_s: float = 10.0) -> None:
        self._control = control
        self.reset_id = control.reset_id
        self._client = _StdioJsonRpcClient(control.spawn_stdio_route())
        try:
            self._client.wait_for_central_route(route_timeout_s)
            self._client.initialize(route_timeout_s)
            if "search_sessions" not in self._client.list_tools(route_timeout_s):
                raise McpProtocolError("search_sessions tool missing")
        except BaseException:
            self._client.close()
            control.close()
            raise

    def search(self, case: Mapping[str, Any], timeout_s: float) -> Mapping[str, Any]:
        return self._client.search(case, timeout_s)

    def telemetry(self) -> Mapping[str, Any]:
        return self._control.telemetry()

    def abort(self) -> None:
        self._client.close()

    def close(self) -> None:
        self._client.close()
        self._control.close()

    def leaked_work(self) -> bool:
        return self._client.proc.poll() is None or self._control.leaked_work()


def make_stdio_qps_runtime_factory(
    reset_factory: Callable[[QpsTrialSpec], QpsResetControl],
) -> Callable[[QpsTrialSpec], StdioQpsTrialRuntime]:
    """Bind the owned physical-reset primitive to the production stdio adapter."""

    def factory(spec: QpsTrialSpec) -> StdioQpsTrialRuntime:
        return StdioQpsTrialRuntime(reset_factory(spec))

    return factory


class OwnedSandboxQpsPrimitives(Protocol):
    """The production sandbox surface required by a QPS trial."""

    sandbox_id: str

    def spawn_stdio_route(self) -> subprocess.Popen[bytes]: ...
    def down(self) -> None: ...


class OwnedSandboxQpsControl:
    """Concrete physical-reset control for one owned sandbox."""

    def __init__(
        self,
        sandbox: OwnedSandboxQpsPrimitives,
        telemetry_reader: Callable[[OwnedSandboxQpsPrimitives], Mapping[str, Any]],
    ) -> None:
        self._sandbox = sandbox
        self._telemetry_reader = telemetry_reader
        self.reset_id = sandbox.sandbox_id
        self._closed = False
        self._cleanup_succeeded = False

    def spawn_stdio_route(self) -> subprocess.Popen[bytes]:
        return self._sandbox.spawn_stdio_route()

    def telemetry(self) -> Mapping[str, Any]:
        return self._telemetry_reader(self._sandbox)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._sandbox.down()
        self._cleanup_succeeded = True

    def leaked_work(self) -> bool:
        return not self._cleanup_succeeded


def make_owned_sandbox_qps_runtime_factory(
    sandbox_factory: Callable[[QpsTrialSpec], OwnedSandboxQpsPrimitives],
    telemetry_reader: Callable[[OwnedSandboxQpsPrimitives], Mapping[str, Any]],
) -> Callable[[QpsTrialSpec], StdioQpsTrialRuntime]:
    """Create one new owned sandbox/volume for every rate replicate.

    Accepting an already-running sandbox here would permit state reuse, so the
    API deliberately requires a factory keyed by the immutable trial spec.
    """

    def factory(spec: QpsTrialSpec) -> StdioQpsTrialRuntime:
        sandbox = sandbox_factory(spec)
        control = OwnedSandboxQpsControl(sandbox, telemetry_reader)
        return StdioQpsTrialRuntime(control)

    return factory


class TtrCentralControl(Protocol):
    reset_id: str

    def previous_identity(self) -> CentralIdentity: ...
    def spawn_central(self) -> None: ...
    def wait_exec(self, deadline_ns: int) -> CentralIdentity: ...
    def wait_ready(self, deadline_ns: int) -> None: ...
    def spawn_stdio_route(self) -> subprocess.Popen[bytes]: ...
    def cache_state(self, deadline_ns: int) -> CacheState: ...
    def central_alive(self) -> bool: ...
    def close(self) -> None: ...
    def surviving_children(self) -> Sequence[int]: ...


class StdioTtrRuntime:
    """Concrete spawn-through-oracle runtime over an owned central control."""

    def __init__(self, control: TtrCentralControl, clock_ns: Callable[[], int] = time.perf_counter_ns) -> None:
        self._control = control
        self._clock_ns = clock_ns
        self.reset_id = control.reset_id
        self._client: Optional[_StdioJsonRpcClient] = None
        self._pending_search: Optional[tuple[int, _PendingRpc]] = None
        self._search_writes = 0
        self._route_marker = ""

    def _remaining(self, deadline_ns: int) -> float:
        remaining = (deadline_ns - self._clock_ns()) / 1_000_000_000.0
        if remaining <= 0:
            raise RequestTimeout("TTR deadline expired")
        return remaining

    def previous_identity(self) -> CentralIdentity:
        return self._control.previous_identity()

    def spawn_central(self) -> None:
        self._control.spawn_central()

    def wait_exec(self, deadline_ns: int) -> CentralIdentity:
        return self._control.wait_exec(deadline_ns)

    def wait_ready(self, deadline_ns: int) -> None:
        self._control.wait_ready(deadline_ns)

    def connect_route(self, deadline_ns: int) -> None:
        self._client = _StdioJsonRpcClient(self._control.spawn_stdio_route())
        self._route_marker = self._client.wait_for_central_route(self._remaining(deadline_ns))

    def initialize(self, deadline_ns: int) -> None:
        if self._client is None:
            raise McpProtocolError("MCP route is not connected")
        self._client.initialize(self._remaining(deadline_ns))

    def list_tools(self, deadline_ns: int) -> Sequence[str]:
        if self._client is None:
            raise McpProtocolError("MCP route is not connected")
        return self._client.list_tools(self._remaining(deadline_ns))

    def cache_state(self, deadline_ns: int) -> CacheState:
        return self._control.cache_state(deadline_ns)

    def route_evidence(self, deadline_ns: int) -> RouteEvidence:
        del deadline_ns
        return RouteEvidence(
            benchmark_searches=self._search_writes,
            central_requests=self._search_writes,
            route_marker=self._route_marker,
            central_proxy_observed=bool(self._route_marker),
            embedded_fallback=not bool(self._route_marker),
        )

    def write_search(self, case: Mapping[str, Any], deadline_ns: int) -> None:
        if self._client is None or self._pending_search is not None:
            raise McpProtocolError("measured search write state is invalid")
        arguments = case.get("arguments")
        if not isinstance(arguments, Mapping):
            raise ScenarioError("query case omitted frozen search arguments")
        self._pending_search = self._client.begin_call(
            "tools/call", {"name": "search_sessions", "arguments": dict(arguments)}
        )
        self._search_writes += 1
        self._remaining(deadline_ns)

    def read_search(self, deadline_ns: int) -> Mapping[str, Any]:
        if self._client is None or self._pending_search is None:
            raise McpProtocolError("measured search was not written")
        request_id, pending = self._pending_search
        self._pending_search = None
        result = self._client.finish_call(request_id, pending, self._remaining(deadline_ns))
        return self._client.decode_search_result(result)

    def central_alive(self) -> bool:
        return self._control.central_alive()

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
        self._control.close()

    def surviving_children(self) -> Sequence[int]:
        children = list(self._control.surviving_children())
        if self._client is not None and self._client.proc.poll() is None:
            children.append(self._client.proc.pid)
        return tuple(children)


class OwnedSandboxTtrPrimitives(Protocol):
    """The concrete central lifecycle exposed by ``performance_runtime``."""

    sandbox_id: str

    def central_status(self) -> Mapping[str, Any]: ...
    def stop_central(self) -> None: ...
    def spawn_central(self) -> subprocess.Popen[bytes]: ...
    def wait_central_ready_without_search(
        self,
        start_process: Optional[subprocess.Popen[bytes]] = None,
        *,
        timeout_s: float = 120.0,
    ) -> Mapping[str, Any]: ...
    def spawn_stdio_route(self) -> subprocess.Popen[bytes]: ...


def _central_identity_from_status(status: Mapping[str, Any]) -> CentralIdentity:
    central = status.get("central")
    generation = status.get("cache_generation")
    if not isinstance(central, Mapping) or not isinstance(generation, str) or not generation:
        raise ScenarioError("central lifecycle status omitted process/cache identity")
    try:
        identity = CentralIdentity(
            pid=int(central["pid"]),
            starttime_ticks=int(central["starttime"]),
            cache_generation=generation,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ScenarioError("central lifecycle status identity is malformed") from exc
    if identity.pid <= 0 or identity.starttime_ticks <= 0:
        raise ScenarioError("central lifecycle status identity is invalid")
    return identity


class OwnedSandboxTtrControl:
    """Production ``TtrCentralControl`` backed by one owned sandbox.

    Preparing/stopping the previous process happens in this constructor, before
    ``run_ttr_scenario`` captures t0.  Only the subsequent ``spawn_central`` call
    is inside the measured boundary.
    """

    def __init__(
        self,
        sandbox: OwnedSandboxTtrPrimitives,
        *,
        sample_id: str,
        clock_ns: Callable[[], int] = time.perf_counter_ns,
        os_page_cache: str = "uncontrolled",
    ) -> None:
        if os_page_cache not in {"uncontrolled", "privileged_reset"}:
            raise ScenarioError("invalid OS page-cache evidence")
        self._sandbox = sandbox
        self._clock_ns = clock_ns
        self._os_page_cache = os_page_cache
        self.reset_id = f"{sandbox.sandbox_id}:{sample_id}"
        self._start_process: Optional[subprocess.Popen[bytes]] = None
        self._current: Optional[CentralIdentity] = None
        self._survivors: tuple[int, ...] = ()
        try:
            status = sandbox.central_status()
        except BaseException:
            # A prior TTR sample intentionally left central stopped.  Create a
            # comparison identity outside the next measured boundary.
            prior_start = sandbox.spawn_central()
            status = sandbox.wait_central_ready_without_search(prior_start)
        self._previous = _central_identity_from_status(status)
        sandbox.stop_central()

    def _remaining(self, deadline_ns: int) -> float:
        remaining = (deadline_ns - self._clock_ns()) / 1_000_000_000.0
        if remaining <= 0:
            raise RequestTimeout("TTR central lifecycle deadline expired")
        return remaining

    def previous_identity(self) -> CentralIdentity:
        return self._previous

    def spawn_central(self) -> None:
        if self._start_process is not None:
            raise ScenarioError("central spawn was attempted twice")
        self._start_process = self._sandbox.spawn_central()

    def wait_exec(self, deadline_ns: int) -> CentralIdentity:
        if self._start_process is None:
            raise ScenarioError("central spawn process is missing")
        try:
            stdout, stderr = self._start_process.communicate(timeout=self._remaining(deadline_ns))
        except subprocess.TimeoutExpired as exc:
            try:
                os.killpg(self._start_process.pid, 9)
            except ProcessLookupError:
                pass
            self._start_process.wait(timeout=1.0)
            raise RequestTimeout("central exec observation timed out") from exc
        if self._start_process.returncode:
            detail = (stderr or stdout)[-1024:].decode(errors="replace").strip()
            raise CentralCrashed(f"central start process failed: {detail}")
        while self._clock_ns() < deadline_ns:
            try:
                status = self._sandbox.central_status()
                self._current = _central_identity_from_status(status)
                return self._current
            except BaseException:
                time.sleep(min(0.005, self._remaining(deadline_ns)))
        raise RequestTimeout("central exec identity was not observable")

    def wait_ready(self, deadline_ns: int) -> None:
        status = self._sandbox.wait_central_ready_without_search(
            None, timeout_s=self._remaining(deadline_ns)
        )
        observed = _central_identity_from_status(status)
        if self._current is None or observed != self._current:
            raise ScenarioError("central identity changed between exec and readiness")

    def spawn_stdio_route(self) -> subprocess.Popen[bytes]:
        return self._sandbox.spawn_stdio_route()

    def cache_state(self, deadline_ns: int) -> CacheState:
        self._remaining(deadline_ns)
        if self._current is None:
            raise ScenarioError("central cache identity is unavailable")
        return CacheState(
            label="fresh_moraine_existing_clickhouse",
            central_process="fresh",
            mcp_caches="fresh",
            target_query_prewarmed=False,
            clickhouse="seed_warmed",
            os_page_cache=self._os_page_cache,
            generation=self._current.cache_generation,
        )

    def central_alive(self) -> bool:
        if self._current is None:
            return False
        try:
            return _central_identity_from_status(self._sandbox.central_status()) == self._current
        except BaseException:
            return False

    def close(self) -> None:
        status: Optional[Mapping[str, Any]] = None
        cleanup_deadline = time.monotonic() + 2.0
        try:
            while True:
                status = self._sandbox.central_status()
                routes = status.get("route_processes")
                if not isinstance(routes, list):
                    raise ScenarioError("central status omitted route process cleanup evidence")
                self._survivors = tuple(int(pid) for pid in routes)
                if not self._survivors or time.monotonic() >= cleanup_deadline:
                    break
                time.sleep(0.02)
        except BaseException:
            status = None
            self._survivors = (-1,)
        try:
            self._sandbox.stop_central()
        except BaseException:
            if status is not None:
                children = status.get("server_children")
                if isinstance(children, list):
                    self._survivors += tuple(int(pid) for pid in children)
            raise

    def surviving_children(self) -> Sequence[int]:
        return self._survivors


def make_owned_sandbox_ttr_runtime_factory(
    sandbox: OwnedSandboxTtrPrimitives,
    *,
    clock_ns: Callable[[], int] = time.perf_counter_ns,
    os_page_cache: str = "uncontrolled",
) -> Callable[[TtrSampleSpec], StdioTtrRuntime]:
    """Construct the production fresh-central TTR runtime factory."""

    def factory(spec: TtrSampleSpec) -> StdioTtrRuntime:
        control = OwnedSandboxTtrControl(
            sandbox,
            sample_id=spec.sample_id,
            clock_ns=clock_ns,
            os_page_cache=os_page_cache,
        )
        return StdioTtrRuntime(control, clock_ns)

    return factory


def make_stdio_ttr_runtime_factory(
    control_factory: Callable[[TtrSampleSpec], TtrCentralControl],
    *,
    clock_ns: Callable[[], int] = time.perf_counter_ns,
) -> Callable[[TtrSampleSpec], StdioTtrRuntime]:
    """Bind owned central lifecycle control to the production TTR adapter."""

    def factory(spec: TtrSampleSpec) -> StdioTtrRuntime:
        return StdioTtrRuntime(control_factory(spec), clock_ns)

    return factory


# ---------------------------------------------------------------------------
# Source/database-ack-to-searchable ETD


ACK_OBSERVATION_KEYS = frozenset(
    {"batch_sequence", "event_identity_digests", "ack_monotonic_ns"}
)
CACHE_BYPASS_KEYS = (
    "result",
    "document_frequency",
    "posting",
    "corpus",
    "hydration",
)
ETD_SAMPLE_KEYS = frozenset(
    {
        "event_identity_sha256",
        "term_sha256",
        "batch_sequence",
        "publication_durable_ms",
        "db_ack_ms",
        "last_miss_ms",
        "first_hit_ms",
        "first_valid_ms",
        "source_interval",
        "db_ack_interval",
        "term_use_count",
        "cache_bypass",
        "valid",
        "error_code",
    }
)
ETD_SCHEDULER_SLIP_LIMIT_MS = 10.0


@dataclass(frozen=True)
class PublicationEvidence:
    t0_ns: int
    publication_durable_ns: int


@dataclass(frozen=True)
class EtdProbeResult:
    visible: bool
    structured_content: Mapping[str, Any]
    latency_ms: float
    error_code: Optional[str] = None


@dataclass(frozen=True)
class IngestAckObservation:
    batch_sequence: int
    event_identity_digests: tuple[str, ...]
    ack_monotonic_ns: int


class EtdSampleFailure(ScenarioError):
    def __init__(self, code: str, *, semantic: bool = False) -> None:
        super().__init__(code)
        self.code = code
        self.semantic = semantic


class IngestAckCursor:
    """Validate and correlate the allowlisted content-free sink trace stream."""

    def __init__(self, reader: Callable[[], Sequence[Mapping[str, Any]]]) -> None:
        self._reader = reader
        self._last_sequence = 0
        self._last_ack_ns = 0
        self._by_digest: dict[str, list[IngestAckObservation]] = {}
        self._lock = threading.Lock()

    def drain(self) -> None:
        with self._lock:
            raw_observations = self._reader()
            if not isinstance(raw_observations, Sequence):
                raise ScenarioError("ack reader did not return a sequence")
            for raw in raw_observations:
                if not isinstance(raw, Mapping) or set(raw) != ACK_OBSERVATION_KEYS:
                    raise ScenarioError("ack observation contains non-allowlisted fields")
                sequence = raw["batch_sequence"]
                ack_ns = raw["ack_monotonic_ns"]
                digests = raw["event_identity_digests"]
                if (
                    isinstance(sequence, bool)
                    or not isinstance(sequence, int)
                    or sequence != self._last_sequence + 1
                ):
                    raise ScenarioError("ack batch sequence is not contiguous and monotonic")
                if (
                    isinstance(ack_ns, bool)
                    or not isinstance(ack_ns, int)
                    or ack_ns < self._last_ack_ns
                ):
                    raise ScenarioError("ack timestamp is not monotonic")
                if not isinstance(digests, (list, tuple)) or not digests:
                    raise ScenarioError("ack observation has no event identity digests")
                normalized: list[str] = []
                for digest in digests:
                    if (
                        not isinstance(digest, str)
                        or len(digest) != 64
                        or any(character not in "0123456789abcdef" for character in digest)
                    ):
                        raise ScenarioError("ack event identity digest is invalid")
                    normalized.append(digest)
                observation = IngestAckObservation(sequence, tuple(normalized), ack_ns)
                for digest in normalized:
                    self._by_digest.setdefault(digest, []).append(observation)
                self._last_sequence = sequence
                self._last_ack_ns = ack_ns

    def matches(self, digest: str) -> tuple[IngestAckObservation, ...]:
        self.drain()
        with self._lock:
            return tuple(self._by_digest.get(digest, ()))


def _safe_event_destination(watched_dir: os.PathLike[str] | str, filename: str) -> str:
    if (
        not isinstance(filename, str)
        or not filename.endswith(".jsonl")
        or os.path.basename(filename) != filename
        or filename in {".", ".."}
    ):
        raise ScenarioError("event destination filename is not a watched JSONL basename")
    root = os.fspath(watched_dir)
    if not os.path.isdir(root):
        raise ScenarioError("owned watched directory is not ready")
    return os.path.join(root, filename)


def publish_event_durably(
    watched_dir: os.PathLike[str] | str,
    destination_filename: str,
    payload: bytes,
    *,
    clock_ns: Callable[[], int] = time.perf_counter_ns,
) -> PublicationEvidence:
    """Durably stage bytes beside the destination and atomically publish them."""

    if not isinstance(payload, bytes) or not payload:
        raise ScenarioError("ETD publication payload must be nonempty bytes")
    destination = _safe_event_destination(watched_dir, destination_filename)
    token_material = f"{os.getpid()}:{threading.get_ident()}:{clock_ns()}:{destination_filename}"
    token = hashlib.sha256(token_material.encode("utf-8")).hexdigest()[:20]
    stage = os.path.join(os.fspath(watched_dir), f".{destination_filename}.{token}.pending")
    file_fd: Optional[int] = None
    directory_fd: Optional[int] = None
    renamed = False
    phase = "stage_open"
    try:
        file_fd = os.open(stage, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        phase = "stage_write"
        view = memoryview(payload)
        written = 0
        while written < len(view):
            count = os.write(file_fd, view[written:])
            if count <= 0:
                raise OSError("short staged write")
            written += count
        phase = "file_fsync"
        os.fsync(file_fd)
        phase = "file_close"
        os.close(file_fd)
        file_fd = None
        phase = "directory_open"
        directory_fd = os.open(
            os.fspath(watched_dir), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        )
        phase = "stage_directory_fsync"
        os.fsync(directory_fd)
        phase = "rename"
        t0_ns = clock_ns()
        os.replace(stage, destination)
        renamed = True
        phase = "destination_directory_fsync"
        os.fsync(directory_fd)
        publication_durable_ns = clock_ns()
        return PublicationEvidence(t0_ns, publication_durable_ns)
    except OSError as exc:
        raise EtdSampleFailure(f"publication_{phase}_failed") from exc
    finally:
        if file_fd is not None:
            try:
                os.close(file_fd)
            except OSError:
                pass
        if directory_fd is not None:
            try:
                os.close(directory_fd)
            except OSError:
                pass
        if not renamed:
            try:
                os.unlink(stage)
            except FileNotFoundError:
                pass
            except OSError:
                pass


def _milliseconds(later_ns: int, earlier_ns: int) -> float:
    return max(0.0, (later_ns - earlier_ns) / 1_000_000.0)


def _term_sha256(term: Optional[str]) -> Optional[str]:
    if term is None:
        return None
    return "sha256:" + hashlib.sha256(term.encode("utf-8")).hexdigest()


def _right_interval(lower_ms: float = 0.0) -> dict[str, Any]:
    return {"lower_ms": lower_ms, "upper_ms": None, "censoring": "right"}


def _failed_etd_sample(event: Mapping[str, Any], error_code: str) -> dict[str, Any]:
    digest = event.get("expected_ack_digest")
    if isinstance(digest, str):
        digest = "sha256:" + digest
    else:
        digest = "sha256:" + hashlib.sha256(
            str(event.get("case_id", "")).encode("utf-8")
        ).hexdigest()
    return {
        "event_identity_sha256": digest,
        "term_sha256": _term_sha256(None),
        "batch_sequence": None,
        "publication_durable_ms": None,
        "db_ack_ms": None,
        "last_miss_ms": None,
        "first_hit_ms": None,
        "first_valid_ms": None,
        "source_interval": _right_interval(),
        "db_ack_interval": _right_interval(),
        "term_use_count": 0,
        "cache_bypass": {key: True for key in CACHE_BYPASS_KEYS},
        "valid": False,
        "error_code": error_code,
    }


def _sleep_until(
    target_ns: int,
    clock_ns: Callable[[], int],
    sleeper: Callable[[float], None],
) -> int:
    while True:
        now_ns = clock_ns()
        remaining_ns = target_ns - now_ns
        if remaining_ns <= 0:
            return now_ns
        sleeper(remaining_ns / 1_000_000_000.0)


def _await_ack(
    cursor: IngestAckCursor,
    digest: str,
    deadline_ns: int,
    clock_ns: Callable[[], int],
    sleeper: Callable[[float], None],
    poll_interval_s: float,
) -> tuple[IngestAckObservation, ...]:
    while True:
        matches = cursor.matches(digest)
        if matches:
            return matches
        now_ns = clock_ns()
        if now_ns >= deadline_ns:
            return ()
        sleeper(min(poll_interval_s, (deadline_ns - now_ns) / 1_000_000_000.0))


def _run_one_etd_event(
    event: Mapping[str, Any],
    scheduled_ns: int,
    watched_dir: os.PathLike[str] | str,
    probe: Callable[[Mapping[str, Any]], EtdProbeResult],
    ack_cursor: IngestAckCursor,
    watcher_ready: Callable[[], bool],
    timeout_s: float,
    poll_interval_s: float,
    clock_ns: Callable[[], int],
    sleeper: Callable[[float], None],
    publisher: Callable[..., PublicationEvidence],
) -> tuple[dict[str, Any], bool, int, int]:
    from performance_fixtures import (
        FixtureError,
        OneUseTermBank,
        codex_event_lines,
        validate_event_result,
    )

    sample = _failed_etd_sample(event, "unstarted")
    semantic_failure = False
    t0_ns: Optional[int] = None
    publication_durable_ns: Optional[int] = None
    ack: Optional[IngestAckObservation] = None
    last_miss_ns: Optional[int] = None
    first_hit_ns: Optional[int] = None
    first_valid_ns: Optional[int] = None
    last_term: Optional[str] = None
    term_use_count = 0
    bank: Optional[OneUseTermBank] = None
    try:
        released_ns = _sleep_until(scheduled_ns, clock_ns, sleeper)
        if _milliseconds(released_ns, scheduled_ns) > ETD_SCHEDULER_SLIP_LIMIT_MS:
            raise EtdSampleFailure("event_schedule_slip")
        if watcher_ready() is not True:
            raise EtdSampleFailure("watcher_not_ready")
        bank = OneUseTermBank(event)
        publication = publisher(
            watched_dir,
            event["destination_filename"],
            codex_event_lines(event),
            clock_ns=clock_ns,
        )
        t0_ns = publication.t0_ns
        publication_durable_ns = publication.publication_durable_ns
        if publication_durable_ns < t0_ns:
            raise EtdSampleFailure("publication_clock_regressed")
        deadline_ns = t0_ns + int(timeout_s * 1_000_000_000)

        while True:
            if clock_ns() >= deadline_ns:
                raise EtdSampleFailure("visibility_timeout")
            try:
                query = bank.claim_query()
            except FixtureError as exc:
                raise EtdSampleFailure("cache_bank_exhausted") from exc
            last_term = query["query"]
            term_use_count += 1
            try:
                observation = probe(query)
            except TimeoutError as exc:
                raise EtdSampleFailure("search_timeout") from exc
            except EtdSampleFailure:
                raise
            except Exception as exc:
                raise EtdSampleFailure("search_error") from exc
            observed_ns = clock_ns()
            if not isinstance(observation, EtdProbeResult):
                raise EtdSampleFailure("malformed_search_observation")
            if observation.error_code is not None:
                raise EtdSampleFailure(observation.error_code)
            if observation.visible:
                first_hit_ns = observed_ns
                try:
                    validate_event_result(observation.structured_content, event)
                except FixtureError as exc:
                    semantic_failure = True
                    raise EtdSampleFailure("identity_mismatch", semantic=True) from exc
                first_valid_ns = observed_ns
                break
            last_miss_ns = observed_ns
            remaining_ns = deadline_ns - clock_ns()
            if remaining_ns <= 0:
                raise EtdSampleFailure("visibility_timeout")
            sleeper(min(poll_interval_s, remaining_ns / 1_000_000_000.0))

        digest = event.get("expected_ack_digest")
        if not isinstance(digest, str):
            raise EtdSampleFailure("missing_event_identity_digest")
        matches = _await_ack(
            ack_cursor,
            digest,
            deadline_ns,
            clock_ns,
            sleeper,
            poll_interval_s,
        )
        if not matches:
            raise EtdSampleFailure("missing_db_ack")
        if len(matches) != 1:
            raise EtdSampleFailure("duplicate_db_ack")
        ack = matches[0]
        if ack.ack_monotonic_ns < t0_ns:
            raise EtdSampleFailure("stale_db_ack")
        if first_valid_ns is None:
            raise AssertionError("successful event must have a valid hit")

        source_lower = _milliseconds(last_miss_ns, t0_ns) if last_miss_ns else 0.0
        source_upper = _milliseconds(first_valid_ns, t0_ns)
        source_censoring = "interval" if last_miss_ns is not None else "left"
        misses_after_ack = (
            last_miss_ns is not None and last_miss_ns >= ack.ack_monotonic_ns
        )
        db_lower = (
            _milliseconds(last_miss_ns, ack.ack_monotonic_ns)
            if misses_after_ack and last_miss_ns is not None
            else 0.0
        )
        db_upper = (
            _milliseconds(first_valid_ns, ack.ack_monotonic_ns)
            if first_valid_ns >= ack.ack_monotonic_ns
            else 0.0
        )
        db_censoring = "interval" if misses_after_ack else "left"
        sample.update(
            {
                "term_sha256": _term_sha256(last_term),
                "batch_sequence": ack.batch_sequence,
                "publication_durable_ms": _milliseconds(publication_durable_ns, t0_ns),
                "db_ack_ms": _milliseconds(ack.ack_monotonic_ns, t0_ns),
                "last_miss_ms": (
                    _milliseconds(last_miss_ns, t0_ns) if last_miss_ns is not None else None
                ),
                "first_hit_ms": _milliseconds(first_hit_ns, t0_ns),
                "first_valid_ms": _milliseconds(first_valid_ns, t0_ns),
                "source_interval": {
                    "lower_ms": source_lower,
                    "upper_ms": source_upper,
                    "censoring": source_censoring,
                },
                "db_ack_interval": {
                    "lower_ms": db_lower,
                    "upper_ms": db_upper,
                    "censoring": db_censoring,
                },
                "term_use_count": term_use_count,
                "valid": True,
                "error_code": None,
            }
        )
        return sample, False, released_ns, clock_ns()
    except EtdSampleFailure as exc:
        semantic_failure = semantic_failure or exc.semantic
        sample.update(
            {
                "term_sha256": _term_sha256(last_term),
                "batch_sequence": ack.batch_sequence if ack else None,
                "publication_durable_ms": (
                    _milliseconds(publication_durable_ns, t0_ns)
                    if publication_durable_ns is not None and t0_ns is not None
                    else None
                ),
                "db_ack_ms": (
                    _milliseconds(ack.ack_monotonic_ns, t0_ns)
                    if ack is not None and t0_ns is not None
                    else None
                ),
                "last_miss_ms": (
                    _milliseconds(last_miss_ns, t0_ns)
                    if last_miss_ns is not None and t0_ns is not None
                    else None
                ),
                "first_hit_ms": (
                    _milliseconds(first_hit_ns, t0_ns)
                    if first_hit_ns is not None and t0_ns is not None
                    else None
                ),
                "first_valid_ms": (
                    _milliseconds(first_valid_ns, t0_ns)
                    if first_valid_ns is not None and t0_ns is not None
                    else None
                ),
                "source_interval": _right_interval(
                    _milliseconds(last_miss_ns, t0_ns)
                    if last_miss_ns is not None and t0_ns is not None
                    else 0.0
                ),
                "db_ack_interval": _right_interval(
                    _milliseconds(last_miss_ns, ack.ack_monotonic_ns)
                    if last_miss_ns is not None
                    and ack is not None
                    and last_miss_ns >= ack.ack_monotonic_ns
                    else 0.0
                ),
                "term_use_count": term_use_count,
                "valid": False,
                "error_code": exc.code,
            }
        )
        return sample, semantic_failure, released_ns, clock_ns()


def _etd_percentile(values: Sequence[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, math.ceil(quantile * len(ordered)) - 1)
    return float(ordered[index])


def _aggregate_etd_interval(
    samples: Sequence[Mapping[str, Any]], key: str
) -> dict[str, Any]:
    intervals = [sample[key] for sample in samples]
    lowers = [float(interval["lower_ms"]) for interval in intervals]
    has_right = any(interval["upper_ms"] is None for interval in intervals)
    if has_right:
        upper: Optional[float] = None
        censoring = "right"
    else:
        upper = _etd_percentile(
            [float(interval["upper_ms"]) for interval in intervals], 0.95
        )
        kinds = {str(interval["censoring"]) for interval in intervals}
        censoring = "interval" if "interval" in kinds else ("left" if "left" in kinds else "none")
    return {
        "lower_ms": _etd_percentile(lowers, 0.95),
        "upper_ms": upper,
        "censoring": censoring,
    }


def run_etd_scenario(
    events: Sequence[Mapping[str, Any]],
    event_schedule: Sequence[Mapping[str, Any]],
    watched_dir: os.PathLike[str] | str,
    probe: Callable[[Mapping[str, Any]], EtdProbeResult],
    ack_reader: Callable[[], Sequence[Mapping[str, Any]]],
    watcher_ready: Callable[[], bool],
    *,
    mode: str,
    timeout_s: float,
    poll_interval_s: float = 0.05,
    baseline_sustainable_qps: Optional[float] = None,
    query_load: Optional[Callable[[float], Mapping[str, Any]]] = None,
    clock_ns: Callable[[], int] = time.perf_counter_ns,
    sleeper: Callable[[float], None] = time.sleep,
    publisher: Callable[..., PublicationEvidence] = publish_event_durably,
    operational_evidence: Optional[dict[str, Any]] = None,
) -> ScenarioResult:
    """Run fixed idle or 75%-baseline-loaded ETD from a one-use event schedule."""

    if mode not in {"idle", "loaded"} or timeout_s <= 0 or poll_interval_s <= 0:
        raise ScenarioError("ETD mode and timing policy are invalid")
    if not events or len(event_schedule) != len(events):
        raise ScenarioError("ETD requires one scheduled operation per event")
    events_by_id = {event.get("case_id"): event for event in events}
    if len(events_by_id) != len(events) or None in events_by_id:
        raise ScenarioError("ETD event identities are missing or duplicated")
    scheduled_ids: list[str] = []
    previous_offset = -1
    for operation in event_schedule:
        case_id = operation.get("case_id")
        offset = operation.get("scheduled_offset_ns")
        if (
            case_id not in events_by_id
            or case_id in scheduled_ids
            or isinstance(offset, bool)
            or not isinstance(offset, int)
            or offset < previous_offset
        ):
            raise ScenarioError("ETD event schedule is invalid")
        scheduled_ids.append(case_id)
        previous_offset = offset
    if set(scheduled_ids) != set(events_by_id):
        raise ScenarioError("ETD event schedule does not cover the event bank exactly")
    if watcher_ready() is not True:
        failed = tuple(_failed_etd_sample(event, "watcher_not_ready") for event in events)
        metrics = {
            "direction": "lower",
            "event_count": len(events),
            "source_etd_p95": _aggregate_etd_interval(failed, "source_interval"),
            "db_ack_etd_p95": _aggregate_etd_interval(failed, "db_ack_interval"),
        }
        return ScenarioResult("fail", metrics, failed, diagnostics=("watcher_not_ready",))

    loaded_qps: Optional[float] = None
    if mode == "loaded":
        if (
            baseline_sustainable_qps is None
            or not math.isfinite(baseline_sustainable_qps)
            or baseline_sustainable_qps <= 0
            or query_load is None
        ):
            raise ScenarioError("loaded ETD requires baseline capacity and a query load runner")
        loaded_qps = baseline_sustainable_qps * 0.75
    elif query_load is not None or baseline_sustainable_qps is not None:
        raise ScenarioError("idle ETD cannot carry background query load")

    ack_cursor = IngestAckCursor(ack_reader)
    load_result: list[Mapping[str, Any]] = []
    load_errors: list[str] = []
    load_barrier: Optional[threading.Barrier] = None
    load_thread: Optional[threading.Thread] = None
    if loaded_qps is not None and query_load is not None:
        load_barrier = threading.Barrier(2)

        def run_background_load() -> None:
            try:
                load_barrier.wait()
                observed = query_load(loaded_qps)
                if not isinstance(observed, Mapping):
                    raise ScenarioError("loaded query runner returned invalid evidence")
                load_result.append(observed)
            except Exception as exc:
                load_errors.append(type(exc).__name__)

        load_thread = threading.Thread(target=run_background_load, daemon=True)
        load_thread.start()
        load_barrier.wait()

    scenario_start_ns = clock_ns()
    samples_by_id: dict[str, tuple[dict[str, Any], bool, int, int]] = {}
    with ThreadPoolExecutor(max_workers=min(32, len(event_schedule))) as executor:
        futures: dict[Future[tuple[dict[str, Any], bool, int, int]], str] = {}
        for operation in event_schedule:
            case_id = str(operation["case_id"])
            future = executor.submit(
                _run_one_etd_event,
                events_by_id[case_id],
                scenario_start_ns + int(operation["scheduled_offset_ns"]),
                watched_dir,
                probe,
                ack_cursor,
                watcher_ready,
                timeout_s,
                poll_interval_s,
                clock_ns,
                sleeper,
                publisher,
            )
            futures[future] = case_id
        for future, case_id in futures.items():
            try:
                samples_by_id[case_id] = future.result(timeout=timeout_s + previous_offset / 1_000_000_000.0 + 5.0)
            except Exception as exc:
                now_ns = clock_ns()
                samples_by_id[case_id] = (
                    _failed_etd_sample(events_by_id[case_id], f"worker_{type(exc).__name__}"),
                    False,
                    now_ns,
                    now_ns,
                )

    if load_thread is not None:
        load_thread.join(timeout=timeout_s + previous_offset / 1_000_000_000.0 + 5.0)
        if load_thread.is_alive():
            load_errors.append("query_load_backlog")

    ordered = tuple(samples_by_id[case_id][0] for case_id in scheduled_ids)
    semantic_failures = sum(1 for case_id in scheduled_ids if samples_by_id[case_id][1])
    if operational_evidence is not None:
        targets = {
            str(operation["case_id"]): scenario_start_ns
            + int(operation["scheduled_offset_ns"])
            for operation in event_schedule
        }
        slips_ms = [
            _milliseconds(samples_by_id[case_id][2], targets[case_id])
            for case_id in scheduled_ids
        ]
        operational_evidence.update(
            {
                "planned": len(event_schedule),
                "started": len(samples_by_id),
                "completed": len(samples_by_id),
                "scheduler_p99_slip_ms": _nearest_rank(slips_ms, 99.0),
                "first_started_ns": min(
                    samples_by_id[case_id][2] for case_id in scheduled_ids
                ),
                "last_completed_ns": max(
                    samples_by_id[case_id][3] for case_id in scheduled_ids
                ),
            }
        )
    diagnostics = sorted(
        {
            str(sample["error_code"])
            for sample in ordered
            if sample["error_code"] is not None
        }
    )
    status = "pass" if all(sample["valid"] for sample in ordered) else "fail"

    if mode == "loaded":
        if load_errors or len(load_result) != 1:
            status = "fail"
            diagnostics.extend(load_errors or ["missing_query_load_evidence"])
        else:
            evidence = load_result[0]
            offered = evidence.get("offered_qps")
            if (
                isinstance(offered, bool)
                or not isinstance(offered, (int, float))
                or not math.isclose(float(offered), float(loaded_qps), rel_tol=0.0, abs_tol=1e-12)
                or evidence.get("schedule_delivered") is not True
                or evidence.get("drained") is not True
                or evidence.get("backlog", 0) != 0
                or isinstance(evidence.get("first_started_ns"), bool)
                or not isinstance(evidence.get("first_started_ns"), int)
                or isinstance(evidence.get("last_completed_ns"), bool)
                or not isinstance(evidence.get("last_completed_ns"), int)
                or evidence["first_started_ns"]
                > scenario_start_ns + int(ETD_SCHEDULER_SLIP_LIMIT_MS * 1_000_000)
                or evidence["last_completed_ns"] < scenario_start_ns + previous_offset
            ):
                status = "fail"
                diagnostics.append("invalid_loaded_query_evidence")

    metrics = {
        "direction": "lower",
        "event_count": len(ordered),
        "source_etd_p95": _aggregate_etd_interval(ordered, "source_interval"),
        "db_ack_etd_p95": _aggregate_etd_interval(ordered, "db_ack_interval"),
    }
    return ScenarioResult(
        status,
        metrics,
        ordered,
        semantic_failures,
        tuple(sorted(set(diagnostics))),
    )


# ---------------------------------------------------------------------------
# Mixed query/ingest interference


MIXED_QUERY_GOODPUT_FLOOR = 0.90
MIXED_DEGRADATION_CEILING = 1.25


class MixedArmRuntime(Protocol):
    reset_id: str
    recipe_fingerprint: str

    def run_query(self, schedule: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]: ...

    def run_ingest(self, schedule: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]: ...

    def queue_depth(self) -> int: ...

    def close(self) -> None: ...


def _finite_nonnegative(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ScenarioError(f"{name} is not numeric")
    converted = float(value)
    if not math.isfinite(converted) or converted < 0:
        raise ScenarioError(f"{name} is not finite and nonnegative")
    return converted


def _query_arm_metrics(evidence: Mapping[str, Any]) -> dict[str, float]:
    raw = evidence.get("metrics")
    if not isinstance(raw, Mapping):
        raise ScenarioError("mixed query arm omitted metrics")
    return {
        "goodput_qps": _finite_nonnegative(raw.get("goodput_qps"), "query goodput"),
        "p95_ms": _finite_nonnegative(raw.get("p95_ms"), "query p95"),
        "p99_ms": _finite_nonnegative(raw.get("p99_ms"), "query p99"),
    }


def _interval_metric(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ScenarioError(f"{name} interval is missing")
    lower = _finite_nonnegative(value.get("lower_ms"), f"{name} lower")
    upper = value.get("upper_ms")
    if upper is None:
        raise ScenarioError(f"{name} is right-censored")
    upper_number = _finite_nonnegative(upper, f"{name} upper")
    if lower > upper_number:
        raise ScenarioError(f"{name} interval is inverted")
    censoring = value.get("censoring")
    if censoring not in {"none", "left", "interval"}:
        raise ScenarioError(f"{name} censoring is invalid")
    return {"lower_ms": lower, "upper_ms": upper_number, "censoring": censoring}


def _ingest_arm_metrics(evidence: Mapping[str, Any]) -> dict[str, Any]:
    raw = evidence.get("metrics")
    if not isinstance(raw, Mapping):
        raise ScenarioError("mixed ingest arm omitted metrics")
    return {
        "source_etd_p95": _interval_metric(raw.get("source_etd_p95"), "source ETD"),
        "db_ack_etd_p95": _interval_metric(raw.get("db_ack_etd_p95"), "DB-ack ETD"),
    }


def _exact_stream_delivery(
    evidence: Mapping[str, Any], expected: int, *, query: bool
) -> bool:
    if any(evidence.get(key) != expected for key in ("planned", "started", "completed")):
        return False
    if evidence.get("drained") is not True or evidence.get("backlog", 0) != 0:
        return False
    slip = evidence.get("scheduler_p99_slip_ms")
    if isinstance(slip, bool) or not isinstance(slip, (int, float)) or float(slip) > 10.0:
        return False
    if query:
        failures = evidence.get("failures", {})
        if not isinstance(failures, Mapping) or any(value != 0 for value in failures.values()):
            return False
    return True


def _exact_ingest_samples(evidence: Mapping[str, Any], expected: int) -> bool:
    samples = evidence.get("samples")
    if (
        isinstance(samples, (str, bytes, bytearray))
        or not isinstance(samples, Sequence)
        or len(samples) != expected
    ):
        return False
    event_digests: set[str] = set()
    term_digests: set[str] = set()
    for sample in samples:
        if not isinstance(sample, Mapping) or set(sample) != ETD_SAMPLE_KEYS:
            return False
        event_digest = sample["event_identity_sha256"]
        term_digest = sample["term_sha256"]
        if (
            not isinstance(event_digest, str)
            or not isinstance(term_digest, str)
            or event_digest in event_digests
            or term_digest in term_digests
            or sample["valid"] is not True
            or sample["error_code"] is not None
            or not isinstance(sample["batch_sequence"], int)
            or isinstance(sample["batch_sequence"], bool)
            or not isinstance(sample["term_use_count"], int)
            or isinstance(sample["term_use_count"], bool)
            or sample["term_use_count"] < 1
        ):
            return False
        bypass = sample["cache_bypass"]
        if (
            not isinstance(bypass, Mapping)
            or set(bypass) != set(CACHE_BYPASS_KEYS)
            or not all(value is True for value in bypass.values())
        ):
            return False
        event_digests.add(event_digest)
        term_digests.add(term_digest)
    return True


def _safe_ratio(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator > 0 else 0.0


def run_mixed_scenario(
    query_schedule: Sequence[Mapping[str, Any]],
    event_schedule: Sequence[Mapping[str, Any]],
    arm_factory: Callable[[str], MixedArmRuntime],
    *,
    combined_timeout_s: float = 120.0,
    clock_ns: Callable[[], int] = time.perf_counter_ns,
) -> ScenarioResult:
    """Run fresh query-only, ingest-only, then barrier-started combined arms."""

    if not query_schedule or not event_schedule or combined_timeout_s <= 0:
        raise ScenarioError("mixed scenario requires both fixed schedules")
    runtimes: list[MixedArmRuntime] = []
    reset_ids: set[str] = set()
    recipe_fingerprints: set[str] = set()

    def acquire(label: str) -> MixedArmRuntime:
        runtime = arm_factory(label)
        if not isinstance(runtime.reset_id, str) or not runtime.reset_id:
            raise ScenarioError("mixed arm has no physical reset identity")
        if runtime.reset_id in reset_ids:
            raise ScenarioError("mixed arm reused a physical reset")
        if not isinstance(runtime.recipe_fingerprint, str) or not runtime.recipe_fingerprint:
            raise ScenarioError("mixed arm has no logical recipe fingerprint")
        reset_ids.add(runtime.reset_id)
        recipe_fingerprints.add(runtime.recipe_fingerprint)
        runtimes.append(runtime)
        return runtime

    try:
        query_control_runtime = acquire("query_only")
        query_control = query_control_runtime.run_query(query_schedule)
        query_control_depth = query_control_runtime.queue_depth()
        query_control_runtime.close()

        ingest_control_runtime = acquire("ingest_only")
        ingest_control = ingest_control_runtime.run_ingest(event_schedule)
        ingest_control_depth = ingest_control_runtime.queue_depth()
        ingest_control_runtime.close()

        combined_runtime = acquire("combined")
        if len(recipe_fingerprints) != 1:
            raise ScenarioError("mixed arms do not share one logical recipe")
        barrier = threading.Barrier(3)
        combined_results: dict[str, Mapping[str, Any]] = {}
        combined_bounds: dict[str, tuple[int, int]] = {}
        combined_errors: list[str] = []
        result_lock = threading.Lock()

        def run_combined_stream(name: str) -> None:
            try:
                barrier.wait()
                started_ns = clock_ns()
                if name == "query":
                    evidence = combined_runtime.run_query(query_schedule)
                else:
                    evidence = combined_runtime.run_ingest(event_schedule)
                ended_ns = clock_ns()
                if not isinstance(evidence, Mapping):
                    raise ScenarioError(f"combined {name} evidence is invalid")
                first_started_ns = evidence.get("first_started_ns")
                last_completed_ns = evidence.get("last_completed_ns")
                if (
                    isinstance(first_started_ns, bool)
                    or not isinstance(first_started_ns, int)
                    or isinstance(last_completed_ns, bool)
                    or not isinstance(last_completed_ns, int)
                    or first_started_ns > last_completed_ns
                ):
                    raise ScenarioError(f"combined {name} omitted stream overlap evidence")
                with result_lock:
                    combined_results[name] = evidence
                    combined_bounds[name] = (first_started_ns, last_completed_ns)
            except Exception as exc:
                with result_lock:
                    combined_errors.append(f"{name}_{type(exc).__name__}")

        query_thread = threading.Thread(target=run_combined_stream, args=("query",), daemon=True)
        ingest_thread = threading.Thread(target=run_combined_stream, args=("ingest",), daemon=True)
        query_thread.start()
        ingest_thread.start()
        barrier.wait()
        query_thread.join(timeout=combined_timeout_s)
        ingest_thread.join(timeout=combined_timeout_s)
        if query_thread.is_alive() or ingest_thread.is_alive():
            combined_errors.append("combined_backlog")
        combined_depth = combined_runtime.queue_depth()
        combined_runtime.close()
        if combined_errors or set(combined_results) != {"query", "ingest"}:
            raise ScenarioError("combined streams did not both complete and drain")

        query_control_metrics = _query_arm_metrics(query_control)
        ingest_control_metrics = _ingest_arm_metrics(ingest_control)
        combined_query_metrics = _query_arm_metrics(combined_results["query"])
        combined_ingest_metrics = _ingest_arm_metrics(combined_results["ingest"])
        query_delivery = _exact_stream_delivery(
            query_control, len(query_schedule), query=True
        ) and _exact_stream_delivery(
            combined_results["query"], len(query_schedule), query=True
        )
        ingest_delivery = (
            _exact_stream_delivery(ingest_control, len(event_schedule), query=False)
            and _exact_stream_delivery(
                combined_results["ingest"], len(event_schedule), query=False
            )
            and _exact_ingest_samples(ingest_control, len(event_schedule))
            and _exact_ingest_samples(
                combined_results["ingest"], len(event_schedule)
            )
        )
        query_control_pass = (
            query_delivery
            and query_control_depth == 0
            and query_control_metrics["p95_ms"] <= 750.0
            and query_control_metrics["p99_ms"] <= 2_000.0
            and query_control_metrics["goodput_qps"] > 0
        )
        ingest_control_pass = (
            ingest_delivery
            and ingest_control_depth == 0
            and ingest_control.get("status") == "pass"
            and ingest_control.get("lost_events", 0) == 0
            and ingest_control.get("duplicate_events", 0) == 0
        )
        ratios = {
            "query_goodput": _safe_ratio(
                combined_query_metrics["goodput_qps"], query_control_metrics["goodput_qps"]
            ),
            "query_p95": _safe_ratio(
                combined_query_metrics["p95_ms"], query_control_metrics["p95_ms"]
            ),
            "query_p99": _safe_ratio(
                combined_query_metrics["p99_ms"], query_control_metrics["p99_ms"]
            ),
            "source_etd": _safe_ratio(
                combined_ingest_metrics["source_etd_p95"]["upper_ms"],
                ingest_control_metrics["source_etd_p95"]["upper_ms"],
            ),
            "db_ack_etd": _safe_ratio(
                combined_ingest_metrics["db_ack_etd_p95"]["upper_ms"],
                ingest_control_metrics["db_ack_etd_p95"]["upper_ms"],
            ),
        }
        query_slo = (
            combined_query_metrics["p95_ms"] <= 750.0
            and combined_query_metrics["p99_ms"] <= 2_000.0
            and combined_query_metrics["goodput_qps"] > 0
        )
        query_degradation = (
            ratios["query_goodput"] >= MIXED_QUERY_GOODPUT_FLOOR
            and ratios["query_p95"] <= MIXED_DEGRADATION_CEILING
            and ratios["query_p99"] <= MIXED_DEGRADATION_CEILING
        )
        ingest_slo = (
            combined_results["ingest"].get("status") == "pass"
            and combined_results["ingest"].get("lost_events", 0) == 0
            and combined_results["ingest"].get("duplicate_events", 0) == 0
        )
        ingest_degradation = (
            ratios["source_etd"] <= MIXED_DEGRADATION_CEILING
            and ratios["db_ack_etd"] <= MIXED_DEGRADATION_CEILING
        )
        query_bounds = combined_bounds["query"]
        ingest_bounds = combined_bounds["ingest"]
        overlap = (
            query_bounds[0] < ingest_bounds[1]
            and ingest_bounds[0] < query_bounds[1]
        )
        lost_events = int(combined_results["ingest"].get("lost_events", 0))
        duplicate_events = int(combined_results["ingest"].get("duplicate_events", 0))
        exact_events = (
            lost_events == 0
            and duplicate_events == 0
            and combined_results["ingest"].get("completed") == len(event_schedule)
        )
        drained = (
            combined_depth == 0
            and combined_results["query"].get("drained") is True
            and combined_results["ingest"].get("drained") is True
        )
        gates = {
            "query_slo": query_slo,
            "query_degradation": query_degradation,
            "ingest_slo": ingest_slo,
            "ingest_degradation": ingest_degradation,
            "schedules_delivered": query_delivery and ingest_delivery,
            "overlap": overlap,
            "drained": drained,
            "exact_events": exact_events,
        }
        metrics = {
            "direction": "gates",
            "query_control": query_control_metrics,
            "ingest_control": ingest_control_metrics,
            "combined_query": combined_query_metrics,
            "combined_ingest": combined_ingest_metrics,
            "ratios": ratios,
            "mixed_gates": gates,
            "lost_events": lost_events,
            "duplicate_events": duplicate_events,
        }
        query_samples = combined_results["query"].get("samples", ())
        ingest_samples = combined_results["ingest"].get("samples", ())
        status = (
            "pass"
            if query_control_pass
            and ingest_control_pass
            and all(gates.values())
            else "fail"
        )
        diagnostics = tuple(key for key, value in gates.items() if not value)
        return ScenarioResult(
            status,
            metrics,
            {"query_records": query_samples, "ingest": ingest_samples},
            int(combined_results["query"].get("semantic_failures", 0))
            + int(combined_results["ingest"].get("semantic_failures", 0)),
            diagnostics,
        )
    finally:
        for runtime in runtimes:
            try:
                runtime.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Owned production ETD and mixed adapters


class OwnedSandboxEtdPrimitives(Protocol):
    sandbox_id: str
    watched_source_dir: os.PathLike[str]

    def spawn_stdio_route(self) -> subprocess.Popen[bytes]: ...

    def wait_watcher_ready(self, *, timeout_s: float) -> Any: ...

    def read_ingest_ack_logs(self, cursor: int = 0) -> Any: ...

    def wait_ingest_drained(self, *, timeout_s: float) -> Any: ...

    def down(self) -> None: ...


class OwnedSandboxAckReader:
    """Adapt the ownership-checked stable-cursor ack log API to ETD input."""

    def __init__(self, sandbox: OwnedSandboxEtdPrimitives) -> None:
        self._sandbox = sandbox
        self._cursor = 0

    def __call__(self) -> Sequence[Mapping[str, Any]]:
        batch = self._sandbox.read_ingest_ack_logs(self._cursor)
        next_cursor = getattr(batch, "next_cursor", None)
        observations = getattr(batch, "observations", None)
        gap_detected = getattr(batch, "gap_detected", None)
        if (
            isinstance(next_cursor, bool)
            or not isinstance(next_cursor, int)
            or not isinstance(observations, tuple)
            or not isinstance(gap_detected, bool)
        ):
            raise ScenarioError("owned sandbox returned malformed ack-log evidence")
        if gap_detected:
            raise ScenarioError("owned sandbox ack-log cursor was truncated")
        if next_cursor < self._cursor:
            raise ScenarioError("owned sandbox ack-log cursor regressed")
        result: list[dict[str, Any]] = []
        for observation in observations:
            result.append(
                {
                    "batch_sequence": getattr(observation, "batch_sequence", None),
                    "event_identity_digests": getattr(
                        observation, "event_identity_digests", None
                    ),
                    "ack_monotonic_ns": getattr(
                        observation, "ack_monotonic_ns", None
                    ),
                }
            )
        self._cursor = next_cursor
        return tuple(result)


class OwnedSandboxEtdRuntime:
    """Concrete watcher, ack, and MCP visibility adapters for one sandbox."""

    def __init__(
        self,
        sandbox: OwnedSandboxEtdPrimitives,
        *,
        timeout_s: float = 5.0,
        clock_ns: Callable[[], int] = time.perf_counter_ns,
    ) -> None:
        if timeout_s <= 0:
            raise ScenarioError("owned ETD adapter timeout must be positive")
        self.sandbox = sandbox
        self.timeout_s = timeout_s
        self._clock_ns = clock_ns
        self._ack_reader = OwnedSandboxAckReader(sandbox)
        self._client = _StdioJsonRpcClient(sandbox.spawn_stdio_route())
        self._client.wait_for_central_route(timeout_s)
        self._client.initialize(timeout_s)
        if "search_sessions" not in self._client.list_tools(timeout_s):
            self._client.close()
            raise ScenarioError("owned sandbox central route omitted search_sessions")
        self._closed = False

    @property
    def watched_dir(self) -> os.PathLike[str]:
        return self.sandbox.watched_source_dir

    def watcher_ready(self) -> bool:
        try:
            evidence = self.sandbox.wait_watcher_ready(timeout_s=self.timeout_s)
        except Exception:
            return False
        files_watched = getattr(evidence, "files_watched", None)
        return (
            isinstance(files_watched, int)
            and not isinstance(files_watched, bool)
            and files_watched > 0
        )

    def ack_reader(self) -> Sequence[Mapping[str, Any]]:
        return self._ack_reader()

    def probe(self, query: Mapping[str, Any]) -> EtdProbeResult:
        started_ns = self._clock_ns()
        result = self._client.call(
            "tools/call",
            {"name": "search_sessions", "arguments": dict(query)},
            self.timeout_s,
        )
        structured = self._client.decode_search_result(result)
        completed_ns = self._clock_ns()
        data = structured.get("data")
        result_count = data.get("result_count") if isinstance(data, Mapping) else None
        if (
            isinstance(result_count, bool)
            or not isinstance(result_count, int)
            or result_count < 0
        ):
            raise MalformedResult("search_sessions returned no exact result count")
        return EtdProbeResult(
            result_count > 0,
            structured,
            _milliseconds(completed_ns, started_ns),
        )

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._client.close()


def make_owned_sandbox_etd_runtime(
    sandbox: OwnedSandboxEtdPrimitives,
    *,
    timeout_s: float = 5.0,
    clock_ns: Callable[[], int] = time.perf_counter_ns,
) -> OwnedSandboxEtdRuntime:
    """Construct real watched-source, ack-log, readiness, and MCP adapters."""

    return OwnedSandboxEtdRuntime(sandbox, timeout_s=timeout_s, clock_ns=clock_ns)


def run_owned_sandbox_etd_scenario(
    sandbox: OwnedSandboxEtdPrimitives,
    events: Sequence[Mapping[str, Any]],
    event_schedule: Sequence[Mapping[str, Any]],
    *,
    mode: str,
    timeout_s: float,
    poll_interval_s: float = 0.05,
    baseline_sustainable_qps: Optional[float] = None,
    query_load: Optional[Callable[[float], Mapping[str, Any]]] = None,
) -> ScenarioResult:
    """Run ETD without suite-provided placeholder callbacks."""

    runtime = make_owned_sandbox_etd_runtime(sandbox, timeout_s=timeout_s)
    try:
        return run_etd_scenario(
            events,
            event_schedule,
            runtime.watched_dir,
            runtime.probe,
            runtime.ack_reader,
            runtime.watcher_ready,
            mode=mode,
            timeout_s=timeout_s,
            poll_interval_s=poll_interval_s,
            baseline_sustainable_qps=baseline_sustainable_qps,
            query_load=query_load,
        )
    finally:
        runtime.close()


class OwnedSandboxQueryLoad:
    """Execute one fixed open-arrival query schedule over the central MCP route."""

    def __init__(
        self,
        sandbox: OwnedSandboxEtdPrimitives,
        cases: Mapping[str, Mapping[str, Any]],
        schedule: Sequence[Mapping[str, Any]],
        *,
        offered_qps: float,
        timeout_s: float = 5.0,
        clock_ns: Callable[[], int] = time.perf_counter_ns,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if not cases or not schedule or offered_qps <= 0 or timeout_s <= 0:
            raise ScenarioError("owned query load configuration is invalid")
        self._sandbox = sandbox
        self._cases = cases
        self._schedule = schedule
        self._offered_qps = float(offered_qps)
        self._timeout_s = timeout_s
        self._clock_ns = clock_ns
        self._sleeper = sleeper
        self._used = False

    def __call__(self, offered_qps: float) -> Mapping[str, Any]:
        if self._used:
            raise ScenarioError("owned query load schedules are one-use")
        self._used = True
        if (
            isinstance(offered_qps, bool)
            or not isinstance(offered_qps, (int, float))
            or not math.isclose(
                float(offered_qps), self._offered_qps, rel_tol=0.0, abs_tol=1e-12
            )
        ):
            raise ScenarioError("owned query load rate differs from its fixed schedule")
        client = _StdioJsonRpcClient(self._sandbox.spawn_stdio_route())
        client.wait_for_central_route(self._timeout_s)
        client.initialize(self._timeout_s)
        if "search_sessions" not in client.list_tools(self._timeout_s):
            client.close()
            raise ScenarioError("owned query load route omitted search_sessions")
        scenario_start_ns = self._clock_ns()
        lock = threading.Lock()
        records: list[dict[str, Any]] = []

        def execute(operation: Mapping[str, Any]) -> None:
            case_id = operation.get("case_id")
            offset_ns = operation.get("scheduled_offset_ns")
            sequence = operation.get("sequence")
            if (
                not isinstance(case_id, str)
                or case_id not in self._cases
                or isinstance(offset_ns, bool)
                or not isinstance(offset_ns, int)
                or offset_ns < 0
                or isinstance(sequence, bool)
                or not isinstance(sequence, int)
            ):
                raise ScenarioError("owned query schedule operation is invalid")
            target_ns = scenario_start_ns + offset_ns
            started_ns = _sleep_until(target_ns, self._clock_ns, self._sleeper)
            outcome = "correct"
            try:
                structured = client.search(self._cases[case_id], self._timeout_s)
                validate_query_oracle(structured, self._cases[case_id])
            except RequestTimeout:
                outcome = "timeout"
            except AdmissionRejected:
                outcome = "admission_rejection"
            except McpToolError:
                outcome = "tool_error"
            except MalformedResult:
                outcome = "malformed"
            except Exception:
                outcome = "semantic"
            completed_ns = self._clock_ns()
            with lock:
                records.append(
                    {
                        "sequence": sequence,
                        "scheduled_offset_ns": offset_ns,
                        "started_offset_ns": started_ns - scenario_start_ns,
                        "completed_offset_ns": completed_ns - scenario_start_ns,
                        "slip_ms": _milliseconds(started_ns, target_ns),
                        "latency_ms": _milliseconds(completed_ns, started_ns),
                        "outcome": outcome,
                    }
                )

        try:
            with ThreadPoolExecutor(max_workers=min(64, len(self._schedule))) as executor:
                futures = [executor.submit(execute, operation) for operation in self._schedule]
                final_offset = max(
                    int(operation.get("scheduled_offset_ns", 0))
                    for operation in self._schedule
                )
                done, pending = wait(
                    futures,
                    timeout=final_offset / 1_000_000_000.0 + self._timeout_s + 5.0,
                )
                for future in done:
                    future.result()
                for future in pending:
                    future.cancel()
        finally:
            client.close()
        ordered = sorted(records, key=lambda record: int(record["sequence"]))
        failures = {
            name: sum(record["outcome"] == name for record in ordered)
            for name in (
                "timeout",
                "admission_rejection",
                "tool_error",
                "malformed",
                "semantic",
            )
        }
        correct = sum(record["outcome"] == "correct" for record in ordered)
        terminal_ns = (
            scenario_start_ns + max(int(record["completed_offset_ns"]) for record in ordered)
            if ordered
            else scenario_start_ns
        )
        elapsed_s = max((terminal_ns - scenario_start_ns) / 1_000_000_000.0, 1e-9)
        latencies = [
            float(record["latency_ms"])
            for record in ordered
            if record["outcome"] == "correct"
        ]
        slips = [float(record["slip_ms"]) for record in ordered]
        backlog = len(self._schedule) - len(ordered)
        schedule_delivered = (
            backlog == 0
            and all(value == 0 for value in failures.values())
            and _nearest_rank(slips, 99.0) <= ETD_SCHEDULER_SLIP_LIMIT_MS
        )
        return {
            "offered_qps": self._offered_qps,
            "planned": len(self._schedule),
            "started": len(ordered),
            "completed": len(ordered),
            "scheduler_p99_slip_ms": _nearest_rank(slips, 99.0),
            "schedule_delivered": schedule_delivered,
            "drained": backlog == 0,
            "backlog": backlog,
            "first_started_ns": min(
                (
                    scenario_start_ns + int(record["started_offset_ns"])
                    for record in ordered
                ),
                default=scenario_start_ns,
            ),
            "last_completed_ns": terminal_ns,
            "metrics": {
                "goodput_qps": correct / elapsed_s,
                "p95_ms": _nearest_rank(latencies, 95.0),
                "p99_ms": _nearest_rank(latencies, 99.0),
            },
            "samples": tuple(ordered),
            "failures": failures,
            "semantic_failures": failures["semantic"],
        }


def make_owned_sandbox_query_load(
    sandbox: OwnedSandboxEtdPrimitives,
    cases: Mapping[str, Mapping[str, Any]],
    schedule: Sequence[Mapping[str, Any]],
    *,
    offered_qps: float,
    timeout_s: float = 5.0,
) -> OwnedSandboxQueryLoad:
    """Construct the production loaded-ETD query load runner."""

    return OwnedSandboxQueryLoad(
        sandbox,
        cases,
        schedule,
        offered_qps=offered_qps,
        timeout_s=timeout_s,
    )


class OwnedSandboxMixedArm:
    """One fresh physical sandbox implementing one mixed control/combined arm."""

    def __init__(
        self,
        sandbox: OwnedSandboxEtdPrimitives,
        query_cases: Mapping[str, Mapping[str, Any]],
        event_cases: Mapping[str, Mapping[str, Any]],
        *,
        query_rate_qps: float,
        recipe_fingerprint: str,
        request_timeout_s: float = 5.0,
        poll_interval_s: float = 0.05,
    ) -> None:
        if (
            not query_cases
            or not event_cases
            or query_rate_qps <= 0
            or not recipe_fingerprint
        ):
            raise ScenarioError("owned mixed arm configuration is invalid")
        self._sandbox = sandbox
        self._query_cases = query_cases
        self._event_cases = event_cases
        self._query_rate_qps = float(query_rate_qps)
        self._request_timeout_s = request_timeout_s
        self._poll_interval_s = poll_interval_s
        self.reset_id = sandbox.sandbox_id
        self.recipe_fingerprint = recipe_fingerprint
        self._queue_depth: Optional[int] = None
        self._closed = False
        self._run_lock = threading.Lock()
        self._streams_run: set[str] = set()

    def _claim_stream(self, name: str) -> None:
        with self._run_lock:
            if name in self._streams_run:
                raise ScenarioError(f"owned mixed {name} schedule was reused")
            self._streams_run.add(name)

    def run_query(
        self, schedule: Sequence[Mapping[str, Any]]
    ) -> Mapping[str, Any]:
        self._claim_stream("query")
        runner = make_owned_sandbox_query_load(
            self._sandbox,
            self._query_cases,
            schedule,
            offered_qps=self._query_rate_qps,
            timeout_s=self._request_timeout_s,
        )
        return runner(self._query_rate_qps)

    def run_ingest(
        self, schedule: Sequence[Mapping[str, Any]]
    ) -> Mapping[str, Any]:
        self._claim_stream("ingest")
        events: list[Mapping[str, Any]] = []
        for operation in schedule:
            case_id = operation.get("case_id")
            if not isinstance(case_id, str) or case_id not in self._event_cases:
                raise ScenarioError("owned mixed ingest schedule references an unknown event")
            events.append(self._event_cases[case_id])
        if len({event.get("case_id") for event in events}) != len(events):
            raise ScenarioError("owned mixed ingest schedule repeats an event")
        runtime = make_owned_sandbox_etd_runtime(
            self._sandbox,
            timeout_s=self._request_timeout_s,
        )
        operational: dict[str, Any] = {}
        try:
            result = run_etd_scenario(
                events,
                schedule,
                runtime.watched_dir,
                runtime.probe,
                runtime.ack_reader,
                runtime.watcher_ready,
                mode="idle",
                timeout_s=self._request_timeout_s,
                poll_interval_s=self._poll_interval_s,
                operational_evidence=operational,
            )
            drained = self._sandbox.wait_ingest_drained(
                timeout_s=self._request_timeout_s
            )
        finally:
            runtime.close()
        queue_depth = getattr(drained, "queue_depth", None)
        files_active = getattr(drained, "files_active", None)
        if (
            isinstance(queue_depth, bool)
            or not isinstance(queue_depth, int)
            or queue_depth < 0
            or isinstance(files_active, bool)
            or not isinstance(files_active, int)
            or files_active < 0
        ):
            raise ScenarioError("owned sandbox returned malformed drain evidence")
        self._queue_depth = queue_depth
        valid_digests = [
            str(sample["event_identity_sha256"])
            for sample in result.samples
            if sample.get("valid") is True
        ]
        expected_digests = {
            "sha256:" + str(event["expected_ack_digest"]).removeprefix("sha256:")
            for event in events
        }
        observed_digests = set(valid_digests)
        lost_events = len(expected_digests - observed_digests)
        duplicate_events = len(valid_digests) - len(observed_digests)
        invalid_events = len(events) - len(valid_digests)
        first_started_ns = operational.get("first_started_ns")
        last_completed_ns = operational.get("last_completed_ns")
        if (
            isinstance(first_started_ns, bool)
            or not isinstance(first_started_ns, int)
            or isinstance(last_completed_ns, bool)
            or not isinstance(last_completed_ns, int)
        ):
            now_ns = time.perf_counter_ns()
            first_started_ns = now_ns
            last_completed_ns = now_ns
        return {
            "status": result.status,
            "planned": int(operational.get("planned", len(events))),
            "started": int(operational.get("started", len(result.samples))),
            "completed": int(operational.get("completed", len(result.samples))),
            "scheduler_p99_slip_ms": float(
                operational.get("scheduler_p99_slip_ms", math.inf)
            ),
            "drained": queue_depth == 0 and files_active == 0,
            "backlog": queue_depth + files_active + invalid_events,
            "first_started_ns": first_started_ns,
            "last_completed_ns": last_completed_ns,
            "metrics": dict(result.metrics),
            "samples": tuple(result.samples),
            "semantic_failures": result.semantic_failures,
            "lost_events": lost_events,
            "duplicate_events": duplicate_events,
        }

    def queue_depth(self) -> int:
        if self._queue_depth is not None:
            return self._queue_depth
        evidence = self._sandbox.wait_ingest_drained(
            timeout_s=self._request_timeout_s
        )
        queue_depth = getattr(evidence, "queue_depth", None)
        files_active = getattr(evidence, "files_active", None)
        if (
            isinstance(queue_depth, bool)
            or not isinstance(queue_depth, int)
            or isinstance(files_active, bool)
            or not isinstance(files_active, int)
        ):
            raise ScenarioError("owned sandbox returned malformed drain evidence")
        self._queue_depth = queue_depth + files_active
        return self._queue_depth

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._sandbox.down()


def make_owned_sandbox_mixed_arm_factory(
    sandbox_factory: Callable[[str], OwnedSandboxEtdPrimitives],
    query_cases: Mapping[str, Mapping[str, Any]],
    event_cases: Mapping[str, Mapping[str, Any]],
    *,
    query_rate_qps: float,
    recipe_fingerprint: str,
    request_timeout_s: float = 5.0,
    poll_interval_s: float = 0.05,
) -> Callable[[str], OwnedSandboxMixedArm]:
    """Bind per-arm physical reset creation to the production mixed runtime."""

    def factory(label: str) -> OwnedSandboxMixedArm:
        sandbox = sandbox_factory(label)
        return OwnedSandboxMixedArm(
            sandbox,
            query_cases,
            event_cases,
            query_rate_qps=query_rate_qps,
            recipe_fingerprint=recipe_fingerprint,
            request_timeout_s=request_timeout_s,
            poll_interval_s=poll_interval_s,
        )

    return factory
