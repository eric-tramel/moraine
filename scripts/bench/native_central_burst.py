#!/usr/bin/env python3
"""Thin native-macOS burst driver for Moraine's central MCP daemon.

The public CLI is exposed by ``performance_suite.py native-central-burst``.
This module deliberately composes the existing frozen fixture, multiplexed
stdio protocol client, and process cleanup helper instead of defining another
benchmark framework.
"""
from __future__ import annotations

import ipaddress
import json
import math
import os
import platform
import re
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Protocol, Sequence

from performance_fixtures import (
    FixtureError,
    QUERY_MODES,
    SPLITS,
    build_recipe,
    validate_query_result,
)
from performance_protocol import sha256_json, write_json_atomic
from performance_runtime import RuntimeFailure, hash_file, terminate_process_group
from performance_scenarios import (
    AdmissionRejected,
    MalformedResult,
    McpProtocolError,
    McpToolError,
    RequestTimeout,
    ScenarioError,
    _StdioJsonRpcClient,
)

DOCUMENT_TYPE = "native_central_burst"
SCHEMA_VERSION = "moraine-native-central-burst-v1"
CONCURRENCIES = (1, 4, 8)
DEFAULT_MODES = ("rare", "hydration", "session_scope", "ranking_tie")
DEFAULT_COLD_REPETITIONS = 100
DEFAULT_MIN_COLD_SAMPLES = 100
DEFAULT_WARM_P95_LIMIT_MS = 750.0
DEFAULT_COLD_P95_LIMIT_MS = 2_000.0
DEFAULT_MAX_LATENCY_MS = 5_000.0
TOOL_NAME = "search_sessions"
FORBIDDEN_INTERNAL_TOOLS = ("search_mcp_events",)
COLD_SLO_MODES = ("hydration", "session_scope")
COLD_PHASE = "cold_lifecycle"
STEADY_PHASE = "steady_state"
CONTROL_QUERY_STAGES = (
    "publication_capture",
    "append_fence_capture",
    "publication_revalidate",
    "append_fence_revalidate",
)
BUSINESS_QUERY_STAGES = ("candidate", "detail")
QUERY_STAGES = (*CONTROL_QUERY_STAGES, *BUSINESS_QUERY_STAGES, "unknown")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class NativeBurstFailure(RuntimeError):
    """The native burst run cannot produce truthful, complete evidence."""


class SearchClient(Protocol):
    def search(self, case: Mapping[str, Any], timeout_s: float) -> Mapping[str, Any]: ...


class BurstLifecycle(Protocol):
    tool_surfaces: Sequence[Sequence[str]]

    def start(self) -> None: ...
    def open_clients(self, count: int) -> Sequence[SearchClient]: ...
    def close(self) -> None: ...
    def cleanup_evidence(self) -> Mapping[str, Any]: ...


@dataclass(frozen=True)
class ClickHouseEndpoint:
    url: str
    database: str
    username: str
    password: str


@dataclass(frozen=True)
class BackendSelection:
    endpoint: ClickHouseEndpoint
    backend_name: str
    route_policy: str
    repo_marker_count: int


def _validated_tool_surface(tools: Sequence[str]) -> tuple[str, ...]:
    if any(not isinstance(tool, str) or not tool for tool in tools):
        raise NativeBurstFailure("central MCP route exposed a malformed tool surface")
    canonical = tuple(sorted(set(tools)))
    if TOOL_NAME not in canonical:
        raise NativeBurstFailure("central MCP route omitted search_sessions")
    if any(tool in canonical for tool in FORBIDDEN_INTERNAL_TOOLS):
        raise NativeBurstFailure("central MCP route exposed an internal retrieval tool")
    return canonical


def _nearest_rank(values: Sequence[float], percentile: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(percentile / 100.0 * len(ordered)) - 1)
    return ordered[index]


def _error_code(error: Exception) -> tuple[str, bool]:
    if isinstance(error, RequestTimeout):
        return "timeout", True
    if isinstance(error, AdmissionRejected):
        return "admission_rejected", False
    if isinstance(error, McpToolError):
        return "tool_error", False
    if isinstance(error, MalformedResult):
        return "malformed_result", False
    if isinstance(error, McpProtocolError):
        return "protocol_error", False
    if isinstance(error, FixtureError):
        return "semantic_mismatch", False
    if isinstance(error, ScenarioError):
        return "scenario_error", False
    return "unexpected_error", False


def _sample_call(
    client: SearchClient,
    case: Mapping[str, Any],
    worker: int,
    barrier: threading.Barrier,
    timeout_s: float,
    clock_ns: Callable[[], int],
) -> dict[str, Any]:
    try:
        barrier.wait(timeout=max(5.0, timeout_s))
    except threading.BrokenBarrierError as error:
        raise NativeBurstFailure("native burst synchronization barrier broke") from error

    started_ns = clock_ns()
    outcome = "ok"
    error_code: Optional[str] = None
    error_type: Optional[str] = None
    right_censored = False
    try:
        structured = client.search(case, timeout_s)
        validate_query_result(structured, case)
    except Exception as error:  # A failed request is evidence, not a harness crash.
        outcome, right_censored = _error_code(error)
        error_code = outcome
        error_type = type(error).__name__
    completed_ns = clock_ns()
    if completed_ns < started_ns:
        raise NativeBurstFailure("monotonic clock moved backwards during a request")
    return {
        "worker": worker,
        "case_id": case["case_id"],
        "mode": case["mode"],
        "started_ns": started_ns,
        "completed_ns": completed_ns,
        "elapsed_ms": (completed_ns - started_ns) / 1_000_000.0,
        "outcome": outcome,
        "error_code": error_code,
        "error_type": error_type,
        "right_censored": right_censored,
    }


def run_synchronized_bursts(
    clients: Sequence[SearchClient],
    cases: Sequence[Mapping[str, Any]],
    *,
    concurrency: int,
    bursts_per_case: int,
    timeout_s: float,
    phase: str = STEADY_PHASE,
    lifecycle_iteration: int = 0,
    burst_id_prefix: str = "",
    clock_ns: Callable[[], int] = time.monotonic_ns,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Run identical-query bursts over already-initialized persistent clients."""

    if concurrency < 1 or len(clients) < concurrency:
        raise NativeBurstFailure("native burst has fewer clients than its concurrency")
    if not cases or bursts_per_case < 1 or timeout_s <= 0:
        raise NativeBurstFailure(
            "native burst requires cases, repetitions, and a positive timeout"
        )

    samples: list[dict[str, Any]] = []
    bursts: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="native-burst") as pool:
        burst_index = 0
        for _ in range(bursts_per_case):
            for case in cases:
                released: list[int] = []
                barrier = threading.Barrier(
                    concurrency + 1,
                    action=lambda: released.append(clock_ns()),
                )
                futures: list[Future[dict[str, Any]]] = [
                    pool.submit(
                        _sample_call,
                        clients[worker],
                        case,
                        worker,
                        barrier,
                        timeout_s,
                        clock_ns,
                    )
                    for worker in range(concurrency)
                ]
                try:
                    barrier.wait(timeout=max(5.0, timeout_s))
                except threading.BrokenBarrierError as error:
                    for future in futures:
                        future.cancel()
                    raise NativeBurstFailure("native burst launch barrier broke") from error
                if len(released) != 1:
                    raise NativeBurstFailure("native burst did not capture one release instant")

                raw = [future.result(timeout=timeout_s + 5.0) for future in futures]
                raw.sort(key=lambda sample: sample["worker"])
                release_ns = released[0]
                completed_ns = max(sample["completed_ns"] for sample in raw)
                if any(sample["started_ns"] < release_ns for sample in raw):
                    raise NativeBurstFailure("a measured request started before barrier release")
                burst_id = f"{burst_id_prefix}c{concurrency}-b{burst_index:05d}"
                for sample in raw:
                    sample.update(
                        {
                            "burst_id": burst_id,
                            "concurrency": concurrency,
                            "phase": phase,
                            "lifecycle_iteration": lifecycle_iteration,
                            "released_ns": release_ns,
                            "start_slip_ms": (
                                sample["started_ns"] - release_ns
                            ) / 1_000_000.0,
                        }
                    )
                outcomes = [sample["outcome"] for sample in raw]
                bursts.append(
                    {
                        "burst_id": burst_id,
                        "concurrency": concurrency,
                        "phase": phase,
                        "lifecycle_iteration": lifecycle_iteration,
                        "case_id": case["case_id"],
                        "mode": case["mode"],
                        "released_ns": release_ns,
                        "completed_ns": completed_ns,
                        "wall_elapsed_ms": (completed_ns - release_ns) / 1_000_000.0,
                        "request_count": concurrency,
                        "ok_count": outcomes.count("ok"),
                        "timeout_count": outcomes.count("timeout"),
                        "error_count": sum(
                            outcome not in {"ok", "timeout"} for outcome in outcomes
                        ),
                    }
                )
                samples.extend(raw)
                burst_index += 1
    return samples, bursts


def _warm_cases(
    client: SearchClient,
    cases: Sequence[Mapping[str, Any]],
    timeout_s: float,
) -> None:
    """Populate the central process cache outside the steady-state timing boundary."""

    for case in cases:
        structured = client.search(case, timeout_s)
        validate_query_result(structured, case)


def select_fixture_cases(
    profile: str,
    split: str,
    modes: Sequence[str],
    cases_per_mode: int,
) -> tuple[Mapping[str, Any], tuple[Mapping[str, Any], ...]]:
    if split not in SPLITS or cases_per_mode < 1:
        raise NativeBurstFailure("unsupported fixture split or cases-per-mode")
    if not modes or len(modes) != len(set(modes)) or any(mode not in QUERY_MODES for mode in modes):
        raise NativeBurstFailure("native burst query modes are invalid or duplicated")
    recipe = build_recipe(profile)
    selected: list[Mapping[str, Any]] = []
    for mode in modes:
        matching = [case for case in recipe["query_splits"][split] if case["mode"] == mode]
        if len(matching) < cases_per_mode:
            raise NativeBurstFailure(
                f"fixture split {split} has only {len(matching)} {mode} cases"
            )
        selected.extend(matching[:cases_per_mode])
    return recipe, tuple(selected)


class NativeCentralLifecycle:
    """Own one central daemon, its socket, and persistent stdio proxy routes."""

    def __init__(
        self,
        mcp_binary: Path,
        config_path: Path,
        route_cwd: Path,
        *,
        startup_timeout_s: float,
    ) -> None:
        self.mcp_binary = mcp_binary
        self.config_path = config_path
        self.route_cwd = route_cwd
        self.startup_timeout_s = startup_timeout_s
        self._temporary = tempfile.TemporaryDirectory(prefix="moraine-native-burst-", dir="/tmp")
        self.root = Path(self._temporary.name)
        self.socket_path = self.root / "mcp.sock"
        self._central: Optional[subprocess.Popen[bytes]] = None
        self._central_stdout: Optional[Any] = None
        self._central_stderr: Optional[Any] = None
        self._clients: list[_StdioJsonRpcClient] = []
        self.tool_surfaces: list[tuple[str, ...]] = []
        self._closed = False
        self._cleanup: Optional[dict[str, Any]] = None

    def start(self) -> None:
        if self._central is not None or self._closed:
            raise NativeBurstFailure("native central lifecycle cannot be started twice")
        self._central_stdout = (self.root / "central.stdout").open("wb")
        self._central_stderr = (self.root / "central.stderr").open("wb")
        environment = dict(os.environ)
        environment["RUST_LOG"] = "moraine_mcp=debug"
        try:
            self._central = subprocess.Popen(
                [
                    str(self.mcp_binary),
                    "--config",
                    str(self.config_path),
                    "--serve",
                    "socket",
                    "--socket",
                    str(self.socket_path),
                    "--host",
                    "127.0.0.1",
                    "--port",
                    "0",
                ],
                cwd=self.route_cwd,
                env=environment,
                stdout=self._central_stdout,
                stderr=self._central_stderr,
                start_new_session=True,
            )
        except OSError as error:
            raise NativeBurstFailure("native central daemon failed to execute") from error
        self._wait_for_socket()

    def _wait_for_socket(self) -> None:
        assert self._central is not None
        deadline = time.monotonic() + self.startup_timeout_s
        while time.monotonic() < deadline:
            if self._central.poll() is not None:
                raise NativeBurstFailure(
                    f"native central daemon exited during startup ({self._central.returncode})"
                )
            candidate = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                candidate.settimeout(0.1)
                candidate.connect(str(self.socket_path))
                return
            except OSError:
                time.sleep(0.01)
            finally:
                candidate.close()
        raise NativeBurstFailure("native central socket did not become ready")

    def _spawn_route(self) -> subprocess.Popen[bytes]:
        environment = dict(os.environ)
        environment["RUST_LOG"] = "moraine_mcp=debug"
        try:
            return subprocess.Popen(
                [
                    str(self.mcp_binary),
                    "--config",
                    str(self.config_path),
                    "--serve",
                    "stdio",
                    "--socket",
                    str(self.socket_path),
                ],
                cwd=self.route_cwd,
                env=environment,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
            )
        except OSError as error:
            raise NativeBurstFailure("native central proxy route failed to execute") from error

    def open_clients(self, count: int) -> Sequence[SearchClient]:
        if self._central is None or self._closed or count < 1:
            raise NativeBurstFailure("native central is not ready for proxy clients")
        for _ in range(count):
            client = _StdioJsonRpcClient(self._spawn_route())
            self._clients.append(client)
            client.wait_for_central_route(self.startup_timeout_s)
            client.initialize(self.startup_timeout_s)
            tools = client.list_tools(self.startup_timeout_s)
            self.tool_surfaces.append(_validated_tool_surface(tools))
        if len(set(self.tool_surfaces)) != 1:
            raise NativeBurstFailure("central proxy routes exposed inconsistent tool surfaces")
        return tuple(self._clients)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        failures: list[str] = []
        route_processes = tuple(client.proc for client in self._clients)
        for client in reversed(self._clients):
            try:
                client.close()
            except Exception as error:
                failures.append(f"route:{type(error).__name__}")
        try:
            terminate_process_group(self._central, timeout_s=5.0)
        except Exception as error:
            failures.append(f"central:{type(error).__name__}")
        central_terminated = self._central is None or self._central.poll() is not None
        routes_closed = all(process.poll() is not None for process in route_processes)
        try:
            self.socket_path.unlink(missing_ok=True)
        except OSError as error:
            failures.append(f"socket:{type(error).__name__}")
        socket_removed = not self.socket_path.exists()
        for handle in (self._central_stdout, self._central_stderr):
            if handle is not None:
                handle.close()
        self._cleanup = {
            "central_terminated": central_terminated,
            "routes_closed": routes_closed,
            "socket_removed": socket_removed,
            "query_id_prefix": (
                f"moraine-search-sessions-{self._central.pid}-"
                if self._central is not None
                else None
            ),
        }
        self._temporary.cleanup()
        cleanup_complete = central_terminated and routes_closed and socket_removed
        if failures or not cleanup_complete:
            detail = ",".join(failures) if failures else "owned resource remained"
            raise NativeBurstFailure(f"native central cleanup failed: {detail}")

    def cleanup_evidence(self) -> Mapping[str, Any]:
        if self._cleanup is None:
            raise NativeBurstFailure("native central cleanup evidence requested before close")
        return dict(self._cleanup)


def _request_latency_metrics(samples: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    successful = [
        float(sample["elapsed_ms"])
        for sample in samples
        if sample["outcome"] == "ok"
    ]
    all_latency = [float(sample["elapsed_ms"]) for sample in samples]
    return {
        "request_count": len(samples),
        "ok_count": len(successful),
        "timeout_count": sum(sample["outcome"] == "timeout" for sample in samples),
        "error_count": sum(
            sample["outcome"] not in {"ok", "timeout"} for sample in samples
        ),
        "p50_ms": _nearest_rank(successful, 50.0),
        "p95_ms": _nearest_rank(successful, 95.0),
        "p99_ms": _nearest_rank(successful, 99.0),
        "max_ms": max(successful) if successful else None,
        "max_all_ms": max(all_latency) if all_latency else None,
    }


def _latency_summary(
    samples: Sequence[Mapping[str, Any]],
    bursts: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    by_concurrency: dict[str, Any] = {}
    modes = sorted({str(sample["mode"]) for sample in samples})
    for concurrency in CONCURRENCIES:
        selected = [sample for sample in samples if sample["concurrency"] == concurrency]
        selected_bursts = [burst for burst in bursts if burst["concurrency"] == concurrency]
        metrics = _request_latency_metrics(selected)
        metrics["burst_wall_p95_ms"] = _nearest_rank(
            [float(burst["wall_elapsed_ms"]) for burst in selected_bursts],
            95.0,
        )
        metrics["by_mode"] = {
            mode: _request_latency_metrics(
                [sample for sample in selected if sample["mode"] == mode]
            )
            for mode in modes
        }
        by_concurrency[str(concurrency)] = metrics
    summary = _request_latency_metrics(samples)
    summary["by_concurrency"] = by_concurrency
    return summary


def _summary(
    samples: Sequence[Mapping[str, Any]],
    bursts: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    summary = _latency_summary(samples, bursts)
    summary["by_phase"] = {
        phase: _latency_summary(
            [sample for sample in samples if sample.get("phase") == phase],
            [burst for burst in bursts if burst.get("phase") == phase],
        )
        for phase in (COLD_PHASE, STEADY_PHASE)
    }
    return summary


def execute_native_matrix(
    cases: Sequence[Mapping[str, Any]],
    *,
    bursts_per_case: int,
    timeout_s: float,
    lifecycle_factory: Callable[[int], BurstLifecycle],
    measurement_clock_us: Optional[Callable[[], int]] = None,
    clock_ns: Callable[[], int] = time.monotonic_ns,
    burst_runner: Callable[
        ..., tuple[list[dict[str, Any]], list[dict[str, Any]]]
    ] = run_synchronized_bursts,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    set[tuple[str, ...]],
]:
    all_samples: list[dict[str, Any]] = []
    all_bursts: list[dict[str, Any]] = []
    cleanups: list[dict[str, Any]] = []
    tool_surfaces: set[tuple[str, ...]] = set()
    for concurrency in CONCURRENCIES:
        lifecycle = lifecycle_factory(concurrency)
        failure: Optional[BaseException] = None
        measurement_started_at_us: Optional[int] = None
        try:
            lifecycle.start()
            clients = lifecycle.open_clients(concurrency)
            _warm_cases(clients[0], cases, timeout_s)
            if measurement_clock_us is not None:
                measurement_started_at_us = measurement_clock_us()
            samples, bursts = burst_runner(
                clients,
                cases,
                concurrency=concurrency,
                bursts_per_case=bursts_per_case,
                timeout_s=timeout_s,
                phase=STEADY_PHASE,
                lifecycle_iteration=0,
                burst_id_prefix="steady-",
                clock_ns=clock_ns,
            )
            all_samples.extend(samples)
            all_bursts.extend(bursts)
            tool_surfaces.update(
                _validated_tool_surface(surface) for surface in lifecycle.tool_surfaces
            )
        except BaseException as error:
            failure = error
        try:
            lifecycle.close()
            cleanup = {
                "phase": STEADY_PHASE,
                "concurrency": concurrency,
                "lifecycle_iteration": 0,
                **lifecycle.cleanup_evidence(),
            }
            if measurement_started_at_us is not None:
                cleanup["measurement_started_at_us"] = measurement_started_at_us
            cleanups.append(cleanup)
        except BaseException as cleanup_error:
            if failure is not None:
                cleanup_error.add_note(f"measurement also failed: {type(failure).__name__}")
            raise
        if failure is not None:
            raise failure
    return all_samples, all_bursts, cleanups, tool_surfaces


def execute_cold_matrix(
    cases: Sequence[Mapping[str, Any]],
    *,
    cold_repetitions: int,
    timeout_s: float,
    lifecycle_factory: Callable[[int], BurstLifecycle],
    clock_ns: Callable[[], int] = time.monotonic_ns,
    burst_runner: Callable[
        ..., tuple[list[dict[str, Any]], list[dict[str, Any]]]
    ] = run_synchronized_bursts,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    set[tuple[str, ...]],
]:
    """Measure first queries through repeatedly initialized fresh central daemons."""

    if not cases or cold_repetitions < 1:
        raise NativeBurstFailure("cold lifecycle measurement requires cases and repetitions")
    all_samples: list[dict[str, Any]] = []
    all_bursts: list[dict[str, Any]] = []
    cleanups: list[dict[str, Any]] = []
    tool_surfaces: set[tuple[str, ...]] = set()
    for iteration in range(cold_repetitions):
        order = CONCURRENCIES if iteration % 2 == 0 else tuple(reversed(CONCURRENCIES))
        for concurrency in order:
            lifecycle = lifecycle_factory(concurrency)
            failure: Optional[BaseException] = None
            try:
                lifecycle.start()
                clients = lifecycle.open_clients(concurrency)
                case = cases[iteration % len(cases)]
                samples, bursts = burst_runner(
                    clients,
                    (case,),
                    concurrency=concurrency,
                    bursts_per_case=1,
                    timeout_s=timeout_s,
                    phase=COLD_PHASE,
                    lifecycle_iteration=iteration,
                    burst_id_prefix=f"cold-r{iteration:04d}-",
                    clock_ns=clock_ns,
                )
                all_samples.extend(samples)
                all_bursts.extend(bursts)
                tool_surfaces.update(
                    _validated_tool_surface(surface) for surface in lifecycle.tool_surfaces
                )
            except BaseException as error:
                failure = error
            try:
                lifecycle.close()
                cleanups.append(
                    {
                        "phase": COLD_PHASE,
                        "concurrency": concurrency,
                        "lifecycle_iteration": iteration,
                        **lifecycle.cleanup_evidence(),
                    }
                )
            except BaseException as cleanup_error:
                if failure is not None:
                    cleanup_error.add_note(f"measurement also failed: {type(failure).__name__}")
                raise
            if failure is not None:
                raise failure
    return all_samples, all_bursts, cleanups, tool_surfaces


def _latency_gates(
    summary: Mapping[str, Any],
    *,
    min_cold_samples: int,
    warm_p95_limit_ms: float = DEFAULT_WARM_P95_LIMIT_MS,
    cold_p95_limit_ms: float = DEFAULT_COLD_P95_LIMIT_MS,
    max_latency_ms: float = DEFAULT_MAX_LATENCY_MS,
) -> dict[str, Any]:
    by_concurrency: dict[str, Any] = {}
    phase_summary = summary["by_phase"]
    for concurrency in CONCURRENCIES:
        key = str(concurrency)
        cold = phase_summary[COLD_PHASE]["by_concurrency"][key]
        steady = phase_summary[STEADY_PHASE]["by_concurrency"][key]
        enough_cold = cold["ok_count"] >= min_cold_samples
        warm_modes: dict[str, Any] = {}
        for mode, metrics in steady["by_mode"].items():
            p95 = metrics["p95_ms"]
            passed = (
                metrics["ok_count"] > 0
                and p95 is not None
                and math.isfinite(float(p95))
                and float(p95) <= warm_p95_limit_ms
            )
            warm_modes[mode] = {
                "ok_samples": metrics["ok_count"],
                "p95_ms": p95,
                "pass": passed,
            }
        cold_modes: dict[str, Any] = {}
        for mode in COLD_SLO_MODES:
            metrics = cold["by_mode"].get(mode)
            p95 = metrics["p95_ms"] if metrics is not None else None
            passed = (
                metrics is not None
                and metrics["ok_count"] > 0
                and p95 is not None
                and math.isfinite(float(p95))
                and float(p95) <= cold_p95_limit_ms
            )
            cold_modes[mode] = {
                "ok_samples": 0 if metrics is None else metrics["ok_count"],
                "p95_ms": p95,
                "pass": passed,
            }
        maximum = summary["by_concurrency"][key]["max_all_ms"]
        maximum_pass = (
            maximum is not None
            and math.isfinite(float(maximum))
            and float(maximum) <= max_latency_ms
        )
        by_concurrency[key] = {
            "cold_ok_samples": cold["ok_count"],
            "minimum_cold_samples": min_cold_samples,
            "cold_sample_count_pass": enough_cold,
            "warm_by_mode": warm_modes,
            "cold_slo_by_mode": cold_modes,
            "max_all_ms": maximum,
            "max_latency_pass": maximum_pass,
            "pass": (
                enough_cold
                and bool(warm_modes)
                and all(item["pass"] for item in warm_modes.values())
                and all(item["pass"] for item in cold_modes.values())
                and maximum_pass
            ),
        }
    return {
        "policy": {
            "warm_per_mode_p95_limit_ms": warm_p95_limit_ms,
            "cold_slo_modes": list(COLD_SLO_MODES),
            "cold_slo_p95_limit_ms": cold_p95_limit_ms,
            "all_sample_max_limit_ms": max_latency_ms,
            "comparison": "less_than_or_equal",
            "minimum_cold_samples_per_concurrency": min_cold_samples,
        },
        "by_concurrency": by_concurrency,
        "pass": all(item["pass"] for item in by_concurrency.values()),
    }


def _load_config_document(config_path: Path) -> Mapping[str, Any]:
    try:
        document = tomllib.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise NativeBurstFailure("cannot load native fixture configuration") from error
    if not isinstance(document, dict):
        raise NativeBurstFailure("native fixture configuration is not a TOML table")
    return document


def _load_clickhouse_endpoint(config_path: Path) -> ClickHouseEndpoint:
    document = _load_config_document(config_path)
    clickhouse = document.get("clickhouse")
    backends = document.get("backends", {})
    default_backend = backends.get("default") if isinstance(backends, dict) else None
    if clickhouse is not None and default_backend is not None:
        raise NativeBurstFailure(
            "native fixture config ambiguously declares both default backend aliases"
        )
    table = clickhouse if clickhouse is not None else default_backend
    if not isinstance(table, dict):
        raise NativeBurstFailure(
            "native fixture config must explicitly declare [clickhouse] or [backends.default]"
        )
    try:
        endpoint = ClickHouseEndpoint(
            url=str(table["url"]),
            database=str(table.get("database", "moraine")),
            username=str(table.get("username", "default")),
            password=str(table.get("password", "")),
        )
    except KeyError as error:
        raise NativeBurstFailure("native fixture ClickHouse URL is missing") from error
    return endpoint


def _repo_backend_markers(route_cwd: Path) -> tuple[str, ...]:
    backends: list[str] = []
    for directory in (route_cwd, *route_cwd.parents):
        marker = directory / ".moraine.toml"
        if not marker.is_file():
            continue
        try:
            document = tomllib.loads(marker.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as error:
            raise NativeBurstFailure("cannot verify route-cwd backend marker") from error
        backend = document.get("backend")
        if backend is not None:
            if not isinstance(backend, str) or not backend.strip():
                raise NativeBurstFailure("route-cwd backend marker is invalid")
            backends.append(backend.strip())
    return tuple(backends)


def _validated_loopback_url(url: str) -> urllib.parse.ParseResult:
    parsed = urllib.parse.urlparse(url)
    try:
        explicit_port = parsed.port
    except ValueError as error:
        raise NativeBurstFailure("native fixture ClickHouse URL has an invalid port") from error
    try:
        loopback_host = ipaddress.ip_address(parsed.hostname or "").is_loopback
    except ValueError:
        loopback_host = False
    if (
        parsed.scheme not in {"http", "https"}
        or not loopback_host
        or explicit_port is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        raise NativeBurstFailure(
            "native benchmark requires an explicit loopback ClickHouse HTTP endpoint"
        )
    return parsed


def _resolve_backend_selection(
    config_path: Path,
    route_cwd: Path,
) -> BackendSelection:
    document = _load_config_document(config_path)
    routes = document.get("routes", [])
    if not isinstance(routes, list) or any(not isinstance(route, dict) for route in routes):
        raise NativeBurstFailure("native fixture [[routes]] configuration is invalid")
    non_default_routes = [
        route
        for route in routes
        if str(route.get("backend", "")).strip() not in {"", "default"}
    ]
    marker_backends = _repo_backend_markers(route_cwd)
    if non_default_routes or any(backend != "default" for backend in marker_backends):
        raise NativeBurstFailure(
            "native benchmark refuses route-cwd resolution to a non-default backend"
    )
    endpoint = _load_clickhouse_endpoint(config_path)
    _validated_loopback_url(endpoint.url)
    if not _IDENTIFIER_RE.fullmatch(endpoint.database) or endpoint.database == "moraine":
        raise NativeBurstFailure(
            "native benchmark requires an isolated non-default ClickHouse database"
        )
    return BackendSelection(
        endpoint=endpoint,
        backend_name="default",
        route_policy="default_only_no_non_default_routes_or_repo_markers",
        repo_marker_count=len(marker_backends),
    )


def _clickhouse_query(
    endpoint: ClickHouseEndpoint,
    sql: str,
    *,
    timeout_s: float = 30.0,
) -> str:
    _validated_loopback_url(endpoint.url)
    headers = {
        "Content-Type": "text/plain",
        "X-ClickHouse-User": endpoint.username,
        "X-ClickHouse-Key": endpoint.password,
        "X-ClickHouse-Database": endpoint.database,
    }
    request = urllib.request.Request(
        endpoint.url,
        data=sql.encode("utf-8"),
        headers=headers,
        method="POST",
    )
    # Explicitly bypass environment proxies: this request carries ClickHouse
    # credentials and is valid only for the loopback endpoint checked above.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=timeout_s) as response:
            return response.read().decode("utf-8").strip()
    except (OSError, urllib.error.HTTPError, urllib.error.URLError) as error:
        raise NativeBurstFailure(
            f"ClickHouse benchmark evidence request failed: {type(error).__name__}"
        ) from error


def _sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _json_number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise NativeBurstFailure(f"query-log {field} is not numeric")
    try:
        number = float(value)
    except ValueError as error:
        raise NativeBurstFailure(f"query-log {field} is not numeric") from error
    if not math.isfinite(number):
        raise NativeBurstFailure(f"query-log {field} is not finite")
    return number


def _json_nonnegative_integer(value: Any, field: str) -> int:
    number = _json_number(value, field)
    if number < 0 or not number.is_integer():
        raise NativeBurstFailure(f"query-log {field} is not a nonnegative integer")
    return int(number)


def _sql_identifier(value: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(value):
        raise NativeBurstFailure("native fixture database identifier is unsafe")
    return f"`{value}`"


def _fixture_database_sql(endpoint: ClickHouseEndpoint, recipe: Mapping[str, Any]) -> str:
    database = _sql_identifier(endpoint.database)
    corpus = recipe["corpus"]
    source_name = _sql_string(str(corpus["source_name"]))
    source_file = _sql_string(str(corpus["source_file"]))
    return f"""SELECT
  (SELECT count() FROM {database}.events FINAL) AS events_count,
  (SELECT countIf(
      source_name = {source_name}
      AND source_file = {source_file}
      AND harness = 'codex'
      AND inference_provider = 'synthetic'
      AND endpoint_kind = 'generation'
      AND startsWith(source_ref, 'owned:')
    ) FROM {database}.events FINAL) AS events_fixture_count,
  (SELECT uniqExact(event_uid) FROM {database}.events FINAL) AS events_unique_ids,
  (SELECT uniqExact(session_id) FROM {database}.events FINAL) AS event_sessions,
  (SELECT min(event_uid) FROM {database}.events FINAL) AS event_min_uid,
  (SELECT max(event_uid) FROM {database}.events FINAL) AS event_max_uid,
  (SELECT count() FROM {database}.search_documents FINAL) AS documents_count,
  (SELECT countIf(
      source_name = {source_name}
      AND source_file = {source_file}
      AND harness = 'codex'
      AND inference_provider = 'synthetic'
      AND endpoint_kind = 'generation'
      AND startsWith(source_ref, 'owned:')
    ) FROM {database}.search_documents FINAL) AS documents_fixture_count,
  (SELECT uniqExact(event_uid) FROM {database}.search_documents FINAL) AS documents_unique_ids,
  (SELECT min(event_uid) FROM {database}.search_documents FINAL) AS document_min_uid,
  (SELECT max(event_uid) FROM {database}.search_documents FINAL) AS document_max_uid,
  (SELECT uniqExact(doc_id) FROM {database}.search_postings FINAL) AS posting_documents,
  (SELECT docs FROM {database}.search_corpus_stats LIMIT 1) AS corpus_documents,
  (SELECT count() FROM {database}.mcp_open_sessions FINAL) AS projected_sessions,
  (SELECT ifNull(sum(total_events), 0) FROM {database}.mcp_open_sessions FINAL)
    AS projected_session_events,
  (SELECT count()
   FROM {database}.mcp_open_events AS e FINAL
   INNER JOIN {database}.mcp_open_sessions AS s FINAL
     ON e.session_id = s.session_id
     AND e.slot = s.slot
     AND e.generation = s.generation) AS projected_events,
  (SELECT count()
   FROM {database}.mcp_open_dirty_sessions AS d FINAL
   LEFT JOIN {database}.mcp_open_sessions AS s FINAL
     ON d.session_id = s.session_id
   WHERE d.dirty_revision > ifNull(s.dirty_revision, 0)) AS dirty_session_count,
  (SELECT countIf(state_key = 'global' AND ready = 1 AND backfill_cursor = '')
   FROM {database}.mcp_open_projection_state FINAL) AS projection_ready_rows
  ,(SELECT countIf(
      source_host = ''
      AND source_name = {source_name}
      AND source_file = {source_file}
      AND source_generation = 1
    ) FROM {database}.v_current_published_source_generations)
    AS publication_head_count
  ,(SELECT countIf(
      host = ''
      AND source_name = {source_name}
      AND source_file = {source_file}
      AND source_generation = 1
      AND lifecycle = 'active'
      AND final_scan_complete = 1
      AND block_reason = ''
    ) FROM {database}.v_current_ingest_checkpoint_transitions)
    AS active_checkpoint_count
  ,(SELECT countIf(
      source_host = ''
      AND source_name = {source_name}
      AND source_file = {source_file}
      AND source_generation = 1
      AND complete = 1
      AND block_reason = ''
      AND compatibility_prepared = 1
      AND backend_caught_up = 1
    ) FROM {database}.v_current_source_generation_publication_readiness)
    AS publication_readiness_count
  ,(SELECT countIf(host = '' AND state = 'idle')
    FROM {database}.v_current_ingest_append_control)
    AS idle_append_control_count
FORMAT JSONEachRow"""


def _validate_fixture_database_evidence(
    evidence: Any,
    recipe: Mapping[str, Any],
) -> bool:
    expected_fields = {
        "backend_name",
        "database",
        "endpoint_scope",
        "route_policy",
        "repo_marker_count",
        "server_system",
        "expected_document_count",
        "events_count",
        "events_fixture_count",
        "events_unique_ids",
        "event_sessions",
        "event_min_uid",
        "event_max_uid",
        "documents_count",
        "documents_fixture_count",
        "documents_unique_ids",
        "document_min_uid",
        "document_max_uid",
        "posting_documents",
        "corpus_documents",
        "projected_sessions",
        "projected_session_events",
        "projected_events",
        "dirty_session_count",
        "projection_ready_rows",
        "publication_head_count",
        "active_checkpoint_count",
        "publication_readiness_count",
        "idle_append_control_count",
        "pass",
    }
    if not isinstance(evidence, dict) or set(evidence) != expected_fields:
        raise NativeBurstFailure("native fixture database evidence fields differ")
    expected_documents = int(recipe["corpus"]["document_count"])
    integer_fields = expected_fields - {
        "backend_name",
        "database",
        "endpoint_scope",
        "route_policy",
        "server_system",
        "event_min_uid",
        "event_max_uid",
        "document_min_uid",
        "document_max_uid",
        "pass",
    }
    if any(
        isinstance(evidence.get(field), bool)
        or not isinstance(evidence.get(field), int)
        or evidence[field] < 0
        for field in integer_fields
    ):
        raise NativeBurstFailure("native fixture database counters are invalid")
    expected_min_uid = "perf-event-00000000"
    expected_max_uid = f"perf-event-{expected_documents - 1:08d}"
    passed = bool(
        evidence["backend_name"] == "default"
        and isinstance(evidence["database"], str)
        and _IDENTIFIER_RE.fullmatch(evidence["database"]) is not None
        and evidence["database"] != "moraine"
        and evidence["endpoint_scope"] == "explicit_loopback_native"
        and evidence["route_policy"]
        == "default_only_no_non_default_routes_or_repo_markers"
        and evidence["repo_marker_count"] == 0
        and evidence["server_system"] == "Darwin"
        and evidence["expected_document_count"] == expected_documents
        and evidence["events_count"] == expected_documents
        and evidence["events_fixture_count"] == expected_documents
        and evidence["events_unique_ids"] == expected_documents
        and evidence["event_sessions"] > 0
        and evidence["event_min_uid"] == expected_min_uid
        and evidence["event_max_uid"] == expected_max_uid
        and evidence["documents_count"] == expected_documents
        and evidence["documents_fixture_count"] == expected_documents
        and evidence["documents_unique_ids"] == expected_documents
        and evidence["document_min_uid"] == expected_min_uid
        and evidence["document_max_uid"] == expected_max_uid
        and evidence["posting_documents"] == expected_documents
        and evidence["corpus_documents"] == expected_documents
        and evidence["projected_sessions"] == evidence["event_sessions"]
        and evidence["projected_session_events"] == expected_documents
        and evidence["projected_events"] == expected_documents
        and evidence["dirty_session_count"] == 0
        and evidence["projection_ready_rows"] == 1
        and evidence["publication_head_count"] == 1
        and evidence["active_checkpoint_count"] == 1
        and evidence["publication_readiness_count"] == 1
        and evidence["idle_append_control_count"] == 1
    )
    if evidence.get("pass") is not passed:
        raise NativeBurstFailure("native fixture database evidence status differs")
    return passed


def _fixture_database_evidence(
    selection: BackendSelection,
    recipe: Mapping[str, Any],
) -> dict[str, Any]:
    system = _clickhouse_query(
        selection.endpoint,
        "SELECT value FROM system.build_options WHERE name = 'SYSTEM' FORMAT TSVRaw",
    )
    raw = _clickhouse_query(
        selection.endpoint,
        _fixture_database_sql(selection.endpoint, recipe),
    )
    try:
        rows = [json.loads(line) for line in raw.splitlines() if line]
    except (TypeError, ValueError) as error:
        raise NativeBurstFailure("native fixture database evidence is not JSONEachRow") from error
    if len(rows) != 1 or not isinstance(rows[0], dict):
        raise NativeBurstFailure("native fixture database evidence must contain one row")
    row = rows[0]
    numeric_fields = {
        "events_count",
        "events_fixture_count",
        "events_unique_ids",
        "event_sessions",
        "documents_count",
        "documents_fixture_count",
        "documents_unique_ids",
        "posting_documents",
        "corpus_documents",
        "projected_sessions",
        "projected_session_events",
        "projected_events",
        "dirty_session_count",
        "projection_ready_rows",
        "publication_head_count",
        "active_checkpoint_count",
        "publication_readiness_count",
        "idle_append_control_count",
    }
    string_fields = {
        "event_min_uid",
        "event_max_uid",
        "document_min_uid",
        "document_max_uid",
    }
    if set(row) != numeric_fields | string_fields:
        raise NativeBurstFailure("native fixture database query returned unexpected fields")
    evidence: dict[str, Any] = {
        "backend_name": selection.backend_name,
        "database": selection.endpoint.database,
        "endpoint_scope": "explicit_loopback_native",
        "route_policy": selection.route_policy,
        "repo_marker_count": selection.repo_marker_count,
        "server_system": system,
        "expected_document_count": int(recipe["corpus"]["document_count"]),
        **{field: _json_nonnegative_integer(row[field], field) for field in numeric_fields},
        **{field: str(row[field]) for field in string_fields},
        "pass": False,
    }
    evidence["pass"] = _fixture_evidence_passes(evidence, recipe)
    _validate_fixture_database_evidence(evidence, recipe)
    if not evidence["pass"]:
        raise NativeBurstFailure(
            "native fixture database is not isolated, complete, and projection-ready"
        )
    return evidence


def _fixture_evidence_passes(
    evidence: Mapping[str, Any],
    recipe: Mapping[str, Any],
) -> bool:
    candidate = dict(evidence)
    candidate["pass"] = True
    try:
        return _validate_fixture_database_evidence(candidate, recipe)
    except NativeBurstFailure:
        return False


def _query_log_rows_sql(
    started_at_us: int,
    ended_at_us: int,
    query_id_prefixes: Sequence[str],
) -> str:
    prefixes = sorted(set(query_id_prefixes))
    if not prefixes:
        raise NativeBurstFailure("query-log evidence has no owned central query prefixes")
    prefix_filter = " OR ".join(
        f"startsWith(query_id, {_sql_string(prefix)})" for prefix in prefixes
    )
    return f"""SELECT
  query_id,
  multiIf(
    position(query, '/* moraine:publication_snapshot:capture */') > 0,
      'publication_capture',
    position(query, '/* moraine:append_fence:capture */') > 0,
      'append_fence_capture',
    position(query, '/* moraine:publication_snapshot:revalidate */') > 0,
      'publication_revalidate',
    position(query, '/* moraine:append_fence:revalidate */') > 0,
      'append_fence_revalidate',
    position(query, 'projected_candidates AS') > 0, 'candidate',
    position(query, 'candidate_heads AS') > 0, 'detail',
    'unknown'
  ) AS stage,
  type,
  toUnixTimestamp64Micro(event_time_microseconds) AS event_time_us,
  toFloat64(query_duration_ms) AS query_duration_ms,
  toUInt64(read_rows) AS read_rows,
  toUInt64(read_bytes) AS read_bytes,
  toUInt64(result_rows) AS result_rows,
  toUInt64(memory_usage) AS memory_usage
FROM system.query_log
WHERE type IN ('QueryFinish', 'ExceptionBeforeStart', 'ExceptionWhileProcessing')
  AND toUnixTimestamp64Micro(event_time_microseconds) >= {started_at_us}
  AND toUnixTimestamp64Micro(event_time_microseconds) <= {ended_at_us}
  AND ({prefix_filter})
ORDER BY event_time_us, query_id, type
FORMAT JSONEachRow"""


def _empty_stage_counters() -> dict[str, int]:
    return {
        f"{stage}_{suffix}": 0
        for stage in QUERY_STAGES
        for suffix in ("queries", "exceptions")
    }


def _expected_stage_queries(
    *,
    phase: str,
    concurrency: int,
    steady_warmup_case_count: int,
    steady_measured_requests_per_client: int,
) -> tuple[dict[str, int], dict[str, int]]:
    if phase == COLD_PHASE:
        expected = {stage: concurrency for stage in (*CONTROL_QUERY_STAGES, *BUSINESS_QUERY_STAGES)}
        return expected, dict(expected)
    measured = concurrency * steady_measured_requests_per_client
    total = {
        stage: steady_warmup_case_count + measured
        for stage in CONTROL_QUERY_STAGES
    }
    total.update(
        {stage: steady_warmup_case_count for stage in BUSINESS_QUERY_STAGES}
    )
    measured_queries = {stage: measured for stage in CONTROL_QUERY_STAGES}
    measured_queries.update({stage: 0 for stage in BUSINESS_QUERY_STAGES})
    return total, measured_queries


class QueryLogCollector:
    """Collect costs and exact statement budgets for every owned daemon."""

    def __init__(self, endpoint: ClickHouseEndpoint) -> None:
        self.endpoint = endpoint
        self.started_at_us = self.server_now_us()

    def server_now_us(self) -> int:
        value = _clickhouse_query(
            self.endpoint,
            "SELECT toUnixTimestamp64Micro(now64(6)) FORMAT TSVRaw",
        )
        if not value:
            raise NativeBurstFailure("ClickHouse returned an empty server timestamp")
        return _json_nonnegative_integer(value, "server_timestamp_us")

    def collect(
        self,
        ownership: Sequence[Mapping[str, Any]],
        *,
        steady_warmup_case_count: int,
        steady_measured_requests_per_client: int,
    ) -> dict[str, Any]:
        if (
            isinstance(steady_warmup_case_count, bool)
            or not isinstance(steady_warmup_case_count, int)
            or steady_warmup_case_count < 1
            or isinstance(steady_measured_requests_per_client, bool)
            or not isinstance(steady_measured_requests_per_client, int)
            or steady_measured_requests_per_client < 1
        ):
            raise NativeBurstFailure("query-log statement budget is invalid")
        prefixes = [str(item.get("query_id_prefix", "")) for item in ownership]
        if not prefixes or any(not prefix for prefix in prefixes):
            raise NativeBurstFailure("query-log evidence has no owned central query prefixes")
        if len(prefixes) != len(set(prefixes)):
            raise NativeBurstFailure("query-log daemon prefixes are not unique")
        for item in ownership:
            phase = item.get("phase")
            cutoff = item.get("measurement_started_at_us")
            if (
                phase not in {COLD_PHASE, STEADY_PHASE}
                or item.get("concurrency") not in CONCURRENCIES
                or isinstance(item.get("lifecycle_iteration"), bool)
                or not isinstance(item.get("lifecycle_iteration"), int)
                or (phase == STEADY_PHASE and (
                    isinstance(cutoff, bool) or not isinstance(cutoff, int)
                ))
                or (phase == COLD_PHASE and cutoff is not None)
            ):
                raise NativeBurstFailure("query-log lifecycle ownership is invalid")
        ended_at_us = self.server_now_us()
        _clickhouse_query(self.endpoint, "SYSTEM FLUSH LOGS")
        sql = _query_log_rows_sql(self.started_at_us, ended_at_us, prefixes)
        raw = _clickhouse_query(self.endpoint, sql)
        stage_rows: dict[str, list[dict[str, Any]]] = {
            stage: [] for stage in QUERY_STAGES
        }
        lifecycle_rows: dict[tuple[str, int, int], dict[str, Any]] = {}
        ownership_by_prefix = {str(item["query_id_prefix"]): item for item in ownership}
        for item in ownership:
            identity = (
                str(item["phase"]),
                int(item["concurrency"]),
                int(item["lifecycle_iteration"]),
            )
            if identity in lifecycle_rows:
                raise NativeBurstFailure("query-log lifecycle identity is duplicated")
            lifecycle_rows[identity] = {
                "phase": identity[0],
                "concurrency": identity[1],
                "lifecycle_iteration": identity[2],
                "total": _empty_stage_counters(),
                "measured": _empty_stage_counters(),
            }
        for line in raw.splitlines():
            try:
                row = json.loads(line)
            except (ValueError, TypeError) as error:
                raise NativeBurstFailure("query-log evidence is not JSONEachRow") from error
            if not isinstance(row, dict):
                raise NativeBurstFailure("query-log evidence row is not an object")
            stage = row.get("stage")
            query_id = row.get("query_id")
            query_type = row.get("type")
            if (
                stage not in QUERY_STAGES
                or not isinstance(query_id, str)
                or query_type
                not in {"QueryFinish", "ExceptionBeforeStart", "ExceptionWhileProcessing"}
            ):
                raise NativeBurstFailure("query-log evidence has an invalid stage")
            matches = [prefix for prefix in prefixes if query_id.startswith(prefix)]
            if len(matches) != 1:
                raise NativeBurstFailure("query-log row has ambiguous daemon ownership")
            owner = ownership_by_prefix[matches[0]]
            identity = (
                str(owner["phase"]),
                int(owner["concurrency"]),
                int(owner["lifecycle_iteration"]),
            )
            event_time_us = _json_nonnegative_integer(
                row.get("event_time_us"), "event_time_us"
            )
            normalized = {
                "type": query_type,
                "query_duration_ms": _json_number(
                    row.get("query_duration_ms"), "query_duration_ms"
                ),
                "read_rows": _json_nonnegative_integer(row.get("read_rows"), "read_rows"),
                "read_bytes": _json_nonnegative_integer(row.get("read_bytes"), "read_bytes"),
                "result_rows": _json_nonnegative_integer(
                    row.get("result_rows"), "result_rows"
                ),
                "memory_usage": _json_nonnegative_integer(
                    row.get("memory_usage"), "memory_usage"
                ),
            }
            stage_rows[stage].append(normalized)
            measured = owner["phase"] == COLD_PHASE or event_time_us >= int(
                owner["measurement_started_at_us"]
            )
            suffix = "queries" if query_type == "QueryFinish" else "exceptions"
            field = f"{stage}_{suffix}"
            lifecycle_rows[identity]["total"][field] += 1
            if measured:
                lifecycle_rows[identity]["measured"][field] += 1

        by_stage: dict[str, Any] = {}
        for stage, rows in stage_rows.items():
            finished = [row for row in rows if row["type"] == "QueryFinish"]
            latency = [float(row["query_duration_ms"]) for row in finished]
            by_stage[stage] = {
                "query_count": len(finished),
                "exception_count": len(rows) - len(finished),
                "p50_ms": _nearest_rank(latency, 50.0),
                "p95_ms": _nearest_rank(latency, 95.0),
                "max_ms": max(latency) if latency else None,
                "read_rows": sum(row["read_rows"] for row in finished),
                "read_bytes": sum(row["read_bytes"] for row in finished),
                "result_rows": sum(row["result_rows"] for row in finished),
                "max_memory_bytes": max(
                    (row["memory_usage"] for row in finished), default=0
                ),
            }

        by_lifecycle: list[dict[str, Any]] = []
        for identity in sorted(lifecycle_rows):
            item = lifecycle_rows[identity]
            expected_total, expected_measured = _expected_stage_queries(
                phase=item["phase"],
                concurrency=item["concurrency"],
                steady_warmup_case_count=steady_warmup_case_count,
                steady_measured_requests_per_client=steady_measured_requests_per_client,
            )
            item["expected_total_queries"] = expected_total
            item["expected_measured_queries"] = expected_measured
            item["pass"] = bool(
                all(
                    item["total"][f"{stage}_queries"] == count
                    for stage, count in expected_total.items()
                )
                and all(
                    item["measured"][f"{stage}_queries"] == count
                    for stage, count in expected_measured.items()
                )
                and all(
                    item[scope][f"{stage}_exceptions"] == 0
                    for scope in ("total", "measured")
                    for stage in QUERY_STAGES
                )
                and item["total"]["unknown_queries"] == 0
                and item["measured"]["unknown_queries"] == 0
            )
            by_lifecycle.append(item)
        coverage_by_phase = {
            phase: {
                "lifecycle_count": sum(item["phase"] == phase for item in by_lifecycle),
                "passed_lifecycle_count": sum(
                    item["phase"] == phase and item["pass"] for item in by_lifecycle
                ),
            }
            for phase in (COLD_PHASE, STEADY_PHASE)
        }
        coverage_pass = all(
            counts["lifecycle_count"] > 0
            and counts["passed_lifecycle_count"] == counts["lifecycle_count"]
            for counts in coverage_by_phase.values()
        )
        coverage = {
            "lifecycle_count": len(by_lifecycle),
            "passed_lifecycle_count": sum(item["pass"] for item in by_lifecycle),
            "by_phase": coverage_by_phase,
            "pass": coverage_pass,
        }
        return {
            "requested": True,
            "captured": True,
            "scope": "owned_daemons_per_lifecycle_with_steady_measurement_windows",
            "window_started_at_us": self.started_at_us,
            "window_ended_at_us": ended_at_us,
            "owned_query_prefix_count": len(prefixes),
            "by_stage": by_stage,
            "by_lifecycle": by_lifecycle,
            "coverage": coverage,
            "pass": coverage_pass,
        }


def _query_log_not_requested() -> dict[str, Any]:
    return {
        "requested": False,
        "captured": False,
        "scope": None,
        "window_started_at_us": None,
        "window_ended_at_us": None,
        "owned_query_prefix_count": 0,
        "by_stage": {},
        "by_lifecycle": [],
        "coverage": None,
        "pass": True,
    }


def _validate_query_log_evidence(
    evidence: Any,
    *,
    requested: bool,
    lifecycles: Sequence[Mapping[str, Any]],
    steady_warmup_case_count: int,
    steady_measured_requests_per_client: int,
) -> bool:
    expected_fields = {
        "requested",
        "captured",
        "scope",
        "window_started_at_us",
        "window_ended_at_us",
        "owned_query_prefix_count",
        "by_stage",
        "by_lifecycle",
        "coverage",
        "pass",
    }
    if not isinstance(evidence, dict) or set(evidence) != expected_fields:
        raise NativeBurstFailure("native burst query-log fields differ")
    if evidence.get("requested") is not requested:
        raise NativeBurstFailure("native burst query-log request metadata differs")
    if not requested:
        if evidence != _query_log_not_requested():
            raise NativeBurstFailure("native burst omitted query-log evidence is invalid")
        return True
    if (
        evidence.get("captured") is not True
        or evidence.get("scope")
        != "owned_daemons_per_lifecycle_with_steady_measurement_windows"
        or isinstance(evidence.get("window_started_at_us"), bool)
        or not isinstance(evidence.get("window_started_at_us"), int)
        or isinstance(evidence.get("window_ended_at_us"), bool)
        or not isinstance(evidence.get("window_ended_at_us"), int)
        or evidence["window_started_at_us"] > evidence["window_ended_at_us"]
        or isinstance(evidence.get("owned_query_prefix_count"), bool)
        or not isinstance(evidence.get("owned_query_prefix_count"), int)
        or evidence["owned_query_prefix_count"] != len(lifecycles)
        or not isinstance(evidence.get("by_stage"), dict)
        or set(evidence["by_stage"]) != set(QUERY_STAGES)
        or not isinstance(evidence.get("by_lifecycle"), list)
        or len(evidence["by_lifecycle"]) != len(lifecycles)
    ):
        raise NativeBurstFailure("native burst query-log capture metadata is invalid")

    metric_fields = {
        "query_count",
        "exception_count",
        "p50_ms",
        "p95_ms",
        "max_ms",
        "read_rows",
        "read_bytes",
        "result_rows",
        "max_memory_bytes",
    }
    for metrics in evidence["by_stage"].values():
        if not isinstance(metrics, dict) or set(metrics) != metric_fields:
            raise NativeBurstFailure("native burst query-log stage fields differ")
        for field in (
            "query_count",
            "exception_count",
            "read_rows",
            "read_bytes",
            "result_rows",
            "max_memory_bytes",
        ):
            value = metrics[field]
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise NativeBurstFailure("native burst query-log counters are invalid")
        latency = [metrics[field] for field in ("p50_ms", "p95_ms", "max_ms")]
        if metrics["query_count"] == 0:
            if latency != [None, None, None]:
                raise NativeBurstFailure("native burst empty query-log stage has latency")
        elif any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or float(value) < 0
            for value in latency
        ):
            raise NativeBurstFailure("native burst query-log latency is invalid")
        elif not float(latency[0]) <= float(latency[1]) <= float(latency[2]):
            raise NativeBurstFailure("native burst query-log percentiles are unordered")

    counter_fields = set(_empty_stage_counters())
    expected_identities = {
        (item.get("phase"), item.get("concurrency"), item.get("lifecycle_iteration"))
        for item in lifecycles
    }
    observed_identities: set[tuple[Any, Any, Any]] = set()
    for item in evidence["by_lifecycle"]:
        if not isinstance(item, dict) or set(item) != {
            "phase",
            "concurrency",
            "lifecycle_iteration",
            "total",
            "measured",
            "expected_total_queries",
            "expected_measured_queries",
            "pass",
        }:
            raise NativeBurstFailure("native burst query-log lifecycle fields differ")
        identity = (item["phase"], item["concurrency"], item["lifecycle_iteration"])
        if identity not in expected_identities:
            raise NativeBurstFailure("native burst query-log lifecycle identity differs")
        observed_identities.add(identity)
        for field in ("total", "measured"):
            counters = item[field]
            if (
                not isinstance(counters, dict)
                or set(counters) != counter_fields
                or any(
                    isinstance(value, bool) or not isinstance(value, int) or value < 0
                    for value in counters.values()
                )
            ):
                raise NativeBurstFailure("native burst query-log lifecycle counters differ")
        expected_total, expected_measured = _expected_stage_queries(
            phase=item["phase"],
            concurrency=item["concurrency"],
            steady_warmup_case_count=steady_warmup_case_count,
            steady_measured_requests_per_client=steady_measured_requests_per_client,
        )
        passed = bool(
            item["expected_total_queries"] == expected_total
            and item["expected_measured_queries"] == expected_measured
            and all(
                item["total"][f"{stage}_queries"] == count
                for stage, count in expected_total.items()
            )
            and all(
                item["measured"][f"{stage}_queries"] == count
                for stage, count in expected_measured.items()
            )
            and all(
                item[scope][f"{stage}_exceptions"] == 0
                for scope in ("total", "measured")
                for stage in QUERY_STAGES
            )
            and item["total"]["unknown_queries"] == 0
            and item["measured"]["unknown_queries"] == 0
        )
        if item.get("pass") is not passed:
            raise NativeBurstFailure("native burst query-log lifecycle status differs")
    if observed_identities != expected_identities:
        raise NativeBurstFailure("native burst query-log lifecycle coverage differs")
    for stage in QUERY_STAGES:
        if evidence["by_stage"][stage]["query_count"] != sum(
            item["total"][f"{stage}_queries"] for item in evidence["by_lifecycle"]
        ) or evidence["by_stage"][stage]["exception_count"] != sum(
            item["total"][f"{stage}_exceptions"] for item in evidence["by_lifecycle"]
        ):
            raise NativeBurstFailure("native burst query-log stage totals differ")
    expected_coverage_by_phase = {
        phase: {
            "lifecycle_count": sum(
                item["phase"] == phase for item in evidence["by_lifecycle"]
            ),
            "passed_lifecycle_count": sum(
                item["phase"] == phase and item["pass"]
                for item in evidence["by_lifecycle"]
            ),
        }
        for phase in (COLD_PHASE, STEADY_PHASE)
    }
    evidence_passes = all(
        counts["lifecycle_count"] > 0
        and counts["passed_lifecycle_count"] == counts["lifecycle_count"]
        for counts in expected_coverage_by_phase.values()
    )
    expected_coverage = {
        "lifecycle_count": len(lifecycles),
        "passed_lifecycle_count": sum(
            item["pass"] for item in evidence["by_lifecycle"]
        ),
        "by_phase": expected_coverage_by_phase,
        "pass": evidence_passes,
    }
    if evidence.get("coverage") != expected_coverage:
        raise NativeBurstFailure("native burst query-log coverage summary differs")
    if evidence.get("pass") is not evidence_passes:
        raise NativeBurstFailure("native burst query-log status differs")
    return evidence_passes


def _validate_native_inputs(
    mcp_binary: Path,
    config_path: Path,
    route_cwd: Path,
    *,
    system: Optional[str] = None,
    machine: Optional[str] = None,
) -> tuple[str, str, tuple[str, ...]]:
    observed_system = (system or platform.system()).lower()
    observed_machine = (machine or platform.machine()).lower()
    if observed_system != "darwin" or observed_machine not in {"arm64", "aarch64"}:
        raise NativeBurstFailure("native-central-burst requires native arm64 macOS Python")
    if not mcp_binary.is_file() or not os.access(mcp_binary, os.X_OK):
        raise NativeBurstFailure("--mcp-binary must be an executable file")
    if "debug" in mcp_binary.parts:
        raise NativeBurstFailure("native-central-burst refuses a debug binary")
    if not config_path.is_file():
        raise NativeBurstFailure("--config must name an existing fixture configuration")
    try:
        config_path.resolve().relative_to((Path.home() / ".moraine").resolve())
    except ValueError:
        pass
    else:
        raise NativeBurstFailure(
            "native benchmark refuses configuration from live ~/.moraine state"
        )
    if not route_cwd.is_dir():
        raise NativeBurstFailure("--route-cwd must be an existing directory")
    config_document = _load_config_document(config_path)
    mcp_config = config_document.get("mcp", {})
    if not isinstance(mcp_config, dict):
        raise NativeBurstFailure("--config [mcp] must be a TOML table")
    if mcp_config.get("prewarm_on_initialize", False) is not False:
        raise NativeBurstFailure(
            "native cold measurement requires mcp.prewarm_on_initialize=false"
        )
    try:
        identity = subprocess.run(
            ["/usr/bin/lipo", "-archs", str(mcp_binary)],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10.0,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise NativeBurstFailure("cannot inspect native MCP binary architecture") from error
    architectures = tuple(identity.stdout.split())
    if identity.returncode or "arm64" not in architectures:
        raise NativeBurstFailure("--mcp-binary does not contain a native arm64 slice")
    return observed_system, observed_machine, architectures


def _input_hash(path: Path) -> str:
    try:
        return hash_file(path)
    except RuntimeFailure as error:
        raise NativeBurstFailure("native benchmark input identity could not be verified") from error


def run_native_central_burst(
    *,
    mcp_binary: Path,
    config_path: Path,
    route_cwd: Path,
    profile: str,
    split: str,
    modes: Sequence[str],
    cases_per_mode: int,
    bursts_per_case: int,
    timeout_s: float,
    startup_timeout_s: float,
    cold_repetitions: int = DEFAULT_COLD_REPETITIONS,
    min_cold_samples: int = DEFAULT_MIN_COLD_SAMPLES,
    warm_p95_limit_ms: float = DEFAULT_WARM_P95_LIMIT_MS,
    cold_p95_limit_ms: float = DEFAULT_COLD_P95_LIMIT_MS,
    max_latency_ms: float = DEFAULT_MAX_LATENCY_MS,
    collect_query_log: bool = False,
) -> dict[str, Any]:
    observed_system, observed_machine, binary_architectures = _validate_native_inputs(
        mcp_binary, config_path, route_cwd
    )
    limits = (warm_p95_limit_ms, cold_p95_limit_ms, max_latency_ms)
    if (
        cold_repetitions < 1
        or min_cold_samples < 1
        or min_cold_samples > cold_repetitions
        or bursts_per_case < 1
        or timeout_s <= 0
        or startup_timeout_s <= 0
        or any(not math.isfinite(limit) or limit <= 0 for limit in limits)
    ):
        raise NativeBurstFailure(
            "native repetitions, timeouts, and latency gates must be positive"
    )
    recipe, cases = select_fixture_cases(profile, split, modes, cases_per_mode)
    if any(mode not in modes for mode in COLD_SLO_MODES):
        raise NativeBurstFailure(
            "native benchmark modes must include hydration (common) and session_scope"
        )
    binary_pre_sha256 = _input_hash(mcp_binary)
    config_pre_sha256 = _input_hash(config_path)
    source_selection = _resolve_backend_selection(config_path, route_cwd)

    with tempfile.TemporaryDirectory(prefix="moraine-native-frozen-", dir="/tmp") as raw:
        frozen_root = Path(raw)
        frozen_binary = frozen_root / "moraine-mcp"
        frozen_config = frozen_root / "moraine.toml"
        frozen_route_cwd = frozen_root / "route"
        frozen_route_cwd.mkdir()
        try:
            shutil.copy2(mcp_binary, frozen_binary)
            shutil.copy2(config_path, frozen_config)
        except OSError as error:
            raise NativeBurstFailure("cannot freeze native benchmark inputs") from error
        binary_frozen_sha256 = _input_hash(frozen_binary)
        config_frozen_sha256 = _input_hash(frozen_config)
        if (
            binary_frozen_sha256 != binary_pre_sha256
            or config_frozen_sha256 != config_pre_sha256
        ):
            raise NativeBurstFailure("native benchmark input changed while being frozen")
        selection = _resolve_backend_selection(frozen_config, frozen_route_cwd)
        if selection.endpoint != source_selection.endpoint:
            raise NativeBurstFailure("frozen native backend identity differs")
        fixture_before = _fixture_database_evidence(selection, recipe)
        query_log_collector = (
            QueryLogCollector(selection.endpoint) if collect_query_log else None
        )

        def lifecycle_factory(_concurrency: int) -> NativeCentralLifecycle:
            return NativeCentralLifecycle(
                frozen_binary,
                frozen_config,
                frozen_route_cwd,
                startup_timeout_s=startup_timeout_s,
            )

        cold_samples, cold_bursts, cold_cleanups, cold_surfaces = execute_cold_matrix(
            cases,
            cold_repetitions=cold_repetitions,
            timeout_s=timeout_s,
            lifecycle_factory=lifecycle_factory,
        )
        steady_samples, steady_bursts, steady_cleanups, steady_surfaces = (
            execute_native_matrix(
                cases,
                bursts_per_case=bursts_per_case,
                timeout_s=timeout_s,
                lifecycle_factory=lifecycle_factory,
                measurement_clock_us=(
                    query_log_collector.server_now_us
                    if query_log_collector is not None
                    else None
                ),
            )
        )
        samples = cold_samples + steady_samples
        bursts = cold_bursts + steady_bursts
        cleanups = cold_cleanups + steady_cleanups
        surfaces = cold_surfaces | steady_surfaces
        if len(surfaces) != 1:
            raise NativeBurstFailure("native daemon tool surface changed across lifecycles")
        observed_surface = _validated_tool_surface(next(iter(surfaces)))
        summary = _summary(samples, bursts)
        gates = _latency_gates(
            summary,
            min_cold_samples=min_cold_samples,
            warm_p95_limit_ms=warm_p95_limit_ms,
            cold_p95_limit_ms=cold_p95_limit_ms,
            max_latency_ms=max_latency_ms,
        )
        if query_log_collector is None:
            query_log = _query_log_not_requested()
        else:
            query_log = query_log_collector.collect(
                cleanups,
                steady_warmup_case_count=len(cases),
                steady_measured_requests_per_client=len(cases) * bursts_per_case,
            )
        fixture_after = _fixture_database_evidence(selection, recipe)

    binary_post_sha256 = _input_hash(mcp_binary)
    config_post_sha256 = _input_hash(config_path)
    post_selection = _resolve_backend_selection(config_path, route_cwd)
    if (
        binary_post_sha256 != binary_pre_sha256
        or config_post_sha256 != config_pre_sha256
        or post_selection != source_selection
    ):
        raise NativeBurstFailure("native benchmark inputs changed during measurement")
    if fixture_after != fixture_before:
        raise NativeBurstFailure("native fixture or projection changed during measurement")
    input_identity = {
        "mcp_binary": {
            "pre_sha256": binary_pre_sha256,
            "frozen_sha256": binary_frozen_sha256,
            "post_sha256": binary_post_sha256,
        },
        "config": {
            "pre_sha256": config_pre_sha256,
            "frozen_sha256": config_frozen_sha256,
            "post_sha256": config_post_sha256,
        },
        "frozen_execution": True,
        "pass": True,
    }
    fixture_database = {
        "before": fixture_before,
        "after": fixture_after,
        "stable": True,
        "pass": True,
    }
    for cleanup in cleanups:
        cleanup.pop("query_id_prefix", None)
        cleanup.pop("measurement_started_at_us", None)
    requests_pass = summary["ok_count"] == summary["request_count"]
    status = "pass" if requests_pass and gates["pass"] and query_log["pass"] else "fail"
    return {
        "document_type": DOCUMENT_TYPE,
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "run": {
            "clock": "time.monotonic_ns",
            "platform": observed_system,
            "machine": observed_machine,
            "binary_architectures": list(binary_architectures),
            "mcp_binary_sha256": binary_pre_sha256,
            "config_sha256": config_pre_sha256,
            "fixture_sha256": recipe["fixture_sha256"],
            "profile": profile,
            "split": split,
            "modes": list(modes),
            "cases_per_mode": cases_per_mode,
            "bursts_per_case": bursts_per_case,
            "cold_repetitions": cold_repetitions,
            "minimum_cold_samples": min_cold_samples,
            "warm_p95_limit_ms": warm_p95_limit_ms,
            "cold_p95_limit_ms": cold_p95_limit_ms,
            "max_latency_ms": max_latency_ms,
            "timeout_seconds": timeout_s,
            "steady_state_warmup_requests_per_concurrency": len(cases),
            "collect_query_log": collect_query_log,
            "concurrencies": list(CONCURRENCIES),
        },
        "tool_surface": {
            "required_tool": TOOL_NAME,
            "forbidden_internal_tools": list(FORBIDDEN_INTERNAL_TOOLS),
            "observed": [list(observed_surface)],
            "search_mcp_events_exposed": False,
            "consistent": True,
            "pass": True,
        },
        "input_identity": input_identity,
        "fixture_database": fixture_database,
        "cases": [
            {
                "case_id": case["case_id"],
                "mode": case["mode"],
                "arguments_sha256": sha256_json(case["arguments"]),
                "oracle_sha256": sha256_json(case["oracle"]),
            }
            for case in cases
        ],
        "lifecycles": cleanups,
        "bursts": bursts,
        "samples": samples,
        "summary": summary,
        "gates": gates,
        "query_log": query_log,
    }


def validate_native_burst_artifact(document: Any) -> Mapping[str, Any]:
    required = {
        "document_type", "schema_version", "status", "run", "tool_surface",
        "input_identity", "fixture_database", "cases", "lifecycles", "bursts",
        "samples", "summary", "gates", "query_log",
    }
    if not isinstance(document, dict) or set(document) != required:
        raise NativeBurstFailure("native burst artifact fields differ")
    if (
        document["document_type"] != DOCUMENT_TYPE
        or document["schema_version"] != SCHEMA_VERSION
        or document["status"] not in {"pass", "fail"}
    ):
        raise NativeBurstFailure("native burst artifact identity differs")

    run = document["run"]
    cases = document["cases"]
    cold_repetitions = run.get("cold_repetitions", 0) if isinstance(run, dict) else 0
    min_cold_samples = run.get("minimum_cold_samples", 0) if isinstance(run, dict) else 0
    warm_p95_limit_ms = run.get("warm_p95_limit_ms") if isinstance(run, dict) else None
    cold_p95_limit_ms = run.get("cold_p95_limit_ms") if isinstance(run, dict) else None
    max_latency_ms = run.get("max_latency_ms") if isinstance(run, dict) else None
    bursts_per_case = run.get("bursts_per_case", 0) if isinstance(run, dict) else 0
    cases_per_mode = run.get("cases_per_mode", 0) if isinstance(run, dict) else 0
    timeout_seconds = run.get("timeout_seconds") if isinstance(run, dict) else None
    modes = run.get("modes") if isinstance(run, dict) else None
    case_fields = {"case_id", "mode", "arguments_sha256", "oracle_sha256"}
    run_fields = {
        "clock",
        "platform",
        "machine",
        "binary_architectures",
        "mcp_binary_sha256",
        "config_sha256",
        "fixture_sha256",
        "profile",
        "split",
        "modes",
        "cases_per_mode",
        "bursts_per_case",
        "cold_repetitions",
        "minimum_cold_samples",
        "warm_p95_limit_ms",
        "cold_p95_limit_ms",
        "max_latency_ms",
        "timeout_seconds",
        "steady_state_warmup_requests_per_concurrency",
        "collect_query_log",
        "concurrencies",
    }
    if (
        not isinstance(run, dict)
        or set(run) != run_fields
        or run.get("clock") != "time.monotonic_ns"
        or run.get("platform") != "darwin"
        or run.get("machine") not in {"arm64", "aarch64"}
        or "arm64" not in run.get("binary_architectures", [])
        or run.get("concurrencies") != list(CONCURRENCIES)
        or not isinstance(modes, list)
        or not modes
        or any(not isinstance(mode, str) or mode not in QUERY_MODES for mode in modes)
        or len(modes) != len(set(modes))
        or any(mode not in modes for mode in COLD_SLO_MODES)
        or run.get("profile") not in {"smoke", "full"}
        or run.get("split") not in SPLITS
        or isinstance(cases_per_mode, bool)
        or not isinstance(cases_per_mode, int)
        or cases_per_mode < 1
        or isinstance(bursts_per_case, bool)
        or not isinstance(bursts_per_case, int)
        or bursts_per_case < 1
        or not isinstance(timeout_seconds, (int, float))
        or isinstance(timeout_seconds, bool)
        or not math.isfinite(float(timeout_seconds))
        or float(timeout_seconds) <= 0
        or not isinstance(cases, list)
        or len(cases) != len(modes) * cases_per_mode
        or any(not isinstance(case, dict) or set(case) != case_fields for case in cases)
        or any(
            not isinstance(case["case_id"], str)
            or not case["case_id"]
            or not isinstance(case["mode"], str)
            or case["mode"] not in modes
            or not isinstance(case["arguments_sha256"], str)
            or not isinstance(case["oracle_sha256"], str)
            for case in cases
        )
        or len({case["case_id"] for case in cases}) != len(cases)
        or isinstance(cold_repetitions, bool)
        or not isinstance(cold_repetitions, int)
        or cold_repetitions < 1
        or isinstance(min_cold_samples, bool)
        or not isinstance(min_cold_samples, int)
        or min_cold_samples < 1
        or min_cold_samples > cold_repetitions
        or any(
            not isinstance(limit, (int, float))
            or isinstance(limit, bool)
            or not math.isfinite(float(limit))
            or float(limit) <= 0
            for limit in (
                warm_p95_limit_ms,
                cold_p95_limit_ms,
                max_latency_ms,
            )
        )
        or run.get("steady_state_warmup_requests_per_concurrency") != len(cases)
        or not isinstance(run.get("collect_query_log"), bool)
    ):
        raise NativeBurstFailure("native burst run metadata is invalid")

    recipe, expected_cases = select_fixture_cases(
        run["profile"],
        run["split"],
        tuple(modes),
        cases_per_mode,
    )
    expected_case_records = [
        {
            "case_id": case["case_id"],
            "mode": case["mode"],
            "arguments_sha256": sha256_json(case["arguments"]),
            "oracle_sha256": sha256_json(case["oracle"]),
        }
        for case in expected_cases
    ]
    if run.get("fixture_sha256") != recipe["fixture_sha256"] or cases != expected_case_records:
        raise NativeBurstFailure("native burst fixture or case identity differs")

    tool_surface = document["tool_surface"]
    if not isinstance(tool_surface, dict) or set(tool_surface) != {
        "required_tool",
        "forbidden_internal_tools",
        "observed",
        "search_mcp_events_exposed",
        "consistent",
        "pass",
    }:
        raise NativeBurstFailure("native burst tool surface evidence differs")
    observed_surfaces = tool_surface.get("observed")
    if (
        tool_surface.get("required_tool") != TOOL_NAME
        or tool_surface.get("forbidden_internal_tools")
        != list(FORBIDDEN_INTERNAL_TOOLS)
        or not isinstance(observed_surfaces, list)
        or len(observed_surfaces) != 1
        or not isinstance(observed_surfaces[0], list)
    ):
        raise NativeBurstFailure("native burst tool surface metadata differs")
    canonical_surface = _validated_tool_surface(observed_surfaces[0])
    if (
        observed_surfaces[0] != list(canonical_surface)
        or tool_surface.get("search_mcp_events_exposed") is not False
        or tool_surface.get("consistent") is not True
        or tool_surface.get("pass") is not True
    ):
        raise NativeBurstFailure("native burst tool surface evidence differs")

    input_identity = document["input_identity"]
    digest_pattern = re.compile(r"^sha256:[0-9a-f]{64}$")
    if not isinstance(input_identity, dict) or set(input_identity) != {
        "mcp_binary", "config", "frozen_execution", "pass",
    }:
        raise NativeBurstFailure("native burst input identity fields differ")
    for name, run_field in (
        ("mcp_binary", "mcp_binary_sha256"),
        ("config", "config_sha256"),
    ):
        identity = input_identity.get(name)
        if (
            not isinstance(identity, dict)
            or set(identity) != {"pre_sha256", "frozen_sha256", "post_sha256"}
            or any(
                not isinstance(value, str) or not digest_pattern.fullmatch(value)
                for value in identity.values()
            )
            or len(set(identity.values())) != 1
            or identity["pre_sha256"] != run.get(run_field)
        ):
            raise NativeBurstFailure("native burst input identity is unstable")
    if input_identity.get("frozen_execution") is not True or input_identity.get("pass") is not True:
        raise NativeBurstFailure("native burst frozen input evidence differs")

    fixture_database = document["fixture_database"]
    if (
        not isinstance(fixture_database, dict)
        or set(fixture_database) != {"before", "after", "stable", "pass"}
        or fixture_database.get("stable") is not True
        or fixture_database.get("pass") is not True
        or fixture_database.get("before") != fixture_database.get("after")
        or not _validate_fixture_database_evidence(fixture_database.get("before"), recipe)
        or not _validate_fixture_database_evidence(fixture_database.get("after"), recipe)
    ):
        raise NativeBurstFailure("native burst fixture database evidence differs")

    samples = document["samples"]
    bursts = document["bursts"]
    case_modes = {case["case_id"]: case["mode"] for case in cases}
    steady_bursts_per_concurrency = len(cases) * bursts_per_case
    bursts_per_concurrency = steady_bursts_per_concurrency + cold_repetitions
    if (
        not isinstance(samples, list)
        or len(samples) != bursts_per_concurrency * sum(CONCURRENCIES)
        or not isinstance(bursts, list)
        or len(bursts) != bursts_per_concurrency * len(CONCURRENCIES)
    ):
        raise NativeBurstFailure("native burst raw matrix is incomplete")
    for sample in samples:
        started = sample.get("started_ns") if isinstance(sample, dict) else None
        completed = sample.get("completed_ns") if isinstance(sample, dict) else None
        released = sample.get("released_ns") if isinstance(sample, dict) else None
        elapsed = sample.get("elapsed_ms") if isinstance(sample, dict) else None
        if (
            isinstance(started, bool) or not isinstance(started, int)
            or isinstance(completed, bool) or not isinstance(completed, int)
            or isinstance(released, bool) or not isinstance(released, int)
            or not released <= started <= completed
            or not isinstance(elapsed, (int, float)) or isinstance(elapsed, bool)
            or not math.isfinite(float(elapsed))
            or abs(float(elapsed) - (completed - started) / 1_000_000.0) > 1e-6
            or sample.get("concurrency") not in CONCURRENCIES
            or sample.get("phase") not in {COLD_PHASE, STEADY_PHASE}
            or isinstance(sample.get("lifecycle_iteration"), bool)
            or not isinstance(sample.get("lifecycle_iteration"), int)
            or sample.get("lifecycle_iteration") < 0
            or sample.get("case_id") not in case_modes
            or sample.get("mode") != case_modes.get(sample.get("case_id"))
            or sample.get("outcome")
            not in {
                "ok",
                "timeout",
                "admission_rejected",
                "tool_error",
                "malformed_result",
                "protocol_error",
                "semantic_mismatch",
                "scenario_error",
                "unexpected_error",
            }
        ):
            raise NativeBurstFailure("native burst sample timing is invalid")
    if any(
        sum(sample["concurrency"] == concurrency for sample in samples)
        != bursts_per_concurrency * concurrency
        for concurrency in CONCURRENCIES
    ):
        raise NativeBurstFailure("native burst raw sample matrix is incomplete")
    for concurrency in CONCURRENCIES:
        cold_count = sum(
            sample["phase"] == COLD_PHASE and sample["concurrency"] == concurrency
            for sample in samples
        )
        steady_count = sum(
            sample["phase"] == STEADY_PHASE and sample["concurrency"] == concurrency
            for sample in samples
        )
        if (
            cold_count != cold_repetitions * concurrency
            or steady_count != steady_bursts_per_concurrency * concurrency
        ):
            raise NativeBurstFailure("native burst phase matrix is incomplete")

    samples_by_burst: dict[str, list[Mapping[str, Any]]] = {}
    for sample in samples:
        burst_id = sample.get("burst_id")
        if not isinstance(burst_id, str) or not burst_id:
            raise NativeBurstFailure("native burst sample identity is invalid")
        samples_by_burst.setdefault(burst_id, []).append(sample)
    if len(samples_by_burst) != len(bursts):
        raise NativeBurstFailure("native burst identities are duplicated or missing")
    cold_burst_keys: set[tuple[int, int]] = set()
    steady_case_counts: dict[tuple[int, str], int] = {}
    for item in bursts:
        if not isinstance(item, dict):
            raise NativeBurstFailure("native burst record is not an object")
        burst_id = item.get("burst_id")
        concurrency = item.get("concurrency")
        phase = item.get("phase")
        iteration = item.get("lifecycle_iteration")
        released = item.get("released_ns")
        completed = item.get("completed_ns")
        wall_elapsed = item.get("wall_elapsed_ms")
        case_id = item.get("case_id")
        grouped = samples_by_burst.get(burst_id, [])
        if (
            not isinstance(burst_id, str)
            or concurrency not in CONCURRENCIES
            or phase not in {COLD_PHASE, STEADY_PHASE}
            or isinstance(iteration, bool)
            or not isinstance(iteration, int)
            or iteration < 0
            or isinstance(released, bool)
            or not isinstance(released, int)
            or isinstance(completed, bool)
            or not isinstance(completed, int)
            or completed < released
            or not isinstance(wall_elapsed, (int, float))
            or isinstance(wall_elapsed, bool)
            or not math.isfinite(float(wall_elapsed))
            or abs(float(wall_elapsed) - (completed - released) / 1_000_000.0) > 1e-6
            or case_id not in case_modes
            or item.get("mode") != case_modes.get(case_id)
            or len(grouped) != concurrency
            or any(
                sample["phase"] != phase
                or sample["lifecycle_iteration"] != iteration
                or sample["concurrency"] != concurrency
                or sample["case_id"] != case_id
                or sample["released_ns"] != released
                for sample in grouped
            )
            or max(sample["completed_ns"] for sample in grouped) != completed
            or item.get("request_count") != concurrency
            or item.get("ok_count")
            != sum(sample["outcome"] == "ok" for sample in grouped)
            or item.get("timeout_count")
            != sum(sample["outcome"] == "timeout" for sample in grouped)
            or item.get("error_count")
            != sum(sample["outcome"] not in {"ok", "timeout"} for sample in grouped)
        ):
            raise NativeBurstFailure("native burst record disagrees with raw samples")
        if phase == COLD_PHASE:
            if (
                iteration >= cold_repetitions
                or case_id != cases[iteration % len(cases)]["case_id"]
            ):
                raise NativeBurstFailure("native cold case rotation differs")
            cold_burst_keys.add((concurrency, iteration))
        else:
            if iteration != 0:
                raise NativeBurstFailure("native steady lifecycle iteration differs")
            steady_case = (concurrency, case_id)
            steady_case_counts[steady_case] = steady_case_counts.get(steady_case, 0) + 1
    expected_cold_burst_keys = {
        (concurrency, iteration)
        for concurrency in CONCURRENCIES
        for iteration in range(cold_repetitions)
    }
    expected_steady_case_counts = {
        (concurrency, case_id): bursts_per_case
        for concurrency in CONCURRENCIES
        for case_id in case_modes
    }
    if (
        cold_burst_keys != expected_cold_burst_keys
        or steady_case_counts != expected_steady_case_counts
    ):
        raise NativeBurstFailure("native burst case schedule is incomplete")
    expected_cold_order = [
        (iteration, concurrency)
        for iteration in range(cold_repetitions)
        for concurrency in (
            CONCURRENCIES if iteration % 2 == 0 else tuple(reversed(CONCURRENCIES))
        )
    ]
    observed_cold_order = [
        (item["lifecycle_iteration"], item["concurrency"])
        for item in bursts
        if item["phase"] == COLD_PHASE
    ]
    if observed_cold_order != expected_cold_order:
        raise NativeBurstFailure("native cold lifecycle order is not counterbalanced")

    lifecycles = document["lifecycles"]
    lifecycle_fields = {
        "phase",
        "concurrency",
        "lifecycle_iteration",
        "central_terminated",
        "routes_closed",
        "socket_removed",
    }
    if (
        not isinstance(lifecycles, list)
        or len(lifecycles) != (cold_repetitions + 1) * len(CONCURRENCIES)
        or any(not isinstance(item, dict) or set(item) != lifecycle_fields for item in lifecycles)
        or any(
            item.get(field) is not True
            for item in lifecycles
            for field in ("central_terminated", "routes_closed", "socket_removed")
        )
        or any(
            item.get("phase") not in {COLD_PHASE, STEADY_PHASE}
            or item.get("concurrency") not in CONCURRENCIES
            for item in lifecycles
        )
    ):
        raise NativeBurstFailure("native burst cleanup evidence is incomplete")
    expected_lifecycles = {
        (COLD_PHASE, concurrency, iteration)
        for concurrency in CONCURRENCIES
        for iteration in range(cold_repetitions)
    } | {(STEADY_PHASE, concurrency, 0) for concurrency in CONCURRENCIES}
    observed_lifecycles = {
        (item.get("phase"), item.get("concurrency"), item.get("lifecycle_iteration"))
        for item in lifecycles
    }
    if observed_lifecycles != expected_lifecycles:
        raise NativeBurstFailure("native burst lifecycle matrix is incomplete")
    summary = document["summary"]
    if summary != _summary(samples, bursts):
        raise NativeBurstFailure("native burst summary does not cover raw samples")
    expected_gates = _latency_gates(
        summary,
        min_cold_samples=min_cold_samples,
        warm_p95_limit_ms=float(warm_p95_limit_ms),
        cold_p95_limit_ms=float(cold_p95_limit_ms),
        max_latency_ms=float(max_latency_ms),
    )
    if document["gates"] != expected_gates:
        raise NativeBurstFailure("native burst latency gates disagree with raw samples")
    query_log = document["query_log"]
    query_log_pass = _validate_query_log_evidence(
        query_log,
        requested=run["collect_query_log"],
        lifecycles=lifecycles,
        steady_warmup_case_count=len(cases),
        steady_measured_requests_per_client=len(cases) * bursts_per_case,
    )
    expected_status = (
        "pass"
        if summary.get("ok_count") == len(samples)
        and expected_gates["pass"]
        and query_log_pass
        else "fail"
    )
    if document["status"] != expected_status:
        raise NativeBurstFailure("native burst status disagrees with raw outcomes")
    return document


def write_native_burst_artifact(path: Path, document: Mapping[str, Any]) -> None:
    write_json_atomic(path, document, validator=validate_native_burst_artifact)
