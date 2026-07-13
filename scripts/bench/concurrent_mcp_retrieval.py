#!/usr/bin/env python3
"""Concurrent, uncached search_sessions benchmark through the production MCP boundary.

Each configured concurrency emits a separate moraine-benchmark-v1 artifact. Timing is
always diagnostic. The runner owns a fresh central MCP service and client set for every
independent repetition; it never reads the server's admission configuration.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import shutil
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Optional, Sequence

import benchmark_protocol
from central_mcp_resource import (
    BenchmarkFailure,
    McpClient,
    _stop_server,
    infer_build_profile,
    raise_fd_limit,
    resolve_moraine_mcp,
    resolve_source,
    spawn_client,
    wait_for_socket,
    write_config,
)

BENCHMARK_ID = "concurrent-mcp-retrieval"
ORACLE_SCHEMA_VERSION = "moraine-concurrent-mcp-oracle-v1"
MEASURED_BOUNDARY = "stdio-tools-call-write-to-response"
REQUEST_SOURCE = "central-mcp-jsonrpc-stdio"
HARD_DEADLINE_MS = 5_000.0
PROFILE_RECOVERY_OFFSETS = {
    "smoke": (0.0, 1.0),
    "full": (0.0, 10.0),
}
OUTCOME_NAMES = (
    "success",
    "admission_rejection",
    "mcp_error",
    "jsonrpc_error",
    "timeout",
    "deadline_exceeded",
    "semantic_error",
    "client_error",
)


class ConfigurationError(ValueError):
    pass


@dataclass(frozen=True)
class OracleCase:
    query_id: str
    query: str
    result_count: int
    result_digest: str


@dataclass(frozen=True)
class OracleManifest:
    provenance: str
    dataset_fingerprint: str
    dataset_cardinality: int
    warmup: OracleCase
    measured: tuple[OracleCase, ...]
    recovery: tuple[OracleCase, ...]
    query_set_fingerprint: str


@dataclass
class RequestSample:
    query_id: str
    outcome: str
    wall_ms: float
    started_ns: int
    finished_ns: int
    server_elapsed_ms: Optional[float] = None
    sla_target_ms: Optional[float] = None
    met_sla: Optional[bool] = None
    exceeded_hard_deadline: bool = False
    oracle_passed: bool = False


@dataclass
class BurstResult:
    requested_concurrency: int
    samples: list[RequestSample]
    wall_ms: float
    max_in_flight: int
    overlap_proven: bool


@dataclass
class RepetitionResult:
    burst: BurstResult
    recovery: list[RequestSample]
    cleanup_codes: list[str] = field(default_factory=list)


@dataclass
class ScenarioResult:
    concurrency: int
    repetitions: list[RepetitionResult]

    @property
    def burst_samples(self) -> list[RequestSample]:
        return [sample for repetition in self.repetitions for sample in repetition.burst.samples]

    @property
    def successful(self) -> list[RequestSample]:
        return [sample for sample in self.burst_samples if sample.outcome == "success"]

    @property
    def recovery_samples(self) -> list[RequestSample]:
        return [sample for repetition in self.repetitions for sample in repetition.recovery]


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive number") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive number")
    return parsed




def parse_concurrency(values: Sequence[str]) -> list[int]:
    """Parse repeatable integers, comma lists, or inclusive START:STOP[:STEP] ranges."""
    parsed: list[int] = []
    for value in values:
        for token in value.split(","):
            token = token.strip()
            if not token:
                raise ConfigurationError("empty concurrency value")
            if ":" not in token:
                try:
                    number = int(token)
                except ValueError as exc:
                    raise ConfigurationError(f"invalid concurrency value: {token}") from exc
                if number <= 0:
                    raise ConfigurationError("concurrency values must be positive")
                parsed.append(number)
                continue
            parts = token.split(":")
            if len(parts) not in (2, 3):
                raise ConfigurationError(f"invalid concurrency range: {token}")
            try:
                start, stop = int(parts[0]), int(parts[1])
                step = int(parts[2]) if len(parts) == 3 else 1
            except ValueError as exc:
                raise ConfigurationError(f"invalid concurrency range: {token}") from exc
            if min(start, stop, step) <= 0 or stop < start:
                raise ConfigurationError("concurrency ranges require 0 < start <= stop and step > 0")
            parsed.extend(range(start, stop + 1, step))
    if not parsed:
        raise ConfigurationError("at least one concurrency value is required")
    return list(dict.fromkeys(parsed))


def sha256_json(value: Any, *, prefixed: bool = True) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    return f"sha256:{digest}" if prefixed else digest


def _validate_sha256(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.startswith("sha256:"):
        raise ConfigurationError(f"{field} must be sha256:<64 lowercase hex>")
    digest = value[7:]
    if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
        raise ConfigurationError(f"{field} must be sha256:<64 lowercase hex>")
    return value


def _load_case(value: Any, field: str) -> OracleCase:
    if not isinstance(value, dict) or set(value) != {"id", "query", "result_count", "result_digest"}:
        raise ConfigurationError(f"{field} has an invalid shape")
    query_id, query, count = value["id"], value["query"], value["result_count"]
    if not isinstance(query_id, str) or not query_id or any(ch not in "abcdefghijklmnopqrstuvwxyz0123456789._-" for ch in query_id):
        raise ConfigurationError(f"{field}.id must be a lowercase slug")
    if not isinstance(query, str) or not query.strip():
        raise ConfigurationError(f"{field}.query must be non-empty")
    if isinstance(count, bool) or not isinstance(count, int) or count <= 0 or count > 10:
        raise ConfigurationError(f"{field}.result_count must be between 1 and 10")
    return OracleCase(query_id, query, count, _validate_sha256(value["result_digest"], f"{field}.result_digest"))


def load_oracle(path: Path, *, profile: str, needed_queries: int, recovery_count: int) -> OracleManifest:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigurationError("unable to read oracle manifest") from exc
    required = {"schema_version", "provenance", "dataset", "warmup", "measured", "recovery"}
    if not isinstance(raw, dict) or set(raw) != required or raw.get("schema_version") != ORACLE_SCHEMA_VERSION:
        raise ConfigurationError("oracle manifest has an invalid shape or schema version")
    provenance = raw["provenance"]
    dataset = raw["dataset"]
    if (
        not isinstance(provenance, str)
        or not provenance
        or len(provenance) > 128
        or any(ch not in "abcdefghijklmnopqrstuvwxyz0123456789._-" for ch in provenance)
    ):
        raise ConfigurationError("oracle provenance must be a lowercase stable slug")
    if not isinstance(dataset, dict) or set(dataset) != {"fingerprint", "cardinality"}:
        raise ConfigurationError("oracle dataset has an invalid shape")
    cardinality = dataset["cardinality"]
    if isinstance(cardinality, bool) or not isinstance(cardinality, int) or cardinality <= 0:
        raise ConfigurationError("oracle dataset cardinality must be positive")
    if profile == "full" and cardinality < 100_000:
        raise ConfigurationError("full profile requires at least 100000 searchable documents")
    warmup = _load_case(raw["warmup"], "warmup")
    if not isinstance(raw["measured"], list) or not isinstance(raw["recovery"], list):
        raise ConfigurationError("oracle measured and recovery entries must be arrays")
    measured = tuple(_load_case(value, f"measured[{index}]") for index, value in enumerate(raw["measured"]))
    recovery = tuple(_load_case(value, f"recovery[{index}]") for index, value in enumerate(raw["recovery"]))
    if len(measured) < needed_queries:
        raise ConfigurationError(f"oracle needs at least {needed_queries} measured queries")
    if len(recovery) < recovery_count:
        raise ConfigurationError(f"oracle needs at least {recovery_count} recovery queries")
    all_cases = (warmup,) + measured + recovery
    normalized = [" ".join(case.query.split()).casefold() for case in all_cases]
    ids = [case.query_id for case in all_cases]
    if len(normalized) != len(set(normalized)) or len(ids) != len(set(ids)):
        raise ConfigurationError("warmup, measured, and recovery queries and ids must be distinct")
    query_contract = [
        {"id": case.query_id, "query_digest": sha256_json(" ".join(case.query.split()).casefold()),
         "result_count": case.result_count, "result_digest": case.result_digest}
        for case in measured[:needed_queries]
    ]
    return OracleManifest(
        provenance=provenance,
        dataset_fingerprint=_validate_sha256(dataset["fingerprint"], "dataset.fingerprint"),
        dataset_cardinality=cardinality,
        warmup=warmup,
        measured=measured,
        recovery=recovery,
        query_set_fingerprint=sha256_json(query_contract),
    )


def result_digest(results: Sequence[Any]) -> str:
    identities: list[dict[str, str]] = []
    for hit in results:
        if not isinstance(hit, dict):
            raise BenchmarkFailure("semantic-oracle-failed")
        event, session = hit.get("event"), hit.get("session")
        if not isinstance(event, dict) or not isinstance(session, dict):
            raise BenchmarkFailure("semantic-oracle-failed")
        event_id, session_id = event.get("id"), session.get("id")
        if not isinstance(event_id, str) or not isinstance(session_id, str):
            raise BenchmarkFailure("semantic-oracle-failed")
        identities.append({"event_id": event_id, "session_id": session_id})
    identities.sort(key=lambda item: (item["session_id"], item["event_id"]))
    return sha256_json(identities)


def parse_performance(structured: Any) -> tuple[Optional[float], Optional[float], Optional[bool]]:
    if not isinstance(structured, dict) or "performance" not in structured:
        return None, None, None
    performance = structured["performance"]
    if not isinstance(performance, dict):
        raise BenchmarkFailure("performance-contract-invalid")
    elapsed, target, met = (
        performance.get("elapsed_ms"),
        performance.get("sla_target_ms"),
        performance.get("met_sla"),
    )
    if isinstance(elapsed, bool) or not isinstance(elapsed, (int, float)) or elapsed < 0:
        raise BenchmarkFailure("performance-contract-invalid")
    if (
        isinstance(target, bool)
        or not isinstance(target, (int, float))
        or target <= 0
        or type(met) is not bool
    ):
        raise BenchmarkFailure("performance-contract-invalid")
    if met != (elapsed <= target):
        raise BenchmarkFailure("performance-contract-invalid")
    return float(elapsed), float(target), met


def validate_success(structured: Any, oracle: OracleCase) -> tuple[float, float, bool]:
    if not isinstance(structured, dict) or structured.get("schema_version") != "moraine.mcp.search_sessions.v1":
        raise BenchmarkFailure("semantic-oracle-failed")
    if structured.get("tool") != "search_sessions" or "error" in structured:
        raise BenchmarkFailure("semantic-oracle-failed")
    data = structured.get("data")
    if not isinstance(data, dict):
        raise BenchmarkFailure("semantic-oracle-failed")
    results = data.get("results")
    if not isinstance(results, list) or data.get("result_count") != len(results) or len(results) != oracle.result_count:
        raise BenchmarkFailure("semantic-oracle-failed")
    if result_digest(results) != oracle.result_digest:
        raise BenchmarkFailure("semantic-oracle-failed")
    elapsed, target, met = parse_performance(structured)
    if elapsed is None or target is None or met is None:
        raise BenchmarkFailure("performance-contract-invalid")
    return elapsed, target, met


def initialize_client(client: McpClient, request_id: int) -> None:
    client._rpc_result(request_id, "initialize", {})
    tools = client._rpc_result(request_id + 1, "tools/list", {})
    listed = tools.get("tools")
    if not isinstance(listed, list) or not any(isinstance(tool, dict) and tool.get("name") == "search_sessions" for tool in listed):
        raise BenchmarkFailure("semantic-oracle-failed")


def send_with_gate(
    client: McpClient, payload: dict[str, Any], sent_gate: threading.Barrier
) -> dict[str, Any]:
    if client.proc.stdin is None or client.proc.stdout is None:
        raise BenchmarkFailure("client-pipe-unavailable")
    if client.proc.poll() is not None:
        raise BenchmarkFailure("client-exited")
    try:
        client.proc.stdin.write(json.dumps(payload, separators=(",", ":")) + "\n")
        client.proc.stdin.flush()
    except (BrokenPipeError, OSError) as exc:
        sent_gate.abort()
        raise BenchmarkFailure("client-exited") from exc
    sent_gate.wait()
    response_line = client._read_response_line(time.monotonic() + client.request_timeout_s)
    try:
        response = json.loads(response_line)
    except (TypeError, json.JSONDecodeError) as exc:
        raise BenchmarkFailure("invalid-jsonrpc-response") from exc
    if not isinstance(response, dict):
        raise BenchmarkFailure("invalid-jsonrpc-response")
    return response


def invoke_search(
    client: McpClient,
    request_id: int,
    oracle: OracleCase,
    hard_deadline_ms: float,
    sent_gate: Optional[threading.Barrier] = None,
) -> RequestSample:
    started_ns = time.perf_counter_ns()
    outcome = "client_error"
    server_elapsed = target = None
    met_sla = None
    oracle_passed = False
    try:
        payload = {
            "jsonrpc": "2.0", "id": request_id, "method": "tools/call",
            "params": {"name": "search_sessions", "arguments": {"query": oracle.query, "n_hits": 10}},
        }
        response = (
            send_with_gate(client, payload, sent_gate)
            if sent_gate is not None
            else client._send(payload)
        )
        rpc_error = response.get("error")
        if rpc_error is not None:
            message = rpc_error.get("message", "") if isinstance(rpc_error, dict) else ""
            outcome = "admission_rejection" if "concurrent request limit" in str(message).casefold() else "jsonrpc_error"
        elif response.get("id") != request_id or not isinstance(response.get("result"), dict):
            outcome = "jsonrpc_error"
        else:
            result = response["result"]
            structured = result.get("structuredContent")
            error = structured.get("error") if isinstance(structured, dict) else None
            server_elapsed, target, met_sla = parse_performance(structured)
            if isinstance(error, dict) and error.get("code") == "deadline_exceeded":
                outcome = "deadline_exceeded"
            elif bool(result.get("isError")) or isinstance(error, dict):
                outcome = "mcp_error"
            else:
                server_elapsed, target, met_sla = validate_success(structured, oracle)
                outcome = "success"
                oracle_passed = True
    except BenchmarkFailure as exc:
        if exc.codes[0] == "request-timeout":
            outcome = "timeout"
        elif exc.codes[0] in ("semantic-oracle-failed", "performance-contract-invalid"):
            outcome = "semantic_error"
        else:
            outcome = "client_error"
    finished_ns = time.perf_counter_ns()
    wall_ms = (finished_ns - started_ns) / 1_000_000.0
    return RequestSample(
        query_id=oracle.query_id, outcome=outcome, wall_ms=wall_ms,
        started_ns=started_ns, finished_ns=finished_ns,
        server_elapsed_ms=server_elapsed, sla_target_ms=target, met_sla=met_sla,
        exceeded_hard_deadline=outcome == "success" and wall_ms > hard_deadline_ms,
        oracle_passed=oracle_passed,
    )


def run_burst(
    clients: Sequence[McpClient], cases: Sequence[OracleCase], *, hard_deadline_ms: float,
    invoke: Callable[[McpClient, int, OracleCase, float], RequestSample] = invoke_search,
) -> BurstResult:
    concurrency = len(clients)
    if concurrency <= 0 or len(cases) != concurrency:
        raise BenchmarkFailure("invalid-burst-shape")
    ready = threading.Barrier(concurrency + 1)
    sent = threading.Barrier(concurrency)
    lock = threading.Lock()
    active = 0
    max_active = 0
    samples: list[Optional[RequestSample]] = [None] * concurrency
    child_errors: list[BaseException] = []

    def worker(index: int) -> None:
        nonlocal active, max_active
        try:
            ready.wait()
            with lock:
                active += 1
                max_active = max(max_active, active)
            if invoke is invoke_search:
                samples[index] = invoke_search(
                    clients[index], 1000 + index, cases[index], hard_deadline_ms, sent
                )
            else:
                samples[index] = invoke(
                    clients[index], 1000 + index, cases[index], hard_deadline_ms
                )
        except BaseException as exc:
            with lock:
                child_errors.append(exc)
            try:
                sent.abort()
            except threading.BrokenBarrierError:
                pass
        finally:
            with lock:
                if active:
                    active -= 1

    threads = [threading.Thread(target=worker, args=(index,), daemon=True) for index in range(concurrency)]
    for thread in threads:
        thread.start()
    ready.wait()
    burst_started_ns = time.perf_counter_ns()
    for thread in threads:
        thread.join()
    burst_finished_ns = time.perf_counter_ns()
    if child_errors or any(sample is None for sample in samples):
        raise BenchmarkFailure("burst-child-failed") from (child_errors[0] if child_errors else None)
    complete = [sample for sample in samples if sample is not None]
    latest_start = max(sample.started_ns for sample in complete)
    earliest_finish = min(sample.finished_ns for sample in complete)
    overlap = latest_start <= earliest_finish
    if max_active != concurrency or not overlap:
        raise BenchmarkFailure("burst-overlap-not-proven")
    return BurstResult(concurrency, complete, (burst_finished_ns - burst_started_ns) / 1_000_000.0, max_active, overlap)


def _start_server(moraine_mcp: str, config: Path, root_dir: Path) -> subprocess.Popen:
    static_dir = root_dir / "monitor-static"
    static_dir.mkdir(parents=True)
    (static_dir / "index.html").write_text("<!doctype html><title>Moraine benchmark</title>", encoding="utf-8")
    try:
        return subprocess.Popen(
            [moraine_mcp, "--config", str(config), "--serve", "socket", "--host", "127.0.0.1", "--port", "0", "--static-dir", str(static_dir)],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    except OSError as exc:
        raise BenchmarkFailure("central-startup-failed") from exc


def run_repetition(
    concurrency: int, *, moraine_mcp: str, clickhouse_url: str, database: str,
    manifest: OracleManifest, recovery_count: int, recovery_offsets_s: Sequence[float],
    startup_timeout_s: float, request_timeout_s: float, hard_deadline_ms: float,
    run_deadline: Optional[float] = None,
) -> RepetitionResult:
    short_root = "/tmp" if Path("/tmp").is_dir() and os.access("/tmp", os.W_OK) else None
    workdir = Path(tempfile.mkdtemp(prefix=f"mb-concurrent-{concurrency}-", dir=short_root))
    clients: list[McpClient] = []
    server: Optional[subprocess.Popen] = None
    cleanup_codes: list[str] = []
    primary: Optional[BaseException] = None
    result: Optional[RepetitionResult] = None
    def remaining_timeout(limit: float) -> float:
        if run_deadline is None:
            return limit
        remaining = run_deadline - time.monotonic()
        if remaining <= 0:
            raise BenchmarkFailure("run-timeout")
        return min(limit, remaining)

    try:
        root_dir = workdir / "root"
        (root_dir / "run").mkdir(parents=True)
        socket_path = root_dir / "run" / "mcp.sock"
        config = workdir / "config.toml"
        write_config(config, clickhouse_url=clickhouse_url, database=database, root_dir=root_dir,
                     use_central_server=True, central_socket_path=socket_path)
        server = _start_server(moraine_mcp, config, root_dir)
        wait_for_socket(socket_path, server, remaining_timeout(startup_timeout_s))
        warmup_client = spawn_client(moraine_mcp, config, remaining_timeout(request_timeout_s))
        clients.append(warmup_client)
        warmup_client.request_timeout_s = remaining_timeout(request_timeout_s)
        initialize_client(warmup_client, 1)
        warmup = invoke_search(warmup_client, 3, manifest.warmup, hard_deadline_ms)
        if warmup.outcome != "success":
            raise BenchmarkFailure("warmup-failed")
        measured_clients: list[McpClient] = []
        for _ in range(concurrency):
            client = spawn_client(moraine_mcp, config, remaining_timeout(request_timeout_s))
            measured_clients.append(client)
            clients.append(client)
        for index, client in enumerate(measured_clients):
            client.request_timeout_s = remaining_timeout(request_timeout_s)
            initialize_client(client, 10 + index * 2)
        for client in measured_clients:
            client.request_timeout_s = remaining_timeout(request_timeout_s)
        burst = run_burst(measured_clients, manifest.measured[:concurrency], hard_deadline_ms=hard_deadline_ms)
        recovery: list[RequestSample] = []
        recovery_anchor = time.monotonic()
        for index in range(recovery_count):
            offset = recovery_offsets_s[index]
            target_time = recovery_anchor + offset
            if run_deadline is not None and target_time >= run_deadline:
                raise BenchmarkFailure("run-timeout")
            time.sleep(max(0.0, target_time - time.monotonic()))
            warmup_client.request_timeout_s = remaining_timeout(request_timeout_s)
            recovery.append(invoke_search(warmup_client, 20_000 + index, manifest.recovery[index], hard_deadline_ms))
        result = RepetitionResult(burst, recovery)
    except Exception as exc:
        primary = exc
    finally:
        for client in clients:
            cleanup_codes.extend(client.close())
        if server is not None:
            cleanup_codes.extend(_stop_server(server))
        try:
            shutil.rmtree(workdir)
        except OSError:
            cleanup_codes.append("workdir-cleanup-failed")
    cleanup_codes = list(dict.fromkeys(cleanup_codes))
    if primary is not None:
        if isinstance(primary, BenchmarkFailure):
            raise primary.add_codes(cleanup_codes)
        raise BenchmarkFailure("repetition-failed", *cleanup_codes) from primary
    if cleanup_codes:
        raise BenchmarkFailure(*cleanup_codes)
    if result is None:
        raise BenchmarkFailure("repetition-failed")
    return result


def percentile(values: Sequence[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    low, high = math.floor(position), math.ceil(position)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def runner_identity() -> dict[str, str]:
    os_name = platform.system().lower() or "unknown"
    machine = platform.machine().lower() or "unknown"
    cpu = "apple-silicon" if os_name == "darwin" and machine == "arm64" else machine
    return {"os": os_name.replace("_", "-"), "cpu_class": cpu.replace("_", "-")}


def build_target() -> str:
    return f"{(platform.machine().lower() or 'unknown').replace('_', '-')}-{(platform.system().lower() or 'unknown').replace('_', '-')}"


def request_record(sample: RequestSample, repetition: int, phase: str) -> dict[str, Any]:
    record: dict[str, Any] = {
        "case_id": sample.query_id,
        "repetition": repetition,
        "phase": phase,
        "outcome": sample.outcome,
        "wall_latency_ms": sample.wall_ms,
        "hard_deadline_violation": sample.exceeded_hard_deadline,
        "oracle_passed": sample.oracle_passed,
    }
    if sample.server_elapsed_ms is not None:
        record["server_elapsed_ms"] = sample.server_elapsed_ms
    if sample.sla_target_ms is not None:
        record["sla_target_ms"] = sample.sla_target_ms
    if sample.met_sla is not None:
        record["met_sla"] = sample.met_sla
    return record


def build_artifact(
    result: ScenarioResult, *, manifest: OracleManifest, profile: str, source: dict[str, Any],
    build_profile: str, recovery_offsets_s: Sequence[float], request_timeout_s: float,
    run_timeout_s: float, hard_deadline_ms: float,
) -> dict[str, Any]:
    samples = result.burst_samples
    successes = result.successful
    attempted = len(samples)
    outcomes = {name: sum(sample.outcome == name for sample in samples) for name in OUTCOME_NAMES}
    errors = attempted - len(successes)
    wall = [sample.wall_ms for sample in successes]
    server = [sample.server_elapsed_ms for sample in successes]
    targets = [sample.sla_target_ms for sample in successes]
    met = [1.0 if sample.met_sla else 0.0 for sample in successes]
    hard = [1.0 if sample.exceeded_hard_deadline else 0.0 for sample in successes]
    burst_wall = [repetition.burst.wall_ms for repetition in result.repetitions]
    throughput = [
        (sum(sample.outcome == "success" for sample in repetition.burst.samples) / (repetition.burst.wall_ms / 1000.0))
        if repetition.burst.wall_ms > 0 else 0.0 for repetition in result.repetitions
    ]
    recovery = result.recovery_samples
    recovery_wall = [sample.wall_ms for sample in recovery]
    recovery_success = [1.0 if sample.outcome == "success" else 0.0 for sample in recovery]
    max_in_flight = max(repetition.burst.max_in_flight for repetition in result.repetitions)
    records = [
        request_record(sample, repetition_index, phase)
        for repetition_index, repetition in enumerate(result.repetitions)
        for phase, phase_samples in (
            ("burst", repetition.burst.samples),
            ("recovery", repetition.recovery),
        )
        for sample in phase_samples
    ]
    query_contract = [
        {
            "id": case.query_id,
            "query_digest": sha256_json(" ".join(case.query.split()).casefold()),
            "result_count": case.result_count,
            "result_digest": case.result_digest,
        }
        for case in manifest.measured[: result.concurrency]
    ]
    all_probes = samples + recovery
    semantic_failures = sum(sample.outcome == "semantic_error" for sample in all_probes)
    semantic_pass = semantic_failures == 0
    scenario_id = f"central-persistent-cold-concurrent-n{result.concurrency}-{profile}"
    artifact: dict[str, Any] = {
        "schema_version": benchmark_protocol.SCHEMA_VERSION,
        "benchmark_id": BENCHMARK_ID,
        "scenario_id": scenario_id,
        "source": source,
        "build": {"profile": build_profile, "target": build_target()},
        "runner": runner_identity(),
        "scenario": {
            "profile": profile,
            "workload_id": f"search-sessions-distinct-uncached-n{result.concurrency}",
            "measured_boundary": MEASURED_BOUNDARY,
            "dimensions": {"dataset_backed": True, "cache_sensitive": True, "concurrent": True, "request_producing": True},
            "fingerprints": {
                "dataset": {"fingerprint": manifest.dataset_fingerprint, "cardinality": manifest.dataset_cardinality},
                "cache_state": "cold", "concurrency": result.concurrency, "request_source": REQUEST_SOURCE,
                "request_schedule": "concurrent", "max_in_flight": max_in_flight,
                "query_set": {"fingerprint": sha256_json(query_contract), "cardinality": result.concurrency},
                "cache_isolation": "fresh-central-service-per-repetition", "lifecycle": "persistent",
                "boundary": "central", "oracle_provenance": manifest.provenance,
                "request_timeout_seconds": request_timeout_s,
            },
        },
        "samples": {
            "planned": len(result.repetitions) * result.concurrency,
            "attempted": attempted, "successful": len(successes), "errors": errors,
            "measurements": {
                "wall_latency_ms": wall,
                "server_elapsed_ms": [float(value) for value in server if value is not None],
                "sla_target_ms": [float(value) for value in targets if value is not None],
                "met_sla_ratio": met, "hard_deadline_violation_ratio": hard,
            },
            "outcomes": outcomes,
            "observations": {
                "burst_wall_ms": burst_wall, "successful_throughput_per_second": throughput,
                "recovery_wall_latency_ms": recovery_wall,
                "recovery_success_ratio": recovery_success,
                "recovery_offset_seconds": list(recovery_offsets_s) * len(result.repetitions),
            },
            "records": records,
        },
        "semantic": {"status": "pass" if semantic_pass else "fail"},
        "timing": {"status": "not_evaluated", "non_blocking": True},
        "metrics": {
            "quality": {
                "oracle_status": "pass" if semantic_pass else "fail",
                "passed_checks": sum(sample.oracle_passed for sample in all_probes),
                "failed_checks": semantic_failures,
                "expected_count": len(all_probes),
                "observed_count": sum(sample.oracle_passed for sample in all_probes),
                "success_rate": len(successes) / attempted if attempted else 0.0,
                "error_rate": errors / attempted if attempted else 0.0,
            },
            "resources": {
                "requested_concurrency_count": result.concurrency,
                "maximum_in_flight_count": max_in_flight,
                "overlap_proven_ratio": (
                    sum(repetition.burst.overlap_proven for repetition in result.repetitions)
                    / len(result.repetitions)
                ),
                "sla_met_rate_ratio": sum(met) / len(met) if met else 0.0,
                "hard_deadline_violation_count": sum(hard),
                "admission_rejection_count": outcomes["admission_rejection"],
                "admission_rejection_rate_ratio": outcomes["admission_rejection"] / attempted if attempted else 0.0,
                "error_rate_ratio": errors / attempted if attempted else 0.0,
                "deadline_exceeded_count": outcomes["deadline_exceeded"],
                "timeout_count": outcomes["timeout"],
                "p50_latency_ms": percentile(wall, 0.50), "p95_latency_ms": percentile(wall, 0.95),
                "p99_latency_ms": percentile(wall, 0.99), "max_latency_ms": max(wall, default=0.0),
                "mean_throughput_per_second": sum(throughput) / len(throughput) if throughput else 0.0,
                "request_timeout_seconds": request_timeout_s, "run_timeout_seconds": run_timeout_s,
                "hard_deadline_ms": hard_deadline_ms,
            },
        },
        "diagnostics": [], "artifacts": [],
    }
    benchmark_protocol.validate_artifact(artifact)
    return artifact


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--moraine-mcp")
    parser.add_argument("--clickhouse-url", default="http://127.0.0.1:8123")
    parser.add_argument("--database", default="moraine")
    parser.add_argument("--oracle-json", type=Path, required=True)
    parser.add_argument("--concurrency", action="append", required=True, metavar="N|START:STOP[:STEP]")
    parser.add_argument("--reps", type=positive_int, default=1)
    parser.add_argument("--profile", choices=("smoke", "full"), default="smoke")
    parser.add_argument("--startup-timeout-seconds", type=positive_float, default=10.0)
    parser.add_argument("--request-timeout-seconds", type=positive_float, default=20.0)
    parser.add_argument("--run-timeout-seconds", type=positive_float, default=600.0)
    parser.add_argument("--max-processes", type=positive_int, default=256)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--git-commit")
    parser.add_argument("--dirty", choices=("auto", "true", "false"), default="auto")
    args = parser.parse_args(argv)
    try:
        args.concurrency = parse_concurrency(args.concurrency)
    except ConfigurationError as exc:
        parser.error(str(exc))
    args.recovery_offsets = PROFILE_RECOVERY_OFFSETS[args.profile]
    args.recovery_probes = len(args.recovery_offsets)
    if max(args.concurrency) + 2 > args.max_processes:
        parser.error("requested concurrency exceeds the operator-controlled --max-processes safety bound")
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    run_deadline = started + args.run_timeout_seconds
    try:
        manifest = load_oracle(args.oracle_json, profile=args.profile, needed_queries=max(args.concurrency), recovery_count=args.recovery_probes)
        moraine_mcp = resolve_moraine_mcp(args.moraine_mcp)
        repo_root = Path(__file__).resolve().parents[2]
        commit, dirty = resolve_source(repo_root, args.git_commit, args.dirty)
        source = {"git_commit": commit, "dirty": dirty}
        raise_fd_limit(max(8192, args.max_processes * 8))
        args.output_dir.mkdir(parents=True, exist_ok=True)
        semantic_failed = False
        for concurrency in args.concurrency:
            repetitions: list[RepetitionResult] = []
            for _ in range(args.reps):
                if time.monotonic() - started >= args.run_timeout_seconds:
                    raise BenchmarkFailure("run-timeout")
                repetitions.append(run_repetition(
                    concurrency, moraine_mcp=moraine_mcp, clickhouse_url=args.clickhouse_url,
                    database=args.database, manifest=manifest,
                    recovery_count=args.recovery_probes if concurrency == max(args.concurrency) else 0,
                    recovery_offsets_s=args.recovery_offsets if concurrency == max(args.concurrency) else (),
                    startup_timeout_s=args.startup_timeout_seconds,
                    request_timeout_s=args.request_timeout_seconds,
                    hard_deadline_ms=HARD_DEADLINE_MS,
                    run_deadline=run_deadline,
                ))
            result = ScenarioResult(concurrency, repetitions)
            artifact = build_artifact(
                result, manifest=manifest, profile=args.profile, source=source,
                build_profile=infer_build_profile(moraine_mcp),
                recovery_offsets_s=args.recovery_offsets if concurrency == max(args.concurrency) else (),
                request_timeout_s=args.request_timeout_seconds, run_timeout_s=args.run_timeout_seconds,
                hard_deadline_ms=HARD_DEADLINE_MS,
            )
            semantic_failed = semantic_failed or artifact["semantic"]["status"] == "fail"
            path = args.output_dir / f"{BENCHMARK_ID}-n{concurrency}-{args.profile}.json"
            benchmark_protocol.write_artifact(path, artifact)
            print(f"n={concurrency} success={artifact['samples']['successful']}/{artifact['samples']['attempted']} output={path}")
        return 2 if semantic_failed else 0
    except (BenchmarkFailure, ConfigurationError, benchmark_protocol.ProtocolError, OSError) as exc:
        code = exc.codes[0] if isinstance(exc, BenchmarkFailure) else type(exc).__name__.lower()
        print(f"benchmark failed: {code}", file=os.sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
