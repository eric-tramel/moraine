#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "psutil>=5.9,<8",
# ]
# ///
"""Resource benchmark for shared central MCP versus embedded MCP servers.

The benchmark keeps the existing scenario: for every configured client count it
runs embedded and/or central-server arms, performs initialize, tools/list, and a
production search_sessions tools/call for every client, then samples resident
process resources. Each repetition is one paired sample across the complete
configured matrix. Timing is diagnostic and never gates success.

Run inside a dev sandbox with a deterministic seeded ClickHouse dataset:

  python3 /repo/scripts/bench/central_mcp_resource.py \
      --moraine-mcp /home/moraine/target/debug/moraine-mcp \
      --clickhouse-url http://clickhouse:8123 \
      --ns 1,10,50,100 --reps 3 \
      --seed-marker '<known non-secret seeded marker>' \
      --dataset-fingerprint sha256:<64 lowercase hex characters> \
      --dataset-cardinality <seeded session count> \
      --json-out target/bench/central-mcp-resource.json

On macOS the resource numbers are directional because thread accounting differs.
Use the Linux sandbox for authoritative measurements.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import selectors
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import benchmark_protocol

try:
    import psutil
except Exception:  # pragma: no cover - /proc is the supported Linux fallback
    psutil = None


BENCHMARK_ID = "central-mcp-resource"
MEASURED_BOUNDARY = "stdio-tools-call-write-to-response"
REQUEST_SOURCE = "moraine-mcp-jsonrpc-stdio"
SCHEMA_VERSION = "moraine-benchmark-v1"
_DATASET_PATTERN = "sha256:"


class BenchmarkFailure(RuntimeError):
    """A safe, structured benchmark failure suitable for diagnostics."""

    def __init__(self, *codes: str):
        normalized = [code for code in codes if code]
        super().__init__(",".join(normalized))
        self.codes = normalized or ["benchmark-failed"]

    def add_codes(self, codes: Sequence[str]) -> "BenchmarkFailure":
        for code in codes:
            if code not in self.codes:
                self.codes.append(code)
        return self


class OutputFailure(BenchmarkFailure):
    pass


def parse_ns(value: str) -> List[int]:
    out: List[int] = []
    for raw_part in value.split(","):
        part = raw_part.strip()
        if not part:
            continue
        try:
            n = int(part)
        except ValueError as exc:
            raise argparse.ArgumentTypeError("N values must be integers") from exc
        if n <= 0:
            raise argparse.ArgumentTypeError("N values must be > 0")
        if n in out:
            raise argparse.ArgumentTypeError("N values must be unique")
        out.append(n)
    if not out:
        raise argparse.ArgumentTypeError("at least one N is required")
    return out


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("value must be an integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be > 0")
    return parsed


def nonnegative_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("value must be numeric") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise argparse.ArgumentTypeError("value must be finite and >= 0")
    return parsed


def positive_float(value: str) -> float:
    parsed = nonnegative_float(value)
    if parsed == 0:
        raise argparse.ArgumentTypeError("value must be > 0")
    return parsed


def parse_arms(value: str) -> List[str]:
    arms = [part.strip() for part in value.split(",") if part.strip()]
    if not arms:
        raise argparse.ArgumentTypeError("at least one arm is required")
    if len(set(arms)) != len(arms):
        raise argparse.ArgumentTypeError("arms must be unique")
    unknown = [arm for arm in arms if arm not in ("embedded", "central")]
    if unknown:
        raise argparse.ArgumentTypeError("arms must be embedded and/or central")
    return arms


def validate_dataset_fingerprint(value: str) -> str:
    if not value.startswith(_DATASET_PATTERN):
        raise argparse.ArgumentTypeError("dataset fingerprint must start with sha256:")
    digest = value[len(_DATASET_PATTERN) :]
    if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
        raise argparse.ArgumentTypeError("dataset fingerprint must contain 64 lowercase hex characters")
    return value


def raise_fd_limit(target: int = 8192) -> None:
    try:
        import resource

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        new_soft = min(hard, max(soft, target))
        if new_soft > soft:
            resource.setrlimit(resource.RLIMIT_NOFILE, (new_soft, hard))
    except Exception:
        # This is an optimization only. Spawn failures remain explicit later.
        pass


def write_config(
    path: Path,
    *,
    clickhouse_url: str,
    database: str,
    root_dir: Path,
    use_central_server: bool,
    central_socket_path: Path,
) -> None:
    try:
        path.write_text(
            "\n".join(
                [
                    "[clickhouse]",
                    f'url = "{clickhouse_url}"',
                    f'database = "{database}"',
                    "",
                    "[runtime]",
                    f'root_dir = "{root_dir}"',
                    "",
                    "[mcp]",
                    f"use_central_server = {str(use_central_server).lower()}",
                    "start_central_on_up = false",
                    f'central_socket_path = "{central_socket_path}"',
                    "central_connect_timeout_ms = 250",
                    "",
                ]
            )
        )
    except OSError as exc:
        raise BenchmarkFailure("config-write-failed") from exc


def search_corpus_fingerprint(results: Sequence[Dict[str, Any]]) -> str:
    """Fingerprint stable identities and seeded text from a search result set."""
    identities: List[Dict[str, str]] = []
    for hit in results:
        if not isinstance(hit, dict):
            raise BenchmarkFailure("semantic-oracle-failed")
        event = hit.get("event")
        session = hit.get("session")
        snippet = hit.get("snippet")
        if (
            not isinstance(event, dict)
            or not isinstance(session, dict)
            or not isinstance(snippet, dict)
            or not isinstance(event.get("id"), str)
            or not isinstance(session.get("id"), str)
            or not isinstance(snippet.get("text"), str)
        ):
            raise BenchmarkFailure("semantic-oracle-failed")
        identities.append(
            {
                "event_id": event["id"],
                "session_id": session["id"],
                "text": snippet["text"],
            }
        )
    identities.sort(key=lambda value: (value["session_id"], value["event_id"], value["text"]))
    encoded = json.dumps(identities, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def validate_seeded_search(
    structured: Any,
    *,
    marker: str,
    expected_count: int,
    expected_fingerprint: str,
) -> None:
    if not marker:
        raise BenchmarkFailure("semantic-oracle-failed")
    if (
        not isinstance(structured, dict)
        or structured.get("schema_version") != "moraine.mcp.search_sessions.v1"
        or structured.get("tool") != "search_sessions"
        or "error" in structured
    ):
        raise BenchmarkFailure("semantic-oracle-failed")
    data = structured.get("data")
    if not isinstance(data, dict):
        raise BenchmarkFailure("semantic-oracle-failed")
    results = data.get("results")
    result_count = data.get("result_count")
    if (
        not isinstance(results, list)
        or isinstance(result_count, bool)
        or not isinstance(result_count, int)
        or result_count != len(results)
        or result_count != expected_count
        or result_count <= 0
    ):
        raise BenchmarkFailure("semantic-oracle-failed")
    for hit in results:
        snippet = hit.get("snippet") if isinstance(hit, dict) else None
        text = snippet.get("text") if isinstance(snippet, dict) else None
        if not isinstance(text, str) or marker not in text:
            raise BenchmarkFailure("semantic-oracle-failed")
    if search_corpus_fingerprint(results) != expected_fingerprint:
        raise BenchmarkFailure("dataset-corpus-mismatch")


@dataclass
class McpClient:
    proc: subprocess.Popen
    request_timeout_s: float
    tools_call_ms: Optional[float] = None
    _receive_buffer: bytes = field(default=b"", init=False, repr=False)

    def _read_response_line(self, deadline: float) -> str:
        if self.proc.stdout is None:
            raise BenchmarkFailure("client-pipe-unavailable")
        while b"\n" not in self._receive_buffer:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                if self.proc.poll() is not None:
                    raise BenchmarkFailure("client-exited")
                raise BenchmarkFailure("request-timeout")
            selector = selectors.DefaultSelector()
            try:
                selector.register(self.proc.stdout, selectors.EVENT_READ)
                ready = selector.select(remaining)
            except (OSError, ValueError) as exc:
                raise BenchmarkFailure("request-read-failed") from exc
            finally:
                selector.close()
            if not ready:
                if self.proc.poll() is not None:
                    raise BenchmarkFailure("client-exited")
                raise BenchmarkFailure("request-timeout")
            try:
                chunk = os.read(self.proc.stdout.fileno(), 65536)
            except OSError as exc:
                raise BenchmarkFailure("request-read-failed") from exc
            if not chunk:
                raise BenchmarkFailure("client-exited")
            self._receive_buffer += chunk

        raw_line, self._receive_buffer = self._receive_buffer.split(b"\n", 1)
        try:
            return raw_line.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise BenchmarkFailure("invalid-jsonrpc-response") from exc

    def _send(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if self.proc.stdin is None or self.proc.stdout is None:
            raise BenchmarkFailure("client-pipe-unavailable")
        if self.proc.poll() is not None:
            raise BenchmarkFailure("client-exited")
        try:
            self.proc.stdin.write(json.dumps(payload, separators=(",", ":")) + "\n")
            self.proc.stdin.flush()
        except (BrokenPipeError, OSError) as exc:
            raise BenchmarkFailure("client-exited") from exc

        response_line = self._read_response_line(time.monotonic() + self.request_timeout_s)
        try:
            response = json.loads(response_line)
        except (TypeError, json.JSONDecodeError) as exc:
            raise BenchmarkFailure("invalid-jsonrpc-response") from exc
        if not isinstance(response, dict):
            raise BenchmarkFailure("invalid-jsonrpc-response")
        return response

    def _rpc_result(self, request_id: int, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        response = self._send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params,
            }
        )
        if response.get("id") != request_id or "error" in response:
            raise BenchmarkFailure("semantic-oracle-failed")
        result = response.get("result")
        if not isinstance(result, dict):
            raise BenchmarkFailure("semantic-oracle-failed")
        return result

    def warm_up(
        self,
        query: str,
        expected_marker: str,
        *,
        expected_count: int,
        expected_fingerprint: str,
    ) -> None:
        self._rpc_result(1, "initialize", {})
        tools = self._rpc_result(2, "tools/list", {})
        listed = tools.get("tools")
        if not isinstance(listed, list) or not any(
            isinstance(tool, dict) and tool.get("name") == "search_sessions" for tool in listed
        ):
            raise BenchmarkFailure("semantic-oracle-failed")

        started_ns = time.perf_counter_ns()
        result = self._rpc_result(
            3,
            "tools/call",
            {"name": "search_sessions", "arguments": {"query": query}},
        )
        elapsed_ms = (time.perf_counter_ns() - started_ns) / 1_000_000.0
        if bool(result.get("isError")):
            raise BenchmarkFailure("semantic-oracle-failed")
        validate_seeded_search(
            result.get("structuredContent"),
            marker=expected_marker,
            expected_count=expected_count,
            expected_fingerprint=expected_fingerprint,
        )
        self.tools_call_ms = elapsed_ms

    def close(self) -> List[str]:
        errors: List[str] = []
        try:
            if self.proc.stdin is not None:
                self.proc.stdin.close()
        except OSError:
            errors.append("client-stdin-cleanup-failed")
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                self.proc.kill()
                self.proc.wait(timeout=5)
            except (OSError, subprocess.TimeoutExpired):
                errors.append("client-cleanup-failed")
        except OSError:
            errors.append("client-cleanup-failed")
        try:
            if self.proc.stdout is not None:
                self.proc.stdout.close()
        except OSError:
            errors.append("client-stdout-cleanup-failed")
        if self.proc.returncode not in (None, 0, -9):
            errors.append("client-exited")
        return errors


def spawn_client(moraine_mcp: str, config: Path, request_timeout_s: float) -> McpClient:
    try:
        proc = subprocess.Popen(
            [moraine_mcp, "--config", str(config)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            bufsize=1,
        )
    except OSError as exc:
        raise BenchmarkFailure("client-startup-failed") from exc
    return McpClient(proc=proc, request_timeout_s=request_timeout_s)


@dataclass
class ResourceSample:
    proc_count: int = 0
    rss_bytes: int = 0
    threads: int = 0




def _read_proc(pid: int) -> Optional[Tuple[int, int]]:
    """Return (rss_bytes, threads) from Linux /proc, or None."""
    try:
        rss_bytes = 0
        threads = 0
        with open(f"/proc/{pid}/status", "r") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    rss_bytes = int(line.split()[1]) * 1024
                elif line.startswith("Threads:"):
                    threads = int(line.split()[1])
        if rss_bytes <= 0 or threads <= 0:
            return None
        return rss_bytes, threads
    except (OSError, ValueError, IndexError):
        return None


def _read_pid(pid: int) -> Optional[Tuple[int, int]]:
    if psutil is not None:
        try:
            process = psutil.Process(pid)
            with process.oneshot():
                return int(process.memory_info().rss), int(process.num_threads())
        except Exception:
            return None
    return _read_proc(pid)


def measurement_available() -> bool:
    return psutil is not None or sys.platform.startswith("linux")


def sample_pids(pids: List[int]) -> ResourceSample:
    sample = ResourceSample()
    for pid in pids:
        got = _read_pid(pid)
        if got is None:
            raise BenchmarkFailure("resource-sample-failed")
        sample.rss_bytes += got[0]
        sample.threads += got[1]
        sample.proc_count += 1
    return sample


@dataclass
class ArmResult:
    arm: str
    n: int
    client: ResourceSample
    server: ResourceSample
    tools_call_ms: List[float] = field(default_factory=list)

    @property
    def total_rss_bytes(self) -> int:
        return self.client.rss_bytes + self.server.rss_bytes

    @property
    def total_threads(self) -> int:
        return self.client.threads + self.server.threads

    @property
    def total_procs(self) -> int:
        return self.client.proc_count + self.server.proc_count

    @property
    def marginal_rss_bytes(self) -> float:
        return self.client.rss_bytes / self.n

    @property
    def mean_tools_call_ms(self) -> float:
        if not self.tools_call_ms:
            raise BenchmarkFailure("insufficient-successful-samples")
        return statistics.mean(self.tools_call_ms)


def wait_for_socket(
    sock: Path,
    server_proc: subprocess.Popen,
    timeout_s: float,
) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if server_proc.poll() is not None:
            raise BenchmarkFailure("central-server-exited")
        if sock.exists():
            candidate = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                candidate.connect(str(sock))
                return
            except OSError:
                pass
            finally:
                candidate.close()
        time.sleep(0.05)
    raise BenchmarkFailure("central-startup-timeout")


def _stop_server(server_proc: subprocess.Popen) -> List[str]:
    errors: List[str] = []
    if server_proc.poll() is None:
        try:
            server_proc.terminate()
            server_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                server_proc.kill()
                server_proc.wait(timeout=5)
            except (OSError, subprocess.TimeoutExpired):
                errors.append("central-cleanup-failed")
        except OSError:
            errors.append("central-cleanup-failed")
    elif server_proc.returncode not in (0, -15, -9):
        errors.append("central-server-exited")
    return errors


def run_arm(
    arm: str,
    n: int,
    *,
    moraine_mcp: str,
    clickhouse_url: str,
    database: str,
    query: str,
    seed_marker: str,
    dataset_fingerprint: str,
    dataset_cardinality: int,
    settle_s: float,
    batch: int,
    startup_timeout_s: float,
    request_timeout_s: float,
) -> ArmResult:
    central = arm == "central"
    short_temp_root = "/tmp" if Path("/tmp").is_dir() and os.access("/tmp", os.W_OK) else None
    try:
        workdir = Path(tempfile.mkdtemp(prefix=f"mb-{arm}-{n}-", dir=short_temp_root))
    except OSError as exc:
        raise BenchmarkFailure("workdir-create-failed") from exc

    server_proc: Optional[subprocess.Popen] = None
    clients: List[McpClient] = []
    result: Optional[ArmResult] = None
    primary: Optional[BaseException] = None
    cleanup_exception: Optional[BaseException] = None
    cleanup_codes: List[str] = []
    try:
        root_dir = workdir / "root"
        root_dir.mkdir(parents=True, exist_ok=True)
        (root_dir / "run").mkdir(parents=True, exist_ok=True)
        sock = root_dir / "run" / "mcp.sock"
        config = workdir / "config.toml"
        write_config(
            config,
            clickhouse_url=clickhouse_url,
            database=database,
            root_dir=root_dir,
            use_central_server=central,
            central_socket_path=sock,
        )

        if central:
            static_dir = root_dir / "monitor-static"
            try:
                static_dir.mkdir()
                (static_dir / "index.html").write_text("<!doctype html><title>Moraine benchmark</title>")
            except OSError as exc:
                raise BenchmarkFailure("central-static-fixture-failed") from exc
            try:
                server_proc = subprocess.Popen(
                    [
                        moraine_mcp,
                        "--config",
                        str(config),
                        "--serve",
                        "socket",
                        "--host",
                        "127.0.0.1",
                        "--port",
                        "0",
                        "--static-dir",
                        str(static_dir),
                    ],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            except OSError as exc:
                raise BenchmarkFailure("central-startup-failed") from exc
            wait_for_socket(sock, server_proc, startup_timeout_s)

        for start in range(0, n, batch):
            group: List[McpClient] = []
            for _ in range(start, min(start + batch, n)):
                client = spawn_client(moraine_mcp, config, request_timeout_s)
                clients.append(client)
                group.append(client)
            for client in group:
                client.warm_up(
                    query,
                    expected_marker=seed_marker,
                    expected_count=dataset_cardinality,
                    expected_fingerprint=dataset_fingerprint,
                )
            time.sleep(0.1)

        time.sleep(settle_s)
        for client in clients:
            if client.proc.poll() is not None:
                raise BenchmarkFailure("client-exited")
        if server_proc is not None and server_proc.poll() is not None:
            raise BenchmarkFailure("central-server-exited")

        client_sample = sample_pids([client.proc.pid for client in clients])
        server_sample = ResourceSample()
        if server_proc is not None:
            server_sample = sample_pids([server_proc.pid])
        tools_call_ms = [
            client.tools_call_ms for client in clients if client.tools_call_ms is not None
        ]
        if len(tools_call_ms) != n:
            raise BenchmarkFailure("insufficient-successful-samples")
        result = ArmResult(
            arm=arm,
            n=n,
            client=client_sample,
            server=server_sample,
            tools_call_ms=tools_call_ms,
        )
    except BenchmarkFailure as exc:
        primary = exc
    except (KeyboardInterrupt, SystemExit, GeneratorExit) as exc:
        primary = exc
    except Exception as exc:
        primary = BenchmarkFailure("arm-execution-failed")
        primary.__cause__ = exc
    finally:
        for client in clients:
            try:
                cleanup_codes.extend(client.close())
            except BaseException as exc:
                cleanup_codes.append("client-cleanup-failed")
                if cleanup_exception is None:
                    cleanup_exception = exc
        if server_proc is not None:
            try:
                cleanup_codes.extend(_stop_server(server_proc))
            except BaseException as exc:
                cleanup_codes.append("central-cleanup-failed")
                if cleanup_exception is None:
                    cleanup_exception = exc
                try:
                    if server_proc.poll() is None:
                        server_proc.kill()
                        server_proc.wait(timeout=5)
                except BaseException:
                    pass
        try:
            shutil.rmtree(workdir)
        except FileNotFoundError:
            pass
        except KeyboardInterrupt as exc:
            cleanup_codes.append("workdir-cleanup-failed")
            if cleanup_exception is None:
                cleanup_exception = exc
            try:
                shutil.rmtree(workdir)
            except FileNotFoundError:
                pass
            except BaseException:
                pass
        except OSError:
            cleanup_codes.append("workdir-cleanup-failed")

    if isinstance(primary, BenchmarkFailure):
        if cleanup_exception is not None:
            setattr(primary, "cleanup_exception", cleanup_exception)
        raise primary.add_codes(cleanup_codes)
    if primary is not None:
        setattr(primary, "cleanup_codes", tuple(dict.fromkeys(cleanup_codes)))
        if cleanup_exception is not None:
            setattr(primary, "cleanup_exception", cleanup_exception)
        raise primary
    if cleanup_exception is not None:
        setattr(cleanup_exception, "cleanup_codes", tuple(dict.fromkeys(cleanup_codes)))
        raise cleanup_exception
    if cleanup_codes:
        raise BenchmarkFailure(*cleanup_codes)
    if result is None:
        raise BenchmarkFailure("arm-execution-failed")
    return result


@dataclass
class MatrixRun:
    planned: int
    attempted: int
    iterations: List[Dict[Tuple[str, int], ArmResult]]
    diagnostics: List[str]

    @property
    def successful(self) -> int:
        return len(self.iterations)

    @property
    def errors(self) -> int:
        return self.attempted - self.successful


def run_matrix(
    *,
    reps: int,
    arms: List[str],
    ns: List[int],
    run_one: Any = run_arm,
    **arm_kwargs: Any,
) -> MatrixRun:
    iterations: List[Dict[Tuple[str, int], ArmResult]] = []
    diagnostics: List[str] = []
    attempted = 0
    for _ in range(reps):
        attempted += 1
        paired: Dict[Tuple[str, int], ArmResult] = {}
        try:
            for n in ns:
                for arm in arms:
                    paired[(arm, n)] = run_one(arm, n, **arm_kwargs)
        except BenchmarkFailure as exc:
            diagnostics.extend(code for code in exc.codes if code not in diagnostics)
            break
        iterations.append(paired)
    return MatrixRun(
        planned=reps,
        attempted=attempted,
        iterations=iterations,
        diagnostics=diagnostics,
    )


def _matrix_identity(
    arms: List[str],
    ns: List[int],
    query: str,
    settle_s: float,
    batch: int,
    startup_timeout_s: float,
    request_timeout_s: float,
) -> Tuple[str, str]:
    canonical = json.dumps(
        {
            "arms": arms,
            "ns": ns,
            "query_sha256": hashlib.sha256(query.encode("utf-8")).hexdigest(),
            "settle_seconds": settle_s,
            "batch": batch,
            "startup_timeout_seconds": startup_timeout_s,
            "request_timeout_seconds": request_timeout_s,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    suffix = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
    readable = "-".join(arms + ["n" + "-".join(str(n) for n in ns)])
    return f"{BENCHMARK_ID}-{readable}-{suffix}"[:128], f"search-sessions-{suffix}"


def _measurement_key(arm: str, n: int, metric: str) -> str:
    return f"{arm}_n{n}_{metric}"


def build_measurements(
    matrix: MatrixRun,
    arms: List[str],
    ns: List[int],
) -> Dict[str, List[float]]:
    measurements: Dict[str, List[float]] = {}
    accessors = {
        "process_count": lambda result: result.total_procs,
        "client_rss_bytes": lambda result: result.client.rss_bytes,
        "server_rss_bytes": lambda result: result.server.rss_bytes,
        "total_rss_bytes": lambda result: result.total_rss_bytes,
        "marginal_session_rss_bytes": lambda result: result.marginal_rss_bytes,
        "thread_count": lambda result: result.total_threads,
        "tools_call_mean_ms": lambda result: result.mean_tools_call_ms,
    }
    for n in ns:
        for arm in arms:
            for metric, accessor in accessors.items():
                key = _measurement_key(arm, n, metric)
                measurements[key] = [
                    float(accessor(iteration[(arm, n)])) for iteration in matrix.iterations
                ]
    return measurements


def _slug(value: str, fallback: str) -> str:
    normalized = "".join(ch.lower() if ch.isalnum() else "-" for ch in value)
    normalized = "-".join(part for part in normalized.split("-") if part)
    return (normalized or fallback)[:128]


def _cache_state(arms: List[str], ns: List[int], profile: str) -> str:
    if profile == "smoke" and ns == [1]:
        return "cold"
    if arms == ["embedded"]:
        return "cold"
    if arms == ["central"] and max(ns) == 1:
        return "cold"
    return "mixed"


def build_artifact(
    *,
    matrix: MatrixRun,
    arms: List[str],
    ns: List[int],
    profile: str,
    query: str,
    settle_s: float,
    batch: int,
    startup_timeout_s: float,
    request_timeout_s: float,
    dataset_fingerprint: str,
    dataset_cardinality: int,
    git_commit: str,
    dirty: bool,
    build_profile: str,
    target: str,
    runner_os: str,
    cpu_class: str,
) -> Dict[str, Any]:
    scenario_id, workload_id = _matrix_identity(
        arms,
        ns,
        query,
        settle_s,
        batch,
        startup_timeout_s,
        request_timeout_s,
    )
    measurements = build_measurements(matrix, arms, ns)
    successful = matrix.successful
    error_rate = matrix.errors / matrix.attempted if matrix.attempted else 0.0
    quality: Dict[str, Any] = {
        "oracle_status": "pass" if matrix.errors == 0 and successful == matrix.planned else "fail",
        "passed_checks": successful,
        "failed_checks": matrix.errors,
        "expected_count": matrix.planned,
        "observed_count": successful,
        "mismatches": matrix.errors,
        "success_rate": successful / matrix.planned,
        "error_rate": error_rate,
    }
    metrics: Dict[str, Any] = {
        "quality": quality,
        "storage": {"dataset_cardinality_count": dataset_cardinality},
    }
    if successful:
        metrics["resources"] = {
            f"mean_{name}": statistics.mean(values)
            for name, values in measurements.items()
            if not name.endswith("_tools_call_mean_ms")
        }

    dimensions = {
        "dataset_backed": True,
        "cache_sensitive": True,
        "concurrent": max(ns) > 1,
        "request_producing": True,
    }
    fingerprints: Dict[str, Any] = {
        "dataset": {
            "fingerprint": dataset_fingerprint,
            "cardinality": dataset_cardinality,
        },
        "cache_state": _cache_state(arms, ns, profile),
        "request_source": REQUEST_SOURCE,
    }
    if dimensions["concurrent"]:
        fingerprints["concurrency"] = max(ns)

    return {
        "schema_version": SCHEMA_VERSION,
        "benchmark_id": BENCHMARK_ID,
        "scenario_id": scenario_id,
        "source": {"git_commit": git_commit, "dirty": dirty},
        "build": {"profile": _slug(build_profile, "unknown"), "target": _slug(target, "unknown")},
        "runner": {"os": _slug(runner_os, "unknown"), "cpu_class": _slug(cpu_class, "unknown")},
        "scenario": {
            "profile": profile,
            "workload_id": workload_id,
            "measured_boundary": MEASURED_BOUNDARY,
            "dimensions": dimensions,
            "fingerprints": fingerprints,
        },
        "samples": {
            "planned": matrix.planned,
            "attempted": matrix.attempted,
            "successful": successful,
            "errors": matrix.errors,
            "measurements": measurements,
        },
        "semantic": {"status": "pass" if matrix.errors == 0 and successful == matrix.planned else "fail"},
        "timing": {"status": "not_evaluated", "non_blocking": True},
        "metrics": metrics,
        "diagnostics": [{"code": code} for code in matrix.diagnostics],
        "artifacts": [],
    }


def validate_artifact(artifact: Dict[str, Any]) -> None:
    try:
        benchmark_protocol.validate_artifact(artifact)
    except Exception as exc:
        raise OutputFailure("protocol-validation-failed") from exc


def atomic_write_artifact(path: Path, artifact: Dict[str, Any]) -> None:
    try:
        benchmark_protocol.write_artifact(path, artifact)
    except benchmark_protocol.ProtocolError as exc:
        raise OutputFailure("protocol-validation-failed") from exc
    except (OSError, TypeError, ValueError) as exc:
        raise OutputFailure("output-write-failed") from exc


def resolve_moraine_mcp(explicit: Optional[str]) -> str:
    if explicit:
        if not Path(explicit).is_file():
            raise BenchmarkFailure("binary-not-found")
        return explicit
    found = shutil.which("moraine-mcp")
    if found:
        return found
    repo_root = Path(__file__).resolve().parents[2]
    for relative in ("target/debug/moraine-mcp", "target/release/moraine-mcp"):
        candidate = repo_root / relative
        if candidate.is_file():
            return str(candidate)
    raise BenchmarkFailure("binary-not-found")


def resolve_source(repo_root: Path, explicit_commit: Optional[str], dirty_arg: str) -> Tuple[str, bool]:
    commit = explicit_commit or os.environ.get("MORAINE_BENCHMARK_GIT_COMMIT")
    dirty: Optional[bool] = None
    if dirty_arg != "auto":
        dirty = dirty_arg == "true"
    elif os.environ.get("MORAINE_BENCHMARK_GIT_DIRTY") in ("0", "1"):
        dirty = os.environ["MORAINE_BENCHMARK_GIT_DIRTY"] == "1"

    try:
        if commit is None:
            commit = subprocess.run(
                ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
                check=True,
                capture_output=True,
                text=True,
                timeout=5,
            ).stdout.strip()
        if dirty is None:
            status = subprocess.run(
                ["git", "-C", str(repo_root), "status", "--porcelain", "--untracked-files=normal"],
                check=True,
                capture_output=True,
                text=True,
                timeout=10,
            ).stdout
            dirty = bool(status.strip())
    except (OSError, subprocess.SubprocessError) as exc:
        raise BenchmarkFailure("source-metadata-failed") from exc

    if len(commit) != 40 or any(ch not in "0123456789abcdef" for ch in commit):
        raise BenchmarkFailure("source-metadata-invalid")
    return commit, bool(dirty)


def infer_build_profile(binary: str) -> str:
    parts = Path(binary).parts
    if "release" in parts:
        return "release"
    if "debug" in parts:
        return "debug"
    return "unknown"


def print_table(matrix: MatrixRun, arms: List[str], ns: List[int]) -> None:
    headers = [
        "N",
        "arm",
        "procs",
        "client RSS (MB)",
        "server RSS (MB)",
        "total RSS (MB)",
        "marginal/session (MB)",
        "total threads",
        "tools/call (ms)",
    ]
    print("| " + " | ".join(headers) + " |")
    print("|" + "|".join("---" for _ in headers) + "|")
    for n in ns:
        for arm in arms:
            results = [iteration[(arm, n)] for iteration in matrix.iterations]
            if not results:
                continue
            row = [
                n,
                arm,
                round(statistics.mean(result.total_procs for result in results), 1),
                round(statistics.mean(result.client.rss_bytes for result in results) / (1024 * 1024), 1),
                round(statistics.mean(result.server.rss_bytes for result in results) / (1024 * 1024), 1),
                round(statistics.mean(result.total_rss_bytes for result in results) / (1024 * 1024), 1),
                round(statistics.mean(result.marginal_rss_bytes for result in results) / (1024 * 1024), 2),
                round(statistics.mean(result.total_threads for result in results), 1),
                round(statistics.mean(result.mean_tools_call_ms for result in results), 1),
            ]
            print("| " + " | ".join(str(value) for value in row) + " |")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--moraine-mcp", help="path to the moraine-mcp binary")
    parser.add_argument("--clickhouse-url", default="http://127.0.0.1:8123")
    parser.add_argument("--database", default="moraine")
    parser.add_argument("--ns", type=parse_ns, default=[1, 10, 50, 100])
    parser.add_argument("--reps", type=positive_int, default=3)
    parser.add_argument("--arms", type=parse_arms, default=["embedded", "central"])
    parser.add_argument("--query", default="error handling")
    parser.add_argument("--seed-marker", help="known marker present in every owned seeded result")
    parser.add_argument("--settle-seconds", type=nonnegative_float, default=2.0)
    parser.add_argument("--batch", type=positive_int, default=25)
    parser.add_argument("--startup-timeout-seconds", type=positive_float, default=10.0)
    parser.add_argument("--request-timeout-seconds", type=positive_float, default=20.0)
    parser.add_argument("--profile", choices=("smoke", "full"), default="full")
    parser.add_argument("--dataset-fingerprint", type=validate_dataset_fingerprint)
    parser.add_argument("--dataset-cardinality", type=positive_int)
    parser.add_argument("--git-commit")
    parser.add_argument("--dirty", choices=("auto", "true", "false"), default="auto")
    parser.add_argument("--build-profile")
    parser.add_argument("--target")
    parser.add_argument("--json-out", help="atomically write one moraine-benchmark-v1 result")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.seed_marker or args.dataset_fingerprint is None or args.dataset_cardinality is None:
        parser.error(
            "--seed-marker, --dataset-fingerprint, and --dataset-cardinality are required "
            "seeded-corpus oracles"
        )

    if not measurement_available():
        print("error: resource-measurement-unavailable", file=sys.stderr)
        return 2

    try:
        raise_fd_limit()
        binary = resolve_moraine_mcp(args.moraine_mcp)
        print("# central MCP resource benchmark")
        print(f"# binary: {Path(binary).name}")
        print(f"# platform: {sys.platform} reps={args.reps} Ns={args.ns}")
        print(f"# profile: {args.profile}\n")

        matrix = run_matrix(
            reps=args.reps,
            arms=args.arms,
            ns=args.ns,
            moraine_mcp=binary,
            clickhouse_url=args.clickhouse_url,
            database=args.database,
            query=args.query,
            seed_marker=args.seed_marker,
            dataset_fingerprint=args.dataset_fingerprint,
            dataset_cardinality=args.dataset_cardinality,
            settle_s=args.settle_seconds,
            batch=args.batch,
            startup_timeout_s=args.startup_timeout_seconds,
            request_timeout_s=args.request_timeout_seconds,
        )
        print_table(matrix, args.arms, args.ns)

        if args.json_out:
            repo_root = Path(__file__).resolve().parents[2]
            commit, dirty = resolve_source(repo_root, args.git_commit, args.dirty)
            artifact = build_artifact(
                matrix=matrix,
                arms=args.arms,
                ns=args.ns,
                profile=args.profile,
                query=args.query,
                settle_s=args.settle_seconds,
                batch=args.batch,
                startup_timeout_s=args.startup_timeout_seconds,
                request_timeout_s=args.request_timeout_seconds,
                dataset_fingerprint=args.dataset_fingerprint,
                dataset_cardinality=args.dataset_cardinality,
                git_commit=commit,
                dirty=dirty,
                build_profile=args.build_profile or infer_build_profile(binary),
                target=args.target or f"{platform.machine()}-{sys.platform}",
                runner_os=platform.system(),
                cpu_class=platform.machine(),
            )
            atomic_write_artifact(Path(args.json_out), artifact)
            print(f"\nwrote {Path(args.json_out).name}")

        if matrix.errors:
            print(
                "error: benchmark failed (" + ",".join(matrix.diagnostics) + ")",
                file=sys.stderr,
            )
            return 1
        return 0
    except BenchmarkFailure as exc:
        print("error: benchmark failed (" + ",".join(exc.codes) + ")", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
