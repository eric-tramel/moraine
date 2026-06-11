#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "psutil>=5.9,<8",
# ]
# ///
"""Resource benchmark for the shared central MCP server vs per-session embedded servers.

This quantifies the win behind `mcp.use_central_server`: instead of every agent
session booting a full `moraine-mcp` server (its own multi-threaded tokio runtime,
ClickHouse HTTP client + connection pool, and search caches), sessions share ONE
central server and each become a thin stdio<->socket proxy.

It spawns N simulated agent MCP clients, drives each through the realistic steady
state (initialize -> tools/list -> one search_sessions tools/call -> idle resident),
then measures aggregate RSS, OS thread count, and process count via psutil. Two arms:

  embedded : mcp.use_central_server = false. Each client is a full embedded server.
  central  : one `moraine-mcp --serve socket` daemon + N proxy clients.

For a fair isolation of the variable this PR changes, clients are spawned as
`moraine-mcp --config <cfg>` directly (the child process `moraine run mcp` execs);
the `moraine` launcher-parent overhead is identical in both arms and excluded.

Run it inside the dev sandbox (ClickHouse must be reachable for tools/call):

  scripts/dev/sandbox/moraine-sandbox shell <id>
  # inside:
  python3 /repo/scripts/bench/central_mcp_resource.py \
      --moraine-mcp /home/moraine/target/debug/moraine-mcp \
      --clickhouse-url http://clickhouse:8123 \
      --ns 1,10,50,100 --reps 3

On macOS the numbers are directional (thread accounting differs); take authoritative
figures from the Linux sandbox.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

try:
    import psutil
except Exception:  # pragma: no cover
    psutil = None


# ----------------------------------------------------------------------------- helpers


def parse_ns(value: str) -> list[int]:
    out: list[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        n = int(part)
        if n <= 0:
            raise argparse.ArgumentTypeError("N values must be > 0")
        out.append(n)
    if not out:
        raise argparse.ArgumentTypeError("at least one N is required")
    return out


def raise_fd_limit(target: int = 8192) -> None:
    try:
        import resource

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        new_soft = min(hard, max(soft, target))
        if new_soft > soft:
            resource.setrlimit(resource.RLIMIT_NOFILE, (new_soft, hard))
    except Exception:
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
                # Keep startup fast even when the socket is missing.
                "central_connect_timeout_ms = 250",
                "",
            ]
        )
    )


# ----------------------------------------------------------------------------- client


@dataclass
class McpClient:
    proc: subprocess.Popen
    tools_call_ms: Optional[float] = None

    def _send(self, payload: dict) -> dict:
        line = json.dumps(payload) + "\n"
        assert self.proc.stdin is not None and self.proc.stdout is not None
        self.proc.stdin.write(line)
        self.proc.stdin.flush()
        resp = self.proc.stdout.readline()
        if not resp:
            raise RuntimeError("MCP server closed stream unexpectedly")
        return json.loads(resp)

    def warm_up(self, query: str) -> None:
        self._send({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}})
        self._send({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
        t0 = time.time()
        self._send(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {"name": "search_sessions", "arguments": {"query": query}},
            }
        )
        self.tools_call_ms = (time.time() - t0) * 1000.0
        # Leave stdin OPEN so the process stays resident (idle agent steady state).

    def close(self) -> None:
        try:
            if self.proc.stdin:
                self.proc.stdin.close()
        except Exception:
            pass
        try:
            self.proc.wait(timeout=5)
        except Exception:
            self.proc.kill()


def spawn_client(moraine_mcp: str, config: Path) -> McpClient:
    proc = subprocess.Popen(
        [moraine_mcp, "--config", str(config)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        bufsize=1,
    )
    return McpClient(proc=proc)


# ----------------------------------------------------------------------------- measure


@dataclass
class ResourceSample:
    proc_count: int = 0
    rss_bytes: int = 0
    threads: int = 0


def sample_pids(pids: list[int]) -> ResourceSample:
    if psutil is None:
        raise RuntimeError("psutil is required for measurement")
    sample = ResourceSample()
    for pid in pids:
        try:
            p = psutil.Process(pid)
            with p.oneshot():
                sample.rss_bytes += int(p.memory_info().rss)
                sample.threads += int(p.num_threads())
            sample.proc_count += 1
        except Exception:
            continue
    return sample


# ----------------------------------------------------------------------------- run arm


@dataclass
class ArmResult:
    arm: str
    n: int
    client: ResourceSample
    server: ResourceSample
    tools_call_ms: list[float] = field(default_factory=list)

    @property
    def total_rss_mb(self) -> float:
        return (self.client.rss_bytes + self.server.rss_bytes) / (1024 * 1024)

    @property
    def total_threads(self) -> int:
        return self.client.threads + self.server.threads

    @property
    def total_procs(self) -> int:
        return self.client.proc_count + self.server.proc_count

    @property
    def marginal_rss_mb(self) -> float:
        if self.n == 0:
            return 0.0
        return (self.client.rss_bytes / self.n) / (1024 * 1024)


def wait_for_socket(sock: Path, timeout_s: float = 10.0) -> None:
    import socket as _socket

    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if sock.exists():
            s = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
            try:
                s.connect(str(sock))
                s.close()
                return
            except OSError:
                pass
            finally:
                try:
                    s.close()
                except Exception:
                    pass
        time.sleep(0.05)
    raise RuntimeError(f"central server socket never came up at {sock}")


def run_arm(
    arm: str,
    n: int,
    *,
    moraine_mcp: str,
    clickhouse_url: str,
    database: str,
    query: str,
    settle_s: float,
    batch: int,
) -> ArmResult:
    central = arm == "central"
    workdir = Path(tempfile.mkdtemp(prefix=f"moraine-bench-{arm}-{n}-"))
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

    server_proc: Optional[subprocess.Popen] = None
    clients: list[McpClient] = []
    try:
        if central:
            server_proc = subprocess.Popen(
                [moraine_mcp, "--config", str(config), "--serve", "socket"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            wait_for_socket(sock)

        # Spawn clients in batches to avoid fd/backlog spikes.
        for start in range(0, n, batch):
            group = []
            for _ in range(start, min(start + batch, n)):
                group.append(spawn_client(moraine_mcp, config))
            for c in group:
                c.warm_up(query)
            clients.extend(group)
            time.sleep(0.1)

        time.sleep(settle_s)

        client_sample = sample_pids([c.proc.pid for c in clients])
        server_sample = ResourceSample()
        if server_proc is not None:
            server_sample = sample_pids([server_proc.pid])

        return ArmResult(
            arm=arm,
            n=n,
            client=client_sample,
            server=server_sample,
            tools_call_ms=[c.tools_call_ms for c in clients if c.tools_call_ms is not None],
        )
    finally:
        for c in clients:
            c.close()
        if server_proc is not None:
            server_proc.terminate()
            try:
                server_proc.wait(timeout=5)
            except Exception:
                server_proc.kill()
        shutil.rmtree(workdir, ignore_errors=True)


# ----------------------------------------------------------------------------- main


def resolve_moraine_mcp(explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    for cand in ("moraine-mcp",):
        found = shutil.which(cand)
        if found:
            return found
    # Fall back to the repo debug build.
    repo_root = Path(__file__).resolve().parents[2]
    for rel in ("target/debug/moraine-mcp", "target/release/moraine-mcp"):
        p = repo_root / rel
        if p.exists():
            return str(p)
    raise SystemExit("could not resolve moraine-mcp; pass --moraine-mcp <path>")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--moraine-mcp", help="path to the moraine-mcp binary")
    ap.add_argument("--clickhouse-url", default="http://127.0.0.1:8123")
    ap.add_argument("--database", default="moraine")
    ap.add_argument("--ns", type=parse_ns, default=[1, 10, 50, 100])
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument(
        "--arms",
        default="embedded,central",
        help="comma list of arms to run (embedded, central)",
    )
    ap.add_argument("--query", default="error handling")
    ap.add_argument("--settle-seconds", type=float, default=2.0)
    ap.add_argument("--batch", type=int, default=25)
    ap.add_argument("--json-out", help="write raw results to this JSON path")
    args = ap.parse_args()

    if psutil is None:
        print("error: psutil is required (run via `uv run` or `pip install psutil`)", file=sys.stderr)
        return 2

    raise_fd_limit()
    moraine_mcp = resolve_moraine_mcp(args.moraine_mcp)
    arms = [a.strip() for a in args.arms.split(",") if a.strip()]

    print(f"# central MCP resource benchmark")
    print(f"# binary: {moraine_mcp}")
    print(f"# clickhouse: {args.clickhouse_url} db={args.database}")
    print(f"# platform: {sys.platform}  reps={args.reps}  Ns={args.ns}\n")

    rows: list[dict] = []
    # Aggregate by (arm, n): take mean across reps of each metric.
    for n in args.ns:
        for arm in arms:
            reps: list[ArmResult] = []
            for _ in range(args.reps):
                reps.append(
                    run_arm(
                        arm,
                        n,
                        moraine_mcp=moraine_mcp,
                        clickhouse_url=args.clickhouse_url,
                        database=args.database,
                        query=args.query,
                        settle_s=args.settle_seconds,
                        batch=args.batch,
                    )
                )
            row = {
                "arm": arm,
                "n": n,
                "procs": round(statistics.mean(r.total_procs for r in reps), 1),
                "client_rss_mb": round(statistics.mean(r.client.rss_bytes for r in reps) / (1024 * 1024), 1),
                "server_rss_mb": round(statistics.mean(r.server.rss_bytes for r in reps) / (1024 * 1024), 1),
                "total_rss_mb": round(statistics.mean(r.total_rss_mb for r in reps), 1),
                "marginal_rss_mb": round(statistics.mean(r.marginal_rss_mb for r in reps), 2),
                "total_threads": round(statistics.mean(r.total_threads for r in reps), 1),
                "tools_call_ms": round(
                    statistics.mean(
                        statistics.mean(r.tools_call_ms) if r.tools_call_ms else 0.0 for r in reps
                    ),
                    1,
                ),
            }
            rows.append(row)

    # Markdown table.
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
    for row in rows:
        print(
            "| "
            + " | ".join(
                str(x)
                for x in [
                    row["n"],
                    row["arm"],
                    row["procs"],
                    row["client_rss_mb"],
                    row["server_rss_mb"],
                    row["total_rss_mb"],
                    row["marginal_rss_mb"],
                    row["total_threads"],
                    row["tools_call_ms"],
                ]
            )
            + " |"
        )

    if args.json_out:
        Path(args.json_out).write_text(json.dumps(rows, indent=2))
        print(f"\nwrote {args.json_out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
