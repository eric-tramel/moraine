#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import re
import select
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen


WINDOW_RE = re.compile(r"^\s*(\d+)\s*([smhdw])\s*$", re.IGNORECASE)
BENCHMARK_REPLAY_SOURCE = "benchmark-replay"


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be > 0")
    return parsed


def non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be >= 0")
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Measure the Moraine MCP v1 search_sessions/open interface over JSON-RPC stdio. "
            "The tool calls are read-only; corpus and query selection use ClickHouse SELECTs only."
        )
    )
    parser.add_argument("--config", required=True, help="Path to moraine.toml/config.toml")
    parser.add_argument(
        "--moraine-bin",
        default="bin/moraine",
        help="Path to the moraine CLI used to launch MCP",
    )
    parser.add_argument(
        "--service-bin-dir",
        help=(
            "Directory containing moraine-mcp. Defaults to the sibling directory of "
            "--moraine-bin, then repo target/debug when present."
        ),
    )
    parser.add_argument(
        "--query",
        action="append",
        default=[],
        help="Query to benchmark. May be repeated.",
    )
    parser.add_argument(
        "--top-n-log-queries",
        type=non_negative_int,
        default=0,
        help="Also select this many historical queries from search_query_log.",
    )
    parser.add_argument("--window", default="7d", help="Log-query selection window, e.g. 24h, 7d")
    parser.add_argument("--n-hits", type=positive_int, default=10)
    parser.add_argument("--warmup", type=non_negative_int, default=1)
    parser.add_argument("--repeats", type=positive_int, default=5)
    parser.add_argument("--timeout-seconds", type=positive_int, default=20)
    parser.add_argument(
        "--open-kind",
        action="append",
        choices=["event", "turn", "session", "none"],
        default=[],
        help=(
            "Open ID kind from the first search hit. May be repeated. "
            "Defaults to event, turn, and session."
        ),
    )
    parser.add_argument(
        "--event-type",
        action="append",
        default=[],
        help="Optional search_sessions event_types entry. May be repeated.",
    )
    parser.add_argument(
        "--min-docs",
        type=non_negative_int,
        default=100_000,
        help="Minimum search_documents rows expected for SLA validation.",
    )
    parser.add_argument(
        "--allow-small-corpus",
        action="store_true",
        help="Do not fail when search_documents has fewer than --min-docs rows.",
    )
    parser.add_argument(
        "--fail-on-sla-miss",
        action="store_true",
        help="Exit non-zero if any measured tool response reports met_sla=false.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Select queries and print corpus only.")
    parser.add_argument("--output-json", help="Write machine-readable results JSON.")
    return parser.parse_args()


def strip_inline_comment(value: str) -> str:
    in_quotes = False
    escaped = False
    chars: list[str] = []
    for ch in value:
        if escaped:
            chars.append(ch)
            escaped = False
            continue
        if ch == "\\":
            chars.append(ch)
            escaped = True
            continue
        if ch == '"':
            chars.append(ch)
            in_quotes = not in_quotes
            continue
        if ch == "#" and not in_quotes:
            break
        chars.append(ch)
    return "".join(chars).strip()


def parse_toml_scalar(value: str) -> Any:
    stripped = strip_inline_comment(value)
    if stripped.startswith('"') and stripped.endswith('"'):
        return json.loads(stripped)
    lowered = stripped.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if re.fullmatch(r"[+-]?\d+", stripped):
        return int(stripped)
    if re.fullmatch(r"[+-]?(\d+\.\d*|\d*\.\d+)([eE][+-]?\d+)?", stripped):
        return float(stripped)
    return stripped


def read_clickhouse_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise RuntimeError(f"config not found: {path}")

    section = ""
    clickhouse: dict[str, Any] = {}
    for line_no, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1].strip()
            continue
        if section != "clickhouse" or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        try:
            clickhouse[key.strip()] = parse_toml_scalar(raw_value)
        except Exception as exc:
            raise RuntimeError(f"failed parsing [clickhouse] line {line_no}: {exc}") from exc

    return {
        "url": str(clickhouse.get("url", "http://127.0.0.1:8123")),
        "database": str(clickhouse.get("database", "moraine")),
        "username": str(clickhouse.get("username", "default")),
        "password": str(clickhouse.get("password", "")),
        "timeout_seconds": float(clickhouse.get("timeout_seconds", 30.0)),
    }


def clickhouse_select_json_each_row(ch_cfg: dict[str, Any], sql: str) -> list[dict[str, Any]]:
    query = urlencode({"query": sql})
    url = f"{ch_cfg['url'].rstrip('/')}?{query}"
    request = Request(url)
    username = ch_cfg.get("username") or "default"
    password = ch_cfg.get("password") or ""
    if username != "default" or password:
        token = (f"{username}:{password}").encode("utf-8")
        import base64

        request.add_header("Authorization", f"Basic {base64.b64encode(token).decode('ascii')}")

    timeout = float(ch_cfg.get("timeout_seconds", 30.0))
    with urlopen(request, timeout=timeout) as response:
        body = response.read().decode("utf-8")

    rows: list[dict[str, Any]] = []
    for line in body.splitlines():
        line = line.strip()
        if line:
            parsed = json.loads(line)
            if isinstance(parsed, dict):
                rows.append(parsed)
    return rows


def parse_window_interval(value: str) -> str:
    match = WINDOW_RE.match(value)
    if not match:
        raise ValueError("invalid --window, expected format like 24h, 7d, 30m")
    amount = int(match.group(1))
    if amount <= 0:
        raise ValueError("window amount must be > 0")
    unit = {
        "s": "SECOND",
        "m": "MINUTE",
        "h": "HOUR",
        "d": "DAY",
        "w": "WEEK",
    }[match.group(2).lower()]
    return f"INTERVAL {amount} {unit}"


def corpus_counts(ch_cfg: dict[str, Any]) -> dict[str, int]:
    database = ch_cfg["database"]
    sql = f"""
SELECT metric, value FROM (
  SELECT 'events' AS metric, count() AS value FROM {database}.events
  UNION ALL SELECT 'search_documents', count() FROM {database}.search_documents
  UNION ALL SELECT 'search_postings', count() FROM {database}.search_postings
  UNION ALL SELECT 'sessions', uniqExact(session_id) FROM {database}.events
  UNION ALL SELECT 'turns', uniqExact(tuple(session_id, turn_index)) FROM {database}.events
  UNION ALL SELECT 'search_query_log', count() FROM {database}.search_query_log
) ORDER BY metric FORMAT JSONEachRow
""".strip()
    return {
        str(row["metric"]): int(row["value"])
        for row in clickhouse_select_json_each_row(ch_cfg, sql)
    }


def select_log_queries(ch_cfg: dict[str, Any], window: str, top_n: int) -> list[dict[str, Any]]:
    if top_n <= 0:
        return []
    interval = parse_window_interval(window)
    database = ch_cfg["database"]
    sql = f"""
SELECT
  raw_query,
  max(response_ms) AS baseline_max_ms,
  quantileExact(0.95)(response_ms) AS baseline_p95_ms,
  max(result_count) AS baseline_result_count,
  count() AS samples
FROM {database}.search_query_log
WHERE ts >= now() - {interval}
  AND source != '{BENCHMARK_REPLAY_SOURCE}'
  AND length(trim(raw_query)) > 0
GROUP BY raw_query
ORDER BY baseline_max_ms DESC
LIMIT {int(top_n)}
FORMAT JSONEachRow
""".strip()
    return clickhouse_select_json_each_row(ch_cfg, sql)


def collect_stderr(proc: subprocess.Popen[str], max_bytes: int = 8192) -> str:
    if proc.stderr is None:
        return ""
    chunks: list[str] = []
    bytes_read = 0
    fd = proc.stderr.fileno()
    while bytes_read < max_bytes:
        ready, _, _ = select.select([proc.stderr], [], [], 0)
        if not ready:
            break
        chunk = os.read(fd, min(4096, max_bytes - bytes_read))
        if not chunk:
            break
        chunks.append(chunk.decode("utf-8", errors="replace"))
        bytes_read += len(chunk)
    return "".join(chunks)


def read_json_line(proc: subprocess.Popen[str], timeout_seconds: int) -> dict[str, Any]:
    if proc.stdout is None:
        raise RuntimeError("MCP stdout pipe is unavailable")
    ready, _, _ = select.select([proc.stdout], [], [], timeout_seconds)
    if not ready:
        raise TimeoutError(f"timed out waiting for MCP response; stderr={collect_stderr(proc)}")
    line = proc.stdout.readline()
    if line == "":
        raise RuntimeError(f"MCP exited unexpectedly; stderr={collect_stderr(proc)}")
    parsed = json.loads(line)
    if not isinstance(parsed, dict):
        raise RuntimeError("MCP returned non-object JSON-RPC payload")
    return parsed


def send_rpc(
    proc: subprocess.Popen[str],
    request_id: int,
    method: str,
    params: Optional[dict[str, Any]],
    timeout_seconds: int,
) -> dict[str, Any]:
    if proc.stdin is None:
        raise RuntimeError("MCP stdin pipe is unavailable")
    payload: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
    if params is not None:
        payload["params"] = params
    proc.stdin.write(json.dumps(payload) + "\n")
    proc.stdin.flush()
    response = read_json_line(proc, timeout_seconds)
    if response.get("id") != request_id:
        raise RuntimeError(f"unexpected JSON-RPC id={response.get('id')} expected={request_id}")
    if "error" in response:
        raise RuntimeError(f"JSON-RPC error: {response['error']}")
    result = response.get("result")
    if not isinstance(result, dict):
        raise RuntimeError("JSON-RPC response missing result object")
    return result


def resolve_service_bin_dir(moraine_bin: str, explicit: Optional[str]) -> Optional[str]:
    if explicit:
        return str(Path(explicit).expanduser().resolve())

    moraine_path = Path(moraine_bin).expanduser().resolve()
    sibling_dir = moraine_path.parent
    if (sibling_dir / "moraine-mcp").exists():
        return str(sibling_dir)

    repo_root = Path(__file__).resolve().parents[2]
    target_debug = repo_root / "target" / "debug"
    if (target_debug / "moraine-mcp").exists():
        return str(target_debug)

    return None


def start_mcp(moraine_bin: str, config: Path, service_bin_dir: Optional[str]) -> subprocess.Popen[str]:
    env = os.environ.copy()
    if service_bin_dir:
        env["MORAINE_SERVICE_BIN_DIR"] = service_bin_dir
    return subprocess.Popen(
        [moraine_bin, "run", "mcp", "--config", str(config)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        env=env,
    )


def stop_mcp(proc: Optional[subprocess.Popen[str]]) -> None:
    if proc is None:
        return
    try:
        if proc.stdin:
            proc.stdin.close()
    except Exception:
        pass
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def measured_tool_call(
    proc: subprocess.Popen[str],
    request_id: int,
    timeout_seconds: int,
    name: str,
    arguments: dict[str, Any],
) -> tuple[int, dict[str, Any]]:
    started_ns = time.perf_counter_ns()
    result = send_rpc(
        proc,
        request_id,
        "tools/call",
        {"name": name, "arguments": arguments},
        timeout_seconds,
    )
    e2e_ms = (time.perf_counter_ns() - started_ns) / 1_000_000.0
    structured = result.get("structuredContent")
    if not isinstance(structured, dict):
        raise RuntimeError(f"{name} response missing structuredContent")
    if bool(result.get("isError")):
        raise RuntimeError(f"{name} returned isError=true: {structured}")
    performance = structured.get("performance")
    if not isinstance(performance, dict):
        raise RuntimeError(f"{name} response missing performance object")
    return request_id + 1, {
        "tool": name,
        "arguments": arguments,
        "e2e_ms": e2e_ms,
        "server_elapsed_ms": performance.get("elapsed_ms"),
        "sla_target_ms": performance.get("sla_target_ms"),
        "met_sla": performance.get("met_sla"),
        "structured": structured,
    }


def percentile(values: list[float], q: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    pos = (len(ordered) - 1) * q
    lower = int(math.floor(pos))
    upper = int(math.ceil(pos))
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (pos - lower)


def summarize(values: list[float]) -> Optional[dict[str, float]]:
    if not values:
        return None
    return {
        "min": min(values),
        "p50": percentile(values, 0.50) or 0.0,
        "p95": percentile(values, 0.95) or 0.0,
        "p99": percentile(values, 0.99) or 0.0,
        "max": max(values),
        "avg": sum(values) / len(values),
    }


def summarize_samples(samples: list[dict[str, Any]]) -> dict[str, Any]:
    e2e = [float(sample["e2e_ms"]) for sample in samples]
    server = [
        float(sample["server_elapsed_ms"])
        for sample in samples
        if isinstance(sample.get("server_elapsed_ms"), (int, float))
    ]
    misses = [
        sample
        for sample in samples
        if sample.get("met_sla") is False
    ]
    return {
        "sample_count": len(samples),
        "e2e_ms": summarize(e2e),
        "server_elapsed_ms": summarize(server),
        "sla_miss_count": len(misses),
    }


def print_stats(label: str, stats: dict[str, Any]) -> None:
    e2e = stats.get("e2e_ms") or {}
    server = stats.get("server_elapsed_ms") or {}
    print(
        f"{label}: samples={stats.get('sample_count', 0)} "
        f"e2e_p50={e2e.get('p50', 0.0):.2f}ms "
        f"e2e_p95={e2e.get('p95', 0.0):.2f}ms "
        f"server_p50={server.get('p50', 0.0):.2f}ms "
        f"server_p95={server.get('p95', 0.0):.2f}ms "
        f"sla_misses={stats.get('sla_miss_count', 0)}"
    )


def first_hit_open_ids(search_payload: dict[str, Any]) -> dict[str, str]:
    results = search_payload.get("data", {}).get("results", [])
    if not isinstance(results, list) or not results:
        return {}
    first = results[0]
    if not isinstance(first, dict):
        return {}
    open_block = first.get("open")
    if not isinstance(open_block, dict):
        return {}
    keys = {
        "event": "event_id",
        "turn": "turn_id",
        "session": "session_id",
    }
    return {
        kind: value
        for kind, key in keys.items()
        if isinstance((value := open_block.get(key)), str) and value
    }


def unique_queries(args: argparse.Namespace, log_rows: list[dict[str, Any]]) -> list[str]:
    queries: list[str] = []
    seen: set[str] = set()
    for query in list(args.query) + [str(row.get("raw_query", "")) for row in log_rows]:
        compact = " ".join(query.split())
        lowered = compact.lower()
        if compact and lowered not in seen:
            seen.add(lowered)
            queries.append(compact)
    return queries


def main() -> int:
    args = parse_args()
    config = Path(args.config).expanduser().resolve()

    try:
        ch_cfg = read_clickhouse_config(config)
        counts = corpus_counts(ch_cfg)
        log_rows = select_log_queries(ch_cfg, args.window, args.top_n_log_queries)
        queries = unique_queries(args, log_rows)
    except Exception as exc:
        print(f"fatal: {exc}", file=sys.stderr)
        return 2

    print("Corpus")
    for key in ["events", "sessions", "turns", "search_documents", "search_postings", "search_query_log"]:
        print(f"  {key}: {counts.get(key, 0)}")

    if counts.get("search_documents", 0) < args.min_docs and not args.allow_small_corpus:
        print(
            f"fatal: search_documents={counts.get('search_documents', 0)} below --min-docs={args.min_docs}",
            file=sys.stderr,
        )
        return 2

    if not queries:
        print("fatal: no queries supplied or selected", file=sys.stderr)
        return 2

    print("Queries")
    for idx, query in enumerate(queries, start=1):
        print(f"  {idx}. {query}")

    if args.dry_run:
        return 0

    open_kinds = args.open_kind or ["event", "turn", "session"]
    if "none" in open_kinds:
        open_kinds = []

    proc: Optional[subprocess.Popen[str]] = None
    request_id = 1
    samples: list[dict[str, Any]] = []
    failures: list[str] = []
    service_bin_dir = resolve_service_bin_dir(args.moraine_bin, args.service_bin_dir)

    try:
        proc = start_mcp(args.moraine_bin, config, service_bin_dir)
        init_started_ns = time.perf_counter_ns()
        send_rpc(proc, request_id, "initialize", {}, args.timeout_seconds)
        init_ms = (time.perf_counter_ns() - init_started_ns) / 1_000_000.0
        request_id += 1

        tools_result = send_rpc(proc, request_id, "tools/list", {}, args.timeout_seconds)
        request_id += 1
        tool_names = {
            tool.get("name")
            for tool in tools_result.get("tools", [])
            if isinstance(tool, dict)
        }
        missing_tools = {"search_sessions", "open"} - tool_names
        if missing_tools:
            raise RuntimeError(f"tools/list missing expected tools: {sorted(missing_tools)}")

        print(f"Initialized MCP in {init_ms:.2f}ms")

        for query in queries:
            total_runs = args.warmup + args.repeats
            for run_idx in range(total_runs):
                measured = run_idx >= args.warmup
                search_args: dict[str, Any] = {"query": query, "n_hits": args.n_hits}
                if args.event_type:
                    search_args["event_types"] = args.event_type

                try:
                    request_id, search_sample = measured_tool_call(
                        proc,
                        request_id,
                        args.timeout_seconds,
                        "search_sessions",
                        search_args,
                    )
                    open_ids = first_hit_open_ids(search_sample["structured"])
                    if measured:
                        sample = dict(search_sample)
                        sample.pop("structured", None)
                        sample["query"] = query
                        samples.append(sample)

                    for kind in open_kinds:
                        target_id = open_ids.get(kind)
                        if not target_id:
                            if measured:
                                failures.append(f"query={query!r} produced no {kind} id to open")
                            continue
                        request_id, open_sample = measured_tool_call(
                            proc,
                            request_id,
                            args.timeout_seconds,
                            "open",
                            {"id": target_id},
                        )
                        if measured:
                            sample = dict(open_sample)
                            sample.pop("structured", None)
                            sample["query"] = query
                            sample["open_kind"] = kind
                            samples.append(sample)
                except Exception as exc:
                    if measured:
                        failures.append(f"query={query!r} run={run_idx}: {exc}")

    except Exception as exc:
        failures.append(str(exc))
    finally:
        stop_mcp(proc)

    by_label: dict[str, list[dict[str, Any]]] = {}
    for sample in samples:
        label = sample["tool"]
        if sample["tool"] == "open":
            label = f"open:{sample.get('open_kind', 'unknown')}"
        by_label.setdefault(label, []).append(sample)

    print("Results")
    summaries: dict[str, Any] = {}
    for label in sorted(by_label):
        stats = summarize_samples(by_label[label])
        summaries[label] = stats
        print_stats(label, stats)

    if failures:
        print("Failures")
        for failure in failures[:20]:
            print(f"  {failure}")

    total_sla_misses = sum(summary["sla_miss_count"] for summary in summaries.values())
    payload = {
        "corpus": counts,
        "queries": queries,
        "parameters": {
            "warmup": args.warmup,
            "repeats": args.repeats,
            "n_hits": args.n_hits,
            "open_kinds": open_kinds,
            "min_docs": args.min_docs,
            "moraine_bin": args.moraine_bin,
            "service_bin_dir": service_bin_dir,
        },
        "summaries": summaries,
        "failure_count": len(failures),
        "sla_miss_count": total_sla_misses,
        "samples": samples,
    }

    if args.output_json:
        output_path = Path(args.output_json).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    if failures:
        return 1
    if args.fail_on_sla_miss and total_sla_misses:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
