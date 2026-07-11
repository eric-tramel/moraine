#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import re
import select
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Sequence
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import benchmark_protocol


WINDOW_RE = re.compile(r"^\s*(\d+)\s*([smhdw])\s*$", re.IGNORECASE)
BENCHMARK_REPLAY_SOURCE = "benchmark-replay"

PROFILE_DEFAULTS = {
    "smoke": {"warmup": 0, "repeats": 1, "min_docs": 1},
    "full": {"warmup": 1, "repeats": 5, "min_docs": 100_000},
}


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


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Measure the Moraine MCP v1 search_sessions/open/list_sessions interface over JSON-RPC stdio. "
            "The tool calls are read-only; corpus and query selection use ClickHouse SELECTs only."
        )
    )
    parser.add_argument(
        "--profile",
        choices=sorted(PROFILE_DEFAULTS),
        default="full",
        help="Workload profile. Defaults to full, preserving the historical workload.",
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
        "--oracle-json",
        help=(
            "Owned seed oracle JSON. Required when search or list_sessions is measured; "
            "declares expected query results, open IDs, and list-window results."
        ),
    )
    parser.add_argument(
        "--top-n-log-queries",
        type=non_negative_int,
        default=0,
        help="Also select this many historical queries from search_query_log.",
    )
    parser.add_argument("--window", default="7d", help="Log-query selection window, e.g. 24h, 7d")
    parser.add_argument("--n-hits", type=positive_int, default=10)
    parser.add_argument(
        "--build-profile",
        default="release",
        help="Build profile recorded in the benchmark artifact.",
    )
    parser.add_argument(
        "--target",
        help="Build target recorded in the benchmark artifact (auto-detected when omitted).",
    )
    parser.add_argument(
        "--git-commit",
        help="Exact 40-character lowercase git commit (auto-detected when omitted).",
    )
    git_state = parser.add_mutually_exclusive_group()
    git_state.add_argument("--git-dirty", dest="git_dirty", action="store_true")
    git_state.add_argument("--git-clean", dest="git_dirty", action="store_false")
    parser.set_defaults(git_dirty=None)
    parser.add_argument("--warmup", type=non_negative_int)
    parser.add_argument("--repeats", type=positive_int)
    parser.add_argument("--timeout-seconds", type=positive_int, default=20)
    parser.add_argument(
        "--open-kind",
        action="append",
        choices=["event", "turn", "session", "none"],
        default=[],
        help=(
            "Open ID kind declared by the query oracle. May be repeated. "
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
        "--skip-list-sessions",
        action="store_true",
        help="Do not benchmark list_sessions.",
    )
    parser.add_argument(
        "--list-start-datetime",
        help="Explicit list_sessions start_datetime. Defaults to the corpus minimum session start.",
    )
    parser.add_argument(
        "--list-end-datetime",
        help="Explicit list_sessions end_datetime. Defaults to the corpus maximum session update plus 1 ms.",
    )
    parser.add_argument("--list-limit", type=positive_int, default=20)
    parser.add_argument(
        "--list-mode",
        choices=["web_search", "mcp_internal", "tool_calling", "chat"],
        help="Optional list_sessions mode filter.",
    )
    parser.add_argument(
        "--min-docs",
        type=non_negative_int,
        help="Minimum search_documents rows required for a valid benchmark corpus.",
    )
    parser.add_argument(
        "--allow-small-corpus",
        action="store_true",
        help="Do not fail when search_documents has fewer than --min-docs rows.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Select queries and print corpus only.")
    parser.add_argument(
        "--output-json",
        default="target/bench/mcp-two-tool-sla.json",
        help="Write the moraine-benchmark-v1 artifact to this path.",
    )
    args = parser.parse_args(argv)
    defaults = PROFILE_DEFAULTS[args.profile]
    for name, value in defaults.items():
        if getattr(args, name) is None:
            setattr(args, name, value)
    return args


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

def search_corpus_identity(ch_cfg: dict[str, Any]) -> dict[str, Any]:
    """Return a stable identity for the indexed document rows."""
    database = ch_cfg["database"]
    row_hash = (
        "cityHash64(toJSONString(tuple("
        "event_uid, doc_version, session_id, source_name, harness, record_ts, "
        "event_class, payload_type, actor_role, name, phase, source_ref, "
        "doc_len, text_content, payload_json"
        ")))"
    )
    sql = (
        "SELECT\n"
        "  toUInt64(count()) AS cardinality,\n"
        "  toString(groupBitXor(row_hash)) AS content_xor,\n"
        "  toString(sumWithOverflow(row_hash)) AS content_sum\n"
        "FROM (\n"
        f"  SELECT {row_hash} AS row_hash\n"
        f"  FROM {database}.search_documents FINAL\n"
        ")\n"
        "FORMAT JSONEachRow"
    )
    rows = clickhouse_select_json_each_row(ch_cfg, sql)
    if len(rows) != 1:
        raise RuntimeError("search corpus identity query returned no aggregate row")
    row = rows[0]
    cardinality = int(row["cardinality"])
    if cardinality < 0:
        raise RuntimeError("search corpus cardinality must be non-negative")
    return {
        "cardinality": cardinality,
        "content_xor": str(row["content_xor"]),
        "content_sum": str(row["content_sum"]),
    }


def format_utc_rfc3339_ms(unix_ms: int) -> str:
    return (
        datetime.fromtimestamp(unix_ms / 1000, tz=timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def corpus_session_window(ch_cfg: dict[str, Any]) -> Optional[dict[str, Any]]:
    database = ch_cfg["database"]
    sql = f"""
SELECT
  count() AS sessions,
  toInt64(toUnixTimestamp64Milli(min(first_event_time))) AS start_unix_ms,
  toInt64(toUnixTimestamp64Milli(max(last_event_time))) AS end_unix_ms
FROM {database}.v_session_summary
FORMAT JSONEachRow
""".strip()
    rows = clickhouse_select_json_each_row(ch_cfg, sql)
    if not rows:
        return None
    row = rows[0]
    sessions = int(row.get("sessions", 0))
    if sessions <= 0:
        return None
    start_unix_ms = int(row["start_unix_ms"])
    end_unix_ms = int(row["end_unix_ms"]) + 1
    return {
        "sessions": sessions,
        "start_datetime": format_utc_rfc3339_ms(start_unix_ms),
        "end_datetime": format_utc_rfc3339_ms(end_unix_ms),
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


def finite_non_negative_number(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RuntimeError(f"{field_name} must be numeric")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0:
        raise RuntimeError(f"{field_name} must be finite and non-negative")
    return parsed


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
        raise RuntimeError(f"{name} returned isError=true")
    if "error" in structured:
        raise RuntimeError(f"{name} returned an error envelope")
    performance = structured.get("performance")
    if not isinstance(performance, dict):
        raise RuntimeError(f"{name} response missing performance object")
    server_elapsed_ms = finite_non_negative_number(
        performance.get("elapsed_ms"), f"{name} performance.elapsed_ms"
    )
    sla_target_ms = finite_non_negative_number(
        performance.get("sla_target_ms"), f"{name} performance.sla_target_ms"
    )
    met_sla = performance.get("met_sla")
    if not isinstance(met_sla, bool):
        raise RuntimeError(f"{name} performance.met_sla must be boolean")
    return request_id + 1, {
        "tool": name,
        "arguments": arguments,
        "e2e_ms": e2e_ms,
        "server_elapsed_ms": server_elapsed_ms,
        "sla_target_ms": sla_target_ms,
        "met_sla": met_sla,
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


def structured_data(sample: Any, tool_name: str) -> dict[str, Any]:
    if not isinstance(sample, dict):
        raise RuntimeError(f"{tool_name} sample must be an object")
    structured = sample.get("structured")
    if not isinstance(structured, dict):
        raise RuntimeError(f"{tool_name} structuredContent must be an object")
    data = structured.get("data")
    if not isinstance(data, dict):
        raise RuntimeError(f"{tool_name} structuredContent.data must be an object")
    return data


class OracleConfigurationError(RuntimeError):
    def __init__(self, diagnostic_code: str, message: str) -> None:
        super().__init__(message)
        self.diagnostic_code = diagnostic_code


def normalize_query(value: str) -> str:
    return " ".join(value.split()).casefold()


def load_benchmark_oracles(
    path_value: Optional[str],
    queries: list[str],
    open_kinds: list[str],
    require_list_sessions: bool,
) -> tuple[dict[str, dict[str, Any]], Optional[dict[str, Any]]]:
    if not queries and not require_list_sessions:
        return {}, None
    if not path_value:
        code = "missing-search-oracle" if queries else "missing-list-oracle"
        raise OracleConfigurationError(
            code, "--oracle-json is required when search or list_sessions is measured"
        )

    path = Path(path_value).expanduser().resolve()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OracleConfigurationError(
            "invalid-benchmark-oracle", f"cannot read benchmark oracle {path}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise OracleConfigurationError(
            "invalid-benchmark-oracle", "benchmark oracle root must be an object"
        )
    if payload.get("schema_version") != "moraine-mcp-two-tool-oracle-v1":
        raise OracleConfigurationError(
            "invalid-benchmark-oracle",
            "benchmark oracle schema_version must be 'moraine-mcp-two-tool-oracle-v1'",
        )
    entries = payload.get("queries")
    if not isinstance(entries, list):
        raise OracleConfigurationError(
            "invalid-benchmark-oracle", "benchmark oracle queries must be an array"
        )

    by_query: dict[str, dict[str, Any]] = {}
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise RuntimeError(f"query oracle entry {index} must be an object")
        raw_query = entry.get("query")
        if not isinstance(raw_query, str) or not normalize_query(raw_query):
            raise RuntimeError(f"query oracle entry {index} must have a non-empty query")
        query_key = normalize_query(raw_query)
        if query_key in by_query:
            raise RuntimeError(f"query oracle contains duplicate query at entry {index}")

        expected = entry.get("expected")
        if not isinstance(expected, dict):
            raise RuntimeError(f"query oracle entry {index} expected must be an object")
        result_count = expected.get("result_count")
        if result_count is not None and (
            isinstance(result_count, bool)
            or not isinstance(result_count, int)
            or result_count < 0
        ):
            raise RuntimeError(
                f"query oracle entry {index} expected.result_count must be a non-negative integer"
            )
        open_ids = expected.get("open_ids", {})
        if not isinstance(open_ids, dict):
            raise RuntimeError(f"query oracle entry {index} expected.open_ids must be an object")
        unknown_kinds = set(open_ids) - {"event", "turn", "session"}
        if unknown_kinds:
            raise RuntimeError(
                f"query oracle entry {index} has unknown open ID kinds: {sorted(unknown_kinds)}"
            )
        for kind, entity_id in open_ids.items():
            if not isinstance(entity_id, str) or not entity_id:
                raise RuntimeError(
                    f"query oracle entry {index} expected.open_ids.{kind} must be non-empty"
                )
        marker = expected.get("result_marker")
        if marker is not None and (not isinstance(marker, dict) or not marker):
            raise RuntimeError(
                f"query oracle entry {index} expected.result_marker must be a non-empty object"
            )
        if result_count is None and not open_ids and marker is None:
            raise RuntimeError(
                f"query oracle entry {index} must declare result_count, open_ids, or result_marker"
            )
        missing_kinds = [kind for kind in open_kinds if kind not in open_ids]
        if missing_kinds:
            raise RuntimeError(
                f"query oracle entry {index} lacks open IDs for kinds: {missing_kinds}"
            )
        by_query[query_key] = {
            "result_count": result_count,
            "open_ids": dict(open_ids),
            "result_marker": marker,
        }

    missing_queries = [query for query in queries if normalize_query(query) not in by_query]
    if missing_queries:
        raise OracleConfigurationError(
            "missing-search-oracle",
            f"query oracle has no entry for {len(missing_queries)} selected quer"
            f"{'y' if len(missing_queries) == 1 else 'ies'}",
        )
    query_oracles = {query: by_query[normalize_query(query)] for query in queries}

    if not require_list_sessions:
        return query_oracles, None
    list_oracle = payload.get("list_sessions")
    if not isinstance(list_oracle, dict):
        raise OracleConfigurationError(
            "missing-list-oracle",
            "benchmark oracle must declare list_sessions for the measured window",
        )
    result_count = list_oracle.get("result_count")
    if result_count is not None and (
        isinstance(result_count, bool)
        or not isinstance(result_count, int)
        or result_count < 0
    ):
        raise OracleConfigurationError(
            "invalid-benchmark-oracle",
            "list_sessions.result_count must be a non-negative integer",
        )
    session_ids = list_oracle.get("session_ids", [])
    if (
        not isinstance(session_ids, list)
        or any(not isinstance(value, str) or not value for value in session_ids)
        or len(session_ids) != len(set(session_ids))
    ):
        raise OracleConfigurationError(
            "invalid-benchmark-oracle",
            "list_sessions.session_ids must be an array of unique non-empty strings",
        )
    marker = list_oracle.get("result_marker")
    if marker is not None and (not isinstance(marker, dict) or not marker):
        raise OracleConfigurationError(
            "invalid-benchmark-oracle",
            "list_sessions.result_marker must be a non-empty object",
        )
    if result_count is None and not session_ids and marker is None:
        raise OracleConfigurationError(
            "missing-list-oracle",
            "list_sessions must declare result_count, session_ids, or result_marker",
        )
    return query_oracles, {
        "result_count": result_count,
        "session_ids": list(session_ids),
        "result_marker": marker,
    }


def contains_marker(value: Any, marker: Any) -> bool:
    if isinstance(marker, dict):
        return isinstance(value, dict) and all(
            key in value and contains_marker(value[key], expected)
            for key, expected in marker.items()
        )
    if isinstance(marker, list):
        return (
            isinstance(value, list)
            and len(value) == len(marker)
            and all(contains_marker(actual, expected) for actual, expected in zip(value, marker))
        )
    return value == marker


def validate_search_oracle(
    search_data: dict[str, Any],
    oracle: dict[str, Any],
) -> None:
    results = search_data.get("results")
    if not isinstance(results, list):
        raise RuntimeError("search_sessions data.results must be an array")

    result_count = oracle["result_count"]
    if result_count is not None and len(results) != result_count:
        raise RuntimeError(
            f"search_sessions returned {len(results)} results, expected {result_count}"
        )

    open_ids = oracle["open_ids"]
    marker = oracle["result_marker"]
    if open_ids or marker is not None:
        matched = False
        for result in results:
            if not isinstance(result, dict):
                continue
            open_block = result.get("open")
            ids_match = not open_ids or (
                isinstance(open_block, dict)
                and all(
                    open_block.get(f"{kind}_id") == entity_id
                    for kind, entity_id in open_ids.items()
                )
            )
            marker_matches = marker is None or contains_marker(result, marker)
            if ids_match and marker_matches:
                matched = True
                break
        if not matched:
            raise RuntimeError("search_sessions did not return the oracle-selected result")


def validate_list_oracle(
    list_data: dict[str, Any],
    oracle: dict[str, Any],
) -> None:
    sessions = list_data.get("sessions")
    if not isinstance(sessions, list):
        raise RuntimeError("list_sessions data.sessions must be an array")

    result_count = oracle["result_count"]
    if result_count is not None and len(sessions) != result_count:
        raise RuntimeError(
            f"list_sessions returned {len(sessions)} sessions, expected {result_count}"
        )

    expected_ids = set(oracle["session_ids"])
    if expected_ids:
        observed_ids = {
            open_block["session_id"]
            for session in sessions
            if isinstance(session, dict)
            and isinstance((open_block := session.get("open")), dict)
            and isinstance(open_block.get("session_id"), str)
        }
        missing_ids = expected_ids - observed_ids
        if missing_ids:
            raise RuntimeError(
                f"list_sessions omitted {len(missing_ids)} oracle session IDs"
            )

    marker = oracle["result_marker"]
    if marker is not None and not any(
        isinstance(session, dict) and contains_marker(session, marker)
        for session in sessions
    ):
        raise RuntimeError("list_sessions did not return the oracle semantic marker")


def validate_open_oracle(
    open_sample: Any,
    expected_kind: str,
    expected_id: str,
) -> None:
    data = structured_data(open_sample, "open")
    if data.get("kind") != expected_kind:
        raise RuntimeError(
            f"open returned kind={data.get('kind')!r}, expected {expected_kind!r}"
        )
    structured = open_sample["structured"]
    request = structured.get("request")
    if not isinstance(request, dict) or request.get("id") != expected_id:
        returned_id = request.get("id") if isinstance(request, dict) else None
        raise RuntimeError(
            f"open returned request.id={returned_id!r}, expected {expected_id!r}"
        )
    entity = data.get(expected_kind)
    if not isinstance(entity, dict) or entity.get("id") != expected_id:
        returned_id = entity.get("id") if isinstance(entity, dict) else None
        raise RuntimeError(
            f"open returned data.{expected_kind}.id={returned_id!r}, "
            f"expected {expected_id!r}"
        )


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


def sha256_json(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def git_source(
    explicit_commit: Optional[str],
    explicit_dirty: Optional[bool],
) -> dict[str, Any]:
    repo_root = Path(__file__).resolve().parents[2]
    commit = explicit_commit
    dirty = explicit_dirty
    if commit is None:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
        commit = completed.stdout.strip()
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise RuntimeError("git commit must be exactly 40 lowercase hexadecimal characters")
    if dirty is None:
        completed = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=normal"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
        dirty = bool(completed.stdout)
    return {"git_commit": commit, "dirty": dirty}


def runner_identity() -> dict[str, str]:
    os_name = platform.system().lower()
    machine = platform.machine().lower()
    if os_name == "darwin" and machine in {"arm64", "aarch64"}:
        cpu_class = "apple-silicon"
    elif machine in {"x86_64", "amd64"}:
        cpu_class = "x86-64"
    elif machine in {"arm64", "aarch64"}:
        cpu_class = "arm64"
    else:
        cpu_class = re.sub(r"[^a-z0-9._-]+", "-", machine).strip("-") or "other"
    return {"os": os_name, "cpu_class": cpu_class}


def detected_target() -> str:
    os_name = platform.system().lower()
    machine = platform.machine().lower()
    arch = {
        "arm64": "aarch64",
        "aarch64": "aarch64",
        "amd64": "x86_64",
        "x86_64": "x86_64",
    }.get(machine, re.sub(r"[^a-z0-9._-]+", "-", machine).strip("-") or "unknown")
    if os_name == "darwin":
        return f"{arch}-apple-darwin"
    if os_name == "linux":
        return f"{arch}-unknown-linux-gnu"
    return f"{arch}-{re.sub(r'[^a-z0-9._-]+', '-', os_name).strip('-') or 'unknown'}"


def planned_sample_count(
    repeats: int,
    queries: list[str],
    open_kinds: list[str],
    list_sessions_args: Optional[dict[str, Any]],
) -> int:
    per_repeat = len(queries) * (1 + len(open_kinds))
    if list_sessions_args is not None:
        per_repeat += 1
    return repeats * per_repeat


def build_artifact(
    *,
    args: argparse.Namespace,
    counts: dict[str, int],
    corpus_identity: dict[str, Any],
    queries: list[str],
    open_kinds: list[str],
    list_sessions_args: Optional[dict[str, Any]],
    samples: list[dict[str, Any]],
    planned: int,
    attempted: int,
    errors: int,
    initialization_ms: Optional[float],
    diagnostic_codes: list[str],
    oracle_mismatches: int,
    warmup_failures: int,
) -> dict[str, Any]:
    if planned <= 0:
        raise RuntimeError("benchmark workload planned zero measured samples")
    if attempted != len(samples) + errors:
        raise RuntimeError("attempted sample count does not equal successful plus errors")
    if attempted > planned:
        raise RuntimeError("attempted sample count exceeds planned samples")

    workload_shape = {
        "profile": args.profile,
        "query_hashes": [hashlib.sha256(query.encode("utf-8")).hexdigest() for query in queries],
        "open_kinds": open_kinds,
        "list_sessions": list_sessions_args,
        "event_types": sorted(args.event_type),
        "n_hits": args.n_hits,
        "warmup": args.warmup,
        "repeats": args.repeats,
    }
    dataset_shape = {
        "cardinality": corpus_identity["cardinality"],
        "content_sum": corpus_identity["content_sum"],
        "content_xor": corpus_identity["content_xor"],
    }
    sla_successes = sum(sample["met_sla"] is True for sample in samples)
    sla_failures = len(samples) - sla_successes
    insufficient = len(samples) < planned
    oracle_failed = bool(oracle_mismatches)
    semantic_failed = bool(errors or oracle_failed or insufficient or warmup_failures)
    if insufficient and "insufficient-samples" not in diagnostic_codes:
        diagnostic_codes.append("insufficient-samples")

    quality_failed_checks = oracle_mismatches + sla_failures
    artifact: dict[str, Any] = {
        "schema_version": "moraine-benchmark-v1",
        "benchmark_id": "mcp-two-tool-sla",
        "scenario_id": "production-mcp-stdio-search-open-list",
        "source": git_source(args.git_commit, args.git_dirty),
        "build": {
            "profile": args.build_profile,
            "target": args.target or detected_target(),
        },
        "runner": runner_identity(),
        "scenario": {
            "profile": args.profile,
            "workload_id": f"two-tool-{sha256_json(workload_shape)[:24]}",
            "measured_boundary": "json-rpc-stdio-tool-call",
            "dimensions": {
                "dataset_backed": True,
                "cache_sensitive": True,
                "concurrent": False,
                "request_producing": True,
            },
            "fingerprints": {
                "dataset": {
                    "fingerprint": f"sha256:{sha256_json(dataset_shape)}",
                    "cardinality": corpus_identity["cardinality"],
                },
                "cache_state": (
                    "warm" if args.warmup > 0 and warmup_failures == 0 else "mixed"
                ),
                "request_source": "production-mcp",
            },
        },
        "samples": {
            "planned": planned,
            "attempted": attempted,
            "successful": len(samples),
            "errors": errors,
            "measurements": {
                "latency_ms": [float(sample["e2e_ms"]) for sample in samples],
                "server_elapsed_ms": [
                    float(sample["server_elapsed_ms"]) for sample in samples
                ],
                "sla_target_ms": [float(sample["sla_target_ms"]) for sample in samples],
            },
        },
        "semantic": {"status": "fail" if semantic_failed else "pass"},
        "timing": {"status": "not_evaluated", "non_blocking": True},
        "metrics": {
            "quality": {
                "oracle_status": "fail" if oracle_failed else "pass",
                "passed_checks": sla_successes,
                "failed_checks": quality_failed_checks,
                "expected_count": planned,
                "observed_count": len(samples),
                "mismatches": oracle_mismatches,
                "success_rate": len(samples) / attempted if attempted else 0.0,
                "error_rate": errors / attempted if attempted else 0.0,
            },
            "storage": {
                f"corpus_{name}_count": value for name, value in sorted(counts.items())
            },
        },
        "diagnostics": [{"code": code} for code in sorted(set(diagnostic_codes))],
        "artifacts": [],
    }
    if initialization_ms is not None:
        artifact["metrics"]["resources"] = {"mcp_initialization_ms": initialization_ms}
    return artifact




def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    config = Path(args.config).expanduser().resolve()

    try:
        ch_cfg = read_clickhouse_config(config)
        counts = corpus_counts(ch_cfg)
        corpus_identity = search_corpus_identity(ch_cfg)
        session_window = corpus_session_window(ch_cfg) if not args.skip_list_sessions else None
        log_rows = select_log_queries(ch_cfg, args.window, args.top_n_log_queries)
        queries = unique_queries(args, log_rows)
    except Exception as exc:
        print(f"fatal: prerequisite discovery failed: {exc}", file=sys.stderr)
        return 2

    print("Corpus")
    for key in ["events", "sessions", "turns", "search_documents", "search_postings", "search_query_log"]:
        print(f"  {key}: {counts.get(key, 0)}")

    searchable_documents = corpus_identity["cardinality"]
    if searchable_documents < args.min_docs and not args.allow_small_corpus:
        print(
            f"fatal: search_documents={searchable_documents} below --min-docs={args.min_docs}",
            file=sys.stderr,
        )
        return 2

    list_sessions_args: Optional[dict[str, Any]] = None
    if not args.skip_list_sessions:
        if bool(args.list_start_datetime) != bool(args.list_end_datetime):
            print(
                "fatal: --list-start-datetime and --list-end-datetime must be provided together",
                file=sys.stderr,
            )
            return 2
        if args.list_start_datetime and args.list_end_datetime:
            list_sessions_args = {
                "start_datetime": args.list_start_datetime,
                "end_datetime": args.list_end_datetime,
                "limit": args.list_limit,
            }
        elif session_window is not None:
            list_sessions_args = {
                "start_datetime": session_window["start_datetime"],
                "end_datetime": session_window["end_datetime"],
                "limit": args.list_limit,
            }
        else:
            print("warning: no session window available; skipping list_sessions benchmark")
        if list_sessions_args is not None and args.list_mode:
            list_sessions_args["mode"] = args.list_mode

    if list_sessions_args is not None:
        print("list_sessions")
        mode_suffix = f" mode={args.list_mode}" if args.list_mode else ""
        print(
            "  "
            f"{list_sessions_args['start_datetime']} -> {list_sessions_args['end_datetime']} "
            f"limit={list_sessions_args['limit']}{mode_suffix}"
        )

    if not queries and list_sessions_args is None:
        print("fatal: no queries supplied or selected and no list_sessions window", file=sys.stderr)
        return 2

    if queries:
        print("Queries")
        for idx, query in enumerate(queries, start=1):
            print(f"  {idx}. {query}")

    if args.dry_run:
        return 0

    open_kinds = args.open_kind or ["event", "turn", "session"]
    if "none" in open_kinds:
        open_kinds = []
    planned = planned_sample_count(args.repeats, queries, open_kinds, list_sessions_args)
    if planned == 0:
        print("fatal: selected workload plans zero measured samples", file=sys.stderr)
        return 2

    try:
        source = git_source(args.git_commit, args.git_dirty)
    except Exception as exc:
        print(f"fatal: benchmark provenance unavailable: {exc}", file=sys.stderr)
        return 2
    args.git_commit = source["git_commit"]
    args.git_dirty = source["dirty"]

    proc: Optional[subprocess.Popen[str]] = None
    request_id = 1
    samples: list[dict[str, Any]] = []
    failures: list[str] = []
    diagnostic_codes: list[str] = []
    attempted = 0
    errors = 0
    oracle_mismatches = 0
    warmup_failures = 0
    init_ms: Optional[float] = None
    try:
        query_oracles, list_sessions_oracle = load_benchmark_oracles(
            args.oracle_json,
            queries,
            open_kinds,
            list_sessions_args is not None,
        )
    except Exception as exc:
        diagnostic_codes.append(
            getattr(exc, "diagnostic_code", "invalid-benchmark-oracle")
        )
        oracle_mismatches += 1
        failures.append(f"benchmark oracle prerequisite failed: {exc}")
        try:
            payload = build_artifact(
                args=args,
                counts=counts,
                corpus_identity=corpus_identity,
                queries=queries,
                open_kinds=open_kinds,
                list_sessions_args=list_sessions_args,
                samples=samples,
                planned=planned,
                attempted=attempted,
                errors=errors,
                initialization_ms=init_ms,
                diagnostic_codes=diagnostic_codes,
                oracle_mismatches=oracle_mismatches,
                warmup_failures=warmup_failures,
            )
            benchmark_protocol.write_artifact(Path(args.output_json).expanduser(), payload)
        except Exception as artifact_exc:
            print(
                f"fatal: benchmark artifact validation/write failed: {artifact_exc}",
                file=sys.stderr,
            )
            return 2
        print(f"fatal: {failures[-1]}", file=sys.stderr)
        return 1
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
        expected_tools = {"search_sessions", "open"}
        if list_sessions_args is not None:
            expected_tools.add("list_sessions")
        missing_tools = expected_tools - tool_names
        if missing_tools:
            raise RuntimeError(f"tools/list missing expected tools: {sorted(missing_tools)}")

        print(f"Initialized MCP in {init_ms:.2f}ms")

        for query_index, query in enumerate(queries, start=1):
            total_runs = args.warmup + args.repeats
            for run_idx in range(total_runs):
                measured = run_idx >= args.warmup
                search_args: dict[str, Any] = {"query": query, "n_hits": args.n_hits}
                if args.event_type:
                    search_args["event_types"] = args.event_type

                if measured:
                    attempted += 1
                try:
                    request_id, search_sample = measured_tool_call(
                        proc,
                        request_id,
                        args.timeout_seconds,
                        "search_sessions",
                        search_args,
                    )
                except Exception as exc:
                    if measured:
                        errors += 1
                        diagnostic_codes.append("tool-call-error")
                    else:
                        warmup_failures += 1
                        diagnostic_codes.append("warmup-error")
                    failures.append(
                        f"query_index={query_index} run={run_idx} search_sessions: {exc}"
                    )
                    continue
                try:
                    search_data = structured_data(search_sample, "search_sessions")
                except Exception as exc:
                    if measured:
                        errors += 1
                        diagnostic_codes.append("malformed-tool-data")
                    else:
                        warmup_failures += 1
                        diagnostic_codes.extend(["malformed-tool-data", "warmup-error"])
                    failures.append(
                        f"query_index={query_index} run={run_idx} search_sessions: {exc}"
                    )
                    continue

                oracle = query_oracles[query]
                try:
                    validate_search_oracle(search_data, oracle)
                except Exception as exc:
                    if measured:
                        oracle_mismatches += 1
                    else:
                        warmup_failures += 1
                        diagnostic_codes.append("warmup-error")
                    diagnostic_codes.append("search-oracle-mismatch")
                    failures.append(
                        f"query_index={query_index} run={run_idx} search_sessions: {exc}"
                    )
                if measured:
                    sample = dict(search_sample)
                    sample.pop("structured", None)
                    samples.append(sample)

                for kind in open_kinds:
                    target_id = oracle["open_ids"][kind]
                    if measured:
                        attempted += 1
                    try:
                        request_id, open_sample = measured_tool_call(
                            proc,
                            request_id,
                            args.timeout_seconds,
                            "open",
                            {"id": target_id},
                        )
                    except Exception as exc:
                        if measured:
                            errors += 1
                            diagnostic_codes.append("tool-call-error")
                        else:
                            warmup_failures += 1
                            diagnostic_codes.append("warmup-error")
                        failures.append(
                            f"query_index={query_index} run={run_idx} open:{kind}: {exc}"
                        )
                        continue
                    try:
                        validate_open_oracle(open_sample, kind, target_id)
                    except Exception as exc:
                        if measured:
                            errors += 1
                            oracle_mismatches += 1
                        else:
                            warmup_failures += 1
                            diagnostic_codes.append("warmup-error")
                        diagnostic_codes.append("open-oracle-mismatch")
                        failures.append(
                            f"query_index={query_index} run={run_idx} open:{kind}: {exc}"
                        )
                        continue
                    if measured:
                        sample = dict(open_sample)
                        sample.pop("structured", None)
                        sample["open_kind"] = kind
                        samples.append(sample)

        if list_sessions_args is not None:
            total_runs = args.warmup + args.repeats
            for run_idx in range(total_runs):
                measured = run_idx >= args.warmup
                if measured:
                    attempted += 1
                try:
                    request_id, list_sample = measured_tool_call(
                        proc,
                        request_id,
                        args.timeout_seconds,
                        "list_sessions",
                        dict(list_sessions_args),
                    )
                except Exception as exc:
                    if measured:
                        errors += 1
                        diagnostic_codes.append("tool-call-error")
                    else:
                        warmup_failures += 1
                        diagnostic_codes.append("warmup-error")
                    failures.append(f"list_sessions run={run_idx}: {exc}")
                    continue
                try:
                    list_data = structured_data(list_sample, "list_sessions")
                except Exception as exc:
                    if measured:
                        errors += 1
                        diagnostic_codes.append("malformed-tool-data")
                    else:
                        warmup_failures += 1
                        diagnostic_codes.extend(["malformed-tool-data", "warmup-error"])
                    failures.append(f"list_sessions run={run_idx}: {exc}")
                    continue
                try:
                    validate_list_oracle(list_data, list_sessions_oracle)
                except Exception as exc:
                    if measured:
                        oracle_mismatches += 1
                    else:
                        warmup_failures += 1
                        diagnostic_codes.append("warmup-error")
                    diagnostic_codes.append("list-oracle-mismatch")
                    failures.append(f"list_sessions run={run_idx}: {exc}")
                if measured:
                    sample = dict(list_sample)
                    sample.pop("structured", None)
                    samples.append(sample)

    except Exception as exc:
        diagnostic_codes.append("mcp-prerequisite-error")
        failures.append(f"MCP prerequisite failed: {exc}")
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

    try:
        payload = build_artifact(
            args=args,
            counts=counts,
            corpus_identity=corpus_identity,
            queries=queries,
            open_kinds=open_kinds,
            list_sessions_args=list_sessions_args,
            samples=samples,
            planned=planned,
            attempted=attempted,
            errors=errors,
            initialization_ms=init_ms,
            diagnostic_codes=diagnostic_codes,
            oracle_mismatches=oracle_mismatches,
            warmup_failures=warmup_failures,
        )
        benchmark_protocol.write_artifact(Path(args.output_json).expanduser(), payload)
    except Exception as exc:
        print(f"fatal: benchmark artifact validation/write failed: {exc}", file=sys.stderr)
        return 2

    if payload["semantic"]["status"] == "fail":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
