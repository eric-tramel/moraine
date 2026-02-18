#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# ///
from __future__ import annotations

import argparse
import base64
import json
import math
import os
import random
import re
import select
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

WINDOW_RE = re.compile(r"^\s*(\d+)\s*([smhdw])\s*$", re.IGNORECASE)
SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
BENCHMARK_REPLAY_SOURCE = "benchmark-replay"
QUERY_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")
PROSE_TOOK_RE = re.compile(r"\((\d+)\s*ms\)")


@dataclass
class ClickHouseSettings:
    url: str
    database: str
    username: str
    password: str
    timeout_seconds: float


@dataclass
class ReplaySpec:
    rank: int
    variant_label: str
    variant_term_count: int
    ts: str
    query_id: str
    source: str
    session_hint: str
    raw_query: str
    baseline_response_ms: float
    arguments: dict[str, Any]


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
            "Replay top-N search queries against moraine-mcp over JSON-RPC stdio and "
            "report end-to-end + server timing attribution for persistent and cold-process modes."
        )
    )
    parser.add_argument("--config", required=True, help="Path to moraine.toml")
    parser.add_argument("--window", default="24h", help="Selection window (e.g. 24h, 7d)")
    parser.add_argument("--top-n", type=positive_int, default=20, help="Top N rows by response_ms")
    parser.add_argument("--warmup", type=non_negative_int, default=1, help="Warmup runs per query")
    parser.add_argument(
        "--repeats", type=positive_int, default=5, help="Measured replay runs per query"
    )
    parser.add_argument(
        "--timeout-seconds",
        type=positive_int,
        default=20,
        help="Timeout per initialize/tools call (seconds)",
    )
    parser.add_argument(
        "--include-benchmark-replays",
        action="store_true",
        help=f"Include rows with source='{BENCHMARK_REPLAY_SOURCE}' in top-N selection",
    )
    parser.add_argument(
        "--query-variant-mode",
        choices=["none", "subset_scramble"],
        default="subset_scramble",
        help=(
            "How replay queries are expanded before benchmarking. "
            "'subset_scramble' runs one deterministic scrambled variant for each k=1..N terms."
        ),
    )
    parser.add_argument(
        "--max-query-terms",
        type=positive_int,
        default=32,
        help="Max normalized query terms to consider for subset/scramble expansion",
    )
    parser.add_argument(
        "--mode",
        choices=["persistent", "cold_process"],
        default="persistent",
        help="Benchmark mode: one long-lived MCP process vs one fresh process per sample",
    )
    parser.add_argument(
        "--tool",
        choices=["search", "search_conversations", "both"],
        default="both",
        help="MCP tool workload to replay",
    )
    parser.add_argument(
        "--verbosity",
        choices=["full", "prose"],
        default="full",
        help="MCP tool verbosity to request",
    )
    parser.add_argument(
        "--moraine-bin",
        default="bin/moraine",
        help="Path to moraine CLI binary/script used to launch MCP",
    )
    parser.add_argument(
        "--post-init-wait-ms",
        type=non_negative_int,
        default=0,
        help=(
            "Optional wait after initialize and before tool call; useful to model prewarm settling"
        ),
    )
    parser.add_argument("--output-json", help="Write machine-readable results JSON to this path")
    parser.add_argument("--print-sql", action="store_true", help="Print selection SQL before execution")
    parser.add_argument("--dry-run", action="store_true", help="Select rows but skip replay")
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
    if stripped == "":
        raise ValueError("empty value")
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


def parse_clickhouse_section(path: Path) -> dict[str, Any]:
    section = ""
    result: dict[str, Any] = {}
    for line_no, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1].strip()
            continue
        if section != "clickhouse":
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key == "":
            continue
        try:
            result[key] = parse_toml_scalar(value)
        except Exception as exc:
            raise RuntimeError(
                f"failed parsing [clickhouse] value for {key!r} at line {line_no}: {exc}"
            ) from exc
    return result


def read_config(path: Path) -> ClickHouseSettings:
    if not path.exists():
        raise RuntimeError(f"config not found: {path}")
    clickhouse = parse_clickhouse_section(path)
    url = str(clickhouse.get("url", "http://127.0.0.1:8123"))
    database = str(clickhouse.get("database", "moraine"))
    username = str(clickhouse.get("username", "default"))
    password = str(clickhouse.get("password", ""))
    timeout_seconds = float(clickhouse.get("timeout_seconds", 30.0))
    timeout_seconds = max(timeout_seconds, 1.0)

    if not SAFE_IDENTIFIER_RE.match(database):
        raise RuntimeError(
            f"unsupported clickhouse database identifier: {database!r} "
            "(only [A-Za-z_][A-Za-z0-9_]* allowed)"
        )

    return ClickHouseSettings(
        url=url,
        database=database,
        username=username,
        password=password,
        timeout_seconds=timeout_seconds,
    )


def git_sha() -> str:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
        return proc.stdout.strip()
    except Exception:
        return "unknown"


def parse_window_interval(value: str) -> str:
    match = WINDOW_RE.match(value)
    if not match:
        raise ValueError("invalid --window, expected format like 24h, 7d, 30m")
    amount = int(match.group(1))
    if amount <= 0:
        raise ValueError("window amount must be > 0")
    unit = match.group(2).lower()
    unit_map = {
        "s": "SECOND",
        "m": "MINUTE",
        "h": "HOUR",
        "d": "DAY",
        "w": "WEEK",
    }
    return f"INTERVAL {amount} {unit_map[unit]}"


def build_selection_sql(
    database: str,
    interval_expr: str,
    top_n: int,
    include_benchmark_replays: bool,
) -> str:
    source_filter_sql = ""
    if not include_benchmark_replays:
        source_filter_sql = f"  AND source != '{BENCHMARK_REPLAY_SOURCE}'\n"

    return (
        "SELECT\n"
        "  toString(ts) AS selected_ts,\n"
        "  query_id,\n"
        "  source,\n"
        "  session_hint,\n"
        "  raw_query,\n"
        "  result_limit,\n"
        "  min_should_match,\n"
        "  min_score,\n"
        "  include_tool_events,\n"
        "  exclude_codex_mcp,\n"
        "  response_ms\n"
        f"FROM {database}.search_query_log\n"
        f"WHERE ts >= now() - {interval_expr}\n"
        "  AND length(trim(BOTH ' ' FROM raw_query)) > 0\n"
        f"{source_filter_sql}"
        "ORDER BY response_ms DESC\n"
        f"LIMIT {top_n}\n"
        "FORMAT JSONEachRow"
    )


def clickhouse_query_json_each_row(cfg: ClickHouseSettings, query: str) -> list[dict[str, Any]]:
    parsed = urlparse(cfg.url)
    pairs = parse_qsl(parsed.query, keep_blank_values=True)
    pairs.append(("query", query))
    pairs.append(("database", cfg.database))
    url = urlunparse(parsed._replace(query=urlencode(pairs)))

    request = Request(url=url, data=b"", method="POST")
    request.add_header("Content-Type", "text/plain; charset=utf-8")
    request.add_header("Content-Length", "0")

    if cfg.username:
        auth_bytes = f"{cfg.username}:{cfg.password}".encode("utf-8")
        token = base64.b64encode(auth_bytes).decode("ascii")
        request.add_header("Authorization", f"Basic {token}")

    try:
        with urlopen(request, timeout=cfg.timeout_seconds) as response:
            raw = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"clickhouse returned HTTP {exc.code}: {body.strip()}") from exc
    except URLError as exc:
        raise RuntimeError(f"clickhouse request failed: {exc}") from exc

    rows: list[dict[str, Any]] = []
    for line in raw.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        parsed_row = json.loads(stripped)
        if not isinstance(parsed_row, dict):
            raise RuntimeError(f"unexpected non-object JSON row: {parsed_row!r}")
        rows.append(parsed_row)
    return rows


def parse_int_like(value: Any, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} expected integer, got boolean")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not value.is_integer():
            raise ValueError(f"{field_name} expected integer, got {value}")
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            raise ValueError(f"{field_name} is empty")
        try:
            return int(stripped)
        except ValueError as exc:
            raise ValueError(f"{field_name} expected integer, got {value!r}") from exc
    raise ValueError(f"{field_name} expected integer, got {type(value).__name__}")


def parse_float_like(value: Any, field_name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} expected numeric, got boolean")
    if isinstance(value, (int, float)):
        parsed = float(value)
    elif isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            raise ValueError(f"{field_name} is empty")
        try:
            parsed = float(stripped)
        except ValueError as exc:
            raise ValueError(f"{field_name} expected float, got {value!r}") from exc
    else:
        raise ValueError(f"{field_name} expected numeric, got {type(value).__name__}")

    if not math.isfinite(parsed):
        raise ValueError(f"{field_name} must be finite, got {parsed}")
    return parsed


def parse_bool_like(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        if value in (0, 1):
            return bool(value)
        raise ValueError(f"{field_name} expected 0/1, got {value}")
    if isinstance(value, float):
        if value in (0.0, 1.0):
            return bool(int(value))
        raise ValueError(f"{field_name} expected 0/1, got {value}")
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ("0", "false"):
            return False
        if lowered in ("1", "true"):
            return True
        raise ValueError(f"{field_name} expected boolean/0/1, got {value!r}")
    raise ValueError(f"{field_name} expected boolean, got {type(value).__name__}")


def normalize_rows(raw_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[ReplaySpec]]:
    selected: list[dict[str, Any]] = []
    replayable: list[ReplaySpec] = []

    for rank, row in enumerate(raw_rows, start=1):
        entry: dict[str, Any] = {
            "rank": rank,
            "ts": str(row.get("selected_ts", row.get("ts", ""))),
            "query_id": str(row.get("query_id", "")),
            "source": str(row.get("source", "")),
            "session_hint": str(row.get("session_hint", "")),
            "raw_query": str(row.get("raw_query", "")),
            "result_limit": row.get("result_limit"),
            "min_should_match": row.get("min_should_match"),
            "min_score": row.get("min_score"),
            "include_tool_events": row.get("include_tool_events"),
            "exclude_codex_mcp": row.get("exclude_codex_mcp"),
            "baseline_response_ms": row.get("response_ms"),
            "valid_for_replay": False,
            "skip_reason": None,
        }

        try:
            query = str(row.get("raw_query", "")).strip()
            if not query:
                raise ValueError("raw_query is empty")

            result_limit = parse_int_like(row.get("result_limit"), "result_limit")
            if result_limit < 1 or result_limit > 65535:
                raise ValueError(f"result_limit out of range: {result_limit}")

            min_should_match = parse_int_like(row.get("min_should_match"), "min_should_match")
            if min_should_match < 1 or min_should_match > 65535:
                raise ValueError(f"min_should_match out of range: {min_should_match}")

            min_score = parse_float_like(row.get("min_score"), "min_score")
            include_tool_events = parse_bool_like(
                row.get("include_tool_events"), "include_tool_events"
            )
            exclude_codex_mcp = parse_bool_like(
                row.get("exclude_codex_mcp"), "exclude_codex_mcp"
            )
            baseline_response_ms = parse_float_like(row.get("response_ms"), "response_ms")
            if baseline_response_ms < 0:
                raise ValueError(f"response_ms out of range: {baseline_response_ms}")

            session_hint = str(row.get("session_hint", "")).strip()
            arguments: dict[str, Any] = {
                "query": query,
                "limit": result_limit,
                "min_should_match": min_should_match,
                "min_score": min_score,
                "include_tool_events": include_tool_events,
                "exclude_codex_mcp": exclude_codex_mcp,
            }
            if session_hint:
                arguments["session_id"] = session_hint

            entry["result_limit"] = result_limit
            entry["min_should_match"] = min_should_match
            entry["min_score"] = min_score
            entry["include_tool_events"] = include_tool_events
            entry["exclude_codex_mcp"] = exclude_codex_mcp
            entry["baseline_response_ms"] = baseline_response_ms
            entry["valid_for_replay"] = True

            replayable.append(
                ReplaySpec(
                    rank=rank,
                    variant_label="orig",
                    variant_term_count=0,
                    ts=entry["ts"],
                    query_id=entry["query_id"],
                    source=entry["source"],
                    session_hint=session_hint,
                    raw_query=query,
                    baseline_response_ms=baseline_response_ms,
                    arguments=arguments,
                )
            )
        except ValueError as exc:
            entry["skip_reason"] = str(exc)

        selected.append(entry)

    return selected, replayable


def tokenize_query_terms(text: str, max_terms: int) -> list[str]:
    terms: list[str] = []
    seen: set[str] = set()
    for mat in QUERY_TOKEN_RE.finditer(text):
        token = mat.group(0).lower()
        if len(token) < 2 or len(token) > 64:
            continue
        if token in seen:
            continue
        seen.add(token)
        terms.append(token)
        if len(terms) >= max_terms:
            break
    return terms


def collapse_whitespace(text: str) -> str:
    return " ".join(text.split())


def preview_query(text: str, max_len: int = 56) -> str:
    compact = collapse_whitespace(text)
    if len(compact) <= max_len:
        return compact
    return compact[: max_len - 3] + "..."


def expand_replay_specs(
    specs: list[ReplaySpec],
    query_variant_mode: str,
    max_query_terms: int,
) -> list[ReplaySpec]:
    expanded: list[ReplaySpec] = []
    for spec in specs:
        base_terms = tokenize_query_terms(spec.raw_query, max_query_terms)
        base = ReplaySpec(
            rank=spec.rank,
            variant_label="orig",
            variant_term_count=len(base_terms),
            ts=spec.ts,
            query_id=spec.query_id,
            source=spec.source,
            session_hint=spec.session_hint,
            raw_query=spec.raw_query,
            baseline_response_ms=spec.baseline_response_ms,
            arguments=dict(spec.arguments),
        )
        expanded.append(base)

        if query_variant_mode != "subset_scramble" or not base_terms:
            continue

        seen_queries: set[str] = {collapse_whitespace(base.raw_query).lower()}
        for term_count in range(1, len(base_terms) + 1):
            rng = random.Random(
                f"{spec.rank}:{spec.query_id}:{spec.raw_query}:{term_count}:{max_query_terms}"
            )
            chosen_terms = rng.sample(base_terms, term_count)
            rng.shuffle(chosen_terms)
            variant_query = " ".join(chosen_terms)

            normalized = collapse_whitespace(variant_query).lower()
            if normalized in seen_queries:
                continue
            seen_queries.add(normalized)

            variant_args = dict(base.arguments)
            variant_args["query"] = variant_query
            variant_args["min_should_match"] = max(
                1, min(int(variant_args["min_should_match"]), term_count)
            )

            expanded.append(
                ReplaySpec(
                    rank=spec.rank,
                    variant_label=f"k={term_count}",
                    variant_term_count=term_count,
                    ts=spec.ts,
                    query_id=spec.query_id,
                    source=spec.source,
                    session_hint=spec.session_hint,
                    raw_query=variant_query,
                    baseline_response_ms=spec.baseline_response_ms,
                    arguments=variant_args,
                )
            )

    return expanded


def percentile(values: list[float], q: float) -> Optional[float]:
    if not values:
        return None
    sorted_values = sorted(values)
    if len(sorted_values) == 1:
        return sorted_values[0]
    pos = (len(sorted_values) - 1) * q
    lower = int(math.floor(pos))
    upper = int(math.ceil(pos))
    if lower == upper:
        return sorted_values[lower]
    lower_value = sorted_values[lower]
    upper_value = sorted_values[upper]
    return lower_value + (upper_value - lower_value) * (pos - lower)


def summarize_values(values: list[float], include_p99: bool = True) -> Optional[dict[str, float]]:
    if not values:
        return None
    stats = {
        "min": min(values),
        "p50": percentile(values, 0.50) or 0.0,
        "p95": percentile(values, 0.95) or 0.0,
        "max": max(values),
        "avg": sum(values) / len(values),
    }
    if include_p99:
        stats["p99"] = percentile(values, 0.99) or 0.0
    return stats


def format_optional_ms(value: Optional[float]) -> str:
    if value is None:
        return "-"
    return f"{value:.2f}"


def format_scalar_or_stats(label: str, value: Any) -> list[str]:
    if value is None:
        return [f"  {label}: -"]
    if isinstance(value, (int, float)):
        return [f"  {label}: {float(value):.2f}"]
    if isinstance(value, dict):
        return [
            (
                f"  {label}: "
                f"min={value.get('min', 0.0):.2f} "
                f"p50={value.get('p50', 0.0):.2f} "
                f"p95={value.get('p95', 0.0):.2f} "
                f"p99={value.get('p99', 0.0):.2f} "
                f"max={value.get('max', 0.0):.2f} "
                f"avg={value.get('avg', 0.0):.2f}"
            )
        ]
    return [f"  {label}: -"]


def collect_stderr(
    proc: subprocess.Popen[str], wait_seconds: float = 0.2, max_bytes: int = 8192
) -> str:
    if proc.stderr is None:
        return ""

    chunks: list[str] = []
    bytes_read = 0
    timeout = wait_seconds
    fd = proc.stderr.fileno()
    while bytes_read < max_bytes:
        ready, _, _ = select.select([proc.stderr], [], [], timeout)
        if not ready:
            break

        timeout = 0
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
        stderr = collect_stderr(proc)
        raise TimeoutError(f"timed out waiting for MCP response; stderr={stderr.strip()}")

    line = proc.stdout.readline()
    if line == "":
        stderr = collect_stderr(proc)
        raise RuntimeError(f"MCP process exited unexpectedly; stderr={stderr.strip()}")

    parsed = json.loads(line)
    if not isinstance(parsed, dict):
        raise RuntimeError("invalid JSON-RPC payload: non-object")
    return parsed


def send_rpc_request(
    proc: subprocess.Popen[str], payload: dict[str, Any], timeout_seconds: int
) -> dict[str, Any]:
    if proc.stdin is None:
        raise RuntimeError("MCP stdin pipe is unavailable")
    proc.stdin.write(json.dumps(payload) + "\n")
    proc.stdin.flush()
    return read_json_line(proc, timeout_seconds=timeout_seconds)


def parse_rpc_ok(response: dict[str, Any], expected_id: int) -> dict[str, Any]:
    if response.get("id") != expected_id:
        raise RuntimeError(f"unexpected rpc id={response.get('id')} expected={expected_id}")
    if "error" in response:
        raise RuntimeError(f"rpc error: {response['error']}")
    result = response.get("result")
    if not isinstance(result, dict):
        raise RuntimeError("rpc response missing result object")
    return result


def parse_server_took_ms(tool_result: dict[str, Any]) -> Optional[float]:
    structured = tool_result.get("structuredContent")
    if isinstance(structured, dict):
        stats = structured.get("stats")
        if isinstance(stats, dict):
            took_ms = stats.get("took_ms")
            if isinstance(took_ms, (int, float)):
                return float(took_ms)

    content = tool_result.get("content")
    if isinstance(content, list) and content:
        first = content[0]
        if isinstance(first, dict):
            text = first.get("text")
            if isinstance(text, str):
                mat = PROSE_TOOK_RE.search(text)
                if mat:
                    return float(int(mat.group(1)))

    return None


def start_mcp_process(moraine_bin: str, config_path: Path) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [moraine_bin, "run", "mcp", "--config", str(config_path)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )


def stop_mcp_process(proc: Optional[subprocess.Popen[str]]) -> None:
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


def initialize_mcp(
    proc: subprocess.Popen[str], timeout_seconds: int, request_id: int
) -> tuple[int, float]:
    payload = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "initialize",
        "params": {},
    }
    started_ns = time.perf_counter_ns()
    response = send_rpc_request(proc, payload, timeout_seconds=timeout_seconds)
    elapsed_ms = (time.perf_counter_ns() - started_ns) / 1_000_000.0
    result = parse_rpc_ok(response, request_id)
    if "protocolVersion" not in result:
        raise RuntimeError("initialize response missing protocolVersion")
    return request_id + 1, elapsed_ms


def build_tool_arguments(spec: ReplaySpec, tool: str, verbosity: str) -> dict[str, Any]:
    base: dict[str, Any] = {
        "query": str(spec.arguments["query"]),
        "limit": int(spec.arguments["limit"]),
        "min_should_match": int(spec.arguments["min_should_match"]),
        "min_score": float(spec.arguments["min_score"]),
        "include_tool_events": bool(spec.arguments["include_tool_events"]),
        "exclude_codex_mcp": bool(spec.arguments["exclude_codex_mcp"]),
        "verbosity": verbosity,
    }

    if tool == "search":
        session_id = spec.arguments.get("session_id")
        if isinstance(session_id, str) and session_id.strip():
            base["session_id"] = session_id.strip()

    return base


def call_tool(
    proc: subprocess.Popen[str],
    timeout_seconds: int,
    request_id: int,
    tool: str,
    arguments: dict[str, Any],
) -> tuple[int, float, Optional[float]]:
    payload = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {
            "name": tool,
            "arguments": arguments,
        },
    }

    started_ns = time.perf_counter_ns()
    response = send_rpc_request(proc, payload, timeout_seconds=timeout_seconds)
    elapsed_ms = (time.perf_counter_ns() - started_ns) / 1_000_000.0
    result = parse_rpc_ok(response, request_id)

    if bool(result.get("isError")):
        raise RuntimeError(f"tool returned isError=true: {result}")

    server_took_ms = parse_server_took_ms(result)
    return request_id + 1, elapsed_ms, server_took_ms


def choose_tools(tool_arg: str) -> list[str]:
    if tool_arg == "both":
        return ["search", "search_conversations"]
    return [tool_arg]


def print_selection_summary(
    args: argparse.Namespace,
    selected_rows: list[dict[str, Any]],
    base_replay_rows: list[ReplaySpec],
    expanded_replay_rows: list[ReplaySpec],
) -> None:
    ts_values = [row.get("ts", "") for row in selected_rows if row.get("ts")]
    window_range = "-"
    if ts_values:
        window_range = f"{min(ts_values)} .. {max(ts_values)}"

    skipped = len(selected_rows) - len(base_replay_rows)
    print("Selection")
    print(f"  mode: {args.mode}")
    print(f"  tool: {args.tool}")
    print(f"  verbosity: {args.verbosity}")
    print(f"  window: {args.window}")
    print(f"  requested_top_n: {args.top_n}")
    print(f"  include_benchmark_replays: {args.include_benchmark_replays}")
    print(f"  selected_rows: {len(selected_rows)}")
    print(f"  replayable_rows: {len(base_replay_rows)}")
    print(f"  skipped_rows: {skipped}")
    print(f"  query_variant_mode: {args.query_variant_mode}")
    print(f"  max_query_terms: {args.max_query_terms}")
    print(f"  expanded_replay_cases: {len(expanded_replay_rows)}")
    print(f"  warmup: {args.warmup}")
    print(f"  repeats: {args.repeats}")
    print(f"  timeout_seconds: {args.timeout_seconds}")
    print(f"  post_init_wait_ms: {args.post_init_wait_ms}")
    print(f"  selected_ts_range: {window_range}")


def print_dry_run_table(selected_rows: list[dict[str, Any]]) -> None:
    print("\nDry Run Rows")
    header = (
        f"{'rank':>4} {'baseline':>9} {'limit':>6} {'msm':>5} {'min_score':>10} "
        f"{'incl_tools':>10} {'excl_mcp':>9} {'status':>8}  query"
    )
    print(header)
    print("-" * len(header))
    for row in selected_rows:
        baseline = row.get("baseline_response_ms")
        baseline_text = f"{float(baseline):.2f}" if isinstance(baseline, (int, float)) else "-"

        status = "ok" if row.get("valid_for_replay") else "skip"
        detail = (
            f"{row.get('rank', '-')!s:>4} "
            f"{baseline_text:>9} "
            f"{str(row.get('result_limit', '-')):>6} "
            f"{str(row.get('min_should_match', '-')):>5} "
            f"{str(row.get('min_score', '-')):>10} "
            f"{str(row.get('include_tool_events', '-')):>10} "
            f"{str(row.get('exclude_codex_mcp', '-')):>9} "
            f"{status:>8}  "
            f"{preview_query(str(row.get('raw_query', '')))}"
        )
        print(detail)
        if row.get("skip_reason"):
            print(f"      skip_reason: {row['skip_reason']}")


def print_target_summary(target: dict[str, Any]) -> None:
    print(
        "\nTarget"
        f" mode={target['mode']}"
        f" tool={target['tool']}"
        f" verbosity={target['verbosity']}"
    )
    for line in format_scalar_or_stats("process_boot_ms", target.get("process_boot_ms")):
        print(line)
    for line in format_scalar_or_stats("init_ms", target.get("init_ms")):
        print(line)

    sample_count = target.get("sample_count", 0)
    print(f"  samples: {sample_count}")

    e2e_stats = target.get("e2e_search_ms")
    if e2e_stats:
        print(
            "  e2e_search_ms: "
            f"min={e2e_stats['min']:.2f} "
            f"p50={e2e_stats['p50']:.2f} "
            f"p95={e2e_stats['p95']:.2f} "
            f"p99={e2e_stats.get('p99', 0.0):.2f} "
            f"max={e2e_stats['max']:.2f} "
            f"avg={e2e_stats['avg']:.2f}"
        )

    server_stats = target.get("server_took_ms")
    if server_stats:
        print(
            "  server_took_ms: "
            f"min={server_stats['min']:.2f} "
            f"p50={server_stats['p50']:.2f} "
            f"p95={server_stats['p95']:.2f} "
            f"p99={server_stats.get('p99', 0.0):.2f} "
            f"max={server_stats['max']:.2f} "
            f"avg={server_stats['avg']:.2f}"
        )

    overhead_stats = target.get("transport_overhead_ms")
    if overhead_stats:
        print(
            "  transport_overhead_ms: "
            f"min={overhead_stats['min']:.2f} "
            f"p50={overhead_stats['p50']:.2f} "
            f"p95={overhead_stats['p95']:.2f} "
            f"p99={overhead_stats.get('p99', 0.0):.2f} "
            f"max={overhead_stats['max']:.2f} "
            f"avg={overhead_stats['avg']:.2f}"
        )

    cold_stats = target.get("cold_total_ms")
    if cold_stats:
        print(
            "  cold_total_ms: "
            f"min={cold_stats['min']:.2f} "
            f"p50={cold_stats['p50']:.2f} "
            f"p95={cold_stats['p95']:.2f} "
            f"p99={cold_stats.get('p99', 0.0):.2f} "
            f"max={cold_stats['max']:.2f} "
            f"avg={cold_stats['avg']:.2f}"
        )

    first_call_ms = target.get("first_call_ms")
    if first_call_ms is not None:
        print(f"  first_call_ms: {first_call_ms:.2f}")

    warmup_decay = target.get("warmup_decay_ms") or []
    if warmup_decay:
        formatted = ", ".join(f"{value:.2f}" for value in warmup_decay)
        print(f"  warmup_decay_first5_ms: [{formatted}]")

    failures = target.get("failures", {})
    print(
        "  failures: "
        f"spawn={failures.get('spawn', 0)} "
        f"initialize={failures.get('initialize', 0)} "
        f"search={failures.get('search', 0)} "
        f"decode={failures.get('decode', 0)}"
    )


def print_per_case_table(per_case: list[dict[str, Any]]) -> None:
    print("\nPer-Case Replay")
    header = (
        f"{'rank':>4} {'tool':>20} {'case':>7} {'terms':>5} {'baseline':>9} {'e2e_p50':>8} "
        f"{'srv_p50':>8} {'ovh_p50':>8} {'cold_p50':>9} {'ok':>5} {'fail':>5}  query"
    )
    print(header)
    print("-" * len(header))
    for row in per_case:
        e2e = row.get("e2e_search_ms") or {}
        server = row.get("server_took_ms") or {}
        overhead = row.get("transport_overhead_ms") or {}
        cold = row.get("cold_total_ms") or {}
        print(
            f"{row['rank']:>4} "
            f"{row['tool']:>20} "
            f"{row['variant_label']:>7} "
            f"{str(row.get('variant_term_count', '-')):>5} "
            f"{row['baseline_response_ms']:>9.2f} "
            f"{format_optional_ms(e2e.get('p50')):>8} "
            f"{format_optional_ms(server.get('p50')):>8} "
            f"{format_optional_ms(overhead.get('p50')):>8} "
            f"{format_optional_ms(cold.get('p50')):>9} "
            f"{row['success_count']:>5} "
            f"{row['failure_count']:>5}  "
            f"{preview_query(row['query'])}"
        )


def run_target(
    *,
    args: argparse.Namespace,
    config_path: Path,
    tool: str,
    specs: list[ReplaySpec],
) -> dict[str, Any]:
    if args.mode == "persistent":
        return run_target_persistent(args=args, config_path=config_path, tool=tool, specs=specs)
    return run_target_cold_process(args=args, config_path=config_path, tool=tool, specs=specs)


def run_target_persistent(
    *,
    args: argparse.Namespace,
    config_path: Path,
    tool: str,
    specs: list[ReplaySpec],
) -> dict[str, Any]:
    failures = {"spawn": 0, "initialize": 0, "search": 0, "decode": 0}
    samples: list[dict[str, Any]] = []
    per_case: list[dict[str, Any]] = []
    first_call_ms: Optional[float] = None
    warmup_decay_ms: list[float] = []

    proc: Optional[subprocess.Popen[str]] = None
    process_boot_ms: Optional[float] = None
    init_ms: Optional[float] = None
    next_request_id = 1

    spawn_started_ns = time.perf_counter_ns()
    try:
        proc = start_mcp_process(args.moraine_bin, config_path)
    except Exception:
        failures["spawn"] += 1
        return {
            "mode": args.mode,
            "tool": tool,
            "verbosity": args.verbosity,
            "process_boot_ms": None,
            "init_ms": None,
            "sample_count": 0,
            "e2e_search_ms": None,
            "server_took_ms": None,
            "transport_overhead_ms": None,
            "cold_total_ms": None,
            "first_call_ms": None,
            "warmup_decay_ms": [],
            "failures": failures,
            "per_case": [],
        }

    try:
        try:
            next_request_id, init_ms = initialize_mcp(
                proc, timeout_seconds=args.timeout_seconds, request_id=next_request_id
            )
            process_boot_ms = (time.perf_counter_ns() - spawn_started_ns) / 1_000_000.0
        except Exception:
            failures["initialize"] += 1
            return {
                "mode": args.mode,
                "tool": tool,
                "verbosity": args.verbosity,
                "process_boot_ms": process_boot_ms,
                "init_ms": init_ms,
                "sample_count": 0,
                "e2e_search_ms": None,
                "server_took_ms": None,
                "transport_overhead_ms": None,
                "cold_total_ms": None,
                "first_call_ms": None,
                "warmup_decay_ms": [],
                "failures": failures,
                "per_case": [],
            }

        if args.post_init_wait_ms > 0:
            time.sleep(args.post_init_wait_ms / 1000.0)

        for spec in specs:
            case_e2e: list[float] = []
            case_server_took: list[float] = []
            case_overhead: list[float] = []
            case_failures = 0

            total_runs = args.warmup + args.repeats
            for idx in range(total_runs):
                measured = idx >= args.warmup
                tool_args = build_tool_arguments(spec, tool=tool, verbosity=args.verbosity)

                try:
                    next_request_id, e2e_ms, server_took_ms = call_tool(
                        proc,
                        timeout_seconds=args.timeout_seconds,
                        request_id=next_request_id,
                        tool=tool,
                        arguments=tool_args,
                    )
                except TimeoutError:
                    failures["search"] += 1
                    if measured:
                        case_failures += 1
                    continue
                except json.JSONDecodeError:
                    failures["decode"] += 1
                    if measured:
                        case_failures += 1
                    continue
                except Exception:
                    failures["search"] += 1
                    if measured:
                        case_failures += 1
                    continue

                if not measured:
                    continue

                overhead_ms = None
                if server_took_ms is not None:
                    overhead_ms = max(0.0, e2e_ms - server_took_ms)

                record = {
                    "rank": spec.rank,
                    "tool": tool,
                    "variant_label": spec.variant_label,
                    "variant_term_count": spec.variant_term_count,
                    "query": str(spec.arguments["query"]),
                    "e2e_search_ms": e2e_ms,
                    "init_ms": None,
                    "process_boot_ms": None,
                    "server_took_ms": server_took_ms,
                    "transport_overhead_ms": overhead_ms,
                    "cold_total_ms": None,
                }
                samples.append(record)

                case_e2e.append(e2e_ms)
                if server_took_ms is not None:
                    case_server_took.append(server_took_ms)
                if overhead_ms is not None:
                    case_overhead.append(overhead_ms)

                if first_call_ms is None:
                    first_call_ms = e2e_ms
                if len(warmup_decay_ms) < 5:
                    warmup_decay_ms.append(e2e_ms)

            per_case.append(
                {
                    "rank": spec.rank,
                    "tool": tool,
                    "variant_label": spec.variant_label,
                    "variant_term_count": spec.variant_term_count,
                    "query": str(spec.arguments["query"]),
                    "baseline_response_ms": spec.baseline_response_ms,
                    "success_count": len(case_e2e),
                    "failure_count": case_failures,
                    "e2e_search_ms": summarize_values(case_e2e, include_p99=True),
                    "server_took_ms": summarize_values(case_server_took, include_p99=True),
                    "transport_overhead_ms": summarize_values(case_overhead, include_p99=True),
                    "cold_total_ms": None,
                }
            )
    finally:
        stop_mcp_process(proc)

    e2e_values = [float(item["e2e_search_ms"]) for item in samples]
    server_values = [
        float(item["server_took_ms"])
        for item in samples
        if isinstance(item.get("server_took_ms"), (int, float))
    ]
    overhead_values = [
        float(item["transport_overhead_ms"])
        for item in samples
        if isinstance(item.get("transport_overhead_ms"), (int, float))
    ]

    return {
        "mode": args.mode,
        "tool": tool,
        "verbosity": args.verbosity,
        "process_boot_ms": process_boot_ms,
        "init_ms": init_ms,
        "sample_count": len(samples),
        "e2e_search_ms": summarize_values(e2e_values, include_p99=True),
        "server_took_ms": summarize_values(server_values, include_p99=True),
        "transport_overhead_ms": summarize_values(overhead_values, include_p99=True),
        "cold_total_ms": None,
        "first_call_ms": first_call_ms,
        "warmup_decay_ms": warmup_decay_ms,
        "failures": failures,
        "per_case": per_case,
    }


def run_target_cold_process(
    *,
    args: argparse.Namespace,
    config_path: Path,
    tool: str,
    specs: list[ReplaySpec],
) -> dict[str, Any]:
    failures = {"spawn": 0, "initialize": 0, "search": 0, "decode": 0}
    samples: list[dict[str, Any]] = []
    per_case: list[dict[str, Any]] = []
    first_call_ms: Optional[float] = None
    warmup_decay_ms: list[float] = []

    for spec in specs:
        case_e2e: list[float] = []
        case_init: list[float] = []
        case_boot: list[float] = []
        case_server_took: list[float] = []
        case_overhead: list[float] = []
        case_cold_total: list[float] = []
        case_failures = 0

        total_runs = args.warmup + args.repeats
        for idx in range(total_runs):
            measured = idx >= args.warmup
            proc: Optional[subprocess.Popen[str]] = None
            next_request_id = 1
            spawn_started_ns = time.perf_counter_ns()
            try:
                proc = start_mcp_process(args.moraine_bin, config_path)
            except Exception:
                failures["spawn"] += 1
                if measured:
                    case_failures += 1
                continue

            try:
                try:
                    next_request_id, init_ms = initialize_mcp(
                        proc, timeout_seconds=args.timeout_seconds, request_id=next_request_id
                    )
                except TimeoutError:
                    failures["initialize"] += 1
                    if measured:
                        case_failures += 1
                    continue
                except json.JSONDecodeError:
                    failures["decode"] += 1
                    if measured:
                        case_failures += 1
                    continue
                except Exception:
                    failures["initialize"] += 1
                    if measured:
                        case_failures += 1
                    continue

                process_boot_ms = (time.perf_counter_ns() - spawn_started_ns) / 1_000_000.0

                if args.post_init_wait_ms > 0:
                    time.sleep(args.post_init_wait_ms / 1000.0)

                tool_args = build_tool_arguments(spec, tool=tool, verbosity=args.verbosity)
                try:
                    next_request_id, e2e_ms, server_took_ms = call_tool(
                        proc,
                        timeout_seconds=args.timeout_seconds,
                        request_id=next_request_id,
                        tool=tool,
                        arguments=tool_args,
                    )
                except TimeoutError:
                    failures["search"] += 1
                    if measured:
                        case_failures += 1
                    continue
                except json.JSONDecodeError:
                    failures["decode"] += 1
                    if measured:
                        case_failures += 1
                    continue
                except Exception:
                    failures["search"] += 1
                    if measured:
                        case_failures += 1
                    continue

                if not measured:
                    continue

                overhead_ms = None
                if server_took_ms is not None:
                    overhead_ms = max(0.0, e2e_ms - server_took_ms)
                cold_total_ms = init_ms + e2e_ms

                record = {
                    "rank": spec.rank,
                    "tool": tool,
                    "variant_label": spec.variant_label,
                    "variant_term_count": spec.variant_term_count,
                    "query": str(spec.arguments["query"]),
                    "e2e_search_ms": e2e_ms,
                    "init_ms": init_ms,
                    "process_boot_ms": process_boot_ms,
                    "server_took_ms": server_took_ms,
                    "transport_overhead_ms": overhead_ms,
                    "cold_total_ms": cold_total_ms,
                }
                samples.append(record)

                case_e2e.append(e2e_ms)
                case_init.append(init_ms)
                case_boot.append(process_boot_ms)
                case_cold_total.append(cold_total_ms)
                if server_took_ms is not None:
                    case_server_took.append(server_took_ms)
                if overhead_ms is not None:
                    case_overhead.append(overhead_ms)

                if first_call_ms is None:
                    first_call_ms = e2e_ms
                if len(warmup_decay_ms) < 5:
                    warmup_decay_ms.append(e2e_ms)
            finally:
                stop_mcp_process(proc)

        per_case.append(
            {
                "rank": spec.rank,
                "tool": tool,
                "variant_label": spec.variant_label,
                "variant_term_count": spec.variant_term_count,
                "query": str(spec.arguments["query"]),
                "baseline_response_ms": spec.baseline_response_ms,
                "success_count": len(case_e2e),
                "failure_count": case_failures,
                "e2e_search_ms": summarize_values(case_e2e, include_p99=True),
                "init_ms": summarize_values(case_init, include_p99=True),
                "process_boot_ms": summarize_values(case_boot, include_p99=True),
                "server_took_ms": summarize_values(case_server_took, include_p99=True),
                "transport_overhead_ms": summarize_values(case_overhead, include_p99=True),
                "cold_total_ms": summarize_values(case_cold_total, include_p99=True),
            }
        )

    e2e_values = [float(item["e2e_search_ms"]) for item in samples]
    init_values = [float(item["init_ms"]) for item in samples]
    boot_values = [float(item["process_boot_ms"]) for item in samples]
    cold_values = [float(item["cold_total_ms"]) for item in samples]
    server_values = [
        float(item["server_took_ms"])
        for item in samples
        if isinstance(item.get("server_took_ms"), (int, float))
    ]
    overhead_values = [
        float(item["transport_overhead_ms"])
        for item in samples
        if isinstance(item.get("transport_overhead_ms"), (int, float))
    ]

    return {
        "mode": args.mode,
        "tool": tool,
        "verbosity": args.verbosity,
        "process_boot_ms": summarize_values(boot_values, include_p99=True),
        "init_ms": summarize_values(init_values, include_p99=True),
        "sample_count": len(samples),
        "e2e_search_ms": summarize_values(e2e_values, include_p99=True),
        "server_took_ms": summarize_values(server_values, include_p99=True),
        "transport_overhead_ms": summarize_values(overhead_values, include_p99=True),
        "cold_total_ms": summarize_values(cold_values, include_p99=True),
        "first_call_ms": first_call_ms,
        "warmup_decay_ms": warmup_decay_ms,
        "failures": failures,
        "per_case": per_case,
    }


def build_output_json(
    *,
    args: argparse.Namespace,
    config_path: Path,
    selected_rows: list[dict[str, Any]],
    targets: list[dict[str, Any]],
    dry_run: bool,
) -> dict[str, Any]:
    return {
        "meta": {
            "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "git_sha": git_sha(),
            "config_path": str(config_path),
            "parameters": {
                "window": args.window,
                "top_n": args.top_n,
                "warmup": args.warmup,
                "repeats": args.repeats,
                "timeout_seconds": args.timeout_seconds,
                "include_benchmark_replays": args.include_benchmark_replays,
                "query_variant_mode": args.query_variant_mode,
                "max_query_terms": args.max_query_terms,
                "mode": args.mode,
                "tool": args.tool,
                "verbosity": args.verbosity,
                "moraine_bin": args.moraine_bin,
                "post_init_wait_ms": args.post_init_wait_ms,
                "dry_run": dry_run,
            },
            "selected_count": len(selected_rows),
            "target_count": len(targets),
        },
        "selected_queries": selected_rows,
        "targets": targets,
    }


def write_output_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    config_path = Path(args.config).expanduser().resolve()

    try:
        interval_expr = parse_window_interval(args.window)
        ch_cfg = read_config(config_path)
    except Exception as exc:
        print(f"fatal: {exc}", file=sys.stderr)
        return 2

    selection_sql = build_selection_sql(
        ch_cfg.database,
        interval_expr,
        args.top_n,
        args.include_benchmark_replays,
    )
    if args.print_sql:
        print("Selection SQL:")
        print(selection_sql)
        print("")

    try:
        raw_rows = clickhouse_query_json_each_row(ch_cfg, selection_sql)
    except Exception as exc:
        print(f"fatal: failed to query clickhouse: {exc}", file=sys.stderr)
        return 2

    if not raw_rows:
        print(
            "fatal: no rows found in search_query_log for requested window; "
            "try increasing --window or pass --include-benchmark-replays",
            file=sys.stderr,
        )
        payload = build_output_json(
            args=args,
            config_path=config_path,
            selected_rows=[],
            targets=[],
            dry_run=args.dry_run,
        )
        if args.output_json:
            write_output_json(Path(args.output_json).expanduser(), payload)
        return 2

    selected_rows, base_replay_rows = normalize_rows(raw_rows)
    replayable_rows = expand_replay_specs(
        base_replay_rows,
        query_variant_mode=args.query_variant_mode,
        max_query_terms=args.max_query_terms,
    )

    print_selection_summary(args, selected_rows, base_replay_rows, replayable_rows)

    if args.dry_run:
        print_dry_run_table(selected_rows)
        payload = build_output_json(
            args=args,
            config_path=config_path,
            selected_rows=selected_rows,
            targets=[],
            dry_run=True,
        )
        if args.output_json:
            write_output_json(Path(args.output_json).expanduser(), payload)
        return 0

    if not base_replay_rows:
        print(
            "fatal: selected rows were present but none were valid for replay",
            file=sys.stderr,
        )
        payload = build_output_json(
            args=args,
            config_path=config_path,
            selected_rows=selected_rows,
            targets=[],
            dry_run=False,
        )
        if args.output_json:
            write_output_json(Path(args.output_json).expanduser(), payload)
        return 2

    tools = choose_tools(args.tool)
    targets: list[dict[str, Any]] = []

    for tool in tools:
        target = run_target(
            args=args,
            config_path=config_path,
            tool=tool,
            specs=replayable_rows,
        )
        targets.append(target)
        print_target_summary(target)
        print_per_case_table(target.get("per_case", []))

    payload = build_output_json(
        args=args,
        config_path=config_path,
        selected_rows=selected_rows,
        targets=targets,
        dry_run=False,
    )
    if args.output_json:
        write_output_json(Path(args.output_json).expanduser(), payload)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
