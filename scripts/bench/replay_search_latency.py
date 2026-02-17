#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import math
import os
import re
import select
import shutil
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Replay top-N slow search requests from moraine.search_query_log via MCP "
            "search and report observed latency."
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
        "--timeout-seconds", type=positive_int, default=20, help="Timeout per MCP request"
    )
    parser.add_argument("--output-json", help="Write machine-readable results JSON to this path")
    parser.add_argument(
        "--print-sql", action="store_true", help="Print selection SQL before execution"
    )
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
        if not line:
            continue
        if line.startswith("#"):
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


def collapse_whitespace(text: str) -> str:
    return " ".join(text.split())


def preview_query(text: str, max_len: int = 56) -> str:
    compact = collapse_whitespace(text)
    if len(compact) <= max_len:
        return compact
    return compact[: max_len - 3] + "..."


def format_optional_ms(value: Optional[float]) -> str:
    if value is None:
        return "-"
    return f"{value:.2f}"


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


def build_selection_sql(database: str, interval_expr: str, top_n: int) -> str:
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
            query = str(row.get("raw_query", ""))
            query = query.strip()
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
                "verbosity": "full",
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


def summarize_values(values: list[float], include_p99: bool) -> Optional[dict[str, float]]:
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


class McpClient:
    def __init__(self, command: list[str], default_timeout_seconds: int):
        self.command = command
        self.default_timeout_seconds = default_timeout_seconds
        self.next_id = 1
        self.proc = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self._initialize()

    def close(self) -> None:
        if self.proc.stdin:
            self.proc.stdin.close()
        self.proc.terminate()
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=5)

    def _read_json_line(self, timeout_seconds: float) -> dict[str, Any]:
        if self.proc.stdout is None:
            raise RuntimeError("MCP stdout pipe is unavailable")

        ready, _, _ = select.select([self.proc.stdout], [], [], timeout_seconds)
        if not ready:
            stderr = collect_stderr(self.proc)
            raise TimeoutError(f"timed out waiting for MCP response; stderr={stderr.strip()}")

        line = self.proc.stdout.readline()
        if line == "":
            stderr = collect_stderr(self.proc)
            raise RuntimeError(f"MCP process exited unexpectedly; stderr={stderr.strip()}")

        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"invalid JSON-RPC line from MCP: {line.strip()}") from exc
        if not isinstance(payload, dict):
            raise RuntimeError(f"invalid JSON-RPC payload type: {type(payload).__name__}")
        return payload

    def _request(
        self, method: str, params: dict[str, Any], timeout_seconds: Optional[int] = None
    ) -> dict[str, Any]:
        if self.proc.stdin is None:
            raise RuntimeError("MCP stdin pipe is unavailable")

        request_id = self.next_id
        self.next_id += 1

        payload = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        }
        self.proc.stdin.write(json.dumps(payload) + "\n")
        self.proc.stdin.flush()

        deadline = time.monotonic() + float(timeout_seconds or self.default_timeout_seconds)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                stderr = collect_stderr(self.proc)
                raise TimeoutError(
                    f"timed out waiting for method={method} response id={request_id}; "
                    f"stderr={stderr.strip()}"
                )
            response = self._read_json_line(remaining)
            if response.get("id") != request_id:
                continue
            if "error" in response:
                raise RuntimeError(f"rpc error for {method}: {response['error']}")
            result = response.get("result")
            if not isinstance(result, dict):
                raise RuntimeError(f"rpc response for {method} missing result object")
            return result

    def _notify(self, method: str, params: dict[str, Any]) -> None:
        if self.proc.stdin is None:
            raise RuntimeError("MCP stdin pipe is unavailable")
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        }
        self.proc.stdin.write(json.dumps(payload) + "\n")
        self.proc.stdin.flush()

    def _initialize(self) -> None:
        init_result = self._request("initialize", {})
        if "protocolVersion" not in init_result:
            raise RuntimeError("initialize response missing protocolVersion")

        self._notify("notifications/initialized", {})

        tools_result = self._request("tools/list", {})
        tools = tools_result.get("tools")
        if not isinstance(tools, list):
            raise RuntimeError("tools/list missing tools array")
        tool_names = {tool.get("name") for tool in tools if isinstance(tool, dict)}
        if "search" not in tool_names:
            raise RuntimeError(f"tools/list did not include search tool: {sorted(tool_names)}")

    def search(self, arguments: dict[str, Any], timeout_seconds: int) -> dict[str, Any]:
        result = self._request(
            "tools/call",
            {"name": "search", "arguments": arguments},
            timeout_seconds=timeout_seconds,
        )
        if result.get("isError"):
            raise RuntimeError(f"search tool call returned isError=true: {result}")
        return result


def resolve_moraine_bin(repo_root: Path) -> str:
    from_env = os.environ.get("MORAINECTL_BIN")
    if from_env:
        return from_env

    for cmd in ("morainectl", "moraine"):
        path = shutil.which(cmd)
        if path:
            return path

    local_wrapper = repo_root / "bin" / "moraine"
    if local_wrapper.exists() and os.access(local_wrapper, os.X_OK):
        return str(local_wrapper)

    raise RuntimeError(
        "unable to resolve moraine binary; set MORAINECTL_BIN, or install morainectl/moraine"
    )


def print_selection_summary(
    args: argparse.Namespace,
    selected_rows: list[dict[str, Any]],
    replayable_rows: list[ReplaySpec],
) -> None:
    ts_values = [row.get("ts", "") for row in selected_rows if row.get("ts")]
    window_range = "-"
    if ts_values:
        window_range = f"{min(ts_values)} .. {max(ts_values)}"

    skipped = len(selected_rows) - len(replayable_rows)
    print("Selection")
    print(f"  window: {args.window}")
    print(f"  requested_top_n: {args.top_n}")
    print(f"  selected_rows: {len(selected_rows)}")
    print(f"  replayable_rows: {len(replayable_rows)}")
    print(f"  skipped_rows: {skipped}")
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
        if isinstance(baseline, (int, float)):
            baseline_text = f"{float(baseline):.2f}"
        else:
            baseline_text = "-"

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


def print_replay_table(results: list[dict[str, Any]]) -> None:
    print("\nPer-Query Replay")
    header = (
        f"{'rank':>4} {'baseline':>9} {'p50':>8} {'p95':>8} {'delta_p50':>10} "
        f"{'min':>8} {'max':>8} {'ok':>5} {'fail':>5}  query"
    )
    print(header)
    print("-" * len(header))
    for result in results:
        stats = result.get("stats_ms") or {}
        line = (
            f"{result['rank']:>4} "
            f"{result['baseline_response_ms']:>9.2f} "
            f"{format_optional_ms(stats.get('p50')):>8} "
            f"{format_optional_ms(stats.get('p95')):>8} "
            f"{format_optional_ms(result.get('delta_p50_ms')):>10} "
            f"{format_optional_ms(stats.get('min')):>8} "
            f"{format_optional_ms(stats.get('max')):>8} "
            f"{result['success_count']:>5} "
            f"{result['failure_count']:>5}  "
            f"{preview_query(result['query'])}"
        )
        print(line)


def build_output_json(
    args: argparse.Namespace,
    config_path: Path,
    selected_rows: list[dict[str, Any]],
    replay_results: list[dict[str, Any]],
    aggregate: dict[str, Any],
    failures: dict[str, int],
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
                "dry_run": dry_run,
            },
            "selected_count": len(selected_rows),
            "replayed_count": len(replay_results),
        },
        "selected_queries": selected_rows,
        "replay_results": replay_results,
        "aggregate": aggregate,
        "failures": failures,
    }


def write_output_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    config_path = Path(args.config).expanduser().resolve()
    repo_root = Path(__file__).resolve().parents[2]

    try:
        interval_expr = parse_window_interval(args.window)
        ch_cfg = read_config(config_path)
    except Exception as exc:
        print(f"fatal: {exc}", file=sys.stderr)
        return 2

    selection_sql = build_selection_sql(ch_cfg.database, interval_expr, args.top_n)
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
            "try increasing --window or confirm telemetry is present",
            file=sys.stderr,
        )
        empty_payload = build_output_json(
            args=args,
            config_path=config_path,
            selected_rows=[],
            replay_results=[],
            aggregate={"stats_ms": None, "successful_samples": 0},
            failures={"timeouts": 0, "errors": 1},
            dry_run=args.dry_run,
        )
        if args.output_json:
            write_output_json(Path(args.output_json).expanduser(), empty_payload)
        return 2

    selected_rows, replayable_rows = normalize_rows(raw_rows)
    print_selection_summary(args, selected_rows, replayable_rows)

    if args.dry_run:
        print_dry_run_table(selected_rows)
        payload = build_output_json(
            args=args,
            config_path=config_path,
            selected_rows=selected_rows,
            replay_results=[],
            aggregate={"stats_ms": None, "successful_samples": 0},
            failures={"timeouts": 0, "errors": 0},
            dry_run=True,
        )
        if args.output_json:
            write_output_json(Path(args.output_json).expanduser(), payload)
        return 0

    if not replayable_rows:
        print(
            "fatal: selected rows were present but none were valid for replay",
            file=sys.stderr,
        )
        payload = build_output_json(
            args=args,
            config_path=config_path,
            selected_rows=selected_rows,
            replay_results=[],
            aggregate={"stats_ms": None, "successful_samples": 0},
            failures={"timeouts": 0, "errors": 1},
            dry_run=False,
        )
        if args.output_json:
            write_output_json(Path(args.output_json).expanduser(), payload)
        return 2

    try:
        moraine_bin = resolve_moraine_bin(repo_root)
    except Exception as exc:
        print(f"fatal: {exc}", file=sys.stderr)
        return 2

    command = [moraine_bin, "run", "mcp", "--config", str(config_path)]
    replay_results: list[dict[str, Any]] = []
    all_samples: list[float] = []
    failures = {"timeouts": 0, "errors": 0}

    try:
        client = McpClient(command, default_timeout_seconds=args.timeout_seconds)
    except Exception as exc:
        print(f"fatal: failed to start MCP subprocess: {exc}", file=sys.stderr)
        return 2

    try:
        for spec in replayable_rows:
            warmup_samples: list[float] = []
            measured_samples: list[float] = []
            query_failures: list[dict[str, Any]] = []

            for idx in range(args.warmup):
                try:
                    start_ns = time.perf_counter_ns()
                    client.search(spec.arguments, timeout_seconds=args.timeout_seconds)
                    elapsed_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
                    warmup_samples.append(elapsed_ms)
                except TimeoutError as exc:
                    failures["timeouts"] += 1
                    query_failures.append(
                        {
                            "type": "timeout",
                            "phase": "warmup",
                            "iteration": idx + 1,
                            "message": str(exc),
                        }
                    )
                except Exception as exc:
                    failures["errors"] += 1
                    query_failures.append(
                        {
                            "type": "error",
                            "phase": "warmup",
                            "iteration": idx + 1,
                            "message": str(exc),
                        }
                    )

            for idx in range(args.repeats):
                try:
                    start_ns = time.perf_counter_ns()
                    client.search(spec.arguments, timeout_seconds=args.timeout_seconds)
                    elapsed_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
                    measured_samples.append(elapsed_ms)
                except TimeoutError as exc:
                    failures["timeouts"] += 1
                    query_failures.append(
                        {
                            "type": "timeout",
                            "phase": "measured",
                            "iteration": idx + 1,
                            "message": str(exc),
                        }
                    )
                except Exception as exc:
                    failures["errors"] += 1
                    query_failures.append(
                        {
                            "type": "error",
                            "phase": "measured",
                            "iteration": idx + 1,
                            "message": str(exc),
                        }
                    )

            stats_ms = summarize_values(measured_samples, include_p99=False)
            delta_p50_ms = None
            if stats_ms is not None:
                delta_p50_ms = stats_ms["p50"] - spec.baseline_response_ms
                all_samples.extend(measured_samples)

            replay_results.append(
                {
                    "rank": spec.rank,
                    "ts": spec.ts,
                    "query_id": spec.query_id,
                    "source": spec.source,
                    "session_hint": spec.session_hint,
                    "baseline_response_ms": spec.baseline_response_ms,
                    "query": spec.raw_query,
                    "arguments": spec.arguments,
                    "warmup_samples_ms": warmup_samples,
                    "measured_samples_ms": measured_samples,
                    "stats_ms": stats_ms,
                    "delta_p50_ms": delta_p50_ms,
                    "success_count": len(measured_samples),
                    "failure_count": len(query_failures),
                    "failures": query_failures,
                }
            )
    finally:
        client.close()

    print_replay_table(replay_results)

    aggregate_stats = summarize_values(all_samples, include_p99=True)
    failure_total = failures["timeouts"] + failures["errors"]

    aggregate = {
        "successful_samples": len(all_samples),
        "stats_ms": aggregate_stats,
        "failure_total": failure_total,
        "timeout_count": failures["timeouts"],
        "error_count": failures["errors"],
    }

    print("\nAggregate")
    print(f"  successful_samples: {aggregate['successful_samples']}")
    if aggregate_stats is None:
        print("  stats_ms: -")
    else:
        print(
            "  stats_ms: "
            f"min={aggregate_stats['min']:.2f} "
            f"p50={aggregate_stats['p50']:.2f} "
            f"p95={aggregate_stats['p95']:.2f} "
            f"p99={aggregate_stats['p99']:.2f} "
            f"max={aggregate_stats['max']:.2f} "
            f"avg={aggregate_stats['avg']:.2f}"
        )
    print(f"  timeout_count: {aggregate['timeout_count']}")
    print(f"  error_count: {aggregate['error_count']}")

    payload = build_output_json(
        args=args,
        config_path=config_path,
        selected_rows=selected_rows,
        replay_results=replay_results,
        aggregate=aggregate,
        failures=failures,
        dry_run=False,
    )
    if args.output_json:
        write_output_json(Path(args.output_json).expanduser(), payload)

    if failure_total > 0:
        print(
            "benchmark completed with replay failures/timeouts; see per-query failure details",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
