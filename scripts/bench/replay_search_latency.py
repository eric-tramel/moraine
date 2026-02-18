#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "maturin>=1.6,<2",
#   "psutil>=5.9,<8",
# ]
# ///
from __future__ import annotations

import argparse
import base64
import json
import math
import os
import random
import re
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

try:  # pragma: no cover - import path is runtime-dependent
    import resource
except Exception:  # pragma: no cover - import path is runtime-dependent
    resource = None

try:  # pragma: no cover - import path is runtime-dependent
    import psutil
except Exception:  # pragma: no cover - import path is runtime-dependent
    psutil = None

WINDOW_RE = re.compile(r"^\s*(\d+)\s*([smhdw])\s*$", re.IGNORECASE)
SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
BENCHMARK_REPLAY_SOURCE = "benchmark-replay"
QUERY_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")


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


class ProcessMemoryTracker:
    """Best-effort process RSS/peak tracker for memory-vs-latency benchmarking."""

    def __init__(self) -> None:
        self._process = None
        if psutil is not None:
            try:
                self._process = psutil.Process(os.getpid())
            except Exception:
                self._process = None

    def rss_source(self) -> str:
        return "psutil" if self._process is not None else "unavailable"

    def peak_source(self) -> str:
        return "resource.ru_maxrss" if resource is not None else "unavailable"

    def current_rss_bytes(self) -> Optional[int]:
        if self._process is None:
            return None
        try:
            return int(self._process.memory_info().rss)
        except Exception:
            return None

    def peak_rss_bytes(self) -> Optional[int]:
        if resource is None:
            return None
        try:
            usage = resource.getrusage(resource.RUSAGE_SELF)
            raw_value = int(getattr(usage, "ru_maxrss", 0))
        except Exception:
            return None

        if raw_value <= 0:
            return None

        # ru_maxrss is bytes on macOS and KiB on Linux/BSD.
        if sys.platform == "darwin":
            return raw_value
        return raw_value * 1024


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


def ratio_0_to_1(value: str) -> float:
    parsed = float(value)
    if parsed < 0.0 or parsed > 1.0:
        raise argparse.ArgumentTypeError("value must be in [0.0, 1.0]")
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
            "Replay top-N slow search requests from moraine.search_query_log via the "
            "local moraine_conversations Python package and report observed latency."
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
        help="Timeout per search request (seconds)",
    )
    parser.add_argument(
        "--skip-maturin-develop",
        action="store_true",
        help="Skip running maturin develop before replay",
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
        "--use-search-cache",
        action="store_true",
        help="Allow moraine-conversations search result cache during replay (default: disabled)",
    )
    parser.add_argument(
        "--request-source",
        default=BENCHMARK_REPLAY_SOURCE,
        help=(
            "Source value sent to search_events_json. "
            f"Default is '{BENCHMARK_REPLAY_SOURCE}' to avoid replay logging feedback."
        ),
    )
    parser.add_argument(
        "--parse-json-response",
        action="store_true",
        help=(
            "Parse each search_events_json payload with json.loads during measured runs "
            "(default: disabled to minimize benchmark-side JSON decode overhead)"
        ),
    )
    parser.add_argument(
        "--oracle-quality-check",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Compare replay results against oracle_exact SQL search and report "
            "quality regression metrics (enabled by default)"
        ),
    )
    parser.add_argument(
        "--oracle-k",
        type=non_negative_int,
        default=0,
        help=(
            "Top-K used for oracle quality metrics. "
            "Use 0 (default) to use each query's replay limit."
        ),
    )
    parser.add_argument(
        "--oracle-recall-at-k-threshold",
        type=ratio_0_to_1,
        default=1.0,
        help="Minimum Recall@K gate against oracle results",
    )
    parser.add_argument(
        "--oracle-ndcg-at-k-threshold",
        type=ratio_0_to_1,
        default=0.99,
        help="Minimum NDCG@K gate against oracle results",
    )
    parser.add_argument(
        "--oracle-min-stability-recall",
        type=ratio_0_to_1,
        default=0.95,
        help=(
            "Minimum oracle-vs-oracle Recall@K stability required for strict gating. "
            "Below this, the case is treated as unstable and excluded from regressions."
        ),
    )
    parser.add_argument(
        "--oracle-min-stability-ndcg",
        type=ratio_0_to_1,
        default=0.98,
        help=(
            "Minimum oracle-vs-oracle NDCG@K stability required for strict gating. "
            "Below this, the case is treated as unstable and excluded from regressions."
        ),
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


def bytes_to_mib(value: Optional[int]) -> Optional[float]:
    if value is None:
        return None
    return float(value) / (1024.0 * 1024.0)


def summarize_bytes_as_mib(values: list[int], include_p99: bool) -> Optional[dict[str, float]]:
    if not values:
        return None
    values_mib = [float(value) / (1024.0 * 1024.0) for value in values]
    return summarize_values(values_mib, include_p99=include_p99)


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


def parse_search_payload(payload: str) -> dict[str, Any]:
    parsed = json.loads(payload)
    if not isinstance(parsed, dict):
        raise RuntimeError(
            f"search_events_json returned non-object payload: {type(parsed).__name__}"
        )
    return parsed


def top_ranked_event_uids(payload: dict[str, Any], top_k: int) -> list[str]:
    if top_k <= 0:
        return []
    hits = payload.get("hits")
    if not isinstance(hits, list):
        return []

    ranked: list[str] = []
    seen: set[str] = set()
    for hit in hits:
        if not isinstance(hit, dict):
            continue
        event_uid = hit.get("event_uid")
        if not isinstance(event_uid, str):
            continue
        event_uid = event_uid.strip()
        if not event_uid or event_uid in seen:
            continue
        ranked.append(event_uid)
        seen.add(event_uid)
        if len(ranked) >= top_k:
            break
    return ranked


def dcg(scores: list[float]) -> float:
    total = 0.0
    for idx, score in enumerate(scores):
        if score <= 0.0:
            continue
        total += score / math.log2(idx + 2.0)
    return total


def quality_metrics_for_candidate(
    candidate_payload: dict[str, Any],
    reference_payload: dict[str, Any],
    top_k: int,
) -> dict[str, Any]:
    reference_ranked = top_ranked_event_uids(reference_payload, top_k)
    candidate_ranked = top_ranked_event_uids(candidate_payload, top_k)

    reference_set = set(reference_ranked)
    candidate_set = set(candidate_ranked)

    if not reference_set:
        recall_at_k = 1.0
    else:
        recall_at_k = len(reference_set.intersection(candidate_set)) / float(len(reference_set))

    # Rank-sensitive gain model: earlier reference ranks receive higher gain.
    reference_gain_by_uid = {
        event_uid: float(top_k - rank_idx)
        for rank_idx, event_uid in enumerate(reference_ranked)
        if rank_idx < top_k
    }
    candidate_gains = [reference_gain_by_uid.get(event_uid, 0.0) for event_uid in candidate_ranked]
    ideal_gains = sorted(reference_gain_by_uid.values(), reverse=True)
    idcg = dcg(ideal_gains)
    ndcg_at_k = 1.0 if idcg == 0.0 else dcg(candidate_gains) / idcg

    missing_from_candidate = [uid for uid in reference_ranked if uid not in candidate_set]
    unexpected_in_candidate = [uid for uid in candidate_ranked if uid not in reference_set]

    return {
        "k": top_k,
        "candidate_top_event_uids": candidate_ranked,
        "reference_top_event_uids": reference_ranked,
        "candidate_top_count": len(candidate_ranked),
        "reference_top_count": len(reference_ranked),
        "missing_from_candidate_top_k": missing_from_candidate,
        "unexpected_in_candidate_top_k": unexpected_in_candidate,
        "recall_at_k": recall_at_k,
        "ndcg_at_k": ndcg_at_k,
    }


def evaluate_oracle_quality(
    optimized_payload: dict[str, Any],
    oracle_payload: dict[str, Any],
    top_k: int,
    recall_threshold: float,
    ndcg_threshold: float,
    min_stability_recall: float,
    min_stability_ndcg: float,
    oracle_recheck_payloads: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    oracle_snapshots = [oracle_payload]
    if oracle_recheck_payloads:
        oracle_snapshots.extend(oracle_recheck_payloads)

    optimized_metrics_by_snapshot: list[tuple[str, dict[str, Any]]] = []
    for idx, snapshot in enumerate(oracle_snapshots):
        label = "primary" if idx == 0 else f"recheck_{idx}"
        metrics = quality_metrics_for_candidate(
            candidate_payload=optimized_payload,
            reference_payload=snapshot,
            top_k=top_k,
        )
        optimized_metrics_by_snapshot.append((label, metrics))

    oracle_reference_label, optimized_vs_oracle = max(
        optimized_metrics_by_snapshot,
        key=lambda item: (item[1]["recall_at_k"], item[1]["ndcg_at_k"]),
    )

    effective_recall_threshold = recall_threshold
    effective_ndcg_threshold = ndcg_threshold
    oracle_stability: Optional[dict[str, Any]] = None
    unstable_oracle_window = False
    unstable_reasons: list[str] = []

    if len(oracle_snapshots) > 1:
        min_pair_recall = 1.0
        min_pair_ndcg = 1.0
        worst_pair: Optional[tuple[int, int]] = None
        worst_pair_detail: Optional[dict[str, Any]] = None

        for left_idx in range(len(oracle_snapshots)):
            for right_idx in range(left_idx + 1, len(oracle_snapshots)):
                left = oracle_snapshots[left_idx]
                right = oracle_snapshots[right_idx]
                right_vs_left = quality_metrics_for_candidate(
                    candidate_payload=right,
                    reference_payload=left,
                    top_k=top_k,
                )
                left_vs_right = quality_metrics_for_candidate(
                    candidate_payload=left,
                    reference_payload=right,
                    top_k=top_k,
                )

                pair_recall = min(
                    right_vs_left["recall_at_k"],
                    left_vs_right["recall_at_k"],
                )
                pair_ndcg = min(
                    right_vs_left["ndcg_at_k"],
                    left_vs_right["ndcg_at_k"],
                )

                if (pair_recall, pair_ndcg) < (min_pair_recall, min_pair_ndcg):
                    min_pair_recall = pair_recall
                    min_pair_ndcg = pair_ndcg
                    worst_pair = (left_idx, right_idx)
                    worst_pair_detail = {
                        "left_to_right": left_vs_right,
                        "right_to_left": right_vs_left,
                    }

        effective_recall_threshold = min(recall_threshold, min_pair_recall)
        effective_ndcg_threshold = min(ndcg_threshold, min_pair_ndcg)
        oracle_stability = {
            "snapshot_count": len(oracle_snapshots),
            "worst_pair": {
                "left_index": worst_pair[0] if worst_pair is not None else 0,
                "right_index": worst_pair[1] if worst_pair is not None else 0,
            },
            "worst_pair_detail": worst_pair_detail,
            "recall_at_k": min_pair_recall,
            "ndcg_at_k": min_pair_ndcg,
        }

        if min_pair_recall < min_stability_recall:
            unstable_oracle_window = True
            unstable_reasons.append(
                f"oracle_recall_at_k<{min_stability_recall:.3f}"
            )
        if min_pair_ndcg < min_stability_ndcg:
            unstable_oracle_window = True
            unstable_reasons.append(
                f"oracle_ndcg_at_k<{min_stability_ndcg:.3f}"
            )

    passes_gate = (
        optimized_vs_oracle["recall_at_k"] >= effective_recall_threshold
        and optimized_vs_oracle["ndcg_at_k"] >= effective_ndcg_threshold
    )
    if unstable_oracle_window:
        passes_gate = True

    return {
        "enabled": True,
        "status": "ok",
        "k": top_k,
        "oracle_reference": oracle_reference_label,
        "optimized_top_event_uids": optimized_vs_oracle["candidate_top_event_uids"],
        "oracle_top_event_uids": optimized_vs_oracle["reference_top_event_uids"],
        "optimized_top_count": optimized_vs_oracle["candidate_top_count"],
        "oracle_top_count": optimized_vs_oracle["reference_top_count"],
        "missing_from_optimized_top_k": optimized_vs_oracle["missing_from_candidate_top_k"],
        "unexpected_in_optimized_top_k": optimized_vs_oracle["unexpected_in_candidate_top_k"],
        "recall_at_k": optimized_vs_oracle["recall_at_k"],
        "ndcg_at_k": optimized_vs_oracle["ndcg_at_k"],
        "thresholds": {
            "recall_at_k": recall_threshold,
            "ndcg_at_k": ndcg_threshold,
        },
        "effective_thresholds": {
            "recall_at_k": effective_recall_threshold,
            "ndcg_at_k": effective_ndcg_threshold,
        },
        "oracle_stability": oracle_stability,
        "unstable_oracle_window": unstable_oracle_window,
        "unstable_reasons": unstable_reasons,
        "passes_gate": passes_gate,
    }


def summarize_oracle_quality(
    replay_results: list[dict[str, Any]],
    recall_threshold: float,
    ndcg_threshold: float,
) -> dict[str, Any]:
    quality_items: list[dict[str, Any]] = []
    for row in replay_results:
        quality = row.get("oracle_quality")
        if isinstance(quality, dict) and quality.get("enabled", False):
            quality_items.append(quality)

    if not quality_items:
        return {
            "enabled": False,
            "thresholds": {
                "recall_at_k": recall_threshold,
                "ndcg_at_k": ndcg_threshold,
            },
            "evaluated_case_count": 0,
            "unstable_case_count": 0,
            "error_count": 0,
            "pass_count": 0,
            "regression_count": 0,
            "pass_rate": None,
            "recall_at_k": None,
            "ndcg_at_k": None,
            "k_values": [],
        }

    ok_items = [item for item in quality_items if item.get("status") == "ok"]
    error_items = [item for item in quality_items if item.get("status") == "error"]
    unstable_items = [item for item in ok_items if item.get("unstable_oracle_window", False)]
    stable_items = [item for item in ok_items if not item.get("unstable_oracle_window", False)]
    regression_count = sum(1 for item in stable_items if not item.get("passes_gate", False))
    pass_count = len(stable_items) - regression_count
    pass_rate = None
    if stable_items:
        pass_rate = pass_count / float(len(stable_items))

    recall_values = [float(item["recall_at_k"]) for item in stable_items]
    ndcg_values = [float(item["ndcg_at_k"]) for item in stable_items]
    k_values = sorted(
        {int(item.get("k", 0)) for item in stable_items if int(item.get("k", 0)) > 0}
    )

    return {
        "enabled": True,
        "thresholds": {
            "recall_at_k": recall_threshold,
            "ndcg_at_k": ndcg_threshold,
        },
        "evaluated_case_count": len(stable_items),
        "unstable_case_count": len(unstable_items),
        "error_count": len(error_items),
        "pass_count": pass_count,
        "regression_count": regression_count,
        "pass_rate": pass_rate,
        "recall_at_k": summarize_values(recall_values, include_p99=False),
        "ndcg_at_k": summarize_values(ndcg_values, include_p99=False),
        "k_values": k_values,
    }


def ensure_local_python_binding(repo_root: Path) -> None:
    manifest_path = repo_root / "bindings" / "python" / "moraine_conversations" / "Cargo.toml"
    if not manifest_path.exists():
        raise RuntimeError(f"python binding manifest not found: {manifest_path}")

    maturin_bin = shutil.which("maturin")
    if not maturin_bin:
        raise RuntimeError(
            "maturin was not found in PATH; run this script via `uv run --script` so "
            "its self-declared dependencies are installed"
        )

    result = subprocess.run(
        [
            maturin_bin,
            "develop",
            "--manifest-path",
            str(manifest_path),
            "--locked",
            "--release",
        ],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        stdout = (result.stdout or "").strip()
        detail = stderr or stdout or f"exit code {result.returncode}"
        raise RuntimeError(f"maturin develop failed: {detail}")


def ensure_binding_python_source_on_syspath(repo_root: Path) -> None:
    python_source = repo_root / "bindings" / "python" / "moraine_conversations" / "python"
    if not python_source.is_dir():
        raise RuntimeError(f"python binding source directory not found: {python_source}")

    python_source_str = str(python_source)
    if python_source_str not in sys.path:
        # maturin develop uses a .pth for editable installs, but this process does
        # not restart; add the source path explicitly so import works immediately.
        sys.path.insert(0, python_source_str)


def load_conversation_client_class() -> Any:
    try:
        from moraine_conversations import ConversationClient
    except Exception as exc:  # pragma: no cover - exercised in runtime usage
        raise RuntimeError(
            "failed to import moraine_conversations after local build; "
            "check maturin output and Rust toolchain setup"
        ) from exc
    return ConversationClient


class PackageSearchClient:
    def __init__(
        self,
        conversation_client_cls: Any,
        clickhouse: ClickHouseSettings,
        timeout_seconds: int,
        disable_cache: bool,
        parse_json_response: bool,
        request_source: str,
    ) -> None:
        self.client = conversation_client_cls(
            url=clickhouse.url,
            database=clickhouse.database,
            username=clickhouse.username,
            password=clickhouse.password,
            timeout_seconds=float(timeout_seconds),
            max_results=65535,
        )
        self.disable_cache = disable_cache
        self.parse_json_response = parse_json_response
        self.request_source = request_source

    def search_payload(
        self,
        arguments: dict[str, Any],
        *,
        search_strategy: str = "optimized",
        disable_cache_override: Optional[bool] = None,
    ) -> str:
        disable_cache = (
            self.disable_cache if disable_cache_override is None else disable_cache_override
        )
        return self.client.search_events_json(
            query=str(arguments["query"]),
            limit=int(arguments["limit"]),
            session_id=arguments.get("session_id"),
            min_score=float(arguments["min_score"]),
            min_should_match=int(arguments["min_should_match"]),
            include_tool_events=bool(arguments["include_tool_events"]),
            exclude_codex_mcp=bool(arguments["exclude_codex_mcp"]),
            disable_cache=disable_cache,
            source=self.request_source,
            search_strategy=search_strategy,
        )

    def search(self, arguments: dict[str, Any]) -> Optional[dict[str, Any]]:
        payload = self.search_payload(arguments, search_strategy="optimized")
        if not self.parse_json_response:
            return None

        return parse_search_payload(payload)


def classify_failure(exc: Exception) -> str:
    if isinstance(exc, TimeoutError):
        return "timeout"
    lowered = str(exc).lower()
    if "timeout" in lowered or "timed out" in lowered:
        return "timeout"
    return "error"


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
    print(f"  window: {args.window}")
    print(f"  requested_top_n: {args.top_n}")
    print(f"  include_benchmark_replays: {args.include_benchmark_replays}")
    print(f"  selected_rows: {len(selected_rows)}")
    print(f"  replayable_rows: {len(base_replay_rows)}")
    print(f"  skipped_rows: {skipped}")
    print(f"  query_variant_mode: {args.query_variant_mode}")
    print(f"  max_query_terms: {args.max_query_terms}")
    print(f"  expanded_replay_cases: {len(expanded_replay_rows)}")
    print(f"  use_search_cache: {args.use_search_cache}")
    print(f"  request_source: {args.request_source}")
    print(f"  parse_json_response: {args.parse_json_response}")
    print(f"  oracle_quality_check: {args.oracle_quality_check}")
    if args.oracle_quality_check:
        oracle_k = args.oracle_k if args.oracle_k > 0 else "query_limit"
        print(f"  oracle_k: {oracle_k}")
        print(f"  oracle_recall_at_k_threshold: {args.oracle_recall_at_k_threshold:.3f}")
        print(f"  oracle_ndcg_at_k_threshold: {args.oracle_ndcg_at_k_threshold:.3f}")
        print(f"  oracle_min_stability_recall: {args.oracle_min_stability_recall:.3f}")
        print(f"  oracle_min_stability_ndcg: {args.oracle_min_stability_ndcg:.3f}")
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
        f"{'rank':>4} {'case':>7} {'terms':>5} {'baseline':>9} {'p50':>8} {'p95':>8} "
        f"{'delta_p50':>10} {'min':>8} {'max':>8} {'rss+p50MiB':>10} {'peak+maxMiB':>12} "
        f"{'q_rec':>7} {'q_ndcg':>7} {'q_ok':>5} {'ok':>5} {'fail':>5}  query"
    )
    print(header)
    print("-" * len(header))
    for result in results:
        stats = result.get("stats_ms") or {}
        memory_stats = result.get("memory_stats_mib") or {}
        rss_growth_stats = memory_stats.get("rss_growth") or {}
        peak_delta_stats = memory_stats.get("peak_delta") or {}
        quality = result.get("oracle_quality") or {}
        quality_status = quality.get("status")
        if quality_status == "ok":
            quality_recall = f"{quality.get('recall_at_k', 0.0):.3f}"
            quality_ndcg = f"{quality.get('ndcg_at_k', 0.0):.3f}"
            quality_ok = "yes" if quality.get("passes_gate") else "no"
        elif quality_status == "error":
            quality_recall = "error"
            quality_ndcg = "error"
            quality_ok = "no"
        else:
            quality_recall = "-"
            quality_ndcg = "-"
            quality_ok = "-"
        line = (
            f"{result['rank']:>4} "
            f"{str(result.get('variant_label', 'orig')):>7} "
            f"{str(result.get('variant_term_count', '-')):>5} "
            f"{result['baseline_response_ms']:>9.2f} "
            f"{format_optional_ms(stats.get('p50')):>8} "
            f"{format_optional_ms(stats.get('p95')):>8} "
            f"{format_optional_ms(result.get('delta_p50_ms')):>10} "
            f"{format_optional_ms(stats.get('min')):>8} "
            f"{format_optional_ms(stats.get('max')):>8} "
            f"{format_optional_ms(rss_growth_stats.get('p50')):>10} "
            f"{format_optional_ms(peak_delta_stats.get('max')):>12} "
            f"{quality_recall:>7} "
            f"{quality_ndcg:>7} "
            f"{quality_ok:>5} "
            f"{result['success_count']:>5} "
            f"{result['failure_count']:>5}  "
            f"{preview_query(result['query'])}"
        )
        print(line)


def print_oracle_quality_summary(quality: dict[str, Any]) -> None:
    print("\nOracle Quality")
    if not quality.get("enabled"):
        print("  enabled: false")
        return

    print("  enabled: true")
    print(
        "  thresholds: "
        f"recall_at_k>={quality['thresholds']['recall_at_k']:.3f} "
        f"ndcg_at_k>={quality['thresholds']['ndcg_at_k']:.3f}"
    )
    print(f"  evaluated_case_count: {quality['evaluated_case_count']}")
    print(f"  unstable_case_count: {quality.get('unstable_case_count', 0)}")
    print(f"  pass_count: {quality['pass_count']}")
    print(f"  regression_count: {quality['regression_count']}")
    print(f"  error_count: {quality['error_count']}")

    if quality.get("pass_rate") is None:
        print("  pass_rate: -")
    else:
        print(f"  pass_rate: {quality['pass_rate']:.3f}")

    k_values = quality.get("k_values") or []
    if k_values:
        print(f"  k_values: {','.join(str(value) for value in k_values)}")
    else:
        print("  k_values: -")

    recall_stats = quality.get("recall_at_k")
    if recall_stats is None:
        print("  recall_at_k: -")
    else:
        print(
            "  recall_at_k: "
            f"min={recall_stats['min']:.3f} "
            f"p50={recall_stats['p50']:.3f} "
            f"p95={recall_stats['p95']:.3f} "
            f"max={recall_stats['max']:.3f} "
            f"avg={recall_stats['avg']:.3f}"
        )

    ndcg_stats = quality.get("ndcg_at_k")
    if ndcg_stats is None:
        print("  ndcg_at_k: -")
    else:
        print(
            "  ndcg_at_k: "
            f"min={ndcg_stats['min']:.3f} "
            f"p50={ndcg_stats['p50']:.3f} "
            f"p95={ndcg_stats['p95']:.3f} "
            f"max={ndcg_stats['max']:.3f} "
            f"avg={ndcg_stats['avg']:.3f}"
        )


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
                "skip_maturin_develop": args.skip_maturin_develop,
                "include_benchmark_replays": args.include_benchmark_replays,
                "query_variant_mode": args.query_variant_mode,
                "max_query_terms": args.max_query_terms,
                "use_search_cache": args.use_search_cache,
                "request_source": args.request_source,
                "parse_json_response": args.parse_json_response,
                "oracle_quality_check": args.oracle_quality_check,
                "oracle_k": args.oracle_k,
                "oracle_recall_at_k_threshold": args.oracle_recall_at_k_threshold,
                "oracle_ndcg_at_k_threshold": args.oracle_ndcg_at_k_threshold,
                "oracle_min_stability_recall": args.oracle_min_stability_recall,
                "oracle_min_stability_ndcg": args.oracle_min_stability_ndcg,
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
            "try increasing --window, confirm telemetry is present, or pass "
            "--include-benchmark-replays",
            file=sys.stderr,
        )
        empty_payload = build_output_json(
            args=args,
            config_path=config_path,
            selected_rows=[],
            replay_results=[],
            aggregate={
                "stats_ms": None,
                "successful_samples": 0,
                "memory": None,
                "quality": None,
            },
            failures={"timeouts": 0, "errors": 1},
            dry_run=args.dry_run,
        )
        if args.output_json:
            write_output_json(Path(args.output_json).expanduser(), empty_payload)
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
            replay_results=[],
            aggregate={
                "stats_ms": None,
                "successful_samples": 0,
                "memory": None,
                "quality": None,
            },
            failures={"timeouts": 0, "errors": 0},
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
            replay_results=[],
            aggregate={
                "stats_ms": None,
                "successful_samples": 0,
                "memory": None,
                "quality": None,
            },
            failures={"timeouts": 0, "errors": 1},
            dry_run=False,
        )
        if args.output_json:
            write_output_json(Path(args.output_json).expanduser(), payload)
        return 2

    if args.skip_maturin_develop:
        print("Skipping maturin develop (per --skip-maturin-develop)")
    else:
        print("Building local moraine_conversations binding via maturin develop...")
        try:
            ensure_local_python_binding(repo_root)
        except Exception as exc:
            print(f"fatal: failed to build local python binding: {exc}", file=sys.stderr)
            return 2

    try:
        ensure_binding_python_source_on_syspath(repo_root)
        conversation_client_cls = load_conversation_client_class()
        client = PackageSearchClient(
            conversation_client_cls=conversation_client_cls,
            clickhouse=ch_cfg,
            timeout_seconds=args.timeout_seconds,
            disable_cache=not args.use_search_cache,
            parse_json_response=args.parse_json_response,
            request_source=args.request_source,
        )
    except Exception as exc:
        print(f"fatal: failed to initialize local search client: {exc}", file=sys.stderr)
        return 2

    replay_results: list[dict[str, Any]] = []
    all_samples: list[float] = []
    all_rss_growth_bytes: list[int] = []
    all_peak_delta_bytes: list[int] = []
    failures = {"timeouts": 0, "errors": 0}
    memory_tracker = ProcessMemoryTracker()
    benchmark_rss_start_bytes = memory_tracker.current_rss_bytes()
    benchmark_peak_start_bytes = memory_tracker.peak_rss_bytes()

    for spec in replayable_rows:
        warmup_samples: list[float] = []
        measured_samples: list[float] = []
        measured_rss_growth_bytes: list[int] = []
        measured_peak_delta_bytes: list[int] = []
        query_failures: list[dict[str, Any]] = []

        for idx in range(args.warmup):
            try:
                start_ns = time.perf_counter_ns()
                client.search(spec.arguments)
                elapsed_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
                warmup_samples.append(elapsed_ms)
            except Exception as exc:
                failure_type = classify_failure(exc)
                failures["timeouts" if failure_type == "timeout" else "errors"] += 1
                query_failures.append(
                    {
                        "type": failure_type,
                        "phase": "warmup",
                        "iteration": idx + 1,
                        "message": str(exc),
                    }
                )

        for idx in range(args.repeats):
            try:
                rss_before_bytes = memory_tracker.current_rss_bytes()
                peak_before_bytes = memory_tracker.peak_rss_bytes()
                start_ns = time.perf_counter_ns()
                client.search(spec.arguments)
                elapsed_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
                rss_after_bytes = memory_tracker.current_rss_bytes()
                peak_after_bytes = memory_tracker.peak_rss_bytes()
                measured_samples.append(elapsed_ms)

                if rss_before_bytes is not None and rss_after_bytes is not None:
                    measured_rss_growth_bytes.append(max(0, rss_after_bytes - rss_before_bytes))
                if peak_before_bytes is not None and peak_after_bytes is not None:
                    measured_peak_delta_bytes.append(max(0, peak_after_bytes - peak_before_bytes))
            except Exception as exc:
                failure_type = classify_failure(exc)
                failures["timeouts" if failure_type == "timeout" else "errors"] += 1
                query_failures.append(
                    {
                        "type": failure_type,
                        "phase": "measured",
                        "iteration": idx + 1,
                        "message": str(exc),
                    }
                )

        stats_ms = summarize_values(measured_samples, include_p99=False)
        memory_stats_mib = {
            "rss_growth": summarize_bytes_as_mib(measured_rss_growth_bytes, include_p99=False),
            "peak_delta": summarize_bytes_as_mib(measured_peak_delta_bytes, include_p99=False),
        }
        delta_p50_ms = None
        if stats_ms is not None:
            delta_p50_ms = stats_ms["p50"] - spec.baseline_response_ms
            all_samples.extend(measured_samples)
            all_rss_growth_bytes.extend(measured_rss_growth_bytes)
            all_peak_delta_bytes.extend(measured_peak_delta_bytes)

        oracle_quality: dict[str, Any] = {"enabled": False, "status": "disabled"}
        if args.oracle_quality_check:
            top_k = args.oracle_k if args.oracle_k > 0 else int(spec.arguments["limit"])
            top_k = max(1, top_k)
            try:
                oracle_payload_primary = client.search_payload(
                    spec.arguments,
                    search_strategy="oracle_exact",
                    disable_cache_override=True,
                )
                optimized_payload = client.search_payload(
                    spec.arguments,
                    search_strategy="optimized",
                )
                oracle_payload_recheck = client.search_payload(
                    spec.arguments,
                    search_strategy="oracle_exact",
                    disable_cache_override=True,
                )
                oracle_payload_recheck_2 = client.search_payload(
                    spec.arguments,
                    search_strategy="oracle_exact",
                    disable_cache_override=True,
                )
                oracle_quality = evaluate_oracle_quality(
                    optimized_payload=parse_search_payload(optimized_payload),
                    oracle_payload=parse_search_payload(oracle_payload_primary),
                    top_k=top_k,
                    recall_threshold=args.oracle_recall_at_k_threshold,
                    ndcg_threshold=args.oracle_ndcg_at_k_threshold,
                    min_stability_recall=args.oracle_min_stability_recall,
                    min_stability_ndcg=args.oracle_min_stability_ndcg,
                    oracle_recheck_payloads=[
                        parse_search_payload(oracle_payload_recheck),
                        parse_search_payload(oracle_payload_recheck_2),
                    ],
                )
            except Exception as exc:
                oracle_quality = {
                    "enabled": True,
                    "status": "error",
                    "k": top_k,
                    "message": str(exc),
                    "thresholds": {
                        "recall_at_k": args.oracle_recall_at_k_threshold,
                        "ndcg_at_k": args.oracle_ndcg_at_k_threshold,
                    },
                }

        replay_results.append(
            {
                "rank": spec.rank,
                "variant_label": spec.variant_label,
                "variant_term_count": spec.variant_term_count,
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
                "memory_samples_mib": {
                    "rss_growth": [value / (1024.0 * 1024.0) for value in measured_rss_growth_bytes],
                    "peak_delta": [value / (1024.0 * 1024.0) for value in measured_peak_delta_bytes],
                },
                "memory_stats_mib": memory_stats_mib,
                "success_count": len(measured_samples),
                "failure_count": len(query_failures),
                "failures": query_failures,
                "oracle_quality": oracle_quality,
            }
        )

    print_replay_table(replay_results)

    aggregate_stats = summarize_values(all_samples, include_p99=True)
    benchmark_rss_end_bytes = memory_tracker.current_rss_bytes()
    benchmark_peak_end_bytes = memory_tracker.peak_rss_bytes()

    benchmark_rss_delta_bytes = None
    if benchmark_rss_start_bytes is not None and benchmark_rss_end_bytes is not None:
        benchmark_rss_delta_bytes = benchmark_rss_end_bytes - benchmark_rss_start_bytes

    benchmark_peak_delta_bytes = None
    if benchmark_peak_start_bytes is not None and benchmark_peak_end_bytes is not None:
        benchmark_peak_delta_bytes = max(0, benchmark_peak_end_bytes - benchmark_peak_start_bytes)

    aggregate_memory = {
        "rss_source": memory_tracker.rss_source(),
        "peak_source": memory_tracker.peak_source(),
        "benchmark_rss_bytes": {
            "start": benchmark_rss_start_bytes,
            "end": benchmark_rss_end_bytes,
            "delta": benchmark_rss_delta_bytes,
        },
        "benchmark_peak_rss_bytes": {
            "start": benchmark_peak_start_bytes,
            "end": benchmark_peak_end_bytes,
            "delta": benchmark_peak_delta_bytes,
        },
        "benchmark_rss_mib": {
            "start": bytes_to_mib(benchmark_rss_start_bytes),
            "end": bytes_to_mib(benchmark_rss_end_bytes),
            "delta": bytes_to_mib(benchmark_rss_delta_bytes),
        },
        "benchmark_peak_rss_mib": {
            "start": bytes_to_mib(benchmark_peak_start_bytes),
            "end": bytes_to_mib(benchmark_peak_end_bytes),
            "delta": bytes_to_mib(benchmark_peak_delta_bytes),
        },
        "query_rss_growth_mib": summarize_bytes_as_mib(all_rss_growth_bytes, include_p99=True),
        "query_peak_delta_mib": summarize_bytes_as_mib(all_peak_delta_bytes, include_p99=True),
    }

    runtime_failure_total = failures["timeouts"] + failures["errors"]
    quality_summary = summarize_oracle_quality(
        replay_results=replay_results,
        recall_threshold=args.oracle_recall_at_k_threshold,
        ndcg_threshold=args.oracle_ndcg_at_k_threshold,
    )
    quality_failure_total = 0
    if quality_summary.get("enabled", False):
        quality_failure_total = int(quality_summary["regression_count"]) + int(
            quality_summary["error_count"]
        )
    failure_total = runtime_failure_total + quality_failure_total

    aggregate = {
        "successful_samples": len(all_samples),
        "stats_ms": aggregate_stats,
        "memory": aggregate_memory,
        "quality": quality_summary,
        "runtime_failure_total": runtime_failure_total,
        "quality_failure_total": quality_failure_total,
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
    print(
        "  memory_sources: "
        f"rss={aggregate_memory['rss_source']} "
        f"peak={aggregate_memory['peak_source']}"
    )

    benchmark_rss_mib = aggregate_memory["benchmark_rss_mib"]
    if benchmark_rss_mib["start"] is None or benchmark_rss_mib["end"] is None:
        print("  benchmark_rss_mib: -")
    else:
        print(
            "  benchmark_rss_mib: "
            f"start={benchmark_rss_mib['start']:.2f} "
            f"end={benchmark_rss_mib['end']:.2f} "
            f"delta={benchmark_rss_mib['delta']:.2f}"
        )

    benchmark_peak_mib = aggregate_memory["benchmark_peak_rss_mib"]
    if benchmark_peak_mib["start"] is None or benchmark_peak_mib["end"] is None:
        print("  benchmark_peak_rss_mib: -")
    else:
        print(
            "  benchmark_peak_rss_mib: "
            f"start={benchmark_peak_mib['start']:.2f} "
            f"end={benchmark_peak_mib['end']:.2f} "
            f"delta={benchmark_peak_mib['delta']:.2f}"
        )

    query_rss_growth_mib = aggregate_memory["query_rss_growth_mib"]
    if query_rss_growth_mib is None:
        print("  query_rss_growth_mib: -")
    else:
        print(
            "  query_rss_growth_mib: "
            f"min={query_rss_growth_mib['min']:.2f} "
            f"p50={query_rss_growth_mib['p50']:.2f} "
            f"p95={query_rss_growth_mib['p95']:.2f} "
            f"p99={query_rss_growth_mib['p99']:.2f} "
            f"max={query_rss_growth_mib['max']:.2f} "
            f"avg={query_rss_growth_mib['avg']:.2f}"
        )

    query_peak_delta_mib = aggregate_memory["query_peak_delta_mib"]
    if query_peak_delta_mib is None:
        print("  query_peak_delta_mib: -")
    else:
        print(
            "  query_peak_delta_mib: "
            f"min={query_peak_delta_mib['min']:.2f} "
            f"p50={query_peak_delta_mib['p50']:.2f} "
            f"p95={query_peak_delta_mib['p95']:.2f} "
            f"p99={query_peak_delta_mib['p99']:.2f} "
            f"max={query_peak_delta_mib['max']:.2f} "
            f"avg={query_peak_delta_mib['avg']:.2f}"
        )

    print_oracle_quality_summary(quality_summary)

    if quality_summary.get("enabled", False):
        print(f"  quality_failure_total: {quality_failure_total}")

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

    if runtime_failure_total > 0 or quality_failure_total > 0:
        if runtime_failure_total > 0 and quality_failure_total > 0:
            message = (
                "benchmark completed with replay failures/timeouts and oracle quality "
                "regressions/errors; see per-query failure details"
            )
        elif runtime_failure_total > 0:
            message = "benchmark completed with replay failures/timeouts; see per-query failure details"
        else:
            message = (
                "benchmark completed with oracle quality regressions/errors; "
                "see per-query quality details"
            )
        print(message, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
