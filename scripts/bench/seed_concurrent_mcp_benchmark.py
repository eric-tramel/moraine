#!/usr/bin/env python3
"""Seed an owned ClickHouse sandbox and write an independent concurrent-search oracle."""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import tempfile
from pathlib import Path
from typing import Any, Optional, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

ORACLE_SCHEMA_VERSION = "moraine-concurrent-mcp-oracle-v1"
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SeedFailure(RuntimeError):
    pass


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def clickhouse_query(url: str, sql: str, timeout_s: float = 600.0) -> str:
    parsed = urlparse(url)
    query = parse_qsl(parsed.query, keep_blank_values=True)
    query.append(("query", sql))
    request_url = urlunparse(parsed._replace(query=urlencode(query)))
    request = Request(request_url, method="POST")
    try:
        with urlopen(request, timeout=timeout_s) as response:
            return response.read().decode("utf-8")
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        detail = ""
        if isinstance(exc, HTTPError):
            try:
                detail = exc.read(1024).decode("utf-8", errors="replace")
            except OSError:
                pass
        raise SeedFailure(f"ClickHouse query failed: {detail or type(exc).__name__}") from exc


def public_id(prefix: str, raw: str) -> str:
    encoded = base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")
    return f"{prefix}:{encoded}"


def event_uid(number: int) -> str:
    return f"bench-event-{number:08d}"


def session_id(number: int) -> str:
    return f"bench-session-{number:08d}"


def result_digest(numbers: Sequence[int]) -> str:
    identities = [
        {"event_id": public_id("event", event_uid(number)), "session_id": public_id("session", session_id(number))}
        for number in numbers
    ]
    identities.sort(key=lambda item: (item["session_id"], item["event_id"]))
    encoded = json.dumps(identities, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def case(case_id: str, query: str, numbers: Sequence[int]) -> dict[str, Any]:
    selected = list(numbers)[:10]
    if not selected:
        raise SeedFailure(f"oracle case {case_id} has no seeded result")
    return {"id": case_id, "query": query, "result_count": len(selected), "result_digest": result_digest(selected)}


def corpus_fingerprint(documents: int, measured_cases: int, recovery_cases: int) -> str:
    recipe = {
        "schema": "moraine-concurrent-search-corpus-v1",
        "documents": documents,
        "measured_cases": measured_cases,
        "recovery_cases": recovery_cases,
        "event_prefix": "bench-event",
        "session_prefix": "bench-session",
    }
    encoded = json.dumps(recipe, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def build_oracle(documents: int, measured_cases: int, recovery_cases: int) -> dict[str, Any]:
    warmup_number = documents - 1
    measured: list[dict[str, Any]] = []
    for index in range(measured_cases):
        if index % 2 == 0:
            measured.append(case(f"measured-{index:04d}", f"selective_{index:04d}", [index]))
        else:
            common_numbers = list(range(index, documents, measured_cases))
            common_numbers.sort(
                key=lambda number: (
                    number < measured_cases
                    or measured_cases <= number < measured_cases + recovery_cases
                    or number == warmup_number,
                    number,
                )
            )
            measured.append(case(f"measured-{index:04d}", f"common_{index:04d}", common_numbers))
    recovery = [
        case(f"recovery-{index:04d}", f"recovery_{index:04d}", [measured_cases + index])
        for index in range(recovery_cases)
    ]
    return {
        "schema_version": ORACLE_SCHEMA_VERSION,
        "provenance": "synthetic-search-corpus-v1",
        "dataset": {
            "fingerprint": corpus_fingerprint(documents, measured_cases, recovery_cases),
            "cardinality": documents,
        },
        "warmup": case("warmup", "warmupterm", [warmup_number]),
        "measured": measured,
        "recovery": recovery,
    }


def seed_sql(database: str, documents: int, measured_cases: int, recovery_cases: int) -> str:
    recovery_expr = "' '"
    if recovery_cases:
        recovery_expr = (
            f"if(number >= {measured_cases} AND number < {measured_cases + recovery_cases}, "
            f"concat('recovery_', leftPad(toString(number - {measured_cases}), 4, '0'), ' '), ' ')"
        )
    return f"""
INSERT INTO {database}.search_documents
(
  doc_version, ingested_at, event_uid, compacted_parent_uid, session_id, session_date,
  source_name, harness, inference_provider, endpoint_kind, source_file, source_generation,
  source_line_no, source_offset, source_ref, record_ts, event_class, payload_type,
  actor_role, name, phase, text_content, payload_json, token_usage_json
)
SELECT
  1,
  toDateTime64('2026-01-01 00:00:00', 3),
  concat('bench-event-', leftPad(toString(number), 8, '0')),
  '',
  concat('bench-session-', leftPad(toString(number), 8, '0')),
  toDate('2026-01-01'),
  'benchmark',
  'benchmark',
  'benchmark',
  'generation',
  'synthetic/concurrent-search-v1.jsonl',
  1,
  number + 1,
  number,
  concat('synthetic:', toString(number)),
  '2026-01-01T00:00:00.000Z',
  'message',
  'user',
  'user',
  '',
  '',
  concat(
    'synthetic benchmark document ',
    if(number < {measured_cases}, concat('selective_', leftPad(toString(number), 4, '0'), ' '), ' '),
    concat('common_', leftPad(toString(number % {measured_cases}), 4, '0'), ' '),
    {recovery_expr},
    if(number = {documents - 1}, 'warmupterm', '')
  ),
  '{{}}',
  '{{}}'
FROM numbers({documents})
""".strip()


def truncate(database: str, url: str) -> None:
    for table in ("search_hit_log", "search_query_log", "search_postings", "search_documents"):
        clickhouse_query(url, f"TRUNCATE TABLE {database}.{table}")


def atomic_write(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(value, indent=2, sort_keys=True) + "\n"
    temporary: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
        ) as handle:
            temporary = Path(handle.name)
            handle.write(payload)
            handle.flush()
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("seed", "clean"))
    parser.add_argument("--clickhouse-url", default="http://127.0.0.1:8123")
    parser.add_argument("--database", default="moraine")
    parser.add_argument("--documents", type=positive_int, default=100_000)
    parser.add_argument("--measured-cases", type=positive_int, default=64)
    parser.add_argument("--recovery-cases", type=positive_int, default=2)
    parser.add_argument("--oracle-json", type=Path)
    args = parser.parse_args(argv)
    if not IDENTIFIER_RE.fullmatch(args.database):
        parser.error("database must be a ClickHouse identifier")
    if args.action == "seed" and args.oracle_json is None:
        parser.error("seed requires --oracle-json")
    if args.documents <= args.measured_cases + args.recovery_cases:
        parser.error("documents must exceed measured plus recovery cases")
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    try:
        truncate(args.database, args.clickhouse_url)
        if args.action == "clean":
            return 0
        clickhouse_query(args.clickhouse_url, seed_sql(args.database, args.documents, args.measured_cases, args.recovery_cases))
        observed = clickhouse_query(
            args.clickhouse_url,
            f"SELECT count() FROM {args.database}.search_documents FINAL FORMAT TSVRaw",
        ).strip()
        if observed != str(args.documents):
            raise SeedFailure(f"seed cardinality mismatch: expected {args.documents}, observed {observed}")
        atomic_write(args.oracle_json, build_oracle(args.documents, args.measured_cases, args.recovery_cases))
        print(f"seeded={observed} oracle={args.oracle_json}")
        return 0
    except (SeedFailure, OSError) as exc:
        print(f"seed failed: {exc}", file=__import__("sys").stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
