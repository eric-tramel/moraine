#!/usr/bin/env python3
"""Run Moraine's fixed-resource end-to-end search performance suite."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence
from urllib.parse import quote
from urllib.request import Request, urlopen

from performance_fixtures import (
    FreshSeedTarget,
    build_append_probe_events,
    build_recipe,
    mixed_control_schedules,
    open_event_schedule,
    open_query_schedule,
    required_split_usage,
    seed_publication_control_sql,
    seed_search_sql,
    validate_recipe,
    validate_split_usage,
)
from performance_protocol import (
    PAIR_ORDER,
    ProtocolError,
    _validate_resources,
    compare_manifests,
    create_build_identity,
    create_build_recipe,
    create_scenario_result,
    create_suite_definition,
    create_suite_manifest,
    evaluate_repeatability,
    load_document,
    policy_document,
    load_suite_manifest,
    sha256_bytes,
    sha256_json,
    resource_gate_passes,
    schedule_gate_passes,
    semantic_oracle_sha256,
    validate_document,
    write_json_atomic,
)
from native_central_burst import (
    DEFAULT_COLD_REPETITIONS as NATIVE_BURST_DEFAULT_COLD_REPETITIONS,
    DEFAULT_COLD_P95_LIMIT_MS as NATIVE_BURST_DEFAULT_COLD_P95_LIMIT_MS,
    DEFAULT_MAX_LATENCY_MS as NATIVE_BURST_DEFAULT_MAX_LATENCY_MS,
    DEFAULT_MIN_COLD_SAMPLES as NATIVE_BURST_DEFAULT_MIN_COLD_SAMPLES,
    DEFAULT_MODES as NATIVE_BURST_DEFAULT_MODES,
    DEFAULT_WARM_P95_LIMIT_MS as NATIVE_BURST_DEFAULT_WARM_P95_LIMIT_MS,
    NativeBurstFailure,
    run_native_central_burst,
    validate_native_burst_artifact,
    write_native_burst_artifact,
)
from performance_runtime import (
    BuildIdentity,
    FixedEnvelope,
    LocalEnvelope,
    RuntimeFailure,
    build_release_binaries_in_docker,
    ensure_runtime_build_image,
    non_authoritative_resource_evidence,
    run_busy_child_proof,
    run_id,
    start_owned_sandbox,
    verify_process_binary,
)
from performance_scenarios import (
    ScenarioError,
    ScenarioResult,
    make_owned_sandbox_mixed_arm_factory,
    make_owned_sandbox_qps_runtime_factory,
    make_owned_sandbox_query_load,
    make_owned_sandbox_ttr_runtime_factory,
    run_mixed_scenario,
    run_owned_sandbox_append_probe,
    run_owned_sandbox_etd_scenario,
    run_qps_scenario,
    run_ttr_scenario,
)


PUBLICATION_CAPTURE_HEAD_COUNTS = (1, 10_000, 100_000)
PUBLICATION_CAPTURE_WARMUP_REPETITIONS = 2
PUBLICATION_CAPTURE_REPETITIONS = 10
PUBLICATION_APPEND_POLL_INTERVAL_S = 0.05
PUBLICATION_APPEND_WARMUP_MIN_TIMEOUT_S = 30.0
PUBLICATION_CONTROL_TABLE_PATTERN = (
    "(?i)publish|append.*(fence|control)|checkpoint|generation_readiness"
)
SOURCE_HOST_PHYSICAL_TABLES = (
    "raw_events",
    "events",
    "event_links",
    "tool_io",
    "ingest_errors",
    "search_documents",
    "search_postings",
)
SOURCE_HOST_COLUMN_RESOURCE_FIELDS = (
    "rows",
    "active_parts",
    "compressed_bytes",
    "uncompressed_bytes",
    "bytes_on_disk",
)

SUITE_ROOT = Path(__file__).resolve().parents[2]
SCENARIOS = ("qps", "ttr", "etd_idle", "etd_loaded", "mixed")
IMAGE_RECIPE_PATHS = (
    "scripts/dev/sandbox/Dockerfile",
    "scripts/dev/sandbox/compose.yaml",
    "scripts/bench/compose.performance.yaml",
    "scripts/dev/sandbox/entrypoint.sh",
    "scripts/dev/sandbox/performance-entrypoint.sh",
)


class SuiteFailure(RuntimeError):
    """The suite cannot produce a truthful, complete artifact."""


def _git_commit(repo: Path) -> str:
    process = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
    )
    commit = process.stdout.strip()
    if process.returncode or len(commit) != 40:
        raise SuiteFailure(f"cannot resolve immutable commit for {repo}")
    return commit


def _require_clean(repo: Path) -> None:
    process = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=repo,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
    )
    if process.returncode:
        raise SuiteFailure(f"cannot inspect worktree {repo}")
    if process.stdout.strip():
        raise SuiteFailure(f"benchmark worktree is dirty: {repo}")


def _clickhouse_query(
    url: str,
    sql: str,
    *,
    timeout_s: float = 600.0,
    query_id: Optional[str] = None,
) -> str:
    endpoint = url
    if query_id is not None:
        if not query_id or len(query_id) > 160 or any(
            character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
            for character in query_id
        ):
            raise SuiteFailure("ClickHouse query ID is invalid")
        separator = "&" if "?" in endpoint else "?"
        endpoint = f"{endpoint}{separator}query_id={quote(query_id, safe='')}"
    request = Request(
        endpoint,
        data=sql.encode("utf-8"),
        headers={"Content-Type": "text/plain"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout_s) as response:
            return response.read().decode("utf-8").strip()
    except OSError as error:
        raise SuiteFailure(f"ClickHouse request failed: {type(error).__name__}") from error


def _seed_owned_sandbox(sandbox: Any, recipe: Mapping[str, Any]) -> None:
    url = f"http://127.0.0.1:{sandbox.clickhouse_port}"
    database = "moraine"
    existing = _clickhouse_query(url, f"SELECT count() FROM {database}.search_documents")
    if existing != "0":
        raise SuiteFailure(f"fresh owned volume is not empty: observed {existing} documents")
    target = FreshSeedTarget(
        database=database,
        reset_id=sandbox.sandbox_id,
        sandbox_owned=True,
        fresh_volume=True,
        empty_database=True,
    )
    _clickhouse_query(url, seed_search_sql(target, recipe))
    publication_schema = _clickhouse_query(
        url,
        "SELECT count() FROM system.tables WHERE database = 'moraine' "
        "AND name = 'published_source_generations'",
    )
    if publication_schema not in {"0", "1"}:
        raise SuiteFailure("publication schema preflight returned an invalid count")
    if publication_schema == "1":
        for statement in seed_publication_control_sql(target, recipe):
            _clickhouse_query(url, statement)
    sandbox.reconcile_seeded_read_model()
    expected = recipe["corpus"]["document_count"]
    observed = _clickhouse_query(url, f"SELECT count() FROM {database}.search_documents")
    if observed != str(expected):
        raise SuiteFailure(f"seed cardinality mismatch: expected {expected}, observed {observed}")
    projection_ready = _clickhouse_query(
        url,
        f"""SELECT if(count() = 0, 0, max(ready))
FROM {database}.mcp_open_projection_state FINAL
WHERE state_key = 'global'""",
    )
    projection_dirty = _clickhouse_query(
        url,
        f"""SELECT countIf(dirty.dirty_revision > ifNull(published.dirty_revision, 0))
FROM
(
  SELECT session_id, dirty_revision
  FROM {database}.mcp_open_dirty_sessions FINAL
  WHERE notEmpty(session_id)
) AS dirty
LEFT JOIN
(
  SELECT session_id, dirty_revision
  FROM {database}.mcp_open_sessions FINAL
) AS published ON published.session_id = dirty.session_id""",
    )
    if projection_ready != "1" or projection_dirty != "0":
        raise SuiteFailure("seeded MCP read model did not reconcile completely")
    sandbox.checkpoint("seeded")


def _publication_head_snapshot(url: str, database: str = "moraine") -> dict[str, int]:
    available = _clickhouse_query(
        url,
        "SELECT count() FROM system.columns "
        f"WHERE database = '{database}' "
        "AND table = 'published_source_generations' "
        "AND name = 'publication_revision'",
    )
    if available != "1":
        raise SuiteFailure("source-publication probe requires publication-aware schema")
    raw = _clickhouse_query(
        url,
        f"SELECT count(), ifNull(max(publication_revision), 0) "
        f"FROM {database}.published_source_generations FORMAT TSVRaw",
    )
    fields = raw.split("\t")
    if len(fields) != 2:
        raise SuiteFailure("publication head snapshot has an invalid shape")
    try:
        rows, revision = (int(field) for field in fields)
    except ValueError as error:
        raise SuiteFailure("publication head snapshot is not numeric") from error
    if rows < 0 or revision < 0:
        raise SuiteFailure("publication head snapshot contains a negative counter")
    return {"row_count": rows, "max_publication_revision": revision}


def _publication_control_resources(
    url: str, database: str = "moraine"
) -> dict[str, dict[str, int]]:
    raw = _clickhouse_query(
        url,
        "SELECT table, sum(rows) AS rows, count() AS active_parts, "
        "sum(data_compressed_bytes) AS compressed_bytes "
        "FROM system.parts "
        f"WHERE active AND database = '{database}' "
        f"AND match(table, '{PUBLICATION_CONTROL_TABLE_PATTERN}') "
        "GROUP BY table ORDER BY table FORMAT JSONEachRow",
    )
    resources: dict[str, dict[str, int]] = {}
    for line in raw.splitlines():
        try:
            row = json.loads(line)
            table = row["table"]
            values = {
                name: int(row[name])
                for name in ("rows", "active_parts", "compressed_bytes")
            }
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise SuiteFailure("publication control resource row is malformed") from error
        if (
            not isinstance(table, str)
            or not table
            or table in resources
            or any(value < 0 for value in values.values())
        ):
            raise SuiteFailure("publication control resource row is invalid")
        resources[table] = values
    return resources


def _source_host_column_resources(
    url: str, database: str = "moraine"
) -> dict[str, dict[str, int]]:
    encoded_tables = ", ".join(f"'{table}'" for table in SOURCE_HOST_PHYSICAL_TABLES)
    raw = _clickhouse_query(
        url,
        "SELECT table, sum(rows) AS rows, count() AS active_parts, "
        "sum(column_data_compressed_bytes) AS compressed_bytes, "
        "sum(column_data_uncompressed_bytes) AS uncompressed_bytes, "
        "sum(column_bytes_on_disk) AS bytes_on_disk "
        "FROM system.parts_columns "
        f"WHERE active AND database = '{database}' AND column = 'source_host' "
        f"AND table IN ({encoded_tables}) "
        "GROUP BY table ORDER BY table FORMAT JSONEachRow",
    )
    resources = {
        table: {
            "rows": 0,
            "active_parts": 0,
            "compressed_bytes": 0,
            "uncompressed_bytes": 0,
            "bytes_on_disk": 0,
        }
        for table in SOURCE_HOST_PHYSICAL_TABLES
    }
    observed: set[str] = set()
    for line in raw.splitlines():
        try:
            row = json.loads(line)
            table = row["table"]
            values = {
                name: int(row[name])
                for name in SOURCE_HOST_COLUMN_RESOURCE_FIELDS
            }
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise SuiteFailure("source_host column resource row is malformed") from error
        if (
            table not in resources
            or table in observed
            or any(value < 0 for value in values.values())
        ):
            raise SuiteFailure("source_host column resource row is invalid")
        observed.add(table)
        resources[table] = values
    return resources


def _nearest_rank(values: Sequence[float], percentile: float) -> float:
    if not values:
        raise SuiteFailure("source-publication probe has no latency samples")
    ordered = sorted(values)
    index = max(0, math.ceil(percentile / 100.0 * len(ordered)) - 1)
    return float(ordered[index])


def _latency_summary(values: Sequence[float]) -> dict[str, Any]:
    normalized = [float(value) for value in values]
    if not normalized or any(
        not math.isfinite(value) or value < 0 for value in normalized
    ):
        raise SuiteFailure("publication capture latency samples are invalid")
    return {
        "sample_count": len(normalized),
        "p50_ms": _nearest_rank(normalized, 50.0),
        "p95_ms": _nearest_rank(normalized, 95.0),
        "max_ms": max(normalized),
        "raw_ms": normalized,
    }


def _logical_publication_head_snapshot(
    url: str, database: str = "moraine"
) -> dict[str, int]:
    raw = _clickhouse_query(
        url,
        f"SELECT count(), ifNull(max(publication_revision), 0) "
        f"FROM {database}.v_current_published_source_generations FORMAT TSVRaw",
    )
    fields = raw.split("\t")
    if len(fields) != 2:
        raise SuiteFailure("logical publication head snapshot has an invalid shape")
    try:
        rows, revision = (int(field) for field in fields)
    except ValueError as error:
        raise SuiteFailure("logical publication head snapshot is not numeric") from error
    if rows < 0 or revision < 0:
        raise SuiteFailure("logical publication head snapshot contains a negative counter")
    return {"head_count": rows, "max_publication_revision": revision}


def _seed_publication_heads_to(
    url: str,
    *,
    current_count: int,
    target_count: int,
    database: str = "moraine",
) -> dict[str, int]:
    if current_count < 1 or target_count < current_count:
        raise SuiteFailure("publication capture scaling bounds are invalid")
    initial = _logical_publication_head_snapshot(url, database)
    if initial["head_count"] != current_count:
        raise SuiteFailure(
            "publication capture scale seed started from an unexpected head count"
        )
    additional = target_count - current_count
    if additional:
        first_revision = initial["max_publication_revision"] + 1
        _clickhouse_query(
            url,
            f"""
INSERT INTO {database}.published_source_generations
(
  source_host, source_name, source_file, source_generation,
  publication_revision, publisher_id, operation_id
)
SELECT
  '',
  'performance-scale',
  concat('publication-scale-', leftPad(toString(number + {current_count}), 12, '0'), '.jsonl'),
  toUInt32(1),
  toUInt64(number + {first_revision}),
  'performance-scale',
  concat('performance-scale:', toString(number + {current_count}))
FROM numbers({additional})
""".strip(),
        )
    snapshot = _logical_publication_head_snapshot(url, database)
    if snapshot != {
        "head_count": target_count,
        "max_publication_revision": initial["max_publication_revision"] + additional,
    }:
        raise SuiteFailure(
            "publication capture scale seed differs: "
            f"expected {target_count}, observed {snapshot}"
        )
    return snapshot


def _publication_capture_sql(database: str = "moraine") -> str:
    quoted_database = database.replace("`", "``")
    return f"""/* moraine:publication_snapshot:capture */
SELECT
  toUInt64(ifNull(max(publication_revision), 0)) AS publication_revision
FROM `{quoted_database}`.`published_source_generations`
FORMAT JSONEachRow"""


def _validate_publication_capture_result(
    raw: str, *, publication_revision: int
) -> None:
    try:
        lines = raw.splitlines()
        if len(lines) != 1:
            raise ValueError("capture row count differs")
        row = json.loads(lines[0])
        if set(row) != {"publication_revision"}:
            raise ValueError("capture fields differ")
        if int(row["publication_revision"]) != publication_revision:
            raise ValueError("capture values differ")
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise SuiteFailure("publication capture result is malformed") from error


def _publication_capture_query_log_samples(
    url: str, query_ids: Sequence[str]
) -> list[dict[str, Any]]:
    if not query_ids or len(query_ids) != len(set(query_ids)):
        raise SuiteFailure("publication capture query IDs are invalid")
    _clickhouse_query(url, "SYSTEM FLUSH LOGS")
    encoded_ids = ", ".join(f"'{query_id}'" for query_id in query_ids)
    raw = _clickhouse_query(
        url,
        f"""SELECT
  query_id,
  type,
  toFloat64(query_duration_ms) AS query_duration_ms,
  toUInt64(read_rows) AS read_rows,
  toUInt64(read_bytes) AS read_bytes,
  toUInt64(result_rows) AS result_rows,
  toUInt64(greatest(memory_usage, 0)) AS memory_usage_bytes
FROM system.query_log
WHERE query_id IN ({encoded_ids})
  AND type IN ('QueryFinish', 'ExceptionBeforeStart', 'ExceptionWhileProcessing')
ORDER BY query_id, type
FORMAT JSONEachRow""",
    )
    expected = set(query_ids)
    observed: set[str] = set()
    samples: list[dict[str, Any]] = []
    exceptions = 0
    for line in raw.splitlines():
        try:
            row = json.loads(line)
            query_id = row["query_id"]
            row_type = row["type"]
            if query_id not in expected or query_id in observed:
                raise ValueError("query identity differs")
            observed.add(query_id)
            if row_type != "QueryFinish":
                exceptions += 1
                continue
            sample = {
                "query_duration_ms": float(row["query_duration_ms"]),
                "read_rows": int(row["read_rows"]),
                "read_bytes": int(row["read_bytes"]),
                "result_rows": int(row["result_rows"]),
                "memory_usage_bytes": int(row["memory_usage_bytes"]),
            }
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise SuiteFailure("publication capture query-log row is malformed") from error
        if (
            not math.isfinite(sample["query_duration_ms"])
            or sample["query_duration_ms"] < 0
            or any(sample[name] < 0 for name in sample if name != "query_duration_ms")
        ):
            raise SuiteFailure("publication capture query-log row is invalid")
        samples.append(sample)
    if observed != expected or exceptions or len(samples) != len(query_ids):
        raise SuiteFailure(
            "publication capture query-log coverage differs: "
            f"expected={len(query_ids)} observed={len(observed)} exceptions={exceptions}"
        )
    return samples


def _publication_capture_query_log_summary(
    samples: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    durations = [float(sample["query_duration_ms"]) for sample in samples]
    latency = _latency_summary(durations)
    return {
        "sample_count": len(samples),
        "duration_p50_ms": latency["p50_ms"],
        "duration_p95_ms": latency["p95_ms"],
        "duration_max_ms": latency["max_ms"],
        "read_rows_total": sum(int(sample["read_rows"]) for sample in samples),
        "read_bytes_total": sum(int(sample["read_bytes"]) for sample in samples),
        "result_rows_total": sum(int(sample["result_rows"]) for sample in samples),
        "max_memory_bytes": max(int(sample["memory_usage_bytes"]) for sample in samples),
        "raw_samples": [dict(sample) for sample in samples],
    }


def _publication_append_warmup_timeout(timeout_s: float) -> float:
    return max(PUBLICATION_APPEND_WARMUP_MIN_TIMEOUT_S, timeout_s)


def _validate_publication_capture_storage(
    control_tables: Mapping[str, Mapping[str, int]], *, logical_head_count: int
) -> None:
    head_storage = control_tables.get("published_source_generations")
    physical_rows = None if head_storage is None else head_storage.get("rows")
    # published_source_generations is a ReplacingMergeTree that deliberately
    # retains publication history and may temporarily retain response-loss
    # retries.  Physical rows therefore bound logical heads from below; they
    # are not expected to equal the current logical head count.
    if (
        isinstance(physical_rows, bool)
        or not isinstance(physical_rows, int)
        or physical_rows < logical_head_count
    ):
        raise SuiteFailure(
            "publication capture storage rows are fewer than logical head count: "
            f"physical={physical_rows!r} logical={logical_head_count}"
        )


def _measure_publication_capture_point(
    url: str,
    snapshot: Mapping[str, int],
    *,
    query_prefix: str,
    database: str = "moraine",
) -> dict[str, Any]:
    target_count = snapshot.get("head_count")
    publication_revision = snapshot.get("max_publication_revision")
    if (
        isinstance(target_count, bool)
        or not isinstance(target_count, int)
        or target_count < 1
        or isinstance(publication_revision, bool)
        or not isinstance(publication_revision, int)
        or publication_revision < 1
        or _logical_publication_head_snapshot(url, database) != dict(snapshot)
    ):
        raise SuiteFailure("publication capture point identity is invalid")
    capture_sql = _publication_capture_sql(database)
    for iteration in range(PUBLICATION_CAPTURE_WARMUP_REPETITIONS):
        raw = _clickhouse_query(
            url,
            capture_sql,
            query_id=f"{query_prefix}-{target_count}-warm-{iteration}",
        )
        _validate_publication_capture_result(
            raw,
            publication_revision=publication_revision,
        )
    client_latencies: list[float] = []
    query_ids: list[str] = []
    for iteration in range(PUBLICATION_CAPTURE_REPETITIONS):
        query_id = f"{query_prefix}-{target_count}-measured-{iteration}"
        started_ns = time.perf_counter_ns()
        raw = _clickhouse_query(url, capture_sql, query_id=query_id)
        completed_ns = time.perf_counter_ns()
        _validate_publication_capture_result(
            raw,
            publication_revision=publication_revision,
        )
        query_ids.append(query_id)
        client_latencies.append((completed_ns - started_ns) / 1_000_000.0)
    query_log_samples = _publication_capture_query_log_samples(url, query_ids)
    control_tables = _publication_control_resources(url, database)
    _validate_publication_capture_storage(
        control_tables, logical_head_count=target_count
    )
    return {
        **snapshot,
        "client_latency": _latency_summary(client_latencies),
        "query_log": _publication_capture_query_log_summary(query_log_samples),
        "control_tables": control_tables,
    }


def _publication_capture_scaling_artifact(
    points: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if [point.get("head_count") for point in points] != list(
        PUBLICATION_CAPTURE_HEAD_COUNTS
    ):
        raise SuiteFailure("publication capture scaling points differ")
    return {
        "publication_mode": "local",
        "capture_query": "raw_history_max_publication_revision",
        "head_counts": list(PUBLICATION_CAPTURE_HEAD_COUNTS),
        "warmup_repetitions": PUBLICATION_CAPTURE_WARMUP_REPETITIONS,
        "measured_repetitions": PUBLICATION_CAPTURE_REPETITIONS,
        "points": [dict(point) for point in points],
    }


def _append_probe_latency(samples: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    latencies: list[float] = []
    for sample in samples:
        durable = sample.get("publication_durable_ms")
        visible = sample.get("first_valid_ms")
        if (
            sample.get("valid") is not True
            or isinstance(durable, bool)
            or not isinstance(durable, (int, float))
            or isinstance(visible, bool)
            or not isinstance(visible, (int, float))
            or not math.isfinite(float(durable))
            or not math.isfinite(float(visible))
            or float(visible) < float(durable)
        ):
            raise SuiteFailure("source-publication append sample is invalid or censored")
        latencies.append(float(visible) - float(durable))
    return {
        "sample_count": len(latencies),
        "fsync_to_live_p50_ms": _nearest_rank(latencies, 50.0),
        "fsync_to_live_p95_ms": _nearest_rank(latencies, 95.0),
        "fsync_to_live_max_ms": max(latencies),
        "raw_fsync_to_live_ms": latencies,
    }


def _image_recipe_sha256(repo: Path) -> str:
    inputs: dict[str, str] = {}
    for relative in IMAGE_RECIPE_PATHS:
        path = repo / relative
        if not path.is_file():
            raise SuiteFailure(f"image recipe input is missing: {relative}")
        inputs[relative] = sha256_bytes(path.read_bytes())
    return sha256_json(inputs)


def _schedule_hashes(recipe: Mapping[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    templates = recipe["schedule_templates"]
    for scenario, splits in recipe["split_matrix"].items():
        template_name = "etd" if scenario in {"etd_idle", "etd_loaded"} else scenario
        for split in splits:
            result[f"{scenario}:{split}"] = sha256_json(
                {
                    "fixture_sha256": recipe["fixture_sha256"],
                    "scenario": scenario,
                    "split": split,
                    "template": templates[template_name],
                }
            )
    return result


def _physical_reset_sha256(reset_id: str) -> str:
    return "sha256:" + hashlib.sha256(reset_id.encode("utf-8")).hexdigest()


@dataclass
class EvidenceCollector:
    resources: list[dict[str, Any]] = field(default_factory=list)
    binary_hashes: dict[str, tuple[str, bool]] = field(default_factory=dict)
    cache_generations: list[str] = field(default_factory=list)
    physical_resets: dict[str, str] = field(default_factory=dict)

    def record_binary(self, name: str, digest: str, *, verified: bool = True) -> None:
        previous = self.binary_hashes.setdefault(name, (digest, verified))
        if previous != (digest, verified):
            raise SuiteFailure(f"running binary evidence changed within scenario: {name}")

    def record_reset(self, role: str, reset_id: str) -> None:
        digest = _physical_reset_sha256(reset_id)
        if digest in self.physical_resets.values():
            raise SuiteFailure("physical reset was reused within a scenario")
        self.physical_resets[role] = digest

    def resource_artifact(self, *, authoritative: bool) -> dict[str, Any]:
        if not self.resources:
            if authoritative:
                raise SuiteFailure("authoritative scenario has no cgroup evidence")
            return non_authoritative_resource_evidence()
        constants = (
            "cgroup_version",
            "cgroup_driver",
            "controllers_enabled_proven",
            "effective_limits_proven",
            "host_headroom_proven",
            "cpuset_cpus_effective",
            "cpu_max_quota_us",
            "cpu_max_period_us",
            "memory_max_bytes",
            "swap_max_bytes",
            "server_descendants_proven",
            "loadgen_excluded_proven",
        )
        for name in constants:
            values = {json.dumps(item[name], sort_keys=True) for item in self.resources}
            if len(values) != 1:
                raise SuiteFailure(f"resource evidence changed across physical resets: {name}")
        first = self.resources[0]
        aggregate = {name: first[name] for name in constants}
        aggregate.update(
            {
                "authoritative": all(item["authoritative"] for item in self.resources),
                "cgroup_identity_sha256": sha256_json([item["cgroup_identity_sha256"] for item in self.resources]),
                "role_membership_sha256": sha256_json([item["role_membership_sha256"] for item in self.resources]),
                "cpu_usage_usec_delta": sum(item["cpu_usage_usec_delta"] for item in self.resources),
                "cpu_nr_throttled_delta": sum(item["cpu_nr_throttled_delta"] for item in self.resources),
                "throttled_usec_delta": sum(item["throttled_usec_delta"] for item in self.resources),
                "memory_current_bytes": max(item["memory_current_bytes"] for item in self.resources),
                "memory_peak_bytes": max(item["memory_peak_bytes"] for item in self.resources),
                "memory_event_high_delta": sum(item["memory_event_high_delta"] for item in self.resources),
                "memory_event_max_delta": sum(item["memory_event_max_delta"] for item in self.resources),
                "swap_current_bytes": max(item["swap_current_bytes"] for item in self.resources),
                "oom_kill_delta": sum(item["oom_kill_delta"] for item in self.resources),
            }
        )
        if authoritative and not aggregate["authoritative"]:
            raise SuiteFailure("authoritative scenario contains non-authoritative resource evidence")
        return aggregate

    def binary_artifact(self, build: Mapping[str, Any]) -> dict[str, Any]:
        if not self.binary_hashes:
            raise SuiteFailure("scenario has no verified running binary evidence")
        expected = {item["role"]: item["sha256"] for item in build["binaries"]}
        running = []
        for role, (observed, verified) in sorted(self.binary_hashes.items()):
            if expected.get(role) != observed:
                raise SuiteFailure(f"running binary differs from immutable build: {role}")
            running.append(
                {
                    "role": role,
                    "sha256": expected[role],
                    "proc_exe_sha256": observed,
                    "verified": verified,
                }
            )
        return {
            "build_identity_sha256": build["identity_sha256"],
            "image_digest": build["image_digest"],
            "running_binaries": running,
        }

    def cache_artifact(self, recipe: Mapping[str, Any], scenario: str, split: str) -> dict[str, Any]:
        if not self.cache_generations:
            raise SuiteFailure("scenario has no observed cache generation")
        return {
            "label": "fresh_moraine_existing_clickhouse",
            "moraine_process": "fresh",
            "mcp_result_cache": "fresh",
            "mcp_posting_cache": "fresh",
            "mcp_document_frequency_cache": "fresh",
            "mcp_document_cache": "fresh",
            "mcp_hydration_cache": "fresh",
            "target_query_prewarmed": False,
            "clickhouse_cache": "seed_warmed",
            "os_page_cache": "uncontrolled",
            "generation_sha256": sha256_json(sorted(self.cache_generations)),
            "fingerprint_sha256": sha256_json(
                {"fixture": recipe["fixture_sha256"], "scenario": scenario, "split": split}
            ),
        }


class ManagedSandbox:
    """Couple one sandbox with its owned aggregate cgroup and evidence."""

    def __init__(
        self,
        sandbox: Any,
        envelope: FixedEnvelope,
        build: BuildIdentity,
        collector: EvidenceCollector,
        *,
        reset_role: str,
        authoritative: bool,
    ) -> None:
        self._sandbox = sandbox
        self._envelope = envelope
        self._collector = collector
        self._closed = False
        self._captured: Optional[Any] = None
        self.sandbox_id = sandbox.sandbox_id
        self.project = sandbox.project
        self.monitor_port = sandbox.monitor_port
        self.clickhouse_port = sandbox.clickhouse_port
        self.config_dir = sandbox.config_dir
        self.build = sandbox.build
        self._collector.record_reset(reset_role, self.sandbox_id)
        self._build = build
        self._authoritative = authoritative
        self._refresh_server_processes()
        self._before = envelope.reset_measurement(
            self._server_pids,
            self._loadgen_pids,
        )
        central = sandbox.central_status()
        self._collector.cache_generations.append(str(central["cache_generation"]))

    @property
    def watched_source_dir(self) -> Path:
        return self._sandbox.watched_source_dir

    def __getattr__(self, name: str) -> Any:
        return getattr(self._sandbox, name)

    def spawn_stdio_route(self, **kwargs: Any) -> subprocess.Popen[bytes]:
        process = self._sandbox.spawn_stdio_route(**kwargs)
        deadline = __import__("time").monotonic() + 5.0
        while __import__("time").monotonic() < deadline:
            status = self._sandbox.central_status()
            routes = status.get("route_processes", [])
            if routes:
                expected = self.build.binary_sha256["moraine-mcp"]
                if self._authoritative:
                    evidence = verify_process_binary(
                        int(routes[-1]["pid"]), "moraine-mcp", expected
                    )
                    self._collector.record_binary(evidence.name, evidence.exe_sha256)
                return process
            __import__("time").sleep(0.01)
        process.kill()
        process.wait(timeout=2)
        raise SuiteFailure("MCP route process was not observable for binary verification")

    def _refresh_server_processes(self) -> None:
        try:
            central = self._sandbox.central_status()
        except Exception:
            process = self._sandbox.spawn_central()
            self._sandbox.wait_central_ready_without_search(process)
            central = self._sandbox.central_status()
        status = self._sandbox.status()
        self._server_pids = status.server_pids
        self._loadgen_pids = status.loadgen_pids
        if self._authoritative:
            verifier = self._envelope.verify_running_binaries(
                self._build.binary_sha256
            )
        else:
            verifier = self._sandbox.verify_running_binaries(
                self._build.binary_sha256,
                (
                    int(central["central"]["pid"]),
                    int(central["ingest"]["pid"]),
                    *(int(pid) for pid in central["server_children"]),
                ),
            )
        for item in verifier:
            self._collector.record_binary(item.name, item.exe_sha256)

    def _capture(self) -> Any:
        if self._captured is None:
            self._refresh_server_processes()
            evidence = self._envelope.inspect(
                self._server_pids,
                self._loadgen_pids,
            )
            evidence.assert_clean(self._before)
            self._captured = evidence
        return self._captured

    def trial_telemetry(self) -> dict[str, Any]:
        artifact = self._capture().artifact(self._before)
        return {
            name: artifact[name]
            for name in (
                "cpu_usage_usec_delta",
                "cpu_nr_throttled_delta",
                "throttled_usec_delta",
                "memory_current_bytes",
                "memory_peak_bytes",
                "memory_event_high_delta",
                "memory_event_max_delta",
                "swap_current_bytes",
                "oom_kill_delta",
            )
        }

    def down(self) -> None:
        if self._closed:
            return
        self._closed = True
        failure: Optional[BaseException] = None
        try:
            evidence = self._capture()
            self._collector.resources.append(evidence.artifact(self._before))
            self._sandbox.checkpoint("artifact-created")
        except BaseException as error:
            failure = error
        try:
            self._sandbox.down()
        except BaseException as error:
            failure = failure or error
        try:
            self._envelope.remove()
        except BaseException as error:
            failure = failure or error
        if failure is not None:
            raise SuiteFailure(f"owned resource cleanup or evidence capture failed: {failure}") from failure


def _start_measured_sandbox(
    _repo: Path,
    build: BuildIdentity,
    expected_image_digest: str,
    recipe: Mapping[str, Any],
    collector: EvidenceCollector,
    *,
    reset_role: str,
    authoritative: bool = True,
) -> ManagedSandbox:
    envelope = FixedEnvelope(run_id()) if authoritative else LocalEnvelope(run_id())
    sandbox = None
    try:
        parent = envelope.create()
        sandbox = start_owned_sandbox(
            SUITE_ROOT,
            cgroup_parent=parent,
            build=build,
            local=not authoritative,
        )
        observed_image_digest = sha256_json(sandbox.status().image_ids)
        if observed_image_digest != expected_image_digest:
            raise SuiteFailure("measured sandbox image identity differs from prepared build")
        _seed_owned_sandbox(sandbox, recipe)
        return ManagedSandbox(
            sandbox,
            envelope,
            build,
            collector,
            reset_role=reset_role,
            authoritative=authoritative,
        )
    except BaseException as setup_error:
        cleanup_errors: list[str] = []
        if sandbox is not None:
            try:
                sandbox.down()
            except BaseException as error:
                cleanup_errors.append(
                    f"sandbox {sandbox.sandbox_id} cleanup failed: {error}"
                )
        try:
            envelope.remove()
        except BaseException as error:
            cleanup_errors.append(
                f"cgroup {envelope.owned_id} cleanup failed: {error}"
            )
        if cleanup_errors:
            raise SuiteFailure(
                f"benchmark setup failed: {setup_error}; "
                f"owned cleanup incomplete: {'; '.join(cleanup_errors)}"
            ) from setup_error
        raise


def _discover_image_digest(
    repo: Path,
    build: BuildIdentity,
    *,
    prove_cpu: bool,
    authoritative: bool,
) -> str:
    envelope = FixedEnvelope(run_id()) if authoritative else LocalEnvelope(run_id())
    sandbox = None
    try:
        parent = envelope.create()
        if prove_cpu and authoritative:
            proof = run_busy_child_proof(envelope)
            if proof.usage_per_wall_cpu < 0.85 or proof.usage_per_wall_cpu > 1.15:
                raise SuiteFailure("aggregate busy-child CPU proof fell outside the declared tolerance")
        sandbox = start_owned_sandbox(
            SUITE_ROOT,
            cgroup_parent=parent,
            build=build,
            local=not authoritative,
        )
        status = sandbox.status()
        return sha256_json(status.image_ids)
    finally:
        failure: Optional[BaseException] = None
        if sandbox is not None:
            try:
                sandbox.down()
            except BaseException as error:
                failure = error
        try:
            envelope.remove()
        except BaseException as error:
            failure = failure or error
        if failure is not None:
            raise SuiteFailure(f"image-identity sandbox cleanup failed: {failure}") from failure


@dataclass(frozen=True)
class PreparedBuild:
    runtime: BuildIdentity
    protocol: Mapping[str, Any]


def _prepare_builds(
    repositories: Mapping[str, Path],
    output: Path,
    *,
    authoritative: bool,
) -> tuple[dict[str, PreparedBuild], Mapping[str, Any]]:
    prepared: dict[str, PreparedBuild] = {}
    common_recipe: Optional[Mapping[str, Any]] = None
    # The sandbox mounts this directory into a container and deliberately
    # rejects relative/symlinked paths. CLI examples use repository-relative
    # output directories, so canonicalize the build subtree before exporting
    # any binary identity from it.
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    ensure_runtime_build_image(SUITE_ROOT)
    for index, (arm, repo) in enumerate(repositories.items()):
        _require_clean(repo)
        runtime_build = build_release_binaries_in_docker(
            repo,
            output / arm,
            toolchain_file=SUITE_ROOT / "rust-toolchain.toml",
        )
        build_environment_sha256 = runtime_build.artifact()["recipe"]["build_environment_sha256"]
        recipe = create_build_recipe(
            toolchain_sha256=runtime_build.toolchain_sha256,
            target=runtime_build.target,
            linker_sha256=runtime_build.artifact()["recipe"]["linker_sha256"],
            environment_allowlist=sorted(runtime_build.build_environment),
            build_environment_sha256=build_environment_sha256,
            image_recipe_sha256=_image_recipe_sha256(SUITE_ROOT),
        )
        if common_recipe is None:
            common_recipe = recipe
        elif recipe != common_recipe:
            raise SuiteFailure("baseline and candidate build recipes differ")
        image_digest = _discover_image_digest(
            SUITE_ROOT,
            runtime_build,
            prove_cpu=index == 0,
            authoritative=authoritative,
        )
        protocol_build = create_build_identity(
            arm=arm,
            git_commit=_git_commit(repo),
            image_digest=image_digest,
            build_environment_sha256=build_environment_sha256,
            binaries=[
                {"role": name, "sha256": digest}
                for name, digest in sorted(runtime_build.binary_sha256.items())
            ],
        )
        prepared[arm] = PreparedBuild(runtime_build, protocol_build)
    if common_recipe is None:
        raise SuiteFailure("no build repositories were supplied")
    return prepared, common_recipe


def _control_resource_delta(
    before: Mapping[str, Mapping[str, int]],
    after: Mapping[str, Mapping[str, int]],
) -> dict[str, dict[str, int]]:
    fields = ("rows", "active_parts", "compressed_bytes")
    return {
        table: {
            field: int(after.get(table, {}).get(field, 0))
            - int(before.get(table, {}).get(field, 0))
            for field in fields
        }
        for table in sorted(set(before) | set(after))
    }


def _source_host_column_resource_delta(
    before: Mapping[str, Mapping[str, int]],
    after: Mapping[str, Mapping[str, int]],
) -> dict[str, dict[str, int]]:
    return {
        table: {
            field: int(after.get(table, {}).get(field, 0))
            - int(before.get(table, {}).get(field, 0))
            for field in SOURCE_HOST_COLUMN_RESOURCE_FIELDS
        }
        for table in SOURCE_HOST_PHYSICAL_TABLES
    }


def _validate_control_table_resources(
    value: Any, *, allow_negative: bool = False
) -> None:
    if not isinstance(value, dict) or not value:
        raise SuiteFailure("source-publication control resource map is invalid")
    for table, counters in value.items():
        if (
            not isinstance(table, str)
            or not table
            or not isinstance(counters, dict)
            or set(counters) != {"rows", "active_parts", "compressed_bytes"}
        ):
            raise SuiteFailure("source-publication control resource fields differ")
        for counter in counters.values():
            if (
                isinstance(counter, bool)
                or not isinstance(counter, int)
                or (not allow_negative and counter < 0)
            ):
                raise SuiteFailure("source-publication control resource counter is invalid")


def _validate_source_host_column_resources(
    value: Any, *, allow_negative: bool = False
) -> None:
    if not isinstance(value, dict) or set(value) != set(SOURCE_HOST_PHYSICAL_TABLES):
        raise SuiteFailure("source_host column resource tables differ")
    for counters in value.values():
        if not isinstance(counters, dict) or set(counters) != set(
            SOURCE_HOST_COLUMN_RESOURCE_FIELDS
        ):
            raise SuiteFailure("source_host column resource fields differ")
        if any(
            isinstance(counter, bool)
            or not isinstance(counter, int)
            or (not allow_negative and counter < 0)
            for counter in counters.values()
        ):
            raise SuiteFailure("source_host column resource counter is invalid")


def _validate_latency_summary(
    value: Any, *, sample_count: int, context: str
) -> None:
    if not isinstance(value, dict) or set(value) != {
        "sample_count",
        "p50_ms",
        "p95_ms",
        "max_ms",
        "raw_ms",
    }:
        raise SuiteFailure(f"{context} latency fields differ")
    samples = value["raw_ms"]
    if (
        value["sample_count"] != sample_count
        or not isinstance(samples, list)
        or len(samples) != sample_count
        or any(
            isinstance(sample, bool)
            or not isinstance(sample, (int, float))
            or not math.isfinite(float(sample))
            or float(sample) < 0
            for sample in samples
        )
    ):
        raise SuiteFailure(f"{context} latency samples are invalid")
    expected = _latency_summary(samples)
    if any(
        isinstance(value[name], bool)
        or not isinstance(value[name], (int, float))
        or not math.isfinite(float(value[name]))
        or float(value[name]) < 0
        for name in ("p50_ms", "p95_ms", "max_ms")
    ):
        raise SuiteFailure(f"{context} latency summary is invalid")
    if any(
        not math.isclose(
            float(value[name]), float(expected[name]), rel_tol=0.0, abs_tol=1e-9
        )
        for name in ("p50_ms", "p95_ms", "max_ms")
    ):
        raise SuiteFailure(f"{context} latency summary differs")


def _validate_publication_capture_scaling(value: Any) -> None:
    if not isinstance(value, dict) or set(value) != {
        "publication_mode",
        "capture_query",
        "head_counts",
        "warmup_repetitions",
        "measured_repetitions",
        "points",
    }:
        raise SuiteFailure("publication capture scaling fields differ")
    if (
        value["publication_mode"] != "local"
        or value["capture_query"] != "raw_history_max_publication_revision"
        or value["head_counts"] != list(PUBLICATION_CAPTURE_HEAD_COUNTS)
        or value["warmup_repetitions"]
        != PUBLICATION_CAPTURE_WARMUP_REPETITIONS
        or value["measured_repetitions"] != PUBLICATION_CAPTURE_REPETITIONS
        or not isinstance(value["points"], list)
        or len(value["points"]) != len(PUBLICATION_CAPTURE_HEAD_COUNTS)
    ):
        raise SuiteFailure("publication capture scaling policy differs")
    previous_revision = 0
    for expected_count, point in zip(
        PUBLICATION_CAPTURE_HEAD_COUNTS, value["points"]
    ):
        if not isinstance(point, dict) or set(point) != {
            "head_count",
            "max_publication_revision",
            "client_latency",
            "query_log",
            "control_tables",
        }:
            raise SuiteFailure("publication capture scale-point fields differ")
        if (
            isinstance(point["head_count"], bool)
            or not isinstance(point["head_count"], int)
            or isinstance(point["max_publication_revision"], bool)
            or not isinstance(point["max_publication_revision"], int)
            or point["head_count"] != expected_count
            or point["max_publication_revision"] <= previous_revision
        ):
            raise SuiteFailure("publication capture scale-point identity differs")
        previous_revision = point["max_publication_revision"]
        _validate_latency_summary(
            point["client_latency"],
            sample_count=PUBLICATION_CAPTURE_REPETITIONS,
            context="publication capture client",
        )
        query_log = point["query_log"]
        if not isinstance(query_log, dict) or set(query_log) != {
            "sample_count",
            "duration_p50_ms",
            "duration_p95_ms",
            "duration_max_ms",
            "read_rows_total",
            "read_bytes_total",
            "result_rows_total",
            "max_memory_bytes",
            "raw_samples",
        }:
            raise SuiteFailure("publication capture query-log fields differ")
        raw_samples = query_log["raw_samples"]
        if (
            query_log["sample_count"] != PUBLICATION_CAPTURE_REPETITIONS
            or not isinstance(raw_samples, list)
            or len(raw_samples) != PUBLICATION_CAPTURE_REPETITIONS
        ):
            raise SuiteFailure("publication capture query-log coverage differs")
        for sample in raw_samples:
            if not isinstance(sample, dict) or set(sample) != {
                "query_duration_ms",
                "read_rows",
                "read_bytes",
                "result_rows",
                "memory_usage_bytes",
            }:
                raise SuiteFailure("publication capture query-log sample fields differ")
            if (
                isinstance(sample["query_duration_ms"], bool)
                or not isinstance(sample["query_duration_ms"], (int, float))
                or not math.isfinite(float(sample["query_duration_ms"]))
                or float(sample["query_duration_ms"]) < 0
                or any(
                    isinstance(sample[name], bool)
                    or not isinstance(sample[name], int)
                    or sample[name] < 0
                    for name in (
                        "read_rows",
                        "read_bytes",
                        "result_rows",
                        "memory_usage_bytes",
                    )
                )
            ):
                raise SuiteFailure("publication capture query-log sample is invalid")
        expected_log = _publication_capture_query_log_summary(raw_samples)
        for name in (
            "read_rows_total",
            "read_bytes_total",
            "result_rows_total",
            "max_memory_bytes",
        ):
            if (
                isinstance(query_log[name], bool)
                or not isinstance(query_log[name], int)
                or query_log[name] < 0
                or query_log[name] != expected_log[name]
            ):
                raise SuiteFailure("publication capture query-log totals differ")
        for name in (
            "duration_p50_ms",
            "duration_p95_ms",
            "duration_max_ms",
        ):
            if (
                isinstance(query_log[name], bool)
                or not isinstance(query_log[name], (int, float))
                or not math.isfinite(float(query_log[name]))
                or float(query_log[name]) < 0
                or not math.isclose(
                    float(query_log[name]),
                    float(expected_log[name]),
                    rel_tol=0.0,
                    abs_tol=1e-9,
                )
            ):
                raise SuiteFailure("publication capture query-log summary differs")
        _validate_control_table_resources(point["control_tables"])
        _validate_publication_capture_storage(
            point["control_tables"], logical_head_count=expected_count
        )


def validate_source_publication_probe(document: Any) -> None:
    required = {
        "document_type",
        "schema_version",
        "status",
        "git_commit",
        "run",
        "append",
        "publication_head",
        "control_tables",
        "control_capture_scaling",
        "source_host_columns",
        "resources",
        "artifact_sha256",
    }
    if not isinstance(document, dict) or set(document) != required:
        raise SuiteFailure("source-publication probe document fields differ")
    if (
        document.get("document_type") != "source_publication_append_probe"
        or document.get("schema_version") != "moraine.source-publication-probe.v1"
        or document.get("status") not in {"pass", "fail"}
        or not isinstance(document.get("git_commit"), str)
        or len(document["git_commit"]) != 40
        or any(character not in "0123456789abcdef" for character in document["git_commit"])
    ):
        raise SuiteFailure("source-publication probe identity is invalid")
    run = document.get("run")
    append = document.get("append")
    head = document.get("publication_head")
    controls = document.get("control_tables")
    scaling = document.get("control_capture_scaling")
    source_host_columns = document.get("source_host_columns")
    if (
        not isinstance(run, dict)
        or set(run)
        != {"authoritative", "minimum_samples", "p95_limit_ms", "timeout_seconds"}
        or not isinstance(append, dict)
        or set(append)
        != {
            "sample_count",
            "fsync_to_live_p50_ms",
            "fsync_to_live_p95_ms",
            "fsync_to_live_max_ms",
            "raw_fsync_to_live_ms",
        }
        or not isinstance(head, dict)
        or set(head)
        != {
            "before",
            "after",
            "head_write_count",
            "publication_revision_unchanged",
        }
        or not isinstance(controls, dict)
        or set(controls) != {"before", "after", "delta"}
        or not isinstance(source_host_columns, dict)
        or set(source_host_columns) != {"before", "after", "delta"}
    ):
        raise SuiteFailure("source-publication probe evidence shape differs")
    if (
        not isinstance(run["authoritative"], bool)
        or isinstance(run["minimum_samples"], bool)
        or not isinstance(run["minimum_samples"], int)
        or run["minimum_samples"] < 100
        or any(
            isinstance(run[name], bool)
            or not isinstance(run[name], (int, float))
            or not math.isfinite(float(run[name]))
            or float(run[name]) <= 0
            for name in ("p95_limit_ms", "timeout_seconds")
        )
    ):
        raise SuiteFailure("source-publication probe run policy is invalid")
    try:
        resources_pass = _validate_resources(document["resources"])
    except ProtocolError as error:
        raise SuiteFailure(f"source-publication probe resources are invalid: {error}") from error
    if (
        document["resources"]["authoritative"] is not run["authoritative"]
        or (run["authoritative"] and not resources_pass)
    ):
        raise SuiteFailure("source-publication probe resource authority differs")
    _validate_publication_capture_scaling(scaling)
    _validate_source_host_column_resources(source_host_columns["before"])
    _validate_source_host_column_resources(source_host_columns["after"])
    _validate_source_host_column_resources(
        source_host_columns["delta"], allow_negative=True
    )
    if source_host_columns["delta"] != _source_host_column_resource_delta(
        source_host_columns["before"], source_host_columns["after"]
    ):
        raise SuiteFailure("source_host column resource delta differs")
    samples = append["raw_fsync_to_live_ms"]
    minimum = run["minimum_samples"]
    if (
        not isinstance(samples, list)
        or isinstance(append["sample_count"], bool)
        or not isinstance(append["sample_count"], int)
        or append["sample_count"] != len(samples)
        or len(samples) < minimum
        or any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or float(value) < 0
            for value in samples
        )
    ):
        raise SuiteFailure("source-publication probe samples are invalid")
    expected_latency = {
        "fsync_to_live_p50_ms": _nearest_rank(samples, 50.0),
        "fsync_to_live_p95_ms": _nearest_rank(samples, 95.0),
        "fsync_to_live_max_ms": max(samples),
    }
    if any(
        isinstance(append[name], bool)
        or not isinstance(append[name], (int, float))
        or not math.isfinite(float(append[name]))
        or float(append[name]) < 0
        for name in expected_latency
    ):
        raise SuiteFailure("source-publication probe latency summary is invalid")
    if any(
        not math.isclose(
            float(append[name]), value, rel_tol=0.0, abs_tol=1e-9
        )
        for name, value in expected_latency.items()
    ):
        raise SuiteFailure("source-publication probe latency summary differs")
    before = head["before"]
    after = head["after"]
    _validate_control_table_resources(controls["before"])
    _validate_control_table_resources(controls["after"])
    _validate_control_table_resources(controls["delta"], allow_negative=True)
    if (
        not isinstance(before, dict)
        or not isinstance(after, dict)
        or set(before) != {"row_count", "max_publication_revision"}
        or set(after) != set(before)
        or any(
            isinstance(snapshot[name], bool)
            or not isinstance(snapshot[name], int)
            or snapshot[name] < 0
            for snapshot in (before, after)
            for name in ("row_count", "max_publication_revision")
        )
        or isinstance(head["head_write_count"], bool)
        or not isinstance(head["head_write_count"], int)
        or head["head_write_count"] < 0
        or not isinstance(head["publication_revision_unchanged"], bool)
        or head["head_write_count"] != after["row_count"] - before["row_count"]
        or head["publication_revision_unchanged"]
        is not (
            before["max_publication_revision"]
            == after["max_publication_revision"]
        )
        or controls["delta"]
        != _control_resource_delta(controls["before"], controls["after"])
    ):
        raise SuiteFailure("source-publication probe control evidence differs")
    passed = bool(
        head["head_write_count"] == 0
        and head["publication_revision_unchanged"] is True
        and float(append["fsync_to_live_p95_ms"]) <= float(run["p95_limit_ms"])
    )
    if document["status"] != ("pass" if passed else "fail"):
        raise SuiteFailure("source-publication probe status contradicts gates")
    expected_hash = sha256_json(
        {key: value for key, value in document.items() if key != "artifact_sha256"}
    )
    if document["artifact_sha256"] != expected_hash:
        raise SuiteFailure("source-publication probe artifact hash differs")


def run_source_publication_append_probe(
    repo: Path,
    *,
    output: Path,
    samples: int = 100,
    timeout_s: float = 5.0,
    p95_limit_ms: float = 2_000.0,
    authoritative: bool = False,
) -> Path:
    """Run a real same-file append probe and retain PR-ready resource evidence."""

    if (
        isinstance(samples, bool)
        or samples < 100
        or not math.isfinite(timeout_s)
        or timeout_s <= 0
        or not math.isfinite(p95_limit_ms)
        or p95_limit_ms <= 0
    ):
        raise SuiteFailure("source-publication probe requires >=100 samples and positive limits")
    output.mkdir(parents=True, exist_ok=False)
    recipe = build_recipe("smoke")
    prepared_builds, _build_recipe = _prepare_builds(
        {"candidate": repo}, output / "builds", authoritative=authoritative
    )
    prepared = prepared_builds["candidate"]
    collector = EvidenceCollector()
    sandbox = _start_measured_sandbox(
        repo,
        prepared.runtime,
        str(prepared.protocol["image_digest"]),
        recipe,
        collector,
        reset_role="source_publication_append_probe",
        authoritative=authoritative,
    )
    try:
        url = f"http://127.0.0.1:{sandbox.clickhouse_port}"
        scale_query_prefix = f"moraine-publication-capture-{run_id()}"
        initial_scale_snapshot = _logical_publication_head_snapshot(url)
        if initial_scale_snapshot != {
            "head_count": 1,
            "max_publication_revision": 1,
        }:
            raise SuiteFailure(
                "publication capture scaling requires exactly one seeded head"
            )
        scale_points = [
            _measure_publication_capture_point(
                url,
                initial_scale_snapshot,
                query_prefix=scale_query_prefix,
            )
        ]
        source_host_before = _source_host_column_resources(url)
        term_count = max(
            64,
            math.ceil(timeout_s / PUBLICATION_APPEND_POLL_INTERVAL_S) + 2,
        )
        events = build_append_probe_events(samples + 1, term_count=term_count)
        warmup = run_owned_sandbox_append_probe(
            sandbox,
            events[:1],
            timeout_s=_publication_append_warmup_timeout(timeout_s),
            poll_interval_s=PUBLICATION_APPEND_POLL_INTERVAL_S,
        )
        if warmup.status != "pass":
            raise SuiteFailure(
                "source-publication append warmup did not become live: "
                f"diagnostics={list(warmup.diagnostics)!r}"
            )
        head_before = _publication_head_snapshot(url)
        controls_before = _publication_control_resources(url)
        measured = run_owned_sandbox_append_probe(
            sandbox,
            events[1:],
            timeout_s=timeout_s,
            poll_interval_s=PUBLICATION_APPEND_POLL_INTERVAL_S,
        )
        sandbox.wait_ingest_drained(timeout_s=max(30.0, timeout_s * 2))
        head_after = _publication_head_snapshot(url)
        controls_after = _publication_control_resources(url)
        source_host_after = _source_host_column_resources(url)
        current_scale_snapshot = _logical_publication_head_snapshot(url)
        if current_scale_snapshot["head_count"] != 2:
            raise SuiteFailure(
                "ordinary append warmup did not add exactly one logical source head"
            )
        current_count = current_scale_snapshot["head_count"]
        for target_count in PUBLICATION_CAPTURE_HEAD_COUNTS[1:]:
            current_scale_snapshot = _seed_publication_heads_to(
                url,
                current_count=current_count,
                target_count=target_count,
            )
            scale_points.append(
                _measure_publication_capture_point(
                    url,
                    current_scale_snapshot,
                    query_prefix=scale_query_prefix,
                )
            )
            current_count = target_count
        control_capture_scaling = _publication_capture_scaling_artifact(
            scale_points
        )
    finally:
        sandbox.down()
    if measured.status != "pass":
        raise SuiteFailure("source-publication append probe contained invalid samples")
    append = _append_probe_latency(list(measured.samples))
    head = {
        "before": head_before,
        "after": head_after,
        "head_write_count": head_after["row_count"] - head_before["row_count"],
        "publication_revision_unchanged": (
            head_after["max_publication_revision"]
            == head_before["max_publication_revision"]
        ),
    }
    controls = {
        "before": controls_before,
        "after": controls_after,
        "delta": _control_resource_delta(controls_before, controls_after),
    }
    source_host_columns = {
        "before": source_host_before,
        "after": source_host_after,
        "delta": _source_host_column_resource_delta(
            source_host_before, source_host_after
        ),
    }
    document: dict[str, Any] = {
        "document_type": "source_publication_append_probe",
        "schema_version": "moraine.source-publication-probe.v1",
        "status": "pass"
        if (
            head["head_write_count"] == 0
            and head["publication_revision_unchanged"]
            and append["fsync_to_live_p95_ms"] <= p95_limit_ms
        )
        else "fail",
        "git_commit": str(prepared.protocol["git_commit"]),
        "run": {
            "authoritative": authoritative,
            "minimum_samples": samples,
            "p95_limit_ms": p95_limit_ms,
            "timeout_seconds": timeout_s,
        },
        "append": append,
        "publication_head": head,
        "control_tables": controls,
        "control_capture_scaling": control_capture_scaling,
        "source_host_columns": source_host_columns,
        "resources": collector.resource_artifact(authoritative=authoritative),
        "artifact_sha256": "",
    }
    document["artifact_sha256"] = sha256_json(
        {key: value for key, value in document.items() if key != "artifact_sha256"}
    )
    path = output / "source-publication-append-probe.json"
    write_json_atomic(path, document, validator=validate_source_publication_probe)
    return path


def _semantic_evidence(
    result: ScenarioResult,
    scenario: str,
    split: str,
    recipe: Mapping[str, Any],
) -> dict[str, Any]:
    malformed = 0
    missing = 0
    duplicates = 0
    stale = 0
    other = result.semantic_failures
    if scenario == "qps":
        outcomes = [sample["outcomes"] for sample in result.samples]  # type: ignore[index]
        observed = sum(item["correct"] for item in outcomes)
        malformed = sum(item["malformed"] for item in outcomes)
        other = sum(item[name] for item in outcomes for name in ("semantic_error", "protocol_error", "other_error"))
        expected = observed + malformed + other
    elif scenario == "ttr":
        samples = list(result.samples)  # type: ignore[arg-type]
        observed = sum(bool(item["valid"]) for item in samples)
        expected = len(samples)
        other = expected - observed
    elif scenario in {"etd_idle", "etd_loaded"}:
        samples = list(result.samples)  # type: ignore[arg-type]
        observed = sum(bool(item["valid"]) for item in samples)
        expected = len(samples)
        other = expected - observed
    else:
        metrics = result.metrics
        expected = len(result.samples["query_records"]) + len(result.samples["ingest"])  # type: ignore[index]
        missing = int(metrics["lost_events"])
        duplicates = int(metrics["duplicate_events"])
        observed = max(0, expected - missing) + duplicates
        other = result.semantic_failures
    passed = expected == observed and not any((missing, duplicates, stale, malformed, other))
    oracle_sha256 = semantic_oracle_sha256(
        recipe["fingerprints"],
        scenario,
        split,
    )
    return {
        "passed": passed,
        "oracle_sha256": oracle_sha256,
        "expected_count": expected,
        "observed_count": observed,
        "missing_count": missing,
        "duplicate_count": duplicates,
        "stale_count": stale,
        "malformed_count": malformed,
        "other_error_count": other,
    }


def _scenario_pass(result: ScenarioResult, scenario: str) -> bool:
    if scenario == "qps":
        return True
    if scenario == "ttr":
        return all(bool(sample["valid"]) for sample in result.samples)  # type: ignore[arg-type]
    if scenario in {"etd_idle", "etd_loaded"}:
        return all(bool(sample["valid"]) for sample in result.samples)  # type: ignore[arg-type]
    gates = result.metrics["mixed_gates"]
    return all(bool(value) for value in gates.values()) and result.metrics["lost_events"] == 0 and result.metrics["duplicate_events"] == 0


def _schedule_evidence(
    result: ScenarioResult,
    scenario: str,
    split: str,
    definition: Mapping[str, Any],
    recipe: Mapping[str, Any],
    collector: EvidenceCollector,
    *,
    expanded_schedule: Mapping[str, Any],
    query_count: int = 0,
    event_count: int = 0,
) -> dict[str, Any]:
    if scenario == "qps":
        samples = list(result.samples)  # type: ignore[arg-type]
        planned = sum(item["outcomes"]["planned"] for item in samples)
        started = sum(item["outcomes"]["started"] for item in samples)
        completed = started
        dropped = sum(item["outcomes"]["dropped"] for item in samples)
        slip = max(item["scheduler_p99_start_slip_ms"] for item in samples)
        drained = all(item["drained"] for item in samples)
        drain_ms = max(item["drain_ms"] for item in samples)
        physical = [{"role": "trial", "reset_sha256": item["reset_sha256"]} for item in samples]
    elif scenario == "mixed":
        planned = 2 * (query_count + event_count)
        started = completed = planned
        dropped = 0
        slip = 0.0
        drained = bool(result.metrics["mixed_gates"]["drained"])
        drain_ms = 0.0
        physical = [
            {"role": role, "reset_sha256": collector.physical_resets[role]}
            for role in ("query_control", "ingest_control", "combined")
        ]
    elif scenario in {"etd_idle", "etd_loaded"}:
        samples = list(result.samples)  # type: ignore[arg-type]
        operational = result.metrics["operational"]
        planned = int(operational["planned"])
        started = int(operational["started"])
        completed = int(operational["completed"])
        dropped = planned - started
        slip = float(operational["scheduler_p99_slip_ms"])
        drained = completed == planned and all(bool(sample["valid"]) for sample in samples)
        if scenario == "etd_loaded":
            loaded = result.metrics["loaded_query"]
            drained = drained and loaded is not None and loaded["drained"] is True
        drain_ms = 0.0
        physical = [{"role": "scenario", "reset_sha256": collector.physical_resets["scenario"]}]
    else:
        samples = list(result.samples)  # type: ignore[arg-type]
        planned = started = completed = len(samples)
        dropped = 0
        slip = 0.0
        drained = all(bool(sample["valid"]) for sample in samples)
        drain_ms = 0.0
        physical = [{"role": "scenario", "reset_sha256": collector.physical_resets["scenario"]}]
    return {
        "schedule_sha256": definition["schedules"][f"{scenario}:{split}"],
        "expanded_schedule": dict(expanded_schedule),
        "expanded_schedule_sha256": sha256_json(expanded_schedule),
        "seed": recipe["seed"]["value"],
        "planned": planned,
        "started": started,
        "completed": completed,
        "unfinished": started - completed,
        "dropped": dropped,
        "p99_start_slip_ms": slip,
        "drained": drained,
        "drain_ms": drain_ms,
        "streams_overlap": bool(result.metrics["mixed_gates"]["overlap"]) if scenario == "mixed" else None,
        "physical_resets": physical,
    }


def _run_scenario(
    repo: Path,
    prepared: PreparedBuild,
    recipe: Mapping[str, Any],
    definition: Mapping[str, Any],
    *,
    scenario: str,
    split: str,
    run: Mapping[str, Any],
    baseline_sustainable_qps: Optional[float],
    output: Path,
) -> tuple[Path, ScenarioResult]:
    collector = EvidenceCollector()
    query_cases = recipe["query_splits"][split if scenario != "mixed" else "stress"]
    event_cases = recipe["event_splits"][split if scenario != "mixed" else "stress"]
    query_count = 0
    event_count = 0
    expanded_schedule: Mapping[str, Any]
    if scenario == "qps":
        setup_errors: list[str] = []

        def sandbox_factory(_spec: Any) -> ManagedSandbox:
            try:
                return _start_measured_sandbox(
                    repo,
                    prepared.runtime,
                    str(prepared.protocol["image_digest"]),
                    recipe,
                    collector,
                    reset_role="trial",
                    authoritative=bool(run["authoritative"]),
                )
            except BaseException as error:
                setup_errors.append(str(error))
                raise

        runtime_factory = make_owned_sandbox_qps_runtime_factory(
            sandbox_factory, lambda sandbox: sandbox.trial_telemetry()
        )
        result = run_qps_scenario(query_cases, runtime_factory, profile=run["profile"])
        if setup_errors:
            raise SuiteFailure(f"QPS sandbox setup failed: {setup_errors[0]}")
        expanded_schedule = {
            "scenario": scenario,
            "split": split,
            "trials": [
                {
                    "offered_qps": sample["offered_qps"],
                    "planned": sample["outcomes"]["planned"],
                    "duration_s": sample["duration_s"],
                    "replicate": sample["replicate"],
                }
                for sample in result.samples
            ],
        }
    elif scenario == "ttr":
        sandbox = _start_measured_sandbox(
            repo,
            prepared.runtime,
            str(prepared.protocol["image_digest"]),
            recipe,
            collector,
            reset_role="scenario",
            authoritative=bool(run["authoritative"]),
        )
        try:
            samples = 3 if run["profile"] == "smoke" else 15
            result = run_ttr_scenario(
                query_cases,
                make_owned_sandbox_ttr_runtime_factory(sandbox),
                samples=samples,
            )
            expanded_schedule = {
                "scenario": scenario,
                "split": split,
                "sample_case_ids": [
                    query_cases[index % len(query_cases)]["case_id"]
                    for index in range(samples)
                ],
            }
        finally:
            sandbox.down()
    elif scenario in {"etd_idle", "etd_loaded"}:
        sandbox = _start_measured_sandbox(
            repo,
            prepared.runtime,
            str(prepared.protocol["image_digest"]),
            recipe,
            collector,
            reset_role="scenario",
            authoritative=bool(run["authoritative"]),
        )
        event_schedule = open_event_schedule(recipe, split)
        event_count = len(event_schedule)
        timeout_s = recipe["schedule_templates"]["etd"]["visibility_timeout_ns"] / 1_000_000_000
        try:
            query_load = None
            if scenario == "etd_loaded":
                if baseline_sustainable_qps is None or baseline_sustainable_qps <= 0:
                    raise SuiteFailure("loaded ETD requires a frozen positive baseline capacity")
                offered = 0.75 * baseline_sustainable_qps
                load_cases = {case["case_id"]: case for case in recipe["query_splits"]["stress"]}
                load_schedule = open_query_schedule(recipe, "stress", offered, stream="mixed")
                query_load = make_owned_sandbox_query_load(
                    sandbox,
                    load_cases,
                    load_schedule,
                    offered_qps=offered,
                    timeout_s=5.0,
                )
                expanded_schedule = {
                    "scenario": scenario,
                    "split": split,
                    "events": event_schedule,
                    "background_queries": load_schedule,
                    "offered_qps": offered,
                }
            else:
                expanded_schedule = {
                    "scenario": scenario,
                    "split": split,
                    "events": event_schedule,
                    "background_queries": [],
                }
            result = run_owned_sandbox_etd_scenario(
                sandbox,
                event_cases,
                event_schedule,
                mode="idle" if scenario == "etd_idle" else "loaded",
                timeout_s=timeout_s,
                poll_interval_s=recipe["schedule_templates"]["etd"]["poll_interval_ns"] / 1_000_000_000,
                baseline_sustainable_qps=(
                    baseline_sustainable_qps
                    if scenario == "etd_loaded"
                    else None
                ),
                query_load=query_load,
            )
        finally:
            sandbox.down()
    else:
        if baseline_sustainable_qps is None or baseline_sustainable_qps <= 0:
            raise SuiteFailure("mixed scenario requires a frozen positive baseline capacity")
        offered = 0.75 * baseline_sustainable_qps
        schedules = mixed_control_schedules(recipe, offered)
        query_schedule = schedules["combined"]["queries"]
        event_schedule = schedules["combined"]["events"]
        query_count = len(query_schedule)
        event_count = len(event_schedule)
        mixed_query_cases = {case["case_id"]: case for case in recipe["query_splits"]["stress"]}
        mixed_event_cases = {event["case_id"]: event for event in recipe["event_splits"]["stress"]}
        role_map = {"query_only": "query_control", "ingest_only": "ingest_control", "combined": "combined"}

        def sandbox_factory(label: str) -> ManagedSandbox:
            return _start_measured_sandbox(
                repo,
                prepared.runtime,
                str(prepared.protocol["image_digest"]),
                recipe,
                collector,
                reset_role=role_map[label],
                authoritative=bool(run["authoritative"]),
            )

        arm_factory = make_owned_sandbox_mixed_arm_factory(
            sandbox_factory,
            mixed_query_cases,
            mixed_event_cases,
            query_rate_qps=offered,
            recipe_fingerprint=recipe["fixture_sha256"],
            request_timeout_s=5.0,
            poll_interval_s=recipe["schedule_templates"]["etd"]["poll_interval_ns"] / 1_000_000_000,
        )
        result = run_mixed_scenario(query_schedule, event_schedule, arm_factory)
        expanded_schedule = {
            "scenario": scenario,
            "split": split,
            "streams": schedules,
            "offered_qps": offered,
        }
    resources = collector.resource_artifact(authoritative=bool(run["authoritative"]))
    binary = collector.binary_artifact(prepared.protocol)
    cache = collector.cache_artifact(recipe, scenario, split)
    schedule = _schedule_evidence(
        result,
        scenario,
        split,
        definition,
        recipe,
        collector,
        expanded_schedule=expanded_schedule,
        query_count=query_count,
        event_count=event_count,
    )
    semantic = _semantic_evidence(result, scenario, split, recipe)
    schedule_pass = schedule_gate_passes(schedule)
    scenario_pass = _scenario_pass(result, scenario)
    resource_pass = resource_gate_passes(resources)
    gates = {
        "correctness": semantic["passed"],
        "resources": resource_pass,
        "schedule": bool(schedule_pass),
        "scenario": scenario_pass,
    }
    failed = not semantic["passed"] or not schedule_pass or not scenario_pass or (run["authoritative"] and not resource_pass)
    conclusive = not (scenario == "qps" and result.metrics["capacity_censoring"] != "none")
    status = "fail" if failed else ("inconclusive" if not run["authoritative"] or run["profile"] == "smoke" or not conclusive else "pass")
    document = create_scenario_result(
        scenario=scenario,
        split=split,
        suite_definition_sha256=sha256_json(definition),
        run=run,
        status=status,
        cache=cache,
        binary=binary,
        resources=resources,
        schedule=schedule,
        metrics=result.metrics,
        samples=result.samples,
        semantic=semantic,
        gates=gates,
    )
    path = output / f"{scenario}-{split}.json"
    write_json_atomic(path, document)
    return path, result


def _ordered_usage(recipe: Mapping[str, Any], purpose: str) -> tuple[tuple[str, str], ...]:
    if purpose == "comparison":
        usage = tuple((scenario, "research") for scenario in SCENARIOS[:-1]) + tuple(
            (scenario, "holdout") for scenario in SCENARIOS[:-1]
        ) + (("mixed", "stress"),)
        validate_split_usage(recipe, usage, "comparison")
        return usage
    usage = required_split_usage(recipe, "baseline")
    validate_split_usage(recipe, usage, "baseline")
    return usage


def _qps_capacity_for_follow_on_load(
    result: ScenarioResult,
    *,
    authoritative: bool,
) -> float:
    capacity = float(result.metrics["sustainable_qps"])
    if capacity > 0 or authoritative:
        return capacity
    # A best-effort local run may miss the fixed scheduler gate by a small
    # margin even when every request completes correctly. Keep the artifact
    # failed, but use observed goodput to exercise the remaining scenarios.
    return max(
        (float(sample["achieved_goodput_qps"]) for sample in result.samples),
        default=0.0,
    )


def _run_logical_arm(
    repo: Path,
    prepared: PreparedBuild,
    recipe: Mapping[str, Any],
    definition: Mapping[str, Any],
    *,
    arm: str,
    pair_id: int,
    order: str,
    purpose: str,
    authoritative: bool,
    baseline_sustainable_qps: Optional[float],
    output: Path,
) -> tuple[list[Path], float]:
    logical_reset = run_id()
    run = {
        "run_id": f"run-{arm}-{pair_id}-{logical_reset}",
        "reset_id": logical_reset,
        "arm": arm,
        "pair_id": pair_id,
        "order": order,
        "profile": recipe["profile"],
        "authoritative": authoritative,
    }
    paths: list[Path] = []
    measured_capacity = baseline_sustainable_qps
    research_passed = True
    for scenario, split in _ordered_usage(recipe, purpose):
        if purpose == "comparison" and split == "holdout" and not research_passed:
            raise SuiteFailure("holdout is prohibited because the research trigger failed")
        path, result = _run_scenario(
            repo,
            prepared,
            recipe,
            definition,
            scenario=scenario,
            split=split,
            run=run,
            baseline_sustainable_qps=measured_capacity,
            output=output,
        )
        paths.append(path)
        document = load_document(path)
        if split == "research" and document["status"] != "pass":
            research_passed = False
        if scenario == "qps" and split == "research" and measured_capacity is None:
            measured_capacity = _qps_capacity_for_follow_on_load(
                result,
                authoritative=authoritative,
            )
    if measured_capacity is None or measured_capacity <= 0:
        raise SuiteFailure("arm did not produce a positive baseline capacity")
    return paths, measured_capacity


def _definition(
    profile: str,
    recipe: Mapping[str, Any],
    prepared: Mapping[str, PreparedBuild],
    build_recipe: Mapping[str, Any],
) -> Mapping[str, Any]:
    return create_suite_definition(
        profile=profile,
        fixture=recipe,
        build_recipe=build_recipe,
        builds=[item.protocol for item in prepared.values()],
        schedules=_schedule_hashes(recipe),
    )


def freeze(profile: str, output: Path) -> None:
    recipe = build_recipe(profile)
    validate_recipe(recipe)
    output.mkdir(parents=True, exist_ok=False)
    write_json_atomic(
        output / f"fixture-{profile}.json",
        recipe,
        validator=validate_recipe,
    )
    policy = policy_document()

    def validate_policy(document: Any) -> None:
        if document != policy:
            raise SuiteFailure("policy document changed before write")

    write_json_atomic(
        output / f"policy-{profile}.json",
        policy,
        validator=validate_policy,
    )


def validate_path(path: Path) -> None:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SuiteFailure(f"cannot load {path}: {error}") from error
    if (
        isinstance(document, dict)
        and document.get("schema_version") == "moraine-local-comparison-v1"
    ):
        _validate_local_comparison(document)
    elif isinstance(document, dict) and document.get("document_type") == "suite_manifest":
        load_suite_manifest(path)
    elif isinstance(document, dict) and document.get("document_type") == "native_central_burst":
        validate_native_burst_artifact(document)
    elif (
        isinstance(document, dict)
        and document.get("document_type") == "source_publication_append_probe"
    ):
        validate_source_publication_probe(document)
    elif isinstance(document, dict) and "document_type" in document:
        validate_document(document)
    elif isinstance(document, dict) and "recipe_version" in document:
        validate_recipe(document)
    else:
        raise SuiteFailure(f"{path} is neither a protocol document nor a fixture recipe")


def run_baseline(repositories: Mapping[str, Path], profile: str, output: Path) -> list[Path]:
    if set(repositories) != {"baseline"}:
        raise SuiteFailure("baseline workflow requires exactly one baseline repository")
    recipe = build_recipe(profile)
    validate_recipe(recipe)
    output.mkdir(parents=True, exist_ok=False)
    prepared, build_recipe_document = _prepare_builds(
        repositories,
        output / "builds",
        authoritative=profile == "full",
    )
    definition = _definition(profile, recipe, prepared, build_recipe_document)
    manifest_paths: list[Path] = []
    count = 1 if profile == "smoke" else 7
    purpose = "smoke" if profile == "smoke" else "repeatability"
    for index in range(1, count + 1):
        run_root = output / f"baseline-{index:02d}"
        artifacts_root = run_root / "artifacts"
        artifacts_root.mkdir(parents=True)
        paths, _ = _run_logical_arm(
            repositories["baseline"],
            prepared["baseline"],
            recipe,
            definition,
            arm="baseline",
            pair_id=1,
            order="AB",
            purpose=purpose,
            authoritative=profile == "full",
            baseline_sustainable_qps=None,
            output=artifacts_root,
        )
        manifest_path = run_root / "manifest.json"
        manifest = create_suite_manifest(
            purpose=purpose,
            arm="baseline",
            suite_definition=definition,
            artifact_paths=paths,
            manifest_path=manifest_path,
        )
        write_json_atomic(manifest_path, manifest)
        manifest_paths.append(manifest_path)
    if profile == "full":
        write_json_atomic(output / "repeatability.json", evaluate_repeatability(manifest_paths))
    return manifest_paths
def _local_docker_platform() -> Mapping[str, Any]:
    process = subprocess.run(
        ["docker", "info", "--format", "{{json .}}"],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if process.returncode:
        raise SuiteFailure(f"cannot inspect local Docker environment: {process.stderr.strip()}")
    try:
        value = json.loads(process.stdout)
    except json.JSONDecodeError as error:
        raise SuiteFailure("Docker returned invalid environment metadata") from error
    fields = (
        "OperatingSystem",
        "Architecture",
        "NCPU",
        "MemTotal",
        "ServerVersion",
        "CgroupVersion",
        "CgroupDriver",
    )
    return {name: value.get(name) for name in fields}


def _interval_midpoint(interval: Mapping[str, Any]) -> Optional[float]:
    lower = interval.get("lower_ms")
    upper = interval.get("upper_ms")
    if not isinstance(lower, (int, float)) or isinstance(lower, bool):
        return None
    if not isinstance(upper, (int, float)) or isinstance(upper, bool):
        return None
    return (float(lower) + float(upper)) / 2.0


def _positive_geometric_mean(values: Sequence[float]) -> Optional[float]:
    if not values or any(value <= 0 for value in values):
        return None
    return statistics.geometric_mean(values)


def _validate_local_comparison(document: Any) -> None:
    if not isinstance(document, Mapping):
        raise SuiteFailure("local comparison must be a mapping")
    if document.get("schema_version") != "moraine-local-comparison-v1":
        raise SuiteFailure("local comparison schema is invalid")
    if document.get("mode") != "local_comparative" or document.get("authoritative") is not False:
        raise SuiteFailure("local comparison must remain explicitly non-authoritative")
    pairs = document.get("pairs")
    pair_results = document.get("pair_results")
    if (
        isinstance(pairs, bool)
        or not isinstance(pairs, int)
        or pairs < 1
        or not isinstance(pair_results, list)
        or len(pair_results) != pairs
    ):
        raise SuiteFailure("local comparison pair evidence is incomplete")
    artifacts = document.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise SuiteFailure("local comparison artifacts are missing")
    suite_definition_sha256 = document.get("suite_definition_sha256")
    builds = document.get("builds")
    bindings = document.get("artifact_bindings")
    if (
        not isinstance(suite_definition_sha256, str)
        or not isinstance(builds, Mapping)
        or not isinstance(builds.get("candidate"), str)
        or not isinstance(bindings, Mapping)
        or set(bindings) != set(artifacts)
    ):
        raise SuiteFailure("local comparison artifact identities are incomplete")
    for binding in bindings.values():
        if not isinstance(binding, Mapping) or any(
            not isinstance(binding.get(field), str)
            for field in (
                "suite_definition_sha256",
                "build_identity_sha256",
                "semantic_oracle_sha256",
            )
        ):
            raise SuiteFailure("local comparison artifact binding is invalid")
    for raw in artifacts:
        if not isinstance(raw, str) or Path(raw).is_absolute() or ".." in Path(raw).parts:
            raise SuiteFailure("local comparison artifact path is not relative")


def _positive_metric(value: Any, field: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0
    ):
        raise SuiteFailure(f"autoresearch metric {field} must be finite and positive")
    return float(value)


def _local_metric_artifact(
    document: Mapping[str, Any],
    pair_index: int,
    scenario: str,
    artifact_loader: Callable[[str], Mapping[str, Any]],
) -> Mapping[str, Any]:
    relative_path = f"candidate/pair-{pair_index}/artifacts/{scenario}-research.json"
    if relative_path not in document["artifacts"]:
        raise SuiteFailure(
            f"autoresearch local pair {pair_index} {scenario} artifact is missing"
        )
    artifact = artifact_loader(relative_path)
    binding = document["artifact_bindings"].get(relative_path)
    binary = artifact.get("binary")
    semantic = artifact.get("semantic")
    run = artifact.get("run")
    gates = artifact.get("gates")
    if (
        artifact.get("document_type") != "scenario_result"
        or artifact.get("scenario") != scenario
        or artifact.get("split") != "research"
        or not isinstance(run, Mapping)
        or run.get("arm") != "candidate"
        or run.get("pair_id") != pair_index
        or run.get("authoritative") is not False
        or artifact.get("status") == "fail"
        or not isinstance(gates, Mapping)
        or any(gates.get(name) is not True for name in ("correctness", "schedule", "scenario"))
        or not isinstance(binding, Mapping)
        or artifact.get("suite_definition_sha256")
        != document.get("suite_definition_sha256")
        or binding.get("suite_definition_sha256")
        != artifact.get("suite_definition_sha256")
        or not isinstance(binary, Mapping)
        or binary.get("build_identity_sha256")
        != document.get("builds", {}).get("candidate")
        or binding.get("build_identity_sha256")
        != binary.get("build_identity_sha256")
        or not isinstance(semantic, Mapping)
        or binding.get("semantic_oracle_sha256") != semantic.get("oracle_sha256")
    ):
        raise SuiteFailure(
            f"autoresearch local pair {pair_index} {scenario} evidence did not pass"
        )
    return artifact


def autoresearch_metrics(
    document: Mapping[str, Any],
    *,
    artifact_loader: Optional[Callable[[str], Mapping[str, Any]]] = None,
) -> tuple[tuple[str, float | int], ...]:
    """Translate validated suite evidence into OMP autoresearch metrics."""

    if document.get("schema_version") == "moraine-local-comparison-v1":
        _validate_local_comparison(document)
        if artifact_loader is None:
            raise SuiteFailure("local autoresearch metrics require referenced artifacts")
        qps_values: list[float] = []
        ttr_values: list[float] = []
        etd_values: list[float] = []
        for pair_index, pair in enumerate(document["pair_results"], 1):
            if (
                not isinstance(pair, Mapping)
                or pair.get("pair_id") != pair_index
                or not isinstance(pair.get("candidate"), Mapping)
            ):
                raise SuiteFailure(
                    f"autoresearch local pair {pair_index} candidate evidence is missing"
                )
            candidate = pair["candidate"]
            qps_artifact = _local_metric_artifact(
                document, pair_index, "qps", artifact_loader
            )
            qps_metrics = qps_artifact.get("metrics")
            if (
                not isinstance(qps_metrics, Mapping)
                or qps_metrics.get("capacity_censoring") != "none"
            ):
                raise SuiteFailure(
                    f"autoresearch local pair {pair_index} QPS capacity is censored"
                )
            qps = _positive_metric(
                qps_metrics.get("sustainable_qps"),
                f"pair {pair_index} candidate qps",
            )
            ttr_artifact = _local_metric_artifact(
                document, pair_index, "ttr", artifact_loader
            )
            ttr_metrics = ttr_artifact.get("metrics")
            if not isinstance(ttr_metrics, Mapping):
                raise SuiteFailure(
                    f"autoresearch local pair {pair_index} TTR metrics are missing"
                )
            ttr = _positive_metric(
                ttr_metrics.get("p95_ms"),
                f"pair {pair_index} candidate ttr_p95_ms",
            )
            etd_artifact = _local_metric_artifact(
                document, pair_index, "etd_loaded", artifact_loader
            )
            etd_metrics = etd_artifact.get("metrics")
            if not isinstance(etd_metrics, Mapping):
                raise SuiteFailure(
                    f"autoresearch local pair {pair_index} loaded ETD metrics are missing"
                )
            etd = _positive_metric(
                _interval_midpoint(etd_metrics.get("source_etd_p95", {})),
                f"pair {pair_index} candidate source_etd_p95_midpoint_ms",
            )
            copied = (
                candidate.get("qps"),
                candidate.get("ttr_p95_ms"),
                candidate.get("source_etd_p95_midpoint_ms"),
            )
            derived = (qps, ttr, etd)
            if any(
                isinstance(observed, bool)
                or not isinstance(observed, (int, float))
                or not math.isclose(float(observed), expected, rel_tol=1e-12)
                for observed, expected in zip(copied, derived)
            ):
                raise SuiteFailure(
                    f"autoresearch local pair {pair_index} summary metrics disagree with artifacts"
                )
            qps_values.append(qps)
            ttr_values.append(ttr)
            etd_values.append(etd)
        qps = statistics.geometric_mean(qps_values)
        return (
            ("retrieval_operational_ns_per_query", round(1_000_000_000 / qps)),
            ("retrieval_sustainable_qps", qps),
            ("retrieval_ttr_p95_ms", statistics.geometric_mean(ttr_values)),
            (
                "retrieval_loaded_etd_p95_midpoint_ms",
                statistics.geometric_mean(etd_values),
            ),
        )

    validate_document(document)
    if (
        document.get("document_type") != "scenario_result"
        or document.get("scenario") != "qps"
    ):
        raise SuiteFailure("autoresearch metrics require a QPS result or local comparison")
    if document.get("status") != "pass":
        raise SuiteFailure("autoresearch QPS evidence must pass its scenario gates")
    qps = _positive_metric(
        document.get("metrics", {}).get("sustainable_qps"),
        "sustainable_qps",
    )
    return (
        ("retrieval_operational_ns_per_query", round(1_000_000_000 / qps)),
        ("retrieval_sustainable_qps", qps),
    )


def _reject_nonfinite_json(token: str) -> None:
    raise SuiteFailure(f"non-finite JSON constant: {token}")


def emit_autoresearch_metrics(path: Path) -> None:
    document = json.loads(
        path.read_text(encoding="utf-8"),
        parse_constant=_reject_nonfinite_json,
    )
    if not isinstance(document, Mapping):
        raise SuiteFailure("autoresearch evidence must be a JSON object")
    for name, value in autoresearch_metrics(
        document,
        artifact_loader=lambda relative: load_document(path.parent / relative),
    ):
        rendered = str(value) if isinstance(value, int) else f"{value:.6f}"
        print(f"METRIC {name}={rendered}")


def run_local_comparison(
    repositories: Mapping[str, Path],
    *,
    profile: str,
    pairs: int,
    output: Path,
) -> Path:
    """Run a paired, directional comparison under the local Docker scheduler."""

    if set(repositories) != {"baseline", "candidate"}:
        raise SuiteFailure("local comparison requires baseline and candidate repositories")
    if profile not in {"smoke", "full"}:
        raise SuiteFailure("local comparison profile must be smoke or full")
    if pairs < 1 or pairs > len(PAIR_ORDER):
        raise SuiteFailure(f"local comparison pairs must be between 1 and {len(PAIR_ORDER)}")
    recipe = build_recipe(profile)
    validate_recipe(recipe)
    output.mkdir(parents=True, exist_ok=False)
    prepared, build_recipe_document = _prepare_builds(
        repositories,
        output / "builds",
        authoritative=False,
    )
    definition = _definition(profile, recipe, prepared, build_recipe_document)
    frozen_capacity: Optional[float] = None
    pair_documents: list[dict[str, Any]] = []
    artifact_paths: list[str] = []
    artifact_bindings: dict[str, dict[str, str]] = {}
    for pair_id, order in enumerate(PAIR_ORDER[:pairs], 1):
        sequence = ("baseline", "candidate") if order == "AB" else ("candidate", "baseline")
        by_arm: dict[str, dict[str, Mapping[str, Any]]] = {}
        for arm in sequence:
            artifacts_root = output / arm / f"pair-{pair_id}" / "artifacts"
            artifacts_root.mkdir(parents=True)
            paths, measured_capacity = _run_logical_arm(
                repositories[arm],
                prepared[arm],
                recipe,
                definition,
                arm=arm,
                pair_id=pair_id,
                order=order,
                purpose="baseline",
                authoritative=False,
                baseline_sustainable_qps=frozen_capacity,
                output=artifacts_root,
            )
            if frozen_capacity is None:
                if arm != "baseline":
                    raise SuiteFailure("first local pair must establish baseline capacity before candidate")
                frozen_capacity = measured_capacity
            by_arm[arm] = {}
            for path in paths:
                document = load_document(path)
                by_arm[arm][str(document["scenario"])] = document
                relative_path = str(path.relative_to(output))
                artifact_paths.append(relative_path)
                artifact_bindings[relative_path] = {
                    "suite_definition_sha256": document["suite_definition_sha256"],
                    "build_identity_sha256": document["binary"][
                        "build_identity_sha256"
                    ],
                    "semantic_oracle_sha256": document["semantic"]["oracle_sha256"],
                }
        baseline = by_arm["baseline"]
        candidate = by_arm["candidate"]
        baseline_qps = float(baseline["qps"]["metrics"]["sustainable_qps"])
        candidate_qps = float(candidate["qps"]["metrics"]["sustainable_qps"])
        baseline_ttr = float(baseline["ttr"]["metrics"]["p95_ms"])
        candidate_ttr = float(candidate["ttr"]["metrics"]["p95_ms"])
        baseline_etd = _interval_midpoint(
            baseline["etd_loaded"]["metrics"]["source_etd_p95"]
        )
        candidate_etd = _interval_midpoint(
            candidate["etd_loaded"]["metrics"]["source_etd_p95"]
        )
        pair_documents.append(
            {
                "pair_id": pair_id,
                "order": order,
                "baseline": {
                    "qps": baseline_qps,
                    "ttr_p95_ms": baseline_ttr,
                    "source_etd_p95_midpoint_ms": baseline_etd,
                    "mixed_ratios": baseline["mixed"]["metrics"]["ratios"],
                },
                "candidate": {
                    "qps": candidate_qps,
                    "ttr_p95_ms": candidate_ttr,
                    "source_etd_p95_midpoint_ms": candidate_etd,
                    "mixed_ratios": candidate["mixed"]["metrics"]["ratios"],
                },
                "ratios": {
                    "qps": candidate_qps / baseline_qps if baseline_qps > 0 else None,
                    "ttr": baseline_ttr / candidate_ttr if candidate_ttr > 0 else None,
                    "source_etd": (
                        baseline_etd / candidate_etd
                        if baseline_etd is not None
                        and candidate_etd is not None
                        and candidate_etd > 0
                        else None
                    ),
                },
            }
        )
    ratios = {
        name: [
            float(pair["ratios"][name])
            for pair in pair_documents
            if pair["ratios"][name] is not None
        ]
        for name in ("qps", "ttr", "source_etd")
    }
    summary = {
        "schema_version": "moraine-local-comparison-v1",
        "mode": "local_comparative",
        "authoritative": False,
        "suite_definition_sha256": sha256_json(definition),
        "profile": profile,
        "pairs": pairs,
        "docker_platform": _local_docker_platform(),
        "builds": {
            arm: prepared[arm].protocol["identity_sha256"]
            for arm in ("baseline", "candidate")
        },
        "frozen_baseline_capacity_qps": frozen_capacity,
        "pair_results": pair_documents,
        "aggregate_ratios": {
            name: _positive_geometric_mean(values)
            for name, values in ratios.items()
        },
        "candidate_wins": {
            name: sum(value > 1.0 for value in values)
            for name, values in ratios.items()
        },
        "artifact_bindings": artifact_bindings,
        "artifacts": artifact_paths,
        "interpretation": (
            "Directional paired evidence for this Docker environment only; "
            "resource isolation is best-effort and results are not authoritative."
        ),
    }
    path = output / "local-comparison.json"
    write_json_atomic(path, summary, validator=_validate_local_comparison)
    return path




def _bind_repeatability_study(
    manifest_paths: Sequence[Path],
    current_definition: Mapping[str, Any],
) -> None:
    """Reject baseline studies produced by another build or suite contract."""

    current_baseline = next(
        build for build in current_definition["builds"] if build["arm"] == "baseline"
    )
    contract_fields = (
        "suite_id",
        "profile",
        "resource_envelope",
        "cache_policy",
        "fixture",
        "policy",
        "build_recipe",
        "schedules",
    )
    for path in manifest_paths:
        manifest, _ = load_suite_manifest(path)
        study_definition = manifest["suite_definition"]
        study_baseline = next(
            build for build in study_definition["builds"] if build["arm"] == "baseline"
        )
        if study_baseline != current_baseline:
            raise SuiteFailure(
                f"repeatability study build does not match current baseline: {path}"
            )
        if any(
            study_definition[field] != current_definition[field]
            for field in contract_fields
        ):
            raise SuiteFailure(
                f"repeatability study suite contract does not match current comparison: {path}"
            )


def run_comparison(
    repositories: Mapping[str, Path],
    baseline_manifests: Sequence[Path],
    output: Path,
) -> tuple[Path, Path, Path]:
    if set(repositories) != {"baseline", "candidate"}:
        raise SuiteFailure("comparison requires baseline and candidate repositories")
    repeatability = evaluate_repeatability(baseline_manifests)
    if repeatability["status"] != "pass":
        raise SuiteFailure("comparison requires a passing seven-run baseline repeatability study")
    recipe = build_recipe("full")
    validate_recipe(recipe)
    output.mkdir(parents=True, exist_ok=False)
    prepared, build_recipe_document = _prepare_builds(
        repositories,
        output / "builds",
        authoritative=True,
    )
    definition = _definition("full", recipe, prepared, build_recipe_document)
    _bind_repeatability_study(baseline_manifests, definition)
    baseline_capacity = statistics.median(repeatability["metrics"]["values"]["qps"])
    all_paths: dict[str, list[Path]] = {"baseline": [], "candidate": []}
    for pair_id, order in enumerate(PAIR_ORDER, 1):
        sequence = ("baseline", "candidate") if order == "AB" else ("candidate", "baseline")
        for arm in sequence:
            artifacts_root = output / arm / f"pair-{pair_id}" / "artifacts"
            artifacts_root.mkdir(parents=True)
            paths, _ = _run_logical_arm(
                repositories[arm],
                prepared[arm],
                recipe,
                definition,
                arm=arm,
                pair_id=pair_id,
                order=order,
                purpose="comparison",
                authoritative=True,
                baseline_sustainable_qps=baseline_capacity,
                output=artifacts_root,
            )
            all_paths[arm].extend(paths)
    manifests: dict[str, Path] = {}
    for arm in ("baseline", "candidate"):
        manifest_path = output / arm / "manifest.json"
        manifest = create_suite_manifest(
            purpose="comparison",
            arm=arm,
            suite_definition=definition,
            artifact_paths=all_paths[arm],
            manifest_path=manifest_path,
        )
        write_json_atomic(manifest_path, manifest)
        manifests[arm] = manifest_path
    comparison_path = output / "comparison.json"
    write_json_atomic(comparison_path, compare_manifests(manifests["baseline"], manifests["candidate"]))
    return manifests["baseline"], manifests["candidate"], comparison_path


def _parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    freeze_parser = commands.add_parser("freeze")
    freeze_parser.add_argument("--profile", choices=("smoke", "full"), default="full")
    freeze_parser.add_argument("--output", type=Path, required=True)
    validate_parser = commands.add_parser("validate")
    validate_parser.add_argument("paths", type=Path, nargs="+")
    metrics_parser = commands.add_parser("autoresearch-metrics")
    metrics_parser.add_argument("path", type=Path)
    compare_parser = commands.add_parser("compare")
    compare_parser.add_argument("baseline", type=Path)
    compare_parser.add_argument("candidate", type=Path)
    compare_parser.add_argument("--output", type=Path, required=True)
    repeatability_parser = commands.add_parser("repeatability")
    repeatability_parser.add_argument("manifests", type=Path, nargs=7)
    repeatability_parser.add_argument("--output", type=Path, required=True)
    smoke_parser = commands.add_parser("smoke")
    smoke_parser.add_argument("--repo", type=Path, default=Path.cwd())
    smoke_parser.add_argument("--output", type=Path, required=True)
    native_parser = commands.add_parser(
        "native-central-burst",
        help="run synchronized search_sessions bursts through an owned native macOS daemon",
    )
    native_parser.add_argument("--mcp-binary", type=Path, required=True)
    native_parser.add_argument("--config", type=Path, required=True)
    native_parser.add_argument("--route-cwd", type=Path, default=Path.cwd())
    native_parser.add_argument("--profile", choices=("smoke", "full"), default="full")
    native_parser.add_argument(
        "--split",
        choices=("research", "holdout", "stress"),
        default="research",
    )
    native_parser.add_argument("--modes", nargs="+", default=list(NATIVE_BURST_DEFAULT_MODES))
    native_parser.add_argument("--cases-per-mode", type=int, default=1)
    native_parser.add_argument("--bursts-per-case", type=int, default=25)
    native_parser.add_argument(
        "--cold-repetitions",
        type=int,
        default=NATIVE_BURST_DEFAULT_COLD_REPETITIONS,
        help="fresh-daemon first-query repetitions per concurrency",
    )
    native_parser.add_argument(
        "--minimum-cold-samples",
        type=int,
        default=NATIVE_BURST_DEFAULT_MIN_COLD_SAMPLES,
    )
    native_parser.add_argument(
        "--warm-p95-limit-ms",
        type=float,
        default=NATIVE_BURST_DEFAULT_WARM_P95_LIMIT_MS,
        help="inclusive per-mode steady-state p95 gate",
    )
    native_parser.add_argument(
        "--cold-p95-limit-ms",
        type=float,
        default=NATIVE_BURST_DEFAULT_COLD_P95_LIMIT_MS,
        help="inclusive cold hydration/common and session-scope p95 gate",
    )
    native_parser.add_argument(
        "--max-latency-ms",
        type=float,
        default=NATIVE_BURST_DEFAULT_MAX_LATENCY_MS,
        help="inclusive maximum over every raw request sample",
    )
    native_parser.add_argument(
        "--collect-query-log",
        action="store_true",
        help="fail closed unless owned candidate/detail ClickHouse costs are captured",
    )
    native_parser.add_argument("--timeout-seconds", type=float, default=5.0)
    native_parser.add_argument("--startup-timeout-seconds", type=float, default=30.0)
    native_parser.add_argument("--output", type=Path, required=True)
    publication_parser = commands.add_parser(
        "source-publication-append-probe",
        help=(
            "measure 1/10k/100k head capture and >=100 durable same-file "
            "appends through production ingest and MCP"
        ),
    )
    publication_parser.add_argument("--repo", type=Path, default=Path.cwd())
    publication_parser.add_argument("--samples", type=int, default=100)
    publication_parser.add_argument("--timeout-seconds", type=float, default=5.0)
    publication_parser.add_argument("--p95-limit-ms", type=float, default=2_000.0)
    publication_parser.add_argument(
        "--mode", choices=("local", "authoritative"), default="local"
    )
    publication_parser.add_argument("--output", type=Path, required=True)
    run_parser = commands.add_parser("run")
    run_parser.add_argument(
        "--mode",
        choices=("local", "authoritative"),
        default="authoritative",
    )
    run_parser.add_argument("--profile", choices=("smoke", "full"))
    run_parser.add_argument("--pairs", type=int)
    run_parser.add_argument("--baseline", type=Path, required=True)
    run_parser.add_argument("--candidate", type=Path)
    run_parser.add_argument("--baseline-manifests", type=Path, nargs=7)
    run_parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        if args.command == "freeze":
            freeze(args.profile, args.output)
        elif args.command == "validate":
            for path in args.paths:
                validate_path(path)
        elif args.command == "autoresearch-metrics":
            emit_autoresearch_metrics(args.path)
        elif args.command == "compare":
            write_json_atomic(args.output, compare_manifests(args.baseline, args.candidate))
        elif args.command == "repeatability":
            write_json_atomic(args.output, evaluate_repeatability(args.manifests))
        elif args.command == "smoke":
            run_baseline({"baseline": args.repo.resolve()}, "smoke", args.output)
        elif args.command == "native-central-burst":
            document = run_native_central_burst(
                mcp_binary=args.mcp_binary.resolve(),
                config_path=args.config.resolve(),
                route_cwd=args.route_cwd.resolve(),
                profile=args.profile,
                split=args.split,
                modes=tuple(args.modes),
                cases_per_mode=args.cases_per_mode,
                bursts_per_case=args.bursts_per_case,
                timeout_s=args.timeout_seconds,
                startup_timeout_s=args.startup_timeout_seconds,
                cold_repetitions=args.cold_repetitions,
                min_cold_samples=args.minimum_cold_samples,
                warm_p95_limit_ms=args.warm_p95_limit_ms,
                cold_p95_limit_ms=args.cold_p95_limit_ms,
                max_latency_ms=args.max_latency_ms,
                collect_query_log=args.collect_query_log,
            )
            write_native_burst_artifact(args.output, document)
            if document["status"] != "pass":
                raise SuiteFailure(
                    "native central burst failed requests, latency gates, or query-log evidence; "
                    "artifact retained"
                )
        elif args.command == "source-publication-append-probe":
            artifact = run_source_publication_append_probe(
                args.repo.resolve(),
                output=args.output,
                samples=args.samples,
                timeout_s=args.timeout_seconds,
                p95_limit_ms=args.p95_limit_ms,
                authoritative=args.mode == "authoritative",
            )
            document = json.loads(artifact.read_text(encoding="utf-8"))
            validate_source_publication_probe(document)
            if document["status"] != "pass":
                raise SuiteFailure(
                    "source-publication append p95/head-write gates failed; artifact retained"
                )
        elif args.mode == "local":
            if args.candidate is None:
                raise SuiteFailure("local comparison requires --candidate")
            if args.baseline_manifests is not None:
                raise SuiteFailure("local comparison does not accept --baseline-manifests")
            local_profile = args.profile or "smoke"
            local_pairs = (
                args.pairs
                if args.pairs is not None
                else (3 if local_profile == "full" else 1)
            )
            run_local_comparison(
                {
                    "baseline": args.baseline.resolve(),
                    "candidate": args.candidate.resolve(),
                },
                profile=local_profile,
                pairs=local_pairs,
                output=args.output,
            )
        elif args.candidate is None:
            if args.profile is not None or args.pairs is not None:
                raise SuiteFailure("--profile and --pairs apply only to --mode local")
            if args.baseline_manifests is not None:
                raise SuiteFailure("baseline-only run does not accept --baseline-manifests")
            run_baseline({"baseline": args.baseline.resolve()}, "full", args.output)
        else:
            if args.profile is not None or args.pairs is not None:
                raise SuiteFailure("--profile and --pairs apply only to --mode local")
            if args.baseline_manifests is None:
                raise SuiteFailure("comparison run requires seven --baseline-manifests")
            run_comparison(
                {"baseline": args.baseline.resolve(), "candidate": args.candidate.resolve()},
                args.baseline_manifests,
                args.output,
            )
        return 0
    except (
        ProtocolError,
        RuntimeFailure,
        NativeBurstFailure,
        ScenarioError,
        SuiteFailure,
        OSError,
        ValueError,
        json.JSONDecodeError,
    ) as error:
        print(f"performance-suite: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
