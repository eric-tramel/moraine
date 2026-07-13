#!/usr/bin/env python3
"""Normative artifacts and policy for the fixed-resource performance suite.

This module is import-only.  It owns document construction, strict validation,
content hashing, atomic persistence, paired comparison, and repeatability.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import json
import math
import os
import random
import re
import statistics
import tempfile
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence
from urllib.parse import unquote_plus

PERFORMANCE_VERSION = "moraine-performance-v1"
SUITE_VERSION = "moraine-performance-suite-v1"
DOCUMENT_TYPES = frozenset({"scenario_result", "comparison", "repeatability", "suite_manifest"})
SCENARIOS = ("qps", "ttr", "etd_idle", "etd_loaded", "mixed")
SPLITS = frozenset({"research", "holdout", "stress"})
ARMS = frozenset({"baseline", "candidate"})
STATUSES = frozenset({"pass", "fail", "inconclusive"})
PAIR_ORDER = ("AB", "BA", "AB", "BA", "AB", "BA", "AB")
MEASURED_BINARY_ROLES = frozenset({"moraine-ingest", "moraine-mcp"})
COMPARISON_MATRIX = {
    "qps": ("research", "holdout"),
    "ttr": ("research", "holdout"),
    "etd_idle": ("research", "holdout"),
    "etd_loaded": ("research", "holdout"),
    "mixed": ("stress",),
}
REPEATABILITY_MATRIX = {
    "qps": ("research",),
    "ttr": ("research",),
    "etd_idle": ("research",),
    "etd_loaded": ("research",),
    "mixed": ("stress",),
}
CACHE_BYPASS_KEYS = frozenset({"result", "document_frequency", "posting", "corpus", "hydration"})
SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
GIT_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")

POLICY = {
    "pair_count": 7,
    "pair_order": PAIR_ORDER,
    "comparison_split": "holdout",
    "minimum_score_ratio": 1.05,
    "minimum_constituent_ratio": 0.97,
    "minimum_direction_pairs": 5,
    "qps_max_geometric_rsd": 0.10,
    "ttr_max_geometric_rsd": 0.10,
    "etd_max_geometric_rsd": 0.15,
    "bootstrap_samples": 10_000,
    "bootstrap_seed": 0x4D4F5241,
    "qps_trial_seconds": 30,
    "qps_replicates": 3,
    "qps_min": 1,
    "qps_max": 512,
    "smoke_qps_trial_seconds": 3,
    "smoke_qps_max": 16,
    "qps_p95_ms_max": 750.0,
    "qps_p99_ms_max": 2_000.0,
    "qps_deadline_ms": 5_000.0,
    "scheduler_p99_start_slip_ms_max": 10.0,
    "drain_ms_max": 5_000.0,
    "mixed_query_goodput_ratio_min": 0.90,
    "mixed_query_p95_degradation_max": 1.25,
    "mixed_query_p99_degradation_max": 1.25,
    "mixed_source_etd_degradation_max": 1.25,
    "mixed_db_ack_etd_degradation_max": 1.25,
    "repeatability_runs": 7,
    "repeatability_qps_mad_ratio": 0.05,
    "repeatability_ttr_mad_ratio": 0.10,
    "repeatability_etd_mad_ratio": 0.15,
}

_RESOURCE_FIELDS = frozenset({
    "authoritative", "cgroup_version", "cgroup_driver", "cgroup_identity_sha256",
    "role_membership_sha256", "controllers_enabled_proven", "effective_limits_proven",
    "host_headroom_proven", "cpuset_cpus_effective", "cpu_max_quota_us",
    "cpu_max_period_us", "cpu_usage_usec_delta", "cpu_nr_throttled_delta",
    "throttled_usec_delta", "memory_max_bytes", "memory_current_bytes",
    "memory_peak_bytes", "memory_event_high_delta", "memory_event_max_delta",
    "swap_max_bytes", "swap_current_bytes", "oom_kill_delta",
    "server_descendants_proven", "loadgen_excluded_proven",
})
_OUTCOME_FIELDS = frozenset({
    "planned", "started", "correct", "rejected", "timed_out", "semantic_error",
    "protocol_error", "malformed", "late", "dropped", "other_error",
})
_SCENARIO_FIELDS = frozenset({
    "document_type", "schema_version", "scenario", "split", "suite_definition_sha256",
    "artifact_sha256", "run", "status", "cache", "binary", "resources", "schedule",
    "metrics", "samples", "semantic", "gates",
})

CREDENTIALIZED_URL_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9+.-]*://[^\s/@]+@")
HOME_PATH_RE = re.compile(r"(?:^|[\s\"'=])(?:/(?:Users|home)/[^/\s]+|[A-Za-z]:[\\/]Users[\\/][^\\/\s]+)")
AUTH_VALUE_RE = re.compile(r"(?i)(?:authorization\s*:\s*)?\b(bearer|basic)\s+([A-Za-z0-9._~+/=-]+)")
SENSITIVE_ASSIGNMENT_RE = re.compile(
    r"(?i)(?:^|[^a-z0-9])(?:access[_-]?key|access[_-]?token|api[_-]?key|authorization|"
    r"conversation|cookie|credential|env(?:ironment)?|host(?:name)?|message|password|passwd|"
    r"private[_-]?key|prompt|query|refresh[_-]?token|secret|session[_-]?token|source[_-]?path|"
    r"user(?:name)?)\s*[=:]\s*[^/\s&?#]+"
)
PRIVATE_CONTENT_RE = re.compile(
    r"(?i)(?:(?:private|raw)[._ -]*(?:conversation|message|oracle|prompt|query|text)|"
    r"(?:conversation|message|oracle|prompt|query|text)[._ -]*(?:private|raw))"
)
USER_HOST_IDENTITY_RE = re.compile(r"(?i)(?:^|[/_.-])[a-z0-9][a-z0-9_.-]*@[a-z0-9][a-z0-9_.-]*(?:$|[/_.-])")
FORBIDDEN_KEYS = frozenset({
    "access_key", "access_token", "api_key", "authorization", "cgroup_path", "conversation",
    "conversation_text", "cookie", "credential", "credentials", "env", "environment", "host",
    "host_name", "hostname", "machine_id", "machine_name", "message", "messages", "oracle",
    "oracle_answers", "oracle_payload", "password", "passwd", "private_key", "prompt", "query",
    "query_text", "raw_conversation", "raw_oracle", "raw_query", "raw_text", "refresh_token",
    "secret", "session_token", "source_path", "text", "user", "user_name", "username",
})


class ProtocolError(ValueError):
    """A performance artifact violates the normative protocol."""


def _fail(path: str, message: str) -> None:
    raise ProtocolError(f"{path}: {message}")


def policy_document() -> dict[str, Any]:
    """Return the JSON representation embedded in every suite definition."""
    result = dict(POLICY)
    result["pair_order"] = list(PAIR_ORDER)
    return result


def sha256_bytes(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def canonical_json_bytes(value: Any) -> bytes:
    try:
        return (json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ProtocolError(f"value is not finite JSON: {error}") from error


def sha256_json(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def _mapping(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        _fail(path, "must be an object")
    return value


def _sequence(value: Any, path: str) -> Sequence[Any]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        _fail(path, "must be an array")
    return value


def _fields(value: Mapping[str, Any], expected: Iterable[str], path: str) -> None:
    expected_set = frozenset(expected)
    actual = frozenset(value)
    if actual != expected_set:
        missing = sorted(expected_set - actual)
        extra = sorted(actual - expected_set)
        _fail(path, f"fields differ (missing={missing}, extra={extra})")


def _string(value: Any, path: str, *, choices: Iterable[str] | None = None) -> str:
    if not isinstance(value, str) or not value:
        _fail(path, "must be a non-empty string")
    if choices is not None and value not in choices:
        _fail(path, f"unsupported value {value!r}")
    return value


def _boolean(value: Any, path: str) -> bool:
    if not isinstance(value, bool):
        _fail(path, "must be boolean")
    return value


def _integer(value: Any, path: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        _fail(path, f"must be an integer >= {minimum}")
    return value


def _number(value: Any, path: str, *, minimum: float | None = None, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        _fail(path, "must be a finite number")
    result = float(value)
    if positive and result <= 0:
        _fail(path, "must be positive")
    if minimum is not None and result < minimum:
        _fail(path, f"must be >= {minimum}")
    return result


def _optional_number(value: Any, path: str, *, minimum: float = 0.0) -> float | None:
    if value is None:
        return None
    return _number(value, path, minimum=minimum)


def _sha(value: Any, path: str) -> str:
    text = _string(value, path)
    if not SHA256_RE.fullmatch(text):
        _fail(path, "must be sha256:<64 lowercase hex>")
    return text


def _decoded_string_forms(value: str) -> list[str]:
    forms = [value]
    while True:
        decoded = unquote_plus(forms[-1])
        if decoded == forms[-1] or decoded in forms:
            return forms
        forms.append(decoded)


def _contains_authorization_credential(value: str) -> bool:
    for candidate in _decoded_string_forms(value):
        for match in AUTH_VALUE_RE.finditer(candidate):
            if match.group(1).lower() == "bearer":
                return True
            token = match.group(2)
            padded = token + "=" * (-len(token) % 4)
            try:
                decoded = base64.b64decode(padded, validate=True)
            except (binascii.Error, ValueError):
                continue
            if b":" in decoded:
                return True
    return False


def _scan_safe(value: Any, path: str = "$") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if not isinstance(key, str):
                _fail(path, "field names must be strings")
            normalized = key.lower().replace("-", "_")
            if (normalized in FORBIDDEN_KEYS or normalized.endswith("_password") or
                    normalized.endswith("_secret") or normalized.endswith("_token")):
                _fail(f"{path}.{key}", "forbidden sensitive, identity, environment, path, or oracle field")
            _scan_safe(child, f"{path}.{key}")
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, child in enumerate(value):
            _scan_safe(child, f"{path}[{index}]")
    elif isinstance(value, str):
        if _contains_authorization_credential(value):
            _fail(path, "authorization credentials are forbidden")
        for candidate in _decoded_string_forms(value):
            if CREDENTIALIZED_URL_RE.search(candidate):
                _fail(path, "credentialized URLs are forbidden")
            if HOME_PATH_RE.search(candidate):
                _fail(path, "absolute user home paths are forbidden")
            if (SENSITIVE_ASSIGNMENT_RE.search(candidate) or PRIVATE_CONTENT_RE.search(candidate)
                    or USER_HOST_IDENTITY_RE.search(candidate)):
                _fail(path, "embedded sensitive material is forbidden")
    elif isinstance(value, float) and not math.isfinite(value):
        _fail(path, "must be finite")


def _nearest_rank(values: Sequence[float], percentile: float) -> float:
    if not values:
        raise ProtocolError("percentile requires at least one value")
    ordered = sorted(float(value) for value in values)
    rank = max(1, math.ceil(percentile * len(ordered)))
    return ordered[rank - 1]


def _close(left: float, right: float, *, absolute: float = 1e-9, relative: float = 1e-9) -> bool:
    return math.isclose(left, right, abs_tol=absolute, rel_tol=relative)


def _validate_interval(value: Any, path: str) -> tuple[float, float | None, str]:
    interval = _mapping(value, path)
    _fields(interval, {"lower_ms", "upper_ms", "censoring"}, path)
    lower = _number(interval["lower_ms"], f"{path}.lower_ms", minimum=0)
    upper = _optional_number(interval["upper_ms"], f"{path}.upper_ms")
    censoring = _string(interval["censoring"], f"{path}.censoring", choices={"none", "left", "interval", "right"})
    if censoring == "right":
        if upper is not None:
            _fail(path, "right-censored interval must have null upper_ms")
    else:
        if upper is None:
            _fail(path, "only a right-censored interval may have null upper_ms")
        if upper < lower:
            _fail(path, "interval bounds are inverted")
    if censoring == "left" and lower != 0:
        _fail(path, "left-censored interval must start at zero")
    if censoring == "none" and upper != lower:
        _fail(path, "uncensored interval must have equal bounds")
    return lower, upper, censoring


def _validate_run(value: Any, path: str = "$.run") -> Mapping[str, Any]:
    run = _mapping(value, path)
    _fields(run, {"run_id", "reset_id", "arm", "pair_id", "order", "profile", "authoritative"}, path)
    _string(run["run_id"], f"{path}.run_id")
    _string(run["reset_id"], f"{path}.reset_id")
    _string(run["arm"], f"{path}.arm", choices=ARMS)
    _integer(run["pair_id"], f"{path}.pair_id", 1)
    _string(run["order"], f"{path}.order", choices={"AB", "BA"})
    _string(run["profile"], f"{path}.profile", choices={"smoke", "full"})
    _boolean(run["authoritative"], f"{path}.authoritative")
    return run


def _validate_cache(value: Any, path: str = "$.cache") -> None:
    cache = _mapping(value, path)
    _fields(cache, {
        "label", "moraine_process", "mcp_result_cache", "mcp_posting_cache",
        "mcp_document_frequency_cache", "mcp_document_cache", "mcp_hydration_cache",
        "target_query_prewarmed", "clickhouse_cache", "os_page_cache",
        "generation_sha256", "fingerprint_sha256",
    }, path)
    _string(cache["label"], f"{path}.label", choices={"fresh_moraine_existing_clickhouse"})
    for name in ("moraine_process", "mcp_result_cache", "mcp_posting_cache", "mcp_document_frequency_cache", "mcp_document_cache", "mcp_hydration_cache"):
        _string(cache[name], f"{path}.{name}", choices={"fresh"})
    if _boolean(cache["target_query_prewarmed"], f"{path}.target_query_prewarmed"):
        _fail(f"{path}.target_query_prewarmed", "must be false")
    _string(cache["clickhouse_cache"], f"{path}.clickhouse_cache", choices={"seed_warmed"})
    _string(cache["os_page_cache"], f"{path}.os_page_cache", choices={"uncontrolled", "privileged_reset"})
    _sha(cache["generation_sha256"], f"{path}.generation_sha256")
    _sha(cache["fingerprint_sha256"], f"{path}.fingerprint_sha256")


def _validate_binary(value: Any, path: str = "$.binary") -> bool:
    binary = _mapping(value, path)
    _fields(binary, {"build_identity_sha256", "image_digest", "running_binaries"}, path)
    _sha(binary["build_identity_sha256"], f"{path}.build_identity_sha256")
    _sha(binary["image_digest"], f"{path}.image_digest")
    running = _sequence(binary["running_binaries"], f"{path}.running_binaries")
    if not running:
        _fail(f"{path}.running_binaries", "must not be empty")
    roles: set[str] = set()
    all_verified = True
    for index, raw in enumerate(running):
        item_path = f"{path}.running_binaries[{index}]"
        item = _mapping(raw, item_path)
        _fields(item, {"role", "sha256", "proc_exe_sha256", "verified"}, item_path)
        role = _string(item["role"], f"{item_path}.role")
        if role in roles:
            _fail(item_path, f"duplicate binary role {role!r}")
        roles.add(role)
        expected = _sha(item["sha256"], f"{item_path}.sha256")
        observed = _sha(item["proc_exe_sha256"], f"{item_path}.proc_exe_sha256")
        verified = _boolean(item["verified"], f"{item_path}.verified")
        if verified and expected != observed:
            _fail(item_path, "verified /proc executable checksum differs from expected binary")
        all_verified = all_verified and verified and expected == observed
    return all_verified


def _validate_resources(value: Any, path: str = "$.resources") -> bool:
    resources = _mapping(value, path)
    _fields(resources, _RESOURCE_FIELDS, path)
    authoritative = _boolean(resources["authoritative"], f"{path}.authoritative")
    _integer(resources["cgroup_version"], f"{path}.cgroup_version", 1)
    _string(resources["cgroup_driver"], f"{path}.cgroup_driver", choices={"cgroupfs", "systemd", "unavailable"})
    _sha(resources["cgroup_identity_sha256"], f"{path}.cgroup_identity_sha256")
    _sha(resources["role_membership_sha256"], f"{path}.role_membership_sha256")
    for name in ("controllers_enabled_proven", "effective_limits_proven", "host_headroom_proven", "server_descendants_proven", "loadgen_excluded_proven"):
        _boolean(resources[name], f"{path}.{name}")
    _string(resources["cpuset_cpus_effective"], f"{path}.cpuset_cpus_effective")
    for name in ("cpu_max_quota_us", "cpu_max_period_us", "memory_max_bytes"):
        _integer(resources[name], f"{path}.{name}", 1)
    for name in ("cpu_usage_usec_delta", "cpu_nr_throttled_delta", "throttled_usec_delta", "memory_current_bytes", "memory_peak_bytes", "memory_event_high_delta", "memory_event_max_delta", "swap_max_bytes", "swap_current_bytes", "oom_kill_delta"):
        _integer(resources[name], f"{path}.{name}")
    passes = (
        authoritative and resources["cgroup_version"] == 2 and resources["cgroup_driver"] in {"cgroupfs", "systemd"}
        and resources["cpu_max_quota_us"] == 100_000 and resources["cpu_max_period_us"] == 100_000
        and resources["memory_max_bytes"] == 8 * 1024**3 and resources["swap_max_bytes"] == 0
        and resources["memory_peak_bytes"] < resources["memory_max_bytes"]
        and resources["swap_current_bytes"] == 0 and resources["oom_kill_delta"] == 0
        and resources["memory_event_high_delta"] == 0 and resources["memory_event_max_delta"] == 0
        and all(resources[name] for name in ("controllers_enabled_proven", "effective_limits_proven", "host_headroom_proven", "server_descendants_proven", "loadgen_excluded_proven"))
    )
    return bool(passes)


def _validate_schedule(value: Any, path: str = "$.schedule") -> tuple[bool, list[tuple[str, str]]]:
    schedule = _mapping(value, path)
    _fields(schedule, {"schedule_sha256", "expanded_schedule", "expanded_schedule_sha256", "seed", "planned", "started", "completed", "unfinished", "dropped", "p99_start_slip_ms", "drained", "drain_ms", "streams_overlap", "physical_resets"}, path)
    _sha(schedule["schedule_sha256"], f"{path}.schedule_sha256")
    expanded = _mapping(schedule["expanded_schedule"], f"{path}.expanded_schedule")
    expanded_hash = _sha(
        schedule["expanded_schedule_sha256"],
        f"{path}.expanded_schedule_sha256",
    )
    if expanded_hash != sha256_json(expanded):
        _fail(
            f"{path}.expanded_schedule_sha256",
            "does not match expanded_schedule",
        )
    _integer(schedule["seed"], f"{path}.seed")
    for name in ("planned", "started", "completed", "unfinished", "dropped"):
        _integer(schedule[name], f"{path}.{name}")
    if schedule["planned"] != schedule["started"] + schedule["dropped"]:
        _fail(path, "planned must equal started + dropped")
    if schedule["started"] != schedule["completed"] + schedule["unfinished"]:
        _fail(path, "started must equal completed + unfinished")
    slip = _number(schedule["p99_start_slip_ms"], f"{path}.p99_start_slip_ms", minimum=0)
    drained = _boolean(schedule["drained"], f"{path}.drained")
    drain = _number(schedule["drain_ms"], f"{path}.drain_ms", minimum=0)
    overlap = schedule["streams_overlap"]
    if overlap is not None:
        _boolean(overlap, f"{path}.streams_overlap")
    reset_items = _sequence(schedule["physical_resets"], f"{path}.physical_resets")
    if not reset_items:
        _fail(f"{path}.physical_resets", "must record at least one physical reset")
    physical_resets: list[tuple[str, str]] = []
    seen_resets: set[str] = set()
    for index, raw in enumerate(reset_items):
        item_path = f"{path}.physical_resets[{index}]"
        item = _mapping(raw, item_path)
        _fields(item, {"role", "reset_sha256"}, item_path)
        role = _string(item["role"], f"{item_path}.role", choices={"trial", "scenario", "query_control", "ingest_control", "combined"})
        reset_sha256 = _sha(item["reset_sha256"], f"{item_path}.reset_sha256")
        if reset_sha256 in seen_resets:
            _fail(item_path, "physical reset identities must be unique")
        seen_resets.add(reset_sha256)
        physical_resets.append((role, reset_sha256))
    passes = schedule["dropped"] == 0 and schedule["unfinished"] == 0 and drained and slip <= POLICY["scheduler_p99_start_slip_ms_max"] and drain <= POLICY["drain_ms_max"]
    return bool(passes), physical_resets


def _validate_semantic(value: Any, path: str = "$.semantic") -> bool:
    semantic = _mapping(value, path)
    _fields(semantic, {"passed", "oracle_sha256", "expected_count", "observed_count", "missing_count", "duplicate_count", "stale_count", "malformed_count", "other_error_count"}, path)
    passed = _boolean(semantic["passed"], f"{path}.passed")
    _sha(semantic["oracle_sha256"], f"{path}.oracle_sha256")
    for name in ("expected_count", "observed_count", "missing_count", "duplicate_count", "stale_count", "malformed_count", "other_error_count"):
        _integer(semantic[name], f"{path}.{name}")
    computed = (
        semantic["expected_count"] == semantic["observed_count"]
        and all(semantic[name] == 0 for name in ("missing_count", "duplicate_count", "stale_count", "malformed_count", "other_error_count"))
    )
    if passed != computed:
        _fail(path, "passed contradicts count and identity invariants")
    return passed


def _validate_gates(value: Any, path: str = "$.gates") -> Mapping[str, bool]:
    gates = _mapping(value, path)
    _fields(gates, {"correctness", "resources", "schedule", "scenario"}, path)
    for name in gates:
        _boolean(gates[name], f"{path}.{name}")
    return gates  # type: ignore[return-value]


def _validate_outcomes(value: Any, path: str) -> Mapping[str, int]:
    outcomes = _mapping(value, path)
    _fields(outcomes, _OUTCOME_FIELDS, path)
    for name in outcomes:
        _integer(outcomes[name], f"{path}.{name}")
    terminal = sum(outcomes[name] for name in ("correct", "rejected", "timed_out", "semantic_error", "protocol_error", "malformed", "late", "other_error"))
    if outcomes["started"] != terminal:
        _fail(path, "started must equal terminal outcomes")
    if outcomes["planned"] != outcomes["started"] + outcomes["dropped"]:
        _fail(path, "planned must equal started + dropped")
    return outcomes  # type: ignore[return-value]


def _validate_qps_sample(value: Any, path: str, profile: str) -> tuple[int, int, bool, str]:
    sample = _mapping(value, path)
    _fields(sample, {"offered_qps", "replicate", "duration_s", "reset_sha256", "achieved_goodput_qps", "p95_ms", "p99_ms", "max_ms", "scheduler_p99_start_slip_ms", "drain_ms", "drained", "passed", "outcomes", "telemetry"}, path)
    offered = _integer(sample["offered_qps"], f"{path}.offered_qps", 1)
    replicate = _integer(sample["replicate"], f"{path}.replicate", 1)
    duration = _integer(sample["duration_s"], f"{path}.duration_s", 1)
    reset_sha256 = _sha(sample["reset_sha256"], f"{path}.reset_sha256")
    expected_duration = POLICY["qps_trial_seconds"] if profile == "full" else POLICY["smoke_qps_trial_seconds"]
    if duration != expected_duration:
        _fail(f"{path}.duration_s", f"must equal {expected_duration}")
    max_rate = POLICY["qps_max"] if profile == "full" else POLICY["smoke_qps_max"]
    if offered > max_rate:
        _fail(f"{path}.offered_qps", f"must not exceed {max_rate}")
    goodput = _number(sample["achieved_goodput_qps"], f"{path}.achieved_goodput_qps", minimum=0)
    p95 = _number(sample["p95_ms"], f"{path}.p95_ms", minimum=0)
    p99 = _number(sample["p99_ms"], f"{path}.p99_ms", minimum=0)
    maximum = _number(sample["max_ms"], f"{path}.max_ms", minimum=0)
    slip = _number(sample["scheduler_p99_start_slip_ms"], f"{path}.scheduler_p99_start_slip_ms", minimum=0)
    drain = _number(sample["drain_ms"], f"{path}.drain_ms", minimum=0)
    drained = _boolean(sample["drained"], f"{path}.drained")
    outcomes = _validate_outcomes(sample["outcomes"], f"{path}.outcomes")
    telemetry = _mapping(sample["telemetry"], f"{path}.telemetry")
    telemetry_fields = {
        "cpu_usage_usec_delta", "cpu_nr_throttled_delta", "throttled_usec_delta",
        "memory_current_bytes", "memory_peak_bytes", "memory_event_high_delta",
        "memory_event_max_delta", "swap_current_bytes", "oom_kill_delta",
    }
    _fields(telemetry, telemetry_fields, f"{path}.telemetry")
    for name in telemetry_fields:
        _integer(telemetry[name], f"{path}.telemetry.{name}")
    if outcomes["planned"] != offered * duration:
        _fail(f"{path}.outcomes.planned", "must equal offered_qps * duration_s")
    if not _close(goodput, outcomes["correct"] / duration):
        _fail(f"{path}.achieved_goodput_qps", "must equal correct outcomes / duration")
    computed = (
        outcomes["correct"] == outcomes["planned"]
        and all(outcomes[name] == 0 for name in _OUTCOME_FIELDS - {"planned", "started", "correct"})
        and p95 <= POLICY["qps_p95_ms_max"] and p99 <= POLICY["qps_p99_ms_max"]
        and maximum <= POLICY["qps_deadline_ms"] and slip <= POLICY["scheduler_p99_start_slip_ms_max"]
        and drained and drain <= POLICY["drain_ms_max"]
        and telemetry["memory_peak_bytes"] < 8 * 1024**3
        and telemetry["swap_current_bytes"] == 0 and telemetry["oom_kill_delta"] == 0
    )
    if _boolean(sample["passed"], f"{path}.passed") != computed:
        _fail(f"{path}.passed", "contradicts QPS correctness/SLO/drain policy")
    return offered, replicate, computed, reset_sha256


def _validate_full_qps_bracket(by_rate: Mapping[int, Sequence[bool]], path: str) -> tuple[int, str, int, int]:
    expected_rates: list[int] = []
    low = 0
    rate = POLICY["qps_min"]
    high: int | None = None
    while True:
        expected_rates.append(rate)
        passed = all(by_rate.get(rate, ())) and len(by_rate.get(rate, ())) == POLICY["qps_replicates"]
        if not passed:
            high = rate
            break
        low = rate
        if rate == POLICY["qps_max"]:
            break
        rate = min(rate * 2, POLICY["qps_max"])
    if low == 0:
        expected = {POLICY["qps_min"]}
        censoring = "left"
        lower, upper = 0, POLICY["qps_min"]
    elif low == POLICY["qps_max"]:
        expected = set(expected_rates)
        censoring = "right"
        lower = upper = POLICY["qps_max"]
    else:
        assert high is not None
        while high - low > 1:
            mid = (low + high) // 2
            expected_rates.append(mid)
            passed = all(by_rate.get(mid, ())) and len(by_rate.get(mid, ())) == POLICY["qps_replicates"]
            if passed:
                low = mid
            else:
                high = mid
        expected = set(expected_rates)
        censoring = "none"
        lower, upper = low, high
    if set(by_rate) != expected:
        _fail(path, f"attempted rates do not match deterministic bracket (expected={sorted(expected)}, actual={sorted(by_rate)})")
    return low, censoring, lower, upper


def _validate_qps(metrics_value: Any, samples_value: Any, run: Mapping[str, Any]) -> tuple[bool, set[str]]:
    path = "$.metrics"
    metrics = _mapping(metrics_value, path)
    _fields(metrics, {"direction", "sustainable_qps", "capacity_lower_qps", "capacity_upper_qps", "capacity_censoring"}, path)
    _string(metrics["direction"], "$.metrics.direction", choices={"higher"})
    sustainable = _integer(metrics["sustainable_qps"], "$.metrics.sustainable_qps")
    lower = _integer(metrics["capacity_lower_qps"], "$.metrics.capacity_lower_qps")
    upper = _integer(metrics["capacity_upper_qps"], "$.metrics.capacity_upper_qps")
    censoring = _string(metrics["capacity_censoring"], "$.metrics.capacity_censoring", choices={"none", "left", "right"})
    samples = _sequence(samples_value, "$.samples")
    if not samples:
        _fail("$.samples", "QPS requires attempted trials")
    by_rate: dict[int, list[bool]] = {}
    seen: set[tuple[int, int]] = set()
    resets: set[str] = set()
    for index, raw in enumerate(samples):
        offered, replicate, passed, reset_sha256 = _validate_qps_sample(raw, f"$.samples[{index}]", str(run["profile"]))
        key = (offered, replicate)
        if key in seen:
            _fail(f"$.samples[{index}]", "duplicate offered-rate replicate")
        seen.add(key)
        if reset_sha256 in resets:
            _fail(f"$.samples[{index}].reset_sha256", "each trial requires a unique physical reset")
        resets.add(reset_sha256)
        by_rate.setdefault(offered, []).append(passed)
    expected_replicates = POLICY["qps_replicates"] if run["profile"] == "full" else 1
    for rate, outcomes in by_rate.items():
        if len(outcomes) != expected_replicates or {rep for offered, rep in seen if offered == rate} != set(range(1, expected_replicates + 1)):
            _fail("$.samples", f"rate {rate} must have replicates 1..{expected_replicates}")
    if run["profile"] == "full":
        derived_sustainable, derived_censoring, derived_lower, derived_upper = _validate_full_qps_bracket(by_rate, "$.samples")
    else:
        if len(by_rate) != 1:
            _fail("$.samples", "smoke QPS requires exactly one three-second trial")
        rate = next(iter(by_rate))
        passed = all(by_rate[rate])
        derived_sustainable = rate if passed else 0
        derived_censoring = "right" if passed and rate == POLICY["smoke_qps_max"] else ("left" if not passed and rate == 1 else "none")
        derived_lower = derived_sustainable
        derived_upper = rate if passed else rate
    if (sustainable, censoring, lower, upper) != (derived_sustainable, derived_censoring, derived_lower, derived_upper):
        _fail("$.metrics", "capacity summary does not match attempted trials")
    return censoring == "none", resets


def _validate_ttr(metrics_value: Any, samples_value: Any) -> bool:
    metrics = _mapping(metrics_value, "$.metrics")
    _fields(metrics, {"direction", "p95_ms", "sample_count"}, "$.metrics")
    _string(metrics["direction"], "$.metrics.direction", choices={"lower"})
    p95 = _number(metrics["p95_ms"], "$.metrics.p95_ms", minimum=0)
    sample_count = _integer(metrics["sample_count"], "$.metrics.sample_count", 1)
    samples = _sequence(samples_value, "$.samples")
    if len(samples) != sample_count:
        _fail("$.metrics.sample_count", "must equal len(samples)")
    ids: set[str] = set()
    totals: list[float] = []
    all_valid = True
    phase_fields = {"spawn_exec", "central_readiness", "route_connect", "initialize_tools", "request_write", "response_read", "oracle_validation"}
    for index, raw in enumerate(samples):
        path = f"$.samples[{index}]"
        sample = _mapping(raw, path)
        _fields(sample, {"sample_id", "total_ms", "phases_ms", "pid_start_sha256", "cache_generation_sha256", "route_marker_sha256", "searches_before_write", "central_request_count", "embedded_fallback", "valid", "error_code"}, path)
        sample_id = _string(sample["sample_id"], f"{path}.sample_id")
        if sample_id in ids:
            _fail(f"{path}.sample_id", "duplicate sample ID")
        ids.add(sample_id)
        total = _number(sample["total_ms"], f"{path}.total_ms", minimum=0)
        phases = _mapping(sample["phases_ms"], f"{path}.phases_ms")
        _fields(phases, phase_fields, f"{path}.phases_ms")
        phase_total = sum(_number(phases[name], f"{path}.phases_ms.{name}", minimum=0) for name in phase_fields)
        if not _close(total, phase_total, absolute=max(0.5, total * 0.01), relative=0):
            _fail(path, "phase durations do not reconcile with total_ms")
        _sha(sample["pid_start_sha256"], f"{path}.pid_start_sha256")
        _sha(sample["cache_generation_sha256"], f"{path}.cache_generation_sha256")
        _sha(sample["route_marker_sha256"], f"{path}.route_marker_sha256")
        searches = _integer(sample["searches_before_write"], f"{path}.searches_before_write")
        central_requests = _integer(sample["central_request_count"], f"{path}.central_request_count")
        fallback = _boolean(sample["embedded_fallback"], f"{path}.embedded_fallback")
        error_code = sample["error_code"]
        if error_code is not None:
            _string(error_code, f"{path}.error_code")
        computed_valid = searches == 0 and central_requests == 1 and not fallback and error_code is None
        if _boolean(sample["valid"], f"{path}.valid") != computed_valid:
            _fail(f"{path}.valid", "contradicts route/freshness/error evidence")
        all_valid = all_valid and computed_valid
        totals.append(total)
    if not _close(p95, _nearest_rank(totals, 0.95)):
        _fail("$.metrics.p95_ms", "must equal nearest-rank sample p95")
    return all_valid


def _validate_etd_sample(value: Any, path: str) -> tuple[bool, tuple[float, float | None, str], tuple[float, float | None, str]]:
    sample = _mapping(value, path)
    _fields(sample, {"event_identity_sha256", "term_sha256", "batch_sequence", "publication_durable_ms", "db_ack_ms", "last_miss_ms", "first_hit_ms", "first_valid_ms", "source_interval", "db_ack_interval", "term_use_count", "cache_bypass", "valid", "error_code"}, path)
    _sha(sample["event_identity_sha256"], f"{path}.event_identity_sha256")
    term_sha256 = sample["term_sha256"]
    if term_sha256 is not None:
        _sha(term_sha256, f"{path}.term_sha256")
    batch_sequence = sample["batch_sequence"]
    if batch_sequence is not None:
        _integer(batch_sequence, f"{path}.batch_sequence")
    publication = _optional_number(sample["publication_durable_ms"], f"{path}.publication_durable_ms")
    ack = _optional_number(sample["db_ack_ms"], f"{path}.db_ack_ms")
    last_miss = _optional_number(sample["last_miss_ms"], f"{path}.last_miss_ms")
    first_hit = _optional_number(sample["first_hit_ms"], f"{path}.first_hit_ms")
    first_valid = _optional_number(sample["first_valid_ms"], f"{path}.first_valid_ms")
    source = _validate_interval(sample["source_interval"], f"{path}.source_interval")
    db_ack = _validate_interval(sample["db_ack_interval"], f"{path}.db_ack_interval")
    if first_hit is not None and first_valid is not None and first_valid < first_hit:
        _fail(f"{path}.first_valid_ms", "must not precede first hit")
    if last_miss is not None and first_hit is not None and first_hit < last_miss:
        _fail(f"{path}.first_hit_ms", "must not precede last miss")
    if first_valid is None:
        if source[2] != "right" or db_ack[2] != "right":
            _fail(path, "missing first-valid response must be right-censored")
    else:
        if publication is None or ack is None:
            _fail(path, "visible event requires durable-publication and DB-ack timestamps")
        expected_source_lower = last_miss if last_miss is not None else 0.0
        expected_source_censor = "interval" if last_miss is not None else "left"
        if not (_close(source[0], expected_source_lower) and source[1] is not None and _close(source[1], first_valid) and source[2] == expected_source_censor):
            _fail(f"{path}.source_interval", "does not match polling timestamps")
        expected_db_lower = max(0.0, (last_miss if last_miss is not None else ack) - ack)
        expected_db_upper = max(0.0, first_valid - ack)
        expected_db_censor = "interval" if last_miss is not None and last_miss > ack else "left"
        if not (_close(db_ack[0], expected_db_lower) and db_ack[1] is not None and _close(db_ack[1], expected_db_upper) and db_ack[2] == expected_db_censor):
            _fail(f"{path}.db_ack_interval", "does not match ack/poll timestamps")
    term_use_count = _integer(sample["term_use_count"], f"{path}.term_use_count")
    bypass = _mapping(sample["cache_bypass"], f"{path}.cache_bypass")
    _fields(bypass, CACHE_BYPASS_KEYS, f"{path}.cache_bypass")
    all_bypassed = all(_boolean(bypass[name], f"{path}.cache_bypass.{name}") for name in CACHE_BYPASS_KEYS)
    error_code = sample["error_code"]
    if error_code is not None:
        _string(error_code, f"{path}.error_code")
    computed_valid = (
        batch_sequence is not None and term_sha256 is not None and term_use_count >= 1
        and publication is not None and ack is not None
        and first_valid is not None and first_hit is not None
        and all_bypassed and error_code is None
    )
    if _boolean(sample["valid"], f"{path}.valid") != computed_valid:
        _fail(f"{path}.valid", "contradicts visibility/cache/error evidence")
    return computed_valid, source, db_ack


def _validate_etd(metrics_value: Any, samples_value: Any, *, loaded: bool) -> bool:
    metrics = _mapping(metrics_value, "$.metrics")
    _fields(metrics, {"direction", "event_count", "source_etd_p95", "db_ack_etd_p95", "loaded_query", "operational"}, "$.metrics")
    _string(metrics["direction"], "$.metrics.direction", choices={"lower"})
    event_count = _integer(metrics["event_count"], "$.metrics.event_count", 1)
    source_metric = _validate_interval(metrics["source_etd_p95"], "$.metrics.source_etd_p95")
    db_metric = _validate_interval(metrics["db_ack_etd_p95"], "$.metrics.db_ack_etd_p95")
    operational = _mapping(metrics["operational"], "$.metrics.operational")
    _fields(
        operational,
        {
            "planned",
            "started",
            "completed",
            "scheduler_p99_slip_ms",
            "first_started_ns",
            "last_completed_ns",
        },
        "$.metrics.operational",
    )
    operational_planned = _integer(
        operational["planned"], "$.metrics.operational.planned"
    )
    operational_started = _integer(
        operational["started"], "$.metrics.operational.started"
    )
    operational_completed = _integer(
        operational["completed"], "$.metrics.operational.completed"
    )
    operational_slip = _number(
        operational["scheduler_p99_slip_ms"],
        "$.metrics.operational.scheduler_p99_slip_ms",
        minimum=0,
    )
    first_started = _integer(
        operational["first_started_ns"], "$.metrics.operational.first_started_ns"
    )
    last_completed = _integer(
        operational["last_completed_ns"], "$.metrics.operational.last_completed_ns"
    )
    operational_valid = (
        operational_planned == event_count
        and operational_started == event_count
        and operational_completed == event_count
        and operational_slip <= POLICY["scheduler_p99_start_slip_ms_max"]
        and first_started <= last_completed
    )
    loaded_query_valid = True
    loaded_query = metrics["loaded_query"]
    if not loaded:
        if loaded_query is not None:
            _fail("$.metrics.loaded_query", "idle ETD cannot include query-load evidence")
    else:
        load = _mapping(loaded_query, "$.metrics.loaded_query")
        _fields(load, {
            "offered_qps", "planned", "started", "completed",
            "scheduler_p99_slip_ms", "schedule_delivered", "drained",
            "backlog", "first_start_slip_ms", "coverage_ns",
            "failure_count", "semantic_failures",
        }, "$.metrics.loaded_query")
        _number(load["offered_qps"], "$.metrics.loaded_query.offered_qps", positive=True)
        planned = _integer(load["planned"], "$.metrics.loaded_query.planned")
        started = _integer(load["started"], "$.metrics.loaded_query.started")
        completed = _integer(load["completed"], "$.metrics.loaded_query.completed")
        slip = _number(load["scheduler_p99_slip_ms"], "$.metrics.loaded_query.scheduler_p99_slip_ms", minimum=0)
        delivered = _boolean(load["schedule_delivered"], "$.metrics.loaded_query.schedule_delivered")
        drained = _boolean(load["drained"], "$.metrics.loaded_query.drained")
        backlog = _integer(load["backlog"], "$.metrics.loaded_query.backlog")
        first_slip = _number(load["first_start_slip_ms"], "$.metrics.loaded_query.first_start_slip_ms", minimum=0)
        coverage = _integer(load["coverage_ns"], "$.metrics.loaded_query.coverage_ns")
        failures = _integer(load["failure_count"], "$.metrics.loaded_query.failure_count")
        semantic_failures = _integer(load["semantic_failures"], "$.metrics.loaded_query.semantic_failures")
        loaded_query_valid = (
            planned > 0
            and started == planned
            and completed == planned
            and delivered
            and drained
            and backlog == 0
            and slip <= POLICY["scheduler_p99_start_slip_ms_max"]
            and first_slip <= POLICY["scheduler_p99_start_slip_ms_max"]
            and coverage > 0
            and failures == 0
            and semantic_failures == 0
        )
    samples = _sequence(samples_value, "$.samples")
    if len(samples) != event_count:
        _fail("$.metrics.event_count", "must equal len(samples)")
    events: set[str] = set()
    terms: set[str] = set()
    sources: list[tuple[float, float | None, str]] = []
    dbs: list[tuple[float, float | None, str]] = []
    all_valid = True
    for index, raw in enumerate(samples):
        valid, source, db_ack = _validate_etd_sample(raw, f"$.samples[{index}]")
        event = raw["event_identity_sha256"]
        term = raw["term_sha256"]
        if event in events or (term is not None and term in terms):
            _fail(f"$.samples[{index}]", "duplicate event identity or terminal one-use term")
        events.add(event)
        if term is not None:
            terms.add(term)
        sources.append(source)
        dbs.append(db_ack)
        all_valid = all_valid and valid
    def check_aggregate(observed: tuple[float, float | None, str], values: Sequence[tuple[float, float | None, str]], path: str) -> None:
        lower = _nearest_rank([value[0] for value in values], 0.95)
        uppers = [value[1] for value in values]
        if any(value is None for value in uppers):
            if observed[2] != "right" or observed[1] is not None:
                _fail(path, "right-censored sample requires right-censored p95")
            return
        upper = _nearest_rank([float(value) for value in uppers], 0.95)
        censoring = "left" if all(value[2] == "left" for value in values) else ("none" if _close(lower, upper) else "interval")
        if not (_close(observed[0], lower) and observed[1] is not None and _close(observed[1], upper) and observed[2] == censoring):
            _fail(path, "does not equal nearest-rank sample interval p95")
    check_aggregate(source_metric, sources, "$.metrics.source_etd_p95")
    check_aggregate(db_metric, dbs, "$.metrics.db_ack_etd_p95")
    return all_valid and loaded_query_valid and operational_valid


def _validate_mixed(metrics_value: Any, samples_value: Any) -> bool:
    metrics = _mapping(metrics_value, "$.metrics")
    _fields(metrics, {"direction", "query_control", "ingest_control", "combined_query", "combined_ingest", "control_evidence", "ratios", "mixed_gates", "lost_events", "duplicate_events"}, "$.metrics")
    _string(metrics["direction"], "$.metrics.direction", choices={"gates"})
    def query_metrics(value: Any, path: str) -> tuple[float, float, float]:
        item = _mapping(value, path)
        _fields(item, {"goodput_qps", "p95_ms", "p99_ms"}, path)
        return (_number(item["goodput_qps"], f"{path}.goodput_qps", positive=True), _number(item["p95_ms"], f"{path}.p95_ms", minimum=0), _number(item["p99_ms"], f"{path}.p99_ms", minimum=0))
    qc = query_metrics(metrics["query_control"], "$.metrics.query_control")
    cq = query_metrics(metrics["combined_query"], "$.metrics.combined_query")
    def ingest_metrics(value: Any, path: str) -> tuple[tuple[float, float | None, str], tuple[float, float | None, str]]:
        item = _mapping(value, path)
        _fields(item, {"source_etd_p95", "db_ack_etd_p95"}, path)
        return _validate_interval(item["source_etd_p95"], f"{path}.source_etd_p95"), _validate_interval(item["db_ack_etd_p95"], f"{path}.db_ack_etd_p95")
    ic = ingest_metrics(metrics["ingest_control"], "$.metrics.ingest_control")
    ci = ingest_metrics(metrics["combined_ingest"], "$.metrics.combined_ingest")
    controls = _mapping(metrics["control_evidence"], "$.metrics.control_evidence")
    _fields(controls, {
        "query_schedule_delivered", "ingest_schedule_delivered",
        "query_queue_depth", "ingest_queue_depth", "ingest_status_pass",
        "ingest_lost_events", "ingest_duplicate_events",
    }, "$.metrics.control_evidence")
    query_delivery = _boolean(
        controls["query_schedule_delivered"],
        "$.metrics.control_evidence.query_schedule_delivered",
    )
    ingest_delivery = _boolean(
        controls["ingest_schedule_delivered"],
        "$.metrics.control_evidence.ingest_schedule_delivered",
    )
    query_depth = _integer(
        controls["query_queue_depth"],
        "$.metrics.control_evidence.query_queue_depth",
    )
    ingest_depth = _integer(
        controls["ingest_queue_depth"],
        "$.metrics.control_evidence.ingest_queue_depth",
    )
    ingest_status = _boolean(
        controls["ingest_status_pass"],
        "$.metrics.control_evidence.ingest_status_pass",
    )
    ingest_lost = _integer(
        controls["ingest_lost_events"],
        "$.metrics.control_evidence.ingest_lost_events",
    )
    ingest_duplicates = _integer(
        controls["ingest_duplicate_events"],
        "$.metrics.control_evidence.ingest_duplicate_events",
    )
    ratios = _mapping(metrics["ratios"], "$.metrics.ratios")
    _fields(ratios, {"query_goodput", "query_p95", "query_p99", "source_etd", "db_ack_etd"}, "$.metrics.ratios")
    expected_ratios = {
        "query_goodput": cq[0] / qc[0],
        "query_p95": cq[1] / qc[1] if qc[1] else (1.0 if cq[1] == 0 else None),
        "query_p99": cq[2] / qc[2] if qc[2] else (1.0 if cq[2] == 0 else None),
        "source_etd": ci[0][1] / ic[0][1] if ic[0][1] not in (None, 0) and ci[0][1] is not None else None,
        "db_ack_etd": ci[1][1] / ic[1][1] if ic[1][1] not in (None, 0) and ci[1][1] is not None else None,
    }
    for name, expected in expected_ratios.items():
        observed = _number(ratios[name], f"$.metrics.ratios.{name}", minimum=0)
        if expected is None or not _close(observed, expected):
            _fail(f"$.metrics.ratios.{name}", "does not match control/combined metrics")
    mixed_gates = _mapping(metrics["mixed_gates"], "$.metrics.mixed_gates")
    _fields(mixed_gates, {"query_slo", "query_degradation", "ingest_slo", "ingest_degradation", "controls_valid", "schedules_delivered", "overlap", "drained", "exact_events"}, "$.metrics.mixed_gates")
    for name in mixed_gates:
        _boolean(mixed_gates[name], f"$.metrics.mixed_gates.{name}")
    computed_gates = {
        "query_slo": cq[1] <= POLICY["qps_p95_ms_max"] and cq[2] <= POLICY["qps_p99_ms_max"],
        "query_degradation": ratios["query_goodput"] >= POLICY["mixed_query_goodput_ratio_min"] and ratios["query_p95"] <= POLICY["mixed_query_p95_degradation_max"] and ratios["query_p99"] <= POLICY["mixed_query_p99_degradation_max"],
        "ingest_slo": ci[0][1] is not None and ci[1][1] is not None,
        "ingest_degradation": ratios["source_etd"] <= POLICY["mixed_source_etd_degradation_max"] and ratios["db_ack_etd"] <= POLICY["mixed_db_ack_etd_degradation_max"],
        "controls_valid": (
            query_delivery
            and ingest_delivery
            and query_depth == 0
            and ingest_depth == 0
            and ingest_status
            and ingest_lost == 0
            and ingest_duplicates == 0
            and qc[1] <= POLICY["qps_p95_ms_max"]
            and qc[2] <= POLICY["qps_p99_ms_max"]
        ),
    }
    for name, expected in computed_gates.items():
        if mixed_gates[name] != expected:
            _fail(f"$.metrics.mixed_gates.{name}", "contradicts mixed policy")
    lost = _integer(metrics["lost_events"], "$.metrics.lost_events")
    duplicates = _integer(metrics["duplicate_events"], "$.metrics.duplicate_events")
    samples = _mapping(samples_value, "$.samples")
    _fields(samples, {"query_records", "ingest"}, "$.samples")
    _sequence(samples["query_records"], "$.samples.query_records")
    ingest_samples = _sequence(samples["ingest"], "$.samples.ingest")
    for index, raw in enumerate(ingest_samples):
        _validate_etd_sample(raw, f"$.samples.ingest[{index}]")
    return bool(all(mixed_gates.values()) and lost == 0 and duplicates == 0)


def validate_scenario(document: Any) -> Mapping[str, Any]:
    value = _mapping(document, "$")
    _fields(value, _SCENARIO_FIELDS, "$")
    _string(value["document_type"], "$.document_type", choices={"scenario_result"})
    _string(value["schema_version"], "$.schema_version", choices={PERFORMANCE_VERSION})
    scenario = _string(value["scenario"], "$.scenario", choices=SCENARIOS)
    split = _string(value["split"], "$.split", choices=SPLITS)
    if split not in COMPARISON_MATRIX[scenario]:
        _fail("$.split", f"is not valid for scenario {scenario}")
    _sha(value["suite_definition_sha256"], "$.suite_definition_sha256")
    _sha(value["artifact_sha256"], "$.artifact_sha256")
    run = _validate_run(value["run"])
    status = _string(value["status"], "$.status", choices=STATUSES)
    _validate_cache(value["cache"])
    binary_pass = _validate_binary(value["binary"])
    resources_pass = _validate_resources(value["resources"]) and binary_pass
    schedule_pass, physical_resets = _validate_schedule(value["schedule"])
    semantic_pass = _validate_semantic(value["semantic"])
    gates = _validate_gates(value["gates"])
    reset_roles = [role for role, _ in physical_resets]
    reset_hashes = {reset_sha256 for _, reset_sha256 in physical_resets}
    if scenario == "qps":
        scenario_conclusive, sample_resets = _validate_qps(value["metrics"], value["samples"], run)
        scenario_pass = sample_resets == reset_hashes and all(role == "trial" for role in reset_roles)
        if not scenario_pass:
            _fail("$.schedule.physical_resets", "must exactly match QPS trial reset identities")
    elif scenario == "ttr":
        scenario_pass = _validate_ttr(value["metrics"], value["samples"])
        scenario_conclusive = True
        if reset_roles != ["scenario"]:
            _fail("$.schedule.physical_resets", "TTR requires exactly one scenario reset")
    elif scenario in {"etd_idle", "etd_loaded"}:
        scenario_pass = _validate_etd(
            value["metrics"],
            value["samples"],
            loaded=scenario == "etd_loaded",
        )
        scenario_conclusive = True
        if reset_roles != ["scenario"]:
            _fail("$.schedule.physical_resets", "ETD requires exactly one scenario reset")
    else:
        scenario_pass = _validate_mixed(value["metrics"], value["samples"])
        scenario_conclusive = True
        if set(reset_roles) != {"query_control", "ingest_control", "combined"} or len(reset_roles) != 3:
            _fail("$.schedule.physical_resets", "mixed requires query-control, ingest-control, and combined resets")
        if value["schedule"]["streams_overlap"] is not True:
            scenario_pass = False
    expected_gates = {"correctness": semantic_pass, "resources": resources_pass, "schedule": schedule_pass, "scenario": scenario_pass}
    if dict(gates) != expected_gates:
        _fail("$.gates", f"must equal computed gates {expected_gates}")
    failed = not semantic_pass or not schedule_pass or not scenario_pass or (run["authoritative"] and not resources_pass)
    inconclusive = not run["authoritative"] or run["profile"] == "smoke" or not scenario_conclusive
    expected_status = "fail" if failed else ("inconclusive" if inconclusive else "pass")
    if status != expected_status:
        _fail("$.status", f"must be {expected_status!r} from evidence and policy")
    expected_hash = sha256_json({key: child for key, child in value.items() if key != "artifact_sha256"})
    if value["artifact_sha256"] != expected_hash:
        _fail("$.artifact_sha256", "does not match canonical document content")
    return value


def _validate_build_recipe(value: Any, path: str) -> Mapping[str, Any]:
    recipe = _mapping(value, path)
    _fields(recipe, {"toolchain_sha256", "command", "default_features", "locked", "target", "linker_sha256", "environment_allowlist", "build_environment_sha256", "image_recipe_sha256", "recipe_sha256"}, path)
    _sha(recipe["toolchain_sha256"], f"{path}.toolchain_sha256")
    command = list(_sequence(recipe["command"], f"{path}.command"))
    if command != ["cargo", "build", "--workspace", "--release", "--locked"]:
        _fail(f"{path}.command", "must equal frozen release build command")
    if _boolean(recipe["default_features"], f"{path}.default_features") is not True or _boolean(recipe["locked"], f"{path}.locked") is not True:
        _fail(path, "default features and --locked are mandatory")
    _string(recipe["target"], f"{path}.target")
    _sha(recipe["linker_sha256"], f"{path}.linker_sha256")
    allowlist = _sequence(recipe["environment_allowlist"], f"{path}.environment_allowlist")
    if any(not isinstance(name, str) or not name for name in allowlist) or len(set(allowlist)) != len(allowlist):
        _fail(f"{path}.environment_allowlist", "must contain unique non-empty variable names")
    _sha(recipe["build_environment_sha256"], f"{path}.build_environment_sha256")
    _sha(recipe["image_recipe_sha256"], f"{path}.image_recipe_sha256")
    expected = sha256_json({key: child for key, child in recipe.items() if key != "recipe_sha256"})
    if _sha(recipe["recipe_sha256"], f"{path}.recipe_sha256") != expected:
        _fail(f"{path}.recipe_sha256", "does not match build recipe")
    return recipe


def _validate_build(value: Any, path: str) -> Mapping[str, Any]:
    build = _mapping(value, path)
    _fields(build, {"arm", "git_commit", "dirty", "image_digest", "build_environment_sha256", "binaries", "identity_sha256"}, path)
    _string(build["arm"], f"{path}.arm", choices=ARMS)
    commit = _string(build["git_commit"], f"{path}.git_commit")
    if not GIT_COMMIT_RE.fullmatch(commit):
        _fail(f"{path}.git_commit", "must be 40 lowercase hexadecimal characters")
    if _boolean(build["dirty"], f"{path}.dirty"):
        _fail(f"{path}.dirty", "authoritative builds must be immutable")
    _sha(build["image_digest"], f"{path}.image_digest")
    _sha(build["build_environment_sha256"], f"{path}.build_environment_sha256")
    binaries = _sequence(build["binaries"], f"{path}.binaries")
    if not binaries:
        _fail(f"{path}.binaries", "must not be empty")
    roles: set[str] = set()
    for index, raw in enumerate(binaries):
        item_path = f"{path}.binaries[{index}]"
        item = _mapping(raw, item_path)
        _fields(item, {"role", "sha256"}, item_path)
        role = _string(item["role"], f"{item_path}.role")
        if role in roles:
            _fail(item_path, f"duplicate binary role {role!r}")
        roles.add(role)
        _sha(item["sha256"], f"{item_path}.sha256")
    expected = sha256_json({key: child for key, child in build.items() if key != "identity_sha256"})
    if _sha(build["identity_sha256"], f"{path}.identity_sha256") != expected:
        _fail(f"{path}.identity_sha256", "does not match build identity")
    return build


def validate_suite_definition(value: Any) -> Mapping[str, Any]:
    definition = _mapping(value, "$.suite_definition")
    _fields(definition, {"suite_id", "profile", "resource_envelope", "cache_policy", "fixture", "policy", "build_recipe", "builds", "schedules"}, "$.suite_definition")
    _string(definition["suite_id"], "$.suite_definition.suite_id", choices={"fixed-resource-search-v1"})
    profile = _string(definition["profile"], "$.suite_definition.profile", choices={"smoke", "full"})
    envelope = _mapping(definition["resource_envelope"], "$.suite_definition.resource_envelope")
    _fields(envelope, {"cpu_max_quota_us", "cpu_max_period_us", "memory_max_bytes", "swap_max_bytes", "aggregate", "loadgen_excluded"}, "$.suite_definition.resource_envelope")
    expected_envelope = {"cpu_max_quota_us": 100_000, "cpu_max_period_us": 100_000, "memory_max_bytes": 8 * 1024**3, "swap_max_bytes": 0, "aggregate": True, "loadgen_excluded": True}
    if dict(envelope) != expected_envelope:
        _fail("$.suite_definition.resource_envelope", "does not equal the frozen aggregate envelope")
    cache = _mapping(definition["cache_policy"], "$.suite_definition.cache_policy")
    expected_cache = {"label": "fresh_moraine_existing_clickhouse", "moraine_process": "fresh", "mcp_caches": "fresh", "target_query_prewarmed": False, "clickhouse_cache": "seed_warmed", "os_page_cache": "uncontrolled_or_privileged_reset"}
    if dict(cache) != expected_cache:
        _fail("$.suite_definition.cache_policy", "does not equal the frozen layered-cache policy")
    fixture = _mapping(definition["fixture"], "$.suite_definition.fixture")
    _fields(fixture, {"recipe_version", "profile", "seed", "fixture_sha256", "fingerprints", "split_matrix"}, "$.suite_definition.fixture")
    _string(fixture["recipe_version"], "$.suite_definition.fixture.recipe_version")
    if _string(fixture["profile"], "$.suite_definition.fixture.profile", choices={"smoke", "full"}) != profile:
        _fail("$.suite_definition.fixture.profile", "must equal suite profile")
    _integer(fixture["seed"], "$.suite_definition.fixture.seed")
    _sha(fixture["fixture_sha256"], "$.suite_definition.fixture.fixture_sha256")
    fingerprints = _mapping(fixture["fingerprints"], "$.suite_definition.fixture.fingerprints")
    fingerprint_names = {"corpus_sha256", "query_research_sha256", "query_holdout_sha256", "query_stress_sha256", "event_research_sha256", "event_holdout_sha256", "event_stress_sha256", "split_matrix_sha256", "schedule_templates_sha256"}
    _fields(fingerprints, fingerprint_names, "$.suite_definition.fixture.fingerprints")
    for name in fingerprints:
        _sha(fingerprints[name], f"$.suite_definition.fixture.fingerprints.{name}")
    split_matrix = _mapping(fixture["split_matrix"], "$.suite_definition.fixture.split_matrix")
    if dict(split_matrix) != {name: list(splits) for name, splits in COMPARISON_MATRIX.items()}:
        _fail("$.suite_definition.fixture.split_matrix", "does not equal frozen scenario/split matrix")
    if definition["policy"] != policy_document():
        _fail("$.suite_definition.policy", "does not equal frozen protocol policy")
    recipe = _validate_build_recipe(definition["build_recipe"], "$.suite_definition.build_recipe")
    builds = _sequence(definition["builds"], "$.suite_definition.builds")
    build_arms: set[str] = set()
    for index, raw in enumerate(builds):
        build = _validate_build(raw, f"$.suite_definition.builds[{index}]")
        arm = str(build["arm"])
        if arm in build_arms:
            _fail(f"$.suite_definition.builds[{index}]", f"duplicate arm {arm!r}")
        if build["build_environment_sha256"] != recipe["build_environment_sha256"]:
            _fail(f"$.suite_definition.builds[{index}].build_environment_sha256", "differs from build recipe")
        build_arms.add(arm)
    if not build_arms:
        _fail("$.suite_definition.builds", "must not be empty")
    schedules = _mapping(definition["schedules"], "$.suite_definition.schedules")
    expected_schedule_keys = {f"{scenario}:{split}" for scenario, splits in COMPARISON_MATRIX.items() for split in splits}
    _fields(schedules, expected_schedule_keys, "$.suite_definition.schedules")
    for name in schedules:
        _sha(schedules[name], f"$.suite_definition.schedules.{name}")
    return definition


def _artifact_matrix(purpose: str) -> Mapping[str, Sequence[str]]:
    return COMPARISON_MATRIX if purpose == "comparison" else REPEATABILITY_MATRIX


def _validate_manifest(document: Any) -> Mapping[str, Any]:
    value = _mapping(document, "$")
    _fields(value, {"document_type", "schema_version", "purpose", "arm", "suite_definition", "suite_definition_sha256", "artifacts", "manifest_sha256"}, "$")
    _string(value["document_type"], "$.document_type", choices={"suite_manifest"})
    _string(value["schema_version"], "$.schema_version", choices={SUITE_VERSION})
    purpose = _string(value["purpose"], "$.purpose", choices={"comparison", "repeatability", "smoke"})
    arm = _string(value["arm"], "$.arm", choices=ARMS)
    definition = validate_suite_definition(value["suite_definition"])
    if purpose in {"comparison", "repeatability"} and definition["profile"] != "full":
        _fail("$.suite_definition.profile", f"{purpose} requires full profile")
    if purpose == "smoke" and definition["profile"] != "smoke":
        _fail("$.suite_definition.profile", "smoke manifest requires smoke profile")
    definition_hash = _sha(value["suite_definition_sha256"], "$.suite_definition_sha256")
    if definition_hash != sha256_json(definition):
        _fail("$.suite_definition_sha256", "does not match suite_definition")
    build_arms = {build["arm"] for build in definition["builds"]}
    if arm not in build_arms:
        _fail("$.arm", "has no immutable build identity in suite_definition")
    if purpose == "repeatability" and arm != "baseline":
        _fail("$.arm", "repeatability manifests must be baseline")
    artifacts = _sequence(value["artifacts"], "$.artifacts")
    matrix = _artifact_matrix(purpose)
    pairs = range(1, POLICY["pair_count"] + 1) if purpose == "comparison" else range(1, 2)
    expected = {(pair, scenario, split) for pair in pairs for scenario, splits in matrix.items() for split in splits}
    seen: set[tuple[int, str, str]] = set()
    paths: set[str] = set()
    byte_hashes: set[str] = set()
    artifact_hashes: set[str] = set()
    run_by_pair: dict[int, tuple[str, str]] = {}
    for index, raw in enumerate(artifacts):
        path = f"$.artifacts[{index}]"
        item = _mapping(raw, path)
        _fields(item, {"scenario", "split", "run_id", "reset_id", "arm", "pair_id", "order", "artifact_sha256", "sha256", "relative_path"}, path)
        scenario = _string(item["scenario"], f"{path}.scenario", choices=SCENARIOS)
        split = _string(item["split"], f"{path}.split", choices=SPLITS)
        pair = _integer(item["pair_id"], f"{path}.pair_id", 1)
        key = (pair, scenario, split)
        if key in seen:
            _fail(path, f"duplicate artifact identity {key}")
        seen.add(key)
        run_id = _string(item["run_id"], f"{path}.run_id")
        reset_id = _string(item["reset_id"], f"{path}.reset_id")
        if pair in run_by_pair and run_by_pair[pair] != (run_id, reset_id):
            _fail(path, "all scenarios in a physical arm must share run_id/reset_id")
        run_by_pair[pair] = (run_id, reset_id)
        if _string(item["arm"], f"{path}.arm", choices=ARMS) != arm:
            _fail(f"{path}.arm", "differs from manifest arm")
        expected_order = PAIR_ORDER[pair - 1] if purpose == "comparison" and 1 <= pair <= len(PAIR_ORDER) else "AB"
        if _string(item["order"], f"{path}.order", choices={"AB", "BA"}) != expected_order:
            _fail(f"{path}.order", f"must be {expected_order!r}")
        artifact_hash = _sha(item["artifact_sha256"], f"{path}.artifact_sha256")
        byte_hash = _sha(item["sha256"], f"{path}.sha256")
        relative = _string(item["relative_path"], f"{path}.relative_path")
        relative_path = Path(relative)
        if relative_path.is_absolute() or ".." in relative_path.parts:
            _fail(f"{path}.relative_path", "must remain below the manifest directory")
        if relative in paths or byte_hash in byte_hashes or artifact_hash in artifact_hashes:
            _fail(path, "duplicate path or checksum")
        paths.add(relative)
        byte_hashes.add(byte_hash)
        artifact_hashes.add(artifact_hash)
    if seen != expected:
        _fail("$.artifacts", f"artifact set differs (missing={sorted(expected - seen)}, extra={sorted(seen - expected)})")
    if len(set(run_by_pair.values())) != len(run_by_pair):
        _fail("$.artifacts", "each physical arm must have a unique run_id/reset_id")
    expected_manifest_hash = sha256_json({key: child for key, child in value.items() if key != "manifest_sha256"})
    if _sha(value["manifest_sha256"], "$.manifest_sha256") != expected_manifest_hash:
        _fail("$.manifest_sha256", "does not match canonical manifest content")
    return value


def _validate_comparison_summary(
    metrics_value: Any,
    diagnostics: Sequence[str],
) -> str:
    metrics = _mapping(metrics_value, "$.metrics")
    _fields(
        metrics,
        {
            "comparison_split",
            "ratios",
            "source_etd_ratio_upper",
            "geometric_rsd",
            "direction_pairs",
            "suite_score_ratio",
            "suite_score_ci95",
            "pair_results",
        },
        "$.metrics",
    )
    _string(
        metrics["comparison_split"],
        "$.metrics.comparison_split",
        choices={POLICY["comparison_split"]},
    )
    names = {"qps", "ttr", "source_etd"}
    ratios = _mapping(metrics["ratios"], "$.metrics.ratios")
    rsd = _mapping(metrics["geometric_rsd"], "$.metrics.geometric_rsd")
    directions = _mapping(metrics["direction_pairs"], "$.metrics.direction_pairs")
    _fields(ratios, names, "$.metrics.ratios")
    _fields(rsd, names, "$.metrics.geometric_rsd")
    _fields(directions, names, "$.metrics.direction_pairs")
    pairs = _sequence(metrics["pair_results"], "$.metrics.pair_results")
    if len(pairs) != POLICY["pair_count"]:
        _fail("$.metrics.pair_results", "must contain exactly seven pairs")
    series: dict[str, list[float]] = {name: [] for name in names}
    upper_series: list[float] = []
    score_series: list[float] = []
    for index, raw in enumerate(pairs):
        path = f"$.metrics.pair_results[{index}]"
        pair = _mapping(raw, path)
        fields = {
            "pair_id",
            "qps_ratio",
            "ttr_ratio",
            "source_etd_ratio_lower",
            "source_etd_ratio_upper",
            "score_ratio_lower",
            "score_ratio_upper",
        }
        _fields(pair, fields, path)
        if _integer(pair["pair_id"], f"{path}.pair_id", 1) != index + 1:
            _fail(f"{path}.pair_id", "must be contiguous and ordered")
        qps = _number(pair["qps_ratio"], f"{path}.qps_ratio", minimum=0)
        ttr = _number(pair["ttr_ratio"], f"{path}.ttr_ratio", minimum=0)
        source = _number(
            pair["source_etd_ratio_lower"],
            f"{path}.source_etd_ratio_lower",
            minimum=0,
        )
        score = _number(pair["score_ratio_lower"], f"{path}.score_ratio_lower", minimum=0)
        expected_score = _geometric_mean_nonnegative([qps, ttr, source])
        if not _close(score, expected_score):
            _fail(f"{path}.score_ratio_lower", "does not match constituent ratios")
        upper_raw = pair["source_etd_ratio_upper"]
        score_upper_raw = pair["score_ratio_upper"]
        if upper_raw is None:
            if score_upper_raw is not None:
                _fail(f"{path}.score_ratio_upper", "must be null with an unbounded ETD ratio")
        else:
            upper = _number(upper_raw, f"{path}.source_etd_ratio_upper", minimum=0)
            score_upper = _number(score_upper_raw, f"{path}.score_ratio_upper", minimum=0)
            if not _close(score_upper, _geometric_mean_nonnegative([qps, ttr, upper])):
                _fail(f"{path}.score_ratio_upper", "does not match constituent ratios")
            upper_series.append(upper)
        series["qps"].append(qps)
        series["ttr"].append(ttr)
        series["source_etd"].append(source)
        score_series.append(score)
    expected_ratios = {
        name: _geometric_mean_nonnegative(values) for name, values in series.items()
    }
    expected_rsd = {name: geometric_rsd(values) for name, values in series.items()}
    expected_directions = {
        name: sum(value > 1.0 for value in values) for name, values in series.items()
    }
    for name in names:
        if not _close(
            _number(ratios[name], f"$.metrics.ratios.{name}", minimum=0),
            expected_ratios[name],
        ):
            _fail(f"$.metrics.ratios.{name}", "does not match pair results")
        observed_rsd = rsd[name]
        if expected_rsd[name] is None:
            if observed_rsd is not None:
                _fail(f"$.metrics.geometric_rsd.{name}", "must be null")
        elif not _close(
            _number(observed_rsd, f"$.metrics.geometric_rsd.{name}", minimum=0),
            expected_rsd[name],
        ):
            _fail(f"$.metrics.geometric_rsd.{name}", "does not match pair results")
        if _integer(
            directions[name],
            f"$.metrics.direction_pairs.{name}",
            0,
        ) != expected_directions[name]:
            _fail(f"$.metrics.direction_pairs.{name}", "does not match pair results")
    expected_upper = (
        geometric_mean(upper_series)
        if len(upper_series) == POLICY["pair_count"]
        else None
    )
    if expected_upper is None:
        if metrics["source_etd_ratio_upper"] is not None:
            _fail("$.metrics.source_etd_ratio_upper", "must be null")
    elif not _close(
        _number(
            metrics["source_etd_ratio_upper"],
            "$.metrics.source_etd_ratio_upper",
            minimum=0,
        ),
        expected_upper,
    ):
        _fail("$.metrics.source_etd_ratio_upper", "does not match pair results")
    expected_score = _geometric_mean_nonnegative(score_series)
    if not _close(
        _number(metrics["suite_score_ratio"], "$.metrics.suite_score_ratio", minimum=0),
        expected_score,
    ):
        _fail("$.metrics.suite_score_ratio", "does not match pair results")
    interval = _mapping(metrics["suite_score_ci95"], "$.metrics.suite_score_ci95")
    _fields(interval, {"lower", "upper"}, "$.metrics.suite_score_ci95")
    expected_lower, expected_ci_upper = bootstrap_interval(score_series)
    if not _close(
        _number(interval["lower"], "$.metrics.suite_score_ci95.lower", minimum=0),
        expected_lower,
    ) or not _close(
        _number(interval["upper"], "$.metrics.suite_score_ci95.upper", minimum=0),
        expected_ci_upper,
    ):
        _fail("$.metrics.suite_score_ci95", "does not match deterministic bootstrap")
    failed_arms = any(
        diagnostic.endswith(":arm-not-pass") or diagnostic.endswith(":gate-failed")
        for diagnostic in diagnostics
    )
    high_variability = any(
        expected_rsd[name] is None
        or expected_rsd[name] > POLICY[f"{'etd' if name == 'source_etd' else name}_max_geometric_rsd"]
        for name in names
    )
    inconclusive = high_variability or expected_lower <= 1.0 or any(
        diagnostic.endswith(
            (
                ":capacity-censored",
                ":zero-baseline",
                ":zero-candidate",
                ":censored",
                ":threshold-overlap",
            )
        )
        for diagnostic in diagnostics
    )
    threshold_failure = (
        expected_score < POLICY["minimum_score_ratio"]
        or any(
            value < POLICY["minimum_constituent_ratio"]
            for value in expected_ratios.values()
        )
        or any(
            value < POLICY["minimum_direction_pairs"]
            for value in expected_directions.values()
        )
    )
    if failed_arms:
        return "fail"
    if inconclusive:
        return "inconclusive"
    return "fail" if threshold_failure else "pass"


def _validate_repeatability_summary(
    metrics_value: Any,
    diagnostics: Sequence[str],
) -> str:
    metrics = _mapping(metrics_value, "$.metrics")
    _fields(metrics, {"values", "mad_ratios", "limits"}, "$.metrics")
    names = {"qps", "ttr", "source_etd", "db_ack_etd"}
    values = _mapping(metrics["values"], "$.metrics.values")
    ratios = _mapping(metrics["mad_ratios"], "$.metrics.mad_ratios")
    limits = _mapping(metrics["limits"], "$.metrics.limits")
    _fields(values, names, "$.metrics.values")
    _fields(ratios, names, "$.metrics.mad_ratios")
    _fields(limits, names, "$.metrics.limits")
    expected_limits = {
        "qps": POLICY["repeatability_qps_mad_ratio"],
        "ttr": POLICY["repeatability_ttr_mad_ratio"],
        "source_etd": POLICY["repeatability_etd_mad_ratio"],
        "db_ack_etd": POLICY["repeatability_etd_mad_ratio"],
    }
    expected_ratios: dict[str, float] = {}
    for name in names:
        series_raw = _sequence(values[name], f"$.metrics.values.{name}")
        if len(series_raw) != POLICY["repeatability_runs"]:
            _fail(f"$.metrics.values.{name}", "must contain exactly seven runs")
        series = [
            _number(item, f"$.metrics.values.{name}[{index}]", minimum=0)
            for index, item in enumerate(series_raw)
        ]
        if any(item <= 0 for item in series):
            _fail(f"$.metrics.values.{name}", "measurements must be positive")
        expected_ratios[name] = mad_ratio(series)
        if not _close(
            _number(ratios[name], f"$.metrics.mad_ratios.{name}", minimum=0),
            expected_ratios[name],
        ):
            _fail(f"$.metrics.mad_ratios.{name}", "does not match measured values")
        if not _close(
            _number(limits[name], f"$.metrics.limits.{name}", minimum=0),
            expected_limits[name],
        ):
            _fail(f"$.metrics.limits.{name}", "does not match frozen policy")
    failures = [
        f"repeatability:{name}:mad-ratio-exceeded"
        for name in ("qps", "ttr", "source_etd", "db_ack_etd")
        if expected_ratios[name] > expected_limits[name]
    ]
    if list(diagnostics) != failures:
        _fail("$.diagnostics", "does not match repeatability threshold failures")
    return "fail" if failures else "pass"


def _validate_summary(document: Any, document_type: str) -> Mapping[str, Any]:
    value = _mapping(document, "$")
    _fields(value, {"document_type", "schema_version", "status", "inputs", "policy", "metrics", "diagnostics", "document_sha256"}, "$")
    _string(value["document_type"], "$.document_type", choices={document_type})
    _string(value["schema_version"], "$.schema_version", choices={PERFORMANCE_VERSION})
    status = _string(value["status"], "$.status", choices=STATUSES)
    inputs = _sequence(value["inputs"], "$.inputs")
    if document_type == "comparison" and len(inputs) != 2:
        _fail("$.inputs", "comparison requires baseline and candidate manifests")
    if document_type == "repeatability" and len(inputs) != POLICY["repeatability_runs"]:
        _fail("$.inputs", "repeatability requires exactly seven manifests")
    input_arms: list[str] = []
    input_hashes: list[str] = []
    for index, raw in enumerate(inputs):
        item = _mapping(raw, f"$.inputs[{index}]")
        _fields(item, {"arm", "manifest_sha256"}, f"$.inputs[{index}]")
        input_arms.append(
            _string(item["arm"], f"$.inputs[{index}].arm", choices=ARMS)
        )
        input_hashes.append(
            _sha(item["manifest_sha256"], f"$.inputs[{index}].manifest_sha256")
        )
    if len(set(input_hashes)) != len(input_hashes):
        _fail("$.inputs", "manifest checksums must be unique")
    if document_type == "comparison" and input_arms != ["baseline", "candidate"]:
        _fail("$.inputs", "comparison inputs must be baseline then candidate")
    if document_type == "repeatability" and (
        set(input_arms) != {"baseline"} or input_hashes != sorted(input_hashes)
    ):
        _fail("$.inputs", "repeatability inputs must be sorted baseline manifests")
    if value["policy"] != policy_document():
        _fail("$.policy", "does not equal frozen protocol policy")
    diagnostics_raw = _sequence(value["diagnostics"], "$.diagnostics")
    diagnostics = [
        _string(diagnostic, f"$.diagnostics[{index}]")
        for index, diagnostic in enumerate(diagnostics_raw)
    ]
    expected_status = (
        _validate_comparison_summary(value["metrics"], diagnostics)
        if document_type == "comparison"
        else _validate_repeatability_summary(value["metrics"], diagnostics)
    )
    if status != expected_status:
        _fail("$.status", "contradicts validated summary metrics and diagnostics")
    expected_hash = sha256_json({key: child for key, child in value.items() if key != "document_sha256"})
    if _sha(value["document_sha256"], "$.document_sha256") != expected_hash:
        _fail("$.document_sha256", "does not match canonical summary content")
    return value


def validate_comparison(document: Any) -> Mapping[str, Any]:
    return _validate_summary(document, "comparison")


def validate_repeatability(document: Any) -> Mapping[str, Any]:
    return _validate_summary(document, "repeatability")


def validate_document(document: Any) -> Mapping[str, Any]:
    _scan_safe(document)
    value = _mapping(document, "$")
    document_type = _string(value.get("document_type"), "$.document_type")
    if document_type == "scenario_result":
        return validate_scenario(value)
    if document_type == "suite_manifest":
        return _validate_manifest(value)
    if document_type == "comparison":
        return validate_comparison(value)
    if document_type == "repeatability":
        return validate_repeatability(value)
    _fail("$.document_type", f"unsupported document type {document_type!r}")


def _finalize(document: dict[str, Any], hash_field: str) -> dict[str, Any]:
    document[hash_field] = sha256_json({key: child for key, child in document.items() if key != hash_field})
    validate_document(document)
    return document


def create_scenario_result(*, scenario: str, split: str, suite_definition_sha256: str,
                           run: Mapping[str, Any], status: str, cache: Mapping[str, Any],
                           binary: Mapping[str, Any], resources: Mapping[str, Any],
                           schedule: Mapping[str, Any], metrics: Mapping[str, Any], samples: Any,
                           semantic: Mapping[str, Any], gates: Mapping[str, Any]) -> dict[str, Any]:
    """Construct and hash one strictly validated scenario artifact."""
    document = {
        "document_type": "scenario_result", "schema_version": PERFORMANCE_VERSION,
        "scenario": scenario, "split": split, "suite_definition_sha256": suite_definition_sha256,
        "artifact_sha256": "sha256:" + "0" * 64, "run": dict(run), "status": status,
        "cache": dict(cache), "binary": dict(binary), "resources": dict(resources),
        "schedule": dict(schedule), "metrics": dict(metrics), "samples": samples,
        "semantic": dict(semantic), "gates": dict(gates),
    }
    return _finalize(document, "artifact_sha256")


def create_build_recipe(*, toolchain_sha256: str, target: str, linker_sha256: str,
                        environment_allowlist: Sequence[str], build_environment_sha256: str,
                        image_recipe_sha256: str) -> dict[str, Any]:
    recipe: dict[str, Any] = {
        "toolchain_sha256": toolchain_sha256,
        "command": ["cargo", "build", "--workspace", "--release", "--locked"],
        "default_features": True, "locked": True, "target": target, "linker_sha256": linker_sha256,
        "environment_allowlist": list(environment_allowlist),
        "build_environment_sha256": build_environment_sha256,
        "image_recipe_sha256": image_recipe_sha256, "recipe_sha256": "sha256:" + "0" * 64,
    }
    recipe["recipe_sha256"] = sha256_json({key: child for key, child in recipe.items() if key != "recipe_sha256"})
    _validate_build_recipe(recipe, "$.build_recipe")
    return recipe


def create_build_identity(*, arm: str, git_commit: str, image_digest: str,
                          build_environment_sha256: str,
                          binaries: Sequence[Mapping[str, str]]) -> dict[str, Any]:
    build: dict[str, Any] = {
        "arm": arm, "git_commit": git_commit, "dirty": False, "image_digest": image_digest,
        "build_environment_sha256": build_environment_sha256,
        "binaries": [dict(item) for item in binaries], "identity_sha256": "sha256:" + "0" * 64,
    }
    build["identity_sha256"] = sha256_json({key: child for key, child in build.items() if key != "identity_sha256"})
    _validate_build(build, "$.build")
    return build


def create_suite_definition(*, profile: str, fixture: Mapping[str, Any],
                            build_recipe: Mapping[str, Any], builds: Sequence[Mapping[str, Any]],
                            schedules: Mapping[str, str]) -> dict[str, Any]:
    seed = fixture.get("seed")
    if not isinstance(seed, Mapping) or set(seed) != {"algorithm", "value"}:
        raise ProtocolError("fixture seed must be the frozen algorithm/value identity")
    if seed["algorithm"] != "closed-form":
        raise ProtocolError("fixture seed algorithm is unsupported")
    fixture_identity = {
        "recipe_version": fixture["recipe_version"], "profile": fixture["profile"],
        "seed": seed["value"], "fixture_sha256": fixture["fixture_sha256"],
        "fingerprints": dict(fixture["fingerprints"]),
        "split_matrix": {name: list(splits) for name, splits in fixture["split_matrix"].items()},
    }
    definition = {
        "suite_id": "fixed-resource-search-v1", "profile": profile,
        "resource_envelope": {"cpu_max_quota_us": 100_000, "cpu_max_period_us": 100_000, "memory_max_bytes": 8 * 1024**3, "swap_max_bytes": 0, "aggregate": True, "loadgen_excluded": True},
        "cache_policy": {"label": "fresh_moraine_existing_clickhouse", "moraine_process": "fresh", "mcp_caches": "fresh", "target_query_prewarmed": False, "clickhouse_cache": "seed_warmed", "os_page_cache": "uncontrolled_or_privileged_reset"},
        "fixture": fixture_identity, "policy": policy_document(), "build_recipe": dict(build_recipe),
        "builds": [dict(build) for build in builds], "schedules": dict(schedules),
    }
    validate_suite_definition(definition)
    return definition


def _artifact_reference(path: Path, manifest_parent: Path) -> dict[str, Any]:
    document = load_document(path)
    if document["document_type"] != "scenario_result":
        raise ProtocolError(f"{path}: manifest artifacts must be scenario results")
    try:
        relative = path.resolve().relative_to(manifest_parent.resolve())
    except ValueError as error:
        raise ProtocolError(f"{path}: artifact must be below manifest directory") from error
    return {
        "scenario": document["scenario"], "split": document["split"],
        "run_id": document["run"]["run_id"], "reset_id": document["run"]["reset_id"],
        "arm": document["run"]["arm"], "pair_id": document["run"]["pair_id"],
        "order": document["run"]["order"], "artifact_sha256": document["artifact_sha256"],
        "sha256": sha256_bytes(path.read_bytes()), "relative_path": relative.as_posix(),
    }


def create_suite_manifest(*, purpose: str, arm: str, suite_definition: Mapping[str, Any],
                          artifact_paths: Sequence[Path], manifest_path: Path) -> dict[str, Any]:
    """Construct a complete manifest from scenario files below ``manifest_path``."""
    definition = dict(suite_definition)
    document: dict[str, Any] = {
        "document_type": "suite_manifest", "schema_version": SUITE_VERSION,
        "purpose": purpose, "arm": arm, "suite_definition": definition,
        "suite_definition_sha256": sha256_json(definition),
        "artifacts": [_artifact_reference(Path(path), manifest_path.parent) for path in artifact_paths],
        "manifest_sha256": "sha256:" + "0" * 64,
    }
    return _finalize(document, "manifest_sha256")


def _reject_json_constant(value: str) -> None:
    raise ProtocolError(f"invalid non-finite JSON number: {value}")


def load_document(path: Path | str) -> Mapping[str, Any]:
    artifact_path = Path(path)
    try:
        with artifact_path.open("r", encoding="utf-8") as handle:
            value = json.load(handle, parse_constant=_reject_json_constant)
    except ProtocolError:
        raise
    except (OSError, json.JSONDecodeError) as error:
        raise ProtocolError(f"{artifact_path}: unable to load JSON: {error}") from error
    return validate_document(value)


def load_suite_manifest(path: Path | str) -> tuple[Mapping[str, Any], dict[tuple[int, str, str], Mapping[str, Any]]]:
    manifest_path = Path(path)
    manifest = load_document(manifest_path)
    if manifest["document_type"] != "suite_manifest":
        raise ProtocolError(f"{manifest_path}: expected suite_manifest")
    loaded: dict[tuple[int, str, str], Mapping[str, Any]] = {}
    build = next(item for item in manifest["suite_definition"]["builds"] if item["arm"] == manifest["arm"])
    expected_binaries = {item["role"]: item["sha256"] for item in build["binaries"]}
    base = manifest_path.parent.resolve()
    physical_resets: set[str] = set()
    for index, reference in enumerate(manifest["artifacts"]):
        artifact_path = (base / reference["relative_path"]).resolve()
        try:
            artifact_path.relative_to(base)
        except ValueError as error:
            raise ProtocolError(f"artifact reference escapes manifest directory: {reference['relative_path']}") from error
        try:
            raw = artifact_path.read_bytes()
        except OSError as error:
            raise ProtocolError(f"missing artifact {reference['relative_path']}: {error}") from error
        if sha256_bytes(raw) != reference["sha256"]:
            raise ProtocolError(f"artifact checksum mismatch: {reference['relative_path']}")
        try:
            artifact = json.loads(raw, parse_constant=_reject_json_constant)
        except (json.JSONDecodeError, ProtocolError) as error:
            raise ProtocolError(f"invalid artifact {reference['relative_path']}: {error}") from error
        artifact = validate_scenario(artifact)
        identity = {
            "scenario": artifact["scenario"], "split": artifact["split"],
            "run_id": artifact["run"]["run_id"], "reset_id": artifact["run"]["reset_id"],
            "arm": artifact["run"]["arm"], "pair_id": artifact["run"]["pair_id"],
            "order": artifact["run"]["order"], "artifact_sha256": artifact["artifact_sha256"],
        }
        for name, observed in identity.items():
            if reference[name] != observed:
                raise ProtocolError(f"artifact reference {index} {name} does not match artifact")
        if artifact["suite_definition_sha256"] != manifest["suite_definition_sha256"]:
            raise ProtocolError(f"artifact {reference['relative_path']} suite definition mismatch")
        if artifact["run"]["profile"] != manifest["suite_definition"]["profile"]:
            raise ProtocolError(f"artifact {reference['relative_path']} profile mismatch")
        if artifact["binary"]["build_identity_sha256"] != build["identity_sha256"] or artifact["binary"]["image_digest"] != build["image_digest"]:
            raise ProtocolError(f"artifact {reference['relative_path']} build identity mismatch")
        running = {
            item["role"]: item
            for item in artifact["binary"]["running_binaries"]
        }
        if set(running) != MEASURED_BINARY_ROLES:
            raise ProtocolError(
                f"artifact {reference['relative_path']} measured binary roles differ"
            )
        if any(
            role not in expected_binaries
            or not item["verified"]
            or item["sha256"] != expected_binaries[role]
            or item["proc_exe_sha256"] != expected_binaries[role]
            for role, item in running.items()
        ):
            raise ProtocolError(
                f"artifact {reference['relative_path']} running binary evidence mismatch"
            )
        schedule_key = f"{artifact['scenario']}:{artifact['split']}"
        if artifact["schedule"]["schedule_sha256"] != manifest["suite_definition"]["schedules"][schedule_key]:
            raise ProtocolError(f"artifact {reference['relative_path']} schedule fingerprint mismatch")
        artifact_resets = {
            item["reset_sha256"] for item in artifact["schedule"]["physical_resets"]
        }
        if physical_resets & artifact_resets:
            raise ProtocolError("physical reset identities must be unique across scenario artifacts")
        physical_resets.update(artifact_resets)
        key = (artifact["run"]["pair_id"], artifact["scenario"], artifact["split"])
        if key in loaded:
            raise ProtocolError(f"duplicate loaded artifact {key}")
        loaded[key] = artifact
    return manifest, loaded


def write_json_atomic(
    path: Path | str,
    document: Mapping[str, Any],
    *,
    validator: Callable[[Any], Any] = validate_document,
) -> None:
    """Validate, fsync, atomically replace, then fsync the destination directory."""
    validator(document)
    destination = Path(path)
    encoded = canonical_json_bytes(document)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor = -1
    temporary: Path | None = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(dir=destination.parent, prefix=f".{destination.name}.", suffix=".tmp")
        temporary = Path(temporary_name)
        handle = os.fdopen(descriptor, "wb")
        descriptor = -1
        with handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        temporary = None
        directory_descriptor = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary is not None:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


def geometric_mean(values: Sequence[float]) -> float:
    if not values or any(value <= 0 or not math.isfinite(value) for value in values):
        raise ProtocolError("geometric mean requires finite positive values")
    return math.exp(sum(math.log(value) for value in values) / len(values))


def _geometric_mean_nonnegative(values: Sequence[float]) -> float:
    if not values or any(value < 0 or not math.isfinite(value) for value in values):
        raise ProtocolError("geometric mean requires finite non-negative values")
    return 0.0 if any(value == 0 for value in values) else geometric_mean(values)


def geometric_rsd(values: Sequence[float]) -> float | None:
    if any(value <= 0 for value in values):
        return None
    if len(values) < 2:
        return 0.0
    return math.exp(statistics.stdev(math.log(value) for value in values)) - 1.0


def mad_ratio(values: Sequence[float]) -> float:
    if not values:
        raise ProtocolError("MAD ratio requires values")
    median = statistics.median(values)
    if median <= 0:
        raise ProtocolError("MAD ratio requires a positive median")
    return statistics.median(abs(value - median) for value in values) / median


def bootstrap_interval(values: Sequence[float]) -> tuple[float, float]:
    if len(values) != POLICY["pair_count"]:
        raise ProtocolError("paired bootstrap requires exactly seven values")
    rng = random.Random(POLICY["bootstrap_seed"])
    estimates = []
    for _ in range(POLICY["bootstrap_samples"]):
        sample = [values[rng.randrange(len(values))] for _ in values]
        estimates.append(_geometric_mean_nonnegative(sample))
    estimates.sort()
    lower_index = math.floor(0.025 * (len(estimates) - 1))
    upper_index = math.ceil(0.975 * (len(estimates) - 1))
    return estimates[lower_index], estimates[upper_index]


def _summary(document_type: str, status: str, inputs: Sequence[Mapping[str, str]], metrics: Mapping[str, Any], diagnostics: Sequence[str]) -> dict[str, Any]:
    document: dict[str, Any] = {
        "document_type": document_type, "schema_version": PERFORMANCE_VERSION,
        "status": status, "inputs": [dict(item) for item in inputs],
        "policy": policy_document(), "metrics": dict(metrics),
        "diagnostics": list(diagnostics), "document_sha256": "sha256:" + "0" * 64,
    }
    return _finalize(document, "document_sha256")


def compare_manifests(first_path: Path | str, second_path: Path | str) -> dict[str, Any]:
    """Compare complete arm manifests; argument order does not assign A/B labels."""
    loaded = [load_suite_manifest(first_path), load_suite_manifest(second_path)]
    by_arm = {manifest[0]["arm"]: manifest for manifest in loaded}
    if set(by_arm) != ARMS:
        raise ProtocolError("comparison requires exactly one baseline and one candidate manifest")
    baseline_manifest, baseline = by_arm["baseline"]
    candidate_manifest, candidate = by_arm["candidate"]
    if baseline_manifest["purpose"] != "comparison" or candidate_manifest["purpose"] != "comparison":
        raise ProtocolError("compare consumes purpose=comparison manifests")
    if baseline_manifest["suite_definition_sha256"] != candidate_manifest["suite_definition_sha256"]:
        raise ProtocolError("comparison manifests must reference one identical suite definition")
    baseline_roots = {artifact["run"]["reset_id"] for artifact in baseline.values()}
    candidate_roots = {artifact["run"]["reset_id"] for artifact in candidate.values()}
    if baseline_roots & candidate_roots:
        raise ProtocolError("all fourteen comparison arm roots require unique reset IDs")
    baseline_resets = {
        item["reset_sha256"]
        for artifact in baseline.values()
        for item in artifact["schedule"]["physical_resets"]
    }
    candidate_resets = {
        item["reset_sha256"]
        for artifact in candidate.values()
        for item in artifact["schedule"]["physical_resets"]
    }
    if baseline_resets & candidate_resets:
        raise ProtocolError("all nested physical resets must be unique across comparison arms")
    diagnostics: list[str] = []
    failed_arms = False
    inconclusive = False
    ratios: dict[str, list[float]] = {"qps": [], "ttr": [], "source_etd": []}
    etd_upper_ratios: list[float | None] = []
    pair_results: list[dict[str, Any]] = []
    split = POLICY["comparison_split"]
    for pair in range(1, POLICY["pair_count"] + 1):
        for scenario, splits in COMPARISON_MATRIX.items():
            for scenario_split in splits:
                b = baseline[(pair, scenario, scenario_split)]
                c = candidate[(pair, scenario, scenario_split)]
                artifacts = (b, c)
                for artifact in artifacts:
                    qps_censor = (
                        scenario == "qps"
                        and artifact["status"] == "inconclusive"
                        and artifact["metrics"]["capacity_censoring"] != "none"
                        and all(artifact["gates"].values())
                    )
                    if artifact["status"] != "pass" and not qps_censor:
                        failed_arms = True
                        diagnostics.append(f"pair-{pair}:{scenario}:{scenario_split}:arm-not-pass")
                    if not all(artifact["gates"].values()):
                        failed_arms = True
                        diagnostics.append(f"pair-{pair}:{scenario}:{scenario_split}:gate-failed")
        bq = baseline[(pair, "qps", split)]["metrics"]
        cq = candidate[(pair, "qps", split)]["metrics"]
        if bq["capacity_censoring"] != "none" or cq["capacity_censoring"] != "none":
            inconclusive = True
            diagnostics.append(f"pair-{pair}:qps:capacity-censored")
            qps_ratio = 0.0
        elif bq["sustainable_qps"] <= 0:
            inconclusive = True
            diagnostics.append(f"pair-{pair}:qps:zero-baseline")
            qps_ratio = 0.0
        else:
            qps_ratio = cq["sustainable_qps"] / bq["sustainable_qps"]
        bt = baseline[(pair, "ttr", split)]["metrics"]["p95_ms"]
        ct = candidate[(pair, "ttr", split)]["metrics"]["p95_ms"]
        if ct <= 0:
            inconclusive = True
            diagnostics.append(f"pair-{pair}:ttr:zero-candidate")
            ttr_ratio = 0.0
        else:
            ttr_ratio = bt / ct
        be = baseline[(pair, "etd_loaded", split)]["metrics"]["source_etd_p95"]
        ce = candidate[(pair, "etd_loaded", split)]["metrics"]["source_etd_p95"]
        if be["censoring"] in {"left", "right"} or ce["censoring"] in {"left", "right"} or be["upper_ms"] is None or ce["upper_ms"] is None:
            inconclusive = True
            diagnostics.append(f"pair-{pair}:source-etd:censored")
        etd_lower = be["lower_ms"] / ce["upper_ms"] if ce["upper_ms"] else 0.0
        etd_upper = None if ce["lower_ms"] == 0 else be["upper_ms"] / ce["lower_ms"]
        if etd_upper is None or etd_lower <= 1.0 <= etd_upper:
            inconclusive = True
            diagnostics.append(f"pair-{pair}:source-etd:threshold-overlap")
        pair_score_lower = _geometric_mean_nonnegative([qps_ratio, ttr_ratio, etd_lower])
        pair_score_upper = None if etd_upper is None else _geometric_mean_nonnegative([qps_ratio, ttr_ratio, etd_upper])
        ratios["qps"].append(qps_ratio)
        ratios["ttr"].append(ttr_ratio)
        ratios["source_etd"].append(etd_lower)
        etd_upper_ratios.append(etd_upper)
        pair_results.append({"pair_id": pair, "qps_ratio": qps_ratio, "ttr_ratio": ttr_ratio, "source_etd_ratio_lower": etd_lower, "source_etd_ratio_upper": etd_upper, "score_ratio_lower": pair_score_lower, "score_ratio_upper": pair_score_upper})
    geometric = {name: _geometric_mean_nonnegative(values) for name, values in ratios.items()}
    geometric_rsd_values = {name: geometric_rsd(values) for name, values in ratios.items()}
    score_values = [item["score_ratio_lower"] for item in pair_results]
    score = _geometric_mean_nonnegative(score_values)
    bootstrap_lower, bootstrap_upper = bootstrap_interval(score_values)
    direction_pairs = {name: sum(value > 1.0 for value in values) for name, values in ratios.items()}
    high_variability = (
        geometric_rsd_values["qps"] is None or geometric_rsd_values["qps"] > POLICY["qps_max_geometric_rsd"]
        or geometric_rsd_values["ttr"] is None or geometric_rsd_values["ttr"] > POLICY["ttr_max_geometric_rsd"]
        or geometric_rsd_values["source_etd"] is None or geometric_rsd_values["source_etd"] > POLICY["etd_max_geometric_rsd"]
    )
    if high_variability:
        inconclusive = True
        diagnostics.append("comparison:high-geometric-variability")
    threshold_failure = (
        score < POLICY["minimum_score_ratio"]
        or any(value < POLICY["minimum_constituent_ratio"] for value in geometric.values())
        or any(value < POLICY["minimum_direction_pairs"] for value in direction_pairs.values())
    )
    if bootstrap_lower <= 1.0:
        inconclusive = True
        diagnostics.append("comparison:bootstrap-lower-not-above-one")
    if failed_arms:
        status = "fail"
    elif inconclusive:
        status = "inconclusive"
    elif threshold_failure:
        status = "fail"
    else:
        status = "pass"
    inputs = [
        {"arm": "baseline", "manifest_sha256": baseline_manifest["manifest_sha256"]},
        {"arm": "candidate", "manifest_sha256": candidate_manifest["manifest_sha256"]},
    ]
    metrics = {
        "comparison_split": split, "ratios": geometric,
        "source_etd_ratio_upper": None if any(value is None for value in etd_upper_ratios) else geometric_mean([float(value) for value in etd_upper_ratios]),
        "geometric_rsd": geometric_rsd_values, "direction_pairs": direction_pairs,
        "suite_score_ratio": score, "suite_score_ci95": {"lower": bootstrap_lower, "upper": bootstrap_upper},
        "pair_results": pair_results,
    }
    return _summary("comparison", status, inputs, metrics, diagnostics)


def evaluate_repeatability(manifest_paths: Sequence[Path | str]) -> dict[str, Any]:
    """Evaluate exactly seven complete, independent baseline manifests."""
    if len(manifest_paths) != POLICY["repeatability_runs"]:
        raise ProtocolError("repeatability requires exactly seven manifests")
    loaded = sorted(
        (load_suite_manifest(path) for path in manifest_paths),
        key=lambda item: item[0]["manifest_sha256"],
    )
    manifests = [item[0] for item in loaded]
    if any(manifest["purpose"] != "repeatability" or manifest["arm"] != "baseline" for manifest in manifests):
        raise ProtocolError("repeatability consumes baseline purpose=repeatability manifests only")
    definition_hashes = {manifest["suite_definition_sha256"] for manifest in manifests}
    if len(definition_hashes) != 1:
        raise ProtocolError("repeatability manifests must share one suite definition")
    all_reset_ids: list[str] = []
    all_physical_resets: list[str] = []
    values: dict[str, list[float]] = {"qps": [], "ttr": [], "source_etd": [], "db_ack_etd": []}
    for manifest, scenarios in loaded:
        resets = {artifact["run"]["reset_id"] for artifact in scenarios.values()}
        if len(resets) != 1:
            raise ProtocolError("each repeatability manifest must represent one logical arm reset")
        all_reset_ids.extend(resets)
        all_physical_resets.extend(
            item["reset_sha256"]
            for artifact in scenarios.values()
            for item in artifact["schedule"]["physical_resets"]
        )
        qps = scenarios[(1, "qps", "research")]["metrics"]
        if qps["capacity_censoring"] != "none" or qps["sustainable_qps"] <= 0:
            raise ProtocolError("repeatability sustainable QPS must be positive and uncensored")
        for artifact in scenarios.values():
            if artifact["status"] != "pass" or not all(artifact["gates"].values()):
                raise ProtocolError("repeatability requires every correctness/resource/mixed gate to pass")
        ttr = scenarios[(1, "ttr", "research")]["metrics"]["p95_ms"]
        etd = scenarios[(1, "etd_loaded", "research")]["metrics"]
        source = _validate_interval(etd["source_etd_p95"], "source_etd_p95")
        db_ack = _validate_interval(etd["db_ack_etd_p95"], "db_ack_etd_p95")
        if source[1] is None or db_ack[1] is None or source[2] in {"left", "right"} or db_ack[2] == "right":
            raise ProtocolError("repeatability ETD intervals must be bounded and not source-left/right-censored")
        values["qps"].append(float(qps["sustainable_qps"]))
        values["ttr"].append(float(ttr))
        values["source_etd"].append((source[0] + source[1]) / 2)
        values["db_ack_etd"].append((db_ack[0] + db_ack[1]) / 2)
    if len(set(all_reset_ids)) != POLICY["repeatability_runs"]:
        raise ProtocolError("repeatability requires seven unique logical arm reset IDs")
    if len(set(all_physical_resets)) != len(all_physical_resets):
        raise ProtocolError("repeatability requires globally unique nested physical resets")
    ratios = {name: mad_ratio(series) for name, series in values.items()}
    limits = {
        "qps": POLICY["repeatability_qps_mad_ratio"], "ttr": POLICY["repeatability_ttr_mad_ratio"],
        "source_etd": POLICY["repeatability_etd_mad_ratio"], "db_ack_etd": POLICY["repeatability_etd_mad_ratio"],
    }
    status = "pass" if all(ratios[name] <= limits[name] for name in ratios) else "fail"
    inputs = sorted(
        ({"arm": "baseline", "manifest_sha256": manifest["manifest_sha256"]} for manifest in manifests),
        key=lambda item: item["manifest_sha256"],
    )
    diagnostics = [] if status == "pass" else [f"repeatability:{name}:mad-ratio-exceeded" for name in ratios if ratios[name] > limits[name]]
    return _summary("repeatability", status, inputs, {"values": values, "mad_ratios": ratios, "limits": limits}, diagnostics)
