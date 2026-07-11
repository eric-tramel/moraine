#!/usr/bin/env python3
"""Validate and compare moraine-benchmark-v1 result artifacts.

This module intentionally uses only the Python standard library.  The checked-in
JSON Schema is the cross-language contract; the validation below additionally
checks arithmetic, redaction, reference, and comparison invariants that JSON
Schema cannot express portably.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import json
import math
import os
import re
import statistics
import sys
import tempfile
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence, Union
from urllib.parse import unquote_plus

CREDENTIALIZED_URL_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9+.-]*://[^\s/@]+@")
HOME_PATH_RE = re.compile(r"(?:^|[\s\"'=])(?:/(?:Users|home)/[^/\s]+|[A-Za-z]:[\\/]Users[\\/][^\\/\s]+)")
AUTH_VALUE_RE = re.compile(
    r"(?i)(?:authorization\s*:\s*)?\b(bearer|basic)\s+([A-Za-z0-9._~+/=-]+)"
)
SENSITIVE_ASSIGNMENT_RE = re.compile(
    r"(?i)(?:^|[^a-z0-9])"
    r"(?:access[_-]?key|access[_-]?token|api[_-]?key|authorization|"
    r"conversation|cookie|credential|host(?:name)?|message|password|passwd|"
    r"private[_-]?key|prompt|query|refresh[_-]?token|secret|session[_-]?token|"
    r"user(?:name)?)\s*[=:]\s*[^/\s&?#]+"
)
PRIVATE_CONTENT_RE = re.compile(
    r"(?i)(?:(?:private|raw)[._ -]*(?:conversation|message|prompt|query|text)|"
    r"(?:conversation|message|prompt|query|text)[._ -]*(?:private|raw))"
)
USER_HOST_IDENTITY_RE = re.compile(
    r"(?i)(?:^|[/_.-])[a-z0-9][a-z0-9_.-]*@[a-z0-9][a-z0-9_.-]*(?:$|[/_.-])"
)

FORBIDDEN_KEYS = {
    "access_key",
    "access_token",
    "api_key",
    "authorization",
    "conversation",
    "conversation_text",
    "cookie",
    "credential",
    "credentials",
    "host",
    "host_name",
    "hostname",
    "machine_id",
    "machine_name",
    "message",
    "messages",
    "password",
    "passwd",
    "private_key",
    "prompt",
    "query",
    "query_text",
    "raw_conversation",
    "raw_query",
    "raw_text",
    "refresh_token",
    "secret",
    "session_token",
    "text",
    "user",
    "user_name",
    "username",
}


class ProtocolError(ValueError):
    """An artifact violates the benchmark protocol."""


class IncomparableError(ProtocolError):
    """Two valid artifacts do not describe comparable scenarios."""


def _fail(path: str, message: str) -> None:
    raise ProtocolError(f"{path}: {message}")


_SCHEMA_ANNOTATIONS = {
    "$comment",
    "$id",
    "$schema",
    "description",
    "title",
}
_SCHEMA_CONSTRAINTS = {
    "$ref",
    "additionalProperties",
    "allOf",
    "const",
    "else",
    "enum",
    "if",
    "items",
    "maxLength",
    "maximum",
    "minLength",
    "minimum",
    "minProperties",
    "not",
    "oneOf",
    "pattern",
    "properties",
    "propertyNames",
    "required",
    "then",
    "type",
}


def _assert_supported_schema(schema: Mapping[str, Any], path: str = "$") -> None:
    permitted = _SCHEMA_ANNOTATIONS | _SCHEMA_CONSTRAINTS | {"$defs"}
    unknown = schema.keys() - permitted
    if unknown:
        raise RuntimeError(
            f"unsupported normative schema keyword(s) at {path}: "
            f"{', '.join(sorted(unknown))}"
        )
    for container_name in ("properties", "$defs"):
        for name, child in schema.get(container_name, {}).items():
            _assert_supported_schema(child, f"{path}.{container_name}.{name}")
    for name in ("items", "not", "propertyNames", "if", "then", "else"):
        child = schema.get(name)
        if child is not None:
            _assert_supported_schema(child, f"{path}.{name}")
    additional = schema.get("additionalProperties")
    if isinstance(additional, dict):
        _assert_supported_schema(additional, f"{path}.additionalProperties")
    for name in ("allOf", "oneOf"):
        for index, child in enumerate(schema.get(name, ())):
            _assert_supported_schema(child, f"{path}.{name}[{index}]")


def _load_normative_schema() -> Mapping[str, Any]:
    schema_path = Path(__file__).with_name("schema") / "moraine-benchmark-v1.schema.json"
    try:
        with schema_path.open("r", encoding="utf-8") as handle:
            schema = json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"unable to load normative benchmark schema: {error}") from error
    if not isinstance(schema, dict):
        raise RuntimeError("normative benchmark schema must be a JSON object")
    return schema


NORMATIVE_SCHEMA = _load_normative_schema()
_assert_supported_schema(NORMATIVE_SCHEMA)
SCHEMA_VERSION = NORMATIVE_SCHEMA["properties"]["schema_version"]["const"]


def _json_equal(left: Any, right: Any) -> bool:
    if isinstance(left, bool) != isinstance(right, bool):
        return False
    return left == right


def _resolve_schema_reference(reference: str) -> Mapping[str, Any]:
    if not reference.startswith("#/"):
        raise RuntimeError(f"unsupported non-local schema reference: {reference}")
    resolved: Any = NORMATIVE_SCHEMA
    for component in reference[2:].split("/"):
        component = component.replace("~1", "/").replace("~0", "~")
        resolved = resolved[component]
    if not isinstance(resolved, dict):
        raise RuntimeError(f"schema reference does not resolve to an object: {reference}")
    return resolved


def _schema_matches(value: Any, schema: Mapping[str, Any], path: str) -> bool:
    try:
        _validate_schema_node(value, schema, path)
    except ProtocolError:
        return False
    return True


def _validate_schema_type(value: Any, expected: str, path: str) -> None:
    valid = {
        "object": isinstance(value, dict),
        "array": isinstance(value, list),
        "string": isinstance(value, str),
        "boolean": type(value) is bool,
        "integer": not isinstance(value, bool)
        and (
            type(value) is int
            or (
                isinstance(value, float)
                and math.isfinite(value)
                and value.is_integer()
            )
        ),
        "number": not isinstance(value, bool) and isinstance(value, (int, float)),
        "null": value is None,
    }.get(expected)
    if valid is None:
        raise RuntimeError(f"unsupported JSON Schema type: {expected}")
    if not valid:
        _fail(path, f"must be of type {expected}")
    if expected == "number" and isinstance(value, float) and not math.isfinite(value):
        _fail(path, "must be finite")


def _validate_schema_node(value: Any, schema: Mapping[str, Any], path: str) -> None:
    reference = schema.get("$ref")
    if reference is not None:
        _validate_schema_node(value, _resolve_schema_reference(reference), path)

    expected_type = schema.get("type")
    if expected_type is not None:
        _validate_schema_type(value, expected_type, path)

    if "const" in schema and not _json_equal(value, schema["const"]):
        if path == "$.schema_version":
            _fail(path, f"unsupported version; expected {schema['const']!r}")
        _fail(path, f"must equal {schema['const']!r}")
    if "enum" in schema and not any(
        _json_equal(value, permitted) for permitted in schema["enum"]
    ):
        _fail(path, f"must be one of {schema['enum']!r}")

    if isinstance(value, str):
        if len(value) < schema.get("minLength", 0):
            _fail(path, f"must contain at least {schema['minLength']} character(s)")
        if "maxLength" in schema and len(value) > schema["maxLength"]:
            _fail(path, f"must contain at most {schema['maxLength']} character(s)")
        pattern = schema.get("pattern")
        if pattern is not None and re.search(pattern, value) is None:
            _fail(path, "does not match the normative schema pattern")

    if (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and (type(value) is int or math.isfinite(value))
    ):
        if "minimum" in schema and value < schema["minimum"]:
            _fail(path, f"must be >= {schema['minimum']}")
        if "maximum" in schema and value > schema["maximum"]:
            _fail(path, f"must be <= {schema['maximum']}")

    if isinstance(value, dict):
        non_string_keys = [name for name in value if not isinstance(name, str)]
        if non_string_keys:
            _fail(path, "object field names must be strings")
        if "minProperties" in schema and len(value) < schema["minProperties"]:
            _fail(path, f"must contain at least {schema['minProperties']} field(s)")
        required = set(schema.get("required", ()))
        missing = required - value.keys()
        if missing:
            _fail(path, f"missing required field(s): {', '.join(sorted(missing))}")
        properties = schema.get("properties", {})
        additional = schema.get("additionalProperties", True)
        unknown = value.keys() - properties.keys()
        if additional is False and unknown:
            _fail(path, f"unknown field(s): {', '.join(sorted(unknown))}")
        for name, child in value.items():
            child_path = f"{path}.{name}"
            if "propertyNames" in schema:
                _validate_schema_node(name, schema["propertyNames"], child_path)
            child_schema = properties.get(name)
            if child_schema is not None:
                _validate_schema_node(child, child_schema, child_path)
            elif isinstance(additional, dict):
                _validate_schema_node(child, additional, child_path)

    if isinstance(value, list) and "items" in schema:
        for index, child in enumerate(value):
            _validate_schema_node(child, schema["items"], f"{path}[{index}]")

    for child_schema in schema.get("allOf", ()):
        _validate_schema_node(value, child_schema, path)

    if "oneOf" in schema:
        matching = sum(
            _schema_matches(value, child_schema, path)
            for child_schema in schema["oneOf"]
        )
        if matching != 1:
            _fail(path, "must match exactly one permitted structure")

    if "not" in schema and _schema_matches(value, schema["not"], path):
        _fail(path, "matches a prohibited structure")

    condition = schema.get("if")
    if condition is not None:
        branch = schema.get("then") if _schema_matches(value, condition, path) else schema.get("else")
        if branch is not None:
            _validate_schema_node(value, branch, path)


def _validate_schema_structure(value: Any) -> Mapping[str, Any]:
    _validate_schema_node(value, NORMATIVE_SCHEMA, "$")
    if not isinstance(value, dict):
        _fail("$", "must be an object")
    return value


def _validate_sample_invariants(artifact: Mapping[str, Any]) -> None:
    samples = artifact["samples"]
    attempted = samples["attempted"]
    successful = samples["successful"]
    errors = samples["errors"]
    if attempted != successful + errors:
        _fail("$.samples", "attempted must equal successful + errors")
    if attempted > samples["planned"]:
        _fail("$.samples", "attempted must not exceed planned")
    measurements = samples.get("measurements")
    if measurements is not None:
        for name, series in measurements.items():
            if len(series) != successful:
                _fail(
                    f"$.samples.measurements.{name}",
                    "raw measurement count must equal samples.successful",
                )
    else:
        external_samples = samples["external_samples"]
        if external_samples["count"] != successful:
            _fail("$.samples.external_samples.count", "must equal samples.successful")


def _validate_status_invariants(artifact: Mapping[str, Any]) -> None:
    timing = artifact["timing"]
    samples = artifact["samples"]
    if (
        timing["status"] == "pass"
        and samples["errors"]
        and samples["errors"] / samples["attempted"]
        > timing["comparison_policy"]["error_rate"]["maximum"]
    ):
        _fail(
            "$.timing.status",
            "cannot pass when observed error rate exceeds comparison policy",
        )


def _validate_metric_invariants(artifact: Mapping[str, Any]) -> None:
    tokens = artifact.get("metrics", {}).get("model_tokens")
    if tokens is None:
        return
    if "total_tokens" in tokens and {"input_tokens", "output_tokens"} <= tokens.keys():
        if tokens["total_tokens"] != tokens["input_tokens"] + tokens["output_tokens"]:
            _fail(
                "$.metrics.model_tokens.total_tokens",
                "must equal input_tokens + output_tokens",
            )


def _validate_reference_invariants(artifact: Mapping[str, Any]) -> None:
    artifacts_by_id: dict[str, Mapping[str, Any]] = {}
    for index, entry in enumerate(artifact["artifacts"]):
        artifact_id = entry["id"]
        if artifact_id in artifacts_by_id:
            _fail(f"$.artifacts[{index}].id", "artifact ids must be unique")
        artifacts_by_id[artifact_id] = entry

    for index, diagnostic in enumerate(artifact["diagnostics"]):
        reference = diagnostic.get("artifact_id")
        if reference is not None and reference not in artifacts_by_id:
            _fail(
                f"$.diagnostics[{index}].artifact_id",
                "must reference an artifact declared in $.artifacts",
            )

    external = artifact["samples"].get("external_samples")
    if external is None:
        return
    referenced = artifacts_by_id.get(external["artifact_id"])
    if referenced is None:
        _fail(
            "$.samples.external_samples.artifact_id",
            "must reference an artifact declared in $.artifacts",
        )
    declared_checksum = referenced.get("sha256")
    if declared_checksum is not None and external["sha256"] != declared_checksum:
        _fail(
            "$.samples.external_samples.sha256",
            "must equal the referenced artifact sha256",
        )

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
            scheme = match.group(1).lower()
            token = match.group(2)
            if scheme == "bearer":
                return True
            padded = token + "=" * (-len(token) % 4)
            try:
                decoded = base64.b64decode(padded, validate=True)
            except (binascii.Error, ValueError):
                continue
            if b":" in decoded:
                return True
    return False


def _scan_redaction(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = str(key).lower().replace("-", "_")
            if (
                normalized in FORBIDDEN_KEYS
                or normalized.endswith("_password")
                or normalized.endswith("_secret")
                or normalized.endswith("_token")
            ):
                _fail(f"{path}.{key}", "forbidden sensitive or raw-text field")
            _scan_redaction(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _scan_redaction(child, f"{path}[{index}]")
    elif isinstance(value, str):
        if _contains_authorization_credential(value):
            _fail(path, "authorization credentials are forbidden")
        for candidate in _decoded_string_forms(value):
            if CREDENTIALIZED_URL_RE.search(candidate):
                _fail(path, "credentialized URLs are forbidden")
            if HOME_PATH_RE.search(candidate):
                _fail(path, "absolute user home paths are forbidden")
            if (
                SENSITIVE_ASSIGNMENT_RE.search(candidate)
                or PRIVATE_CONTENT_RE.search(candidate)
                or USER_HOST_IDENTITY_RE.search(candidate)
            ):
                _fail(path, "embedded sensitive material is forbidden")


def validate_artifact(value: Any) -> None:
    """Validate one in-memory moraine-benchmark-v1 artifact.

    The checked-in JSON Schema owns structural validation. Python adds only the
    cross-field arithmetic, redaction, reference, and semantic invariants that
    the normative schema deliberately does not express.
    """

    _scan_redaction(value)
    artifact = _validate_schema_structure(value)
    _validate_sample_invariants(artifact)
    _validate_status_invariants(artifact)
    _validate_metric_invariants(artifact)
    _validate_reference_invariants(artifact)


def _reject_json_constant(value: str) -> None:
    raise ProtocolError(f"invalid non-finite JSON number: {value}")


def load_artifact(path: Union[str, Path]) -> dict[str, Any]:
    """Load and validate an artifact without accepting NaN or Infinity."""

    artifact_path = Path(path)
    try:
        with artifact_path.open("r", encoding="utf-8") as handle:
            value = json.load(handle, parse_constant=_reject_json_constant)
    except ProtocolError:
        raise
    except (OSError, json.JSONDecodeError) as error:
        raise ProtocolError(f"{artifact_path}: unable to load JSON: {error}") from error
    validate_artifact(value)
    return value


def _require_same(baseline: Mapping[str, Any], candidate: Mapping[str, Any], path: Sequence[str]) -> None:
    left: Any = baseline
    right: Any = candidate
    for component in path:
        left = left[component]
        right = right[component]
    if left != right:
        dotted = ".".join(path)
        raise IncomparableError(
            f"incomparable {dotted}: baseline={left!r}, candidate={right!r}"
        )


def _raw_measurements(artifact: Mapping[str, Any]) -> Mapping[str, list[float]]:
    measurements = artifact["samples"].get("measurements")
    if measurements is None:
        raise IncomparableError("raw samples are externalized; materialize them before comparison")
    return measurements


def _select_measurement(
    baseline: Mapping[str, Any], candidate: Mapping[str, Any], requested: Optional[str]
) -> str:
    baseline_names = set(_raw_measurements(baseline))
    candidate_names = set(_raw_measurements(candidate))
    common = baseline_names & candidate_names
    if requested is not None:
        if requested not in common:
            raise IncomparableError(f"measurement {requested!r} is not present in both artifacts")
        return requested
    latency_names = sorted(name for name in common if "latency" in name and name.endswith(("_ns", "_us", "_ms", "_seconds")))
    if len(latency_names) != 1:
        raise IncomparableError(
            "comparison requires --measurement when there is not exactly one common latency series"
        )
    return latency_names[0]


def _percentile(values: Sequence[float], percentile: float) -> float:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        raise IncomparableError("cannot compute a statistic from zero successful samples")
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * percentile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _statistic(values: Sequence[float], name: str) -> float:
    if not values:
        raise IncomparableError("cannot compare zero successful samples")
    if name == "mean":
        return statistics.fmean(values)
    if name == "median":
        return statistics.median(values)
    if name == "p95":
        return _percentile(values, 0.95)
    if name == "p99":
        return _percentile(values, 0.99)
    raise ProtocolError(f"unsupported statistic: {name}")


def _variability(values: Sequence[float], name: str) -> float:
    numbers = [float(value) for value in values]
    if len(numbers) < 2:
        return 0.0
    mean = statistics.fmean(numbers)
    if mean == 0.0:
        return 0.0 if all(value == 0.0 for value in numbers) else math.inf
    if name == "coefficient_of_variation":
        return statistics.pstdev(numbers) / mean
    if name == "relative_range":
        return (max(numbers) - min(numbers)) / mean
    raise ProtocolError(f"unsupported variability statistic: {name}")


def compare_artifacts(
    baseline: Mapping[str, Any],
    candidate: Mapping[str, Any],
    measurement: Optional[str] = None,
) -> dict[str, Any]:
    """Compare two valid, fingerprint-equivalent benchmark artifacts.

    Timing results are always diagnostic (`non_blocking: true`).  A missing policy
    produces `not_evaluated`; scenario mismatches raise IncomparableError.
    """

    validate_artifact(baseline)
    validate_artifact(candidate)
    for path in (
        ("schema_version",),
        ("benchmark_id",),
        ("scenario_id",),
        ("build", "profile"),
        ("build", "target"),
        ("runner", "os"),
        ("runner", "cpu_class"),
        ("scenario", "profile"),
        ("scenario", "workload_id"),
        ("scenario", "measured_boundary"),
        ("scenario", "dimensions"),
        ("scenario", "fingerprints"),
    ):
        _require_same(baseline, candidate, path)

    if candidate["scenario"]["profile"] == "smoke":
        return {"status": "not_evaluated", "non_blocking": True, "reason": "smoke_profile"}
    if baseline["semantic"]["status"] != "pass" or candidate["semantic"]["status"] != "pass":
        return {"status": "not_evaluated", "non_blocking": True, "reason": "semantic_failure"}

    baseline_policy = baseline["timing"].get("comparison_policy")
    candidate_policy = candidate["timing"].get("comparison_policy")
    if candidate_policy is None:
        return {"status": "not_evaluated", "non_blocking": True, "reason": "missing_comparison_policy"}
    if baseline_policy is not None and baseline_policy != candidate_policy:
        raise IncomparableError("incomparable timing.comparison_policy")
    policy = candidate_policy
    measurement_name = _select_measurement(baseline, candidate, measurement)
    baseline_values = _raw_measurements(baseline)[measurement_name]
    candidate_values = _raw_measurements(candidate)[measurement_name]

    statistic_name = policy["statistic"]
    baseline_statistic = _statistic(baseline_values, statistic_name)
    candidate_statistic = _statistic(candidate_values, statistic_name)
    variability_name = policy["variability"]["statistic"]
    baseline_variability = _variability(baseline_values, variability_name)
    candidate_variability = _variability(candidate_values, variability_name)
    baseline_error_rate = baseline["samples"]["errors"] / baseline["samples"]["attempted"]
    candidate_error_rate = candidate["samples"]["errors"] / candidate["samples"]["attempted"]

    result: dict[str, Any] = {
        "status": "pass",
        "non_blocking": True,
        "measurement": measurement_name,
        "statistic": statistic_name,
        "baseline_value": baseline_statistic,
        "candidate_value": candidate_statistic,
        "baseline_error_rate": baseline_error_rate,
        "candidate_error_rate": candidate_error_rate,
        "variability": {
            "statistic": variability_name,
            "baseline": baseline_variability,
            "candidate": candidate_variability,
        },
        "independent_run_pairs": 1,
    }
    error_rate_limit = policy["error_rate"]["maximum"]
    violating_arms = [
        arm
        for arm, observed in (
            ("baseline", baseline_error_rate),
            ("candidate", candidate_error_rate),
        )
        if observed > error_rate_limit
    ]
    if violating_arms:
        result.update(
            status="fail",
            reason="error_rate_threshold_exceeded",
            error_rate_limit=error_rate_limit,
            violating_arms=violating_arms,
        )
        return result
    if policy["minimum_independent_run_pairs"] > 1:
        result.update(status="inconclusive", reason="insufficient_independent_run_pairs")
        return result
    variability_limit = policy["variability"]["maximum"]
    if baseline_variability > variability_limit or candidate_variability > variability_limit:
        result.update(status="inconclusive", reason="variability_bound_exceeded")
        return result
    if baseline_statistic == 0.0:
        if candidate_statistic == 0.0:
            relative_regression = 0.0
        else:
            result.update(status="inconclusive", reason="zero_baseline")
            return result
    else:
        relative_regression = (candidate_statistic - baseline_statistic) / baseline_statistic
    result["relative_regression"] = relative_regression
    if relative_regression > policy["threshold"]["maximum_relative_regression"]:
        result.update(status="fail", reason="timing_threshold_exceeded")
    return result


def _atomic_write_json(path: Union[str, Path], value: Mapping[str, Any]) -> None:
    destination = Path(path)
    encoded = (
        json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n"
    ).encode("utf-8")
    destination.parent.mkdir(parents=True, exist_ok=True)

    descriptor = -1
    temporary: Optional[Path] = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
        )
        temporary = Path(temporary_name)
        handle = os.fdopen(descriptor, "wb")
        descriptor = -1
        with handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        temporary = None
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary is not None:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


def write_artifact(path: Union[str, Path], artifact: Mapping[str, Any]) -> None:
    """Validate and atomically replace one benchmark artifact."""

    validate_artifact(artifact)
    _atomic_write_json(path, artifact)


def _parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    validate = commands.add_parser("validate", help="validate one or more result artifacts")
    validate.add_argument("artifacts", nargs="+")
    compare = commands.add_parser("compare", help="compare two result artifacts")
    compare.add_argument("baseline")
    compare.add_argument("candidate")
    compare.add_argument("--measurement")
    compare.add_argument("--output", type=Path)
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        if args.command == "validate":
            for path in args.artifacts:
                load_artifact(path)
                print(f"valid: {path}")
            return 0
        baseline = load_artifact(args.baseline)
        candidate = load_artifact(args.candidate)
        result = compare_artifacts(baseline, candidate, args.measurement)
        if args.output is not None:
            _atomic_write_json(args.output, result)
        print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))
        return 0
    except IncomparableError as error:
        print(f"incomparable: {error}", file=sys.stderr)
        return 2
    except ProtocolError as error:
        print(f"invalid benchmark artifact: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
