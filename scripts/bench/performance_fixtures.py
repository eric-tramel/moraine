#!/usr/bin/env python3
"""Frozen deterministic fixture recipes and independent search oracles.

This module is import-only.  The server receives only the closed-form seed
manifest/SQL; query answers and holdout identities remain in the load generator.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import json
import math
import re
from dataclasses import dataclass
from fractions import Fraction
from typing import Any, Iterable, Mapping, Sequence

from performance_protocol import sha256_json

RECIPE_VERSION = "moraine-search-performance-fixture-v1"
GENERATOR_VERSION = "moraine-search-performance-corpus-v1"
SEARCH_SCHEMA_VERSION = "moraine.mcp.search_sessions.v1"
SEARCH_TOOL = "search_sessions"
SPLITS = ("research", "holdout", "stress")
SCENARIOS = ("qps", "ttr", "etd_idle", "etd_loaded", "mixed")
SPLIT_MATRIX = {
    "qps": ["research", "holdout"],
    "ttr": ["research", "holdout"],
    "etd_idle": ["research", "holdout"],
    "etd_loaded": ["research", "holdout"],
    "mixed": ["stress"],
}
PROFILE_SCALE = {
    "smoke": {
        "documents": 1_000,
        "query_counts": {"research": 16, "holdout": 8, "stress": 8},
        "event_counts": {"research": 3, "holdout": 3, "stress": 3},
        "query_duration_ns": 3_000_000_000,
        "visibility_timeout_ns": 5_000_000_000,
        "probe_terms": 64,
    },
    "full": {
        "documents": 100_000,
        "query_counts": {"research": 96, "holdout": 32, "stress": 24},
        "event_counts": {"research": 15, "holdout": 15, "stress": 5},
        "query_duration_ns": 30_000_000_000,
        "visibility_timeout_ns": 20_000_000_000,
        "probe_terms": 256,
    },
}
QUERY_MODES = (
    "rare",
    "selective",
    "session_scope",
    "ranking_tie",
    "hydration",
    "event_type_filter",
    "age_order",
    "term_frequency",
)
SESSION_LENGTHS = (1, 4, 16, 64)
SESSION_CYCLE = sum(SESSION_LENGTHS)
AGE_DAYS = (0, 1, 7, 30, 90, 180, 365, 730)
PAYLOAD_BYTES = (0, 256, 4_096, 16_384)
EVENT_TYPES = (
    "user_input",
    "assistant_response",
    "reasoning",
    "tool_call",
    "tool_response",
    "compaction",
)
ACTOR_ROLES = ("user", "assistant", "assistant", "assistant", "tool", "system")
EVENT_CLASSES = ("message", "message", "reasoning", "tool_call", "tool_result", "summary")
PAYLOAD_TYPES = ("message", "message", "reasoning", "function_call", "function_call_output", "compacted")
FINGERPRINT_FIELDS = (
    "corpus_sha256",
    "query_research_sha256",
    "query_holdout_sha256",
    "query_stress_sha256",
    "event_research_sha256",
    "event_holdout_sha256",
    "event_stress_sha256",
    "split_matrix_sha256",
    "schedule_templates_sha256",
)
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class FixtureError(ValueError):
    """The frozen fixture or a result checked against it is invalid."""


@dataclass(frozen=True)
class FreshSeedTarget:
    """Runtime evidence required before corpus SQL may be produced."""

    database: str
    reset_id: str
    sandbox_owned: bool
    fresh_volume: bool
    empty_database: bool

    def validate(self) -> None:
        if not IDENTIFIER_RE.fullmatch(self.database):
            raise FixtureError("unsafe database identifier")
        if not isinstance(self.reset_id, str) or not self.reset_id.strip():
            raise FixtureError("fresh seed target requires a reset_id")
        if self.sandbox_owned is not True:
            raise FixtureError("seed target is not sandbox-owned")
        if self.fresh_volume is not True:
            raise FixtureError("seed target does not use a fresh physical volume")
        if self.empty_database is not True:
            raise FixtureError("seed target database is not empty")


class OneUseTermBank:
    """Hands each ETD probe term out once and fails closed on exhaustion."""

    __slots__ = ("case_id", "_terms", "_index", "_used")

    def __init__(self, event: Mapping[str, Any]) -> None:
        case_id = event.get("case_id")
        terms = event.get("probe_terms")
        if not isinstance(case_id, str) or not isinstance(terms, list) or not terms:
            raise FixtureError("event has no finite ETD probe bank")
        if any(not isinstance(term, str) or not term for term in terms):
            raise FixtureError("ETD probe bank contains an invalid term")
        if len(terms) != len(set(terms)):
            raise FixtureError("ETD probe bank contains reused terms")
        self.case_id = case_id
        self._terms = tuple(terms)
        self._index = 0
        self._used: set[str] = set()

    @property
    def remaining(self) -> int:
        return len(self._terms) - self._index

    def claim(self) -> str:
        if self._index >= len(self._terms):
            raise FixtureError(f"ETD probe bank exhausted for {self.case_id}")
        term = self._terms[self._index]
        self._index += 1
        if term in self._used:
            raise FixtureError(f"ETD probe term reused for {self.case_id}")
        self._used.add(term)
        return term

    def claim_query(self) -> dict[str, Any]:
        return {"query": self.claim(), "n_hits": 10}


def public_id(prefix: str, raw: str) -> str:
    encoded = base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")
    return f"{prefix}:{encoded}"


def event_uid(number: int) -> str:
    return f"perf-event-{number:08d}"


def _session_traits(number: int) -> tuple[int, int, int]:
    cycle, position = divmod(number, SESSION_CYCLE)
    start = 0
    for slot, length in enumerate(SESSION_LENGTHS):
        if position < start + length:
            return cycle * len(SESSION_LENGTHS) + slot, length, position - start
        start += length
    raise AssertionError("unreachable session position")


def session_id(number: int) -> str:
    session_number, _, _ = _session_traits(number)
    return f"perf-session-{session_number:08d}"


def _identity(number: int) -> dict[str, str]:
    return {
        "event_id": public_id("event", event_uid(number)),
        "session_id": public_id("session", session_id(number)),
    }


def _ack_digest(raw_event_uid: str) -> str:
    return hashlib.sha256(raw_event_uid.encode("utf-8")).hexdigest()


def _ordered_digest(identities: Sequence[Mapping[str, str]]) -> str:
    return sha256_json(list(identities))


def result_digest(numbers: Sequence[int]) -> str:
    """Return the order-sensitive digest used by the exact result oracle."""

    return _ordered_digest([_identity(number) for number in numbers])


def _age_days(number: int) -> int:
    return AGE_DAYS[number % len(AGE_DAYS)]


def _event_type(number: int) -> str:
    return EVENT_TYPES[number % len(EVENT_TYPES)]


def _actor_role(number: int) -> str:
    return ACTOR_ROLES[number % len(ACTOR_ROLES)]


def _rank_term_frequency(number: int, split_start: int, query_count: int) -> int:
    local = number - split_start
    return 1 + (local // query_count) % 4


def _payload_bytes(number: int, split_start: int, query_count: int) -> int:
    local = number - split_start
    return PAYLOAD_BYTES[(local // query_count) % len(PAYLOAD_BYTES)]


def _query_term(split: str, family: str, bucket: int, width: int = 4) -> str:
    return f"perf{split}{family}{bucket:0{width}d}"


def _split_ranges(documents: int) -> dict[str, dict[str, int]]:
    research_end = documents * 3 // 5
    holdout_end = documents * 4 // 5
    bounds = (("research", 0, research_end), ("holdout", research_end, holdout_end), ("stress", holdout_end, documents))
    return {
        split: {"start": start, "end": end, "document_count": end - start}
        for split, start, end in bounds
    }


def _build_corpus(profile: str) -> dict[str, Any]:
    scale = PROFILE_SCALE[profile]
    ranges = _split_ranges(scale["documents"])
    for split in SPLITS:
        count = scale["query_counts"][split]
        ranges[split].update(
            {
                "query_count": count,
                "tie_modulus": math.lcm(count, len(AGE_DAYS)),
                "role_modulus": count + 1,
                "age_modulus": count + 3,
                "scope_modulus": count + 5,
            }
        )
    return {
        "generator_version": GENERATOR_VERSION,
        "document_count": scale["documents"],
        "source_name": "performance-suite",
        "source_file": "owned/performance-corpus.jsonl",
        "base_timestamp": "2026-07-13T12:00:00.000Z",
        "splits": ranges,
        "dimensions": {
            "term_frequency": {"rank_term_occurrences": [1, 2, 3, 4], "constant_document_term_count": True},
            "selectivity": ["singleton", "split_bucket", "session_scope", "event_type_filter"],
            "scope": ["global", "session"],
            "ranking": ["bm25_term_frequency", "newest_event", "event_uid_ascending"],
            "ties": "equal-score/equal-age ties resolve by event_uid ascending",
            "hydration_payload_bytes": list(PAYLOAD_BYTES),
            "event_types": list(EVENT_TYPES),
            "actor_roles": list(dict.fromkeys(ACTOR_ROLES)),
            "session_lengths": list(SESSION_LENGTHS),
            "age_days": list(AGE_DAYS),
        },
    }


def _candidate_numbers(corpus: Mapping[str, Any], split: str, index: int, mode: str) -> list[int]:
    spec = corpus["splits"][split]
    start, end, count = spec["start"], spec["end"], spec["query_count"]
    if mode == "rare":
        return [start + index]
    if mode in {"selective", "hydration", "term_frequency"}:
        return list(range(start + index, end, count))
    if mode == "ranking_tie":
        return list(range(start + index, end, spec["tie_modulus"]))
    if mode == "event_type_filter":
        selected_type = _event_type(start + index)
        return [
            number
            for number in range(start + index, end, spec["role_modulus"])
            if _event_type(number) == selected_type
        ]
    if mode == "age_order":
        return list(range(start + index, end, spec["age_modulus"]))
    if mode == "session_scope":
        expected_session = session_id(start + index)
        return [
            number
            for number in range(start + index, end, spec["scope_modulus"])
            if session_id(number) == expected_session
        ]
    raise AssertionError(f"unknown query mode {mode}")


def _ranked_numbers(
    numbers: Iterable[int], *, mode: str, split_start: int, query_count: int
) -> list[int]:
    if mode == "term_frequency":
        return sorted(
            numbers,
            key=lambda number: (
                -_rank_term_frequency(number, split_start, query_count),
                _age_days(number),
                event_uid(number),
            ),
        )
    return sorted(numbers, key=lambda number: (_age_days(number), event_uid(number)))


def _oracle(numbers: Sequence[int], *, truncated: bool) -> dict[str, Any]:
    identities = [_identity(number) for number in numbers]
    identity_set = sorted(identities, key=lambda item: (item["event_id"], item["session_id"]))
    return {
        "count": len(identities),
        "truncated": truncated,
        "ordered_identities": identities,
        "ordered_sha256": _ordered_digest(identities),
        "set_sha256": sha256_json(identity_set),
    }


def _ordered_dimension_values(numbers: Sequence[int], function: Any, universe: Sequence[Any]) -> list[Any]:
    present = {function(number) for number in numbers}
    return [value for value in universe if value in present]


def _build_query_case(corpus: Mapping[str, Any], split: str, index: int) -> dict[str, Any]:
    mode = QUERY_MODES[index % len(QUERY_MODES)]
    spec = corpus["splits"][split]
    family = {
        "rare": "rare",
        "selective": "selectivecase",
        "session_scope": "scopecase",
        "ranking_tie": "tiecase",
        "hydration": "hydratecase",
        "event_type_filter": "rolecase",
        "age_order": "agecase",
        "term_frequency": "rankcase",
    }[mode]
    modulus = {
        "rare": None,
        "selective": spec["query_count"],
        "session_scope": spec["scope_modulus"],
        "ranking_tie": spec["tie_modulus"],
        "hydration": spec["query_count"],
        "event_type_filter": spec["role_modulus"],
        "age_order": spec["age_modulus"],
        "term_frequency": spec["query_count"],
    }[mode]
    width = 6 if mode == "rare" else 4
    term = _query_term(split, family, index if modulus is None else index % modulus, width)
    arguments: dict[str, Any] = {"query": term, "n_hits": 10, "event_types": list(EVENT_TYPES)}
    if mode == "session_scope":
        arguments["within_id"] = public_id("session", session_id(spec["start"] + index))
    if mode == "event_type_filter":
        arguments["event_types"] = [_event_type(spec["start"] + index)]
    candidates = _candidate_numbers(corpus, split, index, mode)
    ranked = _ranked_numbers(
        candidates,
        mode=mode,
        split_start=spec["start"],
        query_count=spec["query_count"],
    )
    selected = ranked[: arguments["n_hits"]]
    if not selected:
        raise AssertionError(f"fixture query {split}/{index} has no result")
    session_lengths = _ordered_dimension_values(selected, lambda number: _session_traits(number)[1], SESSION_LENGTHS)
    dimensions = {
        "selectivity": mode,
        "candidate_count": len(candidates),
        "corpus_fraction_ppm": len(candidates) * 1_000_000 // spec["document_count"],
        "scope": "session" if mode == "session_scope" else "global",
        "ranking": "bm25_term_frequency" if mode == "term_frequency" else ("event_uid_tie_break" if mode == "ranking_tie" else "age_then_event_uid"),
        "has_score_ties": mode == "ranking_tie",
        "hydration_payload_bytes": _ordered_dimension_values(
            selected,
            lambda number: _payload_bytes(number, spec["start"], spec["query_count"]),
            PAYLOAD_BYTES,
        ),
        "event_types": _ordered_dimension_values(selected, _event_type, EVENT_TYPES),
        "actor_roles": _ordered_dimension_values(selected, _actor_role, tuple(dict.fromkeys(ACTOR_ROLES))),
        "session_lengths": session_lengths,
        "age_days": _ordered_dimension_values(selected, _age_days, AGE_DAYS),
        "term_frequencies": sorted(
            {
                _rank_term_frequency(number, spec["start"], spec["query_count"])
                for number in selected
            }
        ),
    }
    return {
        "case_id": f"{split}-query-{index:04d}",
        "split": split,
        "mode": mode,
        "arguments": arguments,
        "dimensions": dimensions,
        "oracle": _oracle(selected, truncated=len(candidates) > arguments["n_hits"]),
    }


def _event_identity(split: str, index: int) -> tuple[str, str]:
    return f"perf-etd-{split}-event-{index:04d}", f"perf-etd-{split}-session-{index:04d}"


def _build_event_case(profile: str, split: str, index: int, term_count: int) -> dict[str, Any]:
    raw_event, raw_session = _event_identity(split, index)
    prefix = f"perfprobe{profile}{split}{index:04d}x"
    probe_terms = [f"{prefix}{probe:03d}" for probe in range(term_count)]
    recorded_at = f"2026-07-13T12:{SPLITS.index(split) * 10 + index:02d}:00.000Z"
    event = {
        "case_id": f"{split}-event-{index:04d}",
        "split": split,
        "raw_event_uid": raw_event,
        "raw_session_id": raw_session,
        "destination_filename": f"{split}-event-{index:04d}.jsonl",
        "recorded_at": recorded_at,
        "marker": f"perfmarker{profile}{split}{index:04d}",
        "probe_terms": probe_terms,
    }
    source_bytes = codex_event_lines(event)
    lines = source_bytes.splitlines(keepends=True)
    if len(lines) != 2:
        raise FixtureError("generated Codex ETD source must contain exactly two records")
    source_file = (
        "/sandbox/fixtures/codex/sessions/"
        f"{event['destination_filename']}"
    )
    record_fingerprint = lines[1].rstrip(b"\n").decode("utf-8")
    material = (
        f"{source_file}|1|2|{len(lines[0])}|"
        f"{record_fingerprint}|raw"
    )
    normalized_event_uid = hashlib.sha256(material.encode("utf-8")).hexdigest()
    identity = {
        "event_id": public_id("event", normalized_event_uid),
        "session_id": public_id("session", raw_session),
    }
    event.update(
        {
            "normalized_event_uid": normalized_event_uid,
            "expected_event_id": identity["event_id"],
            "expected_session_id": identity["session_id"],
            "expected_ack_digest": _ack_digest(normalized_event_uid),
            "oracle": {
                "count": 1,
                "truncated": False,
                "ordered_identities": [identity],
                "ordered_sha256": _ordered_digest([identity]),
                "set_sha256": sha256_json([identity]),
            },
        }
    )
    return event


def _event_operations(events: Sequence[Mapping[str, Any]], duration_ns: int) -> list[dict[str, Any]]:
    count = len(events)
    return [
        {
            "kind": "event",
            "sequence": index,
            "scheduled_offset_ns": ((2 * index + 1) * duration_ns) // (2 * count),
            "case_id": event["case_id"],
        }
        for index, event in enumerate(events)
    ]


def _build_schedule_templates(
    profile: str,
    query_splits: Mapping[str, Sequence[Mapping[str, Any]]],
    event_splits: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    scale = PROFILE_SCALE[profile]
    duration_ns = scale["query_duration_ns"]
    poll_interval_ns = 100_000_000
    return {
        "clock": "monotonic",
        "qps": {
            "model": "open",
            "duration_ns": duration_ns,
            "case_order_by_split": {
                split: [case["case_id"] for case in query_splits[split]] for split in SPLITS
            },
        },
        "ttr": {
            "case_order_by_split": {
                split: [case["case_id"] for case in query_splits[split]] for split in SPLITS
            }
        },
        "etd": {
            "model": "open",
            "poll_interval_ns": poll_interval_ns,
            "visibility_timeout_ns": scale["visibility_timeout_ns"],
            "event_stream_by_split": {
                split: _event_operations(event_splits[split], duration_ns) for split in SPLITS
            },
        },
        "mixed": {
            "model": "open",
            "duration_ns": duration_ns,
            "query_split": "stress",
            "event_split": "stress",
            "shared_barrier": "monotonic-zero",
            "controls": ["query_only", "ingest_only", "combined"],
            "combined_streams_identical_to_controls": True,
        },
    }


def compute_fingerprints(recipe: Mapping[str, Any]) -> dict[str, str]:
    """Recompute every component fingerprint from its sole source field."""

    try:
        queries = recipe["query_splits"]
        events = recipe["event_splits"]
        return {
            "corpus_sha256": sha256_json(recipe["corpus"]),
            "query_research_sha256": sha256_json(queries["research"]),
            "query_holdout_sha256": sha256_json(queries["holdout"]),
            "query_stress_sha256": sha256_json(queries["stress"]),
            "event_research_sha256": sha256_json(events["research"]),
            "event_holdout_sha256": sha256_json(events["holdout"]),
            "event_stress_sha256": sha256_json(events["stress"]),
            "split_matrix_sha256": sha256_json(recipe["split_matrix"]),
            "schedule_templates_sha256": sha256_json(recipe["schedule_templates"]),
        }
    except (KeyError, TypeError) as exc:
        raise FixtureError("recipe lacks fingerprint source fields") from exc


def compute_fixture_sha256(recipe: Mapping[str, Any]) -> str:
    unsigned = {key: value for key, value in recipe.items() if key != "fixture_sha256"}
    return sha256_json(unsigned)


def _make_recipe(profile: str) -> dict[str, Any]:
    if profile not in PROFILE_SCALE:
        raise FixtureError(f"unsupported profile {profile!r}")
    scale = PROFILE_SCALE[profile]
    corpus = _build_corpus(profile)
    query_splits = {
        split: [
            _build_query_case(corpus, split, index)
            for index in range(scale["query_counts"][split])
        ]
        for split in SPLITS
    }
    event_splits = {
        split: [
            _build_event_case(profile, split, index, scale["probe_terms"])
            for index in range(scale["event_counts"][split])
        ]
        for split in SPLITS
    }
    recipe: dict[str, Any] = {
        "recipe_version": RECIPE_VERSION,
        "profile": profile,
        "seed": {"algorithm": "closed-form", "value": 20_260_713},
        "corpus": corpus,
        "query_splits": query_splits,
        "event_splits": event_splits,
        "split_matrix": copy.deepcopy(SPLIT_MATRIX),
        "schedule_templates": _build_schedule_templates(profile, query_splits, event_splits),
    }
    recipe["fingerprints"] = compute_fingerprints(recipe)
    recipe["fixture_sha256"] = compute_fixture_sha256(recipe)
    return recipe


def build_recipe(profile: str) -> dict[str, Any]:
    """Build a fresh copy of the one frozen smoke or full recipe."""

    return _make_recipe(profile)


def _validate_oracle(oracle: Any, field: str) -> None:
    required = {"count", "truncated", "ordered_identities", "ordered_sha256", "set_sha256"}
    if not isinstance(oracle, dict) or set(oracle) != required:
        raise FixtureError(f"{field} oracle fields differ")
    identities = oracle["ordered_identities"]
    if isinstance(oracle["count"], bool) or not isinstance(oracle["count"], int) or oracle["count"] <= 0:
        raise FixtureError(f"{field} oracle count is invalid")
    if not isinstance(oracle["truncated"], bool) or not isinstance(identities, list):
        raise FixtureError(f"{field} oracle shape is invalid")
    normalized: list[dict[str, str]] = []
    for identity in identities:
        if not isinstance(identity, dict) or set(identity) != {"event_id", "session_id"}:
            raise FixtureError(f"{field} oracle identity is invalid")
        if not all(isinstance(identity[key], str) and identity[key] for key in identity):
            raise FixtureError(f"{field} oracle identity is invalid")
        normalized.append(identity)
    if len(normalized) != oracle["count"]:
        raise FixtureError(f"{field} oracle count differs from its order")
    keys = [(item["event_id"], item["session_id"]) for item in normalized]
    if len(keys) != len(set(keys)):
        raise FixtureError(f"{field} oracle contains duplicate identities")
    if oracle["ordered_sha256"] != _ordered_digest(normalized):
        raise FixtureError(f"{field} ordered oracle digest differs")
    identity_set = sorted(normalized, key=lambda item: (item["event_id"], item["session_id"]))
    if oracle["set_sha256"] != sha256_json(identity_set):
        raise FixtureError(f"{field} set oracle digest differs")


def validate_recipe(recipe: Mapping[str, Any]) -> None:
    """Fail closed unless *recipe* is exactly the checked-in closed-form recipe."""

    root_fields = {
        "recipe_version",
        "profile",
        "seed",
        "corpus",
        "query_splits",
        "event_splits",
        "split_matrix",
        "schedule_templates",
        "fingerprints",
        "fixture_sha256",
    }
    if not isinstance(recipe, dict) or set(recipe) != root_fields:
        raise FixtureError("recipe root fields differ")
    if recipe["recipe_version"] != RECIPE_VERSION or recipe["profile"] not in PROFILE_SCALE:
        raise FixtureError("recipe identity is unsupported")
    if not isinstance(recipe["fingerprints"], dict) or set(recipe["fingerprints"]) != set(FINGERPRINT_FIELDS):
        raise FixtureError("recipe fingerprint fields differ")
    if recipe["fingerprints"] != compute_fingerprints(recipe):
        raise FixtureError("recipe component fingerprint mismatch")
    if recipe["fixture_sha256"] != compute_fixture_sha256(recipe):
        raise FixtureError("fixture_sha256 mismatch")
    if recipe["split_matrix"] != SPLIT_MATRIX:
        raise FixtureError("split matrix differs from the frozen scenario policy")
    corpus = recipe["corpus"]
    if not isinstance(corpus, dict) or set(corpus.get("splits", {})) != set(SPLITS):
        raise FixtureError("corpus split fields differ")
    ranges = corpus["splits"]
    cursor = 0
    for split in SPLITS:
        spec = ranges[split]
        if spec["start"] != cursor or spec["end"] <= spec["start"]:
            raise FixtureError("corpus splits overlap or have a gap")
        cursor = spec["end"]
    if cursor != corpus["document_count"]:
        raise FixtureError("corpus splits do not cover the corpus")
    query_ids: set[str] = set()
    query_texts: set[str] = set()
    answer_events: dict[str, set[str]] = {split: set() for split in SPLITS}
    if not isinstance(recipe["query_splits"], dict) or set(recipe["query_splits"]) != set(SPLITS):
        raise FixtureError("query split fields differ")
    for split in SPLITS:
        for case in recipe["query_splits"][split]:
            if case.get("split") != split or case.get("case_id") in query_ids:
                raise FixtureError("query splits are not disjoint")
            query = case.get("arguments", {}).get("query")
            if not isinstance(query, str) or query in query_texts:
                raise FixtureError("query text is reused across fixture splits")
            query_ids.add(case["case_id"])
            query_texts.add(query)
            _validate_oracle(case.get("oracle"), case["case_id"])
            answer_events[split].update(
                identity["event_id"] for identity in case["oracle"]["ordered_identities"]
            )
    if any(answer_events[left] & answer_events[right] for index, left in enumerate(SPLITS) for right in SPLITS[index + 1 :]):
        raise FixtureError("query answer splits are not disjoint")
    event_ids: set[str] = set()
    raw_event_ids: set[str] = set()
    probe_terms: set[str] = set()
    if not isinstance(recipe["event_splits"], dict) or set(recipe["event_splits"]) != set(SPLITS):
        raise FixtureError("event split fields differ")
    for split in SPLITS:
        for event in recipe["event_splits"][split]:
            if event.get("split") != split or event.get("case_id") in event_ids:
                raise FixtureError("event splits are not disjoint")
            event_ids.add(event["case_id"])
            raw_event = event.get("raw_event_uid")
            if not isinstance(raw_event, str) or raw_event in raw_event_ids:
                raise FixtureError("ETD event identities are not disjoint")
            raw_event_ids.add(raw_event)
            normalized_event = event.get("normalized_event_uid")
            if (
                not isinstance(normalized_event, str)
                or event.get("expected_ack_digest") != _ack_digest(normalized_event)
            ):
                raise FixtureError("ETD acknowledgement digest differs")
            _validate_oracle(event.get("oracle"), event["case_id"])
            for term in event.get("probe_terms", []):
                if not isinstance(term, str) or not term or term in probe_terms:
                    raise FixtureError("ETD probe terms must be globally one-use")
                probe_terms.add(term)
    schedules = recipe["schedule_templates"]
    required_probes = schedules["etd"]["visibility_timeout_ns"] // schedules["etd"]["poll_interval_ns"] + 1
    if any(len(event["probe_terms"]) < required_probes for split in SPLITS for event in recipe["event_splits"][split]):
        raise FixtureError("ETD probe bank cannot cover the visibility window without reuse")
    if recipe != _make_recipe(recipe["profile"]):
        raise FixtureError("recipe differs from the frozen canonical regeneration")


def required_split_usage(recipe: Mapping[str, Any], mode: str = "comparison") -> tuple[tuple[str, str], ...]:
    validate_recipe(recipe)
    if mode == "comparison":
        return tuple((scenario, split) for scenario in SCENARIOS for split in recipe["split_matrix"][scenario])
    if mode == "baseline":
        return (
            ("qps", "research"),
            ("ttr", "research"),
            ("etd_idle", "research"),
            ("etd_loaded", "research"),
            ("mixed", "stress"),
        )
    if mode == "research":
        return tuple((scenario, "research") for scenario in SCENARIOS if scenario != "mixed")
    if mode == "holdout":
        return tuple((scenario, "holdout") for scenario in SCENARIOS if scenario != "mixed")
    raise FixtureError(f"unsupported split usage mode {mode!r}")


def validate_split_usage(
    recipe: Mapping[str, Any], usage: Iterable[tuple[str, str]], mode: str = "comparison"
) -> None:
    observed = list(usage)
    if any(not isinstance(item, tuple) or len(item) != 2 for item in observed):
        raise FixtureError("split usage entries must be (scenario, split) tuples")
    if len(observed) != len(set(observed)):
        raise FixtureError("split usage contains duplicate scenario/split entries")
    expected = required_split_usage(recipe, mode)
    if set(observed) != set(expected):
        missing = sorted(set(expected) - set(observed))
        extra = sorted(set(observed) - set(expected))
        raise FixtureError(f"split usage differs: missing={missing!r} extra={extra!r}")


def seed_manifest(recipe: Mapping[str, Any]) -> dict[str, Any]:
    """Return the answer-free document allowed to cross into the server environment."""

    validate_recipe(recipe)
    return {
        "recipe_version": recipe["recipe_version"],
        "profile": recipe["profile"],
        "seed": copy.deepcopy(recipe["seed"]),
        "corpus": copy.deepcopy(recipe["corpus"]),
        "corpus_sha256": recipe["fingerprints"]["corpus_sha256"],
    }


def _clickhouse_array(values: Sequence[str]) -> str:
    return "[" + ",".join("'" + value.replace("'", "''") + "'" for value in values) + "]"


def _split_value_expression(corpus: Mapping[str, Any], field: str, *, quote: bool = False) -> str:
    specs = corpus["splits"]
    values: list[str] = []
    for split in SPLITS:
        value: Any = split if field == "name" else specs[split][field]
        values.append(f"'{value}'" if quote or isinstance(value, str) else str(value))
    return f"multiIf(number < {specs['research']['end']}, {values[0]}, number < {specs['holdout']['end']}, {values[1]}, {values[2]})"


def seed_search_sql(target: FreshSeedTarget, recipe: Mapping[str, Any]) -> str:
    """Build the sole INSERT for a proven empty, fresh, sandbox-owned target."""

    if not isinstance(target, FreshSeedTarget):
        raise FixtureError("seed_search_sql requires FreshSeedTarget evidence")
    target.validate()
    manifest = seed_manifest(recipe)
    corpus = manifest["corpus"]
    split_name = _split_value_expression(corpus, "name", quote=True)
    split_start = _split_value_expression(corpus, "start")
    query_count = _split_value_expression(corpus, "query_count")
    tie_modulus = _split_value_expression(corpus, "tie_modulus")
    role_modulus = _split_value_expression(corpus, "role_modulus")
    age_modulus = _split_value_expression(corpus, "age_modulus")
    scope_modulus = _split_value_expression(corpus, "scope_modulus")
    actor_roles = _clickhouse_array(ACTOR_ROLES)
    event_classes = _clickhouse_array(EVENT_CLASSES)
    payload_types = _clickhouse_array(PAYLOAD_TYPES)
    event_types = _clickhouse_array(EVENT_TYPES)
    age_days = ",".join(str(value) for value in AGE_DAYS)
    payload_bytes = ",".join(str(value) for value in PAYLOAD_BYTES)
    documents = corpus["document_count"]
    database = target.database
    return f"""
INSERT INTO {database}.events
(
  event_version, ingested_at, event_uid, origin_event_id, session_id, session_date,
  source_name, harness, inference_provider, endpoint_kind, source_file, source_generation,
  source_line_no, source_offset, source_ref, record_ts, event_kind, payload_type,
  actor_kind, tool_name, tool_phase, text_content, payload_json, token_usage_json
)
WITH
  {split_name} AS fixture_split,
  {split_start} AS fixture_split_start,
  {query_count} AS fixture_query_count,
  {tie_modulus} AS fixture_tie_modulus,
  {role_modulus} AS fixture_role_modulus,
  {age_modulus} AS fixture_age_modulus,
  {scope_modulus} AS fixture_scope_modulus,
  number - fixture_split_start AS fixture_local,
  1 + modulo(intDiv(fixture_local, fixture_query_count), 4) AS fixture_rank_tf,
  arrayElement([{age_days}], modulo(number, {len(AGE_DAYS)}) + 1) AS fixture_age_days,
  toDateTime64('{corpus['base_timestamp'].replace('T', ' ').replace('Z', '')}', 3, 'UTC') - toIntervalDay(fixture_age_days) AS fixture_event_time,
  intDiv(number, {SESSION_CYCLE}) * {len(SESSION_LENGTHS)} + multiIf(modulo(number, {SESSION_CYCLE}) < 1, 0, modulo(number, {SESSION_CYCLE}) < 5, 1, modulo(number, {SESSION_CYCLE}) < 21, 2, 3) AS fixture_session_number,
  arrayElement([{payload_bytes}], modulo(intDiv(fixture_local, fixture_query_count), {len(PAYLOAD_BYTES)}) + 1) AS fixture_payload_bytes
SELECT
  1,
  toDateTime64('{corpus['base_timestamp'].replace('T', ' ').replace('Z', '')}', 3, 'UTC'),
  concat('perf-event-', leftPad(toString(number), 8, '0')),
  '',
  concat('perf-session-', leftPad(toString(fixture_session_number), 8, '0')),
  toDate(fixture_event_time),
  '{corpus['source_name']}',
  'codex',
  'synthetic',
  'generation',
  '{corpus['source_file']}',
  1,
  number + 1,
  number,
  concat('owned:', toString(number)),
  formatDateTime(fixture_event_time, '%Y-%m-%dT%H:%i:%S.000Z', 'UTC'),
  arrayElement({event_classes}, modulo(number, {len(EVENT_TYPES)}) + 1),
  arrayElement({payload_types}, modulo(number, {len(EVENT_TYPES)}) + 1),
  arrayElement({actor_roles}, modulo(number, {len(EVENT_TYPES)}) + 1),
  '',
  'completed',
  concat(
    'deterministic performance fixture ',
    'perf', fixture_split, 'rare', leftPad(toString(fixture_local), 6, '0'), ' ',
    'perf', fixture_split, 'selectivecase', leftPad(toString(modulo(fixture_local, fixture_query_count)), 4, '0'), ' ',
    'perf', fixture_split, 'scopecase', leftPad(toString(modulo(fixture_local, fixture_scope_modulus)), 4, '0'), ' ',
    'perf', fixture_split, 'rolecase', leftPad(toString(modulo(fixture_local, fixture_role_modulus)), 4, '0'), ' ',
    'perf', fixture_split, 'agecase', leftPad(toString(modulo(fixture_local, fixture_age_modulus)), 4, '0'), ' ',
    'perf', fixture_split, 'tiecase', leftPad(toString(modulo(fixture_local, fixture_tie_modulus)), 4, '0'), ' ',
    'perf', fixture_split, 'hydratecase', leftPad(toString(modulo(fixture_local, fixture_query_count)), 4, '0'), ' ',
    repeat(concat('perf', fixture_split, 'rankcase', leftPad(toString(modulo(fixture_local, fixture_query_count)), 4, '0'), ' '), fixture_rank_tf),
    repeat('rankfiller ', 4 - fixture_rank_tf),
    'eventtype', replaceAll(arrayElement({event_types}, modulo(number, {len(EVENT_TYPES)}) + 1), '_', ''), ' ',
    'actorrole', arrayElement({actor_roles}, modulo(number, {len(EVENT_TYPES)}) + 1), ' ',
    'ageband', toString(modulo(number, {len(AGE_DAYS)})), ' ',
    'payload'
  ),
  concat('{{"fixture_blob":"', repeat('x', fixture_payload_bytes), '"}}'),
  '{{}}'
FROM numbers({documents})
""".strip()


def codex_event_lines(event: Mapping[str, Any]) -> bytes:
    """Encode one ETD source fixture without exposing its expected public answer."""

    required = {"raw_event_uid", "raw_session_id", "recorded_at", "marker", "probe_terms"}
    if not required.issubset(event):
        raise FixtureError("ETD event fixture is incomplete")
    session = {
        "timestamp": event["recorded_at"],
        "type": "session_meta",
        "payload": {
            "id": event["raw_session_id"],
            "cwd": "/owned/performance",
            "cli_version": "benchmark",
        },
    }
    message = {
        "timestamp": event["recorded_at"],
        "type": "response_item",
        "payload": {
            "type": "message",
            "id": event["raw_event_uid"],
            "role": "assistant",
            "phase": "completed",
            "content": [
                {
                    "type": "output_text",
                    "text": " ".join([event["marker"], *event["probe_terms"]]),
                }
            ],
        },
    }
    return (
        json.dumps(session, separators=(",", ":"), sort_keys=True)
        + "\n"
        + json.dumps(message, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def _extract_result_identities(structured_content: Mapping[str, Any]) -> tuple[list[dict[str, str]], bool]:
    if not isinstance(structured_content, dict):
        raise FixtureError("search structuredContent must be an object")
    if structured_content.get("schema_version") != SEARCH_SCHEMA_VERSION:
        raise FixtureError("search structuredContent schema_version differs")
    if structured_content.get("tool") != SEARCH_TOOL:
        raise FixtureError("search structuredContent tool differs")
    data = structured_content.get("data")
    if not isinstance(data, dict):
        raise FixtureError("search structuredContent data is absent")
    result_count = data.get("result_count")
    truncated = data.get("truncated")
    results = data.get("results")
    if isinstance(result_count, bool) or not isinstance(result_count, int) or result_count < 0:
        raise FixtureError("search result_count is invalid")
    if not isinstance(truncated, bool) or not isinstance(results, list):
        raise FixtureError("search results/truncated fields are invalid")
    if result_count != len(results):
        raise FixtureError("search result_count differs from result array length")
    identities: list[dict[str, str]] = []
    for hit in results:
        if not isinstance(hit, dict):
            raise FixtureError("search result is not an object")
        event = hit.get("event")
        session = hit.get("session")
        if not isinstance(event, dict) or not isinstance(session, dict):
            raise FixtureError("search result omitted event/session identity")
        event_id_value = event.get("id")
        session_id_value = session.get("id")
        if not isinstance(event_id_value, str) or not isinstance(session_id_value, str):
            raise FixtureError("search result identity is invalid")
        identities.append({"event_id": event_id_value, "session_id": session_id_value})
    keys = [(item["event_id"], item["session_id"]) for item in identities]
    if len(keys) != len(set(keys)):
        raise FixtureError("search results contain duplicate identities")
    return identities, truncated


def _validate_result(structured_content: Mapping[str, Any], oracle: Mapping[str, Any], case_id: str) -> None:
    _validate_oracle(oracle, case_id)
    identities, truncated = _extract_result_identities(structured_content)
    if len(identities) != oracle["count"]:
        raise FixtureError(f"exact oracle count mismatch for {case_id}")
    if truncated is not oracle["truncated"]:
        raise FixtureError(f"exact oracle truncation mismatch for {case_id}")
    if identities != oracle["ordered_identities"]:
        raise FixtureError(f"exact oracle order/identity mismatch for {case_id}")
    if _ordered_digest(identities) != oracle["ordered_sha256"]:
        raise FixtureError(f"exact ordered digest mismatch for {case_id}")
    identity_set = sorted(identities, key=lambda item: (item["event_id"], item["session_id"]))
    if sha256_json(identity_set) != oracle["set_sha256"]:
        raise FixtureError(f"exact set digest mismatch for {case_id}")


def validate_query_result(structured_content: Mapping[str, Any], case: Mapping[str, Any]) -> None:
    """Validate a production search response against a seed-owned exact oracle."""

    case_id = case.get("case_id")
    if not isinstance(case_id, str) or not isinstance(case.get("oracle"), dict):
        raise FixtureError("query case is incomplete")
    _validate_result(structured_content, case["oracle"], case_id)


def validate_event_result(structured_content: Mapping[str, Any], event: Mapping[str, Any]) -> None:
    """Validate exact visibility of one published ETD event."""

    case_id = event.get("case_id")
    if not isinstance(case_id, str) or not isinstance(event.get("oracle"), dict):
        raise FixtureError("event case is incomplete")
    _validate_result(structured_content, event["oracle"], case_id)


def _rate_fraction(rate_qps: Any) -> Fraction:
    if isinstance(rate_qps, bool):
        raise FixtureError("query rate must be positive")
    try:
        rate = Fraction(str(rate_qps))
    except (ValueError, ZeroDivisionError) as exc:
        raise FixtureError("query rate must be a finite positive number") from exc
    if rate <= 0:
        raise FixtureError("query rate must be positive")
    return rate


def open_query_schedule(
    recipe: Mapping[str, Any],
    split: str,
    rate_qps: Any,
    *,
    stream: str = "qps",
) -> list[dict[str, Any]]:
    """Expand an open-arrival schedule independent of response completion."""

    validate_recipe(recipe)
    if split not in SPLITS or stream not in {"qps", "mixed"}:
        raise FixtureError("unsupported query schedule split or stream")
    rate = _rate_fraction(rate_qps)
    duration_ns = recipe["schedule_templates"][stream]["duration_ns"]
    case_order = (
        recipe["schedule_templates"]["qps"]["case_order_by_split"][split]
        if stream == "qps"
        else [case["case_id"] for case in recipe["query_splits"][split]]
    )
    operations: list[dict[str, Any]] = []
    sequence = 0
    while True:
        offset_ns = sequence * 1_000_000_000 * rate.denominator // rate.numerator
        if offset_ns >= duration_ns:
            break
        operations.append(
            {
                "kind": "query",
                "sequence": sequence,
                "scheduled_offset_ns": offset_ns,
                "case_id": case_order[sequence % len(case_order)],
            }
        )
        sequence += 1
    return operations


def open_event_schedule(recipe: Mapping[str, Any], split: str) -> list[dict[str, Any]]:
    validate_recipe(recipe)
    if split not in SPLITS:
        raise FixtureError("unsupported event schedule split")
    return copy.deepcopy(recipe["schedule_templates"]["etd"]["event_stream_by_split"][split])


def mixed_control_schedules(recipe: Mapping[str, Any], query_rate_qps: Any) -> dict[str, Any]:
    """Emit controls and combined streams from the same immutable schedules."""

    validate_recipe(recipe)
    query_stream = open_query_schedule(recipe, "stress", query_rate_qps, stream="mixed")
    event_stream = open_event_schedule(recipe, "stress")
    return {
        "query_only": {"queries": copy.deepcopy(query_stream), "events": []},
        "ingest_only": {"queries": [], "events": copy.deepcopy(event_stream)},
        "combined": {
            "queries": copy.deepcopy(query_stream),
            "events": copy.deepcopy(event_stream),
        },
        "schedule_sha256": sha256_json({"queries": query_stream, "events": event_stream}),
    }
