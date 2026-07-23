#!/usr/bin/env python3
from __future__ import annotations

import copy
import json
import os
import threading
import tempfile
import unittest
from unittest import mock
from pathlib import Path
import sys
from typing import Any, Mapping

BENCH = Path(__file__).resolve().parents[1]
if str(BENCH) not in sys.path:
    sys.path.insert(0, str(BENCH))

import native_central_burst as burst
import performance_suite
from performance_fixtures import SEARCH_SCHEMA_VERSION, SEARCH_TOOL, build_recipe
from performance_scenarios import AdmissionRejected, RequestTimeout
from performance_protocol import sha256_json


def structured_result(case: Mapping[str, Any]) -> dict[str, Any]:
    oracle = case["oracle"]
    return {
        "schema_version": SEARCH_SCHEMA_VERSION,
        "tool": SEARCH_TOOL,
        "data": {
            "result_count": oracle["count"],
            "truncated": oracle["truncated"],
            "results": [
                {
                    "event": {"id": identity["event_id"]},
                    "session": {"id": identity["session_id"]},
                }
                for identity in oracle["ordered_identities"]
            ],
        },
    }


def fixture_database_evidence(recipe: Mapping[str, Any]) -> dict[str, Any]:
    documents = recipe["corpus"]["document_count"]
    return {
        "backend_name": "default",
        "database": "moraine_native_test",
        "endpoint_scope": "explicit_loopback_native",
        "route_policy": "default_only_no_non_default_routes_or_repo_markers",
        "repo_marker_count": 0,
        "server_system": "Darwin",
        "expected_document_count": documents,
        "events_count": documents,
        "events_fixture_count": documents,
        "events_unique_ids": documents,
        "event_sessions": 17,
        "event_min_uid": "perf-event-00000000",
        "event_max_uid": f"perf-event-{documents - 1:08d}",
        "documents_count": documents,
        "documents_fixture_count": documents,
        "documents_unique_ids": documents,
        "document_min_uid": "perf-event-00000000",
        "document_max_uid": f"perf-event-{documents - 1:08d}",
        "posting_documents": documents,
        "corpus_documents": documents,
        "projected_sessions": 17,
        "projected_session_events": documents,
        "projected_events": documents,
        "dirty_session_count": 0,
        "projection_ready_rows": 1,
        "publication_head_count": 1,
        "active_checkpoint_count": 1,
        "publication_readiness_count": 1,
        "idle_append_control_count": 1,
        "pass": True,
    }


class CoordinatedState:
    def __init__(self, expected: int) -> None:
        self.expected = expected
        self.active = 0
        self.peak = 0
        self.released = False
        self.condition = threading.Condition()


class CoordinatedClient:
    def __init__(self, state: CoordinatedState) -> None:
        self.state = state

    def search(self, case: Mapping[str, Any], timeout_s: float) -> Mapping[str, Any]:
        with self.state.condition:
            self.state.active += 1
            self.state.peak = max(self.state.peak, self.state.active)
            if self.state.active == self.state.expected:
                self.state.released = True
                self.state.condition.notify_all()
            elif not self.state.condition.wait_for(lambda: self.state.released, timeout=timeout_s):
                raise RequestTimeout("test coordination timed out")
        try:
            return structured_result(case)
        finally:
            with self.state.condition:
                self.state.active -= 1


class RaisingClient:
    def __init__(self, error: Exception | None) -> None:
        self.error = error

    def search(self, case: Mapping[str, Any], _timeout_s: float) -> Mapping[str, Any]:
        if self.error is not None:
            raise self.error
        return structured_result(case)


class CountingClient(RaisingClient):
    def __init__(self) -> None:
        super().__init__(None)
        self.calls = 0

    def search(self, case: Mapping[str, Any], timeout_s: float) -> Mapping[str, Any]:
        self.calls += 1
        return super().search(case, timeout_s)


class FakeLifecycle:
    def __init__(self, concurrency: int) -> None:
        self.concurrency = concurrency
        self.started = False
        self.closed = False
        self.tool_surfaces = [("search_sessions", "open", "list_sessions", "file_attention")]

    def start(self) -> None:
        self.started = True

    def open_clients(self, count: int) -> list[RaisingClient]:
        if not self.started or count != self.concurrency:
            raise AssertionError("fake lifecycle used out of order")
        return [RaisingClient(None) for _ in range(count)]

    def close(self) -> None:
        self.closed = True

    def cleanup_evidence(self) -> Mapping[str, Any]:
        if not self.closed:
            raise AssertionError("cleanup evidence requested before close")
        return {
            "central_terminated": True,
            "routes_closed": True,
            "socket_removed": True,
        }


class CountingLifecycle(FakeLifecycle):
    def __init__(self, concurrency: int) -> None:
        super().__init__(concurrency)
        self.clients: list[CountingClient] = []

    def open_clients(self, count: int) -> list[CountingClient]:
        if not self.started or count != self.concurrency:
            raise AssertionError("fake lifecycle used out of order")
        self.clients = [CountingClient() for _ in range(count)]
        return self.clients


class NativeBurstTests(unittest.TestCase):
    def setUp(self) -> None:
        self.case = build_recipe("smoke")["query_splits"]["research"][0]

    def test_barrier_releases_full_four_way_burst_and_records_external_time(self) -> None:
        state = CoordinatedState(4)
        samples, bursts = burst.run_synchronized_bursts(
            [CoordinatedClient(state) for _ in range(4)],
            [self.case],
            concurrency=4,
            bursts_per_case=1,
            timeout_s=1.0,
        )

        self.assertEqual(state.peak, 4)
        self.assertEqual(len(samples), 4)
        self.assertEqual(len({sample["released_ns"] for sample in samples}), 1)
        self.assertTrue(all(sample["started_ns"] >= sample["released_ns"] for sample in samples))
        self.assertTrue(all(sample["outcome"] == "ok" for sample in samples))
        self.assertEqual(bursts[0]["ok_count"], 4)
        self.assertEqual(bursts[0]["request_count"], 4)

    def test_raw_samples_keep_timeout_admission_and_semantic_failures(self) -> None:
        bad_case = dict(self.case)
        clients = [
            RaisingClient(None),
            RaisingClient(RequestTimeout("late")),
            RaisingClient(AdmissionRejected("busy")),
            RaisingClient(None),
        ]
        # The final worker receives an exact response; mutate the shared oracle
        # only for that worker by returning an invalid response from a tiny shim.
        class SemanticFailureClient:
            def search(self, _case: Mapping[str, Any], _timeout_s: float) -> Mapping[str, Any]:
                return {
                    "schema_version": SEARCH_SCHEMA_VERSION,
                    "tool": SEARCH_TOOL,
                    "data": {"result_count": 0, "truncated": False, "results": []},
                }

        clients[-1] = SemanticFailureClient()  # type: ignore[assignment]
        samples, bursts = burst.run_synchronized_bursts(
            clients,
            [bad_case],
            concurrency=4,
            bursts_per_case=1,
            timeout_s=1.0,
        )

        self.assertEqual(
            [sample["outcome"] for sample in samples],
            ["ok", "timeout", "admission_rejected", "semantic_mismatch"],
        )
        self.assertTrue(samples[1]["right_censored"])
        self.assertFalse(samples[2]["right_censored"])
        self.assertEqual(bursts[0]["timeout_count"], 1)
        self.assertEqual(bursts[0]["error_count"], 2)

    def test_matrix_closes_owned_lifecycle_when_measurement_raises(self) -> None:
        lifecycles: list[FakeLifecycle] = []

        def lifecycle_factory(concurrency: int) -> FakeLifecycle:
            lifecycle = FakeLifecycle(concurrency)
            lifecycles.append(lifecycle)
            return lifecycle

        def fail_runner(*_args: Any, **_kwargs: Any) -> Any:
            raise RuntimeError("measurement failed")

        with self.assertRaisesRegex(RuntimeError, "measurement failed"):
            burst.execute_native_matrix(
                [self.case],
                bursts_per_case=1,
                timeout_s=1.0,
                lifecycle_factory=lifecycle_factory,
                burst_runner=fail_runner,
            )

        self.assertEqual(len(lifecycles), 1)
        self.assertTrue(lifecycles[0].closed)

    def test_complete_raw_artifact_validates_and_uses_canonical_validate_command(self) -> None:
        lifecycles: list[FakeLifecycle] = []
        recipe, selected_cases = burst.select_fixture_cases(
            "smoke", "research", burst.COLD_SLO_MODES, 1
        )

        def lifecycle_factory(concurrency: int) -> FakeLifecycle:
            lifecycle = FakeLifecycle(concurrency)
            lifecycles.append(lifecycle)
            return lifecycle

        cold_samples, cold_bursts, cold_cleanups, cold_surfaces = burst.execute_cold_matrix(
            selected_cases,
            cold_repetitions=2,
            timeout_s=1.0,
            lifecycle_factory=lifecycle_factory,
        )
        steady_samples, steady_bursts, steady_cleanups, steady_surfaces = (
            burst.execute_native_matrix(
                selected_cases,
                bursts_per_case=1,
                timeout_s=1.0,
                lifecycle_factory=lifecycle_factory,
            )
        )
        samples = cold_samples + steady_samples
        bursts = cold_bursts + steady_bursts
        cleanups = cold_cleanups + steady_cleanups
        surfaces = cold_surfaces | steady_surfaces
        summary = burst._summary(samples, bursts)
        gates = burst._latency_gates(
            summary,
            min_cold_samples=1,
            warm_p95_limit_ms=1_000.0,
            cold_p95_limit_ms=1_000.0,
            max_latency_ms=1_000.0,
        )
        digest = "sha256:" + "0" * 64
        fixture = fixture_database_evidence(recipe)
        document = {
            "document_type": burst.DOCUMENT_TYPE,
            "schema_version": burst.SCHEMA_VERSION,
            "status": "pass",
            "run": {
                "clock": "time.monotonic_ns",
                "platform": "darwin",
                "machine": "arm64",
                "binary_architectures": ["arm64"],
                "mcp_binary_sha256": digest,
                "config_sha256": digest,
                "fixture_sha256": recipe["fixture_sha256"],
                "profile": "smoke",
                "split": "research",
                "modes": list(burst.COLD_SLO_MODES),
                "cases_per_mode": 1,
                "bursts_per_case": 1,
                "cold_repetitions": 2,
                "minimum_cold_samples": 1,
                "warm_p95_limit_ms": 1_000.0,
                "cold_p95_limit_ms": 1_000.0,
                "max_latency_ms": 1_000.0,
                "timeout_seconds": 1.0,
                "steady_state_warmup_requests_per_concurrency": len(selected_cases),
                "collect_query_log": False,
                "concurrencies": list(burst.CONCURRENCIES),
            },
            "tool_surface": {
                "required_tool": "search_sessions",
                "forbidden_internal_tools": list(burst.FORBIDDEN_INTERNAL_TOOLS),
                "observed": [list(surface) for surface in sorted(surfaces)],
                "search_mcp_events_exposed": False,
                "consistent": True,
                "pass": True,
            },
            "input_identity": {
                "mcp_binary": {
                    "pre_sha256": digest,
                    "frozen_sha256": digest,
                    "post_sha256": digest,
                },
                "config": {
                    "pre_sha256": digest,
                    "frozen_sha256": digest,
                    "post_sha256": digest,
                },
                "frozen_execution": True,
                "pass": True,
            },
            "fixture_database": {
                "before": fixture,
                "after": copy.deepcopy(fixture),
                "stable": True,
                "pass": True,
            },
            "cases": [
                {
                    "case_id": case["case_id"],
                    "mode": case["mode"],
                    "arguments_sha256": sha256_json(case["arguments"]),
                    "oracle_sha256": sha256_json(case["oracle"]),
                }
                for case in selected_cases
            ],
            "lifecycles": cleanups,
            "bursts": bursts,
            "samples": samples,
            "summary": summary,
            "gates": gates,
            "query_log": burst._query_log_not_requested(),
        }

        self.assertIs(burst.validate_native_burst_artifact(document), document)
        self.assertTrue(all(lifecycle.closed for lifecycle in lifecycles))
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "native.json"
            burst.write_native_burst_artifact(path, document)
            performance_suite.validate_path(path)
        document["input_identity"]["config"]["post_sha256"] = "sha256:" + "1" * 64
        with self.assertRaisesRegex(burst.NativeBurstFailure, "identity is unstable"):
            burst.validate_native_burst_artifact(document)
        document["input_identity"]["config"]["post_sha256"] = digest
        document["cases"][0]["oracle_sha256"] = digest
        with self.assertRaisesRegex(burst.NativeBurstFailure, "fixture or case identity"):
            burst.validate_native_burst_artifact(document)
        document["cases"][0]["oracle_sha256"] = sha256_json(selected_cases[0]["oracle"])
        document["lifecycles"][1]["socket_removed"] = False
        with self.assertRaisesRegex(burst.NativeBurstFailure, "cleanup evidence"):
            burst.validate_native_burst_artifact(document)

    def test_cold_matrix_restarts_each_repetition_and_rotates_cases(self) -> None:
        lifecycles: list[FakeLifecycle] = []
        cases = build_recipe("smoke")["query_splits"]["research"][:2]

        def lifecycle_factory(concurrency: int) -> FakeLifecycle:
            lifecycle = FakeLifecycle(concurrency)
            lifecycles.append(lifecycle)
            return lifecycle

        samples, bursts, cleanups, _surfaces = burst.execute_cold_matrix(
            cases,
            cold_repetitions=3,
            timeout_s=1.0,
            lifecycle_factory=lifecycle_factory,
        )

        self.assertEqual(len(lifecycles), 3 * len(burst.CONCURRENCIES))
        self.assertTrue(all(lifecycle.closed for lifecycle in lifecycles))
        self.assertEqual(
            [lifecycle.concurrency for lifecycle in lifecycles[:6]],
            [1, 4, 8, 8, 4, 1],
        )
        self.assertTrue(all(sample["phase"] == burst.COLD_PHASE for sample in samples))
        self.assertEqual(
            {item["lifecycle_iteration"] for item in cleanups},
            {0, 1, 2},
        )
        c1_cases = [
            item["case_id"]
            for item in bursts
            if item["concurrency"] == 1
        ]
        self.assertEqual(
            c1_cases,
            [cases[0]["case_id"], cases[1]["case_id"], cases[0]["case_id"]],
        )

    def test_steady_matrix_warms_each_case_outside_raw_samples(self) -> None:
        lifecycles: list[CountingLifecycle] = []

        def lifecycle_factory(concurrency: int) -> CountingLifecycle:
            lifecycle = CountingLifecycle(concurrency)
            lifecycles.append(lifecycle)
            return lifecycle

        samples, _bursts, _cleanups, _surfaces = burst.execute_native_matrix(
            [self.case],
            bursts_per_case=1,
            timeout_s=1.0,
            lifecycle_factory=lifecycle_factory,
        )

        self.assertTrue(all(sample["phase"] == burst.STEADY_PHASE for sample in samples))
        for lifecycle in lifecycles:
            self.assertEqual(
                sum(client.calls for client in lifecycle.clients),
                lifecycle.concurrency + 1,
            )

    def test_latency_gates_require_cold_sample_floor_and_both_phase_p95s(self) -> None:
        _recipe, cases = burst.select_fixture_cases(
            "smoke", "research", burst.COLD_SLO_MODES, 1
        )

        def lifecycle_factory(concurrency: int) -> FakeLifecycle:
            return FakeLifecycle(concurrency)

        cold_samples, cold_bursts, _cleanups, _surfaces = burst.execute_cold_matrix(
            cases,
            cold_repetitions=2,
            timeout_s=1.0,
            lifecycle_factory=lifecycle_factory,
        )
        steady_samples, steady_bursts, _cleanups, _surfaces = burst.execute_native_matrix(
            cases,
            bursts_per_case=1,
            timeout_s=1.0,
            lifecycle_factory=lifecycle_factory,
        )
        summary = burst._summary(
            cold_samples + steady_samples,
            cold_bursts + steady_bursts,
        )

        passing = burst._latency_gates(
            summary,
            min_cold_samples=1,
            warm_p95_limit_ms=1_000.0,
            cold_p95_limit_ms=1_000.0,
            max_latency_ms=1_000.0,
        )
        insufficient = burst._latency_gates(
            summary,
            min_cold_samples=9,
            warm_p95_limit_ms=1_000.0,
            cold_p95_limit_ms=1_000.0,
            max_latency_ms=1_000.0,
        )
        too_slow = burst._latency_gates(
            summary,
            min_cold_samples=1,
            warm_p95_limit_ms=0.000001,
            cold_p95_limit_ms=0.000001,
            max_latency_ms=0.000001,
        )

        self.assertTrue(passing["pass"])
        self.assertTrue(
            passing["by_concurrency"]["1"]["cold_slo_by_mode"]["hydration"]["pass"]
        )
        self.assertFalse(insufficient["by_concurrency"]["1"]["cold_sample_count_pass"])
        self.assertFalse(insufficient["pass"])
        self.assertFalse(too_slow["pass"])

    def test_query_log_collector_aggregates_only_owned_candidate_and_detail_queries(self) -> None:
        endpoint = burst.ClickHouseEndpoint(
            url="http://127.0.0.1:8123",
            database="moraine",
            username="default",
            password="",
        )
        ownership = [
            {
                "phase": burst.COLD_PHASE,
                "concurrency": 1,
                "lifecycle_iteration": 0,
                "query_id_prefix": "moraine-search-sessions-42-",
                "measurement_started_at_us": None,
            },
            {
                "phase": burst.STEADY_PHASE,
                "concurrency": 1,
                "lifecycle_iteration": 0,
                "query_id_prefix": "moraine-search-sessions-43-",
                "measurement_started_at_us": 200,
            },
        ]
        def log_row(prefix: str, suffix: str, stage: str, event_time: int) -> str:
            read_rows = 10 if stage == "candidate" else 6 if stage == "detail" else 1
            return json.dumps(
                {
                    "query_id": f"{prefix}-{suffix}",
                    "stage": stage,
                    "type": "QueryFinish",
                    "event_time_us": str(event_time),
                    "query_duration_ms": 1,
                    "read_rows": str(read_rows),
                    "read_bytes": str(read_rows * 10),
                    "result_rows": "1",
                    "memory_usage": "100",
                },
                separators=(",", ":"),
            )

        business = ("candidate", "detail")
        rows = "\n".join(
            [
                *[
                    log_row("moraine-search-sessions-42", f"cold-{index}", stage, 150 + index)
                    for index, stage in enumerate(
                        (*burst.CONTROL_QUERY_STAGES, *business)
                    )
                ],
                *[
                    log_row("moraine-search-sessions-43", f"warm-{index}", stage, 170 + index)
                    for index, stage in enumerate(
                        (*burst.CONTROL_QUERY_STAGES, *business)
                    )
                ],
                *[
                    log_row("moraine-search-sessions-43", f"measured-{index}", stage, 210 + index)
                    for index, stage in enumerate(burst.CONTROL_QUERY_STAGES)
                ],
            ]
        )
        with mock.patch.object(
            burst,
            "_clickhouse_query",
            side_effect=["100", "300", "", rows],
        ) as query:
            collector = burst.QueryLogCollector(endpoint)
            evidence = collector.collect(
                ownership,
                steady_warmup_case_count=1,
                steady_measured_requests_per_client=1,
            )

        self.assertTrue(evidence["pass"])
        lifecycles = [
            {
                "phase": item["phase"],
                "concurrency": item["concurrency"],
                "lifecycle_iteration": item["lifecycle_iteration"],
            }
            for item in ownership
        ]
        self.assertTrue(
            burst._validate_query_log_evidence(
                evidence,
                requested=True,
                lifecycles=lifecycles,
                steady_warmup_case_count=1,
                steady_measured_requests_per_client=1,
            )
        )
        self.assertEqual(evidence["by_stage"]["candidate"]["read_rows"], 20)
        steady = next(
            item for item in evidence["by_lifecycle"] if item["phase"] == burst.STEADY_PHASE
        )
        self.assertEqual(steady["total"]["candidate_queries"], 1)
        self.assertEqual(steady["measured"]["candidate_queries"], 0)
        self.assertEqual(steady["total"]["publication_capture_queries"], 2)
        self.assertEqual(steady["measured"]["publication_capture_queries"], 1)
        aggregate_sql = query.call_args_list[-1].args[1]
        self.assertIn("startsWith(query_id, 'moraine-search-sessions-42-')", aggregate_sql)
        self.assertIn("projected_candidates AS", aggregate_sql)
        self.assertIn("candidate_heads AS", aggregate_sql)
        self.assertIn("moraine:publication_snapshot:capture", aggregate_sql)
        self.assertIn("moraine:append_fence:revalidate", aggregate_sql)
        self.assertNotIn("OR position(query, 'candidate_heads AS')", aggregate_sql)

        with mock.patch.object(
            burst,
            "_clickhouse_query",
            side_effect=["100", "300", "", "\n".join(rows.splitlines()[:-1])],
        ):
            incomplete = burst.QueryLogCollector(endpoint).collect(
                ownership,
                steady_warmup_case_count=1,
                steady_measured_requests_per_client=1,
            )
        self.assertFalse(incomplete["pass"])
        self.assertEqual(incomplete["coverage"]["passed_lifecycle_count"], 1)

        unknown = (
            '{"query_id":"moraine-search-sessions-42-extra","stage":"unknown",'
            '"type":"QueryFinish","event_time_us":"154","query_duration_ms":1,'
            '"read_rows":"1","read_bytes":"1","result_rows":"1","memory_usage":"1"}'
        )
        with mock.patch.object(
            burst,
            "_clickhouse_query",
            side_effect=["100", "300", "", rows + "\n" + unknown],
        ):
            unexpected = burst.QueryLogCollector(endpoint).collect(
                ownership,
                steady_warmup_case_count=1,
                steady_measured_requests_per_client=1,
            )
        self.assertFalse(unexpected["pass"])
        self.assertEqual(unexpected["by_stage"]["unknown"]["query_count"], 1)

    def test_clickhouse_evidence_bypasses_environment_proxy_and_rechecks_loopback(self) -> None:
        endpoint = burst.ClickHouseEndpoint(
            url="http://127.0.0.1:48123",
            database="moraine_native_test",
            username="fixture-user",
            password="fixture-secret",
        )
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = b"ok\n"
        opener = mock.Mock()
        opener.open.return_value = response
        with mock.patch.dict(
            os.environ,
            {
                "HTTP_PROXY": "http://proxy.invalid:8080",
                "HTTPS_PROXY": "http://proxy.invalid:8080",
            },
        ), mock.patch.object(
            burst.urllib.request,
            "build_opener",
            return_value=opener,
        ) as build_opener:
            self.assertEqual(burst._clickhouse_query(endpoint, "SELECT 1"), "ok")

        handler = build_opener.call_args.args[0]
        self.assertIsInstance(handler, burst.urllib.request.ProxyHandler)
        self.assertEqual(handler.proxies, {})
        request = opener.open.call_args.args[0]
        self.assertEqual(request.get_header("X-clickhouse-key"), "fixture-secret")
        self.assertEqual(request.full_url, endpoint.url)

        remote = burst.ClickHouseEndpoint(
            url="https://clickhouse.example:8443",
            database="moraine_native_test",
            username="fixture-user",
            password="fixture-secret",
        )
        with self.assertRaisesRegex(burst.NativeBurstFailure, "loopback"):
            burst._clickhouse_query(remote, "SELECT 1")

    def test_fixture_selection_is_small_deterministic_and_mode_complete(self) -> None:
        recipe, cases = burst.select_fixture_cases(
            "smoke",
            "research",
            burst.DEFAULT_MODES,
            1,
        )
        self.assertEqual(recipe["profile"], "smoke")
        self.assertEqual([case["mode"] for case in cases], list(burst.DEFAULT_MODES))
        with self.assertRaisesRegex(burst.NativeBurstFailure, "invalid or duplicated"):
            burst.select_fixture_cases("smoke", "research", ("rare", "rare"), 1)

    def test_tool_surface_is_unordered_extensible_and_rejects_internal_search(self) -> None:
        self.assertEqual(
            burst._validated_tool_surface(
                ["future_public_tool", "search_sessions", "open"]
            ),
            ("future_public_tool", "open", "search_sessions"),
        )
        with self.assertRaisesRegex(burst.NativeBurstFailure, "omitted search_sessions"):
            burst._validated_tool_surface(["open", "list_sessions"])
        with self.assertRaisesRegex(burst.NativeBurstFailure, "internal retrieval"):
            burst._validated_tool_surface(["search_sessions", "search_mcp_events"])

    def test_backend_selection_and_fixture_preflight_fail_closed(self) -> None:
        recipe = build_recipe("smoke")
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            route = root / "route"
            route.mkdir()
            config = root / "moraine.toml"
            config.write_text(
                "\n".join(
                    [
                        "[clickhouse]",
                        'url = "http://127.0.0.1:48123"',
                        'database = "moraine_native_test"',
                        'username = "default"',
                        'password = ""',
                    ]
                ),
                encoding="utf-8",
            )
            selection = burst._resolve_backend_selection(config, route)
            self.assertEqual(selection.backend_name, "default")

            full = fixture_database_evidence(recipe)
            row = {
                key: value
                for key, value in full.items()
                if key
                in {
                    "events_count",
                    "events_fixture_count",
                    "events_unique_ids",
                    "event_sessions",
                    "event_min_uid",
                    "event_max_uid",
                    "documents_count",
                    "documents_fixture_count",
                    "documents_unique_ids",
                    "document_min_uid",
                    "document_max_uid",
                    "posting_documents",
                    "corpus_documents",
                    "projected_sessions",
                    "projected_session_events",
                    "projected_events",
                    "dirty_session_count",
                    "projection_ready_rows",
                    "publication_head_count",
                    "active_checkpoint_count",
                    "publication_readiness_count",
                    "idle_append_control_count",
                }
            }
            with mock.patch.object(
                burst,
                "_clickhouse_query",
                side_effect=["Darwin", json.dumps(row)],
            ) as query:
                evidence = burst._fixture_database_evidence(selection, recipe)
            self.assertTrue(evidence["pass"])
            fixture_sql = query.call_args_list[-1].args[1]
            self.assertIn("mcp_open_projection_state", fixture_sql)
            self.assertIn("mcp_open_dirty_sessions", fixture_sql)
            self.assertIn("v_current_published_source_generations", fixture_sql)
            self.assertIn("v_current_ingest_checkpoint_transitions", fixture_sql)
            self.assertIn(
                "v_current_source_generation_publication_readiness", fixture_sql
            )
            self.assertIn("v_current_ingest_append_control", fixture_sql)
            self.assertIn("INNER JOIN `moraine_native_test`.mcp_open_sessions", fixture_sql)
            self.assertNotIn(
                "ANY INNER JOIN `moraine_native_test`.mcp_open_sessions",
                fixture_sql,
            )

            route.joinpath(".moraine.toml").write_text(
                'backend = "team"\n', encoding="utf-8"
            )
            with self.assertRaisesRegex(burst.NativeBurstFailure, "non-default backend"):
                burst._resolve_backend_selection(config, route)

            row["projected_events"] -= 1
            with mock.patch.object(
                burst,
                "_clickhouse_query",
                side_effect=["Darwin", json.dumps(row)],
            ):
                with self.assertRaisesRegex(burst.NativeBurstFailure, "not isolated"):
                    burst._fixture_database_evidence(selection, recipe)

    def test_native_input_gate_requires_arm64_python_and_binary_slice(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            binary = root / "moraine-mcp"
            binary.write_bytes(b"binary")
            binary.chmod(0o755)
            config = root / "config.toml"
            config.write_text("[clickhouse]\n", encoding="utf-8")
            arm64 = mock.Mock(returncode=0, stdout="arm64 x86_64\n", stderr="")
            with mock.patch.object(burst.subprocess, "run", return_value=arm64):
                observed = burst._validate_native_inputs(
                    binary,
                    config,
                    root,
                    system="Darwin",
                    machine="arm64",
                )
            self.assertEqual(observed, ("darwin", "arm64", ("arm64", "x86_64")))
            with self.assertRaisesRegex(burst.NativeBurstFailure, "native arm64 macOS"):
                burst._validate_native_inputs(
                    binary,
                    config,
                    root,
                    system="Darwin",
                    machine="x86_64",
                )
            x86 = mock.Mock(returncode=0, stdout="x86_64\n", stderr="")
            with mock.patch.object(burst.subprocess, "run", return_value=x86):
                with self.assertRaisesRegex(burst.NativeBurstFailure, "arm64 slice"):
                    burst._validate_native_inputs(
                        binary,
                        config,
                        root,
                        system="Darwin",
                        machine="arm64",
                    )
            config.write_text(
                "[clickhouse]\n[mcp]\nprewarm_on_initialize = true\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(burst.NativeBurstFailure, "requires.*prewarm"):
                burst._validate_native_inputs(
                    binary,
                    config,
                    root,
                    system="Darwin",
                    machine="arm64",
                )

    def test_cli_exposes_only_the_thin_native_command_inputs(self) -> None:
        args = performance_suite._parse_args(
            [
                "native-central-burst",
                "--mcp-binary",
                "/tmp/moraine-mcp",
                "--config",
                "/tmp/moraine.toml",
                "--output",
                "/tmp/native.json",
            ]
        )
        self.assertEqual(args.command, "native-central-burst")
        self.assertEqual(args.modes, list(burst.DEFAULT_MODES))
        self.assertEqual(args.bursts_per_case, 25)
        self.assertEqual(args.cold_repetitions, burst.DEFAULT_COLD_REPETITIONS)
        self.assertEqual(args.minimum_cold_samples, burst.DEFAULT_MIN_COLD_SAMPLES)
        self.assertEqual(args.warm_p95_limit_ms, burst.DEFAULT_WARM_P95_LIMIT_MS)
        self.assertEqual(args.cold_p95_limit_ms, burst.DEFAULT_COLD_P95_LIMIT_MS)
        self.assertEqual(args.max_latency_ms, burst.DEFAULT_MAX_LATENCY_MS)
        self.assertFalse(hasattr(args, "p95_limit_ms"))
        self.assertFalse(args.collect_query_log)
        self.assertFalse(hasattr(args, "search_mcp_events"))


if __name__ == "__main__":
    unittest.main()
