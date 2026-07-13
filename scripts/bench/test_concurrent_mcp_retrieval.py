#!/usr/bin/env python3
"""Deterministic tests for concurrent_mcp_retrieval.py."""
from __future__ import annotations

import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import benchmark_protocol
import concurrent_mcp_retrieval as bench

DIGEST = "sha256:" + "a" * 64
COMMIT = "b" * 40


def oracle(case_id: str, query: str | None = None) -> bench.OracleCase:
    return bench.OracleCase(case_id, query or f"term {case_id}", 1, DIGEST)


def manifest(count: int = 4) -> bench.OracleManifest:
    measured = tuple(oracle(f"measured-{index}") for index in range(count))
    recovery = (oracle("recovery-0"), oracle("recovery-1"))
    return bench.OracleManifest(
        "seed-v1", DIGEST, 100_000, oracle("warmup"), measured, recovery,
        bench.sha256_json([case.query_id for case in measured]),
    )


def sample(case_id: str, outcome: str = "success", wall_ms: float = 10.0, start: int = 1) -> bench.RequestSample:
    successful = outcome == "success"
    return bench.RequestSample(
        case_id, outcome, wall_ms, start, start + max(1, int(wall_ms * 1_000_000)),
        server_elapsed_ms=wall_ms - 1 if successful else None,
        sla_target_ms=750.0 if successful else None,
        met_sla=successful,
        exceeded_hard_deadline=successful and wall_ms > 5_000,
        oracle_passed=successful,
    )


class FakeResponseClient:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error

    def _send(self, payload):
        if self.error:
            raise self.error
        return self.response


class ConcurrencyParsingTests(unittest.TestCase):
    def test_repeatable_values_and_inclusive_sweep_are_arbitrary_and_deduplicated(self):
        self.assertEqual(bench.parse_concurrency(["1", "3,5", "8:12:2", "3"]), [1, 3, 5, 8, 10, 12])

    def test_non_positive_or_reversed_values_are_rejected(self):
        for value in ("0", "-1", "4:2", "1:5:0", ""):
            with self.subTest(value=value), self.assertRaises(bench.ConfigurationError):
                bench.parse_concurrency([value])

    def test_process_budget_is_runner_protection_not_backend_policy(self):
        with self.assertRaises(SystemExit):
            bench.parse_args(["--oracle-json", "x", "--concurrency", "8", "--max-processes", "9", "--output-dir", "out"])
        args = bench.parse_args(["--oracle-json", "x", "--concurrency", "8", "--max-processes", "10", "--output-dir", "out"])
        self.assertEqual(args.concurrency, [8])
        self.assertEqual(args.recovery_offsets, (0.0, 1.0))


class OracleTests(unittest.TestCase):
    def write_manifest(self, root: Path, *, duplicate=False, cardinality=100_000) -> Path:
        def entry(case_id, query):
            return {"id": case_id, "query": query, "result_count": 1, "result_digest": DIGEST}
        raw = {
            "schema_version": bench.ORACLE_SCHEMA_VERSION,
            "provenance": "seed-v1",
            "dataset": {"fingerprint": DIGEST, "cardinality": cardinality},
            "warmup": entry("warmup", "startup-only"),
            "measured": [entry("m0", "startup-only" if duplicate else "selective-a"), entry("m1", "common-b")],
            "recovery": [entry("r0", "recovery-c")],
        }
        path = root / "oracle.json"
        path.write_text(json.dumps(raw), encoding="utf-8")
        return path

    def test_distinct_query_allocation_and_query_set_digest(self):
        with tempfile.TemporaryDirectory() as raw:
            loaded = bench.load_oracle(self.write_manifest(Path(raw)), profile="full", needed_queries=2, recovery_count=1)
        self.assertEqual([case.query_id for case in loaded.measured[:2]], ["m0", "m1"])
        self.assertTrue(loaded.query_set_fingerprint.startswith("sha256:"))

    def test_warmup_measured_and_recovery_queries_must_be_distinct(self):
        with tempfile.TemporaryDirectory() as raw:
            with self.assertRaisesRegex(bench.ConfigurationError, "must be distinct"):
                bench.load_oracle(self.write_manifest(Path(raw), duplicate=True), profile="smoke", needed_queries=2, recovery_count=1)

    def test_full_profile_requires_meaningful_corpus(self):
        with tempfile.TemporaryDirectory() as raw:
            with self.assertRaisesRegex(bench.ConfigurationError, "100000"):
                bench.load_oracle(self.write_manifest(Path(raw), cardinality=99_999), profile="full", needed_queries=2, recovery_count=1)


class BurstTests(unittest.TestCase):
    def test_barrier_proves_overlap_and_retains_out_of_order_response_slots(self):
        cases = [oracle(f"q{index}") for index in range(4)]

        def delayed(_client, _request_id, case, _deadline):
            delay = {"q0": 0.04, "q1": 0.01, "q2": 0.03, "q3": 0.02}[case.query_id]
            started = time.perf_counter_ns()
            time.sleep(delay)
            return sample(case.query_id, wall_ms=delay * 1000, start=started)

        result = bench.run_burst([object()] * 4, cases, hard_deadline_ms=5_000, invoke=delayed)
        self.assertEqual(result.max_in_flight, 4)
        self.assertTrue(result.overlap_proven)
        self.assertEqual([item.query_id for item in result.samples], ["q0", "q1", "q2", "q3"])

    def test_production_gate_writes_every_rpc_before_reading_any_response(self):
        writes = []
        clients = []
        cases = []

        for index in range(4):
            results = [
                {
                    "event": {"id": f"event-{index}"},
                    "session": {"id": f"session-{index}"},
                }
            ]
            cases.append(
                bench.OracleCase(
                    f"q{index}", f"term-{index}", 1, bench.result_digest(results)
                )
            )

            class Stdin:
                def write(self, payload, index=index):
                    writes.append((index, payload))

                def flush(self):
                    pass

            rpc_result = {
                "jsonrpc": "2.0",
                "id": 1000 + index,
                "result": {
                    "isError": False,
                    "structuredContent": {
                        "schema_version": "moraine.mcp.search_sessions.v1",
                        "tool": "search_sessions",
                        "data": {"result_count": 1, "results": results},
                        "performance": {
                            "elapsed_ms": 1,
                            "sla_target_ms": 750,
                            "met_sla": True,
                        },
                    },
                },
            }
            client = mock.Mock()
            client.proc.stdin = Stdin()
            client.proc.stdout = object()
            client.proc.poll.return_value = None
            client.request_timeout_s = 1

            def read_response(_deadline, response=rpc_result):
                self.assertEqual(len(writes), 4)
                return json.dumps(response)

            client._read_response_line.side_effect = read_response
            clients.append(client)

        result = bench.run_burst(clients, cases, hard_deadline_ms=5_000)
        self.assertEqual(result.max_in_flight, 4)
        self.assertTrue(result.overlap_proven)
        self.assertEqual(len(writes), 4)

    def test_child_failure_is_not_silently_converted_to_a_sample(self):
        def fail(*_args):
            raise RuntimeError("child died")

        with self.assertRaisesRegex(bench.BenchmarkFailure, "burst-child-failed"):
            bench.run_burst([object(), object()], [oracle("a"), oracle("b")], hard_deadline_ms=5_000, invoke=fail)


class ClassificationTests(unittest.TestCase):
    def rpc(self, result):
        return {"jsonrpc": "2.0", "id": 7, "result": result}

    def test_admission_jsonrpc_mcp_deadline_and_timeout_are_distinct(self):
        variants = [
            ({"error": {"message": "server busy: concurrent request limit reached"}}, None, "admission_rejection"),
            ({"error": {"message": "other"}}, None, "jsonrpc_error"),
            (self.rpc({"isError": True, "structuredContent": {"error": {"code": "bad_request"}}}), None, "mcp_error"),
            (self.rpc({"isError": True, "structuredContent": {
                "error": {"code": "deadline_exceeded"},
                "performance": {"elapsed_ms": 5001, "sla_target_ms": 750, "met_sla": False},
            }}), None, "deadline_exceeded"),
            (None, bench.BenchmarkFailure("request-timeout"), "timeout"),
        ]
        for response, error, expected in variants:
            with self.subTest(expected=expected):
                observed = bench.invoke_search(FakeResponseClient(response, error), 7, oracle("q"), 5_000)
                self.assertEqual(observed.outcome, expected)

    def test_success_validates_server_performance_and_seed_owned_digest(self):
        results = [{"event": {"id": "event-1"}, "session": {"id": "session-1"}}]
        case = bench.OracleCase("q", "term", 1, bench.result_digest(results))
        structured = {
            "schema_version": "moraine.mcp.search_sessions.v1", "tool": "search_sessions",
            "data": {"result_count": 1, "results": results},
            "performance": {"elapsed_ms": 12, "sla_target_ms": 750, "met_sla": True},
        }
        observed = bench.invoke_search(FakeResponseClient(self.rpc({"isError": False, "structuredContent": structured})), 7, case, 5_000)
        self.assertEqual(observed.outcome, "success")
        self.assertTrue(observed.oracle_passed)
        self.assertEqual(observed.server_elapsed_ms, 12)
    def test_oracle_mismatch_is_a_semantic_error_not_a_timing_sample(self):
        results = [{"event": {"id": "event-1"}, "session": {"id": "session-1"}}]
        structured = {
            "schema_version": "moraine.mcp.search_sessions.v1",
            "tool": "search_sessions",
            "data": {"result_count": 1, "results": results},
            "performance": {"elapsed_ms": 12, "sla_target_ms": 750, "met_sla": True},
        }
        observed = bench.invoke_search(
            FakeResponseClient(self.rpc({"isError": False, "structuredContent": structured})),
            7,
            oracle("q"),
            5_000,
        )
        self.assertEqual(observed.outcome, "semantic_error")
        self.assertFalse(observed.oracle_passed)



class ArtifactTests(unittest.TestCase):
    def scenario(self, concurrency=2, outcomes=("success", "admission_rejection")):
        burst_samples = [sample(f"q{index}", outcome, 20 + index, start=index + 1) for index, outcome in enumerate(outcomes)]
        burst = bench.BurstResult(concurrency, burst_samples, 50.0, concurrency, True)
        recovery = [sample("r0", "success", 30.0, 10)] if concurrency == 2 else []
        return bench.ScenarioResult(concurrency, [bench.RepetitionResult(burst, recovery)])

    def build(self, scenario=None, request_timeout=20.0):
        return bench.build_artifact(
            scenario or self.scenario(), manifest=manifest(), profile="full",
            source={"git_commit": COMMIT, "dirty": False}, build_profile="release",
            recovery_offsets_s=[0.0] if (scenario or self.scenario()).concurrency == 2 else [],
            request_timeout_s=request_timeout, run_timeout_s=600.0, hard_deadline_ms=5_000.0,
        )

    def test_schema_valid_artifact_exposes_raw_successes_outcomes_summary_and_recovery(self):
        artifact = self.build()
        benchmark_protocol.validate_artifact(artifact)
        self.assertEqual(artifact["samples"]["measurements"]["wall_latency_ms"], [20.0])
        self.assertEqual(artifact["samples"]["outcomes"]["admission_rejection"], 1)
        self.assertEqual(artifact["samples"]["observations"]["recovery_wall_latency_ms"], [30.0])
        self.assertEqual(artifact["samples"]["records"][0]["case_id"], "q0")
        self.assertEqual(artifact["samples"]["records"][-1]["phase"], "recovery")
        self.assertEqual(artifact["metrics"]["resources"]["maximum_in_flight_count"], 2)
        self.assertEqual(artifact["metrics"]["resources"]["overlap_proven_ratio"], 1.0)
        self.assertEqual(artifact["timing"]["status"], "not_evaluated")

    def test_unlike_concurrency_is_comparison_incomparable(self):
        left = self.build(self.scenario(1, ("success",)))
        right = self.build(self.scenario(2, ("success", "success")))
        with self.assertRaises(benchmark_protocol.IncomparableError):
            benchmark_protocol.compare_artifacts(left, right)
    def test_request_timeout_changes_comparison_identity(self):
        left = self.build(request_timeout=1.0)
        right = self.build(request_timeout=20.0)
        with self.assertRaises(benchmark_protocol.IncomparableError):
            benchmark_protocol.compare_artifacts(left, right)

    def test_all_operational_failures_are_valid_observations_not_semantic_failures(self):
        artifact = self.build(self.scenario(2, ("admission_rejection", "timeout")))
        benchmark_protocol.validate_artifact(artifact)
        self.assertEqual(artifact["samples"]["successful"], 0)
        self.assertEqual(artifact["semantic"]["status"], "pass")

    def test_recovery_oracle_failure_fails_semantics(self):
        scenario = self.scenario()
        scenario.repetitions[0].recovery = [sample("r0", "semantic_error")]
        artifact = self.build(scenario)
        self.assertEqual(artifact["semantic"]["status"], "fail")
        self.assertEqual(artifact["metrics"]["quality"]["failed_checks"], 1)


    def test_outcome_counts_must_cover_every_attempt(self):
        artifact = self.build()
        artifact["samples"]["outcomes"]["timeout"] += 1
        with self.assertRaisesRegex(benchmark_protocol.ProtocolError, "outcome counts"):
            benchmark_protocol.validate_artifact(artifact)

    def test_percentiles_only_use_successful_samples_and_failures_remain_explicit(self):
        scenario = self.scenario(4, ("success", "timeout", "success", "admission_rejection"))
        scenario.repetitions[0].burst.samples[2].wall_ms = 100.0
        artifact = self.build(scenario)
        self.assertEqual(artifact["metrics"]["resources"]["p50_latency_ms"], 60.0)
        self.assertEqual(artifact["samples"]["errors"], 2)
        self.assertEqual(artifact["samples"]["outcomes"]["timeout"], 1)


class CleanupTests(unittest.TestCase):
    def test_repetition_closes_clients_stops_server_and_removes_workdir_after_failure(self):
        fake_client = mock.Mock()
        fake_client.close.return_value = []
        fake_server = mock.Mock()
        fake_server.poll.return_value = None
        with tempfile.TemporaryDirectory() as root:
            workdir = Path(root) / "owned"
            workdir.mkdir()
            with (
                mock.patch.object(bench.tempfile, "mkdtemp", return_value=str(workdir)),
                mock.patch.object(bench, "write_config"),
                mock.patch.object(bench, "_start_server", return_value=fake_server),
                mock.patch.object(bench, "wait_for_socket"),
                mock.patch.object(bench, "spawn_client", return_value=fake_client),
                mock.patch.object(bench, "initialize_client"),
                mock.patch.object(bench, "invoke_search", return_value=sample("warmup")),
                mock.patch.object(bench, "run_burst", side_effect=bench.BenchmarkFailure("burst-child-failed")),
                mock.patch.object(bench, "_stop_server", return_value=[]) as stop_server,
            ):
                with self.assertRaisesRegex(bench.BenchmarkFailure, "burst-child-failed"):
                    bench.run_repetition(
                        2, moraine_mcp="mcp", clickhouse_url="http://clickhouse:8123", database="moraine",
                        manifest=manifest(), recovery_count=0, recovery_offsets_s=(), startup_timeout_s=1,
                        request_timeout_s=1, hard_deadline_ms=5_000,
                    )
            self.assertGreaterEqual(fake_client.close.call_count, 1)
            stop_server.assert_called_once_with(fake_server)
            self.assertFalse(workdir.exists())

    def test_partial_measured_client_spawn_failure_closes_already_owned_children(self):
        warmup_client, measured_client = mock.Mock(), mock.Mock()
        for client in (warmup_client, measured_client):
            client.close.return_value = []
        server = mock.Mock()
        with tempfile.TemporaryDirectory() as root:
            workdir = Path(root) / "owned"
            workdir.mkdir()
            with (
                mock.patch.object(bench.tempfile, "mkdtemp", return_value=str(workdir)),
                mock.patch.object(bench, "write_config"),
                mock.patch.object(bench, "_start_server", return_value=server),
                mock.patch.object(bench, "wait_for_socket"),
                mock.patch.object(
                    bench,
                    "spawn_client",
                    side_effect=[
                        warmup_client,
                        measured_client,
                        bench.BenchmarkFailure("client-startup-failed"),
                    ],
                ),
                mock.patch.object(bench, "initialize_client"),
                mock.patch.object(bench, "invoke_search", return_value=sample("warmup")),
                mock.patch.object(bench, "_stop_server", return_value=[]),
            ):
                with self.assertRaisesRegex(bench.BenchmarkFailure, "client-startup-failed"):
                    bench.run_repetition(
                        2,
                        moraine_mcp="mcp",
                        clickhouse_url="http://clickhouse:8123",
                        database="moraine",
                        manifest=manifest(),
                        recovery_count=0,
                        recovery_offsets_s=(),
                        startup_timeout_s=1,
                        request_timeout_s=1,
                        hard_deadline_ms=5_000,
                    )
            warmup_client.close.assert_called_once()
            measured_client.close.assert_called_once()
            self.assertFalse(workdir.exists())

    def test_each_repetition_owns_fresh_service_config_clients_and_cache_state(self):
        clients = [mock.Mock() for _ in range(4)]
        for client in clients:
            client.close.return_value = []
        servers = [mock.Mock(), mock.Mock()]
        burst = bench.BurstResult(1, [sample("measured-0")], 1.0, 1, True)
        with tempfile.TemporaryDirectory() as root:
            workdirs = [Path(root) / "rep-0", Path(root) / "rep-1"]
            for workdir in workdirs:
                workdir.mkdir()
            with (
                mock.patch.object(
                    bench.tempfile, "mkdtemp", side_effect=[str(path) for path in workdirs]
                ),
                mock.patch.object(bench, "write_config") as write_config,
                mock.patch.object(bench, "_start_server", side_effect=servers) as start_server,
                mock.patch.object(bench, "wait_for_socket"),
                mock.patch.object(bench, "spawn_client", side_effect=clients),
                mock.patch.object(bench, "initialize_client"),
                mock.patch.object(bench, "invoke_search", return_value=sample("warmup")),
                mock.patch.object(bench, "run_burst", return_value=burst),
                mock.patch.object(bench, "_stop_server", return_value=[]),
            ):
                for _ in range(2):
                    bench.run_repetition(
                        1,
                        moraine_mcp="mcp",
                        clickhouse_url="http://clickhouse:8123",
                        database="moraine",
                        manifest=manifest(),
                        recovery_count=0,
                        recovery_offsets_s=(),
                        startup_timeout_s=1,
                        request_timeout_s=1,
                        hard_deadline_ms=5_000,
                    )
            self.assertEqual(start_server.call_count, 2)
            config_paths = [call.args[0] for call in write_config.call_args_list]
            self.assertEqual(len(config_paths), len(set(config_paths)))
            self.assertTrue(all(not path.exists() for path in workdirs))


if __name__ == "__main__":
    unittest.main()
