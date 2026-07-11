#!/usr/bin/env python3
"""Focused deterministic tests for central_mcp_resource.py."""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parent))
import benchmark_protocol
import central_mcp_resource as bench


GOOD_COMMIT = "a" * 40
GOOD_DATASET = "sha256:8ed67df045c5caceb250e5eb6479e057cd24a22e80fec1f12e82ef8391ef009c"


def resource_sample(processes: int, rss_bytes: int, threads: int):
    return bench.ResourceSample(proc_count=processes, rss_bytes=rss_bytes, threads=threads)


def arm_result(arm: str, n: int, offset: int = 0):
    server = resource_sample(1, 2_000 + offset, 2) if arm == "central" else resource_sample(0, 0, 0)
    return bench.ArmResult(
        arm=arm,
        n=n,
        client=resource_sample(n, n * (10_000 + offset), n * 3),
        server=server,
        tools_call_ms=[1.25 + offset / 1000 for _ in range(n)],
    )


def complete_matrix(reps: int = 2, arms=None, ns=None):
    arms = arms or ["embedded", "central"]
    ns = ns or [1, 2]
    iterations = []
    for rep in range(reps):
        iteration = {}
        for n in ns:
            for arm in arms:
                iteration[(arm, n)] = arm_result(arm, n, rep)
        iterations.append(iteration)
    return bench.MatrixRun(planned=reps, attempted=reps, iterations=iterations, diagnostics=[])


def artifact_for(matrix=None, arms=None, ns=None, query="opaque seeded marker"):
    matrix = matrix or complete_matrix()
    arms = arms or ["embedded", "central"]
    ns = ns or [1, 2]
    return bench.build_artifact(
        matrix=matrix,
        arms=arms,
        ns=ns,
        profile="smoke",
        query=query,
        settle_s=0.0,
        batch=2,
        startup_timeout_s=1.0,
        request_timeout_s=1.0,
        dataset_fingerprint=GOOD_DATASET,
        dataset_cardinality=1,
        git_commit=GOOD_COMMIT,
        dirty=False,
        build_profile="debug",
        target="aarch64-apple-darwin",
        runner_os="darwin",
        cpu_class="arm64",
    )


FAKE_MCP = r'''#!/usr/bin/env python3
import json
import sys
import time

mode = __import__("pathlib").Path(sys.argv[0]).name
for line in sys.stdin:
    request = json.loads(line)
    if mode == "crash":
        sys.exit(19)
    if mode == "timeout":
        time.sleep(0.25)
        continue
    request_id = request["id"]
    method = request["method"]
    if mode == "partial":
        sys.stdout.write('{"jsonrpc":"2.0","id":')
        sys.stdout.flush()
        time.sleep(0.25)
        continue
    if method == "initialize":
        result = {"protocolVersion": "2025-03-26"}
    elif method == "tools/list":
        result = {"tools": [{"name": "search_sessions"}]}
    else:
        text = "wrong payload" if mode == "wrong" else "seeded marker payload"
        event_id = "event:different" if mode == "corpus" else "event:seed-event-1"
        results = [] if mode == "empty" else [{
            "event": {"id": event_id},
            "session": {"id": "session:seed-session-1"},
            "snippet": {"text": text},
        }]
        result = {
            "isError": mode == "semantic",
            "structuredContent": {
                "schema_version": "moraine.mcp.search_sessions.v1",
                "tool": "search_sessions",
                "data": {"result_count": len(results), "results": results},
            },
        }
    print(json.dumps({"jsonrpc": "2.0", "id": request_id, "result": result}), flush=True)
'''


class CentralMcpArtifactTests(unittest.TestCase):
    def test_representative_artifact_validates_and_keeps_paired_raw_samples(self):
        artifact = artifact_for()
        bench.validate_artifact(artifact)

        self.assertEqual(artifact["schema_version"], "moraine-benchmark-v1")
        self.assertEqual(artifact["benchmark_id"], "central-mcp-resource")
        self.assertEqual(artifact["samples"]["planned"], 2)
        self.assertEqual(artifact["samples"]["attempted"], 2)
        self.assertEqual(artifact["samples"]["successful"], 2)
        self.assertEqual(artifact["samples"]["errors"], 0)
        self.assertTrue(artifact["samples"]["measurements"])
        self.assertTrue(
            all(len(series) == 2 for series in artifact["samples"]["measurements"].values())
        )
        self.assertEqual(artifact["semantic"]["status"], "pass")
        self.assertEqual(
            artifact["timing"], {"status": "not_evaluated", "non_blocking": True}
        )
        self.assertIn("resources", artifact["metrics"])
        self.assertTrue(
            all(isinstance(value, (int, float)) for value in artifact["metrics"]["resources"].values())
        )
        self.assertEqual(
            artifact["metrics"]["storage"], {"dataset_cardinality_count": 1}
        )

    def test_scenario_records_exact_request_dataset_cache_and_concurrency_fingerprints(self):
        artifact = artifact_for()
        scenario = artifact["scenario"]
        self.assertEqual(scenario["measured_boundary"], "stdio-tools-call-write-to-response")
        self.assertEqual(
            scenario["fingerprints"],
            {
                "dataset": {"fingerprint": GOOD_DATASET, "cardinality": 1},
                "cache_state": "mixed",
                "concurrency": 2,
                "request_source": "moraine-mcp-jsonrpc-stdio",
            },
        )
        changed = artifact_for(query="different opaque marker")
        self.assertNotEqual(artifact["scenario_id"], changed["scenario_id"])
        self.assertNotEqual(artifact["scenario"]["workload_id"], changed["scenario"]["workload_id"])
        self.assertEqual(
            artifact["scenario"]["fingerprints"]["dataset"],
            changed["scenario"]["fingerprints"]["dataset"],
        )

    def test_both_arm_single_client_smoke_is_canonically_cold(self):
        arms = ["embedded", "central"]
        ns = [1]
        artifact = artifact_for(complete_matrix(1, arms, ns), arms, ns)
        bench.validate_artifact(artifact)
        self.assertEqual(artifact["scenario"]["fingerprints"]["cache_state"], "cold")

    def test_single_client_omits_inapplicable_concurrency_fingerprint(self):
        arms = ["embedded"]
        ns = [1]
        artifact = artifact_for(complete_matrix(1, arms, ns), arms, ns)
        bench.validate_artifact(artifact)
        self.assertFalse(artifact["scenario"]["dimensions"]["concurrent"])
        self.assertNotIn("concurrency", artifact["scenario"]["fingerprints"])
        self.assertEqual(artifact["scenario"]["fingerprints"]["cache_state"], "cold")

    def test_failed_artifact_is_valid_and_preserves_explicit_safe_diagnostics(self):
        matrix = bench.MatrixRun(
            planned=3,
            attempted=1,
            iterations=[],
            diagnostics=["central-startup-timeout", "workdir-cleanup-failed"],
        )
        artifact = artifact_for(matrix)
        bench.validate_artifact(artifact)
        self.assertEqual(
            artifact["samples"],
            {
                "planned": 3,
                "attempted": 1,
                "successful": 0,
                "errors": 1,
                "measurements": mock.ANY,
            },
        )
        self.assertTrue(all(not values for values in artifact["samples"]["measurements"].values()))
        self.assertEqual(artifact["semantic"]["status"], "fail")
        self.assertEqual(
            artifact["diagnostics"],
            [{"code": "central-startup-timeout"}, {"code": "workdir-cleanup-failed"}],
        )

    def test_artifact_redacts_query_and_identity_bearing_paths(self):
        secret_query = "private raw conversation text"
        serialized = json.dumps(artifact_for(query=secret_query))
        self.assertNotIn(secret_query, serialized)
        self.assertNotIn("/Users/", serialized)
        self.assertNotIn("/home/", serialized)
        self.assertNotIn("clickhouse", serialized)

    def test_protocol_rejects_count_and_raw_series_mismatch(self):
        artifact = artifact_for()
        artifact["samples"]["measurements"]["embedded_n1_process_count"].pop()
        with self.assertRaises(bench.OutputFailure) as caught:
            bench.validate_artifact(artifact)
        self.assertIn("protocol-validation-failed", caught.exception.codes)

    def test_parse_validation_rejects_duplicates_unknown_arms_and_bad_fingerprint(self):
        with self.assertRaises(argparse_error()):
            bench.parse_ns("1,1")
        with self.assertRaises(argparse_error()):
            bench.parse_arms("embedded,other")
        with self.assertRaises(argparse_error()):
            bench.validate_dataset_fingerprint("sha256:ABC")


class CentralMcpProcessTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def fake_binary(self, mode="ok"):
        path = self.root / mode
        path.write_text(FAKE_MCP)
        path.chmod(0o755)
        return str(path)

    def test_smoke_executes_initialize_list_and_production_tools_call_boundary(self):
        client = bench.spawn_client(self.fake_binary(), self.root / "unused.toml", 1.0)
        try:
            client.warm_up(
                "workload query",
                expected_marker="seeded marker",
                expected_count=1,
                expected_fingerprint=GOOD_DATASET,
            )
            self.assertIsNotNone(client.tools_call_ms)
            self.assertGreaterEqual(client.tools_call_ms, 0.0)
        finally:
            self.assertEqual(client.close(), [])

    def test_embedded_arm_smoke_samples_one_live_child_and_cleans_up(self):
        with mock.patch.object(bench, "_read_pid", return_value=(4096, 1)):
            result = bench.run_arm(
                "embedded",
                1,
                moraine_mcp=self.fake_binary(),
                clickhouse_url="http://127.0.0.1:8123",
                database="moraine",
                query="seeded marker",
                seed_marker="seeded marker",
                dataset_fingerprint=GOOD_DATASET,
                dataset_cardinality=1,
                settle_s=0.0,
                batch=1,
                startup_timeout_s=1.0,
                request_timeout_s=1.0,
            )
        self.assertEqual(result.client.proc_count, 1)
        self.assertEqual(len(result.tools_call_ms), 1)
        self.assertGreater(result.total_rss_bytes, 0)

    def test_missing_child_is_explicit_startup_failure(self):
        with self.assertRaises(bench.BenchmarkFailure) as caught:
            bench.spawn_client(str(self.root / "missing"), self.root / "config", 1.0)
        self.assertIn("client-startup-failed", caught.exception.codes)

    def test_child_crash_is_explicit(self):
        client = bench.spawn_client(self.fake_binary("crash"), self.root / "config", 1.0)
        try:
            with self.assertRaises(bench.BenchmarkFailure) as caught:
                client.warm_up(
                    "workload query",
                    expected_marker="seeded marker",
                    expected_count=1,
                    expected_fingerprint=GOOD_DATASET,
                )
            self.assertIn("client-exited", caught.exception.codes)
        finally:
            client.close()

    def test_request_timeout_is_explicit(self):
        client = bench.spawn_client(self.fake_binary("timeout"), self.root / "config", 0.02)
        try:
            with self.assertRaises(bench.BenchmarkFailure) as caught:
                client.warm_up(
                    "workload query",
                    expected_marker="seeded marker",
                    expected_count=1,
                    expected_fingerprint=GOOD_DATASET,
                )
            self.assertIn("request-timeout", caught.exception.codes)
        finally:
            client.close()

    def test_semantic_oracle_failure_is_not_timing_failure(self):
        client = bench.spawn_client(self.fake_binary("semantic"), self.root / "config", 1.0)
        try:
            with self.assertRaises(bench.BenchmarkFailure) as caught:
                client.warm_up(
                    "workload query",
                    expected_marker="seeded marker",
                    expected_count=1,
                    expected_fingerprint=GOOD_DATASET,
                )
            self.assertIn("semantic-oracle-failed", caught.exception.codes)
        finally:
            client.close()

    def test_partial_json_frame_without_newline_times_out(self):
        client = bench.spawn_client(self.fake_binary("partial"), self.root / "config", 0.02)
        started = bench.time.monotonic()
        try:
            with self.assertRaises(bench.BenchmarkFailure) as caught:
                client.warm_up(
                    "seeded marker",
                    expected_marker="seeded marker",
                    expected_count=1,
                    expected_fingerprint=GOOD_DATASET,
                )
            self.assertIn("request-timeout", caught.exception.codes)
            self.assertLess(bench.time.monotonic() - started, 0.2)
        finally:
            client.close()

    def test_empty_and_wrong_seeded_results_fail_the_semantic_oracle(self):
        for mode in ("empty", "wrong"):
            with self.subTest(mode=mode):
                client = bench.spawn_client(self.fake_binary(mode), self.root / "config", 1.0)
                try:
                    with self.assertRaises(bench.BenchmarkFailure) as caught:
                        client.warm_up(
                            "seeded marker",
                            expected_marker="seeded marker",
                            expected_count=1,
                            expected_fingerprint=GOOD_DATASET,
                        )
                    self.assertIn("semantic-oracle-failed", caught.exception.codes)
                finally:
                    client.close()

    def test_seeded_corpus_digest_mismatch_is_explicit(self):
        client = bench.spawn_client(self.fake_binary("corpus"), self.root / "config", 1.0)
        try:
            with self.assertRaises(bench.BenchmarkFailure) as caught:
                client.warm_up(
                    "seeded marker",
                    expected_marker="seeded marker",
                    expected_count=1,
                    expected_fingerprint=GOOD_DATASET,
                )
            self.assertIn("dataset-corpus-mismatch", caught.exception.codes)
        finally:
            client.close()

    def test_central_process_spawn_failure_is_explicit(self):
        with mock.patch.object(bench.subprocess, "Popen", side_effect=OSError("unavailable")):
            with self.assertRaises(bench.BenchmarkFailure) as caught:
                bench.run_arm(
                    "central",
                    1,
                    moraine_mcp=self.fake_binary(),
                    clickhouse_url="http://127.0.0.1:8123",
                    database="moraine",
                    query="seeded marker",
                    seed_marker="seeded marker",
                    dataset_fingerprint=GOOD_DATASET,
                    dataset_cardinality=1,
                    settle_s=0.0,
                    batch=1,
                    startup_timeout_s=1.0,
                    request_timeout_s=1.0,
                )
        self.assertIn("central-startup-failed", caught.exception.codes)

    def test_central_startup_child_exit_and_timeout_are_distinct(self):
        dead = mock.Mock()
        dead.poll.return_value = 17
        with self.assertRaises(bench.BenchmarkFailure) as exited:
            bench.wait_for_socket(self.root / "missing.sock", dead, 0.01)
        self.assertIn("central-server-exited", exited.exception.codes)

        alive = mock.Mock()
        alive.poll.return_value = None
        with self.assertRaises(bench.BenchmarkFailure) as timeout:
            bench.wait_for_socket(self.root / "missing.sock", alive, 0.001)
        self.assertIn("central-startup-timeout", timeout.exception.codes)

    def test_socket_probe_closes_candidate_on_interrupt(self):
        socket_path = self.root / "ready.sock"
        socket_path.touch()
        server = mock.Mock()
        server.poll.return_value = None
        candidate = mock.Mock()
        candidate.connect.side_effect = KeyboardInterrupt
        with mock.patch.object(bench.socket, "socket", return_value=candidate):
            with self.assertRaises(KeyboardInterrupt):
                bench.wait_for_socket(socket_path, server, 1.0)
        candidate.close.assert_called_once_with()

    def test_cleanup_failure_is_reported_alongside_primary_failure(self):
        owned_workdir = self.root / "owned-workdir"
        owned_workdir.mkdir()
        with mock.patch.object(
            bench.tempfile, "mkdtemp", return_value=str(owned_workdir)
        ), mock.patch.object(
            bench.shutil, "rmtree", side_effect=OSError("private path")
        ):
            with self.assertRaises(bench.BenchmarkFailure) as caught:
                bench.run_arm(
                    "embedded",
                    1,
                    moraine_mcp=self.fake_binary("semantic"),
                    clickhouse_url="http://127.0.0.1:8123",
                    database="moraine",
                    query="seeded marker",
                    seed_marker="seeded marker",
                    dataset_fingerprint=GOOD_DATASET,
                    dataset_cardinality=1,
                    settle_s=0.0,
                    batch=1,
                    startup_timeout_s=1.0,
                    request_timeout_s=1.0,
                )
        self.assertIn("semantic-oracle-failed", caught.exception.codes)
        self.assertIn("workdir-cleanup-failed", caught.exception.codes)


    def test_keyboard_interrupt_after_client_spawn_cleans_process_and_workdir(self):
        owned_workdir = self.root / "interrupted-client"
        owned_workdir.mkdir()
        spawned = []
        real_spawn = bench.spawn_client

        def tracked_spawn(*args, **kwargs):
            client = real_spawn(*args, **kwargs)
            spawned.append(client)
            return client

        with mock.patch.object(
            bench.tempfile, "mkdtemp", return_value=str(owned_workdir)
        ), mock.patch.object(
            bench, "spawn_client", side_effect=tracked_spawn
        ), mock.patch.object(
            bench.McpClient, "warm_up", side_effect=KeyboardInterrupt
        ):
            with self.assertRaises(KeyboardInterrupt):
                bench.run_arm(
                    "embedded",
                    1,
                    moraine_mcp=self.fake_binary(),
                    clickhouse_url="http://127.0.0.1:8123",
                    database="moraine",
                    query="seeded marker",
                    seed_marker="seeded marker",
                    dataset_fingerprint=GOOD_DATASET,
                    dataset_cardinality=1,
                    settle_s=0.0,
                    batch=1,
                    startup_timeout_s=1.0,
                    request_timeout_s=1.0,
                )
        self.assertEqual(len(spawned), 1)
        self.assertIsNotNone(spawned[0].proc.poll())
        self.assertFalse(owned_workdir.exists())

    def test_keyboard_interrupt_preserves_cleanup_failure(self):
        owned_workdir = self.root / "interrupted-cleanup-failure"
        owned_workdir.mkdir()
        with mock.patch.object(
            bench.tempfile, "mkdtemp", return_value=str(owned_workdir)
        ), mock.patch.object(
            bench.McpClient, "warm_up", side_effect=KeyboardInterrupt
        ), mock.patch.object(
            bench.shutil, "rmtree", side_effect=OSError("private path")
        ):
            with self.assertRaises(KeyboardInterrupt) as caught:
                bench.run_arm(
                    "embedded",
                    1,
                    moraine_mcp=self.fake_binary(),
                    clickhouse_url="http://127.0.0.1:8123",
                    database="moraine",
                    query="workload query",
                    seed_marker="seeded marker",
                    dataset_fingerprint=GOOD_DATASET,
                    dataset_cardinality=1,
                    settle_s=0.0,
                    batch=1,
                    startup_timeout_s=1.0,
                    request_timeout_s=1.0,
                )
        self.assertIn("workdir-cleanup-failed", caught.exception.cleanup_codes)

    def test_keyboard_interrupt_after_server_spawn_cleans_server_socket_workdir(self):
        owned_workdir = self.root / "interrupted-server"
        owned_workdir.mkdir()
        server = mock.Mock()
        server.poll.return_value = None
        server.wait.return_value = 0
        with mock.patch.object(
            bench.tempfile, "mkdtemp", return_value=str(owned_workdir)
        ), mock.patch.object(
            bench.subprocess, "Popen", return_value=server
        ), mock.patch.object(
            bench, "wait_for_socket", side_effect=KeyboardInterrupt
        ):
            with self.assertRaises(KeyboardInterrupt):
                bench.run_arm(
                    "central",
                    1,
                    moraine_mcp=self.fake_binary(),
                    clickhouse_url="http://127.0.0.1:8123",
                    database="moraine",
                    query="seeded marker",
                    seed_marker="seeded marker",
                    dataset_fingerprint=GOOD_DATASET,
                    dataset_cardinality=1,
                    settle_s=0.0,
                    batch=1,
                    startup_timeout_s=1.0,
                    request_timeout_s=1.0,
                )
        server.terminate.assert_called_once_with()
        self.assertFalse(owned_workdir.exists())

class CentralMcpOutputTests(unittest.TestCase):
    def test_atomic_output_validates_and_replaces_complete_file(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "result.json"
            bench.atomic_write_artifact(output, artifact_for())
            loaded = json.loads(output.read_text())
            bench.validate_artifact(loaded)
            self.assertEqual(loaded["schema_version"], "moraine-benchmark-v1")
            self.assertEqual([path.name for path in Path(temp).iterdir()], ["result.json"])

    def test_output_replace_failure_is_explicit_and_removes_temporary_file(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "result.json"
            with mock.patch.object(
                benchmark_protocol.os, "replace", side_effect=OSError("private path")
            ):
                with self.assertRaises(bench.OutputFailure) as caught:
                    bench.atomic_write_artifact(output, artifact_for())
            self.assertIn("output-write-failed", caught.exception.codes)
            self.assertFalse(output.exists())
            self.assertEqual(list(Path(temp).iterdir()), [])

    def test_atomic_output_creates_missing_parent_directories(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "missing" / "nested" / "result.json"
            bench.atomic_write_artifact(output, artifact_for())
            self.assertTrue(output.is_file())
            benchmark_protocol.load_artifact(output)

    def test_failed_owned_run_writes_schema_valid_failure_artifact(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            binary = root / "empty"
            binary.write_text(FAKE_MCP)
            binary.chmod(0o755)
            output = root / "artifacts" / "result.json"
            with mock.patch.object(
                bench, "measurement_available", return_value=True
            ), mock.patch("builtins.print"):
                status = bench.main(
                    [
                        "--moraine-mcp",
                        str(binary),
                        "--arms",
                        "embedded",
                        "--ns",
                        "1",
                        "--reps",
                        "1",
                        "--profile",
                        "smoke",
                        "--query",
                        "workload query",
                        "--seed-marker",
                        "seeded marker",
                        "--settle-seconds",
                        "0",
                        "--batch",
                        "1",
                        "--startup-timeout-seconds",
                        "1",
                        "--request-timeout-seconds",
                        "1",
                        "--dataset-fingerprint",
                        GOOD_DATASET,
                        "--dataset-cardinality",
                        "1",
                        "--git-commit",
                        GOOD_COMMIT,
                        "--dirty",
                        "false",
                        "--build-profile",
                        "debug",
                        "--target",
                        "test-target",
                        "--json-out",
                        str(output),
                    ]
                )
            self.assertEqual(status, 1)
            artifact = benchmark_protocol.load_artifact(output)
            self.assertEqual(artifact["samples"]["attempted"], 1)
            self.assertEqual(artifact["samples"]["successful"], 0)
            self.assertEqual(artifact["samples"]["errors"], 1)
            self.assertEqual(artifact["semantic"]["status"], "fail")
            self.assertEqual(
                artifact["diagnostics"], [{"code": "semantic-oracle-failed"}]
            )

    def test_matrix_stops_after_failed_pair_and_counts_attempt(self):
        calls = []

        def run_one(arm, n, **kwargs):
            calls.append((arm, n))
            if len(calls) == 3:
                raise bench.BenchmarkFailure("client-exited")
            return arm_result(arm, n)

        matrix = bench.run_matrix(reps=3, arms=["embedded", "central"], ns=[1], run_one=run_one)
        self.assertEqual(matrix.planned, 3)
        self.assertEqual(matrix.attempted, 2)
        self.assertEqual(matrix.successful, 1)
        self.assertEqual(matrix.errors, 1)
        self.assertEqual(matrix.diagnostics, ["client-exited"])


def argparse_error():
    import argparse

    return argparse.ArgumentTypeError


if __name__ == "__main__":
    unittest.main()
