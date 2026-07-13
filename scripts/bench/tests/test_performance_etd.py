from __future__ import annotations

import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

BENCH_DIR = Path(__file__).resolve().parents[1]
if str(BENCH_DIR) not in sys.path:
    sys.path.insert(0, str(BENCH_DIR))

import performance_fixtures as fixtures
import performance_scenarios as scenarios


class FakeClock:
    def __init__(self, start_ns: int = 1_000_000_000) -> None:
        self._now = start_ns
        self._lock = threading.Lock()

    def __call__(self) -> int:
        with self._lock:
            self._now += 1_000_000
            return self._now

    def sleep(self, seconds: float) -> None:
        with self._lock:
            self._now += max(1, int(seconds * 1_000_000_000))

    def advance_ms(self, milliseconds: float) -> None:
        with self._lock:
            self._now += int(milliseconds * 1_000_000)

    def peek(self) -> int:
        with self._lock:
            return self._now


def visible_result(event: dict) -> dict:
    identities = event["oracle"]["ordered_identities"]
    return {
        "schema_version": fixtures.SEARCH_SCHEMA_VERSION,
        "tool": fixtures.SEARCH_TOOL,
        "data": {
            "result_count": len(identities),
            "truncated": event["oracle"]["truncated"],
            "results": [
                {
                    "event": {"id": identity["event_id"]},
                    "session": {"id": identity["session_id"]},
                }
                for identity in identities
            ],
        },
    }


class AckReader:
    def __init__(self, clock: FakeClock, *digests: str) -> None:
        self.clock = clock
        self.digests = list(digests)
        self.yielded = False

    def __call__(self) -> list[dict]:
        if self.yielded:
            return []
        self.yielded = True
        return [
            {
                "batch_sequence": 1,
                "event_identity_digests": self.digests,
                "ack_monotonic_ns": max(0, self.clock.peek() - 1_000_000),
            }
        ]


class DurablePublicationTests(unittest.TestCase):
    def test_same_directory_stage_is_invisible_until_file_and_directory_fsync(self) -> None:
        clock = FakeClock()
        with tempfile.TemporaryDirectory() as root:
            original_replace = os.replace
            original_fsync = os.fsync
            calls: list[tuple[str, object]] = []

            def checked_replace(source: str, destination: str) -> None:
                calls.append(("rename", (source, destination)))
                self.assertEqual(Path(source).parent, Path(destination).parent)
                self.assertTrue(source.endswith(".pending"))
                self.assertFalse(source.endswith(".jsonl"))
                self.assertEqual(list(Path(root).glob("*.jsonl")), [])
                original_replace(source, destination)

            def checked_fsync(fd: int) -> None:
                calls.append(("fsync", fd))
                original_fsync(fd)

            with mock.patch.object(scenarios.os, "replace", side_effect=checked_replace), mock.patch.object(
                scenarios.os, "fsync", side_effect=checked_fsync
            ):
                evidence = scenarios.publish_event_durably(
                    root, "event.jsonl", b'{"event":"fixture"}\n', clock_ns=clock
                )

            self.assertGreaterEqual(sum(name == "fsync" for name, _ in calls), 3)
            rename_index = next(index for index, call in enumerate(calls) if call[0] == "rename")
            self.assertTrue(any(call[0] == "fsync" for call in calls[:rename_index]))
            self.assertTrue(any(call[0] == "fsync" for call in calls[rename_index + 1 :]))
            self.assertGreaterEqual(evidence.publication_durable_ns, evidence.t0_ns)
            self.assertEqual(Path(root, "event.jsonl").read_bytes(), b'{"event":"fixture"}\n')
            self.assertEqual(list(Path(root).glob("*.pending")), [])

    def test_rename_and_each_durability_boundary_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with mock.patch.object(scenarios.os, "replace", side_effect=OSError("rename")):
                with self.assertRaisesRegex(scenarios.EtdSampleFailure, "publication_rename_failed"):
                    scenarios.publish_event_durably(root, "rename.jsonl", b"x")
            self.assertEqual(list(Path(root).iterdir()), [])

        for failure_number, expected in (
            (1, "publication_file_fsync_failed"),
            (3, "publication_destination_directory_fsync_failed"),
        ):
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as root:
                original_fsync = os.fsync
                calls = 0

                def failing_fsync(fd: int) -> None:
                    nonlocal calls
                    calls += 1
                    if calls == failure_number:
                        raise OSError("fsync")
                    original_fsync(fd)

                with mock.patch.object(scenarios.os, "fsync", side_effect=failing_fsync):
                    with self.assertRaisesRegex(scenarios.EtdSampleFailure, expected):
                        scenarios.publish_event_durably(root, "event.jsonl", b"x")


class EtdScenarioTests(unittest.TestCase):
    def setUp(self) -> None:
        recipe = fixtures.build_recipe("smoke")
        self.event = recipe["event_splits"]["research"][0]
        self.schedule = [{"case_id": self.event["case_id"], "scheduled_offset_ns": 0}]

    def run_event(
        self,
        probe,
        *,
        event: dict | None = None,
        ack_reader=None,
        timeout_s: float = 0.05,
        mode: str = "idle",
        query_load=None,
        baseline_qps=None,
        watcher_ready=lambda: True,
    ):
        selected = event or self.event
        clock = FakeClock()
        reader = ack_reader or AckReader(clock, selected["expected_ack_digest"])
        with tempfile.TemporaryDirectory() as root:
            result = scenarios.run_etd_scenario(
                [selected],
                [{"case_id": selected["case_id"], "scheduled_offset_ns": 0}],
                root,
                probe(clock, selected),
                reader,
                watcher_ready,
                mode=mode,
                timeout_s=timeout_s,
                poll_interval_s=0.001,
                baseline_sustainable_qps=baseline_qps,
                query_load=query_load,
                clock_ns=clock,
                sleeper=clock.sleep,
            )
        return result

    @staticmethod
    def miss_then_hit(clock: FakeClock, event: dict):
        calls = 0

        def probe(_query):
            nonlocal calls
            calls += 1
            clock.advance_ms(4)
            if calls == 1:
                return scenarios.EtdProbeResult(False, {}, 4.0)
            return scenarios.EtdProbeResult(True, visible_result(event), 4.0)

        return probe

    @staticmethod
    def immediate_hit(clock: FakeClock, event: dict):
        def probe(_query):
            clock.advance_ms(2)
            return scenarios.EtdProbeResult(True, visible_result(event), 2.0)

        return probe

    @staticmethod
    def always_miss(clock: FakeClock, _event: dict):
        def probe(_query):
            clock.advance_ms(1)
            return scenarios.EtdProbeResult(False, {}, 1.0)

        return probe

    def test_polling_records_interval_bounds_and_uses_each_cache_key_once(self) -> None:
        result = self.run_event(self.miss_then_hit)
        self.assertEqual(result.status, "pass")
        sample = result.samples[0]
        self.assertTrue(sample["valid"])
        self.assertEqual(sample["term_use_count"], 2)
        self.assertEqual(sample["source_interval"]["censoring"], "interval")
        self.assertLessEqual(
            sample["source_interval"]["lower_ms"], sample["source_interval"]["upper_ms"]
        )
        self.assertEqual(set(sample["cache_bypass"]), set(scenarios.CACHE_BYPASS_KEYS))
        self.assertTrue(all(sample["cache_bypass"].values()))
        self.assertEqual(
            sample["event_identity_sha256"],
            "sha256:" + self.event["expected_ack_digest"],
        )
        self.assertEqual(result.metrics["event_count"], 1)

    def test_immediate_post_ack_hit_is_left_censored_not_invented_uncensored_zero(self) -> None:
        result = self.run_event(self.immediate_hit)
        self.assertEqual(result.status, "pass")
        sample = result.samples[0]
        self.assertEqual(sample["source_interval"]["censoring"], "left")
        self.assertEqual(sample["db_ack_interval"]["censoring"], "left")
        self.assertEqual(sample["db_ack_interval"]["lower_ms"], 0.0)
        self.assertIsNotNone(sample["db_ack_interval"]["upper_ms"])

    def test_watcher_readiness_prevents_any_publication(self) -> None:
        probe_called = False

        def factory(_clock, _event):
            def probe(_query):
                nonlocal probe_called
                probe_called = True
                raise AssertionError("probe must not run")

            return probe

        result = self.run_event(factory, watcher_ready=lambda: False)
        self.assertEqual(result.status, "fail")
        self.assertEqual(result.samples[0]["error_code"], "watcher_not_ready")
        self.assertIsNone(result.samples[0]["publication_durable_ms"])
        self.assertFalse(probe_called)

    def test_cache_bank_exhaustion_and_timeout_are_distinct_right_censors(self) -> None:
        exhausted = dict(self.event)
        exhausted["probe_terms"] = [self.event["probe_terms"][0]]
        result = self.run_event(self.always_miss, event=exhausted, timeout_s=1.0)
        self.assertEqual(result.samples[0]["error_code"], "cache_bank_exhausted")
        self.assertEqual(result.samples[0]["source_interval"]["censoring"], "right")
        self.assertIsNone(result.samples[0]["source_interval"]["upper_ms"])

        timed_out = self.run_event(self.always_miss, timeout_s=0.003)
        self.assertEqual(timed_out.samples[0]["error_code"], "visibility_timeout")
        self.assertEqual(timed_out.metrics["source_etd_p95"]["censoring"], "right")

    def test_materialized_insert_ack_failure_does_not_claim_visibility_success(self) -> None:
        result = self.run_event(self.immediate_hit, ack_reader=lambda: [], timeout_s=0.005)
        self.assertEqual(result.status, "fail")
        self.assertEqual(result.samples[0]["error_code"], "missing_db_ack")
        self.assertIsNone(result.samples[0]["db_ack_ms"])

    def test_exact_identity_mismatch_is_a_semantic_failure(self) -> None:
        wrong = visible_result(self.event)
        wrong["data"]["results"][0]["event"]["id"] = "event:wrong"

        def factory(clock, _event):
            def probe(_query):
                clock.advance_ms(1)
                return scenarios.EtdProbeResult(True, wrong, 1.0)

            return probe

        result = self.run_event(factory)
        self.assertEqual(result.status, "fail")
        self.assertEqual(result.semantic_failures, 1)
        self.assertEqual(result.samples[0]["error_code"], "identity_mismatch")
        self.assertIsNone(result.samples[0]["first_valid_ms"])

    def test_wall_clock_jumps_cannot_change_monotonic_bounds(self) -> None:
        with mock.patch.object(time, "time", side_effect=[10_000.0, -10_000.0]):
            result = self.run_event(self.miss_then_hit)
        self.assertEqual(result.status, "pass")
        self.assertGreater(result.samples[0]["first_valid_ms"], 0.0)

    def test_loaded_arm_is_exactly_seventy_five_percent_and_overlaps_schedule(self) -> None:
        observed_rates: list[float] = []

        def load(rate: float) -> dict:
            observed_rates.append(rate)
            return {
                "offered_qps": rate,
                "planned": 1,
                "started": 1,
                "completed": 1,
                "scheduler_p99_slip_ms": 0.0,
                "schedule_delivered": True,
                "drained": True,
                "backlog": 0,
                "first_started_ns": 0,
                "last_completed_ns": 10**18,
                "failures": {},
                "semantic_failures": 0,
            }

        result = self.run_event(
            self.immediate_hit,
            mode="loaded",
            query_load=load,
            baseline_qps=80.0,
        )
        self.assertEqual(result.status, "pass")
        self.assertEqual(observed_rates, [60.0])


class AckCursorTests(unittest.TestCase):
    def test_one_batch_correlates_multiple_digests_to_one_ack_bound(self) -> None:
        first = "a" * 64
        second = "b" * 64
        observations = [
            {
                "batch_sequence": 1,
                "event_identity_digests": [first, second],
                "ack_monotonic_ns": 123,
            }
        ]
        cursor = scenarios.IngestAckCursor(lambda: [observations.pop(0)] if observations else [])
        first_match = cursor.matches(first)[0]
        second_match = cursor.matches(second)[0]
        self.assertEqual(first_match.batch_sequence, second_match.batch_sequence)
        self.assertEqual(first_match.ack_monotonic_ns, second_match.ack_monotonic_ns)

    def test_non_allowlisted_content_and_nonmonotonic_sequence_are_rejected(self) -> None:
        digest = "a" * 64
        cursor = scenarios.IngestAckCursor(
            lambda: [
                {
                    "batch_sequence": 1,
                    "event_identity_digests": [digest],
                    "ack_monotonic_ns": 1,
                    "source_path": "/forbidden",
                }
            ]
        )
        with self.assertRaisesRegex(scenarios.ScenarioError, "non-allowlisted"):
            cursor.matches(digest)

        batches = iter(
            [
                [{"batch_sequence": 1, "event_identity_digests": [digest], "ack_monotonic_ns": 2}],
                [{"batch_sequence": 3, "event_identity_digests": [digest], "ack_monotonic_ns": 3}],
            ]
        )
        cursor = scenarios.IngestAckCursor(lambda: next(batches, []))
        cursor.matches(digest)
        with self.assertRaisesRegex(scenarios.ScenarioError, "sequence"):
            cursor.matches("b" * 64)


class OwnedSandboxAdapterTests(unittest.TestCase):
    def test_ack_reader_uses_stable_cursor_and_fails_closed_on_gap(self) -> None:
        observation = mock.Mock(
            batch_sequence=1,
            event_identity_digests=("a" * 64, "b" * 64),
            ack_monotonic_ns=42,
        )
        sandbox = mock.Mock()
        sandbox.read_ingest_ack_logs.side_effect = [
            mock.Mock(next_cursor=1, observations=(observation,), gap_detected=False),
            mock.Mock(next_cursor=0, observations=(), gap_detected=True),
        ]
        reader = scenarios.OwnedSandboxAckReader(sandbox)
        self.assertEqual(
            reader(),
            (
                {
                    "batch_sequence": 1,
                    "event_identity_digests": ("a" * 64, "b" * 64),
                    "ack_monotonic_ns": 42,
                },
            ),
        )
        self.assertEqual(sandbox.read_ingest_ack_logs.call_args_list[0], mock.call(0))
        with self.assertRaisesRegex(scenarios.ScenarioError, "cursor was truncated"):
            reader()
        self.assertEqual(sandbox.read_ingest_ack_logs.call_args_list[1], mock.call(1))

    def test_etd_runtime_wires_owned_readiness_ack_and_central_probe(self) -> None:
        observation = mock.Mock(
            batch_sequence=1,
            event_identity_digests=("a" * 64,),
            ack_monotonic_ns=42,
        )
        sandbox = mock.Mock()
        sandbox.watched_source_dir = Path("/owned/watched")
        sandbox.spawn_stdio_route.return_value = object()
        sandbox.wait_watcher_ready.return_value = mock.Mock(files_watched=2)
        sandbox.read_ingest_ack_logs.return_value = mock.Mock(
            next_cursor=1,
            observations=(observation,),
            gap_detected=False,
        )
        client = mock.Mock()
        client.wait_for_central_route.return_value = "proxying stdio to central MCP server"
        client.list_tools.return_value = ("search_sessions",)
        client.call.return_value = {"structuredContent": {"data": {"result_count": 1}}}
        client.decode_search_result.return_value = {"data": {"result_count": 1}}
        clock = FakeClock()
        with mock.patch.object(
            scenarios, "_StdioJsonRpcClient", return_value=client
        ):
            runtime = scenarios.make_owned_sandbox_etd_runtime(
                sandbox, timeout_s=3.0, clock_ns=clock
            )
            self.assertEqual(runtime.watched_dir, Path("/owned/watched"))
            self.assertTrue(runtime.watcher_ready())
            self.assertEqual(runtime.ack_reader()[0]["batch_sequence"], 1)
            probe = runtime.probe({"query": "one-use"})
            self.assertTrue(probe.visible)
            runtime.close()
        sandbox.wait_watcher_ready.assert_called_once_with(timeout_s=3.0)
        client.call.assert_called_once_with(
            "tools/call",
            {"name": "search_sessions", "arguments": {"query": "one-use"}},
            3.0,
        )
        client.close.assert_called_once()

    def test_owned_query_load_runs_fixed_schedule_and_is_one_use(self) -> None:
        sandbox = mock.Mock()
        sandbox.spawn_stdio_route.return_value = object()
        client = mock.Mock()
        client.wait_for_central_route.return_value = "proxying stdio to central MCP server"
        client.list_tools.return_value = ("search_sessions",)
        client.search.return_value = {"data": {"result_count": 1}}
        clock = FakeClock()
        with (
            mock.patch.object(
                scenarios, "_StdioJsonRpcClient", return_value=client
            ),
            mock.patch.object(scenarios, "validate_query_oracle"),
        ):
            runner = scenarios.OwnedSandboxQueryLoad(
                sandbox,
                {"q": {"arguments": {}}},
                ({"case_id": "q", "sequence": 0, "scheduled_offset_ns": 0},),
                offered_qps=4.0,
                timeout_s=2.0,
                clock_ns=clock,
                sleeper=clock.sleep,
            )
            runner.prepare()
            evidence = runner(4.0)
            self.assertTrue(evidence["schedule_delivered"])
            self.assertTrue(evidence["drained"])
            self.assertEqual(evidence["backlog"], 0)
            self.assertEqual(evidence["failures"]["semantic"], 0)
            with self.assertRaisesRegex(scenarios.ScenarioError, "one-use"):
                runner(4.0)


if __name__ == "__main__":
    unittest.main()
