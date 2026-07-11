#!/usr/bin/env python3

from __future__ import annotations

import argparse
import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import benchmark_protocol
import replay_search_latency as benchmark


class FakeChild:
    def __init__(
        self, *, returncode: int = 0, communicate_error: BaseException | None = None
    ) -> None:
        self.returncode = returncode
        self.pid = 1234
        self.communicate_error = communicate_error

    def communicate(self, timeout: int) -> tuple[str, str]:
        if self.communicate_error is not None:
            raise self.communicate_error
        return ("stdout", "stderr")


class ReplaySearchLatencyTests(unittest.TestCase):
    def args(self, *extra: str) -> argparse.Namespace:
        return benchmark.parse_args(["--config", "fixture.toml", *extra])

    def corpus_fingerprint(self) -> dict[str, object]:
        return {"fingerprint": "sha256:" + "f" * 64, "cardinality": 47}

    def spec(self, query: str = "private search terms") -> benchmark.ReplaySpec:
        return benchmark.ReplaySpec(
            rank=1,
            variant_label="orig",
            variant_term_count=3,
            ts="2026-07-10 00:00:00",
            query_id="query-1",
            source="mcp",
            session_hint="session-private",
            raw_query=query,
            baseline_response_ms=12.0,
            arguments={
                "query": query,
                "limit": 3,
                "min_should_match": 1,
                "min_score": 0.1,
                "include_tool_events": True,
                "exclude_codex_mcp": False,
                "session_id": "session-private",
            },
        )

    def result(
        self,
        samples: list[float],
        failures: list[dict[str, object]] | None = None,
        oracle_quality: dict[str, object] | None = None,
    ) -> dict[str, object]:
        return {
            "measured_samples_ms": samples,
            "failures": failures or [],
            "oracle_quality": oracle_quality or {"enabled": True, "status": "ok"},
        }

    def aggregate(
        self,
        *,
        quality: dict[str, object] | None = None,
    ) -> dict[str, object]:
        return {
            "quality": quality
            or {
                "enabled": True,
                "evaluated_case_count": 1,
                "unstable_case_count": 0,
                "error_count": 0,
                "pass_count": 1,
                "regression_count": 0,
            },
            "memory": {
                "benchmark_rss_bytes": {"start": 100, "end": 120, "delta": 20},
                "benchmark_peak_rss_bytes": {"start": 200, "end": 230, "delta": 30},
            },
        }

    def payload(
        self,
        *,
        args: argparse.Namespace | None = None,
        samples: list[float] | None = None,
        failures: list[dict[str, object]] | None = None,
        aggregate: dict[str, object] | None = None,
        failure_counts: dict[str, int] | None = None,
    ) -> dict[str, object]:
        chosen_args = args or self.args()
        chosen_samples = samples if samples is not None else [1.0] * chosen_args.repeats
        with mock.patch.object(
            benchmark,
            "git_metadata",
            return_value={"commit": "a" * 40, "dirty": False},
        ):
            return benchmark.build_output_json(
                args=chosen_args,
                replay_specs=[self.spec()],
                replay_results=[self.result(chosen_samples, failures)],
                aggregate=aggregate or self.aggregate(),
                failures=failure_counts or {"timeouts": 0, "errors": 0},
                dry_run=False,
                corpus_fingerprint=self.corpus_fingerprint(),
                repo_root=Path("/repo"),
            )

    def test_profile_defaults_and_explicit_overrides(self) -> None:
        full = self.args()
        self.assertEqual((full.profile, full.top_n, full.warmup, full.repeats), ("full", 20, 1, 5))

        smoke = self.args("--profile", "smoke")
        self.assertEqual((smoke.profile, smoke.top_n, smoke.warmup, smoke.repeats), ("smoke", 2, 0, 1))
        self.assertEqual(smoke.oracle_recall_at_k_threshold, 1.0)
        self.assertEqual(smoke.oracle_ndcg_at_k_threshold, 0.99)

        overridden = self.args(
            "--profile",
            "smoke",
            "--top-n",
            "4",
            "--warmup",
            "2",
            "--repeats",
            "3",
            "--oracle-recall-at-k-threshold",
            "0.8",
        )
        self.assertEqual((overridden.top_n, overridden.warmup, overridden.repeats), (4, 2, 3))
        self.assertEqual(overridden.oracle_recall_at_k_threshold, 0.8)

    def test_invalid_arguments_fail_closed(self) -> None:
        invalid_argv = (
            ("--profile", "unknown"),
            ("--repeats", "0"),
            ("--request-source", "Not A Slug"),
            ("--oracle-ndcg-at-k-threshold", "1.1"),
            ("--build-timeout-seconds", "0"),
        )
        for extra in invalid_argv:
            with (
                self.subTest(extra=extra),
                contextlib.redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit),
            ):
                self.args(*extra)

    def test_protocol_artifact_validates_and_redacts_sensitive_material(self) -> None:
        payload = self.payload()
        benchmark_protocol.validate_artifact(payload)

        serialized = json.dumps(payload, sort_keys=True)
        self.assertNotIn("private search terms", serialized)
        self.assertNotIn("session-private", serialized)
        self.assertNotIn("fixture.toml", serialized)
        self.assertEqual(payload["samples"]["measurements"]["latency_ms"], [1.0] * 5)
        self.assertEqual(payload["samples"]["successful"], 5)
        self.assertEqual(payload["metrics"]["quality"]["error_rate"], 0.0)
        self.assertEqual(payload["timing"], {"status": "not_evaluated", "non_blocking": True})
        self.assertEqual(payload["semantic"]["status"], "pass")
        dataset = payload["scenario"]["fingerprints"]["dataset"]
        self.assertEqual(dataset["cardinality"], 47)
        self.assertRegex(dataset["fingerprint"], r"^sha256:[0-9a-f]{64}$")
        self.assertEqual(payload["scenario"]["fingerprints"]["cache_state"], "disabled")
        self.assertEqual(payload["scenario"]["fingerprints"]["request_source"], "benchmark-replay")
        self.assertFalse(payload["scenario"]["dimensions"]["concurrent"])

    def test_smoke_records_latency_but_never_latency_gates(self) -> None:
        payload = self.payload(args=self.args("--profile", "smoke"), samples=[2.5])
        self.assertEqual(payload["samples"]["measurements"]["latency_ms"], [2.5])
        self.assertEqual(payload["semantic"]["status"], "pass")
        self.assertEqual(payload["timing"], {"status": "not_evaluated", "non_blocking": True})

    def test_malformed_measured_response_is_an_error_after_timing_when_oracle_disabled(
        self,
    ) -> None:
        args = self.args(
            "--profile",
            "smoke",
            "--query-variant-mode",
            "none",
            "--no-oracle-quality-check",
            "--skip-maturin-develop",
            "--output-json",
            "result.json",
        )
        events: list[str] = []
        clock_values = iter((100, 200))
        emitted: list[dict[str, object]] = []
        original_validate = benchmark.validate_search_response

        class ErrorPayloadClient:
            def search(self, arguments: dict[str, object]) -> str:
                events.append("search")
                return '{"error":{"code":"backend_failed"},"hits":[],"stats":{}}'

        def clock() -> int:
            events.append("timer")
            return next(clock_values)

        def validate(payload: object) -> dict[str, object]:
            events.append("validate")
            return original_validate(payload)

        def emit(_: str | None, payload: dict[str, object]) -> bool:
            emitted.append(payload)
            return True

        with (
            mock.patch.object(benchmark, "parse_args", return_value=args),
            mock.patch.object(
                benchmark,
                "read_config",
                return_value=benchmark.ClickHouseSettings(
                    url="http://localhost:8123",
                    database="moraine",
                    username="default",
                    password="",
                    timeout_seconds=1.0,
                ),
            ),
            mock.patch.object(
                benchmark, "clickhouse_query_json_each_row", return_value=[{}]
            ),
            mock.patch.object(
                benchmark,
                "search_corpus_fingerprint",
                return_value=self.corpus_fingerprint(),
            ),
            mock.patch.object(
                benchmark, "normalize_rows", return_value=([{}], [self.spec()])
            ),
            mock.patch.object(
                benchmark, "expand_replay_specs", return_value=[self.spec()]
            ),
            mock.patch.object(benchmark, "print_selection_summary"),
            mock.patch.object(benchmark, "ensure_binding_python_source_on_syspath"),
            mock.patch.object(benchmark, "load_conversation_client_class", return_value=object),
            mock.patch.object(benchmark, "PackageSearchClient", return_value=ErrorPayloadClient()),
            mock.patch.object(benchmark.time, "perf_counter_ns", side_effect=clock),
            mock.patch.object(benchmark, "validate_search_response", side_effect=validate),
            mock.patch.object(
                benchmark,
                "git_metadata",
                return_value={"commit": "a" * 40, "dirty": False},
            ),
            mock.patch.object(benchmark, "emit_output_json", side_effect=emit),
            contextlib.redirect_stdout(io.StringIO()),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            status = benchmark.main()

        self.assertEqual(status, 1)
        self.assertEqual(events, ["timer", "search", "timer", "validate"])
        self.assertEqual(emitted[0]["samples"]["attempted"], 1)
        self.assertEqual(emitted[0]["samples"]["successful"], 0)
        self.assertEqual(emitted[0]["samples"]["errors"], 1)
        self.assertEqual(emitted[0]["semantic"]["status"], "fail")

    def test_search_corpus_content_and_cardinality_define_dataset_identity(self) -> None:
        cfg = benchmark.ClickHouseSettings(
            url="http://localhost:8123",
            database="moraine",
            username="default",
            password="",
            timeout_seconds=1.0,
        )
        first_row = {"cardinality": "2", "content_xor": "11", "content_sum": "17"}
        changed_row = {"cardinality": "2", "content_xor": "12", "content_sum": "18"}
        with mock.patch.object(
            benchmark,
            "clickhouse_query_json_each_row",
            side_effect=([first_row], [changed_row]),
        ) as query:
            first = benchmark.search_corpus_fingerprint(cfg)
            changed = benchmark.search_corpus_fingerprint(cfg)

        self.assertEqual(first["cardinality"], 2)
        self.assertNotEqual(first["fingerprint"], changed["fingerprint"])
        sql = query.call_args_list[0].args[1]
        self.assertIn("moraine.search_documents FINAL", sql)
        self.assertIn("text_content", sql)


    def test_output_is_atomic_and_schema_validated(self) -> None:
        payload = self.payload()
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "nested" / "result.json"
            benchmark.write_output_json(output, payload)
            self.assertEqual(json.loads(output.read_text(encoding="utf-8")), payload)
            self.assertEqual(list(output.parent.glob(f".{output.name}.*.tmp")), [])

    def test_invalid_schema_is_rejected_before_output_creation(self) -> None:
        payload = self.payload()
        payload["schema_version"] = "unknown"
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "result.json"
            with self.assertRaises(benchmark_protocol.ProtocolError):
                benchmark.write_output_json(output, payload)
            self.assertFalse(output.exists())

    def test_output_replace_failure_preserves_old_file_and_cleans_temp(self) -> None:
        payload = self.payload()
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "result.json"
            output.write_text("old\n", encoding="utf-8")
            with mock.patch.object(
                benchmark.benchmark_protocol.os,
                "replace",
                side_effect=OSError("disk failure"),
            ):
                with self.assertRaises(OSError):
                    benchmark.write_output_json(output, payload)
            self.assertEqual(output.read_text(encoding="utf-8"), "old\n")
            self.assertEqual(list(output.parent.glob(f".{output.name}.*.tmp")), [])

    def test_child_spawn_and_crash_failures_are_reported(self) -> None:
        with mock.patch.object(benchmark.subprocess, "Popen", side_effect=OSError("spawn failed")):
            with self.assertRaisesRegex(OSError, "spawn failed"):
                benchmark.run_owned_child(["child"], Path("/repo"), 1)

        child = FakeChild(returncode=9)
        with mock.patch.object(benchmark.subprocess, "Popen", return_value=child):
            result = benchmark.run_owned_child(["child"], Path("/repo"), 1)
        self.assertEqual(result.returncode, 9)

    def test_maturin_child_crash_is_redacted_and_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest = root / "bindings" / "python" / "moraine_conversations" / "Cargo.toml"
            manifest.parent.mkdir(parents=True)
            manifest.write_text("[package]\nname = \"fixture\"\n", encoding="utf-8")
            crashed = subprocess.CompletedProcess(
                ["maturin"], 7, stdout="sensitive stdout", stderr="secret stderr"
            )
            with (
                mock.patch.object(benchmark.shutil, "which", return_value="/bin/maturin"),
                mock.patch.object(benchmark, "run_owned_child", return_value=crashed),
            ):
                with self.assertRaisesRegex(RuntimeError, r"exit code 7") as raised:
                    benchmark.ensure_local_python_binding(root, timeout_seconds=1)
            self.assertNotIn("sensitive", str(raised.exception))
            self.assertNotIn("secret", str(raised.exception))

    def test_child_timeout_stops_owned_process(self) -> None:
        child = FakeChild(
            communicate_error=subprocess.TimeoutExpired(cmd=["child"], timeout=1)
        )
        with (
            mock.patch.object(benchmark.subprocess, "Popen", return_value=child),
            mock.patch.object(benchmark, "stop_owned_child") as stop,
        ):
            with self.assertRaisesRegex(RuntimeError, "timed out and was stopped"):
                benchmark.run_owned_child(["child"], Path("/repo"), 1)
        stop.assert_called_once_with(child)

    def test_interrupt_and_communicate_error_stop_owned_process(self) -> None:
        for primary in (KeyboardInterrupt(), OSError("communicate failed")):
            child = FakeChild(communicate_error=primary)
            with (
                self.subTest(primary=type(primary).__name__),
                mock.patch.object(benchmark.subprocess, "Popen", return_value=child),
                mock.patch.object(benchmark, "stop_owned_child") as stop,
                self.assertRaises(type(primary)) as raised,
            ):
                benchmark.run_owned_child(["child"], Path("/repo"), 1)
            self.assertIs(raised.exception, primary)
            stop.assert_called_once_with(child)

    def test_stop_owned_child_signals_entire_group_and_reaps_leader(self) -> None:
        class ReapChild:
            pid = 4321

            def __init__(self) -> None:
                self.returncode: int | None = None
                self.wait_calls = 0

            def poll(self) -> int | None:
                return self.returncode

            def wait(self, timeout: float) -> int:
                self.wait_calls += 1
                self.returncode = 0
                return 0

        child = ReapChild()
        with (
            mock.patch.object(benchmark.os, "name", "posix"),
            mock.patch.object(benchmark.os, "killpg") as killpg,
        ):
            benchmark.stop_owned_child(child)

        self.assertEqual(
            killpg.call_args_list,
            [
                mock.call(child.pid, benchmark.signal.SIGTERM),
                mock.call(child.pid, benchmark.signal.SIGKILL),
            ],
        )
        self.assertEqual(child.wait_calls, 1)

    def test_child_cleanup_failure_preserves_primary_and_cleanup_errors(self) -> None:
        primary = KeyboardInterrupt()
        cleanup = OSError("cannot kill")
        child = FakeChild(communicate_error=primary)
        with (
            mock.patch.object(benchmark.subprocess, "Popen", return_value=child),
            mock.patch.object(benchmark, "stop_owned_child", side_effect=cleanup),
            self.assertRaises(benchmark.OwnedChildCleanupError) as raised,
        ):
            benchmark.run_owned_child(["child"], Path("/repo"), 1)
        self.assertIs(raised.exception.primary_error, primary)
        self.assertIs(raised.exception.cleanup_error, cleanup)
        self.assertRegex(str(raised.exception), "KeyboardInterrupt.*cleanup failed.*OSError")

    def test_insufficient_samples_preserve_counts_and_error_rate(self) -> None:
        failure = {"type": "timeout", "phase": "measured", "iteration": 3}
        payload = self.payload(
            samples=[3.0, 4.0],
            failures=[failure],
            failure_counts={"timeouts": 1, "errors": 0},
        )
        self.assertEqual(payload["samples"]["planned"], 5)
        self.assertEqual(payload["samples"]["attempted"], 3)
        self.assertEqual(payload["samples"]["successful"], 2)
        self.assertEqual(payload["samples"]["errors"], 1)
        self.assertAlmostEqual(payload["metrics"]["quality"]["error_rate"], 1.0 / 3.0)
        self.assertEqual(payload["semantic"]["status"], "fail")
        self.assertIn({"code": "insufficient-successful-samples"}, payload["diagnostics"])
        self.assertIn({"code": "request-timeout"}, payload["diagnostics"])
        self.assertEqual(payload["timing"]["status"], "not_evaluated")

    def test_oracle_regression_is_semantic_failure_not_latency_gate(self) -> None:
        quality = {
            "enabled": True,
            "evaluated_case_count": 1,
            "unstable_case_count": 0,
            "error_count": 0,
            "pass_count": 0,
            "regression_count": 1,
        }
        payload = self.payload(aggregate=self.aggregate(quality=quality))
        self.assertEqual(payload["semantic"]["status"], "fail")
        self.assertEqual(payload["metrics"]["quality"]["oracle_status"], "fail")
        self.assertEqual(payload["metrics"]["quality"]["mismatches"], 1)
        self.assertEqual(payload["timing"], {"status": "not_evaluated", "non_blocking": True})
        self.assertIn({"code": "oracle-regression"}, payload["diagnostics"])

    def test_comparator_rejects_smoke_and_full_profiles(self) -> None:
        smoke = self.payload(args=self.args("--profile", "smoke"))
        full = self.payload(args=self.args("--profile", "full"))
        with self.assertRaises(benchmark_protocol.IncomparableError):
            benchmark_protocol.compare_artifacts(smoke, full)

    def test_unstable_oracle_is_reported_without_false_regression(self) -> None:
        optimized = {"hits": [{"event_uid": "a"}]}
        primary = {"hits": [{"event_uid": "a"}]}
        changed = {"hits": [{"event_uid": "b"}]}
        quality = benchmark.evaluate_oracle_quality(
            optimized_payload=optimized,
            oracle_payload=primary,
            top_k=1,
            recall_threshold=1.0,
            ndcg_threshold=0.99,
            min_stability_recall=0.95,
            min_stability_ndcg=0.98,
            oracle_recheck_payloads=[changed],
        )
        self.assertTrue(quality["unstable_oracle_window"])
        self.assertTrue(quality["passes_gate"])
        summary = benchmark.summarize_oracle_quality(
            [{"oracle_quality": quality}],
            recall_threshold=1.0,
            ndcg_threshold=0.99,
        )
        self.assertEqual(summary["unstable_case_count"], 1)
        self.assertEqual(summary["regression_count"], 0)


if __name__ == "__main__":
    unittest.main()
