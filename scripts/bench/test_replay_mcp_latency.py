from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import ExitStack, redirect_stderr
from pathlib import Path
from unittest.mock import patch

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import benchmark_protocol
import replay_mcp_latency as bench


class FakeProcess:
    def __init__(self, returncode: int | None = None) -> None:
        self.returncode = returncode
        self.stdin = None
        self.terminated = False
        self.killed = False

    def poll(self) -> int | None:
        return self.returncode

    def terminate(self) -> None:
        self.terminated = True
        self.returncode = 0

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        return self.returncode or 0


class ReplayMcpLatencyTests(unittest.TestCase):
    def args(self, *extra: str) -> object:
        return bench.parse_args(["--config", "fixture.toml", *extra])

    def corpus(
        self,
        fingerprint: str = "1" * 64,
        cardinality: int = 3,
    ) -> bench.CorpusIdentity:
        return bench.CorpusIdentity(
            fingerprint=f"sha256:{fingerprint}",
            cardinality=cardinality,
        )

    def spec(self, query: str = "private benchmark query") -> bench.ReplaySpec:
        return bench.ReplaySpec(
            rank=1,
            variant_label="orig",
            variant_term_count=3,
            ts="2026-07-10T00:00:00Z",
            query_id="query-1",
            source="test",
            session_hint="",
            raw_query=query,
            baseline_response_ms=4.0,
            arguments={
                "query": query,
                "limit": 10,
                "min_should_match": 1,
                "min_score": 0.0,
                "include_tool_events": False,
                "exclude_codex_mcp": True,
            },
        )

    def successful_target(self, latency_ms: float = 2.5) -> dict[str, object]:
        return {
            "planned": 1,
            "attempted": 1,
            "successful": 1,
            "errors": 0,
            "raw_e2e_search_ms": [latency_ms],
            "failures": bench.new_failure_counts(),
        }

    def run_persistent(self, args: object, call_effect: object, *, cleanup=None):
        proc = FakeProcess()
        with ExitStack() as stack:
            stack.enter_context(patch.object(bench, "start_mcp_process", return_value=proc))
            stack.enter_context(
                patch.object(bench, "initialize_mcp", return_value=(2, 1.0))
            )
            stack.enter_context(patch.object(bench, "call_tool", side_effect=call_effect))
            stack.enter_context(
                patch.object(bench, "stop_mcp_process", return_value=cleanup)
            )
            return bench.run_target_persistent(
                args=args,
                config_path=Path("fixture.toml"),
                tool="search",
                specs=[self.spec()],
            )

    def test_profile_defaults_and_full_overrides_are_deterministic(self) -> None:
        full = self.args()
        self.assertEqual(
            (full.profile, full.top_n, full.warmup, full.repeats, full.query_variant_mode, full.tool),
            ("full", 20, 1, 5, "subset_scramble", "both"),
        )

        overridden = self.args(
            "--top-n", "3", "--warmup", "0", "--repeats", "2",
            "--query-variant-mode", "none", "--tool", "search",
        )
        self.assertEqual(
            (overridden.top_n, overridden.warmup, overridden.repeats, overridden.query_variant_mode, overridden.tool),
            (3, 0, 2, "none", "search"),
        )

        smoke = self.args("--profile", "smoke")
        self.assertEqual(
            (smoke.top_n, smoke.warmup, smoke.repeats, smoke.query_variant_mode, smoke.tool),
            (1, 0, 1, "none", "search"),
        )

    def test_smoke_rejects_contradictory_workload_and_non_execution(self) -> None:
        for extra in (
            ("--profile", "smoke", "--repeats", "2"),
            ("--profile", "smoke", "--tool", "both"),
            ("--profile", "smoke", "--include-benchmark-replays"),
            ("--profile", "smoke", "--dry-run"),
        ):
            with self.subTest(extra=extra), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    self.args(*extra)

    def test_invalid_counts_and_threshold_flags_fail_closed(self) -> None:
        for extra in (
            ("--repeats", "0"),
            ("--warmup", "-1"),
            ("--timeout-seconds", "0"),
            ("--maximum-relative-regression", "0.1"),
        ):
            with self.subTest(extra=extra), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    self.args(*extra)

    def test_representative_artifact_validates_and_redacts_inputs(self) -> None:
        args = self.args("--profile", "smoke")
        spec = self.spec("credential-like private query text")
        with patch.object(bench, "git_metadata", return_value=("a" * 40, True, True)):
            payload = bench.build_output_json(
                args=args,
                replay_specs=[spec],
                corpus_identity=self.corpus(),
                targets=[self.successful_target()],
                dry_run=False,
            )

        benchmark_protocol.validate_artifact(payload)
        encoded = json.dumps(payload)
        self.assertNotIn(spec.raw_query, encoded)
        self.assertNotIn("fixture.toml", encoded)
        self.assertEqual(payload["samples"]["measurements"]["latency_ms"], [2.5])
        self.assertEqual(
            payload["scenario"]["fingerprints"]["dataset"],
            {"fingerprint": "sha256:" + "1" * 64, "cardinality": 3},
        )
        self.assertEqual(payload["semantic"]["status"], "pass")
        self.assertEqual(payload["timing"], {"status": "not_evaluated", "non_blocking": True})
        self.assertNotIn("comparison_policy", payload["timing"])

    def test_searchable_corpus_change_is_recorded_and_incomparable(self) -> None:
        cfg = bench.ClickHouseSettings(
            url="http://clickhouse.invalid",
            database="moraine",
            username="",
            password="",
            timeout_seconds=1.0,
        )
        rows = [
            {
                "event_uid": "event-1",
                "doc_version": "1",
                "text_sha256": "a" * 64,
                "payload_sha256": "b" * 64,
            },
            {
                "event_uid": "event-2",
                "doc_version": "1",
                "text_sha256": "c" * 64,
                "payload_sha256": "d" * 64,
            },
        ]
        with patch.object(
            bench,
            "clickhouse_query_json_each_row",
            side_effect=[rows, [rows[0], {**rows[1], "doc_version": "2"}]],
        ) as query:
            original = bench.searchable_corpus_identity(cfg)
            changed = bench.searchable_corpus_identity(cfg)
        self.assertEqual(original.cardinality, 2)
        self.assertEqual(changed.cardinality, 2)
        self.assertNotEqual(original.fingerprint, changed.fingerprint)
        self.assertIn("search_documents FINAL", query.call_args_list[0].args[1])

        args = self.args("--profile", "smoke")
        spec = self.spec()
        target = self.successful_target()
        with patch.object(bench, "git_metadata", return_value=("f" * 40, False, True)):
            baseline = bench.build_output_json(
                args=args,
                replay_specs=[spec],
                corpus_identity=original,
                targets=[target],
                dry_run=False,
            )
            candidate = bench.build_output_json(
                args=args,
                replay_specs=[spec],
                corpus_identity=changed,
                targets=[target],
                dry_run=False,
            )
        self.assertEqual(baseline["scenario_id"], candidate["scenario_id"])
        with self.assertRaises(benchmark_protocol.IncomparableError):
            benchmark_protocol.compare_artifacts(baseline, candidate)

    def test_spawn_failure_is_unattempted_and_insufficient(self) -> None:
        args = self.args("--profile", "smoke")
        with patch.object(bench, "start_mcp_process", side_effect=OSError("spawn secret")):
            target = bench.run_target_persistent(
                args=args,
                config_path=Path("fixture.toml"),
                tool="search",
                specs=[self.spec()],
            )
        self.assertEqual(
            (target["planned"], target["attempted"], target["successful"], target["errors"]),
            (1, 0, 0, 0),
        )
        with patch.object(bench, "git_metadata", return_value=("b" * 40, False, True)):
            payload = bench.build_output_json(
                args=args,
                replay_specs=[self.spec()],
                corpus_identity=self.corpus(),
                targets=[target],
                dry_run=False,
            )
        benchmark_protocol.validate_artifact(payload)
        self.assertEqual(payload["semantic"]["status"], "fail")
        self.assertEqual(
            {item["code"] for item in payload["diagnostics"]},
            {"child-spawn-failed", "insufficient-samples"},
        )

    def test_half_started_child_timeout_is_cleaned_and_never_attempted(self) -> None:
        args = self.args("--profile", "smoke")
        proc = FakeProcess()
        with ExitStack() as stack:
            stack.enter_context(patch.object(bench, "start_mcp_process", return_value=proc))
            stack.enter_context(
                patch.object(
                    bench, "initialize_mcp", side_effect=TimeoutError("init timeout")
                )
            )
            stop = stack.enter_context(
                patch.object(
                    bench, "stop_mcp_process", return_value="child-cleanup-failed"
                )
            )
            target = bench.run_target_persistent(
                args=args,
                config_path=Path("fixture.toml"),
                tool="search",
                specs=[self.spec()],
            )
        stop.assert_called_once_with(proc)
        self.assertEqual(
            (target["planned"], target["attempted"], target["successful"], target["errors"]),
            (1, 0, 0, 0),
        )
        self.assertEqual(target["failures"]["timeout"], 1)
        self.assertEqual(target["failures"]["cleanup"], 1)

    def test_timeout_and_child_crash_are_counted_as_sample_errors(self) -> None:
        args = self.args("--profile", "smoke")
        timed_out = self.run_persistent(
            args,
            [(3, 1.0, 0.5, "a" * 64), TimeoutError("timed out")],
        )
        self.assertEqual(
            (timed_out["attempted"], timed_out["successful"], timed_out["errors"]),
            (1, 0, 1),
        )
        self.assertEqual(timed_out["failures"]["timeout"], 1)

        crashed_proc = FakeProcess()
        with ExitStack() as stack:
            stack.enter_context(
                patch.object(bench, "start_mcp_process", return_value=crashed_proc)
            )
            stack.enter_context(
                patch.object(bench, "initialize_mcp", return_value=(2, 1.0))
            )
            call_count = [0]

            def crash_on_measured_call(*_args, **_kwargs):
                call_count[0] += 1
                if call_count[0] == 1:
                    return (3, 1.0, 0.5, "a" * 64)
                crashed_proc.returncode = 9
                raise RuntimeError("child exited")

            stack.enter_context(
                patch.object(bench, "call_tool", side_effect=crash_on_measured_call)
            )
            stack.enter_context(
                patch.object(bench, "stop_mcp_process", return_value=None)
            )
            crashed = bench.run_target_persistent(
                args=args,
                config_path=Path("fixture.toml"),
                tool="search",
                specs=[self.spec()],
            )
        self.assertEqual(crashed["failures"]["crash"], 1)
        self.assertEqual(crashed["errors"], 1)

    def test_smoke_wrong_first_measured_response_fails_independent_oracle(self) -> None:
        args = self.args("--profile", "smoke")
        target = self.run_persistent(
            args,
            [(3, 1.0, 0.5, "a" * 64), (4, 2.0, 1.0, "b" * 64)],
        )
        self.assertEqual(
            (target["planned"], target["attempted"], target["successful"], target["errors"]),
            (1, 1, 0, 1),
        )
        self.assertEqual(target["raw_e2e_search_ms"], [])
        self.assertEqual(target["failures"]["oracle"], 1)
        with patch.object(bench, "git_metadata", return_value=("a" * 40, False, True)):
            payload = bench.build_output_json(
                args=args,
                replay_specs=[self.spec()],
                corpus_identity=self.corpus(),
                targets=[target],
                dry_run=False,
            )
        benchmark_protocol.validate_artifact(payload)
        self.assertEqual(payload["semantic"]["status"], "fail")
        self.assertEqual(
            {item["code"] for item in payload["diagnostics"]},
            {"insufficient-samples", "semantic-oracle-failed"},
        )

    def test_oracle_canonicalizes_dynamic_fields_and_rejects_empty_results(self) -> None:
        first = {
            "structuredContent": {
                "query_id": "generated-1",
                "stats": {"took_ms": 1, "result_count": 1},
                "hits": [{"rank": 1, "event_uid": "event-1"}],
            }
        }
        second = {
            "structuredContent": {
                "query_id": "generated-2",
                "stats": {"took_ms": 99, "result_count": 1},
                "hits": [{"rank": 1, "event_uid": "event-1"}],
            }
        }
        self.assertEqual(
            bench.oracle_fingerprint(first),
            bench.oracle_fingerprint(second),
        )
        with self.assertRaisesRegex(RuntimeError, "empty semantic result"):
            bench.oracle_fingerprint(
                {
                    "structuredContent": {
                        "query_id": "generated-3",
                        "stats": {"took_ms": 1, "result_count": 0},
                        "hits": [],
                    }
                }
            )

        args = self.args("--profile", "smoke")
        target = self.run_persistent(
            args,
            RuntimeError("tool response has an empty semantic result"),
        )
        self.assertEqual(
            (target["planned"], target["attempted"], target["successful"], target["errors"]),
            (1, 0, 0, 0),
        )
        with patch.object(bench, "git_metadata", return_value=("b" * 40, False, True)):
            payload = bench.build_output_json(
                args=args,
                replay_specs=[self.spec()],
                corpus_identity=self.corpus(),
                targets=[target],
                dry_run=False,
            )
        benchmark_protocol.validate_artifact(payload)
        self.assertEqual(payload["semantic"]["status"], "fail")

    def test_cleanup_failure_fails_semantics_without_corrupting_counts(self) -> None:
        args = self.args("--profile", "smoke")
        target = self.run_persistent(
            args,
            [
                (3, 1.0, 0.5, "a" * 64),
                (4, 2.0, 1.0, "a" * 64),
            ],
            cleanup="child-cleanup-failed",
        )
        self.assertEqual(
            (target["attempted"], target["successful"], target["errors"]), (1, 1, 0)
        )
        with patch.object(bench, "git_metadata", return_value=("c" * 40, False, True)):
            payload = bench.build_output_json(
                args=args,
                replay_specs=[self.spec()],
                corpus_identity=self.corpus(),
                targets=[target],
                dry_run=False,
            )
        benchmark_protocol.validate_artifact(payload)
        self.assertEqual(payload["semantic"]["status"], "fail")
        self.assertIn("child-cleanup-failed", {d["code"] for d in payload["diagnostics"]})

    def test_cold_process_success_records_one_complete_sample(self) -> None:
        args = self.args(
            "--mode", "cold_process", "--warmup", "0", "--repeats", "1",
            "--tool", "search",
        )
        proc = FakeProcess()
        with ExitStack() as stack:
            stack.enter_context(patch.object(bench, "start_mcp_process", return_value=proc))
            stack.enter_context(
                patch.object(bench, "initialize_mcp", return_value=(2, 1.0))
            )
            stack.enter_context(
                patch.object(
                    bench, "call_tool", return_value=(3, 2.0, 1.0, "a" * 64)
                )
            )
            stack.enter_context(
                patch.object(bench, "stop_mcp_process", return_value=None)
            )
            target = bench.run_target_cold_process(
                args=args,
                config_path=Path("fixture.toml"),
                tool="search",
                specs=[self.spec()],
            )
        self.assertEqual(
            (target["planned"], target["attempted"], target["successful"], target["errors"]),
            (1, 1, 1, 0),
        )
        self.assertEqual(target["raw_e2e_search_ms"], [2.0])

    def test_emission_references_content_addressed_redacted_diagnostics(self) -> None:
        spec = self.spec("sensitive raw query")
        target = self.successful_target()
        target["per_case"] = [{"rank": 1, "query": spec.raw_query, "success_count": 1}]
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary) / "result.json"
            args = self.args(
                "--profile", "smoke", "--output-json", str(destination)
            )
            with patch.object(bench, "git_metadata", return_value=("d" * 40, False, True)):
                payload = bench.build_output_json(
                    args=args,
                    replay_specs=[spec],
                    corpus_identity=self.corpus(),
                    targets=[target],
                    dry_run=False,
                )
            details = bench.build_diagnostics_json(args, [target])
            self.assertTrue(bench.emit_output(args, payload, details))

            envelope = json.loads(destination.read_text(encoding="utf-8"))
            benchmark_protocol.validate_artifact(envelope)
            reference = envelope["artifacts"][0]
            diagnostics_path = destination.with_name(reference["path"])
            diagnostics_bytes = diagnostics_path.read_bytes()
            self.assertEqual(
                bench.hashlib.sha256(diagnostics_bytes).hexdigest(), reference["sha256"]
            )
            self.assertNotIn(spec.raw_query, diagnostics_bytes.decode("utf-8"))
            self.assertEqual(envelope["diagnostics"], [])

    def test_atomic_output_failure_preserves_existing_artifact_and_cleans_temp(self) -> None:
        args = self.args("--profile", "smoke")
        with patch.object(bench, "git_metadata", return_value=("d" * 40, False, True)):
            payload = bench.build_output_json(
                args=args,
                replay_specs=[self.spec()],
                corpus_identity=self.corpus(),
                targets=[self.successful_target()],
                dry_run=False,
            )
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            destination = directory / "result.json"
            destination.write_text("old artifact\n", encoding="utf-8")
            with patch.object(bench.os, "replace", side_effect=OSError("disk full")):
                with self.assertRaises(OSError):
                    benchmark_protocol.write_artifact(destination, payload)
            self.assertEqual(destination.read_text(encoding="utf-8"), "old artifact\n")
            self.assertEqual([path.name for path in directory.iterdir()], ["result.json"])

    def test_envelope_failure_removes_new_sidecar_and_preserves_old_output(self) -> None:
        spec = self.spec()
        target = self.successful_target()
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            destination = directory / "result.json"
            destination.write_text("old artifact\n", encoding="utf-8")
            args = self.args(
                "--profile", "smoke", "--output-json", str(destination)
            )
            with patch.object(bench, "git_metadata", return_value=("e" * 40, False, True)):
                payload = bench.build_output_json(
                    args=args,
                    replay_specs=[spec],
                    corpus_identity=self.corpus(),
                    targets=[target],
                    dry_run=False,
                )
            details = bench.build_diagnostics_json(args, [target])
            real_replace = bench.os.replace
            calls = [0]

            def fail_envelope_replace(source, target_path):
                calls[0] += 1
                if calls[0] == 2:
                    raise OSError("disk full")
                return real_replace(source, target_path)

            with ExitStack() as stack:
                stack.enter_context(
                    patch.object(
                        bench.os, "replace", side_effect=fail_envelope_replace
                    )
                )
                stack.enter_context(redirect_stderr(io.StringIO()))
                self.assertFalse(bench.emit_output(args, payload, details))
            self.assertEqual(destination.read_text(encoding="utf-8"), "old artifact\n")
            self.assertEqual([path.name for path in directory.iterdir()], ["result.json"])


if __name__ == "__main__":
    unittest.main()
