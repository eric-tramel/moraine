#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

BENCH_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BENCH_DIR))

import benchmark_protocol
import mcp_two_tool_sla as bench


COMMIT = "a" * 40
COUNTS = {
    "events": 7,
    "sessions": 3,
    "turns": 5,
    "search_documents": 11,
    "search_postings": 19,
    "search_query_log": 2,
}
CORPUS_IDENTITY = {
    "cardinality": 11,
    "content_xor": "101",
    "content_sum": "202",
}
SEARCH_ORACLE = {
    "schema_version": "moraine-mcp-two-tool-oracle-v1",
    "queries": [
        {
            "query": "raw secret query",
            "expected": {
                "result_count": 1,
                "open_ids": {
                    "event": "evt-1",
                    "turn": "turn-1",
                    "session": "session-1",
                },
            },
        }
    ],
}
LIST_ORACLE = {
    **SEARCH_ORACLE,
    "list_sessions": {
        "result_count": 1,
        "session_ids": ["seed-session"],
        "result_marker": {"title": "seed marker"},
    },
}


def sample(*, met_sla: bool = True, structured: dict[str, object] | None = None) -> dict[str, object]:
    return {
        "tool": "search_sessions",
        "arguments": {"query": "must-not-leak", "n_hits": 10},
        "e2e_ms": 3.5,
        "server_elapsed_ms": 2.0,
        "sla_target_ms": 750.0,
        "met_sla": met_sla,
        "structured": structured
        if structured is not None
        else {
            "data": {
                "results": [
                    {
                        "open": {
                            "event_id": "evt-1",
                            "turn_id": "turn-1",
                            "session_id": "session-1",
                        }
                    }
                ]
            }
        },
    }

def open_sample(
    *,
    kind: str = "event",
    returned_id: str = "evt-1",
    met_sla: bool = True,
) -> dict[str, object]:
    return {
        "tool": "open",
        "arguments": {"id": returned_id},
        "e2e_ms": 2.5,
        "server_elapsed_ms": 1.5,
        "sla_target_ms": 200.0,
        "met_sla": met_sla,
        "structured": {
            "request": {"id": returned_id},
            "data": {
                "kind": kind,
                kind: {"id": returned_id},
            },
        },
    }


def list_sample(*, session_id: str = "seed-session", title: str = "seed marker") -> dict[str, object]:
    return {
        "tool": "list_sessions",
        "arguments": {"limit": 20},
        "e2e_ms": 2.75,
        "server_elapsed_ms": 1.75,
        "sla_target_ms": 500.0,
        "met_sla": True,
        "structured": {
            "data": {
                "sessions": [
                    {
                        "title": title,
                        "open": {"session_id": session_id},
                    }
                ]
            }
        },
    }


class ArgumentTests(unittest.TestCase):
    def test_profiles_preserve_full_defaults_and_reduce_smoke(self) -> None:
        full = bench.parse_args(["--config", "config.toml"])
        self.assertEqual((full.profile, full.warmup, full.repeats, full.min_docs), ("full", 1, 5, 100_000))

        smoke = bench.parse_args(["--config", "config.toml", "--profile", "smoke"])
        self.assertEqual((smoke.warmup, smoke.repeats, smoke.min_docs), (0, 1, 1))

    def test_explicit_workload_arguments_override_profile(self) -> None:
        args = bench.parse_args(
            [
                "--config",
                "config.toml",
                "--profile",
                "smoke",
                "--warmup",
                "2",
                "--repeats",
                "4",
                "--min-docs",
                "37",
                "--output-json",
                "result.json",
            ]
        )
        self.assertEqual((args.warmup, args.repeats, args.min_docs), (2, 4, 37))
        self.assertEqual(args.output_json, "result.json")

    def test_sla_timing_gate_is_not_a_cli_option(self) -> None:
        args = bench.parse_args(["--config", "config.toml"])
        self.assertFalse(hasattr(args, "fail_on_sla_miss"))
        with self.assertRaises(SystemExit):
            bench.parse_args(["--config", "config.toml", "--fail-on-sla-miss"])

    def test_invalid_profile_and_nonpositive_repeats_are_rejected(self) -> None:
        for argv in (
            ["--config", "config.toml", "--profile", "fast"],
            ["--config", "config.toml", "--repeats", "0"],
        ):
            with self.subTest(argv=argv), self.assertRaises(SystemExit):
                bench.parse_args(argv)


class ArtifactTests(unittest.TestCase):
    def args(self, *extra: str) -> object:
        return bench.parse_args(
            [
                "--config",
                "config.toml",
                "--profile",
                "smoke",
                "--git-commit",
                COMMIT,
                "--git-clean",
                *extra,
            ]
        )

    def build(
        self,
        *,
        samples: list[dict[str, object]],
        planned: int,
        attempted: int,
        errors: int,
        oracle_mismatches: int = 0,
        extra_args: tuple[str, ...] = (),
        corpus_identity: dict[str, object] = CORPUS_IDENTITY,
    ) -> dict[str, object]:
        return bench.build_artifact(
            args=self.args(*extra_args),
            counts=COUNTS,
            corpus_identity=corpus_identity,
            queries=["private raw query"],
            open_kinds=[],
            list_sessions_args=None,
            samples=samples,
            planned=planned,
            attempted=attempted,
            errors=errors,
            initialization_ms=1.25,
            diagnostic_codes=[],
            oracle_mismatches=oracle_mismatches,
            warmup_failures=0,
        )

    def test_representative_artifact_validates_and_redacts_workload(self) -> None:
        payload = self.build(samples=[sample()], planned=1, attempted=1, errors=0)
        benchmark_protocol.validate_artifact(payload)

        encoded = json.dumps(payload)
        self.assertNotIn("private raw query", encoded)
        self.assertNotIn("must-not-leak", encoded)
        self.assertEqual(payload["source"], {"git_commit": COMMIT, "dirty": False})
        self.assertEqual(payload["scenario"]["fingerprints"]["request_source"], "production-mcp")
        self.assertEqual(payload["scenario"]["fingerprints"]["cache_state"], "mixed")
        self.assertEqual(payload["scenario"]["measured_boundary"], "json-rpc-stdio-tool-call")
        self.assertEqual(payload["samples"]["measurements"]["latency_ms"], [3.5])
        self.assertEqual(payload["samples"]["measurements"]["server_elapsed_ms"], [2.0])
        self.assertEqual(payload["semantic"]["status"], "pass")
        self.assertEqual(payload["timing"], {"status": "not_evaluated", "non_blocking": True})
        self.assertNotIn("model_tokens", payload["metrics"])

    def test_count_invariant_is_enforced_before_serialization(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "successful plus errors"):
            self.build(samples=[sample()], planned=2, attempted=2, errors=0)

    def test_insufficient_successful_samples_fail_semantics(self) -> None:
        payload = self.build(samples=[sample()], planned=2, attempted=1, errors=0)
        benchmark_protocol.validate_artifact(payload)
        self.assertEqual(payload["semantic"]["status"], "fail")
        self.assertIn({"code": "insufficient-samples"}, payload["diagnostics"])

    def test_oracle_mismatch_fails_independently_of_timing(self) -> None:
        payload = self.build(
            samples=[sample()],
            planned=1,
            attempted=1,
            errors=0,
            oracle_mismatches=1,
        )
        benchmark_protocol.validate_artifact(payload)
        self.assertEqual(payload["semantic"]["status"], "fail")
        self.assertEqual(payload["metrics"]["quality"]["mismatches"], 1)
        self.assertEqual(payload["timing"]["status"], "not_evaluated")

    def test_sla_miss_is_non_blocking_typed_quality_data(self) -> None:
        payload = self.build(samples=[sample(met_sla=False)], planned=1, attempted=1, errors=0)
        benchmark_protocol.validate_artifact(payload)

        self.assertEqual(payload["semantic"]["status"], "pass")
        self.assertEqual(payload["timing"], {"status": "not_evaluated", "non_blocking": True})
        self.assertEqual(payload["samples"]["measurements"]["sla_target_ms"], [750.0])
        self.assertEqual(
            payload["metrics"]["quality"],
            {
                "oracle_status": "pass",
                "passed_checks": 0,
                "failed_checks": 1,
                "expected_count": 1,
                "observed_count": 1,
                "mismatches": 0,
                "success_rate": 1.0,
                "error_rate": 0.0,
            },
        )
        self.assertEqual(payload["diagnostics"], [])

    def test_same_count_corpus_content_change_changes_dataset_fingerprint(self) -> None:
        original = self.build(samples=[sample()], planned=1, attempted=1, errors=0)
        changed = self.build(
            samples=[sample()],
            planned=1,
            attempted=1,
            errors=0,
            corpus_identity={
                "cardinality": CORPUS_IDENTITY["cardinality"],
                "content_xor": "different",
                "content_sum": CORPUS_IDENTITY["content_sum"],
            },
        )

        self.assertNotEqual(
            original["scenario"]["fingerprints"]["dataset"]["fingerprint"],
            changed["scenario"]["fingerprints"]["dataset"]["fingerprint"],
        )

    def test_output_uses_shared_atomic_writer(self) -> None:
        payload = self.build(samples=[sample()], planned=1, attempted=1, errors=0)
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "nested" / "result.json"
            benchmark_protocol.write_artifact(output, payload)
            self.assertEqual(json.loads(output.read_text(encoding="utf-8")), payload)


class MainTests(unittest.TestCase):
    def run_main(
        self,
        output: Path,
        measured_side_effect: object,
        *,
        open_kind: str = "none",
        min_docs: int = 1,
        repeats: int = 1,
        warmup: int = 0,
        corpus_identity: dict[str, object] = CORPUS_IDENTITY,
        oracle: dict[str, object] | None = SEARCH_ORACLE,
        skip_list: bool = True,
    ) -> int:
        fake_proc = SimpleNamespace()
        self.start_mcp = mock.Mock(return_value=fake_proc)

        def rpc(_proc: object, request_id: int, method: str, _params: object, _timeout: int) -> dict[str, object]:
            if method == "initialize":
                return {}
            if method == "tools/list":
                names = ["search_sessions", "open"]
                if not skip_list:
                    names.append("list_sessions")
                return {"tools": [{"name": name} for name in names]}
            raise AssertionError(method)

        argv = [
            "--config",
            "config.toml",
            "--profile",
            "smoke",
            "--query",
            "raw secret query",
            "--open-kind",
            open_kind,
            "--min-docs",
            str(min_docs),
            "--repeats",
            str(repeats),
            "--warmup",
            str(warmup),
            "--git-commit",
            COMMIT,
            "--git-clean",
            "--output-json",
            str(output),
        ]
        if skip_list:
            argv.append("--skip-list-sessions")
        if oracle is not None:
            oracle_path = output.with_name("query-oracle.json")
            oracle_path.write_text(json.dumps(oracle), encoding="utf-8")
            argv.extend(["--oracle-json", str(oracle_path)])
        if isinstance(measured_side_effect, BaseException):
            measured_call = mock.Mock(side_effect=measured_side_effect)
        elif isinstance(measured_side_effect, list):
            measured_call = mock.Mock(side_effect=measured_side_effect)
        else:
            measured_call = mock.Mock(return_value=measured_side_effect)
        self.measured_call = measured_call
        with (
            mock.patch.object(bench, "read_clickhouse_config", return_value={}),
            mock.patch.object(bench, "corpus_counts", return_value=COUNTS),
            mock.patch.object(bench, "search_corpus_identity", return_value=corpus_identity),
            mock.patch.object(
                bench,
                "corpus_session_window",
                return_value={
                    "start_datetime": "2026-01-01T00:00:00.000Z",
                    "end_datetime": "2026-01-02T00:00:00.000Z",
                },
            ),
            mock.patch.object(bench, "select_log_queries", return_value=[]),
            mock.patch.object(bench, "resolve_service_bin_dir", return_value=None),
            mock.patch.object(bench, "start_mcp", self.start_mcp),
            mock.patch.object(bench, "stop_mcp"),
            mock.patch.object(bench, "send_rpc", side_effect=rpc),
            mock.patch.object(bench, "measured_tool_call", measured_call),
        ):
            return bench.main(argv)

    def test_successful_cli_writes_schema_valid_redacted_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "result.json"
            result = self.run_main(output, (2, sample()))
            self.assertEqual(result, 0)
            payload = json.loads(output.read_text(encoding="utf-8"))
            benchmark_protocol.validate_artifact(payload)
            self.assertNotIn("raw secret query", json.dumps(payload))
            self.assertEqual(payload["samples"]["planned"], 1)
            self.assertEqual(payload["samples"]["successful"], 1)

    def test_sla_miss_exits_success_when_semantics_pass(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "result.json"
            result = self.run_main(output, (2, sample(met_sla=False)))
            self.assertEqual(result, 0)
            payload = json.loads(output.read_text(encoding="utf-8"))
            benchmark_protocol.validate_artifact(payload)
            self.assertEqual(payload["semantic"]["status"], "pass")
            self.assertEqual(payload["timing"], {"status": "not_evaluated", "non_blocking": True})
            self.assertEqual(payload["metrics"]["quality"]["failed_checks"], 1)

    def test_tool_error_fails_closed_and_records_error_sample(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "result.json"
            result = self.run_main(output, TimeoutError("synthetic timeout"))
            self.assertEqual(result, 1)
            payload = json.loads(output.read_text(encoding="utf-8"))
            benchmark_protocol.validate_artifact(payload)
            self.assertEqual(payload["samples"], {
                "planned": 1,
                "attempted": 1,
                "successful": 0,
                "errors": 1,
                "measurements": {"latency_ms": [], "server_elapsed_ms": [], "sla_target_ms": []},
            })
            self.assertEqual(payload["semantic"]["status"], "fail")
            self.assertIn({"code": "tool-call-error"}, payload["diagnostics"])

    def test_missing_search_hit_fails_semantics_but_opens_seed_entity(self) -> None:
        search_without_hits = sample(structured={"data": {"results": []}})
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "result.json"
            result = self.run_main(
                output,
                [(2, search_without_hits), (3, open_sample())],
                open_kind="event",
            )
            self.assertEqual(result, 1)
            payload = json.loads(output.read_text(encoding="utf-8"))
            benchmark_protocol.validate_artifact(payload)
            self.assertEqual(payload["samples"]["successful"], 2)
            self.assertEqual(payload["samples"]["errors"], 0)
            self.assertEqual(payload["metrics"]["quality"]["mismatches"], 1)
            self.assertIn({"code": "search-oracle-mismatch"}, payload["diagnostics"])
            open_calls = [
                call
                for call in self.measured_call.call_args_list
                if call.args[3] == "open"
            ]
            self.assertEqual(open_calls[0].args[4], {"id": "evt-1"})

    def test_repeatable_wrong_search_ids_fail_independent_oracle(self) -> None:
        wrong = sample(
            structured={
                "data": {
                    "results": [
                        {
                            "open": {
                                "event_id": "evt-wrong",
                                "turn_id": "turn-1",
                                "session_id": "session-1",
                            }
                        }
                    ]
                }
            }
        )
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "result.json"
            result = self.run_main(
                output,
                [(2, wrong), (3, open_sample()), (4, wrong), (5, open_sample())],
                open_kind="event",
                repeats=2,
            )
            self.assertEqual(result, 1)
            payload = json.loads(output.read_text(encoding="utf-8"))
            benchmark_protocol.validate_artifact(payload)
            self.assertEqual(payload["samples"]["successful"], 4)
            self.assertEqual(payload["samples"]["errors"], 0)
            self.assertEqual(payload["metrics"]["quality"]["mismatches"], 2)
            self.assertIn({"code": "search-oracle-mismatch"}, payload["diagnostics"])
            self.assertNotIn({"code": "oracle-instability"}, payload["diagnostics"])
            open_calls = [
                call
                for call in self.measured_call.call_args_list
                if call.args[3] == "open"
            ]
            self.assertEqual([call.args[4] for call in open_calls], [{"id": "evt-1"}] * 2)

    def test_repeatable_wrong_list_results_fail_independent_oracle(self) -> None:
        wrong_list = list_sample(session_id="wrong-session", title="wrong marker")
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "result.json"
            result = self.run_main(
                output,
                [(2, sample()), (3, sample()), (4, wrong_list), (5, wrong_list)],
                oracle=LIST_ORACLE,
                skip_list=False,
                repeats=2,
            )

            self.assertEqual(result, 1)
            payload = benchmark_protocol.load_artifact(output)
            self.assertEqual(payload["samples"]["planned"], 4)
            self.assertEqual(payload["samples"]["successful"], 4)
            self.assertEqual(payload["samples"]["errors"], 0)
            self.assertEqual(payload["metrics"]["quality"]["mismatches"], 2)
            self.assertIn({"code": "list-oracle-mismatch"}, payload["diagnostics"])
            self.assertNotIn({"code": "oracle-instability"}, payload["diagnostics"])

    def test_missing_list_oracle_fails_closed_with_valid_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "result.json"
            result = self.run_main(
                output,
                (2, sample()),
                oracle=SEARCH_ORACLE,
                skip_list=False,
            )

            self.assertEqual(result, 1)
            self.start_mcp.assert_not_called()
            self.measured_call.assert_not_called()
            payload = benchmark_protocol.load_artifact(output)
            self.assertEqual(payload["samples"]["planned"], 2)
            self.assertEqual(payload["samples"]["attempted"], 0)
            self.assertEqual(payload["samples"]["successful"], 0)
            self.assertEqual(payload["samples"]["errors"], 0)
            self.assertEqual(payload["semantic"]["status"], "fail")
            self.assertEqual(payload["metrics"]["quality"]["mismatches"], 1)
            self.assertIn({"code": "missing-list-oracle"}, payload["diagnostics"])

    def test_wrong_open_entity_or_kind_is_a_failed_attempt(self) -> None:
        wrong_responses = [
            open_sample(returned_id="evt-wrong"),
            open_sample(kind="turn", returned_id="evt-1"),
        ]
        for wrong_response in wrong_responses:
            with self.subTest(wrong_response=wrong_response), tempfile.TemporaryDirectory() as temporary:
                output = Path(temporary) / "result.json"
                result = self.run_main(
                    output,
                    [(2, sample()), (3, wrong_response)],
                    open_kind="event",
                )

                self.assertEqual(result, 1)
                payload = benchmark_protocol.load_artifact(output)
                self.assertEqual(
                    payload["samples"],
                    {
                        "planned": 2,
                        "attempted": 2,
                        "successful": 1,
                        "errors": 1,
                        "measurements": {
                            "latency_ms": [3.5],
                            "server_elapsed_ms": [2.0],
                            "sla_target_ms": [750.0],
                        },
                    },
                )
                self.assertEqual(payload["semantic"]["status"], "fail")
                self.assertEqual(payload["metrics"]["quality"]["mismatches"], 1)
                self.assertIn({"code": "open-oracle-mismatch"}, payload["diagnostics"])

    def test_malformed_structured_data_is_a_failed_attempt_with_valid_counts(self) -> None:
        malformed_values: list[object] = [
            None,
            [],
            {"data": None},
            {"data": []},
        ]
        for malformed in malformed_values:
            with self.subTest(malformed=malformed), tempfile.TemporaryDirectory() as temporary:
                output = Path(temporary) / "result.json"
                malformed_sample = sample()
                malformed_sample["structured"] = malformed

                result = self.run_main(output, (2, malformed_sample))

                self.assertEqual(result, 1)
                payload = benchmark_protocol.load_artifact(output)
                self.assertEqual(payload["samples"]["attempted"], 1)
                self.assertEqual(payload["samples"]["successful"], 0)
                self.assertEqual(payload["samples"]["errors"], 1)
                self.assertEqual(
                    payload["samples"]["attempted"],
                    payload["samples"]["successful"] + payload["samples"]["errors"],
                )
                self.assertIn({"code": "malformed-tool-data"}, payload["diagnostics"])

    def test_failed_warmup_never_labels_measured_success_as_warm(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "result.json"
            result = self.run_main(
                output,
                [TimeoutError("synthetic warmup failure"), (2, sample())],
                warmup=1,
            )

            self.assertEqual(result, 1)
            payload = benchmark_protocol.load_artifact(output)
            self.assertEqual(payload["samples"]["planned"], 1)
            self.assertEqual(payload["samples"]["attempted"], 1)
            self.assertEqual(payload["samples"]["successful"], 1)
            self.assertEqual(payload["samples"]["errors"], 0)
            self.assertEqual(payload["semantic"]["status"], "fail")
            self.assertEqual(payload["scenario"]["fingerprints"]["cache_state"], "mixed")
            self.assertIn({"code": "warmup-error"}, payload["diagnostics"])

    def test_missing_oracle_fails_closed_with_valid_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "result.json"
            result = self.run_main(output, (2, sample()), oracle=None)

            self.assertEqual(result, 1)
            self.start_mcp.assert_not_called()
            self.measured_call.assert_not_called()
            payload = benchmark_protocol.load_artifact(output)
            self.assertEqual(
                payload["samples"],
                {
                    "planned": 1,
                    "attempted": 0,
                    "successful": 0,
                    "errors": 0,
                    "measurements": {
                        "latency_ms": [],
                        "server_elapsed_ms": [],
                        "sla_target_ms": [],
                    },
                },
            )
            self.assertEqual(payload["semantic"]["status"], "fail")
            self.assertEqual(payload["metrics"]["quality"]["mismatches"], 1)
            self.assertIn({"code": "missing-search-oracle"}, payload["diagnostics"])

    def test_missing_config_and_corpus_threshold_fail_before_live_process(self) -> None:
        missing = bench.main(["--config", "/definitely/missing/config.toml", "--dry-run"])
        self.assertEqual(missing, 2)

        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "result.json"
            result = self.run_main(output, (2, sample()), min_docs=12)
            self.assertEqual(result, 2)
            self.start_mcp.assert_not_called()

    def test_artifact_write_error_returns_nonzero(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "result.json"
            with mock.patch.object(
                benchmark_protocol, "write_artifact", side_effect=OSError("disk full")
            ):
                result = self.run_main(output, (2, sample()))
            self.assertEqual(result, 2)

    def test_atomic_write_failure_preserves_prior_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "result.json"
            output.write_text("prior artifact\n", encoding="utf-8")
            with mock.patch.object(
                benchmark_protocol.os,
                "replace",
                side_effect=OSError("synthetic replace failure"),
            ):
                result = self.run_main(output, (2, sample()))

            self.assertEqual(result, 2)
            self.assertEqual(output.read_text(encoding="utf-8"), "prior artifact\n")


if __name__ == "__main__":
    unittest.main()
