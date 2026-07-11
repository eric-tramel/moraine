from __future__ import annotations

import copy
import contextlib
import io
import json
import math
import tempfile
import sys
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest import mock
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).resolve().parent / "fixtures"
sys.path.insert(0, str(BENCH_DIR))

import benchmark_protocol as protocol


class BenchmarkProtocolTests(unittest.TestCase):
    def fixture(self, name: str = "valid-full.json") -> dict:
        return json.loads((FIXTURES / name).read_text(encoding="utf-8"))

    def assert_invalid(self, artifact: dict, message: str) -> None:
        with self.assertRaisesRegex(protocol.ProtocolError, message):
            protocol.validate_artifact(artifact)

    def test_normative_schema_is_parseable_and_names_v1(self) -> None:
        schema = json.loads(
            (BENCH_DIR / "schema" / "moraine-benchmark-v1.schema.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(
            schema["properties"]["schema_version"]["const"],
            protocol.SCHEMA_VERSION,
        )

    def test_every_checked_in_fixture_has_an_explicit_validator_outcome(self) -> None:
        expected = {
            "invalid-nonfinite.json": False,
            "invalid-redaction.json": False,
            "valid-full.json": True,
            "valid-zero-failed.json": True,
        }
        self.assertEqual(
            {path.name for path in FIXTURES.glob("*.json")},
            set(expected),
        )
        for name, is_valid in expected.items():
            with self.subTest(name=name):
                if is_valid:
                    protocol.load_artifact(FIXTURES / name)
                else:
                    with self.assertRaises(protocol.ProtocolError):
                        protocol.load_artifact(FIXTURES / name)

    def test_public_validator_matches_normative_schema_constraints(self) -> None:
        delete = object()
        cases = {
            "root_type": ((), []),
            "required": (("runner", "os"), delete),
            "additional_properties": (("runner", "unexpected"), True),
            "const": (("timing", "non_blocking"), False),
            "enum_unhashable": (("scenario", "profile"), []),
            "referenced_max_length": (("benchmark_id",), "a" * 129),
            "conditional_else_not": (
                ("scenario", "dimensions", "dataset_backed"),
                False,
            ),
            "one_of": (
                ("samples", "external_samples"),
                {
                    "artifact_id": "summary",
                    "measurement": "latency_ms",
                    "count": 3,
                    "sha256": "b" * 64,
                },
            ),
            "min_properties": (("metrics", "quality"), {}),
            "property_names": (
                ("samples", "measurements"),
                {"latency": [10.0, 11.0, 9.0]},
            ),
            "array_item_type": (
                ("samples", "measurements", "latency_ms"),
                ["slow", "slower", "slowest"],
            ),
            "minimum": (("samples", "planned"), 0),
            "maximum": (
                ("timing", "comparison_policy", "error_rate", "maximum"),
                2,
            ),
            "pattern": (("source", "git_commit"), "A" * 40),
            "min_length": (("artifacts", 0, "path"), ""),
            "max_length": (("artifacts", 0, "path"), "a" * 513),
            "uri_scheme": (("artifacts", 0, "path"), "data:text/plain,secret"),
            "array_type": (("diagnostics",), {}),
            "boolean_type": (("source", "dirty"), 0),
            "integer_type": (("samples", "planned"), 3.5),
            "number_type": (
                (
                    "timing",
                    "comparison_policy",
                    "threshold",
                    "maximum_relative_regression",
                ),
                True,
            ),
        }
        for name, (path, replacement) in cases.items():
            artifact: object = self.fixture()
            if not path:
                artifact = replacement
            else:
                target = artifact
                for component in path[:-1]:
                    target = target[component]
                if replacement is delete:
                    del target[path[-1]]
                else:
                    target[path[-1]] = copy.deepcopy(replacement)
            with self.subTest(name=name):
                with self.assertRaises(protocol.ProtocolError):
                    protocol._validate_schema_structure(artifact)
                with self.assertRaises(protocol.ProtocolError):
                    protocol.validate_artifact(artifact)

        artifact = self.fixture()
        artifact["samples"].update(
            planned=3.0,
            attempted=3.0,
            successful=3.0,
            errors=0.0,
        )
        protocol.validate_artifact(artifact)

        artifact = self.fixture()
        artifact[1] = "non-json object key"
        with self.assertRaisesRegex(protocol.ProtocolError, "field names must be strings"):
            protocol.validate_artifact(artifact)

    def test_artifact_paths_enforce_uri_and_length_boundaries(self) -> None:
        artifact = self.fixture()
        artifact["artifacts"][0]["path"] = "a" * 507 + ".json"
        protocol.validate_artifact(artifact)

        for invalid_path in (
            "a" * 508 + ".json",
            "https://example.invalid/result.json",
            "file:result.json",
            "urn:moraine:benchmark",
        ):
            artifact = self.fixture()
            artifact["artifacts"][0]["path"] = invalid_path
            with self.subTest(path=invalid_path[:32]):
                self.assert_invalid(artifact, "normative schema pattern|at most 512")

        artifact = self.fixture()
        artifact["artifacts"][0]["path"] = "reports/name:variant.json"
        protocol.validate_artifact(artifact)

    def test_representative_full_artifact_is_valid(self) -> None:
        protocol.validate_artifact(self.fixture())

    def test_zero_success_failed_artifact_is_valid(self) -> None:
        protocol.validate_artifact(self.fixture("valid-zero-failed.json"))

    def test_missing_required_field_is_rejected(self) -> None:
        artifact = self.fixture()
        del artifact["runner"]
        self.assert_invalid(artifact, "missing required.*runner")

    def test_unknown_version_is_rejected(self) -> None:
        artifact = self.fixture()
        artifact["schema_version"] = "moraine-benchmark-v2"
        self.assert_invalid(artifact, "unsupported version")

    def test_nonfinite_numbers_are_rejected_in_memory_and_on_load(self) -> None:
        artifact = self.fixture()
        artifact["samples"]["measurements"]["latency_ms"][0] = math.inf
        self.assert_invalid(artifact, "must be finite")
        with self.assertRaisesRegex(protocol.ProtocolError, "non-finite JSON number"):
            protocol.load_artifact(FIXTURES / "invalid-nonfinite.json")

    def test_count_invariants_are_enforced(self) -> None:
        mutations = []
        attempted_mismatch = self.fixture()
        attempted_mismatch["samples"]["attempted"] = 2
        mutations.append((attempted_mismatch, "attempted must equal"))
        attempted_over_planned = self.fixture()
        attempted_over_planned["samples"].update(
            planned=2, attempted=3, successful=3, errors=0
        )
        mutations.append((attempted_over_planned, "must not exceed planned"))
        raw_count_mismatch = self.fixture()
        raw_count_mismatch["samples"]["measurements"]["latency_ms"].pop()
        mutations.append((raw_count_mismatch, "raw measurement count"))
        for artifact, message in mutations:
            with self.subTest(message=message):
                self.assert_invalid(artifact, message)

    def test_conditional_fingerprints_are_required_and_inapplicable_ones_forbidden(self) -> None:
        missing = self.fixture()
        del missing["scenario"]["fingerprints"]["dataset"]
        self.assert_invalid(missing, "missing required.*dataset")

        meaningless = self.fixture()
        meaningless["scenario"]["dimensions"]["cache_sensitive"] = False
        self.assert_invalid(meaningless, "prohibited structure")

    def test_redaction_policy_rejects_each_sensitive_class(self) -> None:
        with self.assertRaisesRegex(protocol.ProtocolError, "forbidden sensitive"):
            protocol.load_artifact(FIXTURES / "invalid-redaction.json")

        cases = {
            "credential_key": ("access_token", "abc123"),
            "raw_query": ("query", "select private data"),
            "host_identity": ("hostname", "runner-17"),
        }
        for name, (key, value) in cases.items():
            artifact = self.fixture()
            artifact[key] = value
            with self.subTest(name=name):
                self.assert_invalid(artifact, "forbidden sensitive")

        credentialized = self.fixture()
        credentialized["artifacts"][0]["path"] = "https://alice:secret@example.invalid/a"
        self.assert_invalid(credentialized, "credentialized URLs")

        home_path = self.fixture()
        home_path["artifacts"][0]["path"] = "/Users/alice/private/result.json"
        self.assert_invalid(home_path, "absolute user home paths")

        sensitive_paths = (
            "https%3A%2F%2Falice%3Asecret%40example.invalid%2Fa",
            "target/bench/password=hunter2-private-query-alice@runner17.json",
            "target/bench/private-query-alice.json",
            "target/bench/alice@runner17.json",
        )
        for sensitive_path in sensitive_paths:
            artifact = self.fixture()
            artifact["artifacts"][0]["path"] = sensitive_path
            with self.subTest(sensitive_path=sensitive_path):
                self.assert_invalid(
                    artifact,
                    "credentialized URLs|embedded sensitive material",
                )


    def test_redaction_rejects_short_and_percent_encoded_credentials(self) -> None:
        credentials = (
            "Basic Og==",
            "Bearer a",
            "Basic%20Og%3D%3D",
            "Bearer%20a",
            "Basic%252520Og%25253D%25253D",
            "Authorization%3A%20Basic%20Og%3D%3D",
        )
        for credential in credentials:
            artifact = self.fixture()
            artifact["artifacts"][0]["path"] = credential
            with self.subTest(credential=credential):
                self.assert_invalid(artifact, "authorization credentials")

        artifact = self.fixture()
        artifact["artifacts"][0]["path"] = "Basic YQ=="
        protocol.validate_artifact(artifact)

    def test_typed_metrics_reject_unknown_or_nonfinite_values(self) -> None:
        artifact = self.fixture()
        artifact["metrics"]["quality"]["raw_score"] = 1
        self.assert_invalid(artifact, "unknown field.*raw_score")
        artifact = self.fixture()
        artifact["metrics"]["resources"]["cpu_time_ms"] = float("nan")
        self.assert_invalid(artifact, "must be finite")

    def test_error_rate_and_timing_pass_contradiction_is_rejected(self) -> None:
        artifact = self.fixture()
        artifact["samples"].update(planned=4, attempted=4, successful=3, errors=1)
        artifact["timing"]["comparison_policy"]["error_rate"]["maximum"] = 0.0
        self.assert_invalid(artifact, "observed error rate exceeds")

    def test_evaluated_timing_requires_complete_policy(self) -> None:
        artifact = self.fixture()
        del artifact["timing"]["comparison_policy"]["variability"]
        self.assert_invalid(artifact, "missing required.*variability")

        artifact = self.fixture()
        del artifact["timing"]["comparison_policy"]
        self.assert_invalid(artifact, "missing required.*comparison_policy")

    def test_smoke_timing_is_never_evaluated(self) -> None:
        artifact = self.fixture("valid-zero-failed.json")
        artifact["semantic"]["status"] = "pass"
        artifact["samples"].update(planned=1, attempted=1, successful=1, errors=0)
        artifact["samples"]["measurements"]["latency_ms"] = [1.0]
        artifact["timing"] = copy.deepcopy(self.fixture()["timing"])
        self.assert_invalid(artifact, "must equal.*not_evaluated")

    def test_external_samples_require_count_checksum_and_declared_artifact(self) -> None:
        artifact = self.fixture()
        artifact["samples"].pop("measurements")
        artifact["samples"]["external_samples"] = {
            "artifact_id": "samples",
            "measurement": "latency_ms",
            "count": 3,
            "sha256": "d" * 64,
        }
        artifact["artifacts"].append(
            {
                "id": "samples",
                "kind": "raw-samples-json",
                "path": "target/bench/samples.json",
                "sha256": "d" * 64,
            }
        )
        protocol.validate_artifact(artifact)
        artifact["samples"]["external_samples"]["count"] = 2
        self.assert_invalid(artifact, "must equal samples.successful")
        artifact["samples"]["external_samples"]["count"] = 3
        artifact["samples"]["external_samples"]["sha256"] = "e" * 64
        self.assert_invalid(artifact, "referenced artifact sha256")

    def test_validator_cli_success_and_failure_exit_codes(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            status = protocol.main(
                ["validate", str(FIXTURES / "valid-full.json")]
            )
        self.assertEqual(status, 0)
        self.assertIn("valid:", stdout.getvalue())

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            status = protocol.main(
                ["validate", str(FIXTURES / "invalid-redaction.json")]
            )
        self.assertEqual(status, 1)
        self.assertIn("forbidden sensitive", stderr.getvalue())



class BenchmarkArtifactWriterTests(unittest.TestCase):
    def fixture(self) -> dict:
        return json.loads((FIXTURES / "valid-full.json").read_text(encoding="utf-8"))

    def test_public_writer_validates_and_creates_parent_atomically(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary) / "nested" / "results" / "artifact.json"
            protocol.write_artifact(destination, self.fixture())
            self.assertEqual(protocol.load_artifact(destination), self.fixture())

            invalid = self.fixture()
            invalid["schema_version"] = "moraine-benchmark-v2"
            with self.assertRaises(protocol.ProtocolError):
                protocol.write_artifact(destination, invalid)
            self.assertEqual(protocol.load_artifact(destination), self.fixture())

    def test_writer_ignores_predictable_symlink_temp(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            destination = root / "result.json"
            victim = root / "victim.json"
            victim.write_text("do not overwrite\n", encoding="utf-8")
            destination.symlink_to(victim)
            predictable = root / ".result.json.tmp"
            predictable.symlink_to(victim)

            protocol.write_artifact(destination, self.fixture())

            self.assertEqual(victim.read_text(encoding="utf-8"), "do not overwrite\n")
            self.assertTrue(predictable.is_symlink())
            self.assertFalse(destination.is_symlink())
            self.assertEqual(protocol.load_artifact(destination), self.fixture())

    def test_writer_preserves_destination_and_cleans_temp_on_write_failures(self) -> None:
        failures = ("fsync", "replace")
        for operation in failures:
            with self.subTest(operation=operation):
                with tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    destination = root / "result.json"
                    destination.write_text("existing destination\n", encoding="utf-8")
                    with mock.patch.object(
                        protocol.os,
                        operation,
                        side_effect=OSError("injected write failure"),
                    ):
                        with self.assertRaisesRegex(OSError, "injected write failure"):
                            protocol.write_artifact(destination, self.fixture())
                    self.assertEqual(
                        destination.read_text(encoding="utf-8"),
                        "existing destination\n",
                    )
                    self.assertEqual(
                        [path.name for path in root.iterdir()],
                        ["result.json"],
                    )

    def test_writer_preserves_destination_when_file_write_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            destination = root / "result.json"
            destination.write_text("existing destination\n", encoding="utf-8")
            real_fdopen = protocol.os.fdopen

            class FailingWriter:
                def __init__(self, descriptor: int) -> None:
                    self.handle = real_fdopen(descriptor, "wb")

                def __enter__(self):
                    return self

                def __exit__(self, *_: object) -> None:
                    self.handle.close()

                def write(self, _: bytes) -> None:
                    raise OSError("injected file write failure")

            with mock.patch.object(
                protocol.os,
                "fdopen",
                side_effect=lambda descriptor, _: FailingWriter(descriptor),
            ):
                with self.assertRaisesRegex(OSError, "injected file write failure"):
                    protocol.write_artifact(destination, self.fixture())

            self.assertEqual(
                destination.read_text(encoding="utf-8"),
                "existing destination\n",
            )
            self.assertEqual(
                [path.name for path in root.iterdir()],
                ["result.json"],
            )

    def test_concurrent_comparator_outputs_are_complete_atomic_json(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            baseline = root / "baseline.json"
            candidate = root / "candidate.json"
            output = root / "nested" / "comparison.json"
            protocol.write_artifact(baseline, self.fixture())
            protocol.write_artifact(candidate, self.fixture())

            def compare(_: int) -> int:
                return protocol.main(
                    [
                        "compare",
                        str(baseline),
                        str(candidate),
                        "--output",
                        str(output),
                    ]
                )

            with contextlib.redirect_stdout(io.StringIO()):
                with ThreadPoolExecutor(max_workers=8) as executor:
                    statuses = list(executor.map(compare, range(24)))

            self.assertEqual(statuses, [0] * 24)
            result = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(result["status"], "pass")
            self.assertEqual(result["measurement"], "latency_ms")
            self.assertEqual(
                list(output.parent.glob(f".{output.name}.*.tmp")),
                [],
            )

class BenchmarkComparatorTests(unittest.TestCase):
    def fixture(self) -> dict:
        return json.loads((FIXTURES / "valid-full.json").read_text(encoding="utf-8"))

    def test_equivalent_scenarios_are_compared_non_blockingly(self) -> None:
        baseline = self.fixture()
        candidate = self.fixture()
        candidate["samples"]["measurements"]["latency_ms"] = [9.0, 10.0, 11.0]
        result = protocol.compare_artifacts(baseline, candidate)
        self.assertEqual(result["status"], "pass")
        self.assertTrue(result["non_blocking"])
        self.assertEqual(result["measurement"], "latency_ms")

    def test_regression_is_diagnostic_fail_not_process_gate(self) -> None:
        baseline = self.fixture()
        candidate = self.fixture()
        candidate["samples"]["measurements"]["latency_ms"] = [13.0, 14.0, 12.0]
        result = protocol.compare_artifacts(baseline, candidate)
        self.assertEqual(result["status"], "fail")
        self.assertEqual(result["reason"], "timing_threshold_exceeded")
        self.assertTrue(result["non_blocking"])

    def test_incomplete_or_absent_policy_cannot_evaluate_timing(self) -> None:
        baseline = self.fixture()
        candidate = self.fixture()
        candidate["timing"] = {"status": "not_evaluated", "non_blocking": True}
        result = protocol.compare_artifacts(baseline, candidate)
        self.assertEqual(result["status"], "not_evaluated")
        self.assertEqual(result["reason"], "missing_comparison_policy")

        baseline = self.fixture()
        candidate = self.fixture()
        baseline["timing"] = {"status": "not_evaluated", "non_blocking": True}
        result = protocol.compare_artifacts(baseline, candidate)
        self.assertEqual(result["status"], "not_evaluated")
        self.assertEqual(result["reason"], "missing_comparison_policy")

    def test_scenario_fingerprint_mismatches_are_rejected(self) -> None:
        mutations = {
            "smoke_full": lambda value: value["scenario"].update(profile="smoke"),
            "cache": lambda value: value["scenario"]["fingerprints"].update(
                cache_state="cold"
            ),
            "boundary": lambda value: value["scenario"].update(
                measured_boundary="server-only"
            ),
            "request_source": lambda value: value["scenario"]["fingerprints"].update(
                request_source="embedded-mcp"
            ),
            "concurrency": lambda value: value["scenario"]["fingerprints"].update(
                concurrency=4
            ),
            "build_profile": lambda value: value["build"].update(profile="debug"),
            "dataset_fingerprint": lambda value: value["scenario"]["fingerprints"][
                "dataset"
            ].update(fingerprint="sha256:" + "e" * 64),
            "dataset_cardinality": lambda value: value["scenario"]["fingerprints"][
                "dataset"
            ].update(cardinality=4),
            "workload": lambda value: value["scenario"].update(
                workload_id="different-workload"
            ),
        }
        for name, mutate in mutations.items():
            baseline = self.fixture()
            candidate = self.fixture()
            mutate(candidate)
            if name == "smoke_full":
                candidate["timing"] = {
                    "status": "not_evaluated",
                    "non_blocking": True,
                }
            with self.subTest(name=name):
                with self.assertRaises(protocol.IncomparableError):
                    protocol.compare_artifacts(baseline, candidate)

    def test_variability_and_independent_pair_policy_produce_inconclusive(self) -> None:
        baseline = self.fixture()
        candidate = self.fixture()
        baseline["timing"]["comparison_policy"][
            "minimum_independent_run_pairs"
        ] = 2
        candidate["timing"]["comparison_policy"][
            "minimum_independent_run_pairs"
        ] = 2
        result = protocol.compare_artifacts(baseline, candidate)
        self.assertEqual(result["status"], "inconclusive")
        self.assertEqual(result["reason"], "insufficient_independent_run_pairs")

    def test_error_rate_policy_applies_to_baseline_and_candidate(self) -> None:
        for violating_arm in ("baseline", "candidate"):
            baseline = self.fixture()
            candidate = self.fixture()
            violating = baseline if violating_arm == "baseline" else candidate
            violating["samples"].update(
                planned=4,
                attempted=4,
                successful=3,
                errors=1,
            )
            violating["timing"]["status"] = "inconclusive"

            with self.subTest(violating_arm=violating_arm):
                result = protocol.compare_artifacts(baseline, candidate)
                self.assertEqual(result["status"], "fail")
                self.assertEqual(result["reason"], "error_rate_threshold_exceeded")
                self.assertEqual(result["violating_arms"], [violating_arm])


if __name__ == "__main__":
    unittest.main()
