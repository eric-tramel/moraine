#!/usr/bin/env python3
from __future__ import annotations

import io

import threading
import time
import unittest
from unittest.mock import Mock, patch
from collections import Counter
from pathlib import Path
import sys
from typing import Any, Mapping, Sequence

BENCH = Path(__file__).resolve().parents[1]
if str(BENCH) not in sys.path:
    sys.path.insert(0, str(BENCH))

from performance_fixtures import FixtureError, build_recipe  # noqa: E402
from performance_protocol import _validate_qps  # noqa: E402
from performance_scenarios import (  # noqa: E402
    AdmissionRejected,
    FULL_QPS_POLICY,
    McpProtocolError,
    OwnedSandboxQpsControl,
    QpsPolicy,
    QpsTrialSpec,
    RequestTimeout,
    SMOKE_QPS_POLICY,
    _QpsTrialExecution,
    STDIO_LINE_LIMIT_BYTES,
    _StdioJsonRpcClient,
    _empty_qps_sample,
    _empty_qps_telemetry,
    _execute_open_arrival_trial,
    make_owned_sandbox_qps_runtime_factory,
    run_qps_sweep,
    validate_query_oracle,
)


CASE = {"case_id": "case-1", "arguments": {"query": "needle"}, "oracle": {}}


def oracle(result: Mapping[str, Any], case: Mapping[str, Any]) -> None:
    del case
    if result != {"ok": True}:
        raise AssertionError("wrong result")


class FakeRuntime:
    def __init__(
        self,
        reset_id: str,
        *,
        outcome: str = "success",
        delay_s: float = 0.0,
        leak: bool = False,
        telemetry: Mapping[str, int] | None = None,
    ) -> None:
        self.reset_id = reset_id
        self.outcome = outcome
        self.delay_s = delay_s
        self.leak = leak
        self.telemetry_values = dict(telemetry or _empty_qps_telemetry())
        self.aborted = threading.Event()
        self.closed = False
        self.active = 0
        self.max_active = 0
        self.lock = threading.Lock()

    def search(self, case: Mapping[str, Any], timeout_s: float) -> Mapping[str, Any]:
        del case, timeout_s
        with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            if self.delay_s:
                self.aborted.wait(self.delay_s)
            if self.outcome == "rejected":
                raise AdmissionRejected("busy")
            if self.outcome == "timeout":
                raise RequestTimeout("deadline")
            if self.outcome == "protocol":
                raise McpProtocolError("bad frame")
            if self.outcome == "interrupt":
                raise KeyboardInterrupt
            if self.outcome == "wrong":
                return {"ok": False}
            return {"ok": True}
        finally:
            with self.lock:
                self.active -= 1

    def telemetry(self) -> Mapping[str, Any]:
        return self.telemetry_values

    def abort(self) -> None:
        self.aborted.set()

    def close(self) -> None:
        self.aborted.set()
        self.closed = True

    def leaked_work(self) -> bool:
        return self.leak or self.active != 0


def short_policy(**overrides: Any) -> QpsPolicy:
    values = {
        "profile": "test",
        "duration_s": 0.02,
        "replicates": 1,
        "maximum_qps": 100,
        "p95_limit_ms": 750.0,
        "p99_limit_ms": 2_000.0,
        "hard_deadline_ms": 500.0,
        "scheduler_p99_slip_limit_ms": 20.0,
        "drain_limit_s": 0.1,
        "max_scheduler_workers": 8,
    }
    values.update(overrides)
    return QpsPolicy(**values)


class OpenArrivalTrialTests(unittest.TestCase):
    def run_trial(self, runtime: FakeRuntime, policy: QpsPolicy | None = None) -> _QpsTrialExecution:
        selected = policy or short_policy()
        spec = QpsTrialSpec("test", 100, 1, selected.duration_s)
        return _execute_open_arrival_trial(spec, runtime, [CASE], oracle, selected)

    def test_fast_rejections_never_count_as_goodput(self) -> None:
        execution = self.run_trial(FakeRuntime("reset-rejected", outcome="rejected"))
        outcomes = execution.sample["outcomes"]
        self.assertEqual(outcomes["rejected"], outcomes["planned"])
        self.assertEqual(outcomes["correct"], 0)
        self.assertEqual(execution.sample["achieved_goodput_qps"], 0.0)
        self.assertFalse(execution.sample["passed"])

    def test_fast_protocol_errors_never_count_as_goodput(self) -> None:
        execution = self.run_trial(FakeRuntime("reset-error", outcome="protocol"))
        outcomes = execution.sample["outcomes"]
        self.assertEqual(outcomes["protocol_error"], outcomes["planned"])
        self.assertEqual(outcomes["correct"], 0)
        self.assertFalse(execution.sample["passed"])

    def test_wrong_oracle_result_is_semantic_failure(self) -> None:
        execution = self.run_trial(FakeRuntime("reset-wrong", outcome="wrong"))
        self.assertEqual(execution.sample["outcomes"]["semantic_error"], 2)
        self.assertEqual(execution.sample["outcomes"]["correct"], 0)
        self.assertFalse(execution.sample["passed"])

    def test_oversized_stdio_frame_is_bounded_and_kills_route(self) -> None:
        client = _StdioJsonRpcClient.__new__(_StdioJsonRpcClient)
        client.proc = Mock()
        stream = io.BytesIO(b"x" * (STDIO_LINE_LIMIT_BYTES + 1))
        with self.assertRaisesRegex(McpProtocolError, "bounded frame"):
            client._read_line(stream, "stdout")
        client.proc.kill.assert_called_once()

    def test_process_control_interrupt_is_not_classified_as_a_sample_error(self) -> None:
        runtime = FakeRuntime("reset-interrupt", outcome="interrupt")
        with self.assertRaises(KeyboardInterrupt):
            self.run_trial(runtime)
        self.assertTrue(runtime.closed)

    def test_timeout_is_terminal_and_not_correct(self) -> None:
        execution = self.run_trial(FakeRuntime("reset-timeout", outcome="timeout"))
        self.assertEqual(execution.sample["outcomes"]["timed_out"], 2)
        self.assertEqual(execution.sample["outcomes"]["correct"], 0)

    def test_open_arrivals_overlap(self) -> None:
        runtime = FakeRuntime("reset-overlap", delay_s=0.015)
        execution = self.run_trial(runtime)
        self.assertEqual(execution.sample["outcomes"]["correct"], 2)
        self.assertGreaterEqual(execution.sample["max_in_flight"], 2)
        self.assertGreaterEqual(runtime.max_active, 2)
        self.assertTrue(execution.sample["passed"])

    def test_undrained_backlog_fails_and_is_aborted_before_return(self) -> None:
        runtime = FakeRuntime("reset-backlog", delay_s=0.2)
        execution = self.run_trial(runtime, short_policy(drain_limit_s=0.005))
        self.assertGreater(execution.sample["backlog_at_end"], 0)
        self.assertFalse(execution.sample["drained"])
        self.assertFalse(execution.sample["passed"])
        self.assertTrue(runtime.aborted.is_set())
        self.assertFalse(runtime.leaked_work())

    def test_scheduler_saturation_fails_start_slip_gate(self) -> None:
        runtime = FakeRuntime("reset-saturated", delay_s=0.02)
        execution = self.run_trial(
            runtime,
            short_policy(max_scheduler_workers=1, scheduler_p99_slip_limit_ms=1.0),
        )
        self.assertGreater(execution.sample["scheduler_p99_start_slip_ms"], 1.0)
        self.assertFalse(execution.sample["passed"])

    def test_runtime_leak_is_infrastructure_failure(self) -> None:
        execution = self.run_trial(FakeRuntime("reset-leak", leak=True))
        self.assertIn("runtime_leak", execution.infrastructure_errors)
        self.assertFalse(execution.sample["passed"])

    def test_telemetry_is_published_per_trial(self) -> None:
        telemetry = _empty_qps_telemetry()
        telemetry["cpu_usage_usec_delta"] = 7
        execution = self.run_trial(FakeRuntime("reset-telemetry", telemetry=telemetry))
        self.assertEqual(execution.sample["telemetry"], telemetry)

    def test_swap_oom_and_memory_ceiling_each_fail_the_resource_gate(self) -> None:
        for key, value in (
            ("swap_current_bytes", 1),
            ("oom_kill_delta", 1),
            ("memory_peak_bytes", 8 * 1024**3),
        ):
            with self.subTest(key=key):
                telemetry = _empty_qps_telemetry()
                telemetry[key] = value
                execution = self.run_trial(FakeRuntime(f"reset-{key}", telemetry=telemetry))
                self.assertFalse(execution.sample["passed"])


class DummyRuntime:
    def __init__(self, reset_id: str) -> None:
        self.reset_id = reset_id

    def abort(self) -> None:
        pass

    def close(self) -> None:
        pass


class ScriptedSweep:
    def __init__(
        self,
        threshold: int,
        *,
        fail_replicate: tuple[int, int] | None = None,
        protocol_error_rate: int | None = None,
        leak_rate: int | None = None,
    ) -> None:
        self.threshold = threshold
        self.fail_replicate = fail_replicate
        self.protocol_error_rate = protocol_error_rate
        self.leak_rate = leak_rate
        self.calls: list[QpsTrialSpec] = []

    def factory(self, spec: QpsTrialSpec) -> DummyRuntime:
        self.calls.append(spec)
        return DummyRuntime(f"reset-{spec.offered_qps}-{spec.replicate}-{len(self.calls)}")

    def execute(
        self,
        spec: QpsTrialSpec,
        runtime: DummyRuntime,
        cases: Sequence[Mapping[str, Any]],
        validate: Any,
        policy: QpsPolicy,
    ) -> _QpsTrialExecution:
        del cases, validate
        planned = max(1, int(round(spec.offered_qps * policy.duration_s)))
        sample = _empty_qps_sample(spec, planned)
        passed = spec.offered_qps <= self.threshold and self.fail_replicate != (
            spec.offered_qps,
            spec.replicate,
        )
        sample.update(
            {
                "achieved_goodput_qps": spec.offered_qps if passed else 0.0,
                "p95_ms": 10.0,
                "p99_ms": 10.0,
                "max_ms": 10.0,
                "scheduler_p99_start_slip_ms": 0.1,
                "drain_ms": 0.0,
                "drained": True,
                "passed": passed,
                "telemetry": _empty_qps_telemetry(),
            }
        )
        sample["outcomes"]["started"] = planned
        sample["outcomes"]["dropped"] = 0
        if passed:
            sample["outcomes"]["correct"] = planned
        else:
            sample["outcomes"]["rejected"] = planned
        if self.protocol_error_rate == spec.offered_qps:
            sample["outcomes"]["rejected"] = 0
            sample["outcomes"]["protocol_error"] = planned
        errors = ["runtime_leak"] if self.leak_rate == spec.offered_qps else []
        return _QpsTrialExecution(sample, runtime.reset_id, errors)


def sweep_policy(profile: str = "full", replicates: int = 3, maximum: int = 8) -> QpsPolicy:
    return QpsPolicy(
        profile=profile,
        duration_s=1.0,
        replicates=replicates,
        maximum_qps=maximum,
    )


class BracketingTests(unittest.TestCase):
    def run_script(self, script: ScriptedSweep, policy: QpsPolicy | None = None):
        selected = policy or sweep_policy()
        return run_qps_sweep(
            [CASE], script.factory, oracle, selected, trial_executor=script.execute
        )

    def test_doubling_then_integer_binary_bracket_runs_three_fresh_resets_each(self) -> None:
        script = ScriptedSweep(5)
        result = self.run_script(script)
        self.assertEqual(result.status, "pass")
        self.assertEqual(
            result.metrics,
            {
                "direction": "higher",
                "sustainable_qps": 5,
                "capacity_lower_qps": 5,
                "capacity_upper_qps": 6,
                "capacity_censoring": "none",
            },
        )
        rates = Counter(spec.offered_qps for spec in script.calls)
        self.assertEqual(rates, Counter({1: 3, 2: 3, 4: 3, 8: 3, 6: 3, 5: 3}))
        reset_ids = [execution["replicate"] for execution in result.samples]
        self.assertEqual(len(result.samples), 18)
        self.assertEqual(Counter(reset_ids)[1], 6)
        reset_hashes = [sample["reset_sha256"] for sample in result.samples]
        self.assertEqual(len(set(reset_hashes)), len(reset_hashes))
        self.assertTrue(all(value.startswith("sha256:") for value in reset_hashes))

    def test_single_failed_replicate_fails_the_rate(self) -> None:
        script = ScriptedSweep(8, fail_replicate=(4, 2))
        result = self.run_script(script)
        self.assertEqual(result.metrics["capacity_lower_qps"], 3)
        self.assertEqual(result.metrics["capacity_upper_qps"], 4)
        failed = [s for s in result.samples if s["offered_qps"] == 4 and not s["passed"]]
        self.assertEqual([sample["replicate"] for sample in failed], [2])

    def test_all_pass_is_right_censored_and_inconclusive(self) -> None:
        result = self.run_script(ScriptedSweep(8))
        self.assertEqual(result.status, "inconclusive")
        self.assertEqual(result.metrics["capacity_censoring"], "right")
        self.assertEqual(result.metrics["capacity_lower_qps"], 8)
        self.assertEqual(result.metrics["capacity_upper_qps"], 8)

    def test_all_fail_is_left_censored_and_inconclusive(self) -> None:
        result = self.run_script(ScriptedSweep(0))
        self.assertEqual(result.status, "inconclusive")
        self.assertEqual(result.metrics["sustainable_qps"], 0)
        self.assertEqual(result.metrics["capacity_lower_qps"], 0)
        self.assertEqual(result.metrics["capacity_upper_qps"], 1)
        self.assertEqual(result.metrics["capacity_censoring"], "left")

    def test_smoke_is_one_replicate_three_seconds_and_capped_at_sixteen(self) -> None:
        script = ScriptedSweep(16)
        policy = QpsPolicy(profile="smoke", duration_s=3, replicates=1, maximum_qps=16)
        result = self.run_script(script, policy)
        self.assertEqual(result.status, "pass")
        self.assertEqual(result.metrics["capacity_censoring"], "right")
        self.assertEqual(result.metrics["capacity_lower_qps"], 16)
        self.assertEqual([spec.offered_qps for spec in script.calls], [16])
        self.assertEqual(script.calls[0].duration_s, 3)
        self.assertEqual(script.calls[0].replicate, 1)

    def test_smoke_failure_reports_only_the_capped_trial(self) -> None:
        script = ScriptedSweep(0)
        policy = QpsPolicy(profile="smoke", duration_s=3, replicates=1, maximum_qps=16)
        result = self.run_script(script, policy)
        self.assertEqual(result.status, "fail")
        self.assertEqual(
            result.metrics,
            {
                "direction": "higher",
                "sustainable_qps": 0,
                "capacity_lower_qps": 0,
                "capacity_upper_qps": 16,
                "capacity_censoring": "none",
            },
        )
        self.assertEqual([spec.offered_qps for spec in script.calls], [16])

    def test_missing_replicate_is_a_failed_scenario_but_other_replicates_run(self) -> None:
        script = ScriptedSweep(8)

        def missing_factory(spec: QpsTrialSpec) -> DummyRuntime:
            script.calls.append(spec)
            if spec.offered_qps == 2 and spec.replicate == 2:
                raise RuntimeError("reset failed")
            return DummyRuntime(f"reset-{spec.offered_qps}-{spec.replicate}")

        result = run_qps_sweep(
            [CASE], missing_factory, oracle, sweep_policy(), trial_executor=script.execute
        )
        self.assertEqual(result.status, "fail")
        rate_two = [spec for spec in script.calls if spec.offered_qps == 2]
        self.assertEqual([spec.replicate for spec in rate_two], [1, 2, 3])
        self.assertIn("replicate_failed", result.diagnostics)
        self.assertIn("reset_identity_missing", result.diagnostics)

    def test_duplicate_physical_reset_identity_fails_closed(self) -> None:
        script = ScriptedSweep(8)
        result = run_qps_sweep(
            [CASE], lambda spec: DummyRuntime("same-reset"), oracle, sweep_policy(), trial_executor=script.execute
        )
        self.assertEqual(result.status, "fail")
        self.assertIn("reset_identity_reused", result.diagnostics)
        self.assertEqual(
            len({sample["reset_sha256"] for sample in result.samples}), 1
        )

    def test_protocol_error_at_saturation_is_correctness_failure(self) -> None:
        result = self.run_script(ScriptedSweep(4, protocol_error_rate=8))
        self.assertEqual(result.status, "fail")
        self.assertGreater(result.semantic_failures, 0)
        self.assertEqual(result.metrics["capacity_lower_qps"], 4)
        self.assertEqual(result.metrics["capacity_upper_qps"], 5)

    def test_uncontained_work_stops_later_rates_and_fails(self) -> None:
        script = ScriptedSweep(8, leak_rate=2)
        result = self.run_script(script)
        self.assertEqual(result.status, "fail")
        self.assertIn("runtime_leak", result.diagnostics)
        self.assertNotIn(4, [spec.offered_qps for spec in script.calls])

class FakeOwnedSandbox:
    def __init__(self, sandbox_id: str, *, fail_down: bool = False) -> None:
        self.sandbox_id = sandbox_id
        self.fail_down = fail_down
        self.down_calls = 0

    def spawn_stdio_route(self):
        raise AssertionError("route spawning is covered by the stdio adapter")

    def down(self) -> None:
        self.down_calls += 1
        if self.fail_down:
            raise RuntimeError("owned cleanup failed")


class OwnedSandboxQpsAdapterTests(unittest.TestCase):
    def test_control_exposes_allowlisted_telemetry_and_proven_cleanup(self) -> None:
        sandbox = FakeOwnedSandbox("sandbox-one")
        telemetry = _empty_qps_telemetry()
        control = OwnedSandboxQpsControl(sandbox, lambda observed: telemetry)
        self.assertEqual(control.reset_id, "sandbox-one")
        self.assertIs(control.telemetry(), telemetry)
        self.assertTrue(control.leaked_work())
        control.close()
        self.assertFalse(control.leaked_work())
        self.assertEqual(sandbox.down_calls, 1)
        control.close()
        self.assertEqual(sandbox.down_calls, 1)

    def test_cleanup_failure_remains_an_owned_leak(self) -> None:
        control = OwnedSandboxQpsControl(
            FakeOwnedSandbox("sandbox-leak", fail_down=True),
            lambda observed: _empty_qps_telemetry(),
        )
        with self.assertRaises(RuntimeError):
            control.close()
        self.assertTrue(control.leaked_work())

    def test_factory_requires_and_uses_a_fresh_sandbox_per_trial(self) -> None:
        created: list[FakeOwnedSandbox] = []

        def create(spec: QpsTrialSpec) -> FakeOwnedSandbox:
            sandbox = FakeOwnedSandbox(
                f"sandbox-{spec.offered_qps}-{spec.replicate}-{len(created)}"
            )
            created.append(sandbox)
            return sandbox

        with patch("performance_scenarios.StdioQpsTrialRuntime", lambda control: control):
            factory = make_owned_sandbox_qps_runtime_factory(
                create, lambda observed: _empty_qps_telemetry()
            )
            first = factory(QpsTrialSpec("test", 1, 1, 1.0))
            second = factory(QpsTrialSpec("test", 1, 2, 1.0))
        self.assertEqual(len(created), 2)
        self.assertNotEqual(first.reset_id, second.reset_id)

class ExactOracleAdapterTests(unittest.TestCase):
    def test_fixture_owned_production_oracle_is_the_scenario_default(self) -> None:
        case = build_recipe("smoke")["query_splits"]["research"][0]
        identities = case["oracle"]["ordered_identities"]
        structured = {
            "schema_version": "moraine.mcp.search_sessions.v1",
            "tool": "search_sessions",
            "data": {
                "result_count": len(identities),
                "truncated": case["oracle"]["truncated"],
                "results": [
                    {
                        "event": {"id": identity["event_id"]},
                        "session": {"id": identity["session_id"]},
                    }
                    for identity in identities
                ],
            },
        }
        validate_query_oracle(structured, case)
        structured["data"]["results"][0]["event"]["id"] = "event:wrong"
        with self.assertRaises(FixtureError):
            validate_query_oracle(structured, case)


class QpsProtocolIntegrationTests(unittest.TestCase):
    def test_protocol_policies_emit_integer_duration_samples(self) -> None:
        for policy in (SMOKE_QPS_POLICY, FULL_QPS_POLICY):
            sample = _empty_qps_sample(
                QpsTrialSpec(policy.profile, 1, 1, policy.duration_s),
                int(policy.duration_s),
            )
            self.assertIs(type(policy.duration_s), int)
            self.assertIs(type(sample["duration_s"]), int)

    def test_full_sweep_shape_and_bracket_are_accepted_by_protocol(self) -> None:
        script = ScriptedSweep(5)
        policy = QpsPolicy(
            profile="full", duration_s=30, replicates=3, maximum_qps=512
        )
        result = run_qps_sweep(
            [CASE], script.factory, oracle, policy, trial_executor=script.execute
        )
        conclusive, resets = _validate_qps(
            result.metrics, result.samples, {"profile": "full"}
        )
        self.assertTrue(conclusive)
        self.assertEqual(len(resets), len(result.samples))

    def test_single_capped_smoke_trial_is_accepted_and_noncomparable(self) -> None:
        script = ScriptedSweep(16)
        policy = QpsPolicy(
            profile="smoke", duration_s=3, replicates=1, maximum_qps=16
        )
        result = run_qps_sweep(
            [CASE], script.factory, oracle, policy, trial_executor=script.execute
        )
        conclusive, resets = _validate_qps(
            result.metrics, result.samples, {"profile": "smoke"}
        )
        self.assertFalse(conclusive)
        self.assertEqual(len(resets), 1)




if __name__ == "__main__":
    unittest.main()
