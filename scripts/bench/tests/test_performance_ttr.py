#!/usr/bin/env python3
from __future__ import annotations

import unittest
from pathlib import Path
import sys
from typing import Any, Mapping, Sequence

BENCH = Path(__file__).resolve().parents[1]
if str(BENCH) not in sys.path:
    sys.path.insert(0, str(BENCH))

from performance_protocol import _validate_ttr  # noqa: E402
from performance_scenarios import (  # noqa: E402
    CacheState,
    CentralCrashed,
    CentralIdentity,
    MalformedResult,
    McpToolError,
    OwnedSandboxTtrControl,
    RouteEvidence,
    TtrSampleSpec,
    run_ttr_scenario,
)


CASE = {"case_id": "holdout-1", "arguments": {"query": "needle"}, "oracle": {}}


class FakeClock:
    def __init__(self) -> None:
        self.now_ns = 1_000_000_000

    def __call__(self) -> int:
        return self.now_ns

    def advance(self, milliseconds: float) -> None:
        self.now_ns += int(milliseconds * 1_000_000)


class FakeTtrRuntime:
    def __init__(
        self,
        spec: TtrSampleSpec,
        clock: FakeClock,
        *,
        mode: str = "normal",
        identity_ordinal: int | None = None,
    ) -> None:
        ordinal = identity_ordinal if identity_ordinal is not None else int(spec.sample_id.rsplit("-", 1)[1])
        self.reset_id = f"reset-{spec.sample_id}"
        self.clock = clock
        self.mode = mode
        self.previous = CentralIdentity(100 + ordinal, 1_000 + ordinal, f"old-generation-{ordinal}")
        self.current = CentralIdentity(200 + ordinal, 2_000 + ordinal, f"new-generation-{ordinal}")
        if mode == "same_pid":
            self.current = CentralIdentity(self.previous.pid, 2_000 + ordinal, f"new-generation-{ordinal}")
        elif mode == "same_starttime":
            self.current = CentralIdentity(200 + ordinal, self.previous.starttime_ticks, f"new-generation-{ordinal}")
        elif mode == "same_generation":
            self.current = CentralIdentity(200 + ordinal, 2_000 + ordinal, self.previous.cache_generation)
        self.search_writes = 0
        self.alive_checks = 0
        self.closed = False
        self.calls: list[str] = []

    def previous_identity(self) -> CentralIdentity:
        self.calls.append("previous_identity")
        if self.mode == "prior_delay":
            self.clock.advance(11.0)
        return self.previous

    def spawn_central(self) -> None:
        self.calls.append("spawn_central")
        self.clock.advance(5.0 if self.mode != "timeout" else 50.0)

    def wait_exec(self, deadline_ns: int) -> CentralIdentity:
        del deadline_ns
        self.calls.append("wait_exec")
        self.clock.advance(2.0)
        return self.current

    def wait_ready(self, deadline_ns: int) -> None:
        del deadline_ns
        self.calls.append("wait_ready")
        self.clock.advance(3.0)
        if self.mode == "readiness_crash":
            raise CentralCrashed("central exited")

    def connect_route(self, deadline_ns: int) -> None:
        del deadline_ns
        self.calls.append("connect_route")
        self.clock.advance(2.0)
        if self.mode == "route_connect_error":
            raise RuntimeError("socket absent")

    def initialize(self, deadline_ns: int) -> None:
        del deadline_ns
        self.calls.append("initialize")
        self.clock.advance(1.0)

    def list_tools(self, deadline_ns: int) -> Sequence[str]:
        del deadline_ns
        self.calls.append("list_tools")
        self.clock.advance(2.0)
        if self.mode == "missing_tool":
            return ()
        return ("search_sessions",)

    def cache_state(self, deadline_ns: int) -> CacheState:
        del deadline_ns
        self.calls.append("cache_state")
        self.clock.advance(1.0)
        state = CacheState(
            label="fresh_moraine_existing_clickhouse",
            central_process="fresh",
            mcp_caches="fresh",
            target_query_prewarmed=False,
            clickhouse="seed_warmed",
            os_page_cache="uncontrolled",
            generation=self.current.cache_generation,
        )
        if self.mode == "cache_warm":
            return CacheState(
                label=state.label,
                central_process=state.central_process,
                mcp_caches="warm",
                target_query_prewarmed=True,
                clickhouse=state.clickhouse,
                os_page_cache=state.os_page_cache,
                generation=state.generation,
            )
        return state

    def route_evidence(self, deadline_ns: int) -> RouteEvidence:
        del deadline_ns
        self.calls.append("route_evidence")
        fallback = self.mode == "fallback"
        searches = self.search_writes
        requests = self.search_writes
        if self.mode == "prewarm" and self.search_writes == 0:
            searches = 1
        if self.mode == "two_requests" and self.search_writes:
            searches = 2
            requests = 2
        return RouteEvidence(
            benchmark_searches=searches,
            central_requests=requests,
            route_marker="" if fallback else "proxying stdio to central MCP server",
            central_proxy_observed=not fallback,
            embedded_fallback=fallback,
        )

    def write_search(self, case: Mapping[str, Any], deadline_ns: int) -> None:
        del deadline_ns
        self.calls.append("write_search")
        self.assert_case(case)
        self.clock.advance(1.0)
        self.search_writes += 1

    def read_search(self, deadline_ns: int) -> Mapping[str, Any]:
        del deadline_ns
        self.calls.append("read_search")
        self.clock.advance(5.0)
        if self.mode == "malformed":
            raise MalformedResult("missing structured content")
        if self.mode == "tool_error":
            raise McpToolError("search failed")
        if self.mode == "wrong":
            return {"ok": False}
        return {"ok": True}

    @staticmethod
    def assert_case(case: Mapping[str, Any]) -> None:
        if case is not CASE:
            raise AssertionError("suite case was not passed unchanged")

    def central_alive(self) -> bool:
        self.calls.append("central_alive")
        self.alive_checks += 1
        return not (self.mode == "crash_after_response" and self.alive_checks >= 2)

    def close(self) -> None:
        self.calls.append("close")
        self.closed = True
        if self.mode == "cleanup_error":
            raise RuntimeError("cannot terminate central")

    def surviving_children(self) -> Sequence[int]:
        self.calls.append("surviving_children")
        return (999,) if self.mode == "child" else ()


class TtrScenarioTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock()
        self.runtimes: list[FakeTtrRuntime] = []

    def factory(self, mode: str = "normal", *, same_identity: bool = False):
        def create(spec: TtrSampleSpec) -> FakeTtrRuntime:
            runtime = FakeTtrRuntime(
                spec,
                self.clock,
                mode=mode,
                identity_ordinal=1 if same_identity else None,
            )
            self.runtimes.append(runtime)
            return runtime

        return create

    def oracle(self, result: Mapping[str, Any], case: Mapping[str, Any]) -> None:
        self.clock.advance(2.0)
        self.assertIs(case, CASE)
        if result != {"ok": True}:
            raise AssertionError("wrong exact oracle result")

    def run_mode(self, mode: str = "normal", *, samples: int = 1, timeout_s: float = 1.0):
        return run_ttr_scenario(
            [CASE],
            self.factory(mode),
            self.oracle,
            samples=samples,
            timeout_s=timeout_s,
            clock_ns=self.clock,
        )

    def test_spawn_delay_is_included_and_nonoverlapping_phases_reconcile(self) -> None:
        result = self.run_mode()
        self.assertEqual(result.status, "pass")
        sample = result.samples[0]
        self.assertEqual(
            sample["phases_ms"],
            {
                "spawn_exec": 7.0,
                "central_readiness": 3.0,
                "route_connect": 2.0,
                "initialize_tools": 4.0,
                "request_write": 1.0,
                "response_read": 5.0,
                "oracle_validation": 2.0,
            },
        )
        self.assertEqual(sample["total_ms"], 24.0)
        self.assertEqual(sample["total_ms"], sum(sample["phases_ms"].values()))
        self.assertEqual(result.metrics, {"direction": "lower", "p95_ms": 24.0, "sample_count": 1})

    def test_prior_identity_observation_is_outside_spawn_boundary(self) -> None:
        result = self.run_mode("prior_delay")
        self.assertEqual(result.status, "pass")
        self.assertEqual(result.samples[0]["total_ms"], 24.0)
        self.assertEqual(result.samples[0]["phases_ms"]["spawn_exec"], 7.0)

    def test_normal_run_has_zero_prewarm_one_central_request_and_proxy_marker(self) -> None:
        result = self.run_mode()
        sample = result.samples[0]
        self.assertTrue(sample["valid"])
        self.assertIsNone(sample["error_code"])
        self.assertEqual(sample["searches_before_write"], 0)
        self.assertEqual(sample["central_request_count"], 1)
        self.assertFalse(sample["embedded_fallback"])
        self.assertTrue(sample["route_marker_sha256"].startswith("sha256:"))
        runtime = self.runtimes[0]
        self.assertEqual(runtime.search_writes, 1)
        self.assertLess(runtime.calls.index("route_evidence"), runtime.calls.index("write_search"))
        self.assertTrue(runtime.closed)
        self.assertTrue(_validate_ttr(result.metrics, result.samples))

    def test_wrong_oracle_result_never_ends_successfully(self) -> None:
        result = self.run_mode("wrong")
        self.assertEqual(result.status, "fail")
        self.assertFalse(result.samples[0]["valid"])
        self.assertEqual(result.samples[0]["error_code"], "oracle")
        self.assertEqual(result.semantic_failures, 1)
        self.assertEqual(self.runtimes[0].search_writes, 1)

    def test_malformed_and_tool_error_results_fail_without_retry(self) -> None:
        for mode, expected in (("malformed", "malformed_result"), ("tool_error", "mcp_error")):
            with self.subTest(mode=mode):
                self.setUp()
                result = self.run_mode(mode)
                self.assertEqual(result.status, "fail")
                self.assertEqual(result.samples[0]["error_code"], expected)
                self.assertEqual(self.runtimes[0].search_writes, 1)

    def test_timeout_in_spawn_is_included_and_fails(self) -> None:
        result = self.run_mode("timeout", timeout_s=0.01)
        sample = result.samples[0]
        self.assertEqual(result.status, "fail")
        self.assertEqual(sample["error_code"], "timeout")
        self.assertGreaterEqual(sample["phases_ms"]["spawn_exec"], 50.0)
        self.assertEqual(sample["total_ms"], sum(sample["phases_ms"].values()))
        self.assertEqual(result.metrics["p95_ms"], sample["total_ms"])
        self.assertFalse(_validate_ttr(result.metrics, result.samples))

    def test_central_crash_fails(self) -> None:
        for mode in ("readiness_crash", "crash_after_response"):
            with self.subTest(mode=mode):
                self.setUp()
                result = self.run_mode(mode)
                self.assertEqual(result.status, "fail")
                self.assertEqual(result.samples[0]["error_code"], "central_crash")

    def test_surviving_child_fails_and_stops_later_sample(self) -> None:
        result = self.run_mode("child", samples=2)
        self.assertEqual(result.status, "fail")
        self.assertEqual(result.samples[0]["error_code"], "surviving_child")
        self.assertEqual(result.samples[1]["error_code"], "not_run_after_child_leak")
        self.assertEqual(len(self.runtimes), 1)

    def test_cleanup_failure_fails_sample(self) -> None:
        result = self.run_mode("cleanup_error")
        self.assertEqual(result.status, "fail")
        self.assertEqual(result.samples[0]["error_code"], "cleanup")
        self.assertIn("cleanup_failed", result.diagnostics)

    def test_removed_central_socket_embedded_fallback_is_rejected(self) -> None:
        result = self.run_mode("fallback")
        sample = result.samples[0]
        self.assertEqual(result.status, "fail")
        self.assertEqual(sample["error_code"], "route_proof")
        self.assertTrue(sample["embedded_fallback"])
        self.assertEqual(self.runtimes[0].search_writes, 0)

    def test_route_connection_error_is_rejected(self) -> None:
        result = self.run_mode("route_connect_error")
        self.assertEqual(result.status, "fail")
        self.assertEqual(result.samples[0]["error_code"], "runtime_error")
        self.assertEqual(self.runtimes[0].search_writes, 0)

    def test_prewarm_and_nonexact_central_request_count_fail(self) -> None:
        for mode in ("prewarm", "two_requests"):
            with self.subTest(mode=mode):
                self.setUp()
                result = self.run_mode(mode)
                self.assertEqual(result.status, "fail")
                self.assertEqual(result.samples[0]["error_code"], "route_proof")

    def test_pid_reuse_or_equal_starttime_is_valid_when_process_tuple_is_new(self) -> None:
        for mode in ("same_pid", "same_starttime"):
            with self.subTest(mode=mode):
                self.setUp()
                self.assertEqual(self.run_mode(mode).status, "pass")

    def test_cache_generation_must_be_new(self) -> None:
        result = self.run_mode("same_generation")
        self.assertEqual(result.status, "fail")
        self.assertEqual(result.samples[0]["error_code"], "freshness")
        self.assertEqual(self.runtimes[0].search_writes, 0)

    def test_identity_cannot_repeat_across_samples(self) -> None:
        result = run_ttr_scenario(
            [CASE],
            self.factory(same_identity=True),
            self.oracle,
            samples=2,
            timeout_s=1.0,
            clock_ns=self.clock,
        )
        self.assertEqual(result.status, "fail")
        self.assertTrue(result.samples[0]["valid"])
        self.assertEqual(result.samples[1]["error_code"], "freshness")

    def test_warm_cache_or_target_prewarm_is_rejected(self) -> None:
        result = self.run_mode("cache_warm")
        self.assertEqual(result.status, "fail")
        self.assertEqual(result.samples[0]["error_code"], "cache_state")
        self.assertEqual(self.runtimes[0].search_writes, 0)

    def test_sample_contains_only_protocol_allowlisted_keys(self) -> None:
        sample = self.run_mode().samples[0]
        self.assertEqual(
            set(sample),
            {
                "sample_id",
                "total_ms",
                "phases_ms",
                "pid_start_sha256",
                "cache_generation_sha256",
                "route_marker_sha256",
                "searches_before_write",
                "central_request_count",
                "embedded_fallback",
                "valid",
                "error_code",
            },
        )

class FakeStartProcess:
    def __init__(self) -> None:
        self.returncode = 0
        self.pid = 12345

    def communicate(self, timeout: float):
        if timeout <= 0:
            raise AssertionError("deadline was not propagated")
        return b"", b""

    def wait(self, timeout: float):
        return self.returncode


class FakeOwnedTtrSandbox:
    sandbox_id = "owned-ttr"

    def __init__(self, *, initially_running: bool = True) -> None:
        self.running = initially_running
        self.ordinal = 1 if initially_running else 0
        self.stop_calls = 0
        self.spawn_calls = 0

    def central_status(self) -> Mapping[str, Any]:
        if not self.running:
            raise RuntimeError("central is stopped")
        return {
            "central": {"pid": 100 + self.ordinal, "starttime": 1_000 + self.ordinal},
            "ingest": {"pid": 200 + self.ordinal, "starttime": 2_000 + self.ordinal},
            "cache_generation": f"00000000-0000-4000-8000-{self.ordinal:012d}",
            "server_children": [100 + self.ordinal, 200 + self.ordinal],
            "route_processes": [],
        }

    def stop_central(self) -> None:
        self.stop_calls += 1
        self.running = False

    def spawn_central(self) -> FakeStartProcess:
        self.spawn_calls += 1
        self.ordinal += 1
        self.running = True
        return FakeStartProcess()

    def wait_central_ready_without_search(
        self, start_process=None, *, timeout_s: float = 120.0
    ) -> Mapping[str, Any]:
        if start_process is not None:
            start_process.communicate(timeout_s)
        return self.central_status()

    def spawn_stdio_route(self):
        raise AssertionError("control test does not open the MCP route")


class OwnedSandboxTtrAdapterTests(unittest.TestCase):
    def test_control_prepares_prior_outside_boundary_and_drives_fresh_spawn(self) -> None:
        clock = FakeClock()
        sandbox = FakeOwnedTtrSandbox()
        control = OwnedSandboxTtrControl(
            sandbox, sample_id="ttr-001", clock_ns=clock
        )
        previous = control.previous_identity()
        self.assertFalse(sandbox.running)
        control.spawn_central()
        current = control.wait_exec(clock() + 1_000_000_000)
        control.wait_ready(clock() + 1_000_000_000)
        self.assertNotEqual(previous.pid, current.pid)
        self.assertNotEqual(previous.starttime_ticks, current.starttime_ticks)
        self.assertNotEqual(previous.cache_generation, current.cache_generation)
        self.assertEqual(
            control.cache_state(clock() + 1_000_000_000).label,
            "fresh_moraine_existing_clickhouse",
        )
        self.assertTrue(control.central_alive())
        control.close()
        self.assertFalse(sandbox.running)
        self.assertEqual(control.surviving_children(), ())

    def test_stopped_sandbox_gets_unmeasured_prior_generation_then_stops_it(self) -> None:
        clock = FakeClock()
        sandbox = FakeOwnedTtrSandbox(initially_running=False)
        control = OwnedSandboxTtrControl(
            sandbox, sample_id="ttr-002", clock_ns=clock
        )
        self.assertEqual(sandbox.spawn_calls, 1)
        self.assertEqual(sandbox.stop_calls, 1)
        self.assertFalse(sandbox.running)
        self.assertGreater(control.previous_identity().pid, 0)



if __name__ == "__main__":
    unittest.main()
