#!/usr/bin/env python3
"""Focused fixed-envelope and owned performance sandbox tests."""
from __future__ import annotations

import hashlib
import io
import importlib.util
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import shutil
import unittest
from unittest import mock


REPO = Path(__file__).resolve().parents[4]
RUNTIME_PATH = REPO / "scripts/bench/performance_runtime.py"
SANDBOX = REPO / "scripts/dev/sandbox/moraine-sandbox"
COMPOSE = REPO / "scripts/bench/compose.performance.yaml"
BASE_COMPOSE = REPO / "scripts/dev/sandbox/compose.yaml"
ENTRYPOINT = REPO / "scripts/dev/sandbox/performance-entrypoint.sh"
SPEC = importlib.util.spec_from_file_location("performance_runtime_sandbox_tests", RUNTIME_PATH)
assert SPEC and SPEC.loader
runtime = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = runtime
SPEC.loader.exec_module(runtime)


class FakeCgroupHost:
    def __init__(self, directory: Path) -> None:
        self.root = directory / "cgroup"
        self.proc = directory / "proc"
        self.root.mkdir()
        self.proc.mkdir()
        (self.proc / "meminfo").write_text(
            "MemTotal:       33554432 kB\nMemAvailable:   25165824 kB\n"
        )
        self.materialize(self.root)
        self.family = self.root / "moraine-performance"
        self.family.mkdir()
        self.materialize(self.family)

    def materialize(self, path: Path, *, controllers: str = "cpu memory cpuset") -> None:
        values = {
            "cgroup.controllers": controllers + "\n",
            "cgroup.subtree_control": "cpu memory cpuset\n",
            "cpuset.cpus.effective": "0-3\n",
            "cpu.max": "max 100000\n",
            "cpu.stat": (
                "usage_usec 100\nuser_usec 50\nsystem_usec 50\n"
                "nr_periods 1\nnr_throttled 0\nthrottled_usec 0\n"
            ),
            "memory.max": "max\n",
            "memory.current": "1024\n",
            "memory.peak": "2048\n",
            "memory.swap.max": "max\n",
            "memory.swap.current": "0\n",
            "memory.events": "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n",
            "cgroup.procs": "",
            "pids.current": "0\n",
        }
        for name, value in values.items():
            (path / name).write_text(value)

    def patch_mkdir(self):
        original = Path.mkdir
        root = self.root.resolve()
        materialize = self.materialize

        def mkdir(path: Path, *args, **kwargs):
            existed = path.exists()
            result = original(path, *args, **kwargs)
            if not existed and path.resolve().is_relative_to(root):
                materialize(path)
            return result

        return mock.patch.object(Path, "mkdir", mkdir)

    def add_process(self, pid: int, cgroup: str, *, children: tuple[int, ...] = ()) -> None:
        directory = self.proc / str(pid)
        (directory / "task" / str(pid)).mkdir(parents=True, exist_ok=True)
        (directory / "cgroup").write_text(f"0::{cgroup}\n")
        fields = ["S", *("0" for _ in range(18)), str(10_000 + pid)]
        (directory / "stat").write_text(f"{pid} (process-{pid}) " + " ".join(fields) + "\n")
        (directory / "task" / str(pid) / "children").write_text(
            " ".join(str(child) for child in children) + "\n"
        )

    def create_envelope(self) -> runtime.FixedEnvelope:
        envelope = runtime.FixedEnvelope(
            "perf-000000000000",
            self.root,
            proc_root=self.proc,
            docker_mode_provider=lambda: ("2", "cgroupfs"),
        )
        with mock.patch.object(runtime.platform, "system", return_value="Linux"), self.patch_mkdir():
            parent = envelope.create()
        self.assert_parent(parent)
        return envelope

    @staticmethod
    def assert_parent(parent: str) -> None:
        if parent != "moraine-performance/perf-000000000000":
            raise AssertionError(parent)

    def populate_placement(self, envelope: runtime.FixedEnvelope) -> None:
        assert envelope.path is not None
        clickhouse = envelope.path / "clickhouse"
        moraine = envelope.path / "moraine"
        loadgen = self.root / "loadgen"
        for group in (clickhouse, moraine, loadgen):
            group.mkdir()
            self.materialize(group)
        server_base = "/moraine-performance/perf-000000000000"
        self.add_process(100, f"{server_base}/clickhouse", children=(101,))
        self.add_process(101, f"{server_base}/clickhouse")
        self.add_process(200, f"{server_base}/moraine", children=(201,))
        self.add_process(201, f"{server_base}/moraine")
        self.add_process(300, "/loadgen")
        (clickhouse / "cgroup.procs").write_text("100\n101\n")
        (moraine / "cgroup.procs").write_text("200\n201\n")
        (loadgen / "cgroup.procs").write_text("300\n")
        (envelope.path / "pids.current").write_text("4\n")


class RuntimeCommandTests(unittest.TestCase):
    def test_command_output_is_drained_but_bounded(self) -> None:
        with mock.patch.object(runtime, "COMMAND_OUTPUT_LIMIT_BYTES", 1_024):
            with self.assertRaisesRegex(runtime.RuntimeFailure, "output exceeded"):
                runtime._run(
                    [sys.executable, "-c", "import sys; sys.stdout.write('x' * 4096)"],
                    timeout=5,
                )

    def test_interrupt_kills_child_group_before_joining_readers(self) -> None:
        process = mock.Mock(
            pid=123,
            stdout=io.BytesIO(),
            stderr=io.BytesIO(),
        )
        process.wait.side_effect = [KeyboardInterrupt, 0]
        with (
            mock.patch.object(runtime.subprocess, "Popen", return_value=process),
            mock.patch.object(runtime.os, "killpg") as killpg,
        ):
            with self.assertRaises(KeyboardInterrupt):
                runtime._run(["command"], timeout=5)
        killpg.assert_called_once_with(123, runtime.signal.SIGKILL)
        self.assertEqual(process.wait.call_count, 2)

    def test_failed_docker_build_force_removes_named_container(self) -> None:
        def completed(argv, code=0, stderr=""):
            return subprocess.CompletedProcess(argv, code, "", stderr)

        calls: list[list[str]] = []

        def run(argv, **_kwargs):
            command = list(argv)
            calls.append(command)
            if command[:2] == ["docker", "run"]:
                raise runtime.RuntimeFailure("injected build timeout")
            if command[:3] == ["docker", "container", "inspect"]:
                return completed(command, 1, f"Error: No such object: {command[-1]}")
            return completed(command)

        with tempfile.TemporaryDirectory() as temporary:
            with mock.patch.object(runtime, "_run", side_effect=run):
                with self.assertRaisesRegex(runtime.RuntimeFailure, "injected"):
                    runtime.build_release_binaries_in_docker(
                        REPO,
                        Path(temporary) / "frozen",
                        toolchain_file=REPO / "rust-toolchain.toml",
                    )
        docker_run = next(command for command in calls if command[:2] == ["docker", "run"])
        container_name = docker_run[docker_run.index("--name") + 1]
        self.assertIn(["docker", "rm", "-f", container_name], calls)
        self.assertIn(["docker", "container", "inspect", container_name], calls)

    def test_docker_cleanup_daemon_error_fails_closed(self) -> None:
        unavailable = subprocess.CompletedProcess(
            ["docker"], 1, "", "Cannot connect to the Docker daemon"
        )
        with mock.patch.object(runtime, "_run", return_value=unavailable):
            with self.assertRaisesRegex(runtime.RuntimeFailure, "cannot prove"):
                runtime._remove_owned_build_container("owned-build")


class FixedEnvelopeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.host = FakeCgroupHost(self.base)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_cgroupfs_driver_sets_exact_aggregate_limits_and_proofs(self) -> None:
        envelope = self.host.create_envelope()
        assert envelope.path is not None
        self.assertEqual((envelope.path / "cpu.max").read_text().strip(), "100000 100000")
        self.assertEqual((envelope.path / "memory.max").read_text().strip(), "8589934592")
        self.assertEqual((envelope.path / "memory.swap.max").read_text().strip(), "0")
        self.assertEqual(envelope.driver, "cgroupfs")
        self.assertTrue(envelope._controllers_proven)
        self.assertTrue(envelope._limits_proven)
        self.assertTrue(envelope._headroom_proven)

    def test_unsupported_version_and_absent_controller_fail_closed(self) -> None:
        unsupported = runtime.FixedEnvelope(
            "perf-000000000000",
            self.host.root,
            proc_root=self.host.proc,
            docker_mode_provider=lambda: ("1", "cgroupfs"),
        )
        with mock.patch.object(runtime.platform, "system", return_value="Linux"):
            with self.assertRaisesRegex(runtime.RuntimeFailure, "mode changed"):
                unsupported.create()
        (self.host.root / "cgroup.controllers").write_text("cpu cpuset\n")
        absent = runtime.FixedEnvelope(
            "perf-000000000001",
            self.host.root,
            proc_root=self.host.proc,
            docker_mode_provider=lambda: ("2", "cgroupfs"),
        )
        with mock.patch.object(runtime.platform, "system", return_value="Linux"):
            with self.assertRaisesRegex(runtime.RuntimeFailure, "controllers unavailable"):
                absent.create()

    def test_read_only_controller_write_fails_closed_without_foreign_removal(self) -> None:
        envelope = runtime.FixedEnvelope(
            "perf-000000000002",
            self.host.root,
            proc_root=self.host.proc,
            docker_mode_provider=lambda: ("2", "cgroupfs"),
        )
        (self.host.root / "cgroup.subtree_control").write_text("")
        with mock.patch.object(runtime.platform, "system", return_value="Linux"), mock.patch.object(
            runtime, "_write", side_effect=runtime.RuntimeFailure("read-only controller")
        ):
            with self.assertRaisesRegex(runtime.RuntimeFailure, "read-only"):
                envelope.create()
        self.assertTrue(self.host.family.exists(), "shared family cgroup must be preserved")

    def test_effective_ancestor_cpu_memory_and_cpuset_mismatches_fail(self) -> None:
        (self.host.family / "cpu.max").write_text("50000 100000\n")
        with self.assertRaisesRegex(runtime.RuntimeFailure, "below one core"):
            self.host.create_envelope()
        shutil.rmtree(self.host.family / "perf-000000000000")
        (self.host.family / "cpu.max").write_text("max 100000\n")
        (self.host.family / "memory.max").write_text(f"{runtime.MEMORY_MAX_BYTES}\n")
        with self.assertRaisesRegex(runtime.RuntimeFailure, "memory headroom"):
            self.host.create_envelope()
        shutil.rmtree(self.host.family / "perf-000000000000")
        (self.host.family / "memory.max").write_text("max\n")
        (self.host.family / "cpuset.cpus.effective").write_text("\n")
        with self.assertRaisesRegex(runtime.RuntimeFailure, "cpuset"):
            self.host.create_envelope()

    def test_systemd_driver_uses_controlgroup_readback_branch(self) -> None:
        unit = "moraine-performance-perf-000000000003.slice"
        system_slice = self.host.root / "system.slice"
        system_slice.mkdir()
        self.host.materialize(system_slice)
        group = system_slice / unit
        group.mkdir()
        self.host.materialize(group)
        calls: list[tuple[str, ...]] = []

        def command(argv, _timeout):
            calls.append(tuple(argv))
            stdout = f"/system.slice/{unit}\n" if argv[1] == "show" else ""
            return subprocess.CompletedProcess(argv, 0, stdout, "")

        envelope = runtime.FixedEnvelope(
            "perf-000000000003",
            self.host.root,
            proc_root=self.host.proc,
            docker_mode_provider=lambda: ("2", "systemd"),
            command_runner=command,
        )
        with mock.patch.object(runtime.platform, "system", return_value="Linux"):
            self.assertEqual(envelope.create(), unit)
        self.assertEqual(envelope.path, group)
        self.assertTrue(any(call[:2] == ("systemctl", "set-property") for call in calls))
        self.assertEqual((group / "cpu.max").read_text().strip(), "100000 100000")
        envelope.remove()
        self.assertTrue(any(call[:2] == ("systemctl", "stop") for call in calls))

    def test_failed_remove_retains_owned_cgroup_identity(self) -> None:
        envelope = self.host.create_envelope()
        assert envelope.path is not None
        with self.assertRaisesRegex(runtime.RuntimeFailure, "owned cgroup not empty"):
            envelope.remove()
        self.assertIsNotNone(envelope.path)
        self.assertEqual(envelope.parent_name, "moraine-performance/perf-000000000000")
        self.assertEqual(envelope.driver, "cgroupfs")

    def test_server_descendants_and_loadgen_exclusion_are_both_required(self) -> None:
        envelope = self.host.create_envelope()
        self.host.populate_placement(envelope)
        evidence = envelope.inspect([100, 200], [300])
        self.assertTrue(evidence.server_descendants_proven)
        self.assertTrue(evidence.loadgen_excluded_proven)
        artifact = evidence.artifact(evidence)
        self.assertTrue(artifact["controllers_enabled_proven"])
        self.assertTrue(artifact["effective_limits_proven"])
        self.assertTrue(artifact["host_headroom_proven"])
        self.assertRegex(artifact["role_membership_sha256"], r"^sha256:[0-9a-f]{64}$")

    def test_escaped_descendant_and_loadgen_in_parent_are_rejected(self) -> None:
        envelope = self.host.create_envelope()
        self.host.populate_placement(envelope)
        self.host.add_process(201, "/loadgen")
        escaped = envelope.inspect([100, 200], [300])
        self.assertFalse(escaped.server_descendants_proven)
        self.host.add_process(201, "/moraine-performance/perf-000000000000/moraine")
        self.host.add_process(300, "/moraine-performance/perf-000000000000/moraine")
        included = envelope.inspect([100, 200], [300])
        self.assertFalse(included.loadgen_excluded_proven)

    def test_reset_requires_nonempty_roles_and_rejects_preexisting_swap_or_oom(self) -> None:
        envelope = self.host.create_envelope()
        assert envelope.path is not None
        with self.assertRaisesRegex(runtime.RuntimeFailure, "placement"):
            envelope.reset_measurement([], [])
        self.host.populate_placement(envelope)
        (envelope.path / "memory.swap.current").write_text("1\n")
        with self.assertRaisesRegex(runtime.RuntimeFailure, "not clean"):
            envelope.reset_measurement([100, 200], [300])
        (envelope.path / "memory.swap.current").write_text("0\n")
        (envelope.path / "memory.events").write_text("oom 1\noom_kill 1\n")
        with self.assertRaisesRegex(runtime.RuntimeFailure, "not clean"):
            envelope.reset_measurement([100, 200], [300])

    def test_running_binary_hash_and_starttime_proof_detects_mismatch(self) -> None:
        envelope = self.host.create_envelope()
        assert envelope.path is not None
        binary_dir = self.base / "bin"
        binary_dir.mkdir()
        hashes = {}
        pids = (401, 402)
        for name, pid in zip(runtime.RUNNING_SERVER_BINARIES, pids):
            binary = binary_dir / name
            binary.write_bytes(f"{name}-release".encode())
            binary.chmod(0o555)
            hashes[name] = runtime.hash_file(binary)
            self.host.add_process(pid, "/moraine-performance/perf-000000000000")
            os.symlink(binary, self.host.proc / str(pid) / "exe")
        (envelope.path / "cgroup.procs").write_text("401\n402\n")
        proof = envelope.verify_running_binaries(hashes)
        self.assertEqual([item.name for item in proof], list(runtime.RUNNING_SERVER_BINARIES))
        bad = dict(hashes)
        bad["moraine-mcp"] = "sha256:" + "0" * 64
        with self.assertRaisesRegex(runtime.RuntimeFailure, "hash mismatch"):
            envelope.verify_running_binaries(bad)


class EvidenceAndBuildTests(unittest.TestCase):
    def test_aggregate_telemetry_deltas_and_oom_swap_gates(self) -> None:
        def evidence(*, usage, throttled, oom, swap):
            return runtime.CgroupEvidence(
                True, "cgroupfs", "parent", "path", "0-1",
                runtime.CPU_QUOTA_US, runtime.CPU_PERIOD_US,
                runtime.MEMORY_MAX_BYTES, 0, 1024, 2048, swap, 2,
                {"usage_usec": usage, "nr_throttled": throttled, "throttled_usec": throttled * 10},
                {"high": 0, "max": 1, "oom": oom, "oom_kill": oom},
                True, True, True, True, True,
                "sha256:" + "1" * 64,
            )

        before = evidence(usage=100, throttled=1, oom=0, swap=0)
        after = evidence(usage=1_100, throttled=3, oom=0, swap=0)
        artifact = after.artifact(before)
        self.assertEqual(artifact["cpu_usage_usec_delta"], 1000)
        self.assertEqual(artifact["cpu_nr_throttled_delta"], 2)
        self.assertEqual(artifact["memory_event_max_delta"], 0)
        after.assert_clean(before)
        with self.assertRaisesRegex(runtime.RuntimeFailure, "OOM"):
            evidence(usage=1_100, throttled=3, oom=1, swap=0).assert_clean(before)
        with self.assertRaisesRegex(runtime.RuntimeFailure, "swap"):
            evidence(usage=1_100, throttled=3, oom=0, swap=1).assert_clean(before)

    def test_busy_child_helper_proves_aggregate_not_per_service_limit(self) -> None:
        proof = runtime.evaluate_busy_child_proof(
            before_usage_usec=10,
            after_usage_usec=1_010_010,
            wall_seconds=1.0,
            child_progress=(100, 80),
        )
        self.assertTrue(proof.artifact()["both_children_progressed"])
        self.assertTrue(proof.artifact()["aggregate_one_cpu_proven"])
        with self.assertRaisesRegex(runtime.RuntimeFailure, "approximately one CPU"):
            runtime.evaluate_busy_child_proof(
                before_usage_usec=0,
                after_usage_usec=2_000_000,
                wall_seconds=1.0,
                child_progress=(1, 1),
            )
        with self.assertRaisesRegex(runtime.RuntimeFailure, "progress"):
            runtime.evaluate_busy_child_proof(
                before_usage_usec=0,
                after_usage_usec=1_000_000,
                wall_seconds=1.0,
                child_progress=(1, 0),
            )

    def test_release_freeze_is_read_only_and_manifest_covers_every_binary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "release"
            source.mkdir()
            for name in runtime.RELEASE_BINARIES:
                path = source / name
                path.write_bytes(name.encode())
                path.chmod(0o755)
            frozen = runtime.freeze_release_binaries(
                source,
                root / "frozen",
                target="x86_64-unknown-linux-gnu",
                rustc_release="1.88.0",
                toolchain_sha256="sha256:" + "2" * 64,
                build_environment={"RUSTFLAGS": "-Ctarget-cpu=x86-64", "SECRET": "not-recorded"},
            )
            self.assertEqual(set(frozen.binary_sha256), set(runtime.RELEASE_BINARIES))
            self.assertEqual(frozen.build_environment, {"RUSTFLAGS": "-Ctarget-cpu=x86-64"})
            self.assertEqual(stat.S_IMODE(frozen.directory.stat().st_mode), 0o555)
            self.assertEqual(stat.S_IMODE(frozen.manifest_path.stat().st_mode), 0o444)
            self.assertEqual(runtime.hash_file(frozen.manifest_path), frozen.manifest_sha256)
            manifest = json.loads(frozen.manifest_path.read_text())
            self.assertEqual(
                manifest["recipe"]["environment_allowlist"],
                ["RUSTFLAGS"],
            )
            self.assertNotIn("-Ctarget-cpu=x86-64", frozen.manifest_path.read_text())
            self.assertNotIn("environment", manifest["recipe"])

    def test_sandbox_ports_bind_only_to_host_loopback(self) -> None:
        compose = BASE_COMPOSE.read_text()
        self.assertIn(
            '"127.0.0.1:${SANDBOX_MONITOR_HOST_PORT}:8080"',
            compose,
        )
        self.assertIn(
            '"127.0.0.1:${SANDBOX_CLICKHOUSE_HTTP_HOST_PORT}:8123"',
            compose,
        )
        self.assertIn(
            '"127.0.0.1:${SANDBOX_CLICKHOUSE_TCP_HOST_PORT}:9000"',
            compose,
        )

    def test_build_uses_only_frozen_workspace_release_locked_recipe(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory) / "repo"
            release = repo / "target" / "release"
            release.mkdir(parents=True)
            (repo / "rust-toolchain.toml").write_text(
                '[toolchain]\nchannel = "1.96.0"\nprofile = "minimal"\n'
            )
            for name in runtime.RELEASE_BINARIES:
                binary = release / name
                binary.write_bytes(name.encode())
                binary.chmod(0o755)
            calls: list[tuple[str, ...]] = []

            def fake_run(argv, **_kwargs):
                calls.append(tuple(argv))
                if argv[0] == "rustc":
                    return subprocess.CompletedProcess(
                        argv,
                        0,
                        "rustc 1.96.0\nrelease: 1.96.0\nhost: x86_64-unknown-linux-gnu\n",
                        "",
                    )
                return subprocess.CompletedProcess(argv, 0, "", "")

            with mock.patch.object(runtime, "_run", side_effect=fake_run):
                build = runtime.build_release_binaries(repo, Path(directory) / "immutable")
            self.assertEqual(
                calls,
                [
                    ("rustc", "-vV"),
                    ("cargo", "build", "--workspace", "--release", "--locked"),
                ],
            )
            self.assertEqual(build.target, "x86_64-unknown-linux-gnu")
            self.assertEqual(build.rustc_release, "1.96.0")
            self.assertEqual(build.linker, "rustc-default")
            self.assertNotIn("linker", build.artifact()["recipe"])

    def test_sandbox_status_rejects_role_or_image_identity_mismatch(self) -> None:
        digest = "sha256:" + "a" * 64
        value = {
            "schema_version": runtime.SANDBOX_SCHEMA,
            "sandbox_id": "sb-000000",
            "project": "moraine-sandbox-sb-000000",
            "performance": True,
            "lifecycle_phase": "services-ready",
            "ports": {"monitor": 1234, "clickhouse_http": 1235},
            "config_path": "/tmp/config/moraine.toml",
            "cgroup_parent": "owned",
            "ownership_token_sha256": digest,
            "binary_manifest_sha256": digest,
            "binary_sha256": {name: digest for name in runtime.RELEASE_BINARIES},
            "containers": [
                {"role": role, "service": service, "container_id": service, "pid": index + 1, "image_id": digest, "running": True}
                for index, (role, service) in enumerate((
                    ("server_clickhouse", "clickhouse"),
                    ("server_moraine", "moraine"),
                    ("loadgen", "loadgen"),
                ))
            ],
        }
        runtime.SandboxStatus.parse(value)
        value["containers"][2]["role"] = "server_loadgen"
        with self.assertRaisesRegex(runtime.RuntimeFailure, "role classification"):
            runtime.SandboxStatus.parse(value)

    def test_performance_compose_excludes_dev_work_and_loadgen_from_parent(self) -> None:
        text = COMPOSE.read_text()
        moraine = text.split("  moraine:", 1)[1].split("  loadgen:", 1)[0]
        loadgen = text.split("  loadgen:", 1)[1]
        self.assertIn("build: !reset null", moraine)
        self.assertIn("healthcheck:\n      disable: true", moraine)
        self.assertNotIn("/repo", text)
        self.assertNotIn("cargo-home", text)
        self.assertIn("${SANDBOX_BIN_DIR:?immutable release binary directory required}:/opt/moraine/bin:ro", text)
        self.assertIn("${SANDBOX_WEB_DIR:?monitor static directory required}:/opt/moraine/web:ro", text)
        self.assertNotIn("cgroup_parent", loadgen)
        self.assertIn('["/bin/sleep", "infinity"]', loadgen)

    def test_minimal_entrypoint_has_no_build_tail_or_sentinel_and_tracks_generation(self) -> None:
        text = ENTRYPOINT.read_text()
        self.assertNotIn("cargo build", text)
        self.assertNotIn("tail -", text)
        self.assertNotIn("sentinel", text.lower())
        self.assertIn("cache_generation", text)
        self.assertIn("wait -n", text)
        self.assertIn("moraine-ingest", text)
        self.assertIn("moraine-mcp", text)
        migration = text.index('moraine" db migrate')
        self.assertLess(migration, text.index('"${BIN_DIR}/moraine-ingest"'))


class RuntimeAdapterTests(unittest.TestCase):
    @staticmethod
    def sandbox() -> runtime.OwnedSandbox:
        return runtime.OwnedSandbox(
            REPO,
            SANDBOX,
            "sb-000001",
            "moraine-sandbox-sb-000001",
            41000,
            42000,
            Path("/tmp/moraine-sandbox-sb-000001"),
            "sha256:" + "a" * 64,
            mock.sentinel.build,
        )

    def test_watched_source_and_typed_ack_cursor_are_content_free_and_gap_aware(self) -> None:
        sandbox = self.sandbox()
        digest = "b" * 64
        line = (
            "\x1b[2mINFO\x1b[0m "
            "\x1b[3mbatch_sequence\x1b[0m\x1b[2m=\x1b[0m7 "
            "\x1b[3mevent_identity_digests\x1b[0m\x1b[2m=\x1b[0m"
            f'\x1b[32m["{digest}"]\x1b[0m '
            "\x1b[3mack_monotonic_ns\x1b[0m\x1b[2m=\x1b[0m123456\n"
        )
        completed = subprocess.CompletedProcess([], 0, "", line)
        with mock.patch.object(runtime, "_run", return_value=completed):
            batch = sandbox.read_ingest_ack_logs()
            exhausted = sandbox.read_ingest_ack_logs(batch.next_cursor)
            gap = sandbox.read_ingest_ack_logs(batch.next_cursor + 1)
        self.assertEqual(
            sandbox.watched_source_dir,
            sandbox.config_dir / "fixtures" / "codex" / "sessions",
        )
        self.assertEqual(batch.next_cursor, 1)
        self.assertEqual(
            batch.observations,
            (runtime.AckObservation(7, (digest,), 123456),),
        )
        self.assertFalse(batch.gap_detected)
        self.assertEqual(exhausted.observations, ())
        self.assertFalse(exhausted.gap_detected)
        self.assertTrue(gap.gap_detected)

    def test_ack_reader_fails_closed_on_partial_or_non_digest_observation(self) -> None:
        sandbox = self.sandbox()
        for line in (
            "batch_sequence=1 ack_monotonic_ns=2\n",
            'batch_sequence=1 event_identity_digests=["raw-content"] ack_monotonic_ns=2\n',
        ):
            with self.subTest(line=line), mock.patch.object(
                runtime,
                "_run",
                return_value=subprocess.CompletedProcess([], 0, line, ""),
            ), self.assertRaises(runtime.RuntimeFailure):
                sandbox.read_ingest_ack_logs()

    def test_ingest_status_and_fresh_heartbeat_drain_proof(self) -> None:
        sandbox = self.sandbox()
        payload = json.dumps(
            {
                "ok": True,
                "ingestor": {
                    "latest": {
                        "files_watched": 3,
                        "queue_depth": 2,
                        "files_active": 1,
                        "ts_unix_ms": 100,
                        "last_error": None,
                    }
                },
            }
        ).encode()

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self, _limit):
                return payload

        with mock.patch.object(runtime.urllib.request, "urlopen", return_value=Response()):
            status = sandbox.ingest_status()
        self.assertEqual(status, runtime.WatcherReadiness(3, 2, 1, 100))

        drained = runtime.WatcherReadiness(3, 0, 0, 101)
        with mock.patch.object(
            sandbox,
            "_read_ingest_status",
            side_effect=(status, status, drained),
        ), mock.patch.object(runtime.time, "sleep"):
            self.assertEqual(sandbox.wait_ingest_drained(timeout_s=1), drained)

    def test_watcher_readiness_never_issues_a_search(self) -> None:
        sandbox = self.sandbox()
        ready = runtime.WatcherReadiness(1, 0, 0, 10)
        with mock.patch.object(
            sandbox,
            "_read_ingest_status",
            side_effect=(None, ready),
        ), mock.patch.object(runtime.time, "sleep"):
            self.assertEqual(sandbox.wait_watcher_ready(timeout_s=1), ready)


class SandboxOwnershipShellTests(unittest.TestCase):
    def run_shell(self, body: str, *, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
        merged = dict(os.environ)
        if env:
            merged.update(env)
        return subprocess.run(
            ["bash", "-c", f'source "{SANDBOX}"\n{body}'],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=merged,
            timeout=20,
        )

    def test_failpoint_runs_owned_idempotent_cleanup_and_preserves_sibling(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "config"
            config.mkdir(mode=0o700)
            (config / ".ownership-token").write_text("a" * 64 + "\n")
            (config / "moraine.toml").write_text("owned\n")
            sibling = root / "foreign-sentinel"
            sibling.write_text("preserve-byte-for-byte")
            calls = root / "calls"
            result = self.run_shell(
                f'''
+verify_project_ownership() {{ return 0; }}
+dc() {{ printf 'down\n' >>"{calls}"; return 0; }}
+compose_resources_removed() {{ return 0; }}
+release_project_ownership() {{ printf 'release\n' >>"{calls}"; return 0; }}
+arm_failed_up_cleanup project "{config}" "{'a' * 64}" 0
+export MORAINE_SANDBOX_FAILPOINT=after-config
+maybe_failpoint after-config
+'''.replace("+", "")
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(config.exists())
            self.assertEqual(calls.read_text().splitlines(), ["down", "release"])
            self.assertEqual(sibling.read_text(), "preserve-byte-for-byte")

    def test_signal_uses_same_cleanup_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "config"
            config.mkdir(mode=0o700)
            (config / ".ownership-token").write_text("b" * 64 + "\n")
            calls = root / "calls"
            result = self.run_shell(
                f'''
+verify_project_ownership() {{ return 0; }}
+dc() {{ printf 'down\n' >>"{calls}"; return 0; }}
+compose_resources_removed() {{ return 0; }}
+release_project_ownership() {{ printf 'release\n' >>"{calls}"; return 0; }}
+arm_failed_up_cleanup project "{config}" "{'b' * 64}" 0
+kill -TERM $$
+'''.replace("+", "")
            )
            self.assertEqual(result.returncode, 143)
            self.assertFalse(config.exists())
            self.assertEqual(calls.read_text().splitlines(), ["down", "release"])

    def test_ownership_mismatch_never_calls_destructive_compose(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "config"
            config.mkdir(mode=0o700)
            (config / ".ownership-token").write_text("c" * 64 + "\n")
            destructive = root / "destructive"
            leak = root / "leak"
            result = self.run_shell(
                f'''
+verify_project_ownership() {{ return 1; }}
+dc() {{ touch "{destructive}"; return 0; }}
+write_owned_leak_inventory() {{ touch "{leak}"; }}
+arm_failed_up_cleanup project "{config}" "{'c' * 64}" 0
+exit 17
+'''.replace("+", "")
            )
            self.assertEqual(result.returncode, 17)
            self.assertFalse(destructive.exists())
            self.assertTrue(leak.exists())
            self.assertTrue(config.exists(), "ownership evidence must remain for retry")

    def test_already_removed_sandbox_cleanup_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_shell(
                "project_has_resources() { return 1; }\n"
                "down_one sb-000001\n"
                "down_one sb-000001\n",
                env={"MORAINE_SANDBOX_STATE_ROOT": directory},
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stderr.count("already down"), 2)

    def test_machine_status_is_strict_owned_role_classification(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sandbox_id = "sb-abcdef"
            config = root / f"moraine-sandbox-{sandbox_id}"
            config.mkdir(mode=0o700)
            token = "d" * 64
            (config / ".ownership-token").write_text(token + "\n")
            (config / ".performance-mode").write_text("1\n")
            (config / ".cgroup-parent").write_text("moraine-performance/perf-000000000000\n")
            (config / ".lifecycle-phase").write_text("services-ready\n")
            (config / "moraine.toml").write_text("[clickhouse]\n")
            digest = "sha256:" + "a" * 64
            manifest = {
                "schema_version": runtime.BUILD_SCHEMA,
                "binaries": {
                    name: {"sha256": digest, "size_bytes": 1}
                    for name in runtime.RELEASE_BINARIES
                },
            }
            raw = (json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n").encode()
            (config / ".binary-manifest.json").write_bytes(raw)
            (config / ".binary-manifest.sha256").write_text(
                "sha256:" + hashlib.sha256(raw).hexdigest() + "\n"
            )
            inspected = []
            for index, service in enumerate(("clickhouse", "moraine", "loadgen"), 1):
                inspected.append({
                    "Id": f"{service}-id",
                    "Image": digest,
                    "Config": {"Labels": {"com.docker.compose.service": service}},
                    "State": {"Pid": index, "Running": True},
                })
            inspect_path = root / "inspect.json"
            inspect_path.write_text(json.dumps(inspected))
            result = self.run_shell(
                f'''
+verify_project_ownership() {{ return 0; }}
+container_id_for_service() {{ printf '%s-id' "$2"; }}
+host_port_for() {{ if [[ "$2" == moraine ]]; then printf 41000; else printf 42000; fi; }}
+docker() {{ [[ "$1" == inspect ]] || return 90; cat "{inspect_path}"; }}
+print_machine_status {sandbox_id}
+'''.replace("+", ""),
                env={"MORAINE_SANDBOX_STATE_ROOT": directory},
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            status = json.loads(result.stdout)
            self.assertEqual(status["schema_version"], runtime.SANDBOX_SCHEMA)
            self.assertEqual(
                {item["role"] for item in status["containers"]},
                {"server_clickhouse", "server_moraine", "loadgen"},
            )
            self.assertEqual(
                status["ownership_token_sha256"],
                "sha256:" + hashlib.sha256(token.encode()).hexdigest(),
            )
            self.assertEqual(status["binary_sha256"], {name: digest for name in runtime.RELEASE_BINARIES})

    def test_checkpoint_transitions_fail_closed_on_backward_phase(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            phase = Path(directory) / "phase"
            phase.write_text("reserved\n")
            valid = self.run_shell(
                f'''
+check_docker() {{ :; }}
+require_owned_performance() {{ PERF_CONFIG_DIR="{directory}"; }}
+mv "{phase}" "{directory}/.lifecycle-phase"
+cmd_checkpoint sb-abcdef services-ready
+cmd_checkpoint sb-abcdef seeded
+cmd_checkpoint sb-abcdef artifact-created
+'''.replace("+", "")
            )
            self.assertEqual(valid.returncode, 0, valid.stderr)
            self.assertEqual((Path(directory) / ".lifecycle-phase").read_text(), "artifact-created\n")
            invalid = self.run_shell(
                f'''
+check_docker() {{ :; }}
+require_owned_performance() {{ PERF_CONFIG_DIR="{directory}"; }}
+cmd_checkpoint sb-abcdef seeded
+'''.replace("+", "")
            )
            self.assertNotEqual(invalid.returncode, 0)
            self.assertIn("invalid lifecycle transition", invalid.stderr)

    def test_unknown_failpoint_is_rejected_before_ownership(self) -> None:
        result = subprocess.run(
            [str(SANDBOX), "up", "--id", "sb-000001"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={**os.environ, "MORAINE_SANDBOX_FAILPOINT": "unknown"},
            timeout=10,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unknown MORAINE_SANDBOX_FAILPOINT", result.stderr)


if __name__ == "__main__":
    unittest.main()
