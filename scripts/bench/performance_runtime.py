#!/usr/bin/env python3
"""Authoritative build, sandbox, cgroup-v2, and process evidence.

This module is import-only.  ``performance_suite.py`` is the sole benchmark
command line entrypoint; the sandbox script remains the sole Docker lifecycle
owner.
"""
from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import tomllib
import time
import tempfile
import uuid
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

CPU_QUOTA_US = 100_000
CPU_PERIOD_US = 100_000
MEMORY_MAX_BYTES = 8 * 1024**3
SWAP_MAX_BYTES = 0
CGROUP_VERSION = 2
BUILD_SCHEMA = "moraine-performance-build-v1"
SANDBOX_SCHEMA = "moraine-sandbox-lifecycle-v1"
CENTRAL_STATUS_SCHEMA = "moraine-performance-central-v1"
OWNED_ID_RE = re.compile(r"^perf-[0-9a-f]{12}$")
SANDBOX_ID_RE = re.compile(r"^sb-[0-9a-f]{6}$")
SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
RELEASE_BINARIES = ("moraine", "moraine-ingest", "moraine-monitor", "moraine-mcp")
RUNNING_SERVER_BINARIES = ("moraine-ingest", "moraine-mcp")
BUILD_ENV_ALLOWLIST = (
    "CARGO_BUILD_TARGET",
    "CC",
    "CXX",
    "RUSTFLAGS",
    "RUSTC_WRAPPER",
    "MACOSX_DEPLOYMENT_TARGET",
    "CARGO_ENCODED_RUSTFLAGS",
)
CHECKPOINTS = frozenset({"services-ready", "seeded", "artifact-created"})


class RuntimeFailure(RuntimeError):
    """The requested run cannot produce authoritative evidence."""


def _sha256_bytes(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
    except OSError as exc:
        raise RuntimeFailure(f"cannot hash {path.name}: {exc}") from exc
    return "sha256:" + digest.hexdigest()


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return _sha256_bytes(encoded)


def _run(
    argv: Sequence[str],
    *,
    cwd: Optional[Path] = None,
    env: Optional[Mapping[str, str]] = None,
    timeout: float = 120.0,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            list(argv),
            cwd=cwd,
            env=dict(env) if env is not None else None,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeFailure(f"command failed to execute: {argv[0]}: {exc}") from exc

def _allowed_build_environment(environment: Mapping[str, str], target: str) -> dict[str, str]:
    normalized_target = re.sub(r"[^A-Za-z0-9]", "_", target).upper()
    target_keys = (
        f"CARGO_TARGET_{normalized_target}_LINKER",
        f"CARGO_TARGET_{normalized_target}_RUSTFLAGS",
    )
    return {
        key: environment[key]
        for key in (*BUILD_ENV_ALLOWLIST, *target_keys)
        if key in environment
    }


def _recorded_linker(environment: Mapping[str, str], target: str) -> str:
    normalized_target = re.sub(r"[^A-Za-z0-9]", "_", target).upper()
    return environment.get(
        f"CARGO_TARGET_{normalized_target}_LINKER",
        environment.get("CC", "rustc-default"),
    )



@dataclass(frozen=True)
class BuildIdentity:
    """Frozen release binary directory and reproducibility evidence."""

    directory: Path
    binary_sha256: Mapping[str, str]
    binary_size_bytes: Mapping[str, int]
    target: str
    rustc_release: str
    toolchain_sha256: str
    build_environment: Mapping[str, str]
    manifest_sha256: str

    @property
    def manifest_path(self) -> Path:
        return self.directory / "manifest.json"
    @property
    def linker(self) -> str:
        return _recorded_linker(self.build_environment, self.target)


    def artifact(self) -> dict[str, Any]:
        return {
            "schema_version": BUILD_SCHEMA,
            "recipe": {
                "command": ["cargo", "build", "--workspace", "--release", "--locked"],
                "default_features": True,
                "target": self.target,
                "rustc_release": self.rustc_release,
                "toolchain_sha256": self.toolchain_sha256,
                "linker_sha256": _canonical_sha256(self.linker),
                "environment_allowlist": sorted(self.build_environment),
                "build_environment_sha256": _canonical_sha256(
                    dict(sorted(self.build_environment.items()))
                ),
            },
            "binaries": {
                name: {
                    "sha256": self.binary_sha256[name],
                    "size_bytes": self.binary_size_bytes[name],
                }
                for name in RELEASE_BINARIES
            },
        }


def _validate_source_binary(path: Path) -> None:
    try:
        status = path.lstat()
    except OSError as exc:
        raise RuntimeFailure(f"release binary missing: {path.name}: {exc}") from exc
    if path.is_symlink() or not path.is_file() or status.st_nlink != 1:
        raise RuntimeFailure(f"release binary must be a single regular file: {path.name}")
    if not os.access(path, os.X_OK):
        raise RuntimeFailure(f"release binary is not executable: {path.name}")


def freeze_release_binaries(
    source_directory: Path,
    destination: Path,
    *,
    target: str,
    rustc_release: str,
    toolchain_sha256: str,
    build_environment: Mapping[str, str],
) -> BuildIdentity:
    """Copy one completed release build into a new read-only bind source."""

    if destination.exists() or destination.is_symlink():
        raise RuntimeFailure(f"immutable binary destination already exists: {destination}")
    destination.mkdir(parents=True, mode=0o700)
    hashes: dict[str, str] = {}
    sizes: dict[str, int] = {}
    try:
        for name in RELEASE_BINARIES:
            source = source_directory / name
            _validate_source_binary(source)
            target_path = destination / name
            shutil.copyfile(source, target_path)
            with target_path.open("rb") as stream:
                os.fsync(stream.fileno())
            target_path.chmod(0o555)
            hashes[name] = hash_file(target_path)
            sizes[name] = target_path.stat().st_size
        draft = BuildIdentity(
            destination,
            hashes,
            sizes,
            target,
            rustc_release,
            toolchain_sha256,
            _allowed_build_environment(build_environment, target),
            "",
        )
        manifest_bytes = (json.dumps(draft.artifact(), sort_keys=True, separators=(",", ":")) + "\n").encode()
        manifest_path = destination / "manifest.json"
        descriptor, temporary_name = tempfile.mkstemp(
            dir=destination,
            prefix=".manifest.",
            suffix=".tmp",
        )
        temporary_path = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(manifest_bytes)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary_path, manifest_path)
        finally:
            try:
                temporary_path.unlink()
            except FileNotFoundError:
                pass
        manifest_path.chmod(0o444)
        manifest_hash = _sha256_bytes(manifest_bytes)
        directory_fd = os.open(destination, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        destination.chmod(0o555)
        return BuildIdentity(
            destination,
            hashes,
            sizes,
            target,
            rustc_release,
            toolchain_sha256,
            draft.build_environment,
            manifest_hash,
        )
    except BaseException:
        try:
            destination.chmod(0o700)
            for child in destination.iterdir():
                child.chmod(0o600)
                child.unlink()
            destination.rmdir()
        except OSError:
            pass
        raise


def build_release_binaries(
    repo_root: Path,
    destination: Path,
    *,
    environment: Optional[Mapping[str, str]] = None,
    toolchain_file: Optional[Path] = None,
) -> BuildIdentity:
    """Build the frozen workspace recipe, then freeze all release binaries."""

    toolchain = toolchain_file or (repo_root / "rust-toolchain.toml")
    if not toolchain.is_file():
        raise RuntimeFailure("suite rust-toolchain.toml is required")
    try:
        toolchain_document = tomllib.loads(toolchain.read_text(encoding="utf-8"))
        channel = toolchain_document["toolchain"]["channel"]
    except (OSError, tomllib.TOMLDecodeError, KeyError, TypeError) as error:
        raise RuntimeFailure("suite rust-toolchain.toml is invalid") from error
    if not isinstance(channel, str) or not channel:
        raise RuntimeFailure("suite Rust toolchain channel is invalid")
    build_env = dict(os.environ)
    if environment is not None:
        build_env.update(environment)
    build_env["RUSTUP_TOOLCHAIN"] = channel
    proc = _run(["rustc", "-vV"], cwd=repo_root, env=build_env, timeout=60)
    if proc.returncode:
        raise RuntimeFailure(f"rustc identity failed: {proc.stderr.strip()}")
    fields = dict(
        line.split(": ", 1)
        for line in proc.stdout.splitlines()
        if ": " in line
    )
    target = build_env.get("CARGO_BUILD_TARGET", fields.get("host", ""))
    rustc_release = fields.get("release", "")
    if not target or not rustc_release:
        raise RuntimeFailure("rustc -vV omitted release or target identity")
    proc = _run(
        ["cargo", "build", "--workspace", "--release", "--locked"],
        cwd=repo_root,
        env=build_env,
        timeout=3600,
    )
    if proc.returncode:
        raise RuntimeFailure(f"release build failed: {proc.stderr[-4096:].strip()}")
    target_root = Path(build_env.get("CARGO_TARGET_DIR", str(repo_root / "target")))
    source = target_root / (target if "CARGO_BUILD_TARGET" in build_env else "") / "release"
    return freeze_release_binaries(
        source,
        destination,
        target=target,
        rustc_release=rustc_release,
        toolchain_sha256=hash_file(toolchain),
        build_environment=build_env,
    )


def run_id() -> str:
    return "perf-" + uuid.uuid4().hex[:12]


def _read(path: Path) -> str:
    try:
        return path.read_text().strip()
    except OSError as exc:
        raise RuntimeFailure(f"cannot read cgroup evidence {path.name}: {exc}") from exc


def _write(path: Path, value: str) -> None:
    try:
        path.write_text(value + "\n")
    except OSError as exc:
        raise RuntimeFailure(f"cannot set cgroup control {path.name}: {exc}") from exc


def _parse_cpu_max(text: str) -> tuple[Optional[int], int]:
    fields = text.split()
    if len(fields) != 2:
        raise RuntimeFailure("cpu.max must contain quota and period")
    try:
        quota = None if fields[0] == "max" else int(fields[0])
        period = int(fields[1])
    except ValueError as exc:
        raise RuntimeFailure("cpu.max contains invalid integers") from exc
    if (quota is not None and quota <= 0) or period <= 0:
        raise RuntimeFailure("cpu.max values must be positive")
    return quota, period


def _finite_int(text: str, field: str) -> int:
    if text == "max":
        raise RuntimeFailure(f"{field} must be finite")
    try:
        value = int(text)
    except ValueError as exc:
        raise RuntimeFailure(f"{field} is not an integer") from exc
    if value < 0:
        raise RuntimeFailure(f"{field} is negative")
    return value


def _kv(text: str) -> dict[str, int]:
    result: dict[str, int] = {}
    for line in text.splitlines():
        fields = line.split()
        if len(fields) == 2:
            try:
                result[fields[0]] = int(fields[1])
            except ValueError:
                continue
    return result


def _cpuset_values(text: str) -> tuple[int, ...]:
    values: set[int] = set()
    try:
        for part in text.split(","):
            if not part:
                continue
            bounds = part.split("-", 1)
            start = int(bounds[0])
            end = int(bounds[-1])
            if start < 0 or end < start:
                raise ValueError
            values.update(range(start, end + 1))
    except ValueError as exc:
        raise RuntimeFailure("cpuset.cpus.effective is malformed") from exc
    if not values:
        raise RuntimeFailure("cpuset.cpus.effective is empty")
    return tuple(sorted(values))


def _normalized_cpuset(text: str) -> str:
    values = _cpuset_values(text)
    runs: list[str] = []
    start = previous = values[0]
    for value in values[1:]:
        if value == previous + 1:
            previous = value
            continue
        runs.append(str(start) if start == previous else f"{start}-{previous}")
        start = previous = value
    runs.append(str(start) if start == previous else f"{start}-{previous}")
    return ",".join(runs)


def _meminfo_bytes(proc_root: Path) -> tuple[int, int]:
    values: dict[str, int] = {}
    for line in _read(proc_root / "meminfo").splitlines():
        match = re.fullmatch(r"(MemTotal|MemAvailable):\s+(\d+)\s+kB", line)
        if match:
            values[match.group(1)] = int(match.group(2)) * 1024
    if set(values) != {"MemTotal", "MemAvailable"}:
        raise RuntimeFailure("/proc/meminfo omitted MemTotal or MemAvailable")
    return values["MemTotal"], values["MemAvailable"]


def docker_cgroup_driver() -> tuple[str, str]:
    proc = _run(["docker", "info", "--format", "{{json .}}"], timeout=30)
    if proc.returncode:
        raise RuntimeFailure(f"docker info failed: {proc.stderr.strip()}")
    try:
        info = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeFailure("docker info did not return JSON") from exc
    version = str(info.get("CgroupVersion", ""))
    driver = str(info.get("CgroupDriver", ""))
    security = {str(item).lower() for item in info.get("SecurityOptions", [])}
    if version != "2" or driver not in {"cgroupfs", "systemd"}:
        raise RuntimeFailure(f"unsupported Docker cgroup mode version={version!r} driver={driver!r}")
    if any("rootless" in item for item in security):
        raise RuntimeFailure("authoritative performance runs require rootful Docker")
    return version, driver


@dataclass(frozen=True)
class BinaryProcessEvidence:
    name: str
    pid: int
    starttime: int
    exe_sha256: str

def process_starttime(pid: int, *, proc_root: Path = Path("/proc")) -> int:
    text = _read(proc_root / str(pid) / "stat")
    closing = text.rfind(")")
    fields = text[closing + 2 :].split()
    if closing < 0 or len(fields) < 20:
        raise RuntimeFailure(f"cannot read starttime for pid {pid}")
    try:
        return int(fields[19])
    except ValueError as exc:
        raise RuntimeFailure(f"invalid starttime for pid {pid}") from exc


def verify_process_binary(
    pid: int,
    name: str,
    expected_sha256: str,
    *,
    proc_root: Path = Path("/proc"),
) -> BinaryProcessEvidence:
    """Hash the live /proc executable, including an external loadgen route."""
    exe = proc_root / str(pid) / "exe"
    try:
        actual_name = Path(os.readlink(exe)).name.removesuffix(" (deleted)")
    except OSError as exc:
        raise RuntimeFailure(f"cannot resolve running executable for pid {pid}: {exc}") from exc
    if actual_name != name:
        raise RuntimeFailure(f"running executable role mismatch: expected {name}, found {actual_name}")
    actual_hash = hash_file(exe)
    if actual_hash != expected_sha256:
        raise RuntimeFailure(f"running binary hash mismatch for {name}")
    return BinaryProcessEvidence(name, pid, process_starttime(pid, proc_root=proc_root), actual_hash)


@dataclass(frozen=True)
class CgroupEvidence:
    authoritative: bool
    driver: str
    parent_name: str
    parent_path: str
    cpuset_cpus_effective: str
    cpu_quota_us: int
    cpu_period_us: int
    memory_max_bytes: int
    swap_max_bytes: int
    memory_current_bytes: int
    memory_peak_bytes: int
    swap_current_bytes: int
    pids_current: int
    cpu_stat: Mapping[str, int]
    memory_events: Mapping[str, int]
    server_descendants_proven: bool
    loadgen_excluded_proven: bool
    controllers_enabled_proven: bool
    effective_limits_proven: bool
    host_headroom_proven: bool
    role_membership_sha256: str

    def _delta(self, current: Mapping[str, int], before: Optional[Mapping[str, int]], key: str) -> int:
        value = current.get(key, 0) - (before.get(key, 0) if before is not None else 0)
        if value < 0:
            raise RuntimeFailure(f"cgroup counter {key} moved backwards")
        return value

    def artifact(self, before: "CgroupEvidence | None" = None) -> dict[str, Any]:
        prior_cpu = before.cpu_stat if before is not None else None
        prior_memory = before.memory_events if before is not None else None
        return {
            "authoritative": self.authoritative,
            "cgroup_version": CGROUP_VERSION,
            "cgroup_driver": self.driver,
            "cgroup_identity_sha256": _canonical_sha256([self.driver, self.parent_name, self.parent_path]),
            "role_membership_sha256": self.role_membership_sha256,
            "controllers_enabled_proven": self.controllers_enabled_proven,
            "effective_limits_proven": self.effective_limits_proven,
            "host_headroom_proven": self.host_headroom_proven,
            "cpuset_cpus_effective": self.cpuset_cpus_effective,
            "cpu_max_quota_us": self.cpu_quota_us,
            "cpu_max_period_us": self.cpu_period_us,
            "cpu_usage_usec_delta": self._delta(self.cpu_stat, prior_cpu, "usage_usec"),
            "cpu_nr_throttled_delta": self._delta(self.cpu_stat, prior_cpu, "nr_throttled"),
            "throttled_usec_delta": self._delta(self.cpu_stat, prior_cpu, "throttled_usec"),
            "memory_max_bytes": self.memory_max_bytes,
            "swap_max_bytes": self.swap_max_bytes,
            "memory_current_bytes": self.memory_current_bytes,
            "memory_peak_bytes": self.memory_peak_bytes,
            "swap_current_bytes": self.swap_current_bytes,
            "memory_event_high_delta": self._delta(self.memory_events, prior_memory, "high"),
            "memory_event_max_delta": self._delta(self.memory_events, prior_memory, "max"),
            "oom_kill_delta": self._delta(self.memory_events, prior_memory, "oom_kill"),
            "server_descendants_proven": self.server_descendants_proven,
            "loadgen_excluded_proven": self.loadgen_excluded_proven,
        }

    def telemetry(self, before: "CgroupEvidence | None" = None) -> dict[str, Any]:
        prior_cpu = before.cpu_stat if before is not None else {}
        prior_memory = before.memory_events if before is not None else {}
        return {
            "cpu_stat_delta": {
                key: self._delta(self.cpu_stat, prior_cpu, key)
                for key in sorted(set(self.cpu_stat) | set(prior_cpu))
            },
            "memory_events_delta": {
                key: self._delta(self.memory_events, prior_memory, key)
                for key in sorted(set(self.memory_events) | set(prior_memory))
            },
            "memory_current_bytes": self.memory_current_bytes,
            "memory_peak_bytes": self.memory_peak_bytes,
            "swap_current_bytes": self.swap_current_bytes,
            "pids_current": self.pids_current,
        }

    def assert_clean(self, before: "CgroupEvidence") -> None:
        artifact = self.artifact(before)
        if artifact["oom_kill_delta"] or self._delta(self.memory_events, before.memory_events, "oom"):
            raise RuntimeFailure("server envelope recorded an OOM event")
        if self.swap_current_bytes != 0:
            raise RuntimeFailure("server envelope used swap")
        if not all(
            artifact[key]
            for key in (
                "controllers_enabled_proven",
                "effective_limits_proven",
                "host_headroom_proven",
                "server_descendants_proven",
                "loadgen_excluded_proven",
            )
        ):
            raise RuntimeFailure("server envelope proof became incomplete")


def non_authoritative_resource_evidence() -> dict[str, Any]:
    """Syntactically complete smoke evidence which cannot pass comparison."""

    empty = CgroupEvidence(
        False,
        "cgroupfs",
        "non-authoritative",
        "non-authoritative",
        "0",
        CPU_QUOTA_US,
        CPU_PERIOD_US,
        MEMORY_MAX_BYTES,
        SWAP_MAX_BYTES,
        0,
        0,
        0,
        0,
        {},
        {},
        False,
        False,
        False,
        False,
        False,
        _canonical_sha256([]),
    )
    return empty.artifact(empty)


CommandRunner = Callable[[Sequence[str], float], subprocess.CompletedProcess[str]]


class FixedEnvelope:
    """Own and prove one aggregate server cgroup and no other resources."""

    def __init__(
        self,
        owned_id: str,
        root: Path = Path("/sys/fs/cgroup"),
        *,
        proc_root: Path = Path("/proc"),
        docker_mode_provider: Callable[[], tuple[str, str]] = docker_cgroup_driver,
        command_runner: Optional[CommandRunner] = None,
    ) -> None:
        if not OWNED_ID_RE.fullmatch(owned_id):
            raise RuntimeFailure("invalid performance run id")
        self.owned_id = owned_id
        self.root = root
        self.proc_root = proc_root
        self._docker_mode_provider = docker_mode_provider
        self._command_runner = command_runner or self._default_command_runner
        self.driver: Optional[str] = None
        self.parent_name: Optional[str] = None
        self.path: Optional[Path] = None
        self._systemd_started = False
        self._controllers_proven = False
        self._limits_proven = False
        self._headroom_proven = False

    @staticmethod
    def _default_command_runner(argv: Sequence[str], timeout: float) -> subprocess.CompletedProcess[str]:
        return _run(argv, timeout=timeout)

    def _command(self, argv: Sequence[str], timeout: float = 30.0) -> subprocess.CompletedProcess[str]:
        return self._command_runner(argv, timeout)

    def _require_host(self) -> None:
        if platform.system() != "Linux":
            raise RuntimeFailure("authoritative performance runs require Linux")
        if not (self.root / "cgroup.controllers").is_file():
            raise RuntimeFailure("authoritative performance runs require cgroup v2")
        total, available = _meminfo_bytes(self.proc_root)
        if total <= MEMORY_MAX_BYTES or available <= MEMORY_MAX_BYTES:
            raise RuntimeFailure("host lacks memory headroom outside the 8-GiB server envelope")
        root_controllers = set(_read(self.root / "cgroup.controllers").split())
        required = {"cpu", "memory", "cpuset"}
        if not required.issubset(root_controllers):
            missing = ",".join(sorted(required - root_controllers))
            raise RuntimeFailure(f"required cgroup controllers unavailable: {missing}")
        _normalized_cpuset(_read(self.root / "cpuset.cpus.effective"))
        self._headroom_proven = True

    def _enable_controllers(self, group: Path) -> None:
        required = {"cpu", "memory", "cpuset"}
        available = set(_read(group / "cgroup.controllers").split())
        if not required.issubset(available):
            raise RuntimeFailure(f"controllers not delegated at {group.name}")
        subtree = group / "cgroup.subtree_control"
        enabled = {value.lstrip("+") for value in _read(subtree).split()}
        missing = sorted(required - enabled)
        if missing:
            _write(subtree, " ".join(f"+{name}" for name in missing))
        enabled = {value.lstrip("+") for value in _read(subtree).split()}
        if not required.issubset(enabled):
            raise RuntimeFailure(f"controllers remained disabled at {group.name}")

    def _ancestors(self) -> list[Path]:
        if self.path is None:
            raise RuntimeFailure("fixed envelope has not been created")
        current = self.path.parent
        ancestors: list[Path] = []
        root = self.root.resolve()
        while True:
            resolved = current.resolve()
            if not resolved.is_relative_to(root):
                raise RuntimeFailure("cgroup parent escaped cgroup root")
            ancestors.append(current)
            if resolved == root:
                return ancestors
            current = current.parent

    def _prove_effective_ancestors(self) -> str:
        if self.path is None:
            raise RuntimeFailure("fixed envelope has not been created")
        cpuset = _normalized_cpuset(_read(self.path / "cpuset.cpus.effective"))
        for ancestor in self._ancestors():
            cpuset = _normalized_cpuset(_read(ancestor / "cpuset.cpus.effective"))
            cpu_file = ancestor / "cpu.max"
            if cpu_file.exists():
                quota, period = _parse_cpu_max(_read(cpu_file))
                if quota is not None and quota < period:
                    raise RuntimeFailure(f"ancestor {ancestor.name} limits CPU below one core")
            memory_file = ancestor / "memory.max"
            if memory_file.exists():
                text = _read(memory_file)
                if text != "max":
                    limit = _finite_int(text, "ancestor memory.max")
                    current = _finite_int(_read(ancestor / "memory.current"), "ancestor memory.current")
                    if limit <= MEMORY_MAX_BYTES or limit - min(current, limit) <= MEMORY_MAX_BYTES:
                        raise RuntimeFailure(f"ancestor {ancestor.name} lacks memory headroom")
        self._limits_proven = True
        return _normalized_cpuset(_read(self.path / "cpuset.cpus.effective"))

    def create(self) -> str:
        self._require_host()
        version, self.driver = self._docker_mode_provider()
        if version != "2" or self.driver not in {"cgroupfs", "systemd"}:
            raise RuntimeFailure("Docker cgroup mode changed during preflight")
        try:
            if self.driver == "cgroupfs":
                self._enable_controllers(self.root)
                family = self.root / "moraine-performance"
                family.mkdir(exist_ok=True)
                self._enable_controllers(family)
                self.path = family / self.owned_id
                if self.path.exists():
                    raise RuntimeFailure("owned cgroup path already exists")
                self.path.mkdir()
                self._enable_controllers(self.path)
                self.parent_name = f"moraine-performance/{self.owned_id}"
            else:
                self.parent_name = f"moraine-performance-{self.owned_id}.slice"
                proc = self._command(["systemctl", "start", self.parent_name])
                if proc.returncode:
                    raise RuntimeFailure(f"cannot start systemd cgroup parent: {proc.stderr.strip()}")
                self._systemd_started = True
                proc = self._command(
                    [
                        "systemctl",
                        "set-property",
                        "--runtime",
                        self.parent_name,
                        "CPUAccounting=yes",
                        "MemoryAccounting=yes",
                        "CPUQuota=100%",
                        "CPUQuotaPeriodSec=1s",
                        f"MemoryMax={MEMORY_MAX_BYTES}",
                        "MemorySwapMax=0",
                        "Delegate=yes",
                    ]
                )
                if proc.returncode:
                    raise RuntimeFailure(f"cannot configure systemd cgroup parent: {proc.stderr.strip()}")
                proc = self._command(
                    ["systemctl", "show", "--property=ControlGroup", "--value", self.parent_name]
                )
                if proc.returncode or not proc.stdout.strip().startswith("/"):
                    raise RuntimeFailure("cannot resolve systemd cgroup path")
                self.path = self.root / proc.stdout.strip().lstrip("/")
                if not self.path.is_dir():
                    raise RuntimeFailure("resolved systemd cgroup path is absent")
                self._enable_controllers(self.path)
            assert self.path is not None and self.parent_name is not None
            _write(self.path / "cpu.max", f"{CPU_QUOTA_US} {CPU_PERIOD_US}")
            _write(self.path / "memory.max", str(MEMORY_MAX_BYTES))
            _write(self.path / "memory.swap.max", str(SWAP_MAX_BYTES))
            quota, period = _parse_cpu_max(_read(self.path / "cpu.max"))
            memory_max = _finite_int(_read(self.path / "memory.max"), "memory.max")
            swap_max = _finite_int(_read(self.path / "memory.swap.max"), "memory.swap.max")
            if (quota, period, memory_max, swap_max) != (
                CPU_QUOTA_US,
                CPU_PERIOD_US,
                MEMORY_MAX_BYTES,
                SWAP_MAX_BYTES,
            ):
                raise RuntimeFailure("cgroup controls did not read back exactly")
            self._controllers_proven = True
            self._prove_effective_ancestors()
            return self.parent_name
        except BaseException:
            self.remove(ignore_nonempty=True)
            raise

    def _pid_cgroup(self, pid: int) -> Path:
        text = _read(self.proc_root / str(pid) / "cgroup")
        entries = [line.split(":", 2)[-1] for line in text.splitlines() if line.startswith("0::")]
        if len(entries) != 1:
            raise RuntimeFailure(f"cannot resolve cgroup for pid {pid}")
        return self.root / entries[0].lstrip("/")

    def _descendants(self, roots: Sequence[int]) -> set[int]:
        descendants: set[int] = set()
        pending = list(roots)
        while pending:
            pid = pending.pop()
            if pid in descendants:
                continue
            descendants.add(pid)
            children = self.proc_root / str(pid) / "task" / str(pid) / "children"
            try:
                pending.extend(int(item) for item in children.read_text().split())
            except FileNotFoundError:
                continue
            except (OSError, ValueError) as exc:
                raise RuntimeFailure(f"cannot classify descendants for pid {pid}: {exc}") from exc
        return descendants

    def _cgroup_pids(self) -> set[int]:
        if self.path is None:
            raise RuntimeFailure("fixed envelope has not been created")
        pids: set[int] = set()
        for path in [self.path, *(child for child in self.path.rglob("*") if child.is_dir())]:
            procs = path / "cgroup.procs"
            if not procs.exists():
                continue
            try:
                pids.update(int(item) for item in procs.read_text().split())
            except (OSError, ValueError) as exc:
                raise RuntimeFailure(f"cannot enumerate cgroup processes: {exc}") from exc
        return pids

    def verify_running_binaries(
        self,
        expected_hashes: Mapping[str, str],
        *,
        required_names: Sequence[str] = RUNNING_SERVER_BINARIES,
    ) -> tuple[BinaryProcessEvidence, ...]:
        unknown = set(required_names) - set(expected_hashes)
        if unknown:
            raise RuntimeFailure(f"missing expected binary hashes: {sorted(unknown)}")
        found: dict[str, BinaryProcessEvidence] = {}
        for pid in sorted(self._cgroup_pids()):
            exe = self.proc_root / str(pid) / "exe"
            try:
                name = Path(os.readlink(exe)).name.removesuffix(" (deleted)")
            except (FileNotFoundError, PermissionError):
                continue
            if name not in required_names:
                continue
            if name in found:
                raise RuntimeFailure(f"multiple running {name} binaries in server envelope")
            found[name] = verify_process_binary(
                pid,
                name,
                expected_hashes[name],
                proc_root=self.proc_root,
            )
        missing = set(required_names) - set(found)
        if missing:
            raise RuntimeFailure(f"required server binaries are not running: {sorted(missing)}")
        return tuple(found[name] for name in required_names)

    def inspect(self, server_pids: Sequence[int], excluded_pids: Sequence[int]) -> CgroupEvidence:
        if self.path is None or self.parent_name is None or self.driver is None:
            raise RuntimeFailure("fixed envelope has not been created")
        quota, period = _parse_cpu_max(_read(self.path / "cpu.max"))
        if quota is None:
            raise RuntimeFailure("server cpu.max became unlimited")
        memory_max = _finite_int(_read(self.path / "memory.max"), "memory.max")
        swap_max = _finite_int(_read(self.path / "memory.swap.max"), "memory.swap.max")
        memory_current = _finite_int(_read(self.path / "memory.current"), "memory.current")
        peak_path = self.path / "memory.peak"
        memory_peak = _finite_int(_read(peak_path), "memory.peak") if peak_path.exists() else memory_current
        swap_current = _finite_int(_read(self.path / "memory.swap.current"), "memory.swap.current")
        pids_path = self.path / "pids.current"
        pids_current = _finite_int(_read(pids_path), "pids.current") if pids_path.exists() else len(self._cgroup_pids())
        events_path = self.path / "memory.events.local"
        if not events_path.exists():
            events_path = self.path / "memory.events"
        memory_events = _kv(_read(events_path))
        cpu_stat = _kv(_read(self.path / "cpu.stat"))
        parent = self.path.resolve()
        server_tree = self._descendants(server_pids) if server_pids else set()
        excluded_tree = self._descendants(excluded_pids) if excluded_pids else set()
        actual = self._cgroup_pids()
        server_proven = bool(server_pids) and all(
            self._pid_cgroup(pid).resolve().is_relative_to(parent) for pid in server_tree
        ) and actual.issubset(server_tree)
        loadgen_excluded = bool(excluded_pids) and all(
            not self._pid_cgroup(pid).resolve().is_relative_to(parent) for pid in excluded_tree
        )
        cpuset = self._prove_effective_ancestors()
        exact_limits = (
            quota == CPU_QUOTA_US
            and period == CPU_PERIOD_US
            and memory_max == MEMORY_MAX_BYTES
            and swap_max == SWAP_MAX_BYTES
        )
        role_hash = _canonical_sha256(
            {
                "server": sorted(server_tree),
                "excluded": sorted(excluded_tree),
                "actual": sorted(actual),
            }
        )
        return CgroupEvidence(
            True,
            self.driver,
            self.parent_name,
            str(self.path.relative_to(self.root)),
            cpuset,
            quota,
            period,
            memory_max,
            swap_max,
            memory_current,
            memory_peak,
            swap_current,
            pids_current,
            cpu_stat,
            memory_events,
            server_proven,
            loadgen_excluded,
            self._controllers_proven,
            self._limits_proven and exact_limits,
            self._headroom_proven,
            role_hash,
        )

    def reset_measurement(self, server_pids: Sequence[int], excluded_pids: Sequence[int]) -> CgroupEvidence:
        if self.path is None:
            raise RuntimeFailure("fixed envelope has not been created")
        peak = self.path / "memory.peak"
        if not peak.exists():
            raise RuntimeFailure("kernel does not expose resettable memory.peak")
        _write(peak, "0")
        evidence = self.inspect(server_pids, excluded_pids)
        if not evidence.server_descendants_proven or not evidence.loadgen_excluded_proven:
            raise RuntimeFailure("measured and excluded process placement is not proven")
        if evidence.swap_current_bytes or evidence.memory_events.get("oom_kill", 0):
            raise RuntimeFailure("server envelope is not clean before measurement")
        return evidence

    def remove(self, *, ignore_nonempty: bool = False) -> None:
        path = self.path
        driver = self.driver
        parent = self.parent_name
        if driver == "systemd" and parent and self._systemd_started:
            proc = self._command(["systemctl", "stop", parent])
            if proc.returncode and not ignore_nonempty:
                raise RuntimeFailure(f"cannot stop owned systemd cgroup: {proc.stderr.strip()}")
            self._systemd_started = False
        elif driver == "cgroupfs" and path is not None:
            try:
                path.rmdir()
            except FileNotFoundError:
                pass
            except OSError as exc:
                if not ignore_nonempty:
                    raise RuntimeFailure(f"owned cgroup not empty: {path.name}") from exc
        self.path = None
        self.parent_name = None
        self.driver = None


@dataclass(frozen=True)
class BusyChildProof:
    wall_seconds: float
    cpu_usage_usec: int
    usage_per_wall_cpu: float
    child_progress: tuple[int, int]

    def artifact(self) -> dict[str, Any]:
        return {
            "wall_seconds": self.wall_seconds,
            "cpu_usage_usec": self.cpu_usage_usec,
            "usage_per_wall_cpu": self.usage_per_wall_cpu,
            "both_children_progressed": all(value > 0 for value in self.child_progress),
            "aggregate_one_cpu_proven": 0.65 <= self.usage_per_wall_cpu <= 1.35,
        }


def evaluate_busy_child_proof(
    *, before_usage_usec: int, after_usage_usec: int, wall_seconds: float, child_progress: Sequence[int]
) -> BusyChildProof:
    if wall_seconds <= 0 or len(child_progress) != 2 or any(value <= 0 for value in child_progress):
        raise RuntimeFailure("busy-child proof requires positive wall time and progress from both children")
    delta = after_usage_usec - before_usage_usec
    if delta <= 0:
        raise RuntimeFailure("busy-child aggregate CPU counter did not advance")
    proof = BusyChildProof(wall_seconds, delta, delta / (wall_seconds * 1_000_000), tuple(child_progress))
    if not proof.artifact()["aggregate_one_cpu_proven"]:
        raise RuntimeFailure("aggregate parent did not enforce approximately one CPU")
    return proof


def run_busy_child_proof(envelope: FixedEnvelope, *, duration_seconds: float = 2.0) -> BusyChildProof:
    """Linux proof: two leaf children progress under one aggregate CPU parent."""

    if envelope.path is None:
        raise RuntimeFailure("fixed envelope has not been created")
    if duration_seconds < 1.0:
        raise RuntimeFailure("busy-child proof duration must be at least one second")
    leaves = [envelope.path / "proof-a", envelope.path / "proof-b"]
    children: list[subprocess.Popen[str]] = []
    program = (
        "import sys,time\n"
        "sys.stdin.read(1)\n"
        "end=time.monotonic()+float(sys.argv[1])\n"
        "count=0\n"
        "while time.monotonic()<end:\n"
        " count+=1\n"
        "print(count)\n"
    )
    try:
        for leaf in leaves:
            leaf.mkdir()
        for leaf in leaves:
            child = subprocess.Popen(
                [sys.executable, "-c", program, str(duration_seconds)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            )
            children.append(child)
            _write(leaf / "cgroup.procs", str(child.pid))
        before = _kv(_read(envelope.path / "cpu.stat")).get("usage_usec", 0)
        started = time.monotonic()
        for child in children:
            assert child.stdin is not None
            child.stdin.write("x")
            child.stdin.close()
        progress: list[int] = []
        for child in children:
            assert child.stdout is not None
            output = child.stdout.read().strip()
            try:
                child.wait(timeout=duration_seconds * 4)
            except subprocess.TimeoutExpired as exc:
                raise RuntimeFailure("busy child did not finish") from exc
            if child.returncode:
                stderr = child.stderr.read() if child.stderr is not None else ""
                raise RuntimeFailure(f"busy child failed: {stderr.strip()}")
            try:
                progress.append(int(output))
            except ValueError as exc:
                raise RuntimeFailure("busy child emitted invalid progress") from exc
        elapsed = time.monotonic() - started
        after = _kv(_read(envelope.path / "cpu.stat")).get("usage_usec", 0)
        return evaluate_busy_child_proof(
            before_usage_usec=before,
            after_usage_usec=after,
            wall_seconds=elapsed,
            child_progress=progress,
        )
    finally:
        for child in children:
            terminate_process_group(child)
        for leaf in reversed(leaves):
            try:
                leaf.rmdir()
            except FileNotFoundError:
                pass


@dataclass(frozen=True)
class ContainerRole:
    role: str
    service: str
    container_id: str
    pid: int
    image_id: str
    running: bool


@dataclass(frozen=True)
class SandboxStatus:
    sandbox_id: str
    project: str
    performance: bool
    lifecycle_phase: str
    monitor_port: int
    clickhouse_port: int
    config_path: Path
    cgroup_parent: str
    ownership_token_sha256: str
    binary_manifest_sha256: str
    binary_sha256: Mapping[str, str]
    containers: tuple[ContainerRole, ...]

    @property
    def server_pids(self) -> tuple[int, ...]:
        return tuple(item.pid for item in self.containers if item.role.startswith("server_"))

    @property
    def loadgen_pids(self) -> tuple[int, ...]:
        return tuple(item.pid for item in self.containers if item.role == "loadgen")

    @property
    def image_ids(self) -> dict[str, str]:
        return {item.service: item.image_id for item in self.containers}

    @classmethod
    def parse(cls, value: Any) -> "SandboxStatus":
        expected_fields = {
            "schema_version", "sandbox_id", "project", "performance",
            "lifecycle_phase", "ports", "config_path", "cgroup_parent",
            "ownership_token_sha256", "binary_manifest_sha256",
            "binary_sha256", "containers",
        }
        if (
            not isinstance(value, dict)
            or set(value) != expected_fields
            or value.get("schema_version") != SANDBOX_SCHEMA
        ):
            raise RuntimeFailure("sandbox status has an unknown or non-exact schema")
        if not isinstance(value["performance"], bool):
            raise RuntimeFailure("sandbox performance classification must be boolean")
        ports = value["ports"]
        if (
            not isinstance(ports, dict)
            or set(ports) != {"monitor", "clickhouse_http"}
            or any(not isinstance(port, int) or isinstance(port, bool) or port <= 0 for port in ports.values())
        ):
            raise RuntimeFailure("sandbox status has invalid published ports")
        raw_containers = value["containers"]
        if not isinstance(raw_containers, list):
            raise RuntimeFailure("sandbox status containers must be a list")
        containers: list[ContainerRole] = []
        for item in raw_containers:
            if not isinstance(item, dict) or set(item) != {
                "role", "service", "container_id", "pid", "image_id", "running"
            }:
                raise RuntimeFailure("sandbox container role has non-exact fields")
            if (
                not isinstance(item["role"], str)
                or not isinstance(item["service"], str)
                or not isinstance(item["container_id"], str)
                or not isinstance(item["pid"], int)
                or isinstance(item["pid"], bool)
                or not isinstance(item["image_id"], str)
                or not isinstance(item["running"], bool)
            ):
                raise RuntimeFailure("sandbox container role has invalid field types")
            containers.append(ContainerRole(
                item["role"], item["service"], item["container_id"],
                item["pid"], item["image_id"], item["running"],
            ))
        hashes = value["binary_sha256"]
        if not isinstance(hashes, dict) or any(
            not isinstance(name, str) or not isinstance(digest, str)
            for name, digest in hashes.items()
        ):
            raise RuntimeFailure("sandbox binary identities must be a mapping")
        string_fields = (
            "sandbox_id", "project", "lifecycle_phase", "config_path",
            "cgroup_parent", "ownership_token_sha256", "binary_manifest_sha256",
        )
        if any(not isinstance(value[field], str) for field in string_fields):
            raise RuntimeFailure("sandbox status has invalid lifecycle field types")
        result = cls(
            value["sandbox_id"],
            value["project"],
            value["performance"],
            value["lifecycle_phase"],
            ports["monitor"],
            ports["clickhouse_http"],
            Path(value["config_path"]),
            value["cgroup_parent"],
            value["ownership_token_sha256"],
            value["binary_manifest_sha256"],
            dict(hashes),
            tuple(containers),
        )
        if (
            not SANDBOX_ID_RE.fullmatch(result.sandbox_id)
            or result.project != f"moraine-sandbox-{result.sandbox_id}"
            or not SHA256_RE.fullmatch(result.ownership_token_sha256)
            or not SHA256_RE.fullmatch(result.binary_manifest_sha256)
            or not result.config_path.is_absolute()
        ):
            raise RuntimeFailure("sandbox status contains invalid ownership identity")
        if result.performance:
            expected_roles = {
                "server_clickhouse": "clickhouse",
                "server_moraine": "moraine",
                "loadgen": "loadgen",
            }
            roles = {item.role: item.service for item in result.containers}
            if len(result.containers) != 3 or roles != expected_roles:
                raise RuntimeFailure(f"sandbox status has invalid role classification: {sorted(roles)}")
            if result.lifecycle_phase not in {"reserved", *CHECKPOINTS} or not result.cgroup_parent:
                raise RuntimeFailure("sandbox status has invalid performance lifecycle state")
            if not all(
                item.running
                and item.pid > 0
                and item.container_id
                and SHA256_RE.fullmatch(item.image_id)
                for item in result.containers
            ):
                raise RuntimeFailure("performance containers are not all running with immutable image IDs")
            if set(result.binary_sha256) != set(RELEASE_BINARIES) or not all(
                SHA256_RE.fullmatch(digest) for digest in result.binary_sha256.values()
            ):
                raise RuntimeFailure("sandbox status has incomplete binary identities")
        return result


@dataclass(frozen=True)
class AckObservation:
    batch_sequence: int
    event_identity_digests: tuple[str, ...]
    ack_monotonic_ns: int


@dataclass(frozen=True)
class AckLogBatch:
    next_cursor: int
    observations: tuple[AckObservation, ...]
    gap_detected: bool


@dataclass(frozen=True)
class WatcherReadiness:
    files_watched: int
    queue_depth: int
    files_active: int
    heartbeat_ts_unix_ms: int


@dataclass
class OwnedSandbox:
    repo_root: Path
    script: Path
    sandbox_id: str
    project: str
    monitor_port: int
    clickhouse_port: int
    config_dir: Path
    ownership_token_sha256: str
    build: BuildIdentity
    _stopped: bool = False

    @property
    def watched_source_dir(self) -> Path:
        return self.config_dir / "fixtures" / "codex" / "sessions"

    def _json_command(self, argv: Sequence[str], timeout: float = 120.0) -> Any:
        proc = _run([str(self.script), *argv], timeout=timeout)
        if proc.returncode:
            raise RuntimeFailure(f"sandbox {' '.join(argv)} failed: {proc.stderr[-2048:].strip()}")
        try:
            return json.loads(proc.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeFailure(f"sandbox {' '.join(argv)} did not return JSON") from exc

    def status(self) -> SandboxStatus:
        status = SandboxStatus.parse(self._json_command(["status", "--json", self.sandbox_id]))
        if status.sandbox_id != self.sandbox_id or status.project != self.project:
            raise RuntimeFailure("sandbox status identity changed")
        if status.ownership_token_sha256 != self.ownership_token_sha256:
            raise RuntimeFailure("sandbox ownership identity changed")
        return status

    def central_status(self) -> Mapping[str, Any]:
        value = self._json_command(["benchmark-service", "status", self.sandbox_id])
        expected_fields = {
            "schema_version", "cache_generation", "central", "ingest",
            "server_children", "route_processes",
        }
        if (
            not isinstance(value, dict)
            or set(value) != expected_fields
            or value.get("schema_version") != CENTRAL_STATUS_SCHEMA
        ):
            raise RuntimeFailure("central status has an unknown or non-exact schema")
        for role in ("central", "ingest"):
            process = value[role]
            if (
                not isinstance(process, dict)
                or set(process) != {"pid", "starttime"}
                or any(
                    not isinstance(process[field], int)
                    or isinstance(process[field], bool)
                    or process[field] <= 0
                    for field in ("pid", "starttime")
                )
            ):
                raise RuntimeFailure(f"central status omitted {role} process identity")
        generation = value["cache_generation"]
        if not isinstance(generation, str) or not re.fullmatch(
            r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
            generation,
        ):
            raise RuntimeFailure("central status omitted cache generation")
        children = value["server_children"]
        routes = value["route_processes"]
        if not isinstance(children, list) or any(
            not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0 for pid in children
        ):
            raise RuntimeFailure("central status omitted server_children")
        if not isinstance(routes, list) or any(
            not isinstance(process, dict)
            or set(process) != {"pid", "starttime"}
            or any(
                not isinstance(process[field], int)
                or isinstance(process[field], bool)
                or process[field] <= 0
                for field in ("pid", "starttime")
            )
            for process in routes
        ):
            raise RuntimeFailure("central status omitted route_processes")
        return value

    def stop_central(self) -> None:
        """Stop the minimal measured container before a fresh TTR spawn."""
        proc = _run([str(self.script), "benchmark-service", "stop", self.sandbox_id], timeout=120)
        if proc.returncode:
            raise RuntimeFailure(f"cannot stop central service: {proc.stderr[-2048:].strip()}")

    def spawn_central(self) -> subprocess.Popen[bytes]:
        """Begin Docker start; callers capture t0 immediately before this call."""
        try:
            return subprocess.Popen(
                [str(self.script), "benchmark-service", "start", self.sandbox_id],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
            )
        except OSError as exc:
            raise RuntimeFailure(f"cannot spawn central service: {exc}") from exc

    def wait_central_ready_without_search(
        self,
        start_process: Optional[subprocess.Popen[bytes]] = None,
        *,
        timeout_s: float = 120.0,
    ) -> Mapping[str, Any]:
        """Wait on the health endpoint, which never executes the target search."""
        deadline = time.monotonic() + timeout_s
        if start_process is not None:
            try:
                stdout, stderr = start_process.communicate(timeout=max(0.1, timeout_s))
            except subprocess.TimeoutExpired as exc:
                terminate_process_group(start_process)
                raise RuntimeFailure("central container start did not finish") from exc
            if start_process.returncode:
                raise RuntimeFailure(
                    f"central container start failed: {(stderr or stdout)[-2048:].decode(errors='replace').strip()}"
                )
        url = f"http://127.0.0.1:{self.monitor_port}/api/v1/health"
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(url, timeout=1.0) as response:
                    if response.status == 200:
                        return self.central_status()
            except (urllib.error.URLError, TimeoutError):
                pass
            time.sleep(0.02)
        raise RuntimeFailure("central service did not become ready without a target search")

    def _read_ingest_status(self) -> Optional[WatcherReadiness]:
        url = f"http://127.0.0.1:{self.monitor_port}/api/v1/status"
        try:
            with urllib.request.urlopen(url, timeout=1.0) as response:
                raw = response.read(1024 * 1024 + 1)
        except (urllib.error.URLError, TimeoutError):
            return None
        if len(raw) > 1024 * 1024:
            raise RuntimeFailure("ingest status response is too large")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeFailure("ingest status returned malformed JSON") from exc
        if not isinstance(payload, dict) or payload.get("ok") is not True:
            raise RuntimeFailure("ingest status is not healthy")
        ingestor = payload.get("ingestor")
        latest = ingestor.get("latest") if isinstance(ingestor, dict) else None
        if not isinstance(latest, dict):
            return None
        if latest.get("last_error") not in ("", None):
            raise RuntimeFailure("ingest watcher reported an error before measurement")
        values = {
            name: latest.get(name)
            for name in ("files_watched", "queue_depth", "files_active", "ts_unix_ms")
        }
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0
            for value in values.values()
        ):
            raise RuntimeFailure("ingest watcher heartbeat has invalid counters")
        return WatcherReadiness(
            values["files_watched"],
            values["queue_depth"],
            values["files_active"],
            values["ts_unix_ms"],
        )

    def ingest_status(self) -> WatcherReadiness:
        status = self._read_ingest_status()
        if status is None:
            raise RuntimeFailure("ingest heartbeat is not available")
        return status

    def wait_watcher_ready(self, *, timeout_s: float = 30.0) -> WatcherReadiness:
        """Prove the production watcher registered without issuing a search."""
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            status = self._read_ingest_status()
            if status is not None and status.files_watched > 0:
                return status
            time.sleep(0.05)
        raise RuntimeFailure("ingest watcher did not register before measurement")

    def wait_ingest_drained(self, *, timeout_s: float = 30.0) -> WatcherReadiness:
        """Wait for a fresh heartbeat proving no queued or active ingest work."""
        initial = self.ingest_status()
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            status = self._read_ingest_status()
            if (
                status is not None
                and status.heartbeat_ts_unix_ms != initial.heartbeat_ts_unix_ms
                and status.queue_depth == 0
                and status.files_active == 0
            ):
                return status
            time.sleep(0.05)
        raise RuntimeFailure("ingest work did not drain before timeout")

    def read_ingest_ack_logs(self, cursor: int = 0) -> AckLogBatch:
        """Read typed, content-free ack observations after a stable count cursor."""
        if not isinstance(cursor, int) or isinstance(cursor, bool) or cursor < 0:
            raise RuntimeFailure("ack log cursor must be a non-negative integer")
        proc = _run([str(self.script), "benchmark-logs", self.sandbox_id], timeout=120)
        if proc.returncode:
            raise RuntimeFailure(f"cannot read owned benchmark logs: {proc.stderr[-2048:].strip()}")
        observations: list[AckObservation] = []
        marker_fields = ("batch_sequence=", "event_identity_digests=", "ack_monotonic_ns=")
        for line in (*proc.stdout.splitlines(), *proc.stderr.splitlines()):
            present = tuple(field in line for field in marker_fields)
            if not any(present):
                continue
            if not all(present):
                raise RuntimeFailure("malformed ingest-ack log line")
            sequence_match = re.search(r"batch_sequence=(\d+)", line)
            digests_match = re.search(
                r'event_identity_digests=(?:")?(\[.*?\])(?:")?(?:\s|$)',
                line,
            )
            monotonic_match = re.search(r"ack_monotonic_ns=(\d+)", line)
            if sequence_match is None or digests_match is None or monotonic_match is None:
                raise RuntimeFailure("malformed ingest-ack log fields")
            encoded_digests = digests_match.group(1)
            try:
                decoded = json.loads(encoded_digests)
            except json.JSONDecodeError:
                try:
                    decoded = json.loads(encoded_digests.replace(r'\"', '"'))
                except json.JSONDecodeError as exc:
                    raise RuntimeFailure("ingest-ack digest array is malformed") from exc
            if (
                not isinstance(decoded, list)
                or any(
                    not isinstance(digest, str)
                    or re.fullmatch(r"[0-9a-f]{64}", digest) is None
                    for digest in decoded
                )
            ):
                raise RuntimeFailure("ingest-ack digest array is not content-free SHA-256")
            sequence = int(sequence_match.group(1))
            monotonic_ns = int(monotonic_match.group(1))
            if sequence <= 0 or monotonic_ns <= 0:
                raise RuntimeFailure("ingest-ack counters must be positive")
            observations.append(AckObservation(sequence, tuple(decoded), monotonic_ns))
        if len(observations) < cursor:
            return AckLogBatch(len(observations), (), True)
        return AckLogBatch(len(observations), tuple(observations[cursor:]), False)

    def checkpoint(self, phase: str) -> None:
        if phase not in CHECKPOINTS:
            raise RuntimeFailure(f"invalid benchmark lifecycle checkpoint: {phase}")
        proc = _run([str(self.script), "checkpoint", self.sandbox_id, phase], timeout=30)
        if proc.returncode:
            raise RuntimeFailure(f"cannot record benchmark checkpoint: {proc.stderr[-2048:].strip()}")
        if os.environ.get("MORAINE_PERFORMANCE_FAILPOINT") == phase:
            raise RuntimeFailure(f"performance failpoint reached: {phase}")

    def spawn_stdio_route(
        self,
        *,
        cwd: str = "/home/moraine",
        environment: Optional[Mapping[str, str]] = None,
    ) -> subprocess.Popen[bytes]:
        if not cwd.startswith("/") or "\x00" in cwd:
            raise RuntimeFailure("loadgen working directory must be an absolute container path")
        args = [str(self.script), "exec-loadgen", self.sandbox_id, "--cwd", cwd]
        merged = {"RUST_LOG": "moraine_mcp=debug"}
        if environment:
            merged.update(environment)
        for key, value in sorted(merged.items()):
            if not re.fullmatch(r"[A-Z][A-Z0-9_]*", key) or "\x00" in value:
                raise RuntimeFailure("invalid loadgen environment")
            args.extend(["--env", f"{key}={value}"])
        args.extend(
            [
                "--",
                "/opt/moraine/bin/moraine-mcp",
                "--config",
                "/sandbox/moraine.toml",
                "--serve",
                "stdio",
            ]
        )
        try:
            return subprocess.Popen(
                args,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
            )
        except OSError as exc:
            raise RuntimeFailure(f"cannot spawn loadgen MCP route: {exc}") from exc

    def down(self) -> None:
        if self._stopped:
            return
        proc = _run([str(self.script), "down", self.sandbox_id], timeout=600)
        if proc.returncode:
            raise RuntimeFailure(
                f"sandbox down failed; owned leak {self.sandbox_id}: {proc.stderr[-2048:].strip()}"
            )
        self._stopped = True

    def __enter__(self) -> "OwnedSandbox":
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        self.down()


def _sandbox_status(script: Path, sandbox_id: str) -> SandboxStatus:
    proc = _run([str(script), "status", "--json", sandbox_id], timeout=120)
    if proc.returncode:
        raise RuntimeFailure(f"sandbox status failed: {proc.stderr[-2048:].strip()}")
    try:
        return SandboxStatus.parse(json.loads(proc.stdout))
    except json.JSONDecodeError as exc:
        raise RuntimeFailure("sandbox status did not return JSON") from exc


def start_owned_sandbox(
    repo_root: Path,
    *,
    cgroup_parent: str,
    build: BuildIdentity,
) -> OwnedSandbox:
    """Start one physical performance sandbox and verify its immutable identity."""

    if not cgroup_parent:
        raise RuntimeFailure("performance sandbox requires a cgroup parent")
    if not build.directory.is_dir() or hash_file(build.manifest_path) != build.manifest_sha256:
        raise RuntimeFailure("immutable build manifest changed before sandbox start")
    script = repo_root / "scripts/dev/sandbox/moraine-sandbox"
    sandbox_id = "sb-" + uuid.uuid4().hex[:6]
    args = [
        str(script),
        "up",
        "--quiet",
        "--id",
        sandbox_id,
        "--performance",
        "--cgroup-parent",
        cgroup_parent,
        "--binary-dir",
        str(build.directory),
    ]
    try:
        proc = _run(args, timeout=3600)
        if proc.returncode:
            raise RuntimeFailure(f"sandbox up failed: {proc.stderr[-4096:].strip()}")
        if proc.stdout.strip() != sandbox_id:
            raise RuntimeFailure("sandbox returned a mismatched owned id")
        status = _sandbox_status(script, sandbox_id)
        if not status.performance or status.cgroup_parent != cgroup_parent:
            raise RuntimeFailure("sandbox status omitted performance cgroup identity")
        if dict(status.binary_sha256) != dict(build.binary_sha256):
            raise RuntimeFailure("sandbox binary manifest differs from frozen build")
        if status.binary_manifest_sha256 != build.manifest_sha256:
            raise RuntimeFailure("sandbox build manifest hash mismatch")
        sandbox = OwnedSandbox(
            repo_root,
            script,
            sandbox_id,
            status.project,
            status.monitor_port,
            status.clickhouse_port,
            status.config_path.parent,
            status.ownership_token_sha256,
            build,
        )
        sandbox.checkpoint("services-ready")
        return sandbox
    except BaseException:
        cleanup = _run([str(script), "down", sandbox_id], timeout=600)
        if cleanup.returncode:
            raise RuntimeFailure(
                f"sandbox verification failed and cleanup retained owned leak {sandbox_id}: "
                f"{cleanup.stderr[-2048:].strip()}"
            )
        raise


def terminate_process_group(proc: Optional[subprocess.Popen[Any]], timeout_s: float = 5.0) -> None:
    if proc is None or proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
        proc.wait(timeout=timeout_s)
    except ProcessLookupError:
        return
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait(timeout=timeout_s)
