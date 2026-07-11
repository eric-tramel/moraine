#!/usr/bin/env python3
"""Regenerate issue #477 current-tree migration and topology evidence.

The verifier compiles but does not execute test bodies. It authenticates and
materializes the exact baseline commit outside the candidate tree, compares
every moved Rust function body, collects native libtest names, and measures
same-host cold/warm no-run builds in separate fresh Cargo target directories.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import platform
import re
import shutil
import struct
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, NoReturn

BASELINE_COMMIT = "77c90d6c572ba71bcfaa0362f7ead2cfa5e6712a"
BASELINE_REV = "77c90d6"
BASELINE_SOURCE = "crates/moraine-conversations/tests/repository_integration.rs"
CANDIDATE_ROOT = Path("crates/moraine-conversations/tests/repository_integration")
NAME_MAP = CANDIDATE_ROOT / "test-name-map.json"
PACKAGE = "moraine-conversations"
TARGET = "repository_integration"
FORMAT_VERSION = 1
EXPECTED_REPOSITORY_TESTS = 67
EXPECTED_BASELINE_INTEGRATION_TARGETS = 2
EXPECTED_BASELINE_LINKED_TEST_EXECUTABLES = 3
EXPECTED_CANDIDATE_INTEGRATION_TARGETS = 3
EXPECTED_CANDIDATE_LINKED_TEST_EXECUTABLES = 4
EXPECTED_BASELINE_TREE = "0ae5df87300c6748161f2035394b6e0b9a92e627"
METADATA_COMMAND = ["cargo", "metadata", "--format-version", "1", "--locked", "--no-deps"]
COMPILE_COMMAND = [
    "cargo",
    "test",
    "-p",
    PACKAGE,
    "--locked",
    "--no-run",
    "--message-format=json",
]
COLLECT_COMMAND = [
    "cargo",
    "test",
    "--workspace",
    "--locked",
    "--no-run",
    "--message-format=json",
]
POLICY_COMMAND = ["python3", "scripts/ci/dependency_policy.py"]
TEST_ATTRIBUTE_RE = re.compile(rb"#\s*\[\s*(?:tokio::)?test(?:\s*\([^]]*\))?\s*]")
FUNCTION_RE = re.compile(
    rb"(?m)^\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+"
    rb"([A-Za-z_][A-Za-z0-9_]*)\s*\("
)
ASSERTION_RE = re.compile(rb"\b(?:debug_)?assert[A-Za-z0-9_]*!\s*([({[])" )


def fail(message: str) -> NoReturn:
    raise SystemExit(message)


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def hash_sequence(items: list[bytes]) -> str:
    digest = hashlib.sha256()
    for item in items:
        digest.update(struct.pack(">Q", len(item)))
        digest.update(item)
    return digest.hexdigest()


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def run(
    command: list[str],
    cwd: Path,
    *,
    env: dict[str, str] | None = None,
    check: bool = True,
    text: bool = True,
) -> subprocess.CompletedProcess[Any]:
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=text,
            check=False,
        )
    except FileNotFoundError as error:
        fail(f"required command not found: {command[0]} ({error})")
    if check and result.returncode != 0:
        stderr = result.stderr if text else result.stderr.decode(errors="replace")
        sys.stderr.write(stderr)
        fail(f"command failed ({result.returncode}): {' '.join(command)}")
    return result


def relative_path(path: str | Path, root: Path) -> str:
    resolved = Path(path).resolve()
    try:
        return resolved.relative_to(root.resolve()).as_posix()
    except ValueError:
        fail(f"path escapes repository root: {resolved}")


def command_string(command: list[str]) -> str:
    return " ".join(command)


def authenticate_baseline(repo: Path) -> dict[str, str]:
    commit_command = [
        "git",
        "rev-parse",
        "--verify",
        f"{BASELINE_COMMIT}^{{commit}}",
    ]
    tree_command = [
        "git",
        "rev-parse",
        "--verify",
        f"{BASELINE_COMMIT}^{{tree}}",
    ]
    commit = run(commit_command, repo).stdout.strip()
    tree = run(tree_command, repo).stdout.strip()
    if commit != BASELINE_COMMIT:
        fail(
            "baseline commit authentication failed: "
            f"expected {BASELINE_COMMIT}, observed {commit}"
        )
    if tree != EXPECTED_BASELINE_TREE:
        fail(
            "baseline tree authentication failed: "
            f"expected {EXPECTED_BASELINE_TREE}, observed {tree}"
        )
    return {
        "commit": commit,
        "tree": tree,
        "commit_authentication_command": command_string(commit_command),
        "tree_authentication_command": command_string(tree_command),
    }


def materialize_baseline(
    repo: Path, owned_root: Path, identity: dict[str, str]
) -> tuple[Path, dict[str, str]]:
    source = owned_root / "baseline-source"
    archive = owned_root / "baseline.tar"
    source.mkdir()
    archive_command = [
        "git",
        "archive",
        "--format=tar",
        "--output",
        "$BASELINE_ARCHIVE",
        identity["commit"],
    ]
    run(
        [
            "git",
            "archive",
            "--format=tar",
            "--output",
            str(archive),
            identity["commit"],
        ],
        repo,
    )
    extract_command = [
        "tar",
        "-xf",
        "$BASELINE_ARCHIVE",
        "-C",
        "$BASELINE_SOURCE_DIR",
    ]
    run(["tar", "-xf", str(archive), "-C", str(source)], repo)
    if not (source / "Cargo.toml").is_file() or not (source / "Cargo.lock").is_file():
        fail("authenticated baseline archive did not materialize a Cargo workspace")
    try:
        source.resolve().relative_to(repo.resolve())
    except ValueError:
        pass
    else:
        fail(f"baseline source was materialized inside candidate tree: {source}")
    return source, {
        "archive_command": command_string(archive_command),
        "extract_command": command_string(extract_command),
    }


def rust_literal_end(data: bytes, offset: int) -> int | None:
    """Return the exclusive end of a Rust string/byte-string literal."""

    length = len(data)
    raw_start: int | None = None
    if data[offset : offset + 2] == b"br":
        raw_start = offset + 1
    elif data[offset : offset + 1] == b"r":
        raw_start = offset
    if raw_start is not None:
        quote = raw_start + 1
        while quote < length and data[quote] == ord("#"):
            quote += 1
        if quote < length and data[quote] == ord('"'):
            hashes = quote - raw_start - 1
            terminator = b'"' + b"#" * hashes
            end = data.find(terminator, quote + 1)
            if end < 0:
                fail("unterminated Rust raw string")
            return end + len(terminator)

    quote = offset
    if data[offset : offset + 2] == b'b"':
        quote += 1
    if data[quote : quote + 1] != b'"':
        return None
    cursor = quote + 1
    while cursor < length:
        if data[cursor] == ord("\\"):
            cursor += 2
        elif data[cursor] == ord('"'):
            return cursor + 1
        else:
            cursor += 1
    fail("unterminated Rust string")


def rust_char_end(data: bytes, offset: int) -> int | None:
    """Distinguish short character literals from Rust lifetimes."""

    if data[offset : offset + 1] != b"'":
        return None
    cursor = offset + 1
    escaped = False
    while cursor < min(len(data), offset + 13) and data[cursor] not in (10, 13):
        if not escaped and data[cursor] == ord("'"):
            return cursor + 1
        if not escaped and data[cursor] == ord("\\"):
            escaped = True
        else:
            escaped = False
        cursor += 1
    return None


def matching_delimiter(data: bytes, start: int) -> int:
    pairs = {ord("{"): ord("}"), ord("("): ord(")"), ord("["): ord("]")}
    opening = data[start]
    if opening not in pairs:
        fail(f"not an opening delimiter at byte {start}")
    stack = [pairs[opening]]
    cursor = start + 1
    while cursor < len(data):
        if data.startswith(b"//", cursor):
            newline = data.find(b"\n", cursor + 2)
            cursor = len(data) if newline < 0 else newline + 1
            continue
        if data.startswith(b"/*", cursor):
            depth = 1
            cursor += 2
            while cursor < len(data) and depth:
                if data.startswith(b"/*", cursor):
                    depth += 1
                    cursor += 2
                elif data.startswith(b"*/", cursor):
                    depth -= 1
                    cursor += 2
                else:
                    cursor += 1
            if depth:
                fail("unterminated Rust block comment")
            continue
        literal_end = rust_literal_end(data, cursor)
        if literal_end is not None:
            cursor = literal_end
            continue
        char_end = rust_char_end(data, cursor)
        if char_end is not None:
            cursor = char_end
            continue
        byte = data[cursor]
        if byte in pairs:
            stack.append(pairs[byte])
        elif byte in (ord("}"), ord(")"), ord("]")):
            if byte != stack[-1]:
                fail(f"unbalanced Rust delimiter at byte {cursor}")
            stack.pop()
            if not stack:
                return cursor
        cursor += 1
    fail(f"unterminated Rust delimiter at byte {start}")


def contiguous_attributes(data: bytes, declaration_start: int) -> list[bytes]:
    """Collect attributes immediately preceding a function declaration."""

    attributes: list[bytes] = []
    cursor = declaration_start
    attribute_re = re.compile(rb"#\s*\[[^\]]*]")
    while cursor:
        prefix = data[:cursor]
        stripped = prefix.rstrip()
        if not stripped.endswith(b"]"):
            break
        matches = list(attribute_re.finditer(stripped))
        if not matches or matches[-1].end() != len(stripped):
            break
        match = matches[-1]
        attributes.append(match.group())
        cursor = match.start()
    attributes.reverse()
    return attributes


def canonical_signature(signature: bytes) -> bytes:
    """Normalize only non-semantic move/formatter differences in a fn signature."""

    without_visibility = re.sub(
        rb"^pub(?:\([^)]*\))?\s+", b"", signature.strip(), count=1
    )
    tokens = re.sub(rb"\s+", b"", without_visibility)
    parameters_start = tokens.find(b"(")
    if parameters_start < 0:
        fail("Rust function signature has no parameter list")
    parameters_end = matching_delimiter(tokens, parameters_start)
    if tokens[parameters_end - 1 : parameters_end] == b",":
        tokens = tokens[: parameters_end - 1] + tokens[parameters_end:]
    return tokens


def locate_functions(sources: dict[str, bytes]) -> dict[str, dict[str, Any]]:
    functions: dict[str, dict[str, Any]] = {}
    for path, data in sorted(sources.items()):
        for match in FUNCTION_RE.finditer(data):
            name = match.group(1).decode()
            if name in functions:
                fail(f"duplicate Rust function name in migration surface: {name}")
            body_start = data.find(b"{", match.end())
            if body_start < 0:
                fail(f"function has no body: {path}:{name}")
            declaration_match = re.search(
                rb"(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+"
                + re.escape(name.encode())
                + rb"\s*\(",
                data[match.start() : body_start],
            )
            if declaration_match is None:
                fail(f"cannot locate function declaration start: {path}:{name}")
            declaration_start = match.start() + declaration_match.start()
            signature_tokens = canonical_signature(
                data[declaration_start:body_start]
            )
            body_end = matching_delimiter(data, body_start)
            functions[name] = {
                "path": path,
                "attributes": contiguous_attributes(data, declaration_start),
                "signature_tokens": signature_tokens,
                "body": data[body_start : body_end + 1],
            }
    return functions


def string_literals(data: bytes) -> list[bytes]:
    literals: list[bytes] = []
    cursor = 0
    while cursor < len(data):
        if data.startswith(b"//", cursor):
            newline = data.find(b"\n", cursor + 2)
            cursor = len(data) if newline < 0 else newline + 1
            continue
        if data.startswith(b"/*", cursor):
            depth = 1
            cursor += 2
            while cursor < len(data) and depth:
                if data.startswith(b"/*", cursor):
                    depth += 1
                    cursor += 2
                elif data.startswith(b"*/", cursor):
                    depth -= 1
                    cursor += 2
                else:
                    cursor += 1
            if depth:
                fail("unterminated Rust block comment while collecting literals")
            continue
        end = rust_literal_end(data, cursor)
        if end is not None:
            literals.append(data[cursor:end])
            cursor = end
            continue
        char_end = rust_char_end(data, cursor)
        if char_end is not None:
            cursor = char_end
            continue
        cursor += 1
    return literals


def rust_code_mask(data: bytes) -> bytes:
    """Blank comments and literals while preserving every byte offset."""

    masked = bytearray(data)
    cursor = 0
    while cursor < len(data):
        start = cursor
        if data.startswith(b"//", cursor):
            newline = data.find(b"\n", cursor + 2)
            cursor = len(data) if newline < 0 else newline
        elif data.startswith(b"/*", cursor):
            depth = 1
            cursor += 2
            while cursor < len(data) and depth:
                if data.startswith(b"/*", cursor):
                    depth += 1
                    cursor += 2
                elif data.startswith(b"*/", cursor):
                    depth -= 1
                    cursor += 2
                else:
                    cursor += 1
            if depth:
                fail("unterminated Rust block comment while masking code")
        else:
            literal_end = rust_literal_end(data, cursor)
            char_end = rust_char_end(data, cursor)
            end = literal_end if literal_end is not None else char_end
            if end is None:
                cursor += 1
                continue
            cursor = end
        masked[start:cursor] = b" " * (cursor - start)
    return bytes(masked)


def assertion_invocations(data: bytes) -> list[bytes]:
    invocations: list[bytes] = []
    code = rust_code_mask(data)
    for match in ASSERTION_RE.finditer(code):
        opening = match.end() - 1
        end = matching_delimiter(data, opening)
        invocations.append(data[match.start() : end + 1])
    return invocations


def contract(body: bytes) -> dict[str, Any]:
    assertions = assertion_invocations(body)
    literals = string_literals(body)
    return {
        "body_bytes": len(body),
        "body_sha256": sha256(body),
        "assertion_invocations": len(assertions),
        "assertion_contract_sha256": hash_sequence(assertions),
        "embedded_string_literals": len(literals),
        "embedded_string_literal_bytes": sum(len(item) for item in literals),
        "embedded_string_literal_contract_sha256": hash_sequence(literals),
    }


def load_baseline_source(repo: Path) -> bytes:
    result = run(
        ["git", "show", f"{BASELINE_REV}:{BASELINE_SOURCE}"],
        repo,
        text=False,
    )
    return result.stdout


def load_candidate_sources(repo: Path) -> dict[str, bytes]:
    root = repo / CANDIDATE_ROOT
    if not root.is_dir():
        fail(f"candidate repository test directory is missing: {root}")
    sources: dict[str, bytes] = {}
    for path in sorted(root.rglob("*.rs")):
        relative = path.relative_to(repo).as_posix()
        sources[relative] = path.read_bytes()
    if not sources:
        fail("candidate repository test directory has no Rust sources")
    return sources


def normalized_metadata(repo: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    raw = json.loads(run(METADATA_COMMAND, repo).stdout)
    if raw.get("version") != 1:
        fail(f"unsupported Cargo metadata version: {raw.get('version')!r}")
    members = raw.get("workspace_members")
    packages = raw.get("packages")
    if not isinstance(members, list) or not members:
        fail("Cargo metadata returned zero workspace members")
    if not isinstance(packages, list):
        fail("Cargo metadata packages is not a list")
    by_id = {package.get("id"): package for package in packages}
    if len(by_id) != len(packages) or None in by_id:
        fail("Cargo metadata contains missing or duplicate package ids")
    missing = [member for member in members if member not in by_id]
    if missing:
        fail(f"Cargo metadata workspace members are missing: {missing}")
    selected = [by_id[member] for member in members]
    names = [package.get("name") for package in selected]
    if any(not isinstance(name, str) or not name for name in names):
        fail("Cargo metadata workspace package has an invalid name")
    if len(names) != len(set(names)):
        fail(f"Cargo metadata workspace package names are ambiguous: {names}")

    normalized_packages: list[dict[str, Any]] = []
    normal_dependency_packages: list[dict[str, Any]] = []
    for package in selected:
        targets = [
            {
                "name": target["name"],
                "kind": target["kind"],
                "crate_types": target["crate_types"],
                "src_path": relative_path(target["src_path"], repo),
                "test": target["test"],
                "doctest": target["doctest"],
                "bench": target.get("bench", False),
            }
            for target in package.get("targets", [])
        ]
        targets.sort(key=lambda item: (item["name"], item["kind"], item["src_path"]))
        normalized_packages.append(
            {
                "name": package["name"],
                "version": package["version"],
                "manifest_path": relative_path(package["manifest_path"], repo),
                "targets": targets,
            }
        )
        dependencies = []
        for dependency in package.get("dependencies", []):
            if dependency.get("kind") not in (None, "normal"):
                continue
            dependency_path = dependency.get("path")
            dependencies.append(
                {
                    "name": dependency["name"],
                    "rename": dependency.get("rename"),
                    "req": dependency["req"],
                    "source": dependency.get("source"),
                    "path": (
                        relative_path(dependency_path, repo) if dependency_path else None
                    ),
                    "optional": dependency["optional"],
                    "uses_default_features": dependency["uses_default_features"],
                    "features": sorted(dependency["features"]),
                    "target": dependency.get("target"),
                }
            )
        dependencies.sort(
            key=lambda item: (
                item["name"],
                item["rename"] or "",
                item["target"] or "",
                item["req"],
            )
        )
        normal_dependency_packages.append(
            {
                "name": package["name"],
                "manifest_path": relative_path(package["manifest_path"], repo),
                "dependencies": dependencies,
            }
        )
    normalized_packages.sort(key=lambda item: item["name"])
    normal_dependency_packages.sort(key=lambda item: item["name"])
    target_count = sum(len(package["targets"]) for package in normalized_packages)
    test_target_count = sum(
        target["kind"] == ["test"]
        for package in normalized_packages
        for target in package["targets"]
    )
    inventory = {
        "format_version": FORMAT_VERSION,
        "source": "current working tree",
        "metadata_command": command_string(METADATA_COMMAND),
        "workspace_package_count": len(normalized_packages),
        "target_count": target_count,
        "integration_test_target_count": test_target_count,
        "packages": normalized_packages,
    }
    dependencies = {
        "format_version": FORMAT_VERSION,
        "source": "current working tree",
        "metadata_command": command_string(METADATA_COMMAND),
        "selection": "workspace-member Cargo dependencies whose metadata kind is null/normal",
        "packages": normal_dependency_packages,
    }
    return inventory, dependencies


def package_names_by_id(raw: dict[str, Any]) -> dict[str, str]:
    members = set(raw["workspace_members"])
    return {
        package["id"]: package["name"]
        for package in raw["packages"]
        if package["id"] in members
    }


def package_topology(raw: dict[str, Any]) -> dict[str, Any]:
    members = set(raw.get("workspace_members", []))
    packages = [
        package
        for package in raw.get("packages", [])
        if package.get("id") in members and package.get("name") == PACKAGE
    ]
    if len(packages) != 1:
        fail(
            f"expected exactly one workspace package named {PACKAGE}; "
            f"observed {len(packages)}"
        )
    targets = sorted(
        (
            {
                "name": target["name"],
                "kind": target["kind"],
            }
            for target in packages[0].get("targets", [])
        ),
        key=lambda item: (item["name"], item["kind"]),
    )
    integration_targets = [
        target["name"] for target in targets if target["kind"] == ["test"]
    ]
    return {
        "package": PACKAGE,
        "target_count": len(targets),
        "integration_test_target_count": len(integration_targets),
        "integration_test_targets": integration_targets,
        "targets": targets,
    }


def parse_artifacts(
    stdout: str,
    names_by_id: dict[str, str],
    target_dir: Path,
) -> list[dict[str, Any]]:
    artifacts: dict[str, dict[str, Any]] = {}
    for line in stdout.splitlines():
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        executable = message.get("executable")
        if message.get("reason") != "compiler-artifact" or not executable:
            continue
        if not message.get("profile", {}).get("test"):
            continue
        package = names_by_id.get(message.get("package_id"))
        if package is None:
            continue
        executable_path = Path(executable).resolve()
        try:
            normalized = "$CARGO_TARGET_DIR/" + executable_path.relative_to(
                target_dir.resolve()
            ).as_posix()
        except ValueError:
            fail(f"Cargo test executable escapes owned target directory: {executable}")
        identity = {
            "package": package,
            "target": message.get("target", {}).get("name"),
            "kind": message.get("target", {}).get("kind", []),
            "executable": normalized,
            "fresh": bool(message.get("fresh", False)),
            "_absolute": str(executable_path),
        }
        artifacts[str(executable_path)] = identity
    return sorted(
        artifacts.values(),
        key=lambda item: (item["package"], item["target"] or "", item["executable"]),
    )


def timed_compile(
    repo: Path,
    target_dir: Path,
    env: dict[str, str],
    names_by_id: dict[str, str],
    arm: str,
    phase: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    started = utc_now()
    before = time.perf_counter()
    result = run(COMPILE_COMMAND, repo, env=env, check=False)
    elapsed = time.perf_counter() - before
    if result.returncode != 0:
        sys.stderr.write(result.stderr)
        fail(f"{phase} {arm} compile failed with status {result.returncode}")
    artifacts = parse_artifacts(result.stdout, names_by_id, target_dir)
    package_artifacts = [item for item in artifacts if item["package"] == PACKAGE]
    if not package_artifacts:
        fail(f"{phase} {arm} compile emitted zero linked test executables for {PACKAGE}")
    stable_artifacts = [
        {key: value for key, value in item.items() if not key.startswith("_")}
        for item in package_artifacts
    ]
    measurement = {
        "phase": phase,
        "started_at_utc": started,
        "wall_time_seconds": round(elapsed, 6),
        "exit_code": result.returncode,
        "linked_test_executable_count": len(package_artifacts),
        "linked_test_executables": stable_artifacts,
    }
    return measurement, package_artifacts


def compile_arm(
    source_root: Path,
    target_dir: Path,
    arm: str,
) -> tuple[dict[str, Any], dict[str, str], dict[str, str]]:
    if target_dir.exists():
        fail(f"{arm} Cargo target directory was not fresh: {target_dir}")
    raw_metadata = json.loads(run(METADATA_COMMAND, source_root).stdout)
    topology = package_topology(raw_metadata)
    expected_targets = (
        EXPECTED_BASELINE_INTEGRATION_TARGETS
        if arm == "baseline"
        else EXPECTED_CANDIDATE_INTEGRATION_TARGETS
    )
    if topology["integration_test_target_count"] != expected_targets:
        fail(
            f"{arm} {PACKAGE} integration target count mismatch: "
            f"expected {expected_targets}, "
            f"observed {topology['integration_test_target_count']} "
            f"({topology['integration_test_targets']})"
        )
    names_by_id = package_names_by_id(raw_metadata)
    env = os.environ.copy()
    env["CARGO_TARGET_DIR"] = str(target_dir)
    env["CARGO_TERM_COLOR"] = "never"
    cold, cold_artifacts = timed_compile(
        source_root, target_dir, env, names_by_id, arm, "cold"
    )
    warm, warm_artifacts = timed_compile(
        source_root, target_dir, env, names_by_id, arm, "warm"
    )
    expected_linked = (
        EXPECTED_BASELINE_LINKED_TEST_EXECUTABLES
        if arm == "baseline"
        else EXPECTED_CANDIDATE_LINKED_TEST_EXECUTABLES
    )
    for run_measurement in (cold, warm):
        if run_measurement["linked_test_executable_count"] != expected_linked:
            fail(
                f"{run_measurement['phase']} {arm} linked test executable "
                f"count mismatch: expected {expected_linked}, observed "
                f"{run_measurement['linked_test_executable_count']}"
            )
    cold_identities = {
        (item["package"], item["target"], tuple(item["kind"]))
        for item in cold_artifacts
    }
    warm_identities = {
        (item["package"], item["target"], tuple(item["kind"]))
        for item in warm_artifacts
    }
    if cold_identities != warm_identities:
        fail(
            f"{arm} cold/warm linked test executable identities differ: "
            f"cold={sorted(cold_identities)}, warm={sorted(warm_identities)}"
        )
    return (
        {
            "topology": topology,
            "runs": [cold, warm],
            "linked_identity_sets_equal": True,
        },
        env,
        names_by_id,
    )


def collect_native_libtest(
    repo: Path,
    target_dir: Path,
    env: dict[str, str],
    names_by_id: dict[str, str],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    result = run(COLLECT_COMMAND, repo, env=env)
    artifacts = parse_artifacts(result.stdout, names_by_id, target_dir)
    if not artifacts:
        fail("workspace no-run collection emitted zero linked test executables")
    tests: list[dict[str, Any]] = []
    stable_executables: list[dict[str, Any]] = []
    for artifact in artifacts:
        executable = artifact["_absolute"]
        all_output = run([executable, "--list", "--format", "terse"], repo).stdout
        ignored_output = run(
            [executable, "--list", "--ignored", "--format", "terse"], repo
        ).stdout
        ignored = {
            line.rsplit(": ", 1)[0]
            for line in ignored_output.splitlines()
            if line.endswith(": test")
        }
        identity = {
            key: value
            for key, value in artifact.items()
            if key not in {"_absolute", "fresh"}
        }
        stable_executables.append(identity)
        for line in all_output.splitlines():
            if not line.endswith(": test"):
                continue
            name = line.rsplit(": ", 1)[0]
            tests.append({**identity, "name": name, "ignored": name in ignored})
    tests.sort(
        key=lambda item: (
            item["package"], item["target"] or "", item["name"], item["executable"]
        )
    )
    if not tests:
        fail("native libtest list mode returned zero tests")
    collection = {
        "format_version": FORMAT_VERSION,
        "source": "current working tree",
        "host": {
            "system": platform.system().lower(),
            "machine": platform.machine().lower(),
        },
        "compile_command": command_string(COLLECT_COMMAND),
        "collection_commands": [
            "$EXECUTABLE --list --format terse",
            "$EXECUTABLE --list --ignored --format terse",
        ],
        "method": "Cargo workspace no-run JSON artifacts followed by direct native libtest list modes; no test body is executed",
        "executable_count": len(stable_executables),
        "executables": stable_executables,
        "test_count": len(tests),
        "tests": tests,
    }
    return collection, artifacts


def repository_migration(
    repo: Path,
    inventory: dict[str, Any],
    collection: dict[str, Any],
) -> dict[str, Any]:
    baseline_source = load_baseline_source(repo)
    candidate_sources = load_candidate_sources(repo)
    baseline_functions = locate_functions({BASELINE_SOURCE: baseline_source})
    candidate_functions = locate_functions(candidate_sources)
    map_path = repo / NAME_MAP
    mapping_payload = json.loads(map_path.read_text())
    if mapping_payload.get("target") != TARGET:
        fail(f"repository name map target is not {TARGET}")
    mappings = mapping_payload.get("tests")
    if not isinstance(mappings, list) or len(mappings) != EXPECTED_REPOSITORY_TESTS:
        fail(
            f"repository name map must contain {EXPECTED_REPOSITORY_TESTS} tests; "
            f"observed {len(mappings) if isinstance(mappings, list) else 'invalid'}"
        )
    required_fields = {
        "old_fully_qualified_name",
        "new_fully_qualified_name",
        "leaf_name",
        "ignored",
    }
    for index, item in enumerate(mappings):
        if not isinstance(item, dict) or not required_fields <= item.keys():
            fail(f"repository name map entry {index} is invalid")
    for field in ("old_fully_qualified_name", "new_fully_qualified_name", "leaf_name"):
        values = [item[field] for item in mappings]
        if any(not isinstance(value, str) or not value for value in values):
            fail(f"repository name map contains invalid {field}")
        if len(values) != len(set(values)):
            fail(f"repository name map is not injective for {field}")
    if any(not isinstance(item["ignored"], bool) for item in mappings):
        fail("repository name map ignored states are not boolean")

    test_names = {item["leaf_name"] for item in mappings}
    baseline_attribute_count = len(TEST_ATTRIBUTE_RE.findall(baseline_source))
    candidate_attribute_count = sum(
        len(TEST_ATTRIBUTE_RE.findall(data)) for data in candidate_sources.values()
    )
    if baseline_attribute_count != EXPECTED_REPOSITORY_TESTS:
        fail(f"baseline source has {baseline_attribute_count} test attributes, expected 67")
    if candidate_attribute_count != EXPECTED_REPOSITORY_TESTS:
        fail(f"candidate sources have {candidate_attribute_count} test attributes, expected 67")
    if not test_names <= baseline_functions.keys():
        fail(f"mapped tests missing from baseline source: {sorted(test_names - baseline_functions.keys())}")
    if not test_names <= candidate_functions.keys():
        fail(f"mapped tests missing from candidate source: {sorted(test_names - candidate_functions.keys())}")

    support_names = set(baseline_functions) - test_names
    candidate_support_names = {
        name
        for name, entry in candidate_functions.items()
        if "/support/" in entry["path"]
    }
    if candidate_support_names != support_names:
        fail(
            "candidate support function set differs from baseline: "
            f"missing={sorted(support_names - candidate_support_names)}, "
            f"extra={sorted(candidate_support_names - support_names)}"
        )
    if set(candidate_functions) != set(baseline_functions):
        fail(
            "candidate function set differs from baseline: "
            f"missing={sorted(set(baseline_functions) - set(candidate_functions))}, "
            f"extra={sorted(set(candidate_functions) - set(baseline_functions))}"
        )

    baseline_collection_path = (
        repo
        / "docs/development/test-architecture/baseline-77c90d6"
        / "rust-libtest-darwin-arm64.json"
    )
    baseline_collection = json.loads(baseline_collection_path.read_text())
    baseline_target_tests = [
        item
        for item in baseline_collection["tests"]
        if item["package"] == PACKAGE and item["target"] == TARGET
    ]
    candidate_target_tests = [
        item
        for item in collection["tests"]
        if item["package"] == PACKAGE and item["target"] == TARGET
    ]
    baseline_executables = sorted({item["executable"] for item in baseline_target_tests})
    candidate_executables = sorted({item["executable"] for item in candidate_target_tests})
    if len(baseline_executables) != 1:
        fail(f"baseline {TARGET} has {len(baseline_executables)} linked executables")
    if len(candidate_executables) != 1:
        fail(f"candidate {TARGET} has {len(candidate_executables)} linked executables")

    baseline_by_name = {item["name"]: item for item in baseline_target_tests}
    candidate_by_name = {item["name"]: item for item in candidate_target_tests}
    expected_old = {item["old_fully_qualified_name"] for item in mappings}
    expected_new = {item["new_fully_qualified_name"] for item in mappings}
    if set(baseline_by_name) != expected_old:
        fail(
            "baseline native repository collection differs from old name map: "
            f"missing={sorted(expected_old - set(baseline_by_name))}, "
            f"extra={sorted(set(baseline_by_name) - expected_old)}"
        )
    if set(candidate_by_name) != expected_new:
        fail(
            "candidate native repository collection differs from new name map: "
            f"missing={sorted(expected_new - set(candidate_by_name))}, "
            f"extra={sorted(set(candidate_by_name) - expected_new)}"
        )

    repository_targets = [
        target
        for package in inventory["packages"]
        if package["name"] == PACKAGE
        for target in package["targets"]
        if target["name"] == TARGET and target["kind"] == ["test"]
    ]
    if len(repository_targets) != 1:
        fail(f"candidate metadata contains {len(repository_targets)} {TARGET} test targets")

    test_entries: list[dict[str, Any]] = []
    mismatches: list[dict[str, str]] = []
    for item in mappings:
        leaf = item["leaf_name"]
        old_function = baseline_functions[leaf]
        new_function = candidate_functions[leaf]
        old_body = old_function["body"]
        new_body = new_function["body"]
        old_contract = contract(old_body)
        new_contract = contract(new_body)
        mismatch_fields = [
            key for key in old_contract if old_contract[key] != new_contract[key]
        ]
        if old_function["signature_tokens"] != new_function["signature_tokens"]:
            mismatch_fields.append("signature_tokens")
        if old_function["attributes"] != new_function["attributes"]:
            mismatch_fields.append("function_attributes")
        old_native = baseline_by_name[item["old_fully_qualified_name"]]
        new_native = candidate_by_name[item["new_fully_qualified_name"]]
        if old_native["ignored"] != item["ignored"]:
            mismatch_fields.append("baseline_ignored")
        if new_native["ignored"] != item["ignored"]:
            mismatch_fields.append("candidate_ignored")
        if mismatch_fields:
            mismatches.append({"leaf_name": leaf, "fields": ",".join(mismatch_fields)})
        test_entries.append(
            {
                **item,
                "baseline_source_path": BASELINE_SOURCE,
                "candidate_source_path": candidate_functions[leaf]["path"],
                "body_contract": new_contract,
                "signature_contract_sha256": sha256(new_function["signature_tokens"]),
                "function_attribute_count": len(new_function["attributes"]),
                "function_attribute_contract_sha256": hash_sequence(
                    new_function["attributes"]
                ),
                "baseline_body_sha256": old_contract["body_sha256"],
                "candidate_body_sha256": new_contract["body_sha256"],
                "contract_match": not mismatch_fields,
                "baseline_native_observed": True,
                "candidate_native_observed": True,
            }
        )

    support_entries: list[dict[str, Any]] = []
    for name in sorted(support_names):
        old_function = baseline_functions[name]
        new_function = candidate_functions[name]
        old_body = old_function["body"]
        new_body = new_function["body"]
        old_contract = contract(old_body)
        new_contract = contract(new_body)
        mismatch_fields = []
        if old_contract != new_contract:
            mismatch_fields.append("body_contract")
        if old_function["signature_tokens"] != new_function["signature_tokens"]:
            mismatch_fields.append("signature_tokens")
        if old_function["attributes"] != new_function["attributes"]:
            mismatch_fields.append("function_attributes")
        matches = not mismatch_fields
        if mismatch_fields:
            mismatches.append(
                {"support_function": name, "fields": ",".join(mismatch_fields)}
            )
        support_entries.append(
            {
                "function": name,
                "baseline_source_path": BASELINE_SOURCE,
                "candidate_source_path": candidate_functions[name]["path"],
                "body_contract": new_contract,
                "signature_contract_sha256": sha256(new_function["signature_tokens"]),
                "function_attribute_count": len(new_function["attributes"]),
                "function_attribute_contract_sha256": hash_sequence(
                    new_function["attributes"]
                ),
                "baseline_body_sha256": old_contract["body_sha256"],
                "candidate_body_sha256": new_contract["body_sha256"],
                "contract_match": matches,
            }
        )

    if mismatches:
        fail(f"repository migration contract mismatches: {mismatches}")
    baseline_targets = json.loads(
        (
            repo
            / "docs/development/test-architecture/baseline-77c90d6/cargo-targets.json"
        ).read_text()
    )
    baseline_target_count = sum(
        len(package["targets"]) for package in baseline_targets["packages"]
    )
    candidate_source_files = [
        {
            "path": path,
            "bytes": len(data),
            "sha256": sha256(data),
        }
        for path, data in sorted(candidate_sources.items())
    ]
    return {
        "format_version": FORMAT_VERSION,
        "baseline_commit": BASELINE_COMMIT,
        "baseline_source_command": f"git show {BASELINE_REV}:{BASELINE_SOURCE}",
        "method": {
            "function_structure": "lex Rust comments/literals and balance delimiters; identify uniquely named fn declarations and bodies in baseline and candidate",
            "signature_equivalence": "SHA-256 over function-signature token bytes after normalizing only whitespace, a leading pub/pub(crate) visibility modifier, and an optional trailing comma in the outer parameter list",
            "attribute_equivalence": "length-prefixed SHA-256 sequence of exact contiguous Rust attributes preceding each function; test flavor/cfg/ignore attributes are not scaffolding",
            "body_equivalence": "SHA-256 over exact bytes from each function opening brace through its matched closing brace; imports and module declarations outside functions are excluded",
            "assertion_contract": "length-prefixed SHA-256 sequence of exact lexically identified assert*! macro invocation bytes inside each body",
            "embedded_fixture_contract": "length-prefixed SHA-256 sequence of exact Rust string and byte-string literal token bytes inside each body",
            "native_collection": "baseline committed Darwin libtest list and candidate native Darwin libtest list must exactly equal the old/new name-map domains",
        },
        "source_checksums": {
            "baseline_source": {
                "path": BASELINE_SOURCE,
                "bytes": len(baseline_source),
                "sha256": sha256(baseline_source),
            },
            "candidate_sources": candidate_source_files,
            "name_map": {
                "path": NAME_MAP.as_posix(),
                "bytes": map_path.stat().st_size,
                "sha256": sha256(map_path.read_bytes()),
            },
            "baseline_native_collection": {
                "path": baseline_collection_path.relative_to(repo).as_posix(),
                "sha256": sha256(baseline_collection_path.read_bytes()),
            },
        },
        "topology": {
            "baseline_workspace_target_count": baseline_target_count,
            "candidate_workspace_target_count": inventory["target_count"],
            "workspace_target_count_delta": inventory["target_count"] - baseline_target_count,
            "candidate_repository_target_count": len(repository_targets),
            "candidate_repository_target": repository_targets[0],
            "baseline_repository_linked_executable_count": len(baseline_executables),
            "candidate_repository_linked_executable_count": len(candidate_executables),
            "baseline_repository_linked_executables": baseline_executables,
            "candidate_repository_linked_executables": candidate_executables,
        },
        "bijection": {
            "expected_count": EXPECTED_REPOSITORY_TESTS,
            "map_count": len(mappings),
            "baseline_native_count": len(baseline_target_tests),
            "candidate_native_count": len(candidate_target_tests),
            "unique_old_names": len(expected_old),
            "unique_new_names": len(expected_new),
            "unique_leaf_names": len(test_names),
            "status": "pass",
        },
        "contract_equivalence": {
            "baseline_test_attributes": baseline_attribute_count,
            "candidate_test_attributes": candidate_attribute_count,
            "exact_test_body_matches": len(test_entries),
            "exact_support_function_body_matches": len(support_entries),
            "baseline_function_count": len(baseline_functions),
            "candidate_function_count": len(candidate_functions),
            "mismatch_count": len(mismatches),
            "mismatches": mismatches,
            "status": "pass",
        },
        "tests": test_entries,
        "support_functions": support_entries,
    }


def tool_versions(repo: Path) -> dict[str, str]:
    cargo = run(["cargo", "--version"], repo).stdout.strip()
    rustc_verbose = run(["rustc", "-vV"], repo).stdout
    rustc_lines = rustc_verbose.splitlines()
    host_lines = [line.split(":", 1)[1].strip() for line in rustc_lines if line.startswith("host:")]
    if len(host_lines) != 1:
        fail("rustc -vV did not report exactly one host target")
    release_lines = [
        line.split(":", 1)[1].strip()
        for line in rustc_lines
        if line.startswith("release:")
    ]
    if len(release_lines) != 1:
        fail("rustc -vV did not report exactly one release")
    return {
        "cargo": cargo,
        "rustc": f"rustc {release_lines[0]}",
        "rustc_host_target": host_lines[0],
        "python": platform.python_version(),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--output",
        type=Path,
        help="default: docs/development/test-architecture/candidate-issue-477 under repo root",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    repo = args.repo_root.resolve()
    if not (repo / "Cargo.toml").is_file():
        fail(f"not a Moraine repository root: {repo}")
    system = platform.system().lower()
    machine = platform.machine().lower()
    if system != "darwin":
        fail(f"this evidence run requires native Darwin; observed {system}-{machine}")
    output = (
        args.output.resolve()
        if args.output
        else repo / "docs/development/test-architecture/candidate-issue-477"
    )
    expected_output = repo / "docs/development/test-architecture/candidate-issue-477"
    if output != expected_output:
        fail(f"candidate evidence output must be {expected_output}; got {output}")
    output.mkdir(parents=True, exist_ok=True)

    versions = tool_versions(repo)
    inventory, normal_dependencies = normalized_metadata(repo)

    policy_result = run(POLICY_COMMAND, repo, check=False)
    normal_dependencies["dependency_policy"] = {
        "command": command_string(POLICY_COMMAND),
        "exit_code": policy_result.returncode,
        "result": "pass" if policy_result.returncode == 0 else "fail",
        "stdout": policy_result.stdout.splitlines(),
        "stderr": policy_result.stderr.splitlines(),
        "reference": "Makefile dependency-policy target; direct enforcement command only (not its unit-test preflight)",
    }
    if policy_result.returncode != 0:
        sys.stderr.write(policy_result.stderr)
        fail(f"dependency policy failed with status {policy_result.returncode}")

    baseline_identity = authenticate_baseline(repo)
    candidate_commit = run(
        ["git", "rev-parse", "--verify", "HEAD^{commit}"], repo
    ).stdout.strip()
    candidate_tree = run(
        ["git", "rev-parse", "--verify", "HEAD^{tree}"], repo
    ).stdout.strip()

    owned_root = Path(
        tempfile.mkdtemp(prefix="moraine-issue-477-compile-", dir=tempfile.gettempdir())
    ).resolve()
    try:
        owned_root.relative_to(repo)
    except ValueError:
        pass
    else:
        shutil.rmtree(owned_root)
        fail(f"verifier-owned temporary root is inside candidate tree: {owned_root}")
    baseline_source = owned_root / "baseline-source"
    baseline_target = owned_root / "baseline-target"
    candidate_target = owned_root / "candidate-target"
    baseline_archive = owned_root / "baseline.tar"
    owned_cleanup = {
        "baseline_source": "not_attempted",
        "baseline_archive": "not_attempted",
        "baseline_target": "not_attempted",
        "candidate_target": "not_attempted",
        "temporary_root": "not_attempted",
    }
    try:
        baseline_source, materialization = materialize_baseline(
            repo, owned_root, baseline_identity
        )
        baseline_measurement, _, _ = compile_arm(
            baseline_source, baseline_target, "baseline"
        )
        candidate_measurement, candidate_env, candidate_names_by_id = compile_arm(
            repo, candidate_target, "candidate"
        )
        collection, _ = collect_native_libtest(
            repo, candidate_target, candidate_env, candidate_names_by_id
        )
        migration = repository_migration(repo, inventory, collection)
    finally:
        if owned_root.exists():
            shutil.rmtree(owned_root)
        owned_cleanup = {
            "baseline_archive": (
                "removed" if not baseline_archive.exists() else "failed"
            ),
            "baseline_source": (
                "removed" if not baseline_source.exists() else "failed"
            ),
            "baseline_target": (
                "removed" if not baseline_target.exists() else "failed"
            ),
            "candidate_target": (
                "removed" if not candidate_target.exists() else "failed"
            ),
            "temporary_root": "removed" if not owned_root.exists() else "failed",
        }
    if set(owned_cleanup.values()) != {"removed"}:
        fail(f"failed to remove verifier-owned compile resources: {owned_cleanup}")

    baseline_cold, baseline_warm = baseline_measurement["runs"]
    candidate_cold, candidate_warm = candidate_measurement["runs"]
    measurement = {
        "format_version": FORMAT_VERSION,
        "comparison": "same-host same-toolchain baseline versus candidate target topology",
        "same_host_toolchain": True,
        "host": {
            "system": system,
            "release": platform.release(),
            "machine": machine,
            "rustc_host_target": versions["rustc_host_target"],
        },
        "tool_versions": versions,
        "command": command_string(COMPILE_COMMAND),
        "measurement_order": ["baseline", "candidate"],
        "path_placeholders": {
            "$BASELINE_ARCHIVE": "verifier-owned archive outside the candidate tree",
            "$BASELINE_SOURCE_DIR": "verifier-owned extracted source outside the candidate tree",
            "$BASELINE_OWNED_FRESH_TARGET": "fresh isolated baseline Cargo target directory",
            "$CANDIDATE_OWNED_FRESH_TARGET": "fresh isolated candidate Cargo target directory",
        },
        "environment": {
            "baseline": {
                "CARGO_TARGET_DIR": "$BASELINE_OWNED_FRESH_TARGET",
                "CARGO_TERM_COLOR": "never",
            },
            "candidate": {
                "CARGO_TARGET_DIR": "$CANDIDATE_OWNED_FRESH_TARGET",
                "CARGO_TERM_COLOR": "never",
            },
        },
        "cold_definition": "first invocation after creating an empty isolated per-arm CARGO_TARGET_DIR; shared Cargo registry/git caches are host state",
        "warm_definition": "second identical invocation in the same per-arm CARGO_TARGET_DIR with no intervening source or environment change",
        "sources": {
            "baseline": {
                **baseline_identity,
                **materialization,
                "description": "authenticated git archive materialized outside the candidate tree",
                "authentication": "pass",
            },
            "candidate": {
                "description": "current working tree",
                "head_commit": candidate_commit,
                "head_tree": candidate_tree,
                "commit_command": "git rev-parse --verify HEAD^{commit}",
                "tree_command": "git rev-parse --verify HEAD^{tree}",
            },
        },
        "arms": {
            "baseline": baseline_measurement,
            "candidate": candidate_measurement,
        },
        "delta": {
            "package_target_count": (
                candidate_measurement["topology"]["target_count"]
                - baseline_measurement["topology"]["target_count"]
            ),
            "integration_test_target_count": (
                candidate_measurement["topology"]["integration_test_target_count"]
                - baseline_measurement["topology"]["integration_test_target_count"]
            ),
            "linked_test_executable_count": (
                candidate_cold["linked_test_executable_count"]
                - baseline_cold["linked_test_executable_count"]
            ),
            "cold_wall_time_seconds": round(
                candidate_cold["wall_time_seconds"]
                - baseline_cold["wall_time_seconds"],
                6,
            ),
            "warm_wall_time_seconds": round(
                candidate_warm["wall_time_seconds"]
                - baseline_warm["wall_time_seconds"],
                6,
            ),
        },
        "owned_cleanup": owned_cleanup,
        "non_deterministic_fields": [
            "arms.baseline.runs[0].started_at_utc",
            "arms.baseline.runs[0].wall_time_seconds",
            "arms.baseline.runs[1].started_at_utc",
            "arms.baseline.runs[1].wall_time_seconds",
            "arms.candidate.runs[0].started_at_utc",
            "arms.candidate.runs[0].wall_time_seconds",
            "arms.candidate.runs[1].started_at_utc",
            "arms.candidate.runs[1].wall_time_seconds",
            "delta.cold_wall_time_seconds",
            "delta.warm_wall_time_seconds",
        ],
    }

    host_artifact = f"rust-libtest-{system}-{machine}.json"
    write_json(output / "cargo-targets.json", inventory)
    write_json(output / host_artifact, collection)
    write_json(output / "repository-migration.json", migration)
    write_json(output / f"compile-measurements-{system}-{machine}.json", measurement)
    write_json(output / "normal-dependencies.json", normal_dependencies)

    artifact_names = sorted(
        [
            "cargo-targets.json",
            host_artifact,
            "repository-migration.json",
            f"compile-measurements-{system}-{machine}.json",
            "normal-dependencies.json",
        ]
    )
    artifact_checksums = {
        name: sha256((output / name).read_bytes()) for name in artifact_names
    }
    manifest = {
        "format_version": FORMAT_VERSION,
        "baseline_commit": BASELINE_COMMIT,
        "baseline_tree": baseline_identity["tree"],
        "candidate_head_commit": candidate_commit,
        "candidate_head_tree": candidate_tree,
        "source": "current working tree",
        "generated_at_utc": utc_now(),
        "generator": "docs/development/test-architecture/verify_candidate.py",
        "generator_sha256": sha256(Path(__file__).read_bytes()),
        "host": {
            "system": system,
            "release": platform.release(),
            "machine": machine,
            "rustc_host_target": versions["rustc_host_target"],
        },
        "tool_versions": versions,
        "commands": {
            "metadata": command_string(METADATA_COMMAND),
            "baseline_authenticate_commit": baseline_identity[
                "commit_authentication_command"
            ],
            "baseline_authenticate_tree": baseline_identity[
                "tree_authentication_command"
            ],
            "baseline_archive": materialization["archive_command"],
            "baseline_extract": materialization["extract_command"],
            "baseline_source": f"git show {BASELINE_REV}:{BASELINE_SOURCE}",
            "baseline_cold_compile": command_string(COMPILE_COMMAND),
            "baseline_warm_compile": command_string(COMPILE_COMMAND),
            "candidate_cold_compile": command_string(COMPILE_COMMAND),
            "candidate_warm_compile": command_string(COMPILE_COMMAND),
            "native_collection_compile": command_string(COLLECT_COMMAND),
            "native_collection_list": "$EXECUTABLE --list --format terse",
            "native_collection_ignored_list": "$EXECUTABLE --list --ignored --format terse",
            "dependency_policy": command_string(POLICY_COMMAND),
        },
        "generated_artifacts": artifact_names,
        "artifact_sha256": artifact_checksums,
        "summary": {
            "candidate_workspace_target_count": inventory["target_count"],
            "candidate_repository_target_count": migration["topology"]["candidate_repository_target_count"],
            "candidate_repository_linked_executable_count": migration["topology"]["candidate_repository_linked_executable_count"],
            "repository_test_bijection_count": migration["bijection"]["map_count"],
            "repository_contract_mismatch_count": migration["contract_equivalence"]["mismatch_count"],
            "baseline_source_tree": baseline_identity["tree"],
            "baseline_package_target_count": baseline_measurement["topology"][
                "target_count"
            ],
            "baseline_integration_test_target_count": baseline_measurement["topology"][
                "integration_test_target_count"
            ],
            "baseline_cold_wall_time_seconds": baseline_cold["wall_time_seconds"],
            "baseline_warm_wall_time_seconds": baseline_warm["wall_time_seconds"],
            "baseline_cold_linked_test_executable_count": baseline_cold[
                "linked_test_executable_count"
            ],
            "baseline_warm_linked_test_executable_count": baseline_warm[
                "linked_test_executable_count"
            ],
            "candidate_package_target_count": candidate_measurement["topology"][
                "target_count"
            ],
            "candidate_integration_test_target_count": candidate_measurement["topology"][
                "integration_test_target_count"
            ],
            "candidate_cold_wall_time_seconds": candidate_cold["wall_time_seconds"],
            "candidate_warm_wall_time_seconds": candidate_warm["wall_time_seconds"],
            "candidate_cold_linked_test_executable_count": candidate_cold[
                "linked_test_executable_count"
            ],
            "candidate_warm_linked_test_executable_count": candidate_warm[
                "linked_test_executable_count"
            ],
            "package_target_count_delta": measurement["delta"][
                "package_target_count"
            ],
            "integration_test_target_count_delta": measurement["delta"][
                "integration_test_target_count"
            ],
            "linked_test_executable_count_delta": measurement["delta"][
                "linked_test_executable_count"
            ],
            "cold_wall_time_seconds_delta": measurement["delta"][
                "cold_wall_time_seconds"
            ],
            "warm_wall_time_seconds_delta": measurement["delta"][
                "warm_wall_time_seconds"
            ],
            "dependency_policy": normal_dependencies["dependency_policy"]["result"],
            "owned_compile_cleanup": owned_cleanup,
        },
        "non_deterministic_fields": [
            "generated_at_utc",
            "summary.baseline_cold_wall_time_seconds",
            "summary.baseline_warm_wall_time_seconds",
            "summary.candidate_cold_wall_time_seconds",
            "summary.candidate_warm_wall_time_seconds",
            "summary.cold_wall_time_seconds_delta",
            "summary.warm_wall_time_seconds_delta",
            f"artifact_sha256.compile-measurements-{system}-{machine}.json",
        ],
        "reproduce": "python3 docs/development/test-architecture/verify_candidate.py --repo-root .",
    }
    write_json(output / "manifest.json", manifest)
    print(
        "candidate-evidence "
        f"targets={inventory['target_count']} "
        f"repository_tests={migration['bijection']['map_count']} "
        f"mismatches={migration['contract_equivalence']['mismatch_count']} "
        f"baseline={baseline_cold['wall_time_seconds']:.6f}/"
        f"{baseline_warm['wall_time_seconds']:.6f}s "
        f"candidate={candidate_cold['wall_time_seconds']:.6f}/"
        f"{candidate_warm['wall_time_seconds']:.6f}s "
        f"linked={baseline_cold['linked_test_executable_count']}->"
        f"{candidate_cold['linked_test_executable_count']} "
        "status=pass"
    )


if __name__ == "__main__":
    main()
