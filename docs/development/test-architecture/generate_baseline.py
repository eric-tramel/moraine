#!/usr/bin/env python3
"""Generate issue #477 migration evidence from an exact Moraine checkout.

This is an inventory generator, not a test runner. Its default mode executes only
`cargo metadata`; `--collect-libtest` additionally compiles test harnesses and
invokes their list modes without executing test bodies. Run it on each supported
host from a clean checkout of the recorded source commit.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

BASELINE_COMMIT = "77c90d6c572ba71bcfaa0362f7ead2cfa5e6712a"
BASELINE_SHORT = "77c90d6"
BASELINE_TREE = "0ae5df87300c6748161f2035394b6e0b9a92e627"
FORMAT_VERSION = 1


def fail(message: str) -> "NoReturn":
    raise SystemExit(message)


def run(
    command: list[str],
    cwd: Path,
    *,
    env: dict[str, str] | None = None,
    timeout_seconds: int = 300,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            cwd=cwd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
            timeout=timeout_seconds,
        )
    except FileNotFoundError as error:
        fail(f"required command not found: {command[0]} ({error})")
    except subprocess.TimeoutExpired:
        fail(
            f"command exceeded {timeout_seconds}s timeout: "
            f"{' '.join(command)}"
        )
    except subprocess.CalledProcessError as error:
        sys.stderr.write(error.stderr)
        fail(f"command failed ({error.returncode}): {' '.join(command)}")


def relative(path: str, root: Path) -> str:
    resolved = Path(path).resolve()
    try:
        return resolved.relative_to(root.resolve()).as_posix()
    except ValueError:
        fail(f"metadata path escapes repository root: {resolved}")


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def git_object_hash(kind: str, data: bytes) -> bytes:
    header = f"{kind} {len(data)}\0".encode()
    return hashlib.sha1(header + data).digest()


def git_tree_hash(root: Path) -> str:
    """Compute Git's tree object hash without requiring a `.git` directory."""

    def hash_directory(directory: Path) -> bytes:
        entries: list[tuple[bytes, bytes]] = []
        for path in directory.iterdir():
            if path.name == ".git":
                continue
            name = path.name.encode()
            if path.is_symlink():
                mode = b"120000"
                object_hash = git_object_hash("blob", os.readlink(path).encode())
                sort_key = name
            elif path.is_dir():
                mode = b"40000"
                object_hash = hash_directory(path)
                sort_key = name + b"/"
            elif path.is_file():
                mode = b"100755" if path.stat().st_mode & 0o111 else b"100644"
                object_hash = git_object_hash("blob", path.read_bytes())
                sort_key = name
            else:
                fail(f"unsupported filesystem entry in baseline tree: {path}")
            entries.append((sort_key, mode + b" " + name + b"\0" + object_hash))
        payload = b"".join(entry for _, entry in sorted(entries, key=lambda item: item[0]))
        return git_object_hash("tree", payload)

    return hash_directory(root).hex()


def normalize_metadata(repo: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    result = run(
        ["cargo", "metadata", "--format-version", "1", "--locked", "--no-deps"],
        repo,
    )
    metadata = json.loads(result.stdout)
    if metadata.get("version") != 1:
        fail(f"unsupported Cargo metadata version: {metadata.get('version')!r}")
    members = metadata.get("workspace_members")
    packages = metadata.get("packages")
    if not isinstance(members, list) or not members:
        fail("Cargo metadata returned no workspace members")
    if not isinstance(packages, list):
        fail("Cargo metadata packages is not a list")
    by_id: dict[str, dict[str, Any]] = {}
    for package in packages:
        package_id = package.get("id")
        if not isinstance(package_id, str) or not package_id:
            fail("Cargo metadata package has no valid id")
        if package_id in by_id:
            fail(f"duplicate Cargo package id: {package_id}")
        by_id[package_id] = package
    missing = [member for member in members if member not in by_id]
    if missing:
        fail(f"workspace members missing from packages: {missing}")
    selected = [by_id[member] for member in members]
    names = [package.get("name") for package in selected]
    if any(not isinstance(name, str) or not name for name in names):
        fail("workspace member has no valid package name")
    if len(names) != len(set(names)):
        fail(f"workspace package names are ambiguous: {names}")

    normalized_packages = []
    for package in selected:
        targets = []
        for target in package.get("targets", []):
            targets.append(
                {
                    "name": target["name"],
                    "kind": target["kind"],
                    "crate_types": target["crate_types"],
                    "src_path": relative(target["src_path"], repo),
                    "test": target["test"],
                    "doctest": target["doctest"],
                    "bench": target.get("bench", False),
                }
            )
        targets.sort(key=lambda item: (item["name"], item["kind"]))
        normalized_packages.append(
            {
                "name": package["name"],
                "version": package["version"],
                "manifest_path": relative(package["manifest_path"], repo),
                "targets": targets,
            }
        )
    normalized_packages.sort(key=lambda item: item["name"])
    inventory = {
        "format_version": FORMAT_VERSION,
        "source_commit": BASELINE_COMMIT,
        "packages": normalized_packages,
    }
    package_support = {
        "format_version": FORMAT_VERSION,
        "source_commit": BASELINE_COMMIT,
        "selection": "Cargo metadata workspace_members, sorted by unique package name",
        "invariants": [
            "metadata format version is 1",
            "every workspace member resolves exactly once",
            "workspace package names are nonempty and unique",
            "zero selected packages is an error",
            "every generated Cargo command must exit zero",
        ],
        "commands": [
            {
                "package": name,
                "compile_only": ["cargo", "test", "-p", name, "--locked", "--no-run"],
                "execute": ["cargo", "test", "-p", name, "--locked"],
            }
            for name in sorted(names)
        ],
    }
    return inventory, package_support


def rust_static_inventory(repo: Path, metadata: dict[str, Any]) -> dict[str, Any]:
    roots = {Path(package["manifest_path"]).parent for package in metadata["packages"]}
    integration_files: set[Path] = set()
    source_files: set[Path] = set()
    ignored: list[dict[str, Any]] = []
    attribute_re = re.compile(r"#\s*\[\s*(?:tokio::)?test(?:\s*\([^]]*\))?\s*]")
    ignore_re = re.compile(r"#\s*\[\s*ignore(?:\s*=\s*\"([^\"]*)\")?\s*]")
    function_re = re.compile(r"(?:async\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(")
    source_local_count = 0
    integration_count = 0
    for package_root in sorted(roots):
        absolute_root = repo / package_root
        for path in sorted(absolute_root.rglob("*.rs")):
            relative_path = path.relative_to(repo)
            if any(part in {"target", ".git"} for part in relative_path.parts):
                continue
            text = path.read_text(errors="strict")
            count = len(attribute_re.findall(text))
            if not count:
                continue
            if "tests" in relative_path.parts and relative_path.parts.index("tests") > 0:
                integration_count += count
                integration_files.add(relative_path)
            else:
                source_local_count += count
                source_files.add(relative_path)
            lines = text.splitlines()
            for index, line in enumerate(lines):
                ignore_match = ignore_re.search(line)
                if not ignore_match:
                    continue
                for candidate in lines[index + 1 : index + 8]:
                    name_match = function_re.search(candidate)
                    if name_match:
                        ignored.append(
                            {
                                "path": relative_path.as_posix(),
                                "function": name_match.group(1),
                                "reason": ignore_match.group(1),
                            }
                        )
                        break
    return {
        "format_version": FORMAT_VERSION,
        "source_commit": BASELINE_COMMIT,
        "method": "static Rust test-attribute scan; not an executed-case equivalence oracle",
        "source_local_attributes": source_local_count,
        "source_local_files": len(source_files),
        "prd_reported_source_local_files": 50,
        "integration_attributes": integration_count,
        "integration_files": len(integration_files),
        "prd_reported_total_files": 60,
        "total_attributes": source_local_count + integration_count,
        "ignored_functions": ignored,
        "merged_pr_476_runtime_evidence": {
            "command": "cargo test --workspace --locked",
            "passed": 720,
            "ignored": 2,
            "reported_suites": 32,
            "source": "https://github.com/eric-tramel/moraine/pull/476",
        },
        "warning": "Static attributes and expanded libtest runtime cases are intentionally not compared as counts.",
    }


def normalized_command(
    executable: Path, arguments: list[str], cwd: Path, repo: Path, timeout_seconds: int
) -> dict[str, Any]:
    return {
        "argv": [executable.name, *arguments],
        "cwd": cwd.relative_to(repo).as_posix(),
        "timeout_seconds": timeout_seconds,
    }


def collect_vitest(repo: Path, node_bin: Path) -> dict[str, Any]:
    cwd = repo / "web/monitor"
    executable = node_bin / "vitest"
    arguments = ["run", "--config", "vitest.config.ts", "--reporter=json"]
    result = run([str(executable), *arguments], cwd, timeout_seconds=300)
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        fail(f"Vitest returned invalid JSON: {error}")
    cases: list[dict[str, Any]] = []
    for test_file in payload.get("testResults", []):
        path = relative(test_file.get("name", ""), repo)
        for case in test_file.get("assertionResults", []):
            ancestors = case.get("ancestorTitles")
            title = case.get("title")
            status = case.get("status")
            if (
                not isinstance(ancestors, list)
                or not all(isinstance(value, str) for value in ancestors)
                or not isinstance(title, str)
                or not title
            ):
                fail("Vitest returned a case without qualified suite and case names")
            full_name = " > ".join([*ancestors, title])
            if status not in {"passed", "failed", "pending", "skipped", "todo"}:
                fail(f"Vitest returned unsupported case status: {status!r}")
            cases.append(
                {
                    "name": full_name,
                    "path": path,
                    "qualified_name": f"{path}::{full_name}",
                    "result": status,
                    "state": (
                        "skipped"
                        if status in {"pending", "skipped", "todo"}
                        else "selected"
                    ),
                }
            )
    if not cases:
        fail("Vitest native execution returned zero cases")
    cases.sort(key=lambda item: item["qualified_name"])
    return {
        "collector": "vitest-json-reporter",
        "mode": "executed",
        "command": normalized_command(executable, arguments, cwd, repo, 300),
        "case_total": len(cases),
        "selected": sum(item["state"] == "selected" for item in cases),
        "skipped": sum(item["state"] == "skipped" for item in cases),
        "cases": cases,
    }


def collect_playwright(repo: Path, node_bin: Path) -> dict[str, Any]:
    cwd = repo / "web/monitor"
    executable = node_bin / "playwright"
    arguments = [
        "test",
        "--config",
        "playwright.config.cjs",
        "--list",
        "--reporter=json",
    ]
    result = run([str(executable), *arguments], cwd, timeout_seconds=300)
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        fail(f"Playwright returned invalid JSON: {error}")
    cases: list[dict[str, Any]] = []

    def visit(suite: dict[str, Any], ancestors: list[str]) -> None:
        title = suite.get("title")
        nested_ancestors = ancestors
        if isinstance(title, str) and title and not title.endswith((".spec.ts", ".test.ts")):
            nested_ancestors = [*ancestors, title]
        for spec in suite.get("specs", []):
            spec_title = spec.get("title")
            path_value = spec.get("file")
            if not isinstance(spec_title, str) or not isinstance(path_value, str):
                fail("Playwright returned a spec without a title or file")
            path = f"web/monitor/e2e/{path_value}"
            title_parts = [*nested_ancestors, spec_title]
            for expanded in spec.get("tests", []):
                project = expanded.get("projectName") or expanded.get("projectId")
                expected = expanded.get("expectedStatus")
                if not isinstance(project, str) or not project:
                    fail("Playwright returned an expanded case without a project")
                if not isinstance(expected, str) or not expected:
                    fail("Playwright returned an expanded case without expectedStatus")
                name = " > ".join(title_parts)
                cases.append(
                    {
                        "expected_status": expected,
                        "name": name,
                        "path": path,
                        "project": project,
                        "qualified_name": f"{project}::{path}::{name}",
                        "state": "skipped" if expected == "skipped" else "selected",
                    }
                )
        for child in suite.get("suites", []):
            visit(child, nested_ancestors)

    for suite in payload.get("suites", []):
        visit(suite, [])
    if payload.get("errors"):
        fail(f"Playwright list mode reported collection errors: {payload['errors']}")
    if not cases:
        fail("Playwright native list mode returned zero expanded cases")
    cases.sort(key=lambda item: item["qualified_name"])
    return {
        "collector": "playwright-json-reporter",
        "mode": "collected",
        "command": normalized_command(executable, arguments, cwd, repo, 300),
        "case_total": len(cases),
        "selected": sum(item["state"] == "selected" for item in cases),
        "skipped": sum(item["state"] == "skipped" for item in cases),
        "cases": cases,
    }


PYTEST_COLLECTOR_PLUGIN = """
import json
import os
import unittest
from pathlib import Path

from _pytest.skipping import evaluate_skip_marks


def pytest_collection_finish(session):
    cases = []
    root = Path(os.environ["MORAINE_REPO_ROOT"]).resolve()
    for item in session.items:
        source = Path(item.path).resolve().relative_to(root).as_posix()
        _, separator, suffix = item.nodeid.partition("::")
        qualified_name = source if not separator else f"{source}::{suffix}"
        skipped = evaluate_skip_marks(item) is not None
        framework = "unittest" if item.cls and issubclass(item.cls, unittest.TestCase) else "pytest"
        cases.append({
            "framework": framework,
            "qualified_name": qualified_name,
            "state": "skipped" if skipped else "selected",
        })
    with open(os.environ["MORAINE_PYTEST_COLLECTION"], "w", encoding="utf-8") as handle:
        json.dump(cases, handle)
"""


def collect_python_tests(repo: Path, pytest_executable: Path) -> dict[str, Any]:
    targets = [
        "bindings/python/moraine_conversations/tests/test_smoke.py",
        "scripts/ci/test_dependency_policy.py",
    ]
    arguments = [
        "--collect-only",
        "-q",
        "-p",
        "no:cacheprovider",
        *targets,
        "-p",
        "issue477_pytest_collector",
    ]
    with tempfile.TemporaryDirectory(prefix="moraine-pytest-collector-") as temporary:
        plugin_root = Path(temporary)
        (plugin_root / "issue477_pytest_collector.py").write_text(
            PYTEST_COLLECTOR_PLUGIN
        )
        collection_path = plugin_root / "collection.json"
        environment = os.environ.copy()
        environment["MORAINE_PYTEST_COLLECTION"] = str(collection_path)
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        environment["MORAINE_REPO_ROOT"] = str(repo)
        prior_pythonpath = environment.get("PYTHONPATH")
        environment["PYTHONPATH"] = (
            str(plugin_root)
            if not prior_pythonpath
            else os.pathsep.join((str(plugin_root), prior_pythonpath))
        )
        run(
            [str(pytest_executable), *arguments],
            repo,
            env=environment,
            timeout_seconds=300,
        )
        try:
            cases = json.loads(collection_path.read_text())
        except (FileNotFoundError, json.JSONDecodeError) as error:
            fail(f"pytest collector did not produce valid JSON: {error}")
    if not isinstance(cases, list) or not cases:
        fail("pytest native collection returned zero expanded cases")
    for case in cases:
        if (
            not isinstance(case, dict)
            or case.get("framework") not in {"pytest", "unittest"}
            or case.get("state") not in {"selected", "skipped"}
            or not isinstance(case.get("qualified_name"), str)
        ):
            fail(f"pytest returned an invalid collected case: {case!r}")
    cases.sort(key=lambda item: item["qualified_name"])
    return {
        "collector": "pytest-collection-hook",
        "mode": "collected",
        "command": normalized_command(
            pytest_executable, arguments, repo, repo, 300
        ),
        "case_total": len(cases),
        "selected": sum(item["state"] == "selected" for item in cases),
        "skipped": sum(item["state"] == "skipped" for item in cases),
        "cases": cases,
    }


def ecosystem_collections(
    repo: Path, node_bin: Path, pytest_executable: Path
) -> dict[str, Any]:
    if not node_bin.is_dir():
        fail(f"Node binary directory does not exist: {node_bin}")
    for name in ("vitest", "playwright"):
        if not (node_bin / name).is_file():
            fail(f"required native collector is missing: {node_bin / name}")
    if not pytest_executable.is_file():
        fail(f"pytest executable does not exist: {pytest_executable}")
    monitor_modules = repo / "web/monitor/node_modules"
    if monitor_modules.exists() or monitor_modules.is_symlink():
        fail("authenticated source must not already contain web/monitor/node_modules")
    monitor_modules.symlink_to(node_bin.parent, target_is_directory=True)
    try:
        vitest = collect_vitest(repo, node_bin)
        playwright = collect_playwright(repo, node_bin)
    finally:
        monitor_modules.unlink(missing_ok=True)
    python = collect_python_tests(repo, pytest_executable)
    if git_tree_hash(repo) != BASELINE_TREE:
        fail("native collectors modified the authenticated baseline source tree")
    return {
        "format_version": FORMAT_VERSION,
        "source_commit": BASELINE_COMMIT,
        "collection_method": "native framework collection and execution output",
        "frontend_vitest": vitest,
        "browser_playwright": playwright,
        "python_pytest_unittest": python,
    }


def fixture_inventory(repo: Path) -> tuple[dict[str, Any], str]:
    families = {
        "raw_harness_inputs": repo / "fixtures",
        "source_normalization_goldens": repo
        / "crates/moraine-ingest-core/tests/goldens/source_normalization",
        "analytics_schema_cli_golden": repo
        / "apps/moraine/src/commands/analytics_schema_v1.golden.json",
    }
    payload_families: dict[str, Any] = {}
    tsv_lines = ["family\tpath\tsha256\tbytes"]
    for family, root in families.items():
        if not root.exists():
            fail(f"fixture family is missing: {root}")
        entries = []
        family_hasher = hashlib.sha256()
        family_root = root.parent if root.is_file() else root
        paths = [root] if root.is_file() else sorted(item for item in root.rglob("*") if item.is_file())
        for path in paths:
            relative_to_family = path.relative_to(family_root).as_posix()
            data = path.read_bytes()
            digest = sha256_bytes(data)
            family_hasher.update(relative_to_family.encode())
            family_hasher.update(b"\0")
            family_hasher.update(data)
            entry = {
                "path": path.relative_to(repo).as_posix(),
                "sha256": digest,
                "bytes": len(data),
            }
            entries.append(entry)
            tsv_lines.append(f"{family}\t{entry['path']}\t{digest}\t{len(data)}")
        payload_families[family] = {
            "root": root.relative_to(repo).as_posix(),
            "files": entries,
            "file_count": len(entries),
            "family_sha256": family_hasher.hexdigest(),
            "family_hash_contract": "sha256(relative POSIX path + NUL + bytes, sorted by path)",
        }
    return (
        {
            "format_version": FORMAT_VERSION,
            "source_commit": BASELINE_COMMIT,
            "families": payload_families,
        },
        "\n".join(tsv_lines) + "\n",
    )


def e2e_phases(repo: Path) -> dict[str, Any]:
    sources = [Path("scripts/ci/e2e-stack.sh"), Path("scripts/ci/e2e-install-artifact.sh")]
    allowed_starts = (
        "installing ",
        "ensuring ",
        "bootstrapping ",
        "starting ",
        "checking ",
        "waiting ",
        "cursor sqlite live update",
        "killing ",
        "comparing ",
        "stopping ",
        "packaging ",
        "running ",
    )
    phases = []
    echo_re = re.compile(r'echo\s+"\[(e2e(?:-install)?)\]\s+([^\"]+)"')
    for source in sources:
        for line_number, line in enumerate((repo / source).read_text().splitlines(), start=1):
            match = echo_re.search(line)
            if not match or ">&2" in line:
                continue
            label = match.group(2)
            if not label.startswith(allowed_starts):
                continue
            phases.append(
                {
                    "source": source.as_posix(),
                    "line": line_number,
                    "channel": match.group(1),
                    "label": label,
                    "conditional": "RUN_REPLAY_BENCH_SMOKE" in label or "replay benchmark" in label,
                }
            )
    return {
        "format_version": FORMAT_VERSION,
        "source_commit": BASELINE_COMMIT,
        "method": "ordered non-diagnostic phase echo statements from canonical shell harnesses",
        "phases": phases,
    }


def benchmark_scenarios(repo: Path) -> dict[str, Any]:
    scenarios = [
        {
            "benchmark_id": "analytics-latency",
            "scenario_ids": ["monitor-vs-repository"],
            "producer": "crates/moraine-conversations/tests/analytics_benchmark.rs",
            "baseline_class": "ignored Cargo integration test (timing mixed with helpers/live semantics)",
        },
        {
            "benchmark_id": "central-mcp-resource",
            "scenario_ids": ["embedded-vs-central"],
            "producer": "scripts/bench/central_mcp_resource.py",
            "baseline_class": "Python process/resource benchmark",
        },
        {
            "benchmark_id": "mcp-two-tool-sla",
            "scenario_ids": ["mcp-tool-matrix"],
            "producer": "scripts/bench/mcp_two_tool_sla.py",
            "baseline_class": "Python MCP latency/semantic benchmark",
        },
        {
            "benchmark_id": "replay-mcp-latency",
            "scenario_ids": ["persistent", "cold-process"],
            "producer": "scripts/bench/replay_mcp_latency.py",
            "baseline_class": "Python MCP replay benchmark",
        },
        {
            "benchmark_id": "replay-search-latency",
            "scenario_ids": ["local-pyo3"],
            "producer": "scripts/bench/replay_search_latency.py",
            "baseline_class": "Python/PyO3 search replay benchmark",
        },
    ]
    missing = [item["producer"] for item in scenarios if not (repo / item["producer"]).is_file()]
    if missing:
        fail(f"benchmark producers missing from source checkout: {missing}")
    return {
        "format_version": FORMAT_VERSION,
        "source_commit": BASELINE_COMMIT,
        "scenario_count": sum(len(item["scenario_ids"]) for item in scenarios),
        "producer_count": len(scenarios),
        "scenarios": scenarios,
        "note": "The baseline has four Python programs plus one Rust timing surface; replay MCP has two distinct lifecycle scenarios.",
    }


def libtest_executable_path(path: str, repo: Path) -> str:
    resolved = Path(path).resolve()
    try:
        return resolved.relative_to(repo).as_posix()
    except ValueError:
        target_root_value = os.environ.get("CARGO_TARGET_DIR")
        if not target_root_value:
            fail(f"libtest executable is outside the repository: {resolved}")
        target_root = Path(target_root_value).resolve()
        try:
            suffix = resolved.relative_to(target_root)
        except ValueError:
            fail(
                f"libtest executable is outside both repository and "
                f"CARGO_TARGET_DIR: {resolved}"
            )
        return (Path("target") / suffix).as_posix()


def collect_libtest(repo: Path, metadata: dict[str, Any]) -> dict[str, Any]:
    build = run(
        [
            "cargo",
            "test",
            "--workspace",
            "--locked",
            "--no-run",
            "--message-format=json",
        ],
        repo,
        timeout_seconds=900,
    )
    raw_metadata = json.loads(
        run(
            ["cargo", "metadata", "--format-version", "1", "--locked", "--no-deps"],
            repo,
        ).stdout
    )
    member_ids = set(raw_metadata["workspace_members"])
    package_names_by_id = {
        package["id"]: package["name"]
        for package in raw_metadata["packages"]
        if package["id"] in member_ids
    }
    expected_names = {package["name"] for package in metadata["packages"]}
    if set(package_names_by_id.values()) != expected_names:
        fail("libtest collection package identities differ from normalized metadata")
    executables: dict[str, dict[str, Any]] = {}
    for line in build.stdout.splitlines():
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        if message.get("reason") != "compiler-artifact" or not message.get("executable"):
            continue
        profile = message.get("profile", {})
        target = message.get("target", {})
        package_id = message.get("package_id", "")
        package_name = package_names_by_id.get(package_id)
        if package_name is None or not profile.get("test"):
            continue
        executable = message["executable"]
        executables[executable] = {
            "package": package_name,
            "target": target.get("name"),
            "kind": target.get("kind", []),
            "executable": libtest_executable_path(executable, repo),
        }
    if not executables:
        fail("Cargo emitted zero test executables")
    tests = []
    for executable, identity in sorted(executables.items(), key=lambda item: item[1]["executable"]):
        all_output = run([executable, "--list", "--format", "terse"], repo).stdout
        ignored_output = run(
            [executable, "--list", "--ignored", "--format", "terse"], repo
        ).stdout
        ignored_names = {
            line.rsplit(": ", 1)[0]
            for line in ignored_output.splitlines()
            if line.endswith(": test")
        }
        for line in all_output.splitlines():
            if not line.endswith(": test"):
                continue
            name = line.rsplit(": ", 1)[0]
            tests.append({**identity, "name": name, "ignored": name in ignored_names})
    if not tests:
        fail("libtest list mode returned zero executable tests")
    return {
        "format_version": FORMAT_VERSION,
        "source_commit": BASELINE_COMMIT,
        "host": {
            "system": platform.system().lower(),
            "machine": platform.machine().lower(),
        },
        "method": "Cargo no-run JSON artifacts followed by direct libtest --list and --list --ignored",
        "executables": len(executables),
        "tests": tests,
    }

def canonical_libtest_host(payload: dict[str, Any]) -> str:
    host = payload.get("host")
    if not isinstance(host, dict):
        fail("libtest inventory host is not an object")
    system = host.get("system")
    machine = host.get("machine")
    if not isinstance(system, str) or not isinstance(machine, str):
        fail("libtest inventory host system and machine must be strings")
    return f"{system}-{machine}"


def validate_libtest_inventory(
    payload: Any, expected_host: str, source_label: str
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        fail(f"{source_label}: libtest inventory is not a JSON object")
    if payload.get("format_version") != FORMAT_VERSION:
        fail(
            f"{source_label}: libtest format_version must be {FORMAT_VERSION}, "
            f"got {payload.get('format_version')!r}"
        )
    if payload.get("source_commit") != BASELINE_COMMIT:
        fail(
            f"{source_label}: stale libtest source commit "
            f"{payload.get('source_commit')!r}; expected {BASELINE_COMMIT}"
        )
    observed_host = canonical_libtest_host(payload)
    if observed_host != expected_host:
        fail(
            f"{source_label}: wrong libtest host {observed_host!r}; "
            f"expected {expected_host!r}"
        )
    if expected_host not in {"darwin-arm64", "linux-aarch64"}:
        fail(f"{source_label}: unsupported retained libtest host {expected_host!r}")
    if payload.get("method") != (
        "Cargo no-run JSON artifacts followed by direct libtest --list and "
        "--list --ignored"
    ):
        fail(f"{source_label}: unrecognized libtest collection method")
    executable_total = payload.get("executables")
    tests = payload.get("tests")
    if (
        not isinstance(executable_total, int)
        or isinstance(executable_total, bool)
        or executable_total <= 0
    ):
        fail(f"{source_label}: libtest executable count must be positive")
    if not isinstance(tests, list) or not tests:
        fail(f"{source_label}: libtest inventory contains zero tests")
    executable_paths: set[str] = set()
    identities: set[tuple[str, str, tuple[str, ...], str]] = set()
    for index, test in enumerate(tests):
        if not isinstance(test, dict):
            fail(f"{source_label}: libtest test {index} is not an object")
        package = test.get("package")
        target = test.get("target")
        kind = test.get("kind")
        executable = test.get("executable")
        name = test.get("name")
        ignored = test.get("ignored")
        if not all(isinstance(value, str) and value for value in (package, target, executable, name)):
            fail(f"{source_label}: libtest test {index} has missing string identity fields")
        if (
            not isinstance(kind, list)
            or not kind
            or not all(isinstance(value, str) and value for value in kind)
        ):
            fail(f"{source_label}: libtest test {index} has invalid target kind")
        if not isinstance(ignored, bool):
            fail(f"{source_label}: libtest test {index} ignored state is not boolean")
        executable_path = Path(executable)
        if executable_path.is_absolute() or ".." in executable_path.parts:
            fail(f"{source_label}: libtest executable path is not repository-relative")
        identity = (package, target, tuple(kind), name)
        if identity in identities:
            fail(f"{source_label}: duplicate libtest identity {identity!r}")
        identities.add(identity)
        executable_paths.add(executable)
    if len(executable_paths) != executable_total:
        fail(
            f"{source_label}: libtest executable count {executable_total} does not "
            f"match {len(executable_paths)} distinct executable paths"
        )
    return payload


def parse_libtest_import(specification: str) -> tuple[str, Path, str]:
    expected_host, separator, remainder = specification.partition("=")
    path_text, digest_separator, expected_digest = remainder.rpartition("@sha256:")
    if (
        not separator
        or not digest_separator
        or not path_text
        or len(expected_digest) != 64
        or any(character not in "0123456789abcdef" for character in expected_digest)
    ):
        fail(
            "invalid --import-libtest; expected "
            "HOST=PATH@sha256:<64-lowercase-hex-digest>"
        )
    return expected_host, Path(path_text).resolve(), expected_digest


def libtest_native_commands() -> list[dict[str, Any]]:
    return [
        {
            "argv": [
                "cargo",
                "test",
                "--workspace",
                "--locked",
                "--no-run",
                "--message-format=json",
            ],
            "cwd": ".",
            "timeout_seconds": 900,
        },
        {
            "argv": [
                "<cargo-test-executable>",
                "--list",
                "--format",
                "terse",
            ],
            "cwd": ".",
            "timeout_seconds": 300,
        },
        {
            "argv": [
                "<cargo-test-executable>",
                "--list",
                "--ignored",
                "--format",
                "terse",
            ],
            "cwd": ".",
            "timeout_seconds": 300,
        },
    ]


def load_libtest_import(specification: str) -> tuple[str, dict[str, Any], dict[str, Any]]:
    expected_host, path, expected_digest = parse_libtest_import(specification)
    try:
        raw = path.read_bytes()
    except OSError as error:
        fail(f"cannot read explicit libtest import {path}: {error}")
    observed_digest = sha256_bytes(raw)
    if observed_digest != expected_digest:
        fail(
            f"{path}: libtest checksum mismatch; expected {expected_digest}, "
            f"observed {observed_digest}"
        )
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as error:
        fail(f"{path}: truncated or invalid libtest JSON: {error}")
    validate_libtest_inventory(payload, expected_host, str(path))
    artifact = f"rust-libtest-{expected_host}.json"
    provenance = {
        "artifact": artifact,
        "artifact_sha256": observed_digest,
        "expected_host": expected_host,
        "expected_sha256": expected_digest,
        "mode": "explicit_import",
        "source_file": path.name,
        "native_commands": libtest_native_commands(),
        "validation": [
            "explicit path",
            "trusted SHA-256",
            "source commit",
            "host",
            "format version",
            "structural invariants",
        ],
    }
    return artifact, payload, provenance


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--node-bin-dir", type=Path, required=True)
    parser.add_argument("--pytest-executable", type=Path, required=True)
    parser.add_argument("--collect-libtest", action="store_true")
    parser.add_argument(
        "--import-libtest",
        action="append",
        default=[],
        metavar="HOST=PATH@sha256:DIGEST",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    repo = args.repo_root.resolve()
    if args.source_commit != BASELINE_COMMIT:
        fail(
            f"this frozen generator accepts only {BASELINE_COMMIT}; got {args.source_commit}"
        )
    if not (repo / "Cargo.toml").is_file():
        fail(f"not a Moraine repository root: {repo}")
    output = args.output.resolve()
    if output == repo or repo in output.parents:
        fail("baseline output must be outside the authenticated source tree")
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        fail("baseline output must be absent or an empty directory")
    observed_tree = git_tree_hash(repo)
    if observed_tree != BASELINE_TREE:
        fail(
            f"source tree mismatch: expected {BASELINE_TREE}, observed {observed_tree}; "
            "use an unmodified archive/worktree of the exact baseline commit"
        )

    imported: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    for specification in args.import_libtest:
        artifact, payload, provenance = load_libtest_import(specification)
        if artifact in imported:
            fail(f"duplicate explicit libtest import for {artifact}")
        imported[artifact] = (payload, provenance)

    if args.collect_libtest:
        cargo_target = os.environ.get("CARGO_TARGET_DIR")
        if not cargo_target:
            fail(
                "--collect-libtest requires CARGO_TARGET_DIR outside the "
                "authenticated source tree"
            )
        target_path = Path(cargo_target).resolve()
        if target_path == repo or repo in target_path.parents:
            fail("CARGO_TARGET_DIR must be outside the authenticated source tree")

    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=f".baseline-{BASELINE_SHORT}-stage-", dir=output.parent
    ) as temporary:
        stage = Path(temporary)
        cargo_targets, package_support = normalize_metadata(repo)
        write_json(stage / "cargo-targets.json", cargo_targets)
        write_json(stage / "package-isolation.json", package_support)
        write_json(
            stage / "rust-test-summary.json",
            rust_static_inventory(repo, cargo_targets),
        )
        collections = ecosystem_collections(
            repo,
            args.node_bin_dir.resolve(),
            args.pytest_executable.resolve(),
        )
        write_json(stage / "ecosystem-collections.json", collections)
        fixtures, checksums = fixture_inventory(repo)
        write_json(stage / "fixture-families.json", fixtures)
        (stage / "fixture-checksums.tsv").write_text(checksums)
        write_json(stage / "e2e-phases.json", e2e_phases(repo))
        write_json(stage / "benchmark-scenarios.json", benchmark_scenarios(repo))

        generated = [
            "benchmark-scenarios.json",
            "cargo-targets.json",
            "e2e-phases.json",
            "ecosystem-collections.json",
            "fixture-checksums.tsv",
            "fixture-families.json",
            "package-isolation.json",
            "rust-test-summary.json",
        ]
        libtest_provenance: list[dict[str, Any]] = []
        for artifact, (payload, provenance) in sorted(imported.items()):
            write_json(stage / artifact, payload)
            generated.append(artifact)
            libtest_provenance.append(provenance)

        if args.collect_libtest:
            collection = collect_libtest(repo, cargo_targets)
            host = canonical_libtest_host(collection)
            validate_libtest_inventory(collection, host, "local libtest collection")
            artifact = f"rust-libtest-{host}.json"
            if artifact in imported:
                fail(f"local libtest collection duplicates explicit import {artifact}")
            write_json(stage / artifact, collection)
            artifact_digest = sha256_bytes((stage / artifact).read_bytes())
            generated.append(artifact)
            libtest_provenance.append(
                {
                    "artifact": artifact,
                    "artifact_sha256": artifact_digest,
                    "expected_host": host,
                    "expected_sha256": artifact_digest,
                    "mode": "local_native_collection",
                    "native_commands": libtest_native_commands(),
                    "validation": [
                        "source commit",
                        "local host",
                        "format version",
                        "structural invariants",
                    ],
                }
            )

        if git_tree_hash(repo) != BASELINE_TREE:
            fail("baseline generation modified the authenticated source tree")
        generated = sorted(set(generated))
        artifact_sha256 = {
            name: sha256_bytes((stage / name).read_bytes()) for name in generated
        }
        for provenance in libtest_provenance:
            artifact = provenance["artifact"]
            if provenance["artifact_sha256"] != artifact_sha256[artifact]:
                fail(
                    f"{artifact}: normalized artifact checksum differs from "
                    "authenticated import"
                )

        import_arguments = []
        for provenance in sorted(
            libtest_provenance, key=lambda item: item["artifact"]
        ):
            if provenance["mode"] == "explicit_import":
                import_arguments.extend(
                    [
                        "--import-libtest",
                        (
                            f"{provenance['expected_host']}="
                            f"<authenticated-{provenance['artifact']}>"
                            f"@sha256:{provenance['expected_sha256']}"
                        ),
                    ]
                )
        manifest = {
            "format_version": FORMAT_VERSION,
            "baseline_commit": BASELINE_COMMIT,
            "baseline_short": BASELINE_SHORT,
            "baseline_tree": BASELINE_TREE,
            "baseline_subject": "feat(backend): consolidate read paths behind unified daemon (#476)",
            "generator": "docs/development/test-architecture/generate_baseline.py",
            "generator_sha256": sha256_bytes(Path(__file__).read_bytes()),
            "generated_artifacts": generated,
            "artifact_sha256": artifact_sha256,
            "ecosystem_collection_commands": {
                key: collections[key]["command"]
                for key in (
                    "frontend_vitest",
                    "browser_playwright",
                    "python_pytest_unittest",
                )
            },
            "libtest_collection": {
                "status": "collected" if libtest_provenance else "not_collected",
                "inventories": sorted(
                    libtest_provenance, key=lambda item: item["artifact"]
                ),
            },
            "reproduce": {
                "argv": [
                    "python3",
                    "<issue-477-checkout>/docs/development/test-architecture/generate_baseline.py",
                    "--repo-root",
                    f"<exact-{BASELINE_SHORT}-source>",
                    "--source-commit",
                    BASELINE_COMMIT,
                    "--node-bin-dir",
                    "<baseline-lock-node_modules>/.bin",
                    "--pytest-executable",
                    "<pytest-environment>/bin/pytest",
                    "--output",
                    "<absent-or-empty-output-directory>",
                    *import_arguments,
                ],
                "environment": {},
            },
        }
        write_json(stage / "manifest.json", manifest)

        if output.exists():
            output.rmdir()
        stage.rename(output)


if __name__ == "__main__":
    main()
