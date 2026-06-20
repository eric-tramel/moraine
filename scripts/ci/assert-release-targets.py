#!/usr/bin/env python3
"""Assert release target coverage stays aligned across packaging entrypoints."""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path


EXPECTED_TARGETS = {
    "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-gnu",
    "aarch64-apple-darwin",
    "x86_64-apple-darwin",
}

EXPECTED_MATRIX = {
    "x86_64-unknown-linux-gnu": "ubuntu-latest",
    "aarch64-unknown-linux-gnu": "ubuntu-24.04-arm",
    "aarch64-apple-darwin": "macos-14",
    "x86_64-apple-darwin": "macos-15-intel",
}

EXPECTED_WHEEL_PLATFORMS = {
    "x86_64-unknown-linux-gnu": "manylinux_2_28_x86_64",
    "aarch64-unknown-linux-gnu": "manylinux_2_28_aarch64",
    "aarch64-apple-darwin": "macosx_11_0_arm64",
    "x86_64-apple-darwin": "macosx_11_0_x86_64",
}


ROOT = Path(__file__).resolve().parents[2]


def _read(relpath: str) -> str:
    return (ROOT / relpath).read_text(encoding="utf-8")


def _matrix_targets(relpath: str) -> dict[str, str]:
    text = _read(relpath)
    matrix: dict[str, str] = {}
    current_os = None

    for line in text.splitlines():
        os_match = re.match(r"^\s*-\s+os:\s*([A-Za-z0-9_.-]+)\s*$", line)
        if os_match:
            current_os = os_match.group(1)
            continue

        target_match = re.match(r"^\s*target:\s*([A-Za-z0-9_.-]+)\s*$", line)
        if target_match and current_os is not None:
            target = target_match.group(1)
            if target in matrix:
                raise AssertionError(f"{relpath} declares duplicate target {target}")
            matrix[target] = current_os
            current_os = None

    return matrix


def _publish_loop_targets() -> set[str]:
    text = _read(".github/workflows/release-moraine.yml")
    match = re.search(r"for target in ([^;]+);\s*do", text)
    if not match:
        raise AssertionError("release workflow is missing the PyPI wheel target loop")
    return set(match.group(1).split())


def _wheel_builder_platforms() -> dict[str, str]:
    path = ROOT / "scripts" / "build-python-wheels.py"
    spec = importlib.util.spec_from_file_location("moraine_build_python_wheels", path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"could not load wheel builder from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return dict(module.TARGET_TO_WHEEL_PLATFORM)


def _assert_equal(name: str, actual: set[str]) -> None:
    if actual != EXPECTED_TARGETS:
        missing = sorted(EXPECTED_TARGETS - actual)
        extra = sorted(actual - EXPECTED_TARGETS)
        raise AssertionError(f"{name} target drift; missing={missing} extra={extra}")


def _assert_matrix(name: str, actual: dict[str, str]) -> None:
    _assert_equal(name, set(actual))
    wrong_runner = {
        target: {"expected": expected_os, "actual": actual[target]}
        for target, expected_os in EXPECTED_MATRIX.items()
        if actual[target] != expected_os
    }
    if wrong_runner:
        raise AssertionError(f"{name} runner drift: {wrong_runner}")


def _assert_wheel_platforms(actual: dict[str, str]) -> None:
    _assert_equal("wheel builder", set(actual))
    if actual != EXPECTED_WHEEL_PLATFORMS:
        raise AssertionError(
            f"wheel platform drift; expected={EXPECTED_WHEEL_PLATFORMS} actual={actual}"
        )


def main() -> int:
    matrices = {
        "release matrix": _matrix_targets(".github/workflows/release-moraine.yml"),
        "functional matrix": _matrix_targets(".github/workflows/ci-functional.yml"),
    }
    for name, matrix in matrices.items():
        _assert_matrix(name, matrix)

    target_checks = {
        "PyPI publish loop": _publish_loop_targets(),
    }
    for name, targets in target_checks.items():
        _assert_equal(name, targets)
    _assert_wheel_platforms(_wheel_builder_platforms())
    print("release target, runner, and wheel platform coverage ok:", " ".join(sorted(EXPECTED_TARGETS)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
