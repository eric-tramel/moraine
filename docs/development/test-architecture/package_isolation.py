#!/usr/bin/env python3
"""Compile every root-workspace package independently from Cargo metadata.

This deliberately remains a narrow migration/manifest check instead of becoming
a repository-wide test runner. Missing Cargo, malformed metadata, an empty
workspace, ambiguous package names, or any failed package compilation is fatal.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    return parser.parse_args()


def load_package_names(repo: Path) -> list[str]:
    try:
        result = subprocess.run(
            ["cargo", "metadata", "--format-version", "1", "--locked", "--no-deps"],
            cwd=repo,
            check=True,
            stdout=subprocess.PIPE,
            text=True,
        )
    except FileNotFoundError as error:
        raise SystemExit(f"required command not found: cargo ({error})") from error
    except subprocess.CalledProcessError as error:
        raise SystemExit(f"cargo metadata failed with status {error.returncode}") from error

    try:
        metadata = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise SystemExit(f"cargo metadata returned invalid JSON: {error}") from error
    if metadata.get("version") != 1:
        raise SystemExit(f"unsupported Cargo metadata version: {metadata.get('version')!r}")
    members = metadata.get("workspace_members")
    packages = metadata.get("packages")
    if not isinstance(members, list) or not members:
        raise SystemExit("cargo metadata returned zero workspace members")
    if not isinstance(packages, list):
        raise SystemExit("cargo metadata packages is not a list")

    by_id: dict[str, dict[str, object]] = {}
    for package in packages:
        if not isinstance(package, dict):
            raise SystemExit("cargo metadata package is not an object")
        package_id = package.get("id")
        if not isinstance(package_id, str) or not package_id:
            raise SystemExit("cargo metadata package has an invalid id")
        if package_id in by_id:
            raise SystemExit(f"duplicate cargo metadata package id: {package_id}")
        by_id[package_id] = package

    missing = [member for member in members if member not in by_id]
    if missing:
        raise SystemExit(f"workspace member ids missing from packages: {missing}")
    names = [by_id[member].get("name") for member in members]
    if any(not isinstance(name, str) or not name for name in names):
        raise SystemExit("workspace package has an empty or non-string name")
    package_names = [name for name in names if isinstance(name, str)]
    if len(package_names) != len(set(package_names)):
        raise SystemExit(f"workspace package names are ambiguous: {package_names}")
    return sorted(package_names)


def main() -> None:
    args = parse_args()
    repo = args.repo_root.resolve()
    if not (repo / "Cargo.toml").is_file():
        raise SystemExit(f"not a repository root: {repo}")
    packages = load_package_names(repo)
    commands = [
        ["cargo", "test", "-p", package, "--locked", "--no-run"]
        for package in packages
    ]
    if not commands:
        raise SystemExit("refusing a zero-package isolation run")
    for command in commands:
        print("+ " + " ".join(command), flush=True)
        try:
            subprocess.run(command, cwd=repo, check=True)
        except FileNotFoundError as error:
            raise SystemExit(f"required command not found: cargo ({error})") from error
        except subprocess.CalledProcessError as error:
            raise SystemExit(
                f"package-isolation compilation failed with status {error.returncode}: "
                + " ".join(command)
            ) from error
    print(f"package-isolation packages={len(commands)} status=pass")


if __name__ == "__main__":
    main()
