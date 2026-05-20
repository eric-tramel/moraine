#!/usr/bin/env python3
"""Bump Moraine release-managed versions for a release branch."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


MANAGED_CARGO_TOMLS = [
    "apps/moraine/Cargo.toml",
    "apps/moraine-ingest/Cargo.toml",
    "apps/moraine-mcp/Cargo.toml",
    "apps/moraine-monitor/Cargo.toml",
    "crates/moraine-clickhouse/Cargo.toml",
    "crates/moraine-config/Cargo.toml",
    "crates/moraine-conversations/Cargo.toml",
    "crates/moraine-ingest-core/Cargo.toml",
    "crates/moraine-mcp-core/Cargo.toml",
    "crates/moraine-monitor-core/Cargo.toml",
]

MANAGED_PACKAGE_NAMES = {
    "moraine",
    "moraine-clickhouse",
    "moraine-config",
    "moraine-conversations",
    "moraine-ingest",
    "moraine-ingest-core",
    "moraine-mcp",
    "moraine-mcp-core",
    "moraine-monitor",
    "moraine-monitor-core",
}

VERSION_RE = re.compile(r"^v?(\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bump Moraine release-managed Cargo versions and release examples."
    )
    parser.add_argument("version", help="Target version, with or without v prefix.")
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root. Defaults to the current working directory.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned changes without writing files.",
    )
    return parser.parse_args()


def normalize_version(raw: str) -> str:
    match = VERSION_RE.match(raw)
    if not match:
        raise SystemExit(f"invalid release version: {raw!r}")
    return match.group(1)


def read_text(path: Path) -> str:
    try:
        return path.read_text()
    except FileNotFoundError:
        raise SystemExit(f"missing expected file: {path}") from None


def write_text(path: Path, text: str, *, dry_run: bool) -> None:
    if not dry_run:
        path.write_text(text)


def package_name(text: str, path: Path) -> str:
    match = re.search(r'(?m)^name = "([^"]+)"$', text)
    if not match:
        raise SystemExit(f"could not find package name in {path}")
    return match.group(1)


def package_version(text: str, path: Path) -> str:
    match = re.search(r'(?m)^version = "([^"]+)"$', text)
    if not match:
        raise SystemExit(f"could not find package version in {path}")
    return match.group(1)


def replace_single(pattern: str, repl: str, text: str, path: Path) -> tuple[str, int]:
    new_text, count = re.subn(pattern, repl, text, count=1, flags=re.MULTILINE)
    if count != 1:
        raise SystemExit(f"expected exactly one version replacement in {path}")
    return new_text, count


def bump_cargo_tomls(repo_root: Path, target_version: str, *, dry_run: bool) -> str:
    current_version: str | None = None
    changed: list[str] = []

    for relpath in MANAGED_CARGO_TOMLS:
        path = repo_root / relpath
        text = read_text(path)
        name = package_name(text, path)
        if name not in MANAGED_PACKAGE_NAMES:
            raise SystemExit(f"unexpected managed package name in {path}: {name}")
        version = package_version(text, path)
        if current_version is None:
            current_version = version
        elif version != current_version:
            raise SystemExit(
                f"managed Cargo.toml versions are not in sync: "
                f"{path} has {version}, expected {current_version}"
            )

        new_text, _ = replace_single(
            rf'^version = "{re.escape(version)}"$',
            f'version = "{target_version}"',
            text,
            path,
        )
        if new_text != text:
            write_text(path, new_text, dry_run=dry_run)
            changed.append(relpath)

    if current_version is None:
        raise SystemExit("no managed Cargo.toml files configured")
    if current_version == target_version:
        raise SystemExit(f"target version is already set: {target_version}")

    print(f"Cargo.toml: {current_version} -> {target_version}")
    for relpath in changed:
        print(f"  {relpath}")
    return current_version


def bump_lockfile(
    repo_root: Path, current_version: str, target_version: str, *, dry_run: bool
) -> None:
    path = repo_root / "Cargo.lock"
    text = read_text(path)
    blocks = re.split(r"(?=^\[\[package\]\]\n)", text, flags=re.MULTILINE)
    changed_names: set[str] = set()
    new_blocks: list[str] = []

    for block in blocks:
        name_match = re.search(r'(?m)^name = "([^"]+)"$', block)
        if name_match and name_match.group(1) in MANAGED_PACKAGE_NAMES:
            version_match = re.search(r'(?m)^version = "([^"]+)"$', block)
            if not version_match:
                raise SystemExit(f"lockfile package has no version: {name_match.group(1)}")
            found = version_match.group(1)
            if found != current_version:
                raise SystemExit(
                    f"lockfile {name_match.group(1)} has {found}, expected {current_version}"
                )
            block = re.sub(
                rf'(?m)^version = "{re.escape(current_version)}"$',
                f'version = "{target_version}"',
                block,
                count=1,
            )
            changed_names.add(name_match.group(1))
        new_blocks.append(block)

    missing = MANAGED_PACKAGE_NAMES - changed_names
    if missing:
        raise SystemExit(
            "lockfile did not contain managed packages: " + ", ".join(sorted(missing))
        )

    new_text = "".join(new_blocks)
    write_text(path, new_text, dry_run=dry_run)
    print(f"Cargo.lock: updated {len(changed_names)} managed package entries")


def replace_optional_file(
    repo_root: Path,
    relpath: str,
    current_version: str,
    target_version: str,
    *,
    dry_run: bool,
) -> int:
    path = repo_root / relpath
    if not path.exists():
        return 0
    text = read_text(path)
    replacements = {
        f"v{current_version}": f"v{target_version}",
        f"MORAINE_INSTALL_VERSION=v{current_version}": (
            f"MORAINE_INSTALL_VERSION=v{target_version}"
        ),
    }
    new_text = text
    count = 0
    for old, new in replacements.items():
        occurrences = new_text.count(old)
        if occurrences:
            new_text = new_text.replace(old, new)
            count += occurrences
    if new_text != text:
        write_text(path, new_text, dry_run=dry_run)
        print(f"{relpath}: {count} replacement(s)")
    return count


def bump_release_examples(
    repo_root: Path, current_version: str, target_version: str, *, dry_run: bool
) -> None:
    optional_files = [
        ".github/workflows/release-moraine.yml",
        "README.md",
        "docs/quickstart.md",
        "scripts/install.sh",
    ]
    total = 0
    for relpath in optional_files:
        total += replace_optional_file(
            repo_root, relpath, current_version, target_version, dry_run=dry_run
        )
    print(f"release examples: {total} replacement(s)")


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    if not (repo_root / "Cargo.toml").is_file() or not (repo_root / ".git").exists():
        raise SystemExit(f"not a Moraine repository root: {repo_root}")

    target_version = normalize_version(args.version)
    current_version = bump_cargo_tomls(
        repo_root, target_version, dry_run=args.dry_run
    )
    bump_lockfile(
        repo_root, current_version, target_version, dry_run=args.dry_run
    )
    bump_release_examples(
        repo_root, current_version, target_version, dry_run=args.dry_run
    )
    if args.dry_run:
        print("dry run: no files written")
    return 0


if __name__ == "__main__":
    sys.exit(main())
