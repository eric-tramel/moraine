#!/usr/bin/env python3
"""Assemble platform-tagged Python wheels from moraine release bundles.

Part of the PyPI distribution story tracked in RFC #219 / issue #223.

Input per --target/--bundle pair:
  - a moraine-bundle-<target>.tar.gz produced by
    scripts/package-moraine-release.sh
  - the target triple it was built for (drives the wheel's platform tag)

Output:
  - one moraine-<version>-py3-none-<plat>.whl per --target, laid out per
    the wheel layout specified in the RFC.

Design notes:
  - stdlib + `packaging` only. We assemble the zip directly rather than
    invoking setuptools/wheel — this keeps the dependency surface minimal
    and makes the layout 1:1 with what the exec shim expects.
  - Binaries are preserved with mode 0755 (ZIP external_attr upper bits).
  - The wheel's platform tag is chosen per --target; Python ABI is
    `py3-none-*` because the wheel is "pure Python exec shim + opaque
    binary data" — we do not link against a specific CPython ABI.
  - Bundle integrity is verified against manifest.json sha256 entries
    before any files are copied into the wheel staging area.

Usage:
  python scripts/build-python-wheels.py \
      --target x86_64-unknown-linux-gnu  --bundle dist/moraine-bundle-x86_64-unknown-linux-gnu.tar.gz \
      --target aarch64-apple-darwin      --bundle dist/moraine-bundle-aarch64-apple-darwin.tar.gz \
      --out-dir dist/wheels \
      --version 0.4.1
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import os
import re
import shutil
import sys
import tarfile
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path

# Canonical metadata lives alongside the shim to keep the RFC's "hand-rolled"
# builder self-contained. Everything the builder needs to know about target
# triples + wheel layout sits in this file.

TARGET_TO_WHEEL_PLATFORM = {
    "x86_64-unknown-linux-gnu": "manylinux_2_17_x86_64",
    "aarch64-unknown-linux-gnu": "manylinux_2_17_aarch64",
    "aarch64-apple-darwin": "macosx_11_0_arm64",
    "x86_64-apple-darwin": "macosx_11_0_x86_64",
}

PYTHON_TAG = "py3"
ABI_TAG = "none"

BUNDLE_BINARIES = ("moraine", "moraine-ingest", "moraine-monitor", "moraine-mcp")
BUNDLE_CONFIG = "config/moraine.toml"
BUNDLE_MONITOR_DIST = "web/monitor/dist"

PACKAGE_NAME = "moraine"  # PyPI distribution name
IMPORT_NAME = "moraine_cli"  # package name inside the wheel

# Path inside the repo for the in-tree package skeleton. Used to copy the
# exec shim (__init__.py, py.typed, _version.py) into the wheel.
REPO_PACKAGE_ROOT = Path(__file__).resolve().parent.parent / "bindings/python/moraine-cli"

DESCRIPTION = "Moraine \u2014 unified trace indexer and MCP server for Claude Code / Codex"


# ----------------------------------------------------------------------------
# Manifest verification
# ----------------------------------------------------------------------------


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verify_manifest(extract_dir: Path, manifest: dict) -> None:
    checksums = manifest.get("checksums")
    if not isinstance(checksums, dict) or not checksums:
        raise RuntimeError(
            "bundle manifest.json is missing or has an empty 'checksums' section"
        )
    for relpath, expected in checksums.items():
        target = extract_dir / relpath
        if not target.is_file():
            raise RuntimeError(f"manifest references missing file: {relpath}")
        actual = _sha256(target)
        if actual != expected:
            raise RuntimeError(
                f"checksum mismatch for {relpath}: manifest={expected} actual={actual}"
            )

    for binary in BUNDLE_BINARIES:
        rel = f"bin/{binary}"
        if rel not in checksums:
            raise RuntimeError(f"manifest is missing checksum for required binary {rel}")

    monitor_index = extract_dir / BUNDLE_MONITOR_DIST / "index.html"
    if not monitor_index.is_file():
        raise RuntimeError(
            f"bundle does not contain monitor UI assets at {BUNDLE_MONITOR_DIST}/index.html"
        )


# ----------------------------------------------------------------------------
# Wheel assembly
# ----------------------------------------------------------------------------


@dataclass(frozen=True)
class WheelRecordEntry:
    arcname: str
    sha256_b64: str
    size: int


def _urlsafe_b64nopad(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _write_member(
    zf: zipfile.ZipFile,
    arcname: str,
    payload: bytes,
    *,
    executable: bool,
    records: list[WheelRecordEntry],
) -> None:
    info = zipfile.ZipInfo(arcname)
    # Full Unix st_mode (S_IFREG | perms) in the upper 16 bits of
    # external_attr. pip / installer look at the file-type bits too —
    # without S_IFREG they treat the entry as "not a regular file" and
    # drop the executable bit on unpack.
    mode = 0o100755 if executable else 0o100644
    info.external_attr = mode << 16
    info.compress_type = zipfile.ZIP_DEFLATED
    # Deterministic mtime so wheels rebuilt from the same bundle are
    # byte-identical modulo the zip container metadata.
    info.date_time = (2000, 1, 1, 0, 0, 0)
    zf.writestr(info, payload)

    digest = hashlib.sha256(payload).digest()
    records.append(
        WheelRecordEntry(
            arcname=arcname,
            sha256_b64=_urlsafe_b64nopad(digest),
            size=len(payload),
        )
    )


def _read_file_bytes(path: Path) -> bytes:
    with path.open("rb") as fh:
        return fh.read()


def _iter_files(root: Path):
    for entry in sorted(root.rglob("*")):
        if entry.is_file():
            yield entry


_PEP440_PRE_MAP = (
    ("rc", "rc"),
    ("beta", "b"),
    ("alpha", "a"),
)


def _normalize_version(version: str) -> str:
    """Normalize a git tag to a PEP 440 version.

    - Strips a leading `v` (so `v0.4.1` → `0.4.1`).
    - Maps dashed pre-release segments to their PEP 440 canonical form
      (`0.3.1-rc.1` → `0.3.1rc1`, `0.4.0-beta.2` → `0.4.0b2`).
    - Leaves `.devN` / `.postN` segments untouched — those are already
      PEP 440 compliant in git tags.

    The release workflow pre-normalizes before calling this script, but
    dev-time invocations (smoke tests, ad-hoc rebuilds) often pass a raw
    git tag; keeping the normalization here means both paths work.
    """
    if version.startswith("v") and re.match(r"^v\d", version):
        version = version[1:]
    for prefix, canonical in _PEP440_PRE_MAP:
        version = re.sub(rf"-{prefix}\.?(\d+)", rf"{canonical}\1", version)
    return version


def _render_metadata(version: str, readme: str) -> str:
    lines = [
        "Metadata-Version: 2.1",
        f"Name: {PACKAGE_NAME}",
        f"Version: {version}",
        f"Summary: {DESCRIPTION}",
        "Home-page: https://github.com/eric-tramel/moraine",
        "Author: Moraine Maintainers",
        "License: MIT",
        "Project-URL: Homepage, https://github.com/eric-tramel/moraine",
        "Project-URL: Issues, https://github.com/eric-tramel/moraine/issues",
        "Project-URL: Repository, https://github.com/eric-tramel/moraine",
        "Keywords: moraine,claude-code,codex,mcp,telemetry,observability",
        "Classifier: Development Status :: 4 - Beta",
        "Classifier: License :: OSI Approved :: MIT License",
        "Classifier: Operating System :: MacOS",
        "Classifier: Operating System :: POSIX :: Linux",
        "Classifier: Programming Language :: Python :: 3",
        "Classifier: Programming Language :: Rust",
        "Classifier: Topic :: Software Development :: Libraries",
        "Requires-Python: >=3.9",
        "Description-Content-Type: text/markdown",
        "",
        readme,
        "",
    ]
    return "\n".join(lines)


def _render_wheel_metadata(platform_tag: str) -> str:
    tag = f"{PYTHON_TAG}-{ABI_TAG}-{platform_tag}"
    lines = [
        "Wheel-Version: 1.0",
        "Generator: moraine-build-python-wheels (1.0)",
        "Root-Is-Purelib: false",
        f"Tag: {tag}",
        "",
    ]
    return "\n".join(lines)


def _render_entry_points() -> str:
    lines = [
        "[console_scripts]",
        f"moraine = {IMPORT_NAME}:_moraine_main",
        f"moraine-ingest = {IMPORT_NAME}:_moraine_ingest_main",
        f"moraine-monitor = {IMPORT_NAME}:_moraine_monitor_main",
        f"moraine-mcp = {IMPORT_NAME}:_moraine_mcp_main",
        "",
    ]
    return "\n".join(lines)


def _render_record(records: list[WheelRecordEntry], record_arcname: str) -> str:
    out = io.StringIO()
    for entry in records:
        out.write(f"{entry.arcname},sha256={entry.sha256_b64},{entry.size}\n")
    out.write(f"{record_arcname},,\n")
    return out.getvalue()


def _render_version_module(version: str) -> str:
    return (
        '"""Package version, written by scripts/build-python-wheels.py."""\n'
        f'__version__ = "{version}"\n'
    )


def build_wheel(
    *,
    target: str,
    bundle: Path,
    out_dir: Path,
    version: str,
) -> Path:
    if target not in TARGET_TO_WHEEL_PLATFORM:
        raise SystemExit(
            f"unsupported target {target!r}; known targets: "
            f"{sorted(TARGET_TO_WHEEL_PLATFORM)}"
        )
    platform_tag = TARGET_TO_WHEEL_PLATFORM[target]

    if not bundle.is_file():
        raise SystemExit(f"bundle not found: {bundle}")

    version = _normalize_version(version)
    out_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="moraine-wheel-") as tmp:
        extract_dir = Path(tmp) / "extracted"
        extract_dir.mkdir()
        with tarfile.open(bundle, "r:gz") as tf:
            _safe_extract(tf, extract_dir)

        manifest_path = extract_dir / "manifest.json"
        if not manifest_path.is_file():
            raise SystemExit(f"bundle {bundle.name} has no manifest.json")
        manifest = json.loads(manifest_path.read_text())
        bundle_target = manifest.get("target")
        if bundle_target != target:
            raise SystemExit(
                f"manifest target {bundle_target!r} does not match --target {target!r}"
            )
        _verify_manifest(extract_dir, manifest)

        readme_path = REPO_PACKAGE_ROOT / "README.md"
        readme_body = readme_path.read_text() if readme_path.is_file() else ""

        records: list[WheelRecordEntry] = []

        wheel_name = (
            f"{PACKAGE_NAME}-{version}-{PYTHON_TAG}-{ABI_TAG}-{platform_tag}.whl"
        )
        wheel_path = out_dir / wheel_name
        if wheel_path.exists():
            wheel_path.unlink()

        dist_info = f"{PACKAGE_NAME}-{version}.dist-info"
        record_arcname = f"{dist_info}/RECORD"

        with zipfile.ZipFile(wheel_path, "w") as zf:
            # 1. Package source (shim + marker files).
            for src in _iter_files(REPO_PACKAGE_ROOT / IMPORT_NAME):
                rel = src.relative_to(REPO_PACKAGE_ROOT)
                arcname = str(rel).replace(os.sep, "/")
                if arcname == f"{IMPORT_NAME}/_version.py":
                    payload = _render_version_module(version).encode("utf-8")
                else:
                    payload = _read_file_bytes(src)
                _write_member(zf, arcname, payload, executable=False, records=records)

            # 2. Bundled binaries.
            for binary in BUNDLE_BINARIES:
                src = extract_dir / "bin" / binary
                arcname = f"{IMPORT_NAME}/_binaries/{binary}"
                _write_member(
                    zf,
                    arcname,
                    _read_file_bytes(src),
                    executable=True,
                    records=records,
                )

            # 3. Bundled data: config + monitor dist tree + manifest.
            config_src = extract_dir / BUNDLE_CONFIG
            _write_member(
                zf,
                f"{IMPORT_NAME}/_data/{BUNDLE_CONFIG}",
                _read_file_bytes(config_src),
                executable=False,
                records=records,
            )

            monitor_root = extract_dir / BUNDLE_MONITOR_DIST
            for asset in _iter_files(monitor_root):
                rel = asset.relative_to(extract_dir)
                arcname = f"{IMPORT_NAME}/_data/{str(rel).replace(os.sep, '/')}"
                _write_member(
                    zf,
                    arcname,
                    _read_file_bytes(asset),
                    executable=False,
                    records=records,
                )

            _write_member(
                zf,
                f"{IMPORT_NAME}/_data/manifest.json",
                _read_file_bytes(manifest_path),
                executable=False,
                records=records,
            )

            # 4. dist-info metadata.
            _write_member(
                zf,
                f"{dist_info}/METADATA",
                _render_metadata(version, readme_body).encode("utf-8"),
                executable=False,
                records=records,
            )
            _write_member(
                zf,
                f"{dist_info}/WHEEL",
                _render_wheel_metadata(platform_tag).encode("utf-8"),
                executable=False,
                records=records,
            )
            _write_member(
                zf,
                f"{dist_info}/entry_points.txt",
                _render_entry_points().encode("utf-8"),
                executable=False,
                records=records,
            )

            # 5. RECORD — itself listed with empty sha+size per spec.
            _write_member(
                zf,
                record_arcname,
                _render_record(records, record_arcname).encode("utf-8"),
                executable=False,
                records=[],  # do not self-reference RECORD's own row
            )

    return wheel_path


def _safe_extract(tf: tarfile.TarFile, dest: Path) -> None:
    # Guard against tarballs with paths escaping the extraction root.
    dest_resolved = dest.resolve()
    for member in tf.getmembers():
        target_path = (dest / member.name).resolve()
        if not str(target_path).startswith(str(dest_resolved) + os.sep) and target_path != dest_resolved:
            raise SystemExit(f"refusing to extract unsafe path: {member.name}")
    tf.extractall(dest)


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------


class _PairAction(argparse.Action):
    """Collect --target T1 --bundle B1 --target T2 --bundle B2 pairs."""

    def __call__(self, parser, namespace, values, option_string=None):
        pairs = getattr(namespace, "pairs", None)
        if pairs is None:
            pairs = []
            setattr(namespace, "pairs", pairs)
        if option_string == "--target":
            pairs.append({"target": values})
        elif option_string == "--bundle":
            if not pairs or "bundle" in pairs[-1]:
                parser.error("--bundle must follow a preceding --target")
            pairs[-1]["bundle"] = values


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--target", action=_PairAction, help="Rust target triple for the next --bundle")
    parser.add_argument("--bundle", action=_PairAction, help="Path to moraine-bundle-<target>.tar.gz")
    parser.add_argument("--out-dir", type=Path, required=True, help="Output directory for built wheels")
    parser.add_argument(
        "--version",
        required=True,
        help="Version string for the wheels (typically the release tag with or without leading 'v')",
    )
    args = parser.parse_args(argv)
    pairs = getattr(args, "pairs", None) or []
    if not pairs:
        parser.error("at least one --target/--bundle pair is required")
    for idx, pair in enumerate(pairs):
        if "bundle" not in pair:
            parser.error(f"--target {pair['target']} is missing a following --bundle")
    args.pairs = pairs
    return args


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    for pair in args.pairs:
        wheel = build_wheel(
            target=pair["target"],
            bundle=Path(pair["bundle"]),
            out_dir=args.out_dir,
            version=args.version,
        )
        print(f"built: {wheel}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
