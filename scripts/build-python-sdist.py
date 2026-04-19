#!/usr/bin/env python3
"""Build a stub source distribution for the `moraine` PyPI package.

Moraine ships as prebuilt binary wheels only (see RFC #219). The sdist
exists purely so PyPI has *something* to return for
`pip install moraine --no-binary moraine`, and so `uv tool install`
has a fallback when no platform wheel is available — but that fallback
deliberately fails loudly rather than attempting a Rust + bun build.

The generated tarball contains:

  - pyproject.toml  — `build-backend = "moraine_build_refuser"`
  - moraine_build_refuser.py  — the in-tree refusal backend
  - README.md       — user-facing install instructions
  - PKG-INFO        — PEP 643-compliant core metadata

When pip/uv tries to build a wheel from this sdist, the backend's
`build_wheel` immediately raises with a pointer to the real install
paths.

Usage:
  python scripts/build-python-sdist.py --out-dir dist/wheels --version 0.4.1
"""

from __future__ import annotations

import argparse
import io
import re
import sys
import tarfile
import time
from pathlib import Path

PACKAGE_NAME = "moraine-cli"
DESCRIPTION = "Unified realtime agent trace database & search MCP"

# Sdist filenames follow the same PEP 427-style escaping modern build
# tools use — replace runs of non-alphanumeric chars with underscores so
# the archive name parses as {distribution}-{version}.tar.gz without
# ambiguity (`moraine-cli-0.4.1` otherwise reads as distribution
# `moraine` + version `cli-0.4.1`).
SDIST_DISTRIBUTION_NAME = re.sub(r"[^A-Za-z0-9.]+", "_", PACKAGE_NAME)

REFUSAL_MESSAGE = (
    "moraine ships as prebuilt binary wheels only. Install via a "
    "platform with a published wheel, or build from source with: "
    "https://github.com/eric-tramel/moraine#install-from-source"
)

PYPROJECT_TEMPLATE = """\
[build-system]
requires = []
build-backend = "moraine_build_refuser"
backend-path = ["."]

[project]
name = "{name}"
version = "{version}"
description = "{description}"
readme = "README.md"
license = {{ text = "Apache-2.0" }}
requires-python = ">=3.9"
"""

BACKEND_TEMPLATE = '''\
"""In-tree PEP 517 backend that refuses to build moraine from source.

See RFC #219. Moraine ships as prebuilt binary wheels; building from an
sdist would require a Rust toolchain + bun for the monitor frontend,
which is out of scope for `pip install`.
"""

MESSAGE = (
    {message!r}
)


def _refuse(*_args, **_kwargs):
    raise SystemExit("ERROR: " + MESSAGE)


build_wheel = _refuse
build_sdist = _refuse
get_requires_for_build_wheel = _refuse
get_requires_for_build_sdist = _refuse
prepare_metadata_for_build_wheel = _refuse
'''

README_TEMPLATE = """\
# moraine (source distribution)

This sdist is a stub. Moraine distributes prebuilt binary wheels only
(macOS arm64, Linux x86_64, Linux aarch64). Installing this sdist with
`pip install moraine --no-binary moraine` will fail with a pointer to
the source-install instructions.

To install moraine:

```bash
# Recommended: a published platform wheel
uv tool install moraine

# Fallback on platforms without a wheel: the curl installer
curl -fsSL https://raw.githubusercontent.com/eric-tramel/moraine/main/scripts/install.sh | sh
```

See https://github.com/eric-tramel/moraine for full docs.
"""

PKG_INFO_TEMPLATE = """\
Metadata-Version: 2.1
Name: {name}
Version: {version}
Summary: {description}
License: Apache-2.0
Requires-Python: >=3.9
Description-Content-Type: text/markdown

{readme}
"""


_PEP440_PRE_MAP = (
    ("rc", "rc"),
    ("beta", "b"),
    ("alpha", "a"),
)


def _normalize_version(version: str) -> str:
    # Mirrors scripts/build-python-wheels.py._normalize_version so the
    # sdist and wheels in the same release share a normalized version.
    if version.startswith("v") and re.match(r"^v\d", version):
        version = version[1:]
    for prefix, canonical in _PEP440_PRE_MAP:
        version = re.sub(rf"-{prefix}\.?(\d+)", rf"{canonical}\1", version)
    return version


def _add_file(tar: tarfile.TarFile, arcname: str, payload: bytes, mtime: float) -> None:
    info = tarfile.TarInfo(arcname)
    info.size = len(payload)
    info.mtime = int(mtime)
    info.mode = 0o644
    info.type = tarfile.REGTYPE
    tar.addfile(info, io.BytesIO(payload))


def build_sdist(*, version: str, out_dir: Path) -> Path:
    version = _normalize_version(version)
    out_dir.mkdir(parents=True, exist_ok=True)

    sdist_root = f"{SDIST_DISTRIBUTION_NAME}-{version}"
    sdist_name = f"{sdist_root}.tar.gz"
    sdist_path = out_dir / sdist_name
    if sdist_path.exists():
        sdist_path.unlink()

    readme = README_TEMPLATE
    pyproject = PYPROJECT_TEMPLATE.format(
        name=PACKAGE_NAME,
        version=version,
        description=DESCRIPTION,
    )
    backend = BACKEND_TEMPLATE.format(message=REFUSAL_MESSAGE)
    pkg_info = PKG_INFO_TEMPLATE.format(
        name=PACKAGE_NAME,
        version=version,
        description=DESCRIPTION,
        readme=readme,
    )

    now = time.time()
    with tarfile.open(sdist_path, "w:gz") as tar:
        _add_file(tar, f"{sdist_root}/pyproject.toml", pyproject.encode("utf-8"), now)
        _add_file(tar, f"{sdist_root}/moraine_build_refuser.py", backend.encode("utf-8"), now)
        _add_file(tar, f"{sdist_root}/README.md", readme.encode("utf-8"), now)
        _add_file(tar, f"{sdist_root}/PKG-INFO", pkg_info.encode("utf-8"), now)

    return sdist_path


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--version", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    sdist = build_sdist(version=args.version, out_dir=args.out_dir)
    print(f"built: {sdist}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
