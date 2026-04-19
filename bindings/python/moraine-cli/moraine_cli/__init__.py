"""Exec shim for the bundled `moraine` binaries distributed via PyPI.

Each console-script entry point in `pyproject.toml` maps to one of the
`_moraine_*_main` helpers below, which resolve the packaged binary via
`importlib.resources` and re-exec into it with `os.execvpe`. Bundled
runtime assets (`web/monitor/dist/`, `config/moraine.toml`) are exposed
to the binary via env vars so the Rust code can locate them without
relying on filesystem conventions. See RFC #219.
"""

from __future__ import annotations

import os
import sys
from importlib.resources import as_file, files
from typing import NoReturn

from ._version import __version__

__all__ = [
    "__version__",
    "_moraine_main",
    "_moraine_ingest_main",
    "_moraine_monitor_main",
    "_moraine_mcp_main",
]

_MONITOR_DIST_ENV = "MORAINE_MONITOR_DIST"
_DEFAULT_CONFIG_ENV = "MORAINE_DEFAULT_CONFIG"


def _exec(bin_name: str) -> NoReturn:
    root = files(__package__)
    binaries = root / "_binaries"
    data = root / "_data"

    monitor_dist = data / "web" / "monitor" / "dist"
    default_config = data / "config" / "moraine.toml"
    binary = binaries / bin_name

    # importlib.resources abstracts over wheels vs. editable installs; coerce
    # each Traversable into a concrete filesystem path for exec + env vars.
    with (
        as_file(binary) as binary_path,
        as_file(monitor_dist) as monitor_dist_path,
        as_file(default_config) as default_config_path,
    ):
        env = os.environ.copy()
        # setdefault — do not clobber overrides the user set explicitly.
        env.setdefault(_MONITOR_DIST_ENV, str(monitor_dist_path))
        env.setdefault(_DEFAULT_CONFIG_ENV, str(default_config_path))

        argv = [bin_name, *sys.argv[1:]]
        os.execvpe(str(binary_path), argv, env)

    # execvpe does not return on success; the lines below only run if exec
    # itself fails (e.g. ENOEXEC from a corrupted wheel).
    raise SystemExit(
        f"moraine: failed to exec bundled binary {bin_name!r}; "
        "this usually means the wheel is corrupted or was built for a "
        "different platform. Reinstall with: uv tool install --reinstall moraine"
    )


def _moraine_main() -> NoReturn:
    _exec("moraine")


def _moraine_ingest_main() -> NoReturn:
    _exec("moraine-ingest")


def _moraine_monitor_main() -> NoReturn:
    _exec("moraine-monitor")


def _moraine_mcp_main() -> NoReturn:
    _exec("moraine-mcp")
