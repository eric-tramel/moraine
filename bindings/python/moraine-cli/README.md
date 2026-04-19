# moraine (PyPI distribution)

This directory packages the Moraine CLI binaries as platform-tagged Python
wheels so they can be installed with:

```bash
uv tool install moraine
# or
uvx moraine --help
```

The wheel contains **prebuilt binaries** — the exec-shim package
(`moraine_cli/__init__.py`) calls `os.execvpe` into the bundled
`moraine`, `moraine-ingest`, `moraine-monitor`, and `moraine-mcp` binaries
with the bundled `web/monitor/dist/` and `config/moraine.toml` paths
exposed via `MORAINE_MONITOR_DIST` / `MORAINE_DEFAULT_CONFIG` env vars.

This is a packaging wrapper, not a Python library. It is **not** the
pyo3 binding at `bindings/python/moraine_conversations/` — that ships
separately.

## How wheels are built

Wheels are not produced by `maturin`, `setuptools`, or a direct
`pip wheel .` — they are assembled from the already-built
`moraine-bundle-<target>.tar.gz` release artifacts by
[`scripts/build-python-wheels.py`](../../../scripts/build-python-wheels.py).

This keeps the Rust build and the Python packaging cleanly separated and
guarantees the wheel contents are byte-identical to the GitHub Releases
bundle.

See [RFC #219](https://github.com/eric-tramel/moraine/issues/219) for the
full rationale.

## Why building from source is refused

Moraine needs a Rust toolchain *and* `bun` to produce the monitor
frontend — way out of scope for `pip install`. The sdist is a stub that
raises a clear error:

```
$ pip install moraine --no-binary moraine
ERROR: moraine ships as prebuilt binary wheels only. Install via a
       platform with a published wheel, or build from source with:
       https://github.com/eric-tramel/moraine#install-from-source
```

See [`scripts/build-python-sdist.py`](../../../scripts/build-python-sdist.py).

## Install from the main repo

If you want the full source workflow (editable installs, Rust toolchain,
development ClickHouse), see the top-level
[README](../../../README.md) instead.
