# Moraine

[![Docs](https://github.com/eric-tramel/moraine/actions/workflows/docs-deploy.yml/badge.svg)](https://eric-tramel.github.io/moraine/)

Moraine is a local trace stack for agent work. It indexes sessions from agent
harnesses such as Codex, Claude Code, Kimi CLI, and Hermes into ClickHouse,
serves a monitor UI, and exposes MCP retrieval over the indexed history.

Moraine is under active development. Config keys, schemas, and MCP tools can
change across minor releases.

## Documentation

- [Introduction](docs/index.md)
- [Quickstart and Installation](docs/quickstart.md)
- [Configuration](docs/configuration.md)

Build the documentation site with:

```bash
make docs-build
```

Serve it locally with:

```bash
make docs-serve
```

## Quickstart

Install from PyPI with `uv`:

```bash
uv tool install moraine-cli
```

Or install the latest release bundle:

```bash
curl -fsSL https://raw.githubusercontent.com/eric-tramel/moraine/main/scripts/install.sh | sh
```

Start the local stack:

```bash
moraine up
moraine status
```

The monitor UI runs at `http://127.0.0.1:8080` by default.

## Development

```bash
cargo build --workspace --locked
cargo test --workspace --locked
cargo fmt --all -- --check
```

The repository-managed pre-commit hook can be installed with:

```bash
make hooks-install
```
