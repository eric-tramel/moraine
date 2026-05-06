# Moraine

![Moraine Banner](https://github.com/user-attachments/assets/efa723e6-bcc9-4402-bbdf-083297f7e1e2)

[![Docs](https://github.com/eric-tramel/moraine/actions/workflows/docs-deploy.yml/badge.svg)](https://eric-tramel.github.io/moraine/)

Moraine is a local trace stack for agent work. It indexes sessions from agent
harnesses such as Codex, Claude Code, Kimi CLI, and Hermes into ClickHouse,
serves a monitor UI, and exposes MCP retrieval over the indexed history.

Agents get searchable long-term memory through MCP. You get a unified local
record of what happened across providers, including tools, tokens, and
conversation history.

Moraine is under active development. Config keys, schemas, and MCP tools can
change across minor releases.

## Screenshots

<img width="1635" height="1170" alt="Moraine monitor session overview" src="https://github.com/user-attachments/assets/b1e67007-aa73-4c31-80b9-79c0d4fb91da" />

<img width="1635" height="1170" alt="Moraine monitor trace details" src="https://github.com/user-attachments/assets/f7ec8582-8d3c-4745-bdbe-a2fe71df6004" />

## Documentation

- [Introduction](https://eric-tramel.github.io/moraine/)
- [Quickstart and Installation](https://eric-tramel.github.io/moraine/quickstart.html)
- [Configuration](https://eric-tramel.github.io/moraine/configuration.html)
- [MCP Search Interface Specification](https://eric-tramel.github.io/moraine/mcp-search-interface-spec.html)

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
