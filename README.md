# Cortex

Cortex is a local-first system that ingests your Codex and Claude Code session logs into a local database (ClickHouse) so you can monitor them, inspect them, and use them for retrieval.

What you get:

- A local ingestion pipeline that watches your session logs and keeps ClickHouse up to date
- A monitor UI to see health and browse the stored tables
- An MCP server so agent runtimes can retrieve traces via `search` and `open`

## Quickstart (5 minutes)

Install `cortexctl` (prebuilt bundle, recommended):

```bash
curl -fsSL https://raw.githubusercontent.com/eric-tramel/cortex/main/scripts/install-cortexctl.sh \
  | bash -s -- --repo eric-tramel/cortex
export PATH="$HOME/.local/bin:$PATH"
```

Start the local stack and confirm it is healthy:

```bash
cortexctl up
cortexctl status
```

Open the monitor UI:

- `http://127.0.0.1:8080`

To see value quickly: run a Codex or Claude Code session as you normally would, then refresh the UI (or rerun `cortexctl status`) and watch row counts and heartbeat move.

Notes:

- Cortex stores state under `~/.cortex`.
- If ClickHouse is missing, `cortexctl up` auto-installs a managed ClickHouse build by default.

## Where Data Comes From

By default, Cortex watches these JSONL sources (you can change this in config):

- Codex: `~/.codex/sessions/**/*.jsonl`
- Claude Code: `~/.claude/projects/**/*.jsonl`

## Use Cortex With an Agent (MCP)

MCP (Model Context Protocol) is not started by default. For an ad hoc session:

```bash
cortexctl run mcp
```

Agents typically call `search` to discover relevant events, then `open` to fetch surrounding context.

To start MCP automatically with `cortexctl up`, set `runtime.start_mcp_on_up=true` in `~/.cortex/config.toml`.

For host integration details and the tool contract (`search`, `open`), see `docs/mcp/agent-interface.md`.

## Common Commands

- `cortexctl status`: health + ingest heartbeat
- `cortexctl logs`: service logs
- `cortexctl down`: stop everything
- `cortexctl service install`: start services on login (macOS `launchd`, Linux user `systemd`)

## Configuration (Optional)

Default config lives in `config/cortex.toml`. Runtime config is resolved in this order:

1. `--config <path>`
2. `CORTEX_CONFIG` (and `CORTEX_MCP_CONFIG` for MCP)
3. `~/.cortex/config.toml`

To customize, start by copying `config/cortex.toml` to `~/.cortex/config.toml` and edit values there.

## Install From Source (Optional)

Requires a Rust toolchain (`cargo`, `rustc`):

```bash
git clone https://github.com/eric-tramel/cortex.git ~/src/cortex
cd ~/src/cortex
cargo install --path apps/cortexctl --locked
```

## More Details

- Operations runbook: `docs/operations/build-and-operations.md`
- Migration notes: `docs/operations/migration-guide.md`
- System internals: `docs/core/system-architecture.md`
