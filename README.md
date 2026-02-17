# Moraine

Moraine is a local-first system that ingests your Codex and Claude Code session logs into a local database (ClickHouse) so you can monitor them, inspect them, and use them for retrieval.

What you get:

- A local ingestion pipeline that watches your session logs and keeps ClickHouse up to date
- A monitor UI to see health and browse the stored tables
- An MCP server so agent runtimes can retrieve traces via `search` and `open`

## Quickstart (5 minutes)

Install `moraine` (prebuilt bundle, recommended):

```bash
curl -fsSL https://raw.githubusercontent.com/eric-tramel/moraine/main/scripts/install-moraine.sh \
  | bash
export PATH="$HOME/.local/bin:$PATH"
```

Start the local stack and confirm it is healthy:

```bash
moraine up
moraine status
moraine status --output rich --verbose
```

Open the monitor UI:

- `http://127.0.0.1:8080`

To see value quickly: run a Codex or Claude Code session as you normally would, then refresh the UI (or rerun `moraine status`) and watch row counts and heartbeat move.

Notes:

- Moraine stores state under `~/.moraine`.
- If ClickHouse is missing, `moraine up` auto-installs a managed ClickHouse build by default.

## Where Data Comes From

By default, Moraine watches these JSONL sources (you can change this in config):

- Codex: `~/.codex/sessions/**/*.jsonl`
- Claude Code: `~/.claude/projects/**/*.jsonl`

## Use Moraine With an Agent (MCP)

MCP (Model Context Protocol) is not started by default. For an ad hoc session:

```bash
moraine run mcp
```

Agents typically call `search` to discover relevant events, then `open` to fetch surrounding context.

To start MCP automatically with `moraine up`, set `runtime.start_mcp_on_up=true` in `~/.moraine/config.toml`.

For host integration details and the tool contract (`search`, `open`), see `docs/mcp/agent-interface.md`.

## Common Commands

- `moraine status`: health + ingest heartbeat
- `moraine logs`: service logs
- `moraine down`: stop everything

## Configuration (Optional)

Default config lives in `config/moraine.toml`. Runtime config is resolved in this order:

1. `--config <path>`
2. `MORAINE_CONFIG` (and `MORAINE_MCP_CONFIG` for MCP)
3. `~/.moraine/config.toml`

To customize, start by copying `config/moraine.toml` to `~/.moraine/config.toml` and edit values there.

## Install From Source (Optional)

Requires a Rust toolchain (`cargo`, `rustc`):

```bash
git clone https://github.com/eric-tramel/moraine.git ~/src/moraine
cd ~/src/moraine
for crate in moraine moraine-ingest moraine-monitor moraine-mcp; do
  cargo install --path "apps/$crate" --locked
done
```

This installs all runtime binaries expected by `moraine up`.

## More Details

- Operations runbook: `docs/operations/build-and-operations.md`
- Migration notes: `docs/operations/migration-guide.md`
- System internals: `docs/core/system-architecture.md`

## Source Layout Note

Runtime development is authoritative in `apps/*` and `crates/*`.

Legacy reference-only trees still exist under `rust/*` and `moraine-monitor/backend`; do not add new runtime logic there.
