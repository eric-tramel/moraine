# Moraine

Moraine indexes your Claude Code and Codex session logs into a local ClickHouse database — giving your agents searchable long-term memory and giving you a unified record of everything they've done.

- **Agent memory via MCP**: agents search and retrieve context from past conversations using `search` and `open` tools
- **Unified trace database**: every conversation turn, tool call, and token count in one place — query it however you want
- **Monitor UI**: browse sessions, check ingestion health, inspect what your agents have been doing
- **Fully local**: nothing leaves your machine

## Quickstart

Install `moraine` (prebuilt bundle):

```bash
curl -fsSL https://raw.githubusercontent.com/eric-tramel/moraine/main/scripts/install-moraine.sh \
  | bash
export PATH="$HOME/.local/bin:$PATH"
```

Start the stack and confirm it is healthy:

```bash
moraine up
moraine status
```

Open the monitor UI at `http://127.0.0.1:8080`.

Run a Claude Code or Codex session as you normally would, then check `moraine status` again — you'll see row counts and the ingest heartbeat move as your sessions are indexed.

Use `moraine status --output rich --verbose` for a detailed breakdown.

## Connect an Agent (MCP)

This is where Moraine pays off: your agents can search past sessions to find relevant context.

Start the MCP server:

```bash
moraine run mcp
```

Or start it automatically with `moraine up` by setting `runtime.start_mcp_on_up = true` in `~/.moraine/config.toml`.

To wire it into Claude Code, add this to your `.mcp.json`:

```json
{
  "mcpServers": {
    "moraine": {
      "command": "moraine",
      "args": ["run", "mcp"]
    }
  }
}
```

The MCP server exposes two tools:

- **`search`** — BM25 lexical search across indexed conversation events. Returns ranked hits with snippets.
- **`open`** — fetch an event with surrounding context. Use this after `search` to reconstruct the conversation around a hit.

For the full tool contract and integration guidance, see `docs/mcp/agent-interface.md`.

## Query the Database Directly

Moraine's ClickHouse tables are yours to query. The MCP search path is optimized for fast agent retrieval, but the underlying database holds the complete trace of every session — conversation turns, tool calls, token usage, timestamps, and more.

Connect to ClickHouse and explore:

```bash
clickhouse client -d moraine
```

```sql
-- sessions and their row counts
SELECT session_id, count(*) as events
FROM conversation_events
GROUP BY session_id
ORDER BY events DESC
LIMIT 10;

-- token usage across sessions
SELECT session_id,
       sum(input_tokens) as input,
       sum(output_tokens) as output
FROM conversation_events
WHERE input_tokens > 0
GROUP BY session_id
ORDER BY output DESC
LIMIT 10;
```

See `sql/` for the full schema and view definitions.

## Watched Sources

By default, Moraine watches:

| Source | Path |
|---|---|
| Codex | `~/.codex/sessions/**/*.jsonl` |
| Claude Code | `~/.claude/projects/**/*.jsonl` |

Configurable in `~/.moraine/config.toml` under `[[ingest.sources]]`.

## Common Commands

| Command | What it does |
|---|---|
| `moraine up` | Start all services (ClickHouse, ingestor, monitor) |
| `moraine status` | Health check + ingest heartbeat |
| `moraine logs` | View service logs |
| `moraine down` | Stop everything |

## Configuration

Default config lives in `config/moraine.toml`. Runtime config is resolved in order:

1. `--config <path>` flag
2. `MORAINE_CONFIG` env var (and `MORAINE_MCP_CONFIG` for MCP)
3. `~/.moraine/config.toml`

To customize, copy `config/moraine.toml` to `~/.moraine/config.toml` and edit.

Moraine stores all runtime state under `~/.moraine`. If ClickHouse is not already installed, `moraine up` auto-installs a managed build.

## Install From Source

Requires a Rust toolchain (`cargo`, `rustc`):

```bash
git clone https://github.com/eric-tramel/moraine.git ~/src/moraine
cd ~/src/moraine
for crate in moraine moraine-ingest moraine-monitor moraine-mcp; do
  cargo install --path "apps/$crate" --locked
done
```

## Further Reading

- Operations runbook: `docs/operations/build-and-operations.md`
- Migration notes: `docs/operations/migration-guide.md`
- System internals: `docs/core/system-architecture.md`
- MCP tool contract: `docs/mcp/agent-interface.md`
