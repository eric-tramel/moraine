# Quickstart and Installation

This page gets a local Moraine stack running on macOS or Linux.

## Install

The recommended install path is the PyPI package with `uv`:

```bash
uv tool install moraine-cli
```

Upgrade through the same tool manager:

```bash
uv tool upgrade moraine-cli
```

If you prefer release bundles, run the installer:

```bash
curl -fsSL https://raw.githubusercontent.com/eric-tramel/moraine/main/scripts/install.sh | sh
```

The bundle installer places binaries in `~/.local/bin` by default and writes
`~/.moraine/config.toml` when that file does not already exist. Add
`~/.local/bin` to `PATH` if your shell does not find `moraine`.

## Start Moraine

Start the local stack:

```bash
moraine up
```

Check service health:

```bash
moraine status
```

Open the monitor UI:

```text
http://127.0.0.1:8080
```

Run agent sessions normally. As Moraine indexes session files, the monitor and
status output should show fresh ingest activity.

## Add MCP Retrieval

Moraine MCP search runs as a local stdio MCP server. Each agent harness starts
`moraine run mcp` when it needs the tools, so keep the Moraine stack running
with `moraine up`.

By default `moraine up` also starts a single **shared** MCP server for the host,
and each `moraine run mcp` proxies to it instead of booting a full server per
session — this is what keeps many concurrent agents cheap. Registration is
unchanged; if the shared server is not running, `moraine run mcp` falls back to
an embedded server automatically. See
[Agent MCP Search → Install](agent-mcp-search/install.md#shared-central-server-default).

For Claude Code, install the Moraine plugin marketplace entry. It registers the
MCP server and bundles Moraine search skills. Upgrade Moraine first if your
installed CLI is not current:

```bash
claude plugin marketplace add eric-tramel/moraine --sparse .claude-plugin plugins
claude plugin install moraine@moraine
```

The user-scoped plugin can search the host-wide Moraine history visible to your
user. Enable it only in trusted Claude Code environments. Use manual
project-scoped setup when you need `--project-only`.

If you already added Moraine manually to Claude Code, remove the manual server
first to avoid duplicate MCP tools:

```bash
claude mcp remove moraine --scope user
```

Manual registration still works and is useful for project-scoped Claude Code
setups or other harnesses. Add Moraine to Codex globally:

```bash
codex mcp add moraine -- moraine run mcp
```

Add Moraine to Hermes:

```bash
hermes mcp add moraine --command moraine --args run mcp
```

The MCP server uses the same config resolution rules as the rest of Moraine, with
`MORAINE_MCP_CONFIG` taking precedence over the generic `MORAINE_CONFIG`.
For Cursor, Kimi CLI, Pi, project-scoped Claude Code, and other clients, see
[Agent MCP Search](agent-mcp-search/install.md).

## Common Commands

| Command | Purpose |
| --- | --- |
| `moraine up` | Start ClickHouse, ingest, the monitor UI, and the shared MCP server. |
| `moraine up --mcp` | Force-start the shared MCP server (also on by default). |
| `moraine status` | Print service and ingest health. |
| `moraine logs` | Show recent service logs. |
| `moraine logs ingest --lines 500` | Show recent ingest logs. |
| `moraine db migrate` | Apply database migrations. |
| `moraine db doctor` | Check ClickHouse connectivity and schema health. |
| `moraine down` | Stop managed services. |

## Install From Source

Source builds are useful for development and local testing:

```bash
git clone https://github.com/eric-tramel/moraine.git
cd moraine
cargo build --workspace --locked
MORAINE_SOURCE_TREE_MODE=1 cargo run -p moraine -- up
```

`MORAINE_SOURCE_TREE_MODE=1` tells the control command to run service binaries
from `target/debug`. For installed binaries, keep the service binaries together
or set `MORAINE_SERVICE_BIN_DIR` to the directory containing `moraine-ingest`,
`moraine-monitor`, and `moraine-mcp`.

## Stop

Stop the local stack when you are done:

```bash
moraine down
```
