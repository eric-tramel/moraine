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

## Guided Setup

Run the guided setup once after installing, or rerun it later to repair a broken
config and install or update agent harness integrations:

```bash
moraine setup
```

In non-interactive scripts, create a missing config without registering agent
harnesses:

```bash
moraine setup --yes
```

To preview MCP or plugin registration without touching host agent config, pass
explicit targets:

```bash
moraine setup --dry-run --mcp-target claude-code --mcp-target codex
```

The Claude Code and Codex plugins, plus global MCP registrations, can expose
host-wide Moraine session history to that harness. `moraine setup` asks before
making those changes; for project-scoped or custom harness setup, see
[Agent MCP Search → Install](agent-mcp-search/install.md).

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

Use `moraine setup` to connect your agent harnesses. In an interactive terminal,
setup shows a selector for detected harnesses; use the arrow keys to move, Space
to choose integrations, and Enter to install the selected plugins or MCP
registrations:

```bash
moraine setup
```

You can also target harnesses directly, which is useful for scripts or for
rerunning setup after installing a new harness CLI:

```bash
moraine setup --yes --mcp-target claude-code --mcp-target codex --mcp-target hermes --mcp-target opencode --mcp-target cursor
```

Preview those changes without touching host agent config:

```bash
moraine setup --dry-run --mcp-target claude-code --mcp-target codex --mcp-target hermes --mcp-target opencode --mcp-target cursor --mcp-target pi-coding-agent
```

The Claude Code, Codex, and Hermes plugins bundle Moraine search guidance.
`moraine setup` installs those plugins for default user-scoped setup, and
registers MCP directly or writes global MCP config for supported harnesses such
as Kimi CLI, OpenCode, Cursor, and Pi Coding Agent. These user-scoped
integrations can search the host-wide Moraine history visible to your user, so
enable them only in trusted harness environments.

The same Codex marketplace also contains the contributor-only `moraine-dev`
plugin for Moraine maintainers; end users should install `moraine@moraine`.

The MCP server uses the same config resolution rules as the rest of Moraine, with
`MORAINE_MCP_CONFIG` taking precedence over the generic `MORAINE_CONFIG`.
For manual cleanup, project-scoped setup, and other custom setup, see
[Agent MCP Search](agent-mcp-search/install.md).

## Common Commands

| Command | Purpose |
| --- | --- |
| `moraine up` | Start ClickHouse, ingest, the monitor UI, and the shared MCP server. |
| `moraine up --mcp` | Force-start the shared MCP server (also on by default). |
| `moraine setup` | Create or repair config and guide MCP/plugin registration. |
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
