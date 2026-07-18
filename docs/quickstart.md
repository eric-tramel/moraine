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

In non-interactive scripts, accept the default config and all supported agent
harness integrations:

```bash
moraine setup --yes
```

Pass `--skip-mcp` when you only want config creation or repair.

Preview the default config and all supported harness integrations without
writing files or touching host agent config:

```bash
moraine setup --dry-run
```

The Claude Code and Codex plugins, plus global MCP registrations, can expose
host-wide Moraine session history to that harness. `moraine setup` asks before
making those changes; for project-scoped or custom harness setup, see
[Agent MCP Search → Install](agent-mcp-search/install.md).

## Start Moraine

Start ClickHouse, ingest, and the unified MCP/monitor backend:

```bash
moraine up
```

Every `moraine up` includes the backend. Existing loopback configurations with
`backend.start_on_up = false` are accepted for upgrade compatibility but no
longer suppress it. Non-loopback binds require an explicit true value so an
upgrade cannot unexpectedly expose the unauthenticated monitor API.
Moraine does not install an OS login service; run `moraine up` after a reboot.

Check service health:

```bash
moraine status
```

`moraine status` prefers the live backend's `/api/v1/status` response and
automatically falls back to a direct database read when the backend is
unreachable. Human output labels the source as `daemon API` or `direct DB`;
JSON output adds the compatible top-level `data_source` field with
`daemon_api` or `direct_db`.

For a local ClickHouse process started by `moraine up`, Moraine keeps a small
supervisor running after startup. If ClickHouse exits unexpectedly, the
supervisor waits 1, 2, 4, 8, then 16 seconds between at most five consecutive
replacement attempts. Each replacement must become query-ready before recovery
is recorded. A generation that remains ready for five uninterrupted minutes
resets the consecutive-failure budget. After the retry budget is exhausted,
`moraine status` reports ClickHouse stopped/unhealthy; run
`moraine logs clickhouse` for the exit and retry history, then `moraine up` to
start a fresh recovery budget. External ClickHouse
endpoints are never adopted or restarted.

After starting the backend, open the monitor UI:

```text
http://127.0.0.1:8080
```

The backend HTTP listener binds to `127.0.0.1` by default. Before changing the
bind, read the [experimental HTTP bind guard](configuration.md#experimental-http-bind-guard);
it is startup groundwork, not HTTP request authentication.

Run agent sessions normally. As Moraine indexes session files, the monitor and
status output should show fresh ingest activity.

## Add MCP Retrieval

Moraine MCP search uses a local stdio launcher. Each agent harness starts
`moraine run mcp` when it needs the tools, so keep ClickHouse available with
`moraine up`.

Bare `moraine up` starts the shared backend so each `moraine run mcp` can proxy
to one shared server instead of booting a full server per session. Registration
is unchanged; if the shared backend crashes or is otherwise unreachable, a new
`moraine run mcp` falls back to an embedded server automatically. See
[Agent MCP Search → Install](agent-mcp-search/install.md#shared-central-server-default).

Use `moraine setup` to connect your agent harnesses. In an interactive terminal,
setup shows all supported harnesses selected by default; use the arrow keys to
move, Space to cycle a harness through ingest/plugin-MCP/off choices, and Enter
to apply the selected integrations:

```bash
moraine setup
```

You can also target harness MCP/plugin setup directly, which is useful for
rerunning setup after installing a new harness CLI. Direct targets do not change
ingest source selections:

```bash
moraine setup --yes --mcp-target claude-code --mcp-target codex --mcp-target hermes --mcp-target kiro-cli --mcp-target kimi-cli --mcp-target qwen-code --mcp-target nac --mcp-target opencode --mcp-target cursor --mcp-target pi-coding-agent
```

Preview targeted MCP/plugin changes without touching host agent config:

```bash
moraine setup --dry-run --mcp-target claude-code --mcp-target codex --mcp-target hermes --mcp-target kiro-cli --mcp-target kimi-cli --mcp-target qwen-code --mcp-target nac --mcp-target opencode --mcp-target cursor --mcp-target pi-coding-agent
```

The Claude Code, Codex, and Hermes plugins bundle Moraine search, realtime, and
sanitized bug-report guidance. `moraine setup` installs those plugins for
default user-scoped setup. For Kiro CLI, setup registers global MCP, installs
managed search and realtime guidance under `$KIRO_HOME/steering` when
`KIRO_HOME` is set (or `~/.kiro/steering` otherwise), and points the setup-owned
ingest source at the matching `sessions/cli` directory. It also registers MCP
directly or writes global MCP config for supported harnesses such as Qwen Code,
Kimi CLI, NAC, OpenCode,
Cursor, and Pi Coding Agent. For NAC, setup merges `[mcp_servers.moraine]` into the config at
`NAC_HOME`, then `XDG_CONFIG_HOME/nac`, then `~/.config/nac`, without replacing
model, storage, sandbox, or unrelated MCP settings. It adds a `nac_sqlite`
source only for the default store or an absolute `storage.store_path`; relative
paths and per-launch `nac --store-path` overrides need the manual source snippet
described in [Agent MCP Search → NAC](agent-mcp-search/install.md#nac).
These user-scoped integrations can search the host-wide Moraine history visible
to your user, so enable them only in trusted harness environments.
Qwen registration does not enable Qwen's MCP trust option.

The same Codex marketplace also contains the contributor-only `moraine-dev`
plugin for Moraine maintainers; end users should install `moraine@moraine`.

The MCP server uses the same config resolution rules as the rest of Moraine, with
`MORAINE_MCP_CONFIG` taking precedence over the generic `MORAINE_CONFIG`.
For manual cleanup, project-scoped setup, and other custom setup, see
[Agent MCP Search](agent-mcp-search/install.md).

## Common Commands

| Command | Purpose |
| --- | --- |
| `moraine up` | Start ClickHouse, ingest, and the unified MCP/HTTP/static backend. |
| `moraine up --backend` / `--monitor` / `--mcp` | Deprecated, redundant compatibility flags; bare `moraine up` starts the same backend. |
| `moraine setup` | Create or repair config and guide MCP/plugin registration. |
| `moraine status` | Print service and ingest health. |
| `moraine logs` | Show recent service logs. |
| `moraine logs ingest --lines 500` | Show recent ingest logs. |
| `moraine logs clickhouse` | Show managed ClickHouse supervisor and server logs. |
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
from `target/debug`. A source install requires the `moraine-ingest` and
`moraine-mcp` service binaries; `moraine-monitor` is an optional deprecated
compatibility alias and is not required. For installed binaries, keep
`moraine-ingest` and `moraine-mcp` beside the `moraine` control binary, or set
`MORAINE_SERVICE_BIN_DIR` to the directory containing those service binaries.

## Stop

Stop the local stack when you are done:

```bash
moraine down
```
