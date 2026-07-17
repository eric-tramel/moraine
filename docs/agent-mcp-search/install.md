# Install by Harness

Moraine MCP search is a stdio MCP server. Configure your harness to launch:

```bash
moraine run mcp
```

Run `moraine up` first so ClickHouse and ingest are available. For this release,
bare `moraine up` intentionally leaves the unified backend off; use
`moraine up --backend` or set `backend.start_on_up = true` for the monitor UI
and shared MCP socket. Without that backend, `moraine run mcp` automatically
falls back to an embedded server. If you use a non-default Moraine config, pass
it through the harness's environment support as
`MORAINE_MCP_CONFIG=/path/to/moraine.toml`.

## Guided setup (recommended)

For default user-scoped setup, let Moraine create or repair its config and
install or update supported harness integrations:

```bash
uv tool install moraine-cli
moraine setup
moraine up
```

`moraine setup` starts with all supported harnesses selected for ingest plus
plugin or MCP setup. In the interactive selector, turn off any harnesses you do
not want before applying. Use `moraine setup --dry-run` to preview the default
setup without writing files. Use the manual sections below for project-scoped
servers, `--project-only`, or custom environment wiring.

## Claude Code plugin marketplace (recommended)

For Claude Code, the recommended user-scoped setup is the Moraine plugin. The
plugin registers the MCP server and bundles Moraine search, realtime, and
bug-report skills, but it does not install Moraine itself and it does not start
ClickHouse, ingest, or the unified backend. Install the CLI first, or upgrade it
if Moraine is already installed. Then start the core stack:

```bash
uv tool install moraine-cli
```

```bash
uv tool upgrade moraine-cli
```

```bash
moraine up
```

Add the marketplace from this repository and install the plugin:

```bash
claude plugin marketplace add eric-tramel/moraine --sparse .claude-plugin plugins
claude plugin install moraine@moraine
```

The Claude plugin MCP launcher looks for `moraine` on `PATH` and then runs
`moraine run mcp`. If the CLI is missing, it reports a `binary_missing` message
with install guidance instead of failing as a raw command-not-found error.

Security note: the user-scoped plugin launches unscoped `moraine run mcp`, so
Claude Code can search the host-wide Moraine history visible to your user. Enable
the plugin only in trusted Claude Code environments. For untrusted repositories,
or when you want retrieval limited to the current project, use the manual
project-scoped `--project-only` registration below. Also start Claude Code from a
trusted shell where `moraine` resolves to the installed CLI, not to a repo-local
shim or relative `PATH` entry.

If you previously registered Moraine manually with `claude mcp add`, remove that
manual entry before relying on the plugin to avoid duplicate MCP servers:

```bash
claude mcp remove moraine --scope user
```

Manual Claude MCP registration remains useful for project-scoped setup,
`--project-only`, and custom environment wiring such as `MORAINE_MCP_CONFIG`.

## Codex plugin marketplace (recommended)

For Codex, the recommended user-scoped setup is the Moraine plugin. The plugin
registers the MCP server and bundles Moraine search, realtime, and bug-report
skills, but it does not install Moraine itself and it does not start ClickHouse,
ingest, or the unified backend.
Install or upgrade the CLI first, then start the core stack:

```bash
uv tool install moraine-cli
```

```bash
uv tool upgrade moraine-cli
```

```bash
moraine up
```

Add the marketplace from this repository and install the end-user plugin:

```bash
codex plugin marketplace add eric-tramel/moraine --sparse .agents/plugins --sparse plugins/moraine --sparse plugins/moraine-dev
codex plugin add moraine@moraine
```

The same marketplace also exposes the contributor-only `moraine-dev` plugin for
Moraine maintainers; end users should install `moraine@moraine`.

The Codex plugin registers a hardened stdio launcher that finds an installed
`moraine` on `PATH` and then runs `moraine run mcp`. If the CLI is missing, the
launcher reports `binary_missing`; reinstall or upgrade it with
`uv tool install moraine-cli` or `uv tool upgrade moraine-cli`, then restart
Codex from a shell where `moraine` is on `PATH`.

Security note: the user-scoped plugin launches unscoped `moraine run mcp`, so
Codex can search the host-wide Moraine history visible to your user. Enable the
plugin only in trusted Codex environments. For untrusted repositories, or when
you want retrieval limited to the current project, use the manual
project-scoped `--project-only` registration below. Also start Codex from a
trusted shell where `moraine` resolves to the installed CLI, not to a repo-local
shim or relative `PATH` entry.

If you previously registered Moraine manually with `codex mcp add`, remove that
manual entry before relying on the plugin to avoid duplicate MCP servers:

```bash
codex mcp list
codex mcp remove moraine
```

Manual Codex MCP registration remains useful for project-scoped setup,
`--project-only`, and custom environment wiring such as `MORAINE_MCP_CONFIG`.

<a id="shared-central-server-default"></a>
## Shared central server (opt-in daemon for this release)

`moraine run mcp` tries the shared socket by default, but bare `moraine up`
intentionally leaves the unified backend off in this release. Start it with
`moraine up --backend`, or set `backend.start_on_up = true` to include it in
every `moraine up`. When the backend is running, every `moraine run mcp` becomes
a thin stdio↔socket proxy to its shared repository, ClickHouse client, caches,
and runtime threads. The same process also serves the monitor HTTP API and
static UI.

What this means for you:

- **Registration is unchanged.** Keep registering `moraine run mcp` exactly as
  shown below. The proxy-vs-embedded choice is made internally.
- **The backend is required for the shared server.** Start it explicitly with
  `moraine up --backend`, or set `backend.start_on_up = true` before running
  bare `moraine up`. It listens on a Unix socket at
  `~/.moraine/run/mcp.sock` (mode `0o600`, so it is scoped to your user).
  `moraine down` stops it and removes the socket.
- **Automatic fallback.** If the backend was not started or it crashed,
  `moraine run mcp` transparently falls back to an embedded server after
  ~250&nbsp;ms, so retrieval keeps working either way.
- **Crash blast radius.** A backend crash drops all live sessions' MCP
  connections at once; harnesses re-establish the connection on their next tool
  use. Restart it with `moraine up --backend` (or bare `moraine up` when
  `backend.start_on_up = true`). To opt out and return to a server per session,
  set `use_central_server = false` (see
  [Configuration → MCP](../configuration.md#shared-central-mcp-server)).

## Project-scoped retrieval (`--project-only`)

Add `--project-only` to restrict retrieval to sessions that originated from
the directory the server is launched in:

```bash
claude mcp add --transport stdio --scope project moraine -- moraine run mcp --project-only
```

With the flag set, `search_sessions`, `list_sessions`, `open`, and
`file_attention` only see sessions whose recorded working directory is the
launch directory or a subdirectory of it (worktrees under a repo root count).
Opening an ID from another project answers `not_found`, exactly as if the
session did not exist.

Details worth knowing:

- **A session's origin is the first working directory it recorded.** Claude
  Code, Codex, Pi, and Cursor (`state.vscdb`) sessions all record one.
  Sessions that never recorded a working directory (e.g. Hermes trajectories)
  are not visible to a project-scoped server.
- **Scoped servers always run embedded.** The shared central server serves
  every project on the host, so a `--project-only` session never proxies to
  it — it boots its own server, like `use_central_server = false` does.
  Pair the flag with project-scoped registration (e.g. a per-repo
  `.mcp.json`) rather than user-scoped registration, unless you want every
  project scoped to itself.
- The `initialize` response advertises the active scope in its
  `instructions` field, so agents can tell they are looking at a filtered
  view.

The remaining sections register the unchanged `moraine run mcp` command with
each harness, including manual Codex and Claude Code registration for
project-scoped or custom setups.

## Manual Codex

Codex stores user-level configuration in `~/.codex/config.toml`, and the Codex
CLI can add MCP servers directly. The plugin is the recommended user-scoped path
above. Use manual registration when you want project scope, `--project-only`, or
custom environment handling. OpenAI's Codex docs note that the CLI and IDE
extension share MCP configuration, so a CLI install is enough for both clients:
[Codex MCP docs](https://developers.openai.com/codex/mcp) and
[Codex configuration reference](https://developers.openai.com/codex/config-reference).

```bash
codex mcp add moraine -- moraine run mcp
codex mcp list
```

Equivalent manual config:

```toml
[mcp_servers.moraine]
command = "moraine"
args = ["run", "mcp"]
```

## Manual Claude Code

Claude Code supports stdio MCP servers through `claude mcp add`. The plugin is
the recommended user-scoped path above. Use manual registration when you want
project scope, `--project-only`, or custom environment handling. See the official
[Claude Code MCP docs](https://code.claude.com/docs/en/mcp).

User scope:

```bash
claude mcp add --transport stdio --scope user moraine -- moraine run mcp
claude mcp list
```

Project scope:

```bash
claude mcp add --transport stdio --scope project moraine -- moraine run mcp
```

Project scope writes or updates `.mcp.json` in the current project. Claude Code
may ask you to approve project-scoped MCP servers before it uses them.

## Hermes

For Hermes, the recommended user-scoped setup is `moraine setup`, which installs
and enables the Moraine Hermes plugin, then asks the plugin to register MCP for
the active Hermes profile. The plugin registers plugin-scoped Moraine search,
realtime, and bug-report skills, injects compact guidance when the user asks
about prior or active agent sessions or Moraine bug reports, and adds
setup/doctor commands. It still uses the `moraine` CLI on `PATH` and the running
local stack.

```bash
moraine setup --mcp-target hermes
hermes moraine doctor
```

The delegated `hermes moraine setup` command writes a `mcp_servers.moraine`
entry that launches `moraine run mcp`, then verifies it with
`hermes mcp test moraine` unless `--no-test` is passed. `moraine setup` skips
that live test while installing the plugin; run `hermes moraine doctor` for
profile-local diagnostics. If you run multiple Hermes profiles, run
`moraine setup --mcp-target hermes` in each profile that should be able to search
Moraine history.

Manual plugin installation remains available when you are not using
`moraine setup`:

```bash
hermes plugins install eric-tramel/moraine/plugins/hermes-moraine --enable
hermes moraine setup
```

Manual Hermes MCP registration remains useful for custom environment wiring such
as `MORAINE_MCP_CONFIG`, or when you do not want to install the plugin:

```bash
hermes mcp add moraine --command moraine --args run mcp
hermes mcp list
hermes mcp test moraine
```

## Kiro CLI

For Kiro CLI, the recommended user-scoped setup registers Moraine as a global
MCP server and installs a dedicated global steering file with Moraine search and
realtime guidance:

```bash
moraine setup --mcp-target kiro-cli
```

Setup writes `$KIRO_HOME/steering/moraine.md` when `KIRO_HOME` is set, or
`~/.kiro/steering/moraine.md` otherwise. It replaces the global `moraine` MCP
registration with one that launches `moraine run mcp` through the absolute path
of the CLI running setup. The steering file is managed by Moraine and updated
when setup runs again; setup does not modify `AGENTS.md` or other Kiro steering
files. Start a new Kiro session after setup so it loads the MCP server and
steering guidance.

Kiro documents global steering files and its MCP commands here:
[Kiro CLI steering](https://kiro.dev/docs/cli/steering/) and
[Kiro CLI MCP](https://kiro.dev/docs/cli/mcp/).

Equivalent manual MCP registration is shown below. Set `moraine_bin` to the
explicit trusted install path when using a custom install directory; do not
derive it from a project-local `PATH` entry.

```bash
moraine_bin="${HOME}/.local/bin/moraine"
case "$moraine_bin" in
  /*) ;;
  *) echo "moraine_bin must be an absolute installed path" >&2; exit 1 ;;
esac
test -x "$moraine_bin" || { echo "moraine is not executable: $moraine_bin" >&2; exit 1; }
kiro-cli mcp add --name moraine --scope global \
  --command "$moraine_bin" --args '["run","mcp"]' --force
```

When registering MCP manually, also add the guidance from
[Patterns](patterns.md) to a markdown file under `~/.kiro/steering/`.

## Kimi CLI

Kimi CLI has built-in MCP configuration commands. Its MCP reference describes
`kimi mcp add` with `stdio` and `http` transports:
[Kimi MCP reference](https://moonshotai.github.io/kimi-cli/en/reference/kimi-mcp.html).

```bash
kimi mcp add --transport stdio moraine -- moraine run mcp
kimi mcp list
kimi mcp test moraine
```

## OpenCode

OpenCode reads MCP servers from the `mcp` object in its config. For global use,
`moraine setup --mcp-target opencode` creates or updates
`~/.config/opencode/opencode.json`. OpenCode's docs describe local MCP servers
with `type = "local"` and a `command` array:
[OpenCode MCP servers](https://opencode.ai/docs/mcp-servers) and
[OpenCode config](https://opencode.ai/docs/config/).

Equivalent manual config:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "moraine": {
      "type": "local",
      "command": ["moraine", "run", "mcp"],
      "enabled": true
    }
  }
}
```

## Cursor

Cursor reads MCP server definitions from `mcp.json`. For global use,
`moraine setup --mcp-target cursor` creates or updates `~/.cursor/mcp.json`.
Cursor's docs describe project config at `.cursor/mcp.json`, global config at
`~/.cursor/mcp.json`, and CLI inspection through `agent mcp`:
[Cursor MCP guide](https://cursor.com/docs/mcp.md) and
[Cursor CLI MCP guide](https://cursor.com/docs/cli/mcp.md).

For global use, create or update `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "moraine": {
      "type": "stdio",
      "command": "moraine",
      "args": ["run", "mcp"]
    }
  }
}
```

Then verify from the Cursor CLI when it is installed:

```bash
agent mcp list
agent mcp list-tools moraine
```

For project-only use, put the same JSON in `.cursor/mcp.json` at the project
root.

If Cursor reports a spawn error for a path such as
`/path/to/project/scripts/launch.sh`, replace that stale server entry with the
JSON above. That launcher path belongs to the Claude plugin bundle and is not a
valid Cursor command.

## Pi Coding Agent

Pi uses an extension to bridge MCP servers into Pi tools. Install the MCP
extension, then add a Moraine stdio server to Pi's MCP config.
`moraine setup --mcp-target pi-coding-agent` runs the extension install and
creates or updates global `~/.pi/agent/mcp.json`. The extension docs describe
global and project `mcp.json` files plus stdio server fields:
[Pi MCP extension](https://pi.dev/packages/pi-mcp-extension).

```bash
pi install npm:pi-mcp-extension
```

Global `~/.pi/agent/mcp.json`:

```json
{
  "mcpServers": {
    "moraine": {
      "transport": "stdio",
      "command": "moraine",
      "args": ["run", "mcp"],
      "lifecycle": "eager"
    }
  }
}
```

With the extension's default prefix, Pi exposes Moraine tools as
`mcp_moraine_search_sessions`, `mcp_moraine_open`, and
`mcp_moraine_list_sessions`, and `mcp_moraine_file_attention`. Use `/mcp` inside
Pi to inspect server status.

## Generic MCP Clients

Most MCP clients that support local stdio servers can use this shape:

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

Use a short server name such as `moraine`. Agent models use names and tool
descriptions to decide which tool to call, and short names make that selection
less ambiguous.
