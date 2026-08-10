# Install by Harness

Codex and Claude Code connect directly to the MCP endpoint served by the
unified local backend:

```text
http://127.0.0.1:8080/mcp
```

Run `moraine setup` to register the endpoint from your effective
`backend.bind` and `monitor.port`, then run `moraine up`. The backend must be
running while these URL clients use Moraine. The stdio command
`moraine run mcp` remains available for other harnesses, project-scoped
retrieval, and compatibility workflows.

For a custom config, use the same file for registration and startup:

```bash
moraine setup --config /absolute/path/to/moraine.toml
moraine up --config /absolute/path/to/moraine.toml
```

Setup persists a URL derived from that file. Ad hoc `moraine up --host` or
`--port` overrides are runtime-only and are not reflected in the registered
URL; change `backend.bind` or `monitor.port` in the config and rerun setup
instead.

<a id="environment-backed-clickhouse-credentials"></a>
## Environment-backed ClickHouse credentials

If the Moraine config uses `{ env = "VARIABLE_NAME" }` for a ClickHouse value,
start the backend through the same environment injector that provides that
variable. Codex and Claude Code connect to the already-running `/mcp` endpoint;
their MCP configuration does not need the ClickHouse credential.

For example:

```bash
op run --env-file=/absolute/path/to/.env.1password -- moraine up
```

`moraine setup` still needs the variable when it loads and validates the
config. Launch setup through the injector too when necessary:

```bash
op run --env-file=/absolute/path/to/.env.1password -- moraine setup
```

Do not launch the whole agent through the injector solely for Moraine:
commands spawned by the agent would inherit the ClickHouse credentials. Other
harnesses that still launch `moraine run mcp` need the same environment as
before.

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
not want before applying. Use `moraine setup integrations --all --dry-run` to
preview every harness integration without writing files. Use the manual sections below for project-scoped
servers, `--project-only`, or custom environment wiring.

For Codex and Claude Code, setup refreshes the Moraine plugin and removes any
obsolete bundled stdio MCP declaration from the installed manifest before
registering the backend URL.

## Claude Code plugin marketplace (recommended)

For Claude Code, the recommended user-scoped setup installs the Moraine plugin
for search, realtime, and bug-report skills and registers the backend URL as a
native HTTP MCP server. The plugin does not install Moraine or start ClickHouse,
ingest, or the unified backend. Install or upgrade the CLI first, then start the
core stack:

```bash
uv tool install moraine-cli
```

```bash
uv tool upgrade moraine-cli
```

```bash
moraine up
```

For a first-time plugin install, add the marketplace and install the plugin:

```bash
claude plugin marketplace add eric-tramel/moraine --sparse .claude-plugin plugins
claude plugin install moraine@moraine
```

For an existing plugin installation, refresh the marketplace and update the
plugin after upgrading the CLI:

```bash
claude plugin marketplace update moraine
claude plugin update moraine@moraine
```

Start a new Claude Code session after installation or update so it loads the
current plugin version.

The plugin contains skills and commands only; `moraine setup` owns MCP
transport registration. It removes an older `moraine` stdio entry and runs:

```bash
claude mcp add --transport http --scope user moraine http://127.0.0.1:8080/mcp
```

The actual URL follows the selected Moraine config. Start a new Claude Code
session after setup or plugin update so it loads the current skills and MCP
registration.

Security note: the user-scoped endpoint searches the host-wide Moraine history
visible to your user. Enable it only in trusted Claude Code environments. For
untrusted repositories, or when retrieval must be limited to the current
project, use the manual project-scoped `--project-only` stdio registration
below.

## Codex plugin marketplace (recommended)

For Codex, the recommended user-scoped setup installs the Moraine plugin for
search, realtime, and bug-report skills and registers the backend URL as a
native HTTP MCP server. The plugin does not install Moraine or start ClickHouse,
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

The plugin contains skills and commands only; `moraine setup` owns MCP
transport registration. It refreshes the marketplace, removes an older
`moraine` stdio entry, and runs:

```bash
codex mcp add moraine --url http://127.0.0.1:8080/mcp
```

The actual URL follows the selected Moraine config. Restart Codex after setup
or plugin update so it loads the current skills and MCP registration.

Security note: the user-scoped endpoint searches the host-wide Moraine history
visible to your user. Enable it only in trusted Codex environments. For
untrusted repositories, or when retrieval must be limited to the current
project, use the manual project-scoped `--project-only` stdio registration
below.

<a id="shared-central-server-default"></a>
## Shared central server

`moraine up` starts one unified backend. The same process serves the monitor UI
and API, the direct Streamable HTTP MCP endpoint at `/mcp`, and the private
Unix socket used by compatibility stdio clients. All of those surfaces share
the default repository, ClickHouse client, caches, and request budget.

What this means for you:

- **Codex and Claude Code connect directly.** Guided setup registers
  `http://127.0.0.1:8080/mcp` by default. No per-client tunnel process or
  `mcp.pid` is created.
- **The backend is required for URL clients.** Start it with `moraine up`.
  `moraine down` closes `/mcp`; URL clients reconnect after the backend starts
  again.
- **The URL serves the default backend.** Named-backend routing and
  `--project-only` rely on launch-directory context and therefore remain on the
  stdio path.
- **Stdio compatibility remains.** Each `moraine run mcp` invocation is a
  transient client: concurrent harness sessions may each proxy through the
  private Unix socket, and no singleton `mcp.pid` is created. A client falls
  back to an embedded server when the backend is unreachable.

## Project-scoped retrieval (`--project-only`)

Add `--project-only` after Moraine's `--` service-argument separator to restrict
retrieval to sessions that originated from the directory where the server starts:

```bash
claude mcp add --transport stdio --scope project moraine -- moraine run mcp -- --project-only
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

The remaining sections document direct URL registration where supported and
the retained `moraine run mcp` command for project-scoped or compatibility
setups.

## Manual Codex

Codex stores user-level configuration in `~/.codex/config.toml`, and the Codex
CLI can add the shared HTTP endpoint directly:

```bash
codex mcp add moraine --url http://127.0.0.1:8080/mcp
codex mcp list
```

Equivalent manual config:

```toml
[mcp_servers.moraine]
url = "http://127.0.0.1:8080/mcp"
```

Use a stdio command instead only when you need project scope,
`--project-only`, or compatibility environment handling:

```bash
codex mcp add moraine -- moraine run mcp -- --project-only
```

See [Codex MCP docs](https://developers.openai.com/codex/mcp) and the
[Codex configuration reference](https://developers.openai.com/codex/config-reference).

## Manual Claude Code

Claude Code can add the shared HTTP endpoint directly:

```bash
claude mcp add --transport http --scope user moraine http://127.0.0.1:8080/mcp
claude mcp list
```

Use the retained stdio transport for project-scoped retrieval:

```bash
claude mcp add --transport stdio --scope project moraine -- moraine run mcp -- --project-only
```

Project scope writes or updates `.mcp.json` in the current project. Claude Code
may ask you to approve project-scoped MCP servers before it uses them. See the
official [Claude Code MCP docs](https://code.claude.com/docs/en/mcp).


## Hermes

For Hermes, the recommended user-scoped setup is `moraine setup`, which installs
and enables the Moraine Hermes plugin, then asks the plugin to register MCP for
the active Hermes profile. The plugin registers plugin-scoped Moraine search,
realtime, and bug-report skills, injects compact guidance when the user asks
about prior or active agent sessions or Moraine bug reports, and adds
setup/doctor commands. It still uses the `moraine` CLI on `PATH` and the running
local stack.

```bash
moraine setup integrations hermes
hermes moraine doctor
```

The delegated `hermes moraine setup` command writes a `mcp_servers.moraine`
entry that launches `moraine run mcp`, then verifies it with
`hermes mcp test moraine` unless `--no-test` is passed. `moraine setup` skips
that live test while installing the plugin; run `hermes moraine doctor` for
profile-local diagnostics. If you run multiple Hermes profiles, run
`moraine setup integrations hermes` in each profile that should be able to search
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
moraine setup integrations kiro-cli
```

Setup writes `$KIRO_HOME/steering/moraine.md` when `KIRO_HOME` is set, or
`~/.kiro/steering/moraine.md` otherwise. Moraine resolves the setup-owned
`kiro` ingest source under `$KIRO_HOME/sessions/cli` when the override is set;
without it, the source remains `~/.kiro/sessions/cli`. Setup replaces the
global `moraine` MCP registration with one that launches `moraine run mcp`
through the absolute path of the CLI running setup. The steering file is managed
by Moraine and updated when setup runs again; setup does not modify `AGENTS.md`
or other Kiro steering files. Start a new Kiro session after setup so it loads
the MCP server and steering guidance.

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
[Patterns](patterns.md) to a markdown file under `$KIRO_HOME/steering` when set,
or `~/.kiro/steering` otherwise.

## Kimi CLI

Kimi CLI has built-in MCP configuration commands. Its MCP reference describes
`kimi mcp add` with `stdio` and `http` transports:
[Kimi MCP reference](https://moonshotai.github.io/kimi-cli/en/reference/kimi-mcp.html).

```bash
kimi mcp add --transport stdio moraine -- moraine run mcp
kimi mcp list
kimi mcp test moraine
```

## Qwen Code

For user-scoped Qwen setup, run:

```bash
moraine setup integrations qwen-code
```

Moraine invokes Qwen's native CLI to add or update only the user-scoped
`moraine` stdio server. The equivalent manual command is:

```bash
qwen mcp add --scope user --transport stdio moraine moraine -- run mcp
```

This launches `moraine run mcp`; an explicit Moraine config adds
`--config /path/to/config.toml` as separate arguments. Moraine does not pass
Qwen's `--trust` option, so tool trust remains disabled. Restart Qwen Code after
registration so a new session loads the server. See Qwen's
[MCP documentation](https://github.com/QwenLM/qwen-code/blob/v0.19.0/docs/users/features/mcp.md).
Qwen's setup command owns user scope only; project-scoped registration is not
managed by `moraine setup`.

## NAC

NAC reads MCP server definitions from `config.toml`. For global use:

```bash
moraine setup integrations nac
```

Setup chooses the config directory in this order:

1. `NAC_HOME` when set.
2. `${XDG_CONFIG_HOME}/nac` when `XDG_CONFIG_HOME` is set.
3. `~/.config/nac`.

It creates or updates only `[mcp_servers.moraine]`; existing model, storage,
sandbox, and unrelated MCP settings are preserved. The equivalent manual
configuration is:

```toml
[mcp_servers.moraine]
enabled = true
transport = "stdio"
command = "moraine"
args = ["run", "mcp"]
```

When NAC is also selected as an ingest source in regular guided
`moraine setup`, setup resolves its SQLite store. The default is
`<resolved config directory>/store.db`; an absolute `storage.store_path` in
NAC's config is followed directly. A relative `storage.store_path` is resolved
by NAC from its launch directory, so setup does not add a potentially wrong
source and instead prints a ready-to-copy `[[ingest.sources]]` snippet. The same
manual step is required when launching NAC with `--store-path`, because that
per-process override is not present in `config.toml`.
For that manual case, add the resolved absolute database path and its parent
directory to Moraine's `moraine.toml`:

```toml
[[ingest.sources]]
name = "nac"
harness = "nac"
enabled = true
glob = "/absolute/path/to/store.db"
watch_root = "/absolute/path/to"
format = "nac_sqlite"
materialize = true
```

Replace both paths with the location NAC actually uses. Do not use a relative
path in this source: Moraine and NAC may have different launch directories.

Review the preview before applying custom paths:

```bash
moraine setup integrations nac --dry-run
```

Setup-owned writes are atomic and idempotent. Re-running the targeted command
repairs only the Moraine MCP table; re-running guided setup can also reconcile
the setup-owned NAC ingest source. A differently named custom NAC source is
preserved, so do not point two enabled sources at the same database unless
duplicate ingestion is intentional.

To roll back setup, remove only `[mcp_servers.moraine]` from NAC's
`config.toml`. If guided setup also added the setup-owned ingest entry, remove
the `[[ingest.sources]]` table whose `name` is `nac` from Moraine's
`moraine.toml`; leave differently named custom sources untouched. Remove that
ingest table before running the same configuration with an older Moraine
release, which does not recognize the `nac_sqlite` format.

## OpenCode

OpenCode reads MCP servers from the `mcp` object in its config. For global use,
`moraine setup integrations opencode` creates or updates
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
`moraine setup integrations cursor` creates or updates `~/.cursor/mcp.json`.
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

If Cursor reports a spawn error for a stale plugin-local command, rerun
`moraine setup --mcp-target cursor` or replace that entry with the JSON above.
Cursor should invoke the installed `moraine` command directly.

## Pi Coding Agent

Pi uses an extension to bridge MCP servers into Pi tools. Install the MCP
extension, then add a Moraine stdio server to Pi's MCP config.
`moraine setup integrations pi-coding-agent` runs the extension install and
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

## OMP (Oh My Pi)

OMP has native MCP support and keeps its own agent state.
`moraine setup integrations omp` creates or updates
`~/.omp/agent/mcp.json`; no MCP extension is required. Existing servers and
unrelated settings in that file are preserved. OMP setup is independent from
Pi: it detects `omp` or `~/.omp/agent`, owns only the `omp` ingest source, and
does not read or write `~/.pi/agent/mcp.json`.

Do not install `pi-mcp-extension` for this integration. That Pi extension reads
Pi's global MCP config rather than OMP's, while current OMP releases load
`~/.omp/agent/mcp.json` directly.

To preview the OMP config change without writing files, run:

```bash
moraine setup integrations omp --dry-run
```

For manual setup, use the same JSON shape shown above at OMP's global config
path:

```bash
$EDITOR ~/.omp/agent/mcp.json
```

## Prime Agent

Prime Agent v0.7.0 is the verified compatibility contract. Stop Prime Agent,
then install its Python-backed Moraine skill and stdio registration:

```bash
moraine setup integrations prime-agent --yes
```

Setup manages `settings.json` and `skills/moraine` under
`$PRIME_AGENT_CODING_AGENT_DIR`, or `~/.prime/agent` when the variable is unset.
The override must be absolute (a leading `~` is expanded). The same directory
drives both setup-owned ingest sources: root sessions under `sessions` and RLM
children under `session-artifacts`. Unrelated settings and skills are preserved;
a customized `mcpServers.moraine` or modified managed skill is reported as a
conflict instead of overwritten. Setup stores validated absolute paths to the
installed sibling `moraine-mcp` binary and Moraine config.

Start a fresh Prime Agent session after setup. The first managed Python kernel
may need network access to install `mcp>=1,<2` and `hatchling`. If
`PRIME_AGENT_KERNEL_PYTHON` selects a custom interpreter, setup installs files
but does not mutate that interpreter; activate the skill with the exact path
reported by setup, then verify `import mcp, moraine` in that interpreter.

The managed `moraine` module exposes MCP tools dynamically for session search,
opening results, ingest status, and realtime agent inspection. Like every
user-scoped integration, it can search host-wide Moraine history visible to your
user, so enable it only in a trusted harness environment.

Preview without writing:

```bash
moraine setup integrations prime-agent --dry-run
```

If setup reports a partial prior installation, keep Prime Agent stopped and rerun
setup. Move an intentionally customized `skills/moraine` or
`mcpServers.moraine` aside before asking setup to take ownership.

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
