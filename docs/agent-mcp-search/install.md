# Install by Harness

Moraine MCP search is a stdio MCP server. Configure your harness to launch:

```bash
moraine run mcp
```

Run `moraine up` first so ClickHouse, ingest, and the monitor are available.
If you use a non-default Moraine config, pass it through the harness's
environment support as `MORAINE_MCP_CONFIG=/path/to/moraine.toml`.

## Shared central server (default)

By default `moraine up` starts a single shared MCP server for the whole host,
and every `moraine run mcp` becomes a thin stdio↔socket proxy to it. This
replaces the previous model of booting a full MCP server (its own ClickHouse
client, caches, and runtime threads) inside every agent session, and is what
keeps hundreds of concurrent sessions cheap.

What this means for you:

- **Registration is unchanged.** Keep registering `moraine run mcp` exactly as
  shown below. The proxy-vs-embedded choice is made internally.
- **`moraine up` is required** for the shared server. The daemon listens on a
  Unix socket at `~/.moraine/run/mcp.sock` (mode `0o600`, so it is scoped to
  your user). `moraine down` stops it and removes the socket.
- **Automatic fallback.** If the central server is not running (you skipped
  `moraine up`, or it crashed), `moraine run mcp` transparently falls back to an
  embedded server after ~250&nbsp;ms, so retrieval keeps working either way.
- **Crash blast radius.** A central-server crash drops all live sessions' MCP
  connections at once; harnesses re-establish the connection on their next tool
  use, and `moraine up` restarts the daemon. To opt out and return to a server
  per session, set `use_central_server = false` (see
  [Configuration → MCP](../configuration.md#shared-central-mcp-server)).

## Project-scoped retrieval (`--project-only`)

Add `--project-only` to restrict retrieval to sessions that originated from
the directory the server is launched in:

```bash
claude mcp add --transport stdio --scope project moraine -- moraine run mcp --project-only
```

With the flag set, `search_sessions`, `list_sessions`, and `open` only see
sessions whose recorded working directory is the launch directory or a
subdirectory of it (worktrees under a repo root count). Opening an ID from
another project answers `not_found`, exactly as if the session did not exist.

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
each harness.

## Global Codex

Codex stores user-level configuration in `~/.codex/config.toml`, and the Codex
CLI can add MCP servers directly. OpenAI's Codex docs note that the CLI and IDE
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

## Claude Code

Claude Code supports stdio MCP servers through `claude mcp add`. Use user scope
when you want Moraine available in every project; use project scope when you want
to commit a `.mcp.json` for a repository. See the official
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

Hermes has an MCP manager command. Add Moraine as a stdio server:

```bash
hermes mcp add moraine --command moraine --args run mcp
hermes mcp list
hermes mcp test moraine
```

If you run Hermes profiles, add the server in each profile that should be able
to search Moraine history.

## Kimi CLI

Kimi CLI has built-in MCP configuration commands. Its MCP reference describes
`kimi mcp add` with `stdio` and `http` transports:
[Kimi MCP reference](https://moonshotai.github.io/kimi-cli/en/reference/kimi-mcp.html).

```bash
kimi mcp add --transport stdio moraine -- moraine run mcp
kimi mcp list
kimi mcp test moraine
```

## Cursor

Cursor and `cursor-agent` read MCP server definitions from `mcp.json`. Cursor's
docs describe project config at `.cursor/mcp.json`, global config at
`~/.cursor/mcp.json`, and CLI inspection through `cursor-agent mcp`:
[Cursor MCP guide](https://docs.cursor.com/advanced/model-context-protocol) and
[Cursor CLI MCP guide](https://docs.cursor.com/cli/mcp).

For global use, create or update `~/.cursor/mcp.json`:

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

Then verify from the CLI when `cursor-agent` is installed:

```bash
cursor-agent mcp list
cursor-agent mcp list-tools moraine
```

For project-only use, put the same JSON in `.cursor/mcp.json` at the project
root.

## Pi Coding Agent

Pi uses an extension to bridge MCP servers into Pi tools. Install the MCP
extension, then add a Moraine stdio server to Pi's MCP config. The extension
docs describe global config at `~/.pi/agent/mcp.json`, project config at
`.pi/mcp.json`, and stdio server fields:
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
`mcp_moraine_list_sessions`. Use `/mcp` inside Pi to inspect server status.

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
