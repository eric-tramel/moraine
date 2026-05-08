# Install by Harness

Moraine MCP search is a stdio MCP server. Configure your harness to launch:

```bash
moraine run mcp
```

Run `moraine up` first so ClickHouse, ingest, and the monitor are available.
If you use a non-default Moraine config, pass it through the harness's
environment support as `MORAINE_MCP_CONFIG=/path/to/moraine.toml`.

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
