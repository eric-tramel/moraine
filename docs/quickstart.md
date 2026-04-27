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

For Claude Code, add Moraine to `.mcp.json`:

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

For Codex, add the MCP server from a shell:

```bash
codex mcp add moraine -- moraine run mcp
```

The MCP server uses the same config resolution rules as the rest of Moraine, with
`MORAINE_MCP_CONFIG` taking precedence over the generic `MORAINE_CONFIG`.

## Common Commands

| Command | Purpose |
| --- | --- |
| `moraine up` | Start ClickHouse, ingest, and the monitor UI. |
| `moraine up --mcp` | Start the MCP server along with the default services. |
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
