# Moraine Hermes Plugin

This plugin gives Hermes the same Moraine guidance that the Claude Code and
Codex plugins bundle, while keeping the actual search implementation in the
Moraine MCP server.

Install from this repository:

```bash
hermes plugins install eric-tramel/moraine/plugins/hermes-moraine --enable
hermes moraine setup
hermes moraine doctor
```

What it adds:

- `moraine:session-search` and `moraine:realtime-peek` plugin skills.
- A compact context hook that reminds Hermes how to use Moraine when a turn
  asks about prior sessions, active agents, or agent history.
- `moraine_doctor`, a read-only tool for checking Moraine/Hermes wiring.
- `hermes moraine status`, `hermes moraine doctor`, and `hermes moraine setup`.

The plugin does not install the Moraine CLI and does not replace MCP. It expects
`moraine` on `PATH` and configures Hermes to launch `moraine run mcp`.
