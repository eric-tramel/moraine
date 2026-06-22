# Moraine for Hermes

Enable the plugin if you did not install with `--enable`:

```bash
hermes plugins enable moraine
```

Then register the Moraine MCP server in this Hermes profile:

```bash
hermes moraine setup
```

Check the integration:

```bash
hermes moraine doctor
```

Start a new Hermes session after setup. Ask:

```text
What are my agents doing right now?
```

The plugin adds Moraine guidance and diagnostics, but the search tools still
come from the MCP server launched as `moraine run mcp`.
