# Configuration

Moraine reads TOML configuration. The default template is
`config/moraine.toml`, and runtime installs normally use
`~/.moraine/config.toml`.

## Resolution Order

The top-level `moraine` command resolves config in this order:

1. `--config <path>`
2. `MORAINE_CONFIG`
3. `~/.moraine/config.toml`, when it exists
4. `MORAINE_DEFAULT_CONFIG`, when it points at an existing file
5. `config/moraine.toml`, when running from a source checkout

Service-specific binaries accept the same `--config <path>` flag and add
service-specific environment variables:

| Service | Environment override |
| --- | --- |
| MCP | `MORAINE_MCP_CONFIG`, then `MORAINE_CONFIG` |
| Monitor | `MORAINE_MONITOR_CONFIG`, then `MORAINE_CONFIG` |
| Ingest | `MORAINE_INGEST_CONFIG`, then `MORAINE_CONFIG` |

## Minimal Example

Most users only need to override paths, ports, or watched sources:

```toml
[monitor]
host = "127.0.0.1"
port = 8080

[runtime]
root_dir = "~/.moraine"
start_monitor_on_up = true
start_mcp_on_up = false

[[ingest.sources]]
name = "codex"
harness = "codex"
enabled = true
glob = "~/.codex/sessions/**/*.jsonl"
watch_root = "~/.codex/sessions"
```

## ClickHouse

`[clickhouse]` controls database connectivity:

```toml
[clickhouse]
url = "http://127.0.0.1:8123"
database = "moraine"
username = "default"
password = ""
timeout_seconds = 30.0
async_insert = true
wait_for_async_insert = true
```

For a managed local install, leave these values at their defaults. For an
external ClickHouse instance, point `url`, credentials, and `database` at that
server.

## Ingest Sources

Each `[[ingest.sources]]` entry describes one watched source:

```toml
[[ingest.sources]]
name = "claude"
harness = "claude-code"
enabled = true
glob = "~/.claude/projects/**/*.jsonl"
watch_root = "~/.claude/projects"
format = "jsonl"
```

Supported `harness` values are `codex`, `claude-code`, `kimi-cli`, and
`hermes`. `glob` selects files to ingest. `watch_root` is the directory Moraine
watches for changes. `format` can usually be omitted; set it when a source needs
an explicit parser such as `jsonl` or `session_json`.

The `[ingest]` table controls batching and watcher behavior:

```toml
[ingest]
batch_size = 4000
flush_interval_seconds = 0.5
state_dir = "~/.moraine/ingestor"
backfill_on_start = true
max_file_workers = 8
max_inflight_batches = 64
```

## MCP

`[mcp]` sets defaults for agent retrieval:

```toml
[mcp]
max_results = 25
preview_chars = 320
default_context_before = 3
default_context_after = 3
default_include_tool_events = false
default_exclude_codex_mcp = true
async_log_writes = true
protocol_version = "2024-11-05"
```

Raise `max_results` only when clients need larger result windows. Increase
context defaults when retrieval snippets are too narrow.

## Search Ranking

`[bm25]` tunes search behavior:

```toml
[bm25]
k1 = 1.2
b = 0.75
default_min_score = 0.0
default_min_should_match = 1
max_query_terms = 32
```

Most installations should keep these defaults.

## Runtime Paths

`[runtime]` controls where Moraine keeps state and where it finds service
binaries:

```toml
[runtime]
root_dir = "~/.moraine"
logs_dir = "logs"
pids_dir = "run"
service_bin_dir = "~/.local/bin"
managed_clickhouse_dir = "~/.local/lib/moraine/clickhouse/current"
clickhouse_auto_install = true
start_monitor_on_up = true
start_mcp_on_up = false
```

Relative `logs_dir` and `pids_dir` values are resolved under `root_dir`.
`service_bin_dir` must contain `moraine-ingest`, `moraine-monitor`, and
`moraine-mcp`, unless `MORAINE_SERVICE_BIN_DIR` is set or source-tree mode is
enabled.
