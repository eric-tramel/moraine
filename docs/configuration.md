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

Supported `harness` values are `codex`, `claude-code`, `cursor`, `kimi-cli`,
`hermes`, and `pi-coding-agent`. Each value maps to a registered ingest source
adapter; see
[Ingest Sources](development/ingest-sources.md) for the adapter contract and
[Harness Author Workflow](development/harness-author-workflow.md) for source
development steps.

`glob` selects files to ingest. `watch_root` is the directory Moraine watches
for changes. `format` controls the file parser:

| Format | Use for |
| --- | --- |
| `jsonl` | Append-only newline-delimited trace records. This is the default for most sources. |
| `session_json` | One JSON file per live session that is rewritten in place. Moraine emits only newly appended synthetic session records. |

When `format` is omitted, Moraine infers it. Hermes sources with a `.json` glob
are inferred as `session_json`; otherwise sources are treated as `jsonl`.

## Source Matrix

The default template in `config/moraine.toml` enables these source families:

| Source | Harness | Default glob | Watch root | Format |
| --- | --- | --- | --- | --- |
| Codex | `codex` | `~/.codex/sessions/**/*.jsonl` | `~/.codex/sessions` | inferred `jsonl` |
| Claude Code | `claude-code` | `~/.claude/projects/**/*.jsonl` | `~/.claude/projects` | inferred `jsonl` |
| Kimi CLI | `kimi-cli` | `~/.kimi/sessions/**/wire.jsonl` | `~/.kimi/sessions` | inferred `jsonl` |
| Cursor Agent | `cursor` | `~/.cursor/projects/*/agent-transcripts/**/*.jsonl` | `~/.cursor/projects` | inferred `jsonl` |
| Pi Coding Agent | `pi-coding-agent` | `~/.pi/agent/sessions/**/*.jsonl` | `~/.pi/agent/sessions` | `jsonl` |
| Hermes live sessions | `hermes` | `~/.hermes/sessions/session_*.json` | `~/.hermes/sessions` | `session_json` |
| Hermes trajectories | `hermes` | user-provided trajectory JSONL | trajectory output directory | `jsonl` |

Hermes supports both live session JSON and offline trajectory JSONL because the
harness is the same but the file format differs. Use a separate
`[[ingest.sources]]` entry for each watched directory.

## Source Examples

Codex:

```toml
[[ingest.sources]]
name = "codex"
harness = "codex"
enabled = true
glob = "~/.codex/sessions/**/*.jsonl"
watch_root = "~/.codex/sessions"
```

Claude Code:

```toml
[[ingest.sources]]
name = "claude"
harness = "claude-code"
enabled = true
glob = "~/.claude/projects/**/*.jsonl"
watch_root = "~/.claude/projects"
```

Kimi CLI:

```toml
[[ingest.sources]]
name = "kimi-cli"
harness = "kimi-cli"
enabled = true
glob = "~/.kimi/sessions/**/wire.jsonl"
watch_root = "~/.kimi/sessions"
format = "jsonl"
```

Cursor Agent JSONL:

```toml
[[ingest.sources]]
name = "cursor"
harness = "cursor"
enabled = true
glob = "~/.cursor/projects/*/agent-transcripts/**/*.jsonl"
watch_root = "~/.cursor/projects"
format = "jsonl"
```

Cursor support watches local Agent JSONL transcripts under
`agent-transcripts/`. Cursor SQLite workspace history is not ingested by this
source.

Pi Coding Agent:

```toml
[[ingest.sources]]
name = "pi"
harness = "pi-coding-agent"
enabled = true
glob = "~/.pi/agent/sessions/**/*.jsonl"
watch_root = "~/.pi/agent/sessions"
format = "jsonl"
```

Hermes live sessions:

```toml
[[ingest.sources]]
name = "hermes"
harness = "hermes"
enabled = true
glob = "~/.hermes/sessions/session_*.json"
watch_root = "~/.hermes/sessions"
format = "session_json"
```

Hermes trajectories:

```toml
[[ingest.sources]]
name = "hermes-trajectories"
harness = "hermes"
enabled = true
glob = "~/trajectories/**/*.jsonl"
watch_root = "~/trajectories"
format = "jsonl"
```

## Adding Harnesses

The config crate validates harness names before ingest-core runs, but
`moraine-config` cannot call the ingest adapter registry without creating a
dependency cycle. Adding a harness therefore requires coordinated updates:

- Add and register the adapter under `crates/moraine-ingest-core/src/sources/`.
- Add the harness string to `moraine_config::KNOWN_INGEST_HARNESSES`.
- Update defaults in `crates/moraine-config/src/lib.rs` and
  `config/moraine.toml` when the source should ship enabled by default.
- Update this page and the ingest source development docs.
- Run the registry/config sync tests and ingest fixture contract tests.

The `[ingest]` table controls batching and watcher behavior:

```toml
[ingest]
batch_size = 4000
max_batch_bytes = 8388608
flush_interval_seconds = 0.5
state_dir = "~/.moraine/ingestor"
backfill_on_start = true
max_file_workers = 8
max_inflight_batches = 16
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
prewarm_on_initialize = false
async_log_writes = true
protocol_version = "2024-11-05"
use_central_server = true
start_central_on_up = true
central_socket_path = "mcp.sock"
central_connect_timeout_ms = 250
```

Raise `max_results` only when clients need larger result windows. Increase
context defaults when retrieval snippets are too narrow.
Leave `prewarm_on_initialize` disabled for harnesses that launch multiple MCP
processes at once; enabling it trades startup CPU/database work for lower
first-search latency.

### Shared central MCP server

By default Moraine runs a single shared MCP server per host instead of one
full server per agent session, which sharply reduces CPU and memory when many
sessions are active at once. The fields above control it:

| Field | Default | Purpose |
| --- | --- | --- |
| `use_central_server` | `true` | When set, `moraine run mcp` connects to the central server's socket and proxies to it; if the socket is missing or unreachable it transparently falls back to an embedded server. Set to `false` to always run embedded (pre-central behavior). |
| `start_central_on_up` | `true` | When set, `moraine up` launches the central server as a background daemon. |
| `central_socket_path` | `mcp.sock` | Unix socket path. A bare filename resolves under the runtime pids dir (`~/.moraine/run/mcp.sock`, mode `0o600`); an absolute path is used verbatim. |
| `central_connect_timeout_ms` | `250` | How long a client waits to connect before falling back to embedded. |

The MCP registration command is unchanged — agents still launch
`moraine run mcp`. The proxy-vs-embedded decision is internal. The daemon and
its clients must resolve the same `central_socket_path` (i.e. load the same
config) to share a server; otherwise clients silently fall back to embedded.
The `0o600` socket scopes the server to a single user, so on a shared host each
user runs their own central server. See
[Agent MCP Search → Install](agent-mcp-search/install.md#shared-central-server-default)
for operational notes.

The up-managed MCP service is always the central socket server; the legacy
per-`up` stdio daemon is gone (v0.6.0). `runtime.start_mcp_on_up` is
deprecated: it is still parsed so existing configs keep loading, and it now
acts as a force-on alias for the central server (the same as `moraine up
--mcp`). Use `mcp.start_central_on_up` instead.

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
```

Relative `logs_dir` and `pids_dir` values are resolved under `root_dir`.
`service_bin_dir` must contain `moraine-ingest`, `moraine-monitor`, and
`moraine-mcp`, unless `MORAINE_SERVICE_BIN_DIR` is set or source-tree mode is
enabled.
