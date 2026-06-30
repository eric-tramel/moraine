# Configuration

Moraine reads TOML configuration. The default template is
`config/moraine.toml`, and runtime installs normally use
`~/.moraine/config.toml`.

For a copyable, full-file reference with comments beside every supported key,
see [Fully Commented Configuration TOML](full-config.toml). That file is
intended as a documentation reference; the shipped runtime template stays more
compact.

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

Unknown top-level sections and unknown keys inside normal config tables are
rejected at load time. The only intentionally loose file is a repo-level
`.moraine.toml`, which is a name-only backend reference and ignores unknown
future keys.

## Top-Level Sections

| Section | Purpose |
| --- | --- |
| `[clickhouse]` | Default ClickHouse backend used by local ingest, monitor, MCP, and migrations. |
| `[backends.<name>]` | Optional named ClickHouse backend for project mirroring and routed MCP. |
| `[[routes]]` | Optional ordered working-directory routes to named backends. |
| `[ingest]` | Ingest batching, backfill, watcher, checkpoint, and heartbeat settings. |
| `[[ingest.sources]]` | Watched agent trace sources. |
| `[mcp]` | MCP retrieval defaults and shared central server settings. |
| `[bm25]` | Search ranking defaults. |
| `[monitor]` | Monitor HTTP bind settings. |
| `[runtime]` | Runtime directories, service startup behavior, and managed ClickHouse settings. |

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
allow_newer_server = false
```

For a managed local install, leave these values at their defaults. For an
external ClickHouse instance, point `url`, credentials, and `database` at that
server. For a complete single-user walkthrough, including Docker and personal
server examples, see [Remote ClickHouse Tutorial](remote-clickhouse.md).

`[clickhouse]` is an alias for `[backends.default]`. To mirror specific
projects to additional servers, see
[Backends and Per-Project Routing](#backends-and-per-project-routing).

| Field | Default | Purpose |
| --- | --- | --- |
| `url` | `http://127.0.0.1:8123` | ClickHouse HTTP endpoint. Managed ClickHouse is started only for local URLs; remote endpoints must already be running. |
| `database` | `moraine` | Database containing Moraine tables. |
| `username` | `default` | ClickHouse user for ingest, monitor, MCP, and migrations. |
| `password` | empty | ClickHouse password. |
| `timeout_seconds` | `30.0` | Per-request ClickHouse HTTP timeout. |
| `async_insert` | `true` | Enables ClickHouse async insert mode on writes. |
| `wait_for_async_insert` | `true` | Waits for async insert completion before advancing checkpoints, so write failures are visible. |
| `allow_newer_server` | `false` | Allows a non-default backend whose migration ledger is ahead of this Moraine build. The default backend is migrated by Moraine itself, so this is only useful on `[backends.<name>]`. |

## Backends and Per-Project Routing

A project may belong to a team with a shared ClickHouse deployment. Named
backends plus routes mirror that project's sessions to the team server while
keeping the complete local history:

```toml
[backends.team-ch]
url = "https://ch.team.example:8443"
database = "moraine_team"
username = "svc-moraine"
password = "..."
allow_newer_server = false

[[routes]]
dir = "~/src/teamproject/**"
backend = "team-ch"
mode = "mirror"
```

### Named backends

Each `[backends.<name>]` block takes the same fields as `[clickhouse]`, plus
`allow_newer_server` (see the
[schema version handshake](#schema-version-handshake)). The `default` backend
always exists after config load: `[clickhouse]` and `[backends.default]` are
aliases for it, so when `[backends.default]` is not declared it is synthesized
from `[clickhouse]` (or built-in defaults). Declaring both blocks is a load
error.

### Routes

Each `[[routes]]` entry maps session working directories to a named backend.
Routes are ordered; the first matching route wins.

| Field | Behavior |
| --- | --- |
| `dir` | Directory glob matched against a session's absolute working directory. `~` expands during load. `*` stays within one path component; a glob ending in `/**` matches the base directory itself as well as everything beneath it (`~/p/**` matches `~/p`). |
| `backend` | Name of a `[backends.<name>]` entry. An unknown name here is a load error — the home config is user-owned, so typos fail loudly. Routing to `default` is accepted but is a no-op: the default backend already receives everything. |
| `mode` | Optional, defaults to `"mirror"` — the only supported mode. Other values (including `"exclusive"`) are rejected at load. |

A session's routing directory is sticky: the first non-empty working
directory observed for the session decides its route, so a mid-session `cd`
never splits a session across backends. The working directory is extracted
from session trace content during ingest (record-level where the harness
stamps it, session metadata otherwise); sessions whose traces carry no
discoverable working directory stay on the default backend.

### Repo-level `.moraine.toml`

A repository can opt into a backend without a home-config route by carrying a
`.moraine.toml` at its root:

```toml
backend = "team-ch"
```

Moraine walks up from the session's working directory, stopping at `$HOME` or
the filesystem root; the nearest file wins and ends the walk. The file is a
**name reference only** — never URLs or credentials — and resolves only
against `[backends.*]` entries in your home config. This is the trust
boundary: a hostile cloned repo cannot redirect traces to a server you never
configured. A name with no matching backend logs a warning (once per name)
and the session stays on the default backend. Unknown keys in the file are
ignored, and an explicit `[[routes]]` match in the home config takes
precedence over the repo file.

### Mirror semantics

Routing never replaces local history. The default backend receives every
session unconditionally; a routed session is *additionally* mirrored to its
backend. Each mirror runs its own sink with its own `ingest_checkpoints`
stored in that backend's database, scoped per host (migration 018), so team
members sharing one backend never disturb each other's mirror progress —
even when session files on two machines share an absolute path.

A slow or unreachable backend never stalls local ingest. Mirror forwarding
uses a bounded queue; on overflow the backend is marked lagging and live
mirroring to it pauses. The source session files on disk act as the
write-ahead log: at startup and whenever a lagging or unreachable backend
recovers, a targeted replay pass re-reads tracked files against that
backend's own checkpoints and closes the gap, without touching the default
sink. Files deleted before a backend catches up are lost to that backend only
(the local copy already ingested them live); Moraine logs when this happens.
One known limitation: rows observed before a session's working directory is
known resolve to the default backend only and are not retroactively
mirrored once the session pins to a route. The harness adapters keep this
window effectively empty (the working directory rides the records
themselves, or is recovered from the session header), so in practice it
only affects traces that carry no working directory at all.

Per-backend mirror status — `connecting`, `ok`, `lagging`, `unreachable`, or
`disabled_skew` — is written to ingest heartbeats as a `backend_sinks` map
and surfaced through the monitor's `/api/health`. This uses a column added by
migration 017; until `moraine db migrate` runs (and ingest restarts), ingest
warns and omits the field rather than failing heartbeats.

### Schema version handshake

Moraine migrates only the default backend (`moraine db migrate`, or
automatically on `moraine up`). It **never** runs migrations against a
non-default backend. Before mirroring starts, it compares the backend's
`schema_migrations` ledger against the migrations bundled in the running
build — a strictly read-only probe — and enforces:

| Skew | Outcome |
| --- | --- |
| Server behind (bundled migrations missing on the server) | Mirror disabled. The error names the backend and the missing versions; apply those migrations on the server first. |
| Server ahead (server-applied migrations unknown to this build) | Mirror disabled unless that backend sets `allow_newer_server = true`. Upgrade Moraine, or opt in. |

For ingest mirroring, a skew failure disables that one mirror until the
ingest service restarts and shows as `disabled_skew` in `/api/health`; the
default backend is unaffected. A backend that simply does not answer retries
the handshake periodically and shows as `unreachable`. The same handshake
also runs when `moraine run mcp` starts in a routed directory, where it
fails the MCP process instead (see below). The handshake exists to make skew
loud, not to manage it.

### MCP in routed directories

Routing changes what agents search, not just what ingest mirrors. When
`moraine run mcp` starts in a directory that routes to a non-default backend
(home-config `[[routes]]` first, then the repo `.moraine.toml` walk-up), it
skips the central proxy entirely — the central server only serves the
default backend — and runs an embedded server against the routed backend.
Search results then come from the team server, not from local history.

Startup in a routed directory is deliberately fail-fast: the schema
handshake runs first, and if the backend is skewed *or simply unreachable*,
`moraine run mcp` exits with an error naming the backend instead of falling
back to the default backend. Serving local results while the agent believes
it is searching the team server would be silently wrong. So if MCP dies at
startup in exactly one project, check that project's route and the team
backend's health/schema first. As with ingest routes, a repo `.moraine.toml`
naming an unconfigured backend logs a warning and keeps the default
behavior.

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

| Field | Default | Purpose |
| --- | --- | --- |
| `name` | empty string | Stable source name used in logs, checkpoints, heartbeats, and health output. |
| `harness` | empty string | Source normalizer. Must be one of the supported harness values below. |
| `enabled` | `true` | Keeps a source configured while allowing it to be skipped. |
| `glob` | empty string | Files this source ingests. `~` expands during config load. |
| `watch_root` | derived from `glob` when empty | Directory watched for changes. Set it explicitly when the glob root is ambiguous or platform-specific. |
| `format` | inferred from `harness` and `glob` | On-disk parser: `jsonl`, `session_json`, `cursor_sqlite`, or `opencode_sqlite`. |

Supported `harness` values are `codex`, `claude-code`, `cursor`, `kimi-cli`,
`opencode`, `hermes`, and `pi-coding-agent`. Each value maps to a registered
ingest source adapter; see
[Ingest Sources](development/ingest-sources.md) for the adapter contract and
[Harness Author Workflow](development/harness-author-workflow.md) for source
development steps.

`glob` selects files to ingest. `watch_root` is the directory Moraine watches
for changes. `format` controls the file parser:

| Format | Use for |
| --- | --- |
| `jsonl` | Append-only newline-delimited trace records. This is the default for most sources. |
| `session_json` | One JSON file per live session that is rewritten in place. Moraine emits only newly appended synthetic session records. |
| `cursor_sqlite` | Cursor `state.vscdb` SQLite databases. Moraine polls the database read-only and emits synthetic records for new or changed rows. |
| `opencode_sqlite` | OpenCode `opencode*.db` SQLite databases. Moraine polls the database read-only and emits synthetic records from append-only conversation events. |

When `format` is omitted, Moraine infers it. Hermes sources with a `.json` glob
are inferred as `session_json`, Cursor globs ending in `.vscdb` are inferred as
`cursor_sqlite`, OpenCode globs ending in `opencode.db` or `opencode*.db` are
inferred as `opencode_sqlite`, and otherwise sources are treated as `jsonl`.

## Source Matrix

The default template in `config/moraine.toml` includes these source families:

| Source | Harness | Default glob | Watch root | Format |
| --- | --- | --- | --- | --- |
| Codex | `codex` | `~/.codex/sessions/**/*.jsonl` | `~/.codex/sessions` | inferred `jsonl` |
| Claude Code | `claude-code` | `~/.claude/projects/**/*.jsonl` | `~/.claude/projects` | inferred `jsonl` |
| Kimi CLI | `kimi-cli` | `~/.kimi/sessions/**/wire.jsonl` | `~/.kimi/sessions` | inferred `jsonl` |
| OpenCode | `opencode` | `~/.local/share/opencode/opencode*.db` | `~/.local/share/opencode` | `opencode_sqlite` (default on) |
| Cursor Agent | `cursor` | `~/.cursor/projects/*/agent-transcripts/**/*.jsonl` | `~/.cursor/projects` | inferred `jsonl` |
| Cursor SQLite history | `cursor` | `~/Library/Application Support/Cursor/User/**/state.vscdb` (macOS) | `~/Library/Application Support/Cursor/User` | `cursor_sqlite` (default on) |
| Pi Coding Agent | `pi-coding-agent` | `~/.pi/agent/sessions/**/*.jsonl` | `~/.pi/agent/sessions` | `jsonl` |
| Hermes live sessions | `hermes` | `~/.hermes/sessions/session_*.json` | `~/.hermes/sessions` | `session_json` |
| Hermes trajectories | `hermes` | user-provided trajectory JSONL | trajectory output directory | `jsonl` |

Hermes supports both live session JSON and offline trajectory JSONL because the
harness is the same but the file format differs. Use a separate
`[[ingest.sources]]` entry for each watched directory. Cursor likewise has two
trace forms under one harness: Agent transcript JSONL and SQLite chat history
(`cursor_sqlite`); both are enabled by default. OpenCode stores conversation
history in default or channel-specific SQLite databases (`opencode_sqlite`);
the template enables it by default.

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

OpenCode:

```toml
[[ingest.sources]]
name = "opencode"
harness = "opencode"
enabled = true
glob = "~/.local/share/opencode/opencode*.db"
watch_root = "~/.local/share/opencode"
format = "opencode_sqlite"
```

This source polls OpenCode's `opencode*.db` databases read-only and ingests
conversation records synthesized from the `event` and `event_sequence` tables.
Account, credential, and token-bearing tables are deliberately out of scope.
Moraine also reacts to the `-wal`/`-shm` sidecar files so WAL-only writes
trigger polls.

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

This source watches local Agent JSONL transcripts under `agent-transcripts/`.
Cursor's IDE chat history lives elsewhere — in `state.vscdb` SQLite databases —
and is ingested by the separate `cursor_sqlite` source below.

Cursor SQLite history (macOS):

```toml
[[ingest.sources]]
name = "cursor-sqlite"
harness = "cursor"
enabled = true
glob = "~/Library/Application Support/Cursor/User/**/state.vscdb"
watch_root = "~/Library/Application Support/Cursor/User"
format = "cursor_sqlite"
```

Cursor SQLite history (Linux):

```toml
[[ingest.sources]]
name = "cursor-sqlite"
harness = "cursor"
enabled = true
glob = "~/.config/Cursor/User/**/state.vscdb"
watch_root = "~/.config/Cursor/User"
format = "cursor_sqlite"
```

This source polls Cursor's `state.vscdb` databases read-only and ingests
composer sessions and message bubbles (session titles, chat turns, tool calls).
The conversation data lives in the `globalStorage` database; per-workspace
databases under `workspaceStorage` match the same glob but are mostly empty.
Moraine also reacts to the `-wal`/`-shm` sidecar files so WAL-only writes
trigger polls; `state.vscdb.backup` files are ignored.

This source ships enabled. Cursor has no stable local-database contract, so an
editor update can change the schema at any time (issue #361) — tracking that
drift is Moraine's job. When the schema drifts, Moraine reports rate-limited
`sqlite_*` ingest errors and skips the database rather than ingesting bad rows,
until the normalizer is updated to follow the new format. Set `enabled = false`
if you don't want IDE chat history ingested.

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
debounce_ms = 50
reconcile_interval_seconds = 30.0
heartbeat_interval_seconds = 5.0
```

| Field | Default | Purpose |
| --- | --- | --- |
| `batch_size` | `4000` | Maximum rows collected before a sink flushes to ClickHouse. |
| `max_batch_bytes` | `8388608` | Maximum serialized JSONEachRow batch size, in bytes, before flushing. |
| `flush_interval_seconds` | `0.5` | Maximum time a non-empty batch waits before flushing. |
| `state_dir` | `~/.moraine/ingestor` | Directory for ingest checkpoints and local ingestor state. |
| `backfill_on_start` | `true` | Enumerates matching files at startup and ingests records newer than stored checkpoints. |
| `max_file_workers` | `8` | Maximum source files processed concurrently during enumeration, backfill, and replay. |
| `max_inflight_batches` | `16` | Maximum pending batches between processors and sink tasks. |
| `debounce_ms` | `50` | Milliseconds to coalesce repeated filesystem notifications for the same tracked file. |
| `reconcile_interval_seconds` | `30.0` | Seconds between reconciliation scans for missed watcher events, deleted files, and lagging backend replay. |
| `heartbeat_interval_seconds` | `5.0` | Seconds between ingest heartbeat writes. |

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

| Field | Default | Purpose |
| --- | --- | --- |
| `max_results` | `25` | Default result limit for MCP tools when a request omits an explicit limit. |
| `preview_chars` | `320` | Text preview characters included per result row. |
| `default_context_before` | `3` | Default number of records included before a matched event. |
| `default_context_after` | `3` | Default number of records included after a matched event. |
| `default_include_tool_events` | `false` | Includes tool-call and tool-result events in MCP responses by default. |
| `default_exclude_codex_mcp` | `true` | Filters Moraine's own Codex MCP traffic by default to reduce self-noise. |
| `prewarm_on_initialize` | `false` | Warms query metadata during MCP initialize, trading startup work for lower first-search latency. |
| `async_log_writes` | `true` | Writes MCP observability rows asynchronously so tool calls stay responsive. |
| `protocol_version` | `2024-11-05` | MCP protocol version advertised by the server. |
| `use_central_server` | `true` | Makes `moraine run mcp` prefer the shared central server socket, with embedded fallback. |
| `start_central_on_up` | `true` | Starts the shared central MCP server when `moraine up` starts services. |
| `central_socket_path` | `mcp.sock` | Unix socket path. Bare filenames resolve under `runtime.pids_dir`; absolute paths are used verbatim. |
| `central_connect_timeout_ms` | `250` | Milliseconds a proxy client waits for the central socket before falling back to embedded mode. |

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
| `use_central_server` | `true` | When set, `moraine run mcp` connects to the central server's socket and proxies to it; if the socket is missing or unreachable it transparently falls back to an embedded server. Set to `false` to always run embedded (pre-central behavior). In a directory routed to a non-default backend the central proxy is bypassed regardless of this flag (see [MCP in routed directories](#mcp-in-routed-directories)). |
| `start_central_on_up` | `true` | When set, `moraine up` launches the central server as a background daemon. |
| `central_socket_path` | `mcp.sock` | Unix socket path. A bare filename resolves under the runtime pids dir (`~/.moraine/run/mcp.sock`, mode `0o600`); an absolute path is used verbatim. |
| `central_connect_timeout_ms` | `250` | How long a client waits to connect before falling back to embedded. |

The MCP registration command is unchanged — agents still launch
`moraine run mcp`. The proxy-vs-embedded decision is internal, with one
exception: directories routed to a non-default backend always run embedded
against that backend and fail fast when it is unreachable or skewed (see
[MCP in routed directories](#mcp-in-routed-directories)). The daemon and
its clients must resolve the same `central_socket_path` (i.e. load the same
config) to share a server; otherwise clients silently fall back to embedded.
The `0o600` socket scopes the server to a single user, so on a shared host each
user runs their own central server. See
[Agent MCP Search → Install](agent-mcp-search/install.md#shared-central-server-default)
for operational notes.

One exception: `moraine run mcp --project-only` (retrieval restricted to
sessions that originated from the launch directory) always runs an embedded
server, because the shared central server serves every project on the host.
See
[Agent MCP Search → Install](agent-mcp-search/install.md#project-scoped-retrieval-project-only).

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

| Field | Default | Purpose |
| --- | --- | --- |
| `k1` | `1.2` | BM25 term-frequency saturation. Larger values give repeated terms more influence. |
| `b` | `0.75` | BM25 length normalization. `0` disables length normalization; `1` applies full normalization. |
| `default_min_score` | `0.0` | Default minimum BM25 score when a request omits `min_score`. |
| `default_min_should_match` | `1` | Default minimum number of query terms that should match. |
| `max_query_terms` | `32` | Maximum query terms kept after tokenization. |

## Monitor

`[monitor]` controls the monitor HTTP server:

```toml
[monitor]
host = "127.0.0.1"
port = 8080
```

| Field | Default | Purpose |
| --- | --- | --- |
| `host` | `127.0.0.1` | Interface the monitor binds. Keep the default for local-only access. |
| `port` | `8080` | Monitor HTTP port. |

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
clickhouse_start_timeout_seconds = 30.0
healthcheck_interval_ms = 500
clickhouse_auto_install = true
clickhouse_version = "v25.12.5.44-stable"
start_monitor_on_up = true
start_mcp_on_up = false
```

Relative `logs_dir` and `pids_dir` values are resolved under `root_dir`.
`service_bin_dir` must contain `moraine-ingest`, `moraine-monitor`, and
`moraine-mcp`, unless `MORAINE_SERVICE_BIN_DIR` is set or source-tree mode is
enabled.

| Field | Default | Purpose |
| --- | --- | --- |
| `root_dir` | `~/.moraine` | Root directory for Moraine runtime state. |
| `logs_dir` | `logs` | Log directory. Relative paths resolve under `root_dir`. |
| `pids_dir` | `run` | PID and socket directory. Relative paths resolve under `root_dir`. |
| `service_bin_dir` | `~/.local/bin` | Directory containing installed service binaries. |
| `managed_clickhouse_dir` | `~/.local/lib/moraine/clickhouse/current` | Directory for the managed ClickHouse installation. |
| `clickhouse_start_timeout_seconds` | `30.0` | Seconds to wait for managed ClickHouse to become healthy during startup. |
| `healthcheck_interval_ms` | `500` | Milliseconds between service health checks while waiting for startup. |
| `clickhouse_auto_install` | `true` | Automatically installs the managed ClickHouse binary when needed. |
| `clickhouse_version` | `v25.12.5.44-stable` | Managed ClickHouse release tag expected by this Moraine build. |
| `start_monitor_on_up` | `true` | Starts the monitor service when `moraine up` starts services. |
| `start_mcp_on_up` | `false` | Deprecated compatibility flag. Existing configs may keep it; new configs should use `mcp.start_central_on_up`. |
