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
| `[identity]` | Explicit person identity stamped on ingested raw/events rows. |
| `[ingest]` | Ingest batching, backfill, watcher, checkpoint, and heartbeat settings. |
| `[[ingest.sources]]` | Watched agent trace sources. |
| `[mcp]` | MCP retrieval defaults and shared backend socket settings. |
| `[bm25]` | Search ranking defaults. |
| `[query_budgets]` | Per-class ClickHouse query budgets (deadline, memory, spill, read allowances, statement cap) for the mandatory query envelope. |
| `[monitor]` | Monitor HTTP port. |
| `[backend]` | Unified MCP socket and monitor HTTP daemon startup, HTTP bind, and experimental non-loopback guard. |
| `[runtime]` | Runtime directories and managed ClickHouse settings. |

## Minimal Example

Most users only need to override paths, ports, or watched sources:

```toml
[backend]
bind = "127.0.0.1"
start_on_up = true

[monitor]
port = 8080

[runtime]
root_dir = "~/.moraine"

[[ingest.sources]]
name = "codex"
harness = "codex"
enabled = true
glob = "~/.codex/sessions/**/*.jsonl"
watch_root = "~/.codex/sessions"
```

## Identity

`[identity]` configures the stable person identity stamped on newly ingested
`raw_events` and `events` rows:

```toml
[identity]
author = "alice@example.com"
```

The value is free-form by design; email-shaped strings are conventional but
not enforced. Moraine never infers this identity from git, `$USER`,
`$HOSTNAME`, the machine host, or the working directory. Missing and
whitespace-only values normalize to empty.

Local/default-only installs may leave `author` empty. If any route targets a
non-default backend and `author` is empty, that mirror is disabled for the
process and reported as `disabled_missing_identity_author`; default ingest
continues. After setting `author`, restart the ingest service. Retained source
files are then replayed to the mirror using the configured author; files
deleted before the mirror catches up cannot be recovered by this replay path.

## ClickHouse

`[clickhouse]` controls database connectivity:

```toml
[clickhouse]
url = "http://127.0.0.1:8123"
database = "moraine"
username = "default"
password = ""
timeout_seconds = 30.0
request_compression = "none"
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
| `request_compression` | `none` | Compression for non-empty HTTP request bodies. Supported values are `none` and `gzip`. |
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
request_compression = "gzip"
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

Shared-backend ownership uses a random installation UUID persisted at
`<ingest.state_dir>/publication-host-id` (normally
`~/.moraine/ingestor/publication-host-id`), not `HOSTNAME` or `USER`. Moraine
creates the file atomically with owner-only permissions where the platform
supports them, syncs it durably, and reuses the same ID after restart. A missing
file creates a new identity; malformed, insecure, or unreadable files stop
ingest before any publisher or sink starts. Keep this file when moving the
state directory for the same installation, but do not copy it to another
machine.

The first #602-capable run intentionally does **not** adopt or merge checkpoint
or source-host keys written by older releases under an environment-derived
hostname/username. When a new identity is created while named backends are
configured, ingest emits a migration warning and mirror catch-up replays
tracked sources under the new UUID. It never rewrites or claims the old key.
Previously published rows under a nonempty legacy key remain a separate
historical publisher and can therefore duplicate the replayed history until an
administrator retires or rebuilds that legacy backend data. Hostless legacy
rows remain fail-closed and are surfaced by publication diagnostics. Review
the startup warning and clean up the old publisher deliberately; copying its
key into `publication-host-id` is rejected because the file accepts only a
canonical random UUID.

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

Per-backend mirror status — `connecting`, `ok`, `lagging`, `unreachable`,
`disabled_skew`, or `disabled_missing_identity_author` — is written to ingest
heartbeats as a `backend_sinks` map and surfaced through the monitor's
`/api/v1/health`. This uses a column added by migration 017; until
`moraine db migrate` runs (and ingest restarts), ingest warns and omits the
field rather than failing heartbeats. `disabled_missing_identity_author` means
`[identity].author` is empty; set it and restart ingest before mirroring to
team backends.

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
ingest service restarts and shows as `disabled_skew` in `/api/v1/health`; the
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

## Secret Redaction

Secret redaction is enabled by default for every ingested row. Moraine scans
the secret-bearing string fields in raw events, normalized events, and tool
I/O, then replaces matched secrets with typed placeholders such as
`[REDACTED:github-token]`. Derived previews and hashes are recomputed from
the redacted content.

```toml
[redaction]
ruleset = "builtin"
# extra_patterns = ["acme_internal_[A-Za-z0-9]{32}"]
# dangerously_skip_secret_redaction = true
```

`dangerously_skip_secret_redaction = true` is honored only from
`$HOME/.moraine/config.toml`. It disables redaction for the local/default
backend only; mirror egress to named backends is always redacted. The same
flag in a repo or checkout config is ignored with a warning so a cloned
repository cannot weaken local capture settings.

The builtin ruleset covers common API keys, provider tokens, private-key
blocks, JWTs, and high-entropy generic credential assignments. `extra_patterns`
adds local Rust regular expressions that redact the full matched span with
`[REDACTED:extra-pattern-N]`; invalid patterns are a startup error. Redaction
counts are written to ingest heartbeats in `redactions_total` after migration
022; before that migration, redaction still runs and ingest logs that the
heartbeat audit column is not available.

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
| `format` | inferred from `harness` and `glob` | On-disk parser: `jsonl`, `session_json`, `kiro_session`, `cursor_sqlite`, `nac_sqlite`, or `opencode_sqlite`. |

Supported `harness` values are `codex`, `claude-code`, `cursor`, `hermes`,
`kiro-cli`, `kimi-cli`, `nac`, `opencode`, `pi-coding-agent`, and `qwen-code`.
Each value maps to a
registered ingest source adapter; see
[Ingest Sources](development/ingest-sources.md) for the adapter contract and
[Harness Author Workflow](development/harness-author-workflow.md) for source
development steps.

`glob` selects files to ingest. `watch_root` is the directory Moraine watches
for changes. `format` controls the file parser:

| Format | Use for |
| --- | --- |
| `jsonl` | Append-only newline-delimited trace records. This is the default for most sources. |
| `session_json` | One JSON file per live session that is rewritten in place. Moraine emits only newly appended synthetic session records. |
| `kiro_session` | Kiro CLI append-only JSONL transcripts paired with same-named JSON metadata. Changes to either file enqueue the transcript. |
| `cursor_sqlite` | Cursor `state.vscdb` SQLite databases. Moraine polls the database read-only and emits synthetic records for new or changed rows. |
| `nac_sqlite` | NAC `store.db` databases. Moraine polls parent sessions and managed-worker episodes read-only with a persisted incremental cursor. |
| `opencode_sqlite` | OpenCode `opencode*.db` SQLite databases. Moraine polls the database read-only and emits synthetic records from append-only conversation events. |

When `format` is omitted, Moraine infers it. Hermes sources with a `.json` glob
are inferred as `session_json`, Kiro CLI sources as `kiro_session`, Cursor globs
ending in `.vscdb` as `cursor_sqlite`, NAC globs whose filename is `store.db` as
`nac_sqlite`, OpenCode globs ending in `opencode.db` or `opencode*.db` as
`opencode_sqlite`, and otherwise sources are treated as `jsonl`.

## Source Matrix

The built-in defaults and `config/moraine.toml` reference cover these source families:

| Source | Harness | Default glob | Watch root | Format |
| --- | --- | --- | --- | --- |
| Codex | `codex` | `~/.codex/sessions/**/*.jsonl` | `~/.codex/sessions` | inferred `jsonl` |
| Claude Code | `claude-code` | `~/.claude/projects/**/*.jsonl` | `~/.claude/projects` | inferred `jsonl` |
| Claude Cowork (local macOS) | `claude-code` | `~/Library/Application Support/Claude/local-agent-mode-sessions/**/.claude/projects/**/*.jsonl` | `~/Library/Application Support/Claude/local-agent-mode-sessions` | inferred `jsonl` |
| Kiro CLI | `kiro-cli` | `$KIRO_HOME/sessions/cli/*.jsonl` when set; otherwise `~/.kiro/sessions/cli/*.jsonl` | matching `sessions/cli` directory | `kiro_session` |
| Kimi CLI | `kimi-cli` | `~/.kimi/sessions/**/wire.jsonl` | `~/.kimi/sessions` | inferred `jsonl` |
| Qwen Code | `qwen-code` | `~/.qwen/projects/*/chats/*.jsonl` | `~/.qwen/projects` | `jsonl` |
| NAC | `nac` | setup-resolved `store.db` | resolved NAC config directory | `nac_sqlite` (setup-managed) |
| OpenCode | `opencode` | `~/.local/share/opencode/opencode*.db` | `~/.local/share/opencode` | `opencode_sqlite` (default on) |
| Cursor Agent | `cursor` | `~/.cursor/projects/*/agent-transcripts/**/*.jsonl` | `~/.cursor/projects` | inferred `jsonl` |
| Cursor SQLite history | `cursor` | `~/Library/Application Support/Cursor/User/**/state.vscdb` (macOS) | `~/Library/Application Support/Cursor/User` | `cursor_sqlite` (default on) |
| Pi Coding Agent (historical) | `pi-coding-agent` | `~/.pi/agent/sessions/**/*.jsonl` | `~/.pi/agent/sessions` | `jsonl` |
| OMP (oh-my-pi) | `pi-coding-agent` | `~/.omp/agent/sessions/**/*.jsonl` | `~/.omp/agent/sessions` | `jsonl` |
| Hermes live sessions | `hermes` | `~/.hermes/sessions/session_*.json` | `~/.hermes/sessions` | `session_json` |
| Hermes trajectories | `hermes` | user-provided trajectory JSONL | trajectory output directory | `jsonl` |

Hermes supports both live session JSON and offline trajectory JSONL because the
harness is the same but the file format differs. Use a separate
`[[ingest.sources]]` entry for each watched directory. Cursor likewise has two
trace forms under one harness: Agent transcript JSONL and SQLite chat history
(`cursor_sqlite`); both are enabled by default. Kiro CLI stores transcript records in
`<session-id>.jsonl` and session-level cwd, title, model, and token totals in
`<session-id>.json`; the paired `kiro_session` format watches both files and
checkpoints sidecar changes independently of transcript growth. Running
`moraine setup` with the Kiro target rewrites the setup-owned `kiro` source to
use `$KIRO_HOME/sessions/cli` when `KIRO_HOME` is set. NAC stores parent sessions and
managed-worker episodes in `store.db` (`nac_sqlite`); guided `moraine setup`
materializes this source only when NAC's effective store path is stable.
OpenCode stores conversation history in default or channel-specific SQLite databases
(`opencode_sqlite`); the template enables it by default.

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

Local Claude Cowork on macOS:

```toml
[[ingest.sources]]
name = "claude-cowork"
harness = "claude-code"
enabled = true
glob = "~/Library/Application Support/Claude/local-agent-mode-sessions/**/.claude/projects/**/*.jsonl"
watch_root = "~/Library/Application Support/Claude/local-agent-mode-sessions"
```

macOS defaults and `moraine setup` add this as a separate source while retaining
the ordinary `claude` source. Moraine groups every nested Claude CLI transcript
under its containing `local_*` Cowork session. The live ingest gate accepts only
the `.claude/projects/` transcript subtree, so sibling `audit.jsonl` files are
not ingested.

Cowork root metadata is allowlisted into one `session_meta` record: session and
nested CLI IDs, created/last-activity times, cwd, model, title, and archive/star
flags. Account/email data, prompts, selected folders, MCP/plugin/tool settings,
and unknown fields are not copied. Transcript `attachment`, `last-prompt`, and
`ai-title` records remain available as raw records but do not become searchable
events.
Root metadata is refreshed when a transcript is created or appended. A
metadata-only sidecar edit is picked up by the next transcript activity.

This source supports the observed local macOS layout only. Claude's JSONL format
is internal and may change. A stable signed-in Claude Desktop path has not been
verified across Windows installer/MSIX variants, so Moraine does not ship a
Windows Cowork glob. Remote Cowork is also outside this source; its documented
observability path is OpenTelemetry.

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

Qwen Code:

```toml
[[ingest.sources]]
name = "qwen-code"
harness = "qwen-code"
enabled = true
glob = "~/.qwen/projects/*/chats/*.jsonl"
watch_root = "~/.qwen/projects"
format = "jsonl"
```

For a custom Qwen storage root, keep the normalized `qwen-code` harness and use
a distinct source name so `moraine setup` leaves the custom entry untouched:

```toml
[[ingest.sources]]
name = "qwen-code-archive"
harness = "qwen-code"
enabled = true
glob = "/srv/qwen-archive/projects/*/chats/*.jsonl"
watch_root = "/srv/qwen-archive/projects"
format = "jsonl"
```

The built-in adapter is fixture-tested against Qwen Code 0.19.x's internal
append-only `ChatRecord` shape; that upstream persistence format is not a stable
public API.

NAC:

```toml
[[ingest.sources]]
name = "nac"
harness = "nac"
enabled = true
glob = "~/.config/nac/store.db"
watch_root = "~/.config/nac"
format = "nac_sqlite"
```

This source polls NAC's SQLite store read-only. It indexes parent sessions,
assistant reasoning, tool request/response pairs, token usage, and
managed-worker episodes. Durable remote-session and remote-worker bodies are
retained but receive no local project/worktree attribution. Credential-bearing
columns and host identifiers are never selected. Qualified NAC tool names such
as `mcp__moraine__search_sessions` normalize to canonical Moraine tool names
while their raw names remain in event provenance.

`moraine setup --mcp-target nac` resolves the NAC config directory in this
order: `NAC_HOME`, `XDG_CONFIG_HOME/nac`, then `~/.config/nac`, and merges only
the owned `[mcp_servers.moraine]` table. Model, storage, sandbox, and unrelated
MCP settings are preserved.

When NAC is selected as an ingest source in regular guided `moraine setup`, its
automatic path follows an absolute `storage.store_path`, or the default
`<NAC config dir>/store.db` when the config home is stable. A relative store
path depends on NAC's launch directory, and a per-launch `nac --store-path`
override is invisible to setup, so use the manual snippet it prints instead of
silently following the wrong database.

Replacing a NAC database at the same path starts a new source generation and
reconciles the old rows after the replacement snapshot is durable. WAL-only
changes are picked up through `store.db-wal` and `store.db-shm`; unrelated
SQLite files and backups are ignored.

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

Pi Coding Agent and OMP:

```toml
[[ingest.sources]]
name = "pi"
harness = "pi-coding-agent"
enabled = true
glob = "~/.pi/agent/sessions/**/*.jsonl"
watch_root = "~/.pi/agent/sessions"
format = "jsonl"

[[ingest.sources]]
name = "omp"
harness = "pi-coding-agent"
enabled = true
glob = "~/.omp/agent/sessions/**/*.jsonl"
watch_root = "~/.omp/agent/sessions"
format = "jsonl"
```

OMP uses the Pi session schema, so both sources share the `pi-coding-agent`
adapter. Separate source names preserve historical `~/.pi` checkpoints while
allowing startup backfill and live watching of current `~/.omp` sessions.

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
exclude_project_dirs = []
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
| `exclude_project_dirs` | `[]` | Directory globs for sessions to omit, matched against each session's first non-empty absolute working directory. |
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

Exclusions use the same directory matching semantics as [`[[routes]]`](#routes):
`~` expands when configuration loads, `*` stays within one path component, and
a trailing `/**` matches both the project directory itself and everything
below it. Moraine keeps the first non-empty working directory for the whole
session, so a later `cd` cannot change the decision. For example:

```toml
[ingest]
exclude_project_dirs = [
  "~/code/project-with-large-trajectories/**",
]
```

For JSONL sources, Moraine reads only far enough to find that initial working
directory before normalization. Codex provides it in the early `session_meta`
record, so excluded Codex trajectories do not require a full-file read and
never reach the sink.

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
# max_parallel_requests = 16 # optional; omitted defaults to 8
use_central_server = true
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
| `max_parallel_requests` | `8` | Maximum retrieval requests executed concurrently by each MCP server process. At most 16 additional requests wait in FIFO order until capacity is available or the request is cancelled. A full queue is rejected immediately with a structured retryable error. A configured value must be greater than zero. |
| `use_central_server` | `true` | Makes `moraine run mcp` prefer the shared central server socket, with embedded fallback. |
| `central_socket_path` | `mcp.sock` | Unix socket path. Bare filenames resolve under `runtime.pids_dir`; absolute paths are used verbatim. |
| `central_connect_timeout_ms` | `250` | Milliseconds a proxy client waits for the central socket before falling back to embedded mode. |

Raise `max_results` only when clients need larger result windows. Increase
context defaults when retrieval snippets are too narrow.
Leave `prewarm_on_initialize` disabled for harnesses that launch multiple MCP
processes at once; enabling it trades startup CPU/database work for lower
first-search latency.

The shared central server applies one parallel-request budget across every MCP
socket connection and queues at most 16 valid retrievals in FIFO order when that
budget is busy. Queued and running retrievals have no fixed admission deadline;
they continue until completion, client cancellation, disconnect, or service
shutdown. A full queue is rejected immediately with a structured retryable tool
error. An embedded fallback is a separate process, so its default or configured
execution budget is process-local. Validation and control requests continue to
run while retrievals wait.

### Shared central MCP server

When the backend daemon is running, Moraine uses one repository, ClickHouse
client, and warm cache set for every MCP proxy session and the monitor HTTP
server. Every `moraine up` starts the daemon. `use_central_server` controls
whether `moraine run mcp` attempts that shared socket before falling back to an
embedded server:

| Field | Default | Purpose |
| --- | --- | --- |
| `use_central_server` | `true` | When set, `moraine run mcp` connects to the central server's socket and proxies to it; if the socket is missing or unreachable it transparently falls back to an embedded server. Set to `false` to always run embedded (pre-central behavior). In a directory routed to a non-default backend the central proxy is bypassed regardless of this flag (see [MCP in routed directories](#mcp-in-routed-directories)). |
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

The up-managed MCP service is the unified backend daemon; the legacy per-`up`
stdio daemon and standalone monitor service are gone. The old
`backend.start_on_up`, `runtime.start_monitor_on_up`,
`mcp.start_central_on_up`, and `runtime.start_mcp_on_up` launch switches remain
load-compatible for upgrades, but their values no longer gate startup. Every
well-typed combination loads with an effective `backend.start_on_up = true`.
`moraine setup` canonicalizes the value to true and atomically removes the three
obsolete aliases.

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

## Query Budgets

`[query_budgets]` bounds every ClickHouse query Moraine issues. Each query
runs inside a mandatory envelope drawn from one of five classes, and the
envelope enforces an absolute execution deadline, per-statement memory and
spill ceilings, cumulative read allowances, and a fixed statement cap for the
whole logical operation. There is no unlimited setting: zero values are
rejected at config load (ClickHouse treats zero limits as "unlimited"), so a
config that would produce an unbounded query never loads. The envelope is
also mandatory at the transport: a statement issued without one is refused
client-side with a typed error before it reaches ClickHouse, so an
unenveloped query is unrepresentable.

```toml
[query_budgets.interactive]
deadline_seconds = 15.0
memory_bytes = 1073741824   # 1 GiB
spill_bytes = 268435456     # 256 MiB
read_rows = 500000000
read_bytes = 10737418240    # 10 GiB
statement_cap = 256
```

Each class is a table with the same six required fields. Absent classes and
fields fall back to the defaults below, so a partial table overrides only the
fields it names.

| Field | Purpose |
| --- | --- |
| `deadline_seconds` | Absolute server execution deadline for the whole logical operation. Statements are issued with `max_execution_time` set to the remaining portion, so multi-statement operations share one deadline. |
| `memory_bytes` | Per-statement `max_memory_usage` ceiling. |
| `spill_bytes` | Sort/group-by spill threshold (`max_bytes_before_external_group_by` / `_sort`). |
| `read_rows` | Cumulative request-level row-read allowance (`max_rows_to_read`). |
| `read_bytes` | Cumulative request-level byte-read allowance (`max_bytes_to_read`). |
| `statement_cap` | Maximum number of statements one logical operation may issue. |

Per-class defaults:

| Class | Used by | `deadline_seconds` | `memory_bytes` | `spill_bytes` | `read_rows` | `read_bytes` | `statement_cap` |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `interactive` | MCP tools, monitor reads, CLI status/doctor reads | `15.0` | 1 GiB | 256 MiB | 500M | 10 GiB | `256` |
| `background` | Ingest publication inspections, projection refreshes, janitor work | `600.0` | 2 GiB | 512 MiB | 5B | 100 GiB | `512` |
| `migration` | Schema migrations, read-model backfills | `600.0` | 4 GiB | 1 GiB | 10B | 200 GiB | `1024` |
| `administrative` | KILL statements and telemetry one-shots only | `5.0` | 256 MiB | 64 MiB | 1M | 256 MiB | `4` |
| `export` | `moraine export` streaming reads | `600.0` | 2 GiB | 512 MiB | 10B | 200 GiB | `64` |

Validation is fail-closed and applies to every class:

- every field must be at least 1 (zero would mean "unlimited" in ClickHouse);
- `deadline_seconds` must be a finite number greater than zero and at most
  `86400` (24 hours);
- `memory_bytes` may not exceed the 4 GiB per-query server backstop that
  managed installs ship in their ClickHouse user profile (see
  [Runtime Paths](#runtime-paths)). The envelope's per-statement settings are
  the primary enforcement mechanism and apply to external ClickHouse backends
  too; the server profile is only the last-resort bound for managed installs.

The `background` deadline intentionally matches the projection debounce
ceiling so the worst-case legitimate projection refresh completes within one
budget instead of retrying forever. Migrations additionally keep honoring a
larger operator-configured client timeout when one is set; the migration
budget never tightens below it.

The defaults are deliberately generous for healthy local corpora. Tighten
them after observing budget-exhaustion telemetry in monitor health/status
(the `query_budgets` block in `/api/v1/health` and `/api/v1/status`, also
summarized as a `moraine status` note when nonzero); repeated
`deadline_exceeded` or `resource_exhausted` results indicate either an
under-provisioned budget or a query-shape problem worth investigating.

## Backend Daemon

`[backend]` controls the unified daemon's HTTP listener:

```toml
[backend]
bind = "127.0.0.1"
# auth_token = "<generate-a-random-guard-token>"
start_on_up = true
```

| Field | Default | Purpose |
| --- | --- | --- |
| `bind` | `127.0.0.1` | Interface for the monitor HTTP listener. Keep the loopback default for local-only access. |
| `auth_token` | unset | Experimental startup prerequisite for a non-loopback effective bind. It does not authenticate HTTP requests. |
| `start_on_up` | `true` | Deprecated compatibility key. Every `moraine up` starts one unified backend; an existing loopback `false` value is accepted but ignored. |

`moraine up --backend`, `moraine up --monitor`, and `moraine up --mcp` remain
deprecated, redundant compatibility forms. They never launch separate services.
For upgrade safety, a non-loopback `backend.bind` also requires an affirmative
existing launch setting (`backend.start_on_up = true`, or a legacy true alias);
otherwise config loading fails rather than unexpectedly exposing the
unauthenticated monitor API. Loopback configs normalize existing false values
to true automatically.

### Experimental HTTP bind guard

`backend.bind` defaults to the loopback interface. When the effective bind is
non-loopback, `backend.auth_token` must contain at least one non-whitespace
character or backend startup fails before creating any listener.

This is experimental configuration groundwork and a startup prerequisite only.
It does not authenticate HTTP requests: the monitor API and UI remain
unauthenticated, and exposing them to an untrusted network remains unsafe. Real
monitor authentication is tracked in
[issue #383, Phase 3](https://github.com/eric-tramel/moraine/issues/383).

The guard does not change the MCP transport. The MCP server continues to use
the same per-user Unix socket and proxy/embedded fallback behavior described in
[Shared central MCP server](#shared-central-mcp-server).

## Monitor

`[monitor]` controls the monitor HTTP port:

```toml
[monitor]
port = 8080
```

| Field | Default | Purpose |
| --- | --- | --- |
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
```

Relative `logs_dir` and `pids_dir` values are resolved under `root_dir`.
`service_bin_dir` must contain `moraine-ingest` and `moraine-mcp`, unless
`MORAINE_SERVICE_BIN_DIR` is set or source-tree mode is enabled. Release
packages also retain `moraine-monitor` as a deprecated executable alias that
delegates to the unified backend; `moraine up` never manages it separately.

| Field | Default | Purpose |
| --- | --- | --- |
| `root_dir` | `~/.moraine` | Root directory for Moraine runtime state. |
| `logs_dir` | `logs` | Log directory. Relative paths resolve under `root_dir`. |
| `pids_dir` | `run` | PID and socket directory. Relative paths resolve under `root_dir`. |
| `service_bin_dir` | `~/.local/bin` | Directory containing installed service binaries. |
| `managed_clickhouse_dir` | `~/.local/lib/moraine/clickhouse/current` | Directory for the managed ClickHouse installation. |
| `clickhouse_start_timeout_seconds` | `30.0` | Seconds to wait for each managed ClickHouse process generation to become healthy, both initially and after an automatic restart. |
| `healthcheck_interval_ms` | `500` | Milliseconds between readiness checks while a managed ClickHouse process generation starts. This is not a permanent health-poll interval after readiness. |
| `clickhouse_auto_install` | `true` | Automatically installs the managed ClickHouse binary when needed. |
| `clickhouse_version` | `v25.12.5.44-stable` | Managed ClickHouse release tag expected by this Moraine build. |

Moraine's managed ClickHouse uses concurrency control with fair round-robin
scheduling and a soft aggregate query-thread limit equal to the detected CPU
core count. Individual repository reads do not override `max_threads`, so an
idle query can scale up while concurrent queries share the aggregate budget.
These defaults apply only to Moraine-managed ClickHouse; external ClickHouse
deployments retain their administrator-defined scheduling policy.

The managed server also ships a query-safety profile sized for a local host:

| Setting | Value | Purpose |
| --- | --- | --- |
| `max_concurrent_queries` | `64` | Server-wide query slots. Moraine's own interactive load is admission-bounded well below this (8 MCP + 4 monitor + 2 background reads), so ingest inserts, merges, and administrative statements always have free slots. |
| `max_server_memory_usage_to_ram_ratio` | `0.9` | Aggregate server memory ceiling, pinned explicitly. Kept at 0.9 because background merges can legitimately demand most of it; per-query limits keep reads from occupying merge headroom. |
| `max_memory_usage` (default profile) | `4 GiB` | Per-query memory backstop, replacing the previous unlimited (`0`) profile. Moraine's query envelope sends tighter per-statement budgets and the transport refuses unenveloped statements; this catches non-Moraine clients and defends in depth. |
| `max_bytes_before_external_group_by` / `max_bytes_before_external_sort` (default profile) | `2 GiB` | Spill-to-disk thresholds at half the per-query backstop, so a query approaching the cap spills instead of failing on memory. |

The per-statement query envelope (query IDs, deadlines, memory and read
ceilings) is the primary safety mechanism and applies to every deployment
shape; this server profile is a defense-in-depth backstop that only reaches
Moraine-managed ClickHouse. The profile files are re-rendered from the
built-in templates every time the managed server starts, so existing
installations pick up profile changes at the next `moraine up` or managed
restart — local edits to the rendered `~/.moraine/clickhouse/config.xml` and
`users.xml` files do not persist.
