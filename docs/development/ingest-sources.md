# Ingest Sources

Moraine normalizes agent trace files through source adapters in
`crates/moraine-ingest-core/src/sources/`. Each adapter owns one harness wire
format and returns normalized event, link, and tool rows. The generic ingest
wrapper keeps cross-source behavior centralized: raw rows, timestamp parse
errors, session/model hints, source references, schema-domain checks, and sink
fan-out.

The adapter boundary is intentionally small. New source work should add parsing
and source-local handlers in `sources/<name>.rs`, then use shared builders for
all rows that leave the adapter.

## Source Map

| Harness | Module | Default provider | Typical format | Notes |
| --- | --- | --- | --- | --- |
| `codex` | `sources/codex.rs` | `openai` | `jsonl` | OpenAI/Codex events, response items, tool calls, compaction, token counts. |
| `claude-code` | `sources/claude_code.rs` | `anthropic` | `jsonl` | Claude Code and local macOS Cowork message blocks, operational records, parent/tool external links, and Cowork root metadata. |
| `kiro-cli` | `sources/kiro_cli.rs` | `kiro` | `kiro_session` | Kiro CLI prompt, assistant, tool-result, and compaction records. A same-named JSON sidecar supplies session cwd, title, model, and aggregate token/credit metadata. |
| `kimi-cli` | `sources/kimi_cli.rs` | `moonshot` | `jsonl` | Kimi `wire.jsonl`; skips metadata headers and keeps parent `SubagentEvent` envelopes raw-only. |
| `qwen-code` | `sources/qwen_code.rs` | unset unless recorded | `jsonl` | Qwen Code `ChatRecord` envelopes, ordered message parts, parent/rewind links, tools, and Google-style usage metadata. |
| `nac` | `sources/nac.rs` | record-derived | `nac_sqlite` | NAC parent sessions and managed-worker episodes synthesized from `store.db`; remote bodies are retained without local project attribution, while credential-bearing columns remain excluded. |
| `opencode` | `sources/opencode.rs` | record-derived | `opencode_sqlite` | OpenCode `opencode*.db`; append-only conversation events synthesized into session, message, part, and session-message records. Credential/account tables are deliberately out of scope. |
| `cursor` | `sources/cursor.rs` | `cursor` | `jsonl` or `cursor_sqlite` | Cursor Agent transcripts under `agent-transcripts/`; text blocks, tool-use blocks, and local file references. Also normalizes the synthetic `cursor_composer`/`cursor_bubble` records produced by polling `state.vscdb` (see SQLite-Polled Sources below); composer names become `session_meta` events that carry the session title. |
| `hermes` | `sources/hermes.rs` | record-derived | `jsonl` or `session_json` | ShareGPT trajectories and live Hermes session JSON with vendor/model splitting. |
| `pi-coding-agent` | `sources/pi.rs` | record-derived | `jsonl` | Pi and OMP session JSONL trees, model/thinking metadata, assistant tool calls, tool results, and parent links. |

### Local macOS Claude Cowork

Cowork reuses the `claude-code` adapter but has its own `claude-cowork`
source/checkpoint namespace. macOS defaults and setup use:

```toml
glob = "~/Library/Application Support/Claude/local-agent-mode-sessions/**/.claude/projects/**/*.jsonl"
watch_root = "~/Library/Application Support/Claude/local-agent-mode-sessions"
```

The watch root also contains sibling `audit.jsonl` files. Live watcher events do
not consult the startup glob, so the dispatch ingestability gate revalidates the
Cowork path and accepts only transcripts beneath a containing
`local_*/.claude/projects/` subtree. The containing `local_*` name is the Moraine
session ID; nested Claude CLI `sessionId` values do not split a resumed Cowork
conversation.

When transcript bytes are processed, dispatch reads the sibling
`local_<id>.json` and constructs a synthetic `cowork-session-meta` record from a
fixed safe allowlist. The original root object is never stored. `attachment`,
`last-prompt`, and `ai-title` transcript records remain raw-only to avoid
attachment indexing, duplicate prompts, and untimestamped metadata noise.
Metadata is refreshed on transcript processing, not as an independent sidecar;
a metadata-only edit is picked up by the next transcript activity.

This contract is limited to the observed local macOS layout. Claude documents
the JSONL entry format as internal and unstable. Windows local storage is not
enabled because no path is verified across installer modes, and remote Cowork
OpenTelemetry is a separate source surface.

### Kimi parent and sub-agent streams

Kimi treats every `wire.jsonl` as its own session boundary. A parent stream at
`<session_id>/wire.jsonl` maps to `kimi-cli:<session_id>`, while
`<session_id>/subagents/<agent_id>/wire.jsonl` remains a first-class standalone
session named `kimi-cli:<agent_id>` rather than being folded into its parent.
The first normalized event from the sub-agent stream carries a
`subagent_parent` external link whose `linked_external_id` is
`kimi-cli:<session_id>`. This preserves the parent relationship without
combining the two event streams.

Kimi also writes each sub-agent event as a `SubagentEvent` envelope on the
parent wire. That envelope remains available in `raw_events`, but it does not
emit a normalized event; the event from the sub-agent's own `wire.jsonl` is the
single normalized copy. This prevents sub-agent activity from being counted
once in the standalone sub-agent session and again as parent-session progress.

### Qwen Code records and branches

The `qwen-code` source watches append-only
`~/.qwen/projects/*/chats/*.jsonl` files. Moraine preserves each persisted
record as raw data and expands recognized text, thought, function-call,
function-response, title, compression, and rewind shapes into canonical rows.
Each emitted event links to its record's non-empty `parentUuid`. Rewind does not
delete history: abandoned and replacement branches remain queryable and are
distinguished by their parent links.

Assistant parts retain source order. `functionCall.id` is the request identity;
responses prefer `toolCallResult.callId`, then `functionResponse.id`, then the
record UUID. Fully qualified names such as
`mcp__moraine__search_sessions` are preserved on tool rows while the shared MCP
classifier recognizes them as Moraine-internal. Assistant usage is applied once
per record. Google-style totals populate legacy token columns, while cache,
reasoning, tool-use, and modality values remain non-additive bucket breakdowns.
The adapter never infers a provider from the model because Qwen supports
multiple providers.

Unknown record types, unknown system variants, telemetry, file-history and
attribution snapshots, and binary-only parts remain raw-only. Compression rows
retain bounded metrics but do not copy replay history into searchable content.
Malformed timestamps use generic ingest error routing without dropping adjacent
records.

This compatibility boundary is pinned by sanitized Qwen Code 0.19.x fixtures.
Qwen's persistence shape is an upstream internal contract, not a stable public
API. See the upstream
[ChatRecord persistence source](https://github.com/QwenLM/qwen-code/blob/v0.19.0/packages/core/src/services/chatRecordingService.ts),
[rewind reconstruction](https://github.com/QwenLM/qwen-code/blob/v0.19.0/packages/core/src/services/sessionService.ts),
and [MCP documentation](https://github.com/QwenLM/qwen-code/blob/v0.19.0/docs/users/features/mcp.md).

## Adapter Contract

Each adapter implements `IngestSource` from `sources/mod.rs`:

| Method | Responsibility |
| --- | --- |
| `harness()` | Stable harness string written to ClickHouse and matched by config. |
| `default_inference_provider()` | Static provider when the source has one. Return `None` when provider is record-derived. |
| `format()` | Source default format. Most sources use `Jsonl`; config can still select parser behavior. |
| `preflight()` | Skip or rewrite source records before generic raw/error handling. Kimi skips its metadata header here. |
| `source_metadata()` | Per-record provider and model hint discovery. Hermes splits `vendor/model` here. |
| `record_ts()` | Timestamp string consumed by the shared parser. Kimi converts Unix-second wire timestamps here. |
| `top_type()` | Source-level discriminator used for dispatch inside the adapter. |
| `session_id()` | Stable session id derived from record fields, file path, prior session hint, or raw UID. |
| `normalize()` | Emit `NormalizedPartials`: event rows, event link rows, and tool IO rows. |

`normalize()` receives a `RecordContext`. That context has already been stamped
with source name, harness, provider, session id, source file coordinates, and
parsed timestamps. Adapters should not rebuild those fields by hand.

### Kiro paired sessions

Kiro CLI writes `<session-id>.jsonl` and `<session-id>.json` under
`$KIRO_HOME/sessions/cli` when `KIRO_HOME` is set, or
`~/.kiro/sessions/cli` otherwise. The transcript remains the canonical
checkpoint path. Watcher events for either file coalesce to that JSONL path;
the sidecar's fingerprint is persisted in the checkpoint so title, usage, and
credit changes re-emit a stable `session_meta` event without replaying
transcript lines. Missing sidecars do not block transcript ingestion. When a
valid sidecar first appears, or when its session id, cwd, or model changes,
Moraine replays the transcript once with stable event IDs to backfill those
searchable fields. Malformed sidecars produce a checkpointed ingest error. The
checkpoint cursor also preserves the last transcript timestamp so live tails
do not need to rescan prior records. Kiro's per-turn
metering entries are summed into `token_usage_native_units["credits"]` on that
session metadata event; token counts remain populated independently when Kiro
supplies them.

## Shared Helpers

Use `sources/shared.rs` for common invariants:

- Scalar conversion helpers such as `to_str`, `to_u32`, `to_u8_bool`, and
  `extract_message_text`.
- Timestamp helpers, session id inference, and stable `event_uid` generation.
- Domain canonicalization for event kind, payload type, link type, and model
  names where the source allows generic canonicalization.
- `RecordContext`, `base_event_obj`, `build_link_row`, and `build_tool_row`.
- `TokenAccounting` for legacy token columns plus `token_usage_buckets` and
  `token_usage_native_units`.

Use `sources/emitter.rs` for row construction:

- `SourceEmitter` owns a `RecordContext` and accumulates `NormalizedPartials`.
- `EventBuilder` wraps `base_event_obj` with typed setters for common columns.
- `push_tool_request`, `push_tool_response`, and link helpers keep row shape in
  sync with shared constructors.

Use `sources/record_view.rs` when a source benefits from structured field reads
or table-driven dispatch. It gives path-aware errors and keeps handler routing
explicit without ad hoc `Value` indexing everywhere.

## Handler Pattern

Keep `normalize()` thin. It should usually create a source-local record context,
construct a `SourceEmitter`, dispatch to named handlers, and return
`emitter.finish()`.

Prefer source-local helpers for source semantics:

- `normalize_*_record` or `dispatch_*_record` for top-level routing.
- `handle_*` or `emit_*` functions for source event families.
- Small context structs for repeated record fields, token accounting, model
  state, pending tool correlation, and timestamp sequencing.
- Source-specific parsing and correlation in the source module, not in shared
  helpers.

Shared helpers should only grow when more than one source needs the same row
contract or schema-domain rule. Source quirks such as Hermes trajectory tool
backpatching, Kimi placeholder models, or Claude external parent UUID links
belong in the source module.

## Row Invariants

Adapters must emit rows that already satisfy the normalized schema domain:

- Event UIDs are deterministic for the source file coordinate, record payload,
  and source-local suffix. Do not reuse the raw record UID for multiple
  normalized events.
- `event_kind`, `payload_type`, and `actor_kind` must use the canonical domain.
- Links must distinguish normalized event links (`linked_event_uid`) from
  source external ids (`linked_external_id`).
- Tool request/response rows must reference an emitted event UID and preserve
  `tool_call_id`, `tool_name`, `tool_phase`, `tool_error`, input JSON, output
  JSON, and output text.
- Token usage must populate both legacy scalar columns and
  `token_usage_buckets`. Bucket names are schema-controlled and tested against
  SQL migrations.
- `model`, `inference_provider`, and `session_id` must preserve source-specific
  semantics. Hermes model strings are split from provider/model input and are
  not blindly canonicalized through generic helpers.
- Synthetic timestamp sequencing should only advance for rows that are actually
  emitted. This matters for Hermes trajectory segment parsing.

## SQLite-Polled Sources

Some harnesses keep history in a live SQLite database instead of append-only
trace files. `crates/moraine-ingest-core/src/sqlite_poll.rs` holds the shared
read-only open, stat/WAL fingerprint, `SyntheticRecord`, and error helpers.
Cursor's `cursor_sqlite` poller remains in that module; OpenCode and NAC own
their source-specific lifecycles in `sqlite_poll/opencode.rs` and
`sqlite_poll/nac.rs`. Every poller synthesizes records and feeds them through
`normalize_record`, so the source adapters stay format-agnostic:
`sources/cursor.rs`, `sources/opencode.rs`, and `sources/nac.rs` normalize the
same synthetic shapes independently of the database scan.

Mechanics that differ from file-backed sources:

- **Hash-based Cursor cursors.** Cursor's `cursorDiskKV` table has no rowid or
  timestamp watermark, so the poll cursor is a per-key content-hash map
  persisted in the `ingest_checkpoints.cursor_json` column (migration
  `sql/015_sqlite_checkpoint_cursor.sql`). A poll emits synthetic records only
  for keys that are new or whose hash changed, and prunes deleted keys after
  each full scan.
- **Append-only OpenCode cursors.** OpenCode's projection tables are mutable
  and incomplete during streaming. The poller instead reads only `event` and
  `event_sequence`, persists the last `seq` seen for each aggregate id, and
  synthesizes records from new durable events. Repeated updates for the same
  message or part coalesce to one synthetic record with a stable logical UID.
- **Hybrid NAC cursors.** NAC sessions are keyed by immutable session ID. The
  cursor keeps a metadata hash plus per-logical-message hashes so an updated
  session emits only changed parts; worker episodes advance by immutable
  integer ID, while per-worker metadata hashes prevent duplicate worker-session
  rows. Local and remote durable history is ingested; a non-empty remote
  `host_id` suppresses local project/worktree attribution without selecting the
  host identifier or credential values.
- **Bounded NAC scans.** NAC requires only the `sessions` and `episodes`
  allowlisted columns. Polls cap sessions, episodes, synthetic records, text,
  individual JSON values, total scan bytes, and each fully normalized
  ClickHouse JSON object. Schema drift, malformed required JSON, or an oversized
  normalized row fails without advancing the cursor.
- **Bounded prefix scans.** Only the `composerData:` and `bubbleId:` key
  prefixes are scanned, in pages, with a 10,000-key ceiling. Larger key spaces
  fail the poll instead of persisting an oversized cursor. `agentKv:*`,
  `checkpointId:*`, and the entire `ItemTable` (which holds live auth tokens)
  are deliberately out of scope for v1.
- **Allowlisted OpenCode surface.** OpenCode never reads account, credential,
  or token-bearing tables. Known session, message, part, and model-switch
  events become `opencode_*` synthetic records. Unknown event types are ignored
  until Moraine intentionally maps them. The event cursor has a 10,000-event
  ceiling plus row/scan byte ceilings so a large history fails the poll instead
  of persisting an oversized checkpoint.
- **Stable logical event UIDs.** Cursor UID material derives from the kv key,
  not the mutable payload, so a rewritten row re-emits the same event UIDs with
  a newer `event_version`. OpenCode UID material derives from the synthetic
  logical record (`session`, `message`, `part`, or `session_message`) plus
  source id, so repeated updates coalesce to the same normalized identity.
  NAC uses namespace-prefixed parent IDs, immutable episode IDs, message index,
  fixed per-kind line slots, and tool-call IDs so optional message parts cannot
  move surviving event coordinates across polls.
- **Sidecar watching.** `moraine_config::map_tracked_path` maps SQLite
  `-wal`/`-shm` filesystem events back to the canonical database path so
  WAL-only writes trigger polls. Cursor tracks `state.vscdb` sidecars; NAC
  tracks only `store.db`; OpenCode tracks only `opencode*.db` sidecars.
  Unrelated backups or SQLite files are untracked. Databases are opened
  read-only with a short busy timeout, and Moraine never checkpoints another
  application's WAL.
- **Rate-limited errors.** Failures surface as `ingest_errors` rows with kinds
  `sqlite_open_error`, `sqlite_schema_mismatch`, `sqlite_cursor_too_large`, and
  `sqlite_scan_error`. The cursor records the last reported kind so a
  persistent failure is emitted once rather than on every reconcile tick, and
  the data cursor is left untouched so the next poll retries.
- **Volatile no-op poll state (issue #443).** Cursor, NAC, and OpenCode can
  touch DB/WAL/SHM sidecars without changing transcript-relevant rows. A scan
  that emits no records and changes nothing else the durable checkpoint
  carries persists *nothing*: the stat fingerprint it covered is recorded
  only in a per-pipeline in-memory map (`sqlite_poll::VolatilePollMap`), so
  the durable checkpoint's `cursor_json` stat is expected to lag the file on
  a quiet database — that staleness is intentional, not a bug. After three
  consecutive no-op scans the database is treated as stat-noisy and rescans
  are throttled to one per 15 s (a failed scan refreshes that clock); any
  scan that finds real changes persists a checkpoint, clears the volatile
  entry, and restores immediate pickup. Worst-case pickup latency for the
  first relevant write after an idle stretch is one throttle window
  (~15–30 s including the reconcile tick); a process restart merely costs
  one redundant no-op scan per database.

### NAC SQLite contract

NAC stores parent sessions in `sessions` and managed-worker activity in
`episodes`. Moraine reads both through an immutable query projection:

- Parent sessions emit one stable `session_meta` row plus message parts in
  source order. User, assistant, reasoning, tool request, and tool response
  roles are retained; multiple tool calls in one assistant message are paired
  by call ID rather than adjacency.
- Each worker episode becomes its own Moraine session. Its `session_meta` row
  links back to the parent with `subagent_parent`; action and result rows retain
  the episode ID and thread label. Remote parent and worker bodies remain
  retrievable, but do not receive local project/worktree attribution.
- Parent metadata updates reuse the canonical `session_meta` UID and event
  timestamp, so `ReplacingMergeTree` replaces the row instead of creating a
  second metadata event. Message and episode identities likewise remain stable
  across retries and live updates.
- NAC usage maps `input_tokens`, `output_tokens`, `cache_read_tokens`,
  `cache_write_tokens`, and `reasoning_tokens` to Moraine's schema-controlled
  token buckets. Provider attribution is derived from the stored base URL;
  OpenRouter model names map to `openrouter`.
- The allowlisted query never selects API-key environment names, extra headers,
  or host identifiers. Raw records are compact projections, not copies of
  NAC's credential-bearing session rows.
- Replacing `store.db` at the same path changes the SQLite schema/data
  fingerprint, advances `source_generation`, and archives the previous
  generation after the replacement scan commits. A malformed or mixed
  DB/WAL/SHM snapshot fails closed and leaves both the data cursor and durable
  checkpoint unchanged for retry.

Fixtures: `fixtures/cursor/state-vscdb-kv.jsonl` stores `cursorDiskKV` rows as
JSONL (`key`/`value` pairs), `fixtures/opencode/session.jsonl` stores OpenCode
synthetic records, and `fixtures/nac/store.sql` builds a representative NAC
database whose normalized shapes are frozen in `fixtures/nac/normalized.jsonl`.
Golden contract tests feed records through the production normalization path,
so output cannot drift silently from adapters. Unit tests in `sqlite_poll.rs`,
`sqlite_poll/opencode/tests.rs`, and `sqlite_poll/nac.rs` cover first-poll
emission, no-op repolls, incremental updates, stable UIDs, tool pairing,
worker-parent links, schema/credential boundaries, and error paths; sidecar
path mapping is tested in `moraine-config`.

## Fixtures And Contracts

Deterministic fixtures live under `fixtures/`; golden snapshots live under
`crates/moraine-ingest-core/tests/goldens/source_normalization/`.
`crates/moraine-ingest-core/tests/golden_fixtures.rs` runs the same public
normalization entry point that ingest uses and asserts row shape across all
source families.

Fixture tests should verify both adapter output and generic wrapper output:

- `raw_row.harness`, `raw_row.inference_provider`, `raw_row.top_type`,
  `raw_row.session_id`, and raw JSON hashing.
- Event row `event_kind`, `payload_type`, `actor_kind`, timestamps, model,
  token fields, content types, and text previews.
- Link row target columns and canonical link type.
- Tool row request/response phases, tool ids, previews, and error flags.
- `session_hint` and `model_hint` continuity across sequential records.
- Ingest error rows for invalid timestamps or malformed source records.

When behavior changes intentionally, update fixtures and goldens together.
Otherwise, golden stability failures are regressions.

## Config Boundary

`moraine-config` is lower-level than `moraine-ingest-core`; it cannot call the
ingest registry without creating a dependency cycle. Adding a harness therefore
requires updates on both sides:

- `crates/moraine-ingest-core/src/sources/mod.rs` registry.
- `moraine_config::KNOWN_INGEST_HARNESSES`.
- Default source examples in `crates/moraine-config/src/lib.rs` and
  `config/moraine.toml` when the source should be enabled by default.
- Documentation in `docs/configuration.md`.
- Registry/config sync tests and fixture contract tests.

The ingest-core registry test and golden fixture contract test are the guardrails
that keep these lists aligned.

## Validation

For source adapter changes, run focused tests first and expand based on blast
radius:

```bash
cargo fmt --all -- --check
cargo test -p moraine-ingest-core --locked
cargo test --workspace --locked
```

For ingest, monitor, MCP, ClickHouse schema, or source format changes, also run
the functional stack and sandbox QA described in
[Harness Author Workflow](harness-author-workflow.md):

```bash
bash scripts/ci/e2e-stack.sh
id=$(scripts/dev/sandbox/moraine-sandbox up --quiet)
scripts/dev/sandbox/moraine-sandbox down "$id"
```
