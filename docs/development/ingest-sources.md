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
| `claude-code` | `sources/claude_code.rs` | `anthropic` | `jsonl` | Claude Code message blocks, operational records, parent/tool external links. |
| `kimi-cli` | `sources/kimi_cli.rs` | `moonshot` | `jsonl` | Kimi `wire.jsonl`; skips metadata headers and drops parent `SubagentEvent` rows. |
| `cursor` | `sources/cursor.rs` | `cursor` | `jsonl` or `cursor_sqlite` | Cursor Agent transcripts under `agent-transcripts/`; text blocks, tool-use blocks, and local file references. Also normalizes the synthetic `cursor_composer`/`cursor_bubble` records produced by polling `state.vscdb` (see SQLite-Polled Sources below); composer names become `session_meta` events that carry the session title. |
| `hermes` | `sources/hermes.rs` | record-derived | `jsonl` or `session_json` | ShareGPT trajectories and live Hermes session JSON with vendor/model splitting. |
| `pi-coding-agent` | `sources/pi.rs` | record-derived | `jsonl` | Pi session JSONL trees, model/thinking metadata, assistant tool calls, tool results, and parent links. |

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
trace files. `crates/moraine-ingest-core/src/sqlite_poll.rs` is the shared
polling engine for those sources; the `cursor_sqlite` format (Cursor
`state.vscdb`) is its first consumer. The engine synthesizes one record per
relevant database row and feeds it through `normalize_record`, so the source
adapter stays format-agnostic — `sources/cursor.rs` handles the synthetic
`cursor_composer`/`cursor_bubble` records the same way it handles JSONL lines.

Mechanics that differ from file-backed sources:

- **Hash-based change cursor.** Cursor's `cursorDiskKV` table has no rowid or
  timestamp watermark, so the poll cursor is a per-key content-hash map
  persisted in the `ingest_checkpoints.cursor_json` column (migration
  `sql/015_sqlite_checkpoint_cursor.sql`). A poll emits synthetic records only
  for keys that are new or whose hash changed, and prunes deleted keys after
  each full scan.
- **Bounded prefix scans.** Only the `composerData:` and `bubbleId:` key
  prefixes are scanned, in pages, with a 10,000-key ceiling. Larger key spaces
  fail the poll instead of persisting an oversized cursor. `agentKv:*`,
  `checkpointId:*`, and the entire `ItemTable` (which holds live auth tokens)
  are deliberately out of scope for v1.
- **Stable logical event UIDs.** UID material derives from the kv key
  (`cursor_sqlite:<table>:<pk>`), not the mutable payload, so a row that
  mutates in place (streaming text, tool status changes) re-emits the same
  event UIDs with a newer `event_version` and `ReplacingMergeTree` collapses
  them.
- **Sidecar watching.** `moraine_config::map_tracked_path` maps
  `state.vscdb-wal`/`state.vscdb-shm` filesystem events back to the canonical
  `state.vscdb` path so WAL-only writes trigger polls; `state.vscdb.backup` is
  untracked. Databases are opened read-only with a short busy timeout, and
  Moraine never checkpoints another application's WAL.
- **Rate-limited errors.** Failures surface as `ingest_errors` rows with kinds
  `sqlite_open_error`, `sqlite_schema_mismatch`, `sqlite_cursor_too_large`, and
  `sqlite_scan_error`. The cursor records the last reported kind so a
  persistent failure is emitted once rather than on every reconcile tick, and
  the data cursor is left untouched so the next poll retries.

Fixtures: `fixtures/cursor/state-vscdb-kv.jsonl` stores `cursorDiskKV` rows as
JSONL (`key`/`value` pairs). The golden contract test feeds each row through
the production synthesis path, so golden output cannot drift from the poller;
skipped rows (ghost composers, out-of-scope key families) appear as empty
snapshots so scope changes stay visible in review. Unit tests in
`sqlite_poll.rs` build temporary `rusqlite` databases to cover first-poll
emission, no-op repolls, in-place mutation re-emitting stable UIDs, payload
hygiene, and the schema-mismatch and oversized-key-space error paths; sidecar
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
