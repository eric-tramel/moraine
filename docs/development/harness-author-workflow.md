# Harness Author Workflow

This workflow covers adding or changing an ingest source from module creation
through fixtures, config, CI, and sandbox validation. It assumes the source will
write normalized Moraine events into the shared ClickHouse schema.

## 1. Start From The Source Contract

Create or update a source module under
`crates/moraine-ingest-core/src/sources/`. Implement `IngestSource` and keep
`normalize()` as a router:

```rust
fn normalize(
    &self,
    record: &Value,
    ctx: &RecordContext<'_>,
    top_type: &str,
    base_uid: &str,
    model_hint: &str,
) -> NormalizedPartials {
    let record = ExampleRecord::new(record, top_type, base_uid, model_hint);
    let mut emitter = SourceEmitter::new(ctx);
    dispatch_example_record(&record, &mut emitter);
    emitter.finish()
}
```

Put source-specific parsing in source-local context structs and handlers. Use
shared helpers only for cross-source row contracts.

## 2. Define Source Metadata

Handle provider, model, timestamp, and session identity before emitting rows:

- `default_inference_provider()` for static providers.
- `source_metadata()` for record-derived providers or model hints.
- `record_ts()` for source timestamp conversion.
- `top_type()` for the record discriminator.
- `session_id()` for stable sessions, including source-specific namespacing.
- `preflight()` for non-event records that should be skipped before raw rows.

Do not infer provider/model through config alone. The normalized event row must
carry the source truth for downstream search, monitor, MCP, and analytics.

## 3. Emit Rows Through Builders

Use `SourceEmitter` and `EventBuilder` for event rows:

```rust
let uid = emitter.uid_for_json(block, "example:block:0");
let event = emitter
    .event_for_json(&uid, "message", "message", "assistant", text, block)
    .model(model)
    .content_types(["text"])
    .turn_index(turn_index);
emitter.push_event(event);
```

Use emitter helpers for links and tool IO:

```rust
emitter.push_external_link(&uid, external_parent_id, "parent_uuid", "{}");
emitter.push_tool_request(&uid, tool_call_id, "", tool_name, input_json);
emitter.push_tool_response(&uid, tool_call_id, "", tool_name, 0, "", output_json, output_text);
```

Use `TokenAccounting` instead of hand-building token maps:

```rust
let accounting = TokenAccounting::openai_generation(record.get("usage"));
let event = event.token_accounting(accounting);
```

The shared token bucket keys are schema-controlled. Adding or renaming a key is
a schema migration task, not only an adapter change.

## 4. Preserve Cross-Cutting Invariants

Every source must preserve these invariants:

- Stable deterministic event UIDs based on source file coordinate, payload, and
  source-local suffix.
- Canonical event kind, payload type, actor kind, and link type values.
- Distinct normalized event links and source external-id links.
- Tool rows that reference emitted event UIDs.
- Token buckets that are present on every event row, even when values are zero.
- Model/provider/session semantics that match the source format.
- Timestamp parse errors routed through generic ingest error rows.
- Synthetic timestamps advanced only for emitted rows.

If a source needs correlation state, keep it source-local. Hermes trajectory
uses pending tool-call FIFO state and backpatches call ids; that behavior should
not move into shared helpers unless another source needs the same contract.

## 5. Register And Configure

Register the adapter in `sources::registry()`. If the harness is new, also add
it to `moraine_config::KNOWN_INGEST_HARNESSES`. This duplication is deliberate:
`moraine-config` cannot depend on ingest-core without a cycle.

For default or documented sources, update:

- `crates/moraine-config/src/lib.rs` default source list and config tests.
- `config/moraine.toml` example source entries.
- `docs/configuration.md` source matrix and examples.
- Registry/config sync tests in ingest-core.

Choose `format` carefully:

- Omit it for normal JSONL sources when inference is sufficient.
- Use `jsonl` for append-only newline-delimited trace records.
- Use `session_json` for one JSON file per live session that is rewritten in
  place. The session processor emits only newly appended synthetic records.
- Use `kiro_session` only for Kiro CLI's paired `<session-id>.jsonl` transcript
  and `<session-id>.json` metadata files.
- Use `cursor_sqlite` for Cursor `state.vscdb` databases. A DB-polling format
  is only appropriate when the harness keeps its history in a live local
  database with no append-only trace files; the polling engine in
  `sqlite_poll.rs` diffs the database against a persisted cursor and routes
  synthetic records through the normal adapter path (see
  [Ingest Sources → SQLite-Polled Sources](ingest-sources.md#sqlite-polled-sources)).

## 6. Add Fixtures

Add small deterministic fixtures under `fixtures/<source>/` and cover them with
tests under `crates/moraine-ingest-core/tests/`. Prefer public normalization
entry points over calling adapter internals.

For each source family, assert:

- Raw row harness/provider/session/top-type fields.
- Representative event kinds and payload types.
- Link rows and tool IO rows.
- Token accounting scalars and buckets.
- Model/provider/session hints across sequential records.
- Error behavior for malformed input where relevant.

Add or update golden snapshots with:

```bash
cargo test -p moraine-ingest-core --test golden_fixtures --locked
```

Only update golden files when the behavior change is intentional and reviewed.

## 7. Validate End To End

Run focused checks first:

```bash
cargo fmt --all -- --check
cargo test -p moraine-ingest-core --locked
cargo test --workspace --locked
```

If the change touches ingest, MCP, monitor, ClickHouse schema, or source formats,
run the functional stack:

```bash
bash scripts/ci/e2e-stack.sh
```

That script exercises the maintained harness matrix with generated fixtures,
including Kiro CLI's paired JSONL transcript and JSON metadata sidecar. It then
verifies row counts, representative fields, token buckets, monitor routes, and
MCP search/open/list behavior.

## 8. Use The Dev Sandbox For QA

For ingest, monitor, MCP, or schema work, run QA in the sandbox rather than
against your host install. Use `--quiet` so the sandbox id cannot be lost under
output truncation, and always tear it down:

```bash
id=$(scripts/dev/sandbox/moraine-sandbox up --quiet)
scripts/dev/sandbox/moraine-sandbox status "$id"
scripts/dev/sandbox/moraine-sandbox shell "$id"
scripts/dev/sandbox/moraine-sandbox down "$id"
```

Inside the shell, the worktree is mounted read-only at `/repo`; run commands
from there:

```bash
cd /repo
cargo test -p moraine-ingest-core --locked
bash scripts/ci/e2e-stack.sh
```

Do not pipe `moraine-sandbox up` through `head`, `tail`, or similar commands.
Leftover sandboxes leak ClickHouse data and consume host ports.

## PR Checklist

Before opening or merging a PR:

- Adapter code is split into source-local handlers and shared helpers are only
  used for shared row contracts.
- Config harness lists, defaults, docs, and registry tests are in sync.
- Fixtures and golden snapshots cover raw, event, link, tool, token, and error
  rows.
- `cargo fmt --all -- --check` passes.
- Focused ingest tests and `cargo test --workspace --locked` pass.
- `cargo clippy --workspace --all-targets -- -D warnings` passes when the
  change touches code.
- `bash scripts/ci/e2e-stack.sh` passes for source format changes.
- Sandbox QA has been run and the sandbox has been torn down.
