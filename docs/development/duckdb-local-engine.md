# DuckDB local engine

The experimental DuckDB adapter is a leaf crate linked only into
`moraine-mcp`. Its dependency direction is:

```text
apps/moraine-mcp
  -> moraine-duckdb
       -> moraine-config
       -> moraine-conversations
       -> moraine-ingest-core
```

ClickHouse remains the default and retains its existing multi-process topology.
With `storage.engine = "duckdb"`, the unified backend owns one
`Arc<DuckDbStore>`, one serialized writer, and a bounded reader pool. The same
store implements `IngestStore` and `ConversationRepository`; no second process
may open the writable database.

## Storage and publication model

Schema files live under `sql/duckdb/`. Startup validates a checksummed Moraine
migration ledger before applying any missing migration in a transaction. A
newer, partial, foreign, or checksum-mismatched database is rejected without
rewriting its ledger.

`events.payload_json` contains the complete normalized and redacted event row
and is the only content authority. Derived search rows contain identifiers,
term frequencies, and document lengths only. Historical source generations
remain stored. Reads join `publication_heads` before listing, opening, or
ranking, so a prepared replacement is invisible until the transaction that
advances its head commits.

The tokenizer is ASCII-lowercase `[a-z0-9_]+`. Runtime extension installation,
autoloading, unsigned extensions, and DuckDB external access are disabled.
Database, WAL, owner-lock, and spill paths are kept under the configured
runtime root; Unix directories use mode `0700` and files use `0600`.

## Lifecycle

The backend opens and migrates DuckDB before constructing the repository
router, then starts embedded ingest unless `--no-ingest` is set. The central
daemon is mandatory: writable stdio fallback and project-only embedded mode
are rejected. Shutdown first notifies services and ingest, drains the shared
request runtime, checkpoints and closes DuckDB, and finally releases the
lifetime owner lock.

Manual DuckDB migration and live export/backup are intentionally unsupported.
The supported backup is cold: stop Moraine, confirm the owner lock is free and
the WAL has been checkpointed away, copy the database, restore it beneath a
private empty runtime root, and validate it through `moraine db doctor` plus a
retrieval digest.

## Focused validation

The crate's migration, permissions, lifetime-lock, publication, reopen, and
real-ingest tests run with:

```bash
cargo test -p moraine-duckdb --locked
```

The embedded-ingest test uses the retained Codex fixture through Moraine's real
parser, redactor, watcher, checkpoint, publication, and repository boundaries.
It verifies list, session-open, search, context-open, diagnostics, and table
preview against the committed DuckDB state.

All stack-facing validation must use the repository sandbox and must never
open a developer's installed `~/.moraine/` store. The authoritative promotion
requirements remain those in the DuckDB implementation plan: exact semantic
oracle results, crash recovery, all release targets, three 1 GiB capacity
runs, seven counterbalanced 8 GiB pairs, the fixed memory/latency/QPS/disk
ratios, and complete cleanup evidence. Local macOS results are useful smoke
evidence but cannot satisfy the Linux cgroup promotion gate.

For a local stack smoke test, use:

```bash
id=$(scripts/dev/sandbox/moraine-sandbox up --storage-engine duckdb --quiet)
scripts/dev/sandbox/moraine-sandbox status "$id"
scripts/dev/sandbox/moraine-sandbox down "$id"
```

DuckDB mode does not create or wait for a ClickHouse service. Its database,
WAL, owner lock, and spill files live only in the sandbox's isolated state
volume and are removed by `down`.
