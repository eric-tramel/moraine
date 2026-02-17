# System Architecture

## System Objective

Moraine converts append-heavy JSONL streams from `~/.codex/sessions/**/*.jsonl` and `~/.claude/projects/**/*.jsonl` into a local, queryable corpus that supports both rapid operational inspection and faithful trace reconstruction. This is a single-node operational index, not a distributed analytics system: [ClickHouse](https://github.com/ClickHouse/ClickHouse) binds to loopback, state is rooted under `~/.moraine`, and lifecycle is managed through `moraine`.

The design prioritizes low append-to-visibility latency, replay fidelity across evolving record formats, and a thin retrieval process that relies on write-time index maintenance. These priorities drive the ingestion/runtime and schema choices documented below. [src: rust/ingestor/src/ingestor.rs:L45, rust/ingestor/src/normalize.rs:L336, sql/004_search_index.sql:L28, rust/codex-mcp/src/main.rs:L337]

## Component Topology

The system is composed of four layers with explicit ownership boundaries.

The storage layer is a local ClickHouse instance configured from templated XML and started by `moraine up`. It owns canonical tables, reconstruction views, sparse lexical indexes, and query/hit logs; services interact through SQL only.

The ingestion layer is Rust `moraine-ingest`: watcher plus reconcile scheduling, normalization, batching, checkpoint persistence, and heartbeat emission. It is the only transformation boundary from raw JSON to canonical classes. [src: rust/ingestor/src/ingestor.rs:L187, rust/ingestor/src/ingestor.rs:L284, rust/ingestor/src/ingestor.rs:L390, rust/ingestor/src/normalize.rs:L336]

Indexing is implemented with ClickHouse materialized views that incrementally maintain `search_documents`, `search_postings`, and stats tables as canonical rows land. This shifts cost from corpus scans to sparse term lookups. [src: sql/004_search_index.sql:L28, sql/004_search_index.sql:L100, sql/004_search_index.sql:L154, sql/004_search_index.sql:L170]

The retrieval layer is `moraine-mcp`, a stdio JSON-RPC server exposing `search` and `open`. It performs BM25 scoring over prebuilt postings and returns prose or full JSON output without owning index-state lifecycle.

## End-to-End Causal Flow

A concrete append event follows a deterministic path. A JSONL line is appended in a session file. The watcher thread receives a file event and forwards the path to a debounce queue. After debounce expiration, the path is enqueued unless it is already inflight, in which case it is marked dirty for replay after current processing completes. This dirty-path rule prevents lost updates when writes race with in-progress file reads. [src: rust/ingestor/src/ingestor.rs:L124, rust/ingestor/src/ingestor.rs:L195, rust/ingestor/src/ingestor.rs:L230, rust/ingestor/src/ingestor.rs:L535]

A worker resumes from checkpoint offset, parses appended lines, and emits raw rows plus canonical `events` rows. When lineage or tool payloads are present, it also emits `event_links` and `tool_io` rows. Invalid or non-object lines are captured in `ingest_errors` and do not halt processing. [src: rust/ingestor/src/ingestor.rs:L597, rust/ingestor/src/ingestor.rs:L632, rust/ingestor/src/ingestor.rs:L635, sql/001_schema.sql:L23, sql/001_schema.sql:L92, sql/001_schema.sql:L112]

Batch chunks flow to a sink that flushes by size or timer in fixed order: `raw_events`, `events`, `event_links`, `tool_io`, `ingest_errors`, `ingest_checkpoints`. Success advances counters/cache; failure retains buffers for retry. [src: rust/ingestor/src/ingestor.rs:L597, rust/ingestor/src/ingestor.rs:L598, rust/ingestor/src/ingestor.rs:L599, rust/ingestor/src/ingestor.rs:L600, rust/ingestor/src/ingestor.rs:L601, rust/ingestor/src/ingestor.rs:L603]

As canonical rows commit, MVs update searchable documents, postings, and stats. MCP search then tokenizes input, loads corpus/DF stats, builds BM25 SQL over postings, and returns ranked hits without scanning the full trace view. [src: sql/004_search_index.sql:L22, sql/004_search_index.sql:L129, sql/004_search_index.sql:L147, rust/codex-mcp/src/main.rs:L346, rust/codex-mcp/src/main.rs:L533, rust/codex-mcp/src/main.rs:L572]

## Architectural Invariants

Invariant one is source-addressable provenance. Every canonical event row carries source coordinates (`source_file`, `source_generation`, `source_line_no`, `source_offset`) plus `source_ref`. This allows reconstruction to be traced back to an exact byte region in an original log file. Without this invariant, replay debugging would degrade into heuristic matching of payload text. [src: sql/001_schema.sql:L27, rust/ingestor/src/normalize.rs:L166]

Invariant two is monotonic checkpoint advancement within a generation, with generation rollover on inode change or truncation. If the file inode changes or file length shrinks below stored offset, generation increments and offset/line reset to zero. This is the anti-corruption rule that makes log rotation and truncation safe without global rewinds. [src: rust/ingestor/src/ingestor.rs:L584, rust/ingestor/src/ingestor.rs:L586, rust/ingestor/src/model.rs:L8]

Invariant three is at-least-once processing with eventual replacement semantics. Reprocessing can occur due to retries, dirty-path rescheduling, or reconcile scans. Idempotence is achieved by stable event UIDs and `ReplacingMergeTree(event_version)` in mutable canonical tables, accepting transient duplicates until merges converge. The architecture explicitly trades immediate dedup for robust ingestion continuity. [src: rust/ingestor/src/normalize.rs:L100, sql/001_schema.sql:L46, sql/001_schema.sql:L76, rust/ingestor/src/ingestor.rs:L294]

Invariant four is deterministic conversation ordering derived at query time. `v_conversation_trace` computes `event_order` using an ordered window over parsed record timestamp fallback plus source coordinates, then derives `turn_seq` as cumulative `turn_context` count clamped to at least one. This keeps ordering logic centralized and reproducible across callers. [src: sql/002_views.sql:L72-73, sql/002_views.sql:L77]

Invariant five is retrieval-service thinness. `codex-mcp` does not own corpus state and does not rebuild lexical indexes. It expects postings and statistics to exist, with fallback aggregate queries when stats are missing, and applies lightweight SQL generation and formatting only. This keeps the MCP process simple enough to run per-client without state synchronization concerns. [src: rust/codex-mcp/src/main.rs:L572, rust/codex-mcp/src/main.rs:L591, rust/codex-mcp/src/main.rs:L716]

## Failure and Recovery Model

Watcher loss is treated as expected, not exceptional. The reconcile task scans the sessions glob on a fixed interval and enqueues matching files. If filesystem events are dropped, reconcile eventually repairs visibility by re-queueing files and relying on checkpoints to skip already consumed offsets. Recovery latency is bounded by reconcile interval and queue pressure. [src: rust/ingestor/src/ingestor.rs:L284, rust/ingestor/src/ingestor.rs:L296, config/moraine.toml:L19]

Parse errors are quarantined. A malformed JSON line yields a row in `ingest_errors` with source coordinates and truncated fragment, then ingestion continues with the next line. This keeps corruption blast radius local to one record and preserves forward progress under partially malformed logs. [src: rust/ingestor/src/ingestor.rs:L647, rust/ingestor/src/ingestor.rs:L654, sql/001_schema.sql:L80]

ClickHouse unavailability blocks startup and appears later as flush/heartbeat failures. The service currently retains unsent batches in memory and retries; it does not spill to disk. Prolonged outages therefore translate directly into memory pressure under sustained input. [src: rust/ingestor/src/ingestor.rs:L47, rust/ingestor/src/ingestor.rs:L405, rust/ingestor/src/ingestor.rs:L512]

MCP failures are intentionally narrow: inputs are regex-sanitized, SQL literals escaped, missing `open` targets return `found=false`, and telemetry write failures do not fail query responses. [src: rust/codex-mcp/src/main.rs:L701, rust/codex-mcp/src/main.rs:L721, rust/codex-mcp/src/main.rs:L734, rust/codex-mcp/src/main.rs:L1034]

## Performance Envelope

The ingestion runtime controls throughput through four principal knobs: file-worker concurrency, inflight channel capacity, batch size, and flush interval. The defaults are not arbitrary; they reflect a bias toward sustained throughput with bounded latency (`max_file_workers=8`, `max_inflight_batches=64`, `batch_size=4000`, `flush_interval_seconds=0.5`). This setup performs well under concurrent append streams while keeping write amplification manageable. [src: config/moraine.toml:L11-L12, config/moraine.toml:L15-L16]

Backpressure is explicit: bounded processing/sink channels, semaphore-limited workers, and heartbeat queue-depth telemetry that surfaces pressure before visible staleness. [src: rust/ingestor/src/ingestor.rs:L60, rust/ingestor/src/ingestor.rs:L62, rust/ingestor/src/ingestor.rs:L74, rust/ingestor/src/ingestor.rs:L393]

Retrieval runtime cost scales primarily with query term count and posting list fanout, not total corpus size. Query tokenization caps terms (`max_query_terms`) and BM25 SQL applies `term IN [..]`, `matched_terms` thresholds, and result limits, all of which bound compute. The practical implication is that interactive latency remains predictable so long as term selectivity is reasonable and tool-event noise filters remain enabled by default. [src: rust/codex-mcp/src/main.rs:L346, rust/codex-mcp/src/main.rs:L505, rust/codex-mcp/src/main.rs:L564, config/moraine.toml:L50]

## Design Pressure and Rejected Alternatives

A pure filesystem event pipeline without reconcile was rejected because it optimizes latency at the cost of silent completeness failures. For this domain, eventual correctness is more valuable than minimal background work; missed events are unacceptable for trace reconstruction. [src: rust/ingestor/src/ingestor.rs:L284, rust/ingestor/src/ingestor.rs:L296]

An in-memory-only BM25 index in the MCP process was rejected for freshness and operational reasons. It would force full or partial index rebuilds in process memory and introduce state lifecycle coupling between retrieval process uptime and index correctness. ClickHouse materialized views already provide the incremental maintenance primitive with durable state. [src: sql/004_search_index.sql:L28, sql/004_search_index.sql:L100, rust/codex-mcp/src/main.rs:L410]

An exactly-once ingest guarantee was rejected because file-based append streams with rotation and watcher nondeterminism make strict exactly-once expensive and brittle. The selected model is at-least-once plus replacing semantics, which is simpler, resilient, and sufficient for this workload. [src: rust/ingestor/src/ingestor.rs:L584, sql/001_schema.sql:L46, sql/001_schema.sql:L104]

## Operator Implications

Operators should treat schema and normalization as contracts. Changes to event classification, tokenization, or checkpoint semantics have cross-layer effects that can silently degrade retrieval or recovery behavior if not reviewed together.

For day-to-day operation, health should be interpreted as a chain, not a single ping. A healthy chain includes ClickHouse availability, progressing `raw_events` and canonical event counts, and recent heartbeat timestamps with stable flush latency. A broken link anywhere in this chain predicts stale retrieval even when one service appears alive. Use `bin/moraine status` as the primary health entrypoint.
