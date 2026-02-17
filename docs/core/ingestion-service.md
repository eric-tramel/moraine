# Rust Ingestion Service

## Runtime Responsibility

`cortex-ingestor` is the sole writer that transforms JSONL lines from configured Codex and Claude trace sources into canonical event rows. It owns file-change detection, append-safe consumption, schema-aware normalization, and batched persistence with checkpoint advancement. Although retrieval runs elsewhere, this service defines the invariants retrieval depends on: stable event identity and source provenance. [src: rust/ingestor/src/main.rs:L7, rust/ingestor/src/ingestor.rs:L45, rust/ingestor/src/normalize.rs:L336, config/cortex.toml:L21-L33]

The process is started with a multi-thread Tokio runtime and a TOML config. At startup it pings ClickHouse, loads checkpoint state from `ingest_checkpoints`, instantiates channels and concurrency guards, and then starts watcher, debounce, reconcile, worker, sink, and heartbeat loops. The startup sequence intentionally front-loads dependency failure: if ClickHouse is unreachable, ingestion does not enter a partially alive state. [src: rust/ingestor/src/main.rs:L28, rust/ingestor/src/ingestor.rs:L47, rust/ingestor/src/ingestor.rs:L49, rust/ingestor/src/ingestor.rs:L145]

## Execution Model

The runtime is a hybrid of event-driven and reconciliation-driven scheduling. A watcher thread forwards file events to debounce logic, while reconcile periodically scans the full sessions glob. This dual path exists because watcher streams can drop events; reconcile guarantees eventual reinspection. [src: rust/ingestor/src/ingestor.rs:L187, rust/ingestor/src/ingestor.rs:L230, rust/ingestor/src/ingestor.rs:L284, rust/ingestor/src/ingestor.rs:L752]

Dispatch state uses `pending`, `inflight`, and `dirty`. Updates that arrive while a path is inflight are marked dirty, then replayed immediately after inflight completion. This preserves edits that race with long reads without queue blow-up. [src: rust/ingestor/src/ingestor.rs:L23, rust/ingestor/src/ingestor.rs:L124, rust/ingestor/src/ingestor.rs:L135, rust/ingestor/src/ingestor.rs:L536]

Concurrency is bounded by a semaphore (`max_file_workers`) and channel capacities (`max_inflight_batches`, derived process queue capacity). Worker tasks are spawned per file with owned semaphore permits, and sink writes are serialized by a single sink task. This architecture avoids lock-heavy shared mutable write paths while still allowing high parallelism in read/parse/normalize stages. [src: rust/ingestor/src/ingestor.rs:L60, rust/ingestor/src/ingestor.rs:L62, rust/ingestor/src/ingestor.rs:L74, rust/ingestor/src/ingestor.rs:L106]

## Checkpoint and File Identity Semantics

Checkpoint state is persisted in ClickHouse and cached in memory. Identity is `(source_file, source_inode, source_generation, last_offset, last_line_no)` plus status. `source_generation` is the anti-corruption key: if inode changes or file size drops below offset, generation increments and offsets reset, making rotation/truncation safe. [src: rust/ingestor/src/model.rs:L5, rust/ingestor/src/ingestor.rs:L584, rust/ingestor/src/ingestor.rs:L586, sql/001_schema.sql:L95]

A file is skipped when current size equals checkpoint offset and generation is unchanged. This optimization removes unnecessary scans on quiet files. Conversely, any generation change causes processing even if no new bytes are present, because downstream consumers need the checkpoint transition persisted to avoid stale inode assumptions later. [src: rust/ingestor/src/ingestor.rs:L593, rust/ingestor/src/ingestor.rs:L711]

Checkpoint updates are merged in-memory before flush. Replacement policy prefers higher generation, then higher offset within generation. This ensures that out-of-order chunk completion cannot regress progress. Because updates are eventually persisted through `ReplacingMergeTree(updated_at)`, retries are safe and idempotent at the logical checkpoint level. [src: rust/ingestor/src/ingestor.rs:L427, rust/ingestor/src/ingestor.rs:L434, sql/001_schema.sql:L104]

## Normalization Contract

Normalization starts with a raw ledger row storing original JSON, top-level type, hash, inferred session ID, and source coordinates. This row is emitted for every parseable object and remains the forensic replay surface. [src: rust/ingestor/src/normalize.rs:L372, sql/001_schema.sql:L3]

Canonical event rows are produced by branching on top-level type and payload subtype. Modern envelopes and legacy top-level records are mapped into shared fields (`event_class`, `payload_type`, `actor_role`, `call_id`, `name`, `text_content`), preserving compatibility across historical formats. [src: rust/ingestor/src/normalize.rs:L469, rust/ingestor/src/normalize.rs:L614, rust/ingestor/src/normalize.rs:L664]

Event identity is deterministic and source-based. `event_uid` is SHA-256 over source coordinates, generation, payload fingerprint, and suffix. That means replay of unchanged input yields stable IDs, while semantically distinct expansions (for example compacted-history children) receive unique suffix-scoped IDs. Deterministic identity is foundational for replacement-table semantics and trace joins. [src: rust/ingestor/src/normalize.rs:L100, rust/ingestor/src/normalize.rs:L108, rust/ingestor/src/normalize.rs:L675]

`compacted` records are represented as one parent `compacted_raw` row plus expanded children from `replacement_history`, each linked by `compacted_parent_uid`. This preserves both compaction boundaries and chronological detail. [src: rust/ingestor/src/normalize.rs:L674, rust/ingestor/src/normalize.rs:L694, rust/ingestor/src/normalize.rs:L716, sql/002_views.sql:L6]

Text extraction is recursively schema-aware but bounded. Message extraction walks nested arrays/objects and collects string fields from known keys, truncating very large strings to cap memory. Function arguments and tool output are similarly bounded at high character limits. This keeps indexing useful for verbose payloads without allowing unbounded per-row memory amplification. [src: rust/ingestor/src/normalize.rs:L53, rust/ingestor/src/normalize.rs:L89, rust/ingestor/src/normalize.rs:L498, rust/ingestor/src/normalize.rs:L526]

Token accounting is preserved rather than normalized into fixed numeric columns. For `event_msg` payloads with `token_count` type, the service stores a compact JSON blob in `token_usage_json`. This keeps ingestion schema-forward compatible with evolving token metadata while allowing downstream extraction in SQL when needed. [src: rust/ingestor/src/normalize.rs:L624, sql/001_schema.sql:L43]

## Sink, Flush, and Durability Semantics

The sink task is the ingestion durability boundary. Workers send `RowBatch` messages; sink aggregates rows and flushes on batch threshold or timer. Heartbeats are emitted from the same loop to expose queue depth, flush latency, and counters. [src: rust/ingestor/src/ingestor.rs:L311, rust/ingestor/src/ingestor.rs:L345, rust/ingestor/src/ingestor.rs:L362, rust/ingestor/src/ingestor.rs:L390]

Flush order is fixed and intentional: raw first, canonical second, expansions third, errors fourth, checkpoints last. The ordering prioritizes data availability before progress advancement. If flush fails before checkpoint insert, retried processing may duplicate rows but will not skip unseen data; this is exactly the intended at-least-once safety property. [src: rust/ingestor/src/ingestor.rs:L471, rust/ingestor/src/ingestor.rs:L478, rust/ingestor/src/ingestor.rs:L480]

On successful flush, counters and checkpoint cache advance. On failure, buffers remain resident and `flush_failures` increments; the service does not crash immediately. This favors continuity under transient DB instability but requires operators to monitor heartbeat error fields and queue depth. [src: rust/ingestor/src/ingestor.rs:L492, rust/ingestor/src/ingestor.rs:L500, rust/ingestor/src/ingestor.rs:L513, sql/003_ingest_heartbeats.sql:L14]

## Backpressure and Scheduling Under Load

Backpressure emerges naturally from bounded channels and semaphore limits. If workers produce faster than sink can flush, sink channel saturation slows worker sends. If enqueue rate exceeds worker drain rate, `queue_depth` rises and can be observed in heartbeat rows. This architecture avoids runaway memory on the happy path while providing clear observability when the system nears capacity. [src: rust/ingestor/src/ingestor.rs:L62, rust/ingestor/src/ingestor.rs:L74, rust/ingestor/src/ingestor.rs:L393, rust/ingestor/src/ingestor.rs:L545]

Debounce interval tuning materially changes behavior under churn. Very low debounce values reduce trigger latency but increase duplicate scheduling pressure; higher values coalesce better but delay visibility. The default of 50 ms is a compromise oriented toward fast local append workloads. Reconcile interval similarly trades CPU scan cost for missed-event recovery delay; default is 30 s. [src: config/cortex.toml:L18-19, rust/ingestor/src/ingestor.rs:L238]

Batch size and flush interval jointly define visibility cadence and insert efficiency. Larger batches and longer flush intervals reduce insert overhead and improve throughput; smaller values improve freshness at higher request rates and write amplification. The sink’s timer-based flush means low-traffic files still become visible promptly without waiting for high batch thresholds. [src: config/cortex.toml:L12-13, rust/ingestor/src/ingestor.rs:L326, rust/ingestor/src/ingestor.rs:L362]

## Failure Modes and Recovery Behavior

The highest-risk correctness failure mode is stale progress across file lifecycle changes. Generation rollover resets offsets on inode change or shrink; removing this logic can silently skip rotated data. [src: rust/ingestor/src/ingestor.rs:L584, rust/ingestor/src/ingestor.rs:L587]

Watcher unreliability is handled by design: reconcile rescans for eventual rediscovery, and checkpoints guarantee resumed offsets. If reconcile is disabled, correctness is no longer defensible under missed events. [src: rust/ingestor/src/ingestor.rs:L284, rust/ingestor/src/ingestor.rs:L296, rust/ingestor/src/ingestor.rs:L572]

Malformed records are logged to `ingest_errors` and skipped. This prevents pipeline stalls but can hide upstream regressions if error rates are not observed. Operational policy should treat increasing `ingest_errors` as a schema drift signal requiring normalization updates, not as harmless noise. [src: rust/ingestor/src/ingestor.rs:L648, sql/001_schema.sql:L80]

During prolonged ClickHouse outages, in-memory buffers can grow because failed flushes keep data resident. The system does not currently implement disk-backed queue spill or adaptive throttling. For heavy ingestion workloads, this is the primary known limitation and should inform incident response: restore DB availability quickly or temporarily reduce input rate. [src: rust/ingestor/src/ingestor.rs:L512, rust/ingestor/src/clickhouse.rs:L62]

## Tuning and Extension Guidance

When tuning throughput, change one parameter class at a time and observe heartbeats and lag indicators. Typical order is: increase `max_file_workers`, then `batch_size`, then adjust `flush_interval_seconds`, then revisit `max_inflight_batches`. Simultaneous broad changes make causal attribution impossible under bursty append traffic. [src: config/cortex.toml:L11-L12, config/cortex.toml:L15-L16, sql/003_ingest_heartbeats.sql:L5]

When adding new event payload shapes, extend `normalize_record` rather than branching downstream SQL to interpret raw payload blobs. Canonical field population is the contract boundary; if it weakens, retrieval and analytics logic accumulate format-specific conditionals and eventually diverge. Every new branch should preserve source coordinates, deterministic UID behavior, and payload JSON preservation semantics. [src: rust/ingestor/src/normalize.rs:L157, rust/ingestor/src/normalize.rs:L177, rust/ingestor/src/normalize.rs:L336]

When changing extraction or token-usage capture, verify impacts in both canonical tables and search projections. Search materialized views depend on non-empty `text_content`; extraction regressions directly reduce recall and can appear as retrieval quality failures rather than ingestion incidents. This coupling is subtle and should be tested explicitly after normalization edits. [src: sql/004_search_index.sql:L53, sql/004_search_index.sql:L80, rust/ingestor/src/normalize.rs:L651]

The implementation is intentionally explicit. Preserve that property when extending the service.
