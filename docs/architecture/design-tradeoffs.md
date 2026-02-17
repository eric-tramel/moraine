# Design Tradeoffs

## Decision Context

Cortex was built under unusually tight constraints: complete locality, near-real-time ingest visibility, multi-format trace fidelity, and lightweight retrieval for agent workflows. The important point is that these constraints conflict in practice. A design that optimizes one axis naively will usually violate another. This document records the decisions that resolved those conflicts and the conditions under which each decision should be revisited.

These are architecture decisions, not implementation trivia. They explain why current code and schema look the way they do and provide a stable reference when proposing future changes. [src: config/clickhouse.xml:L17, config/cortex.toml:L11, rust/codex-mcp/src/main.rs:L264]

## ADR-001: Local ClickHouse as the System Database

The system chose ClickHouse as the single local database for raw ingest ledger, canonical events, turn/session views, sparse lexical indexes, and query feedback logs. This decision deliberately avoids introducing separate specialized stores for OLAP, retrieval, and telemetry. [src: sql/001_schema.sql:L1, sql/002_views.sql:L6, sql/004_search_index.sql:L1]

The alternative was a lighter embedded database (for example SQLite-class storage) paired with application-side indexing. That would have reduced binary footprint but pushed substantial complexity into service code: manual incremental index maintenance, limited concurrency under high append rates, and weaker large-scale aggregation performance for diagnostic queries.

ClickHouse provides three primitives Cortex relies on heavily: MergeTree write throughput, materialized view fanout, and SQL window/aggregation capabilities. These map directly to ingest workloads and retrieval design: the system can append aggressively, derive view-based ordering deterministically, and maintain sparse indexes incrementally in storage. [src: sql/001_schema.sql:L17, sql/002_views.sql:L73, sql/004_search_index.sql:L100]

The cost is a heavier runtime than embedded alternatives and a larger schema/engine tuning surface. The mitigation is strict locality: loopback binding, local path templating, and a single Rust lifecycle surface (`cortexctl`) that keeps runtime behavior transparent.

Revisit trigger: if operating envelope shifts to extremely constrained devices where ClickHouse memory/CPU profile becomes unacceptable, revisit this decision with measured workload traces rather than intuition.

## ADR-002: Rust-Only Ingestion Runtime

Ingestion is implemented as a Rust async service and legacy Python ingestion paths were removed from operational flow. The reason is not language preference; it is control over concurrency, backpressure, and predictable behavior under sustained append load.

The most important benefit is explicit control of pressure points. Worker concurrency is semaphore-limited, dispatch queues are bounded, sink channels are bounded, and retry behavior is explicit. In an interpreter-based ad hoc pipeline, these limits are often implicit or unevenly enforced, which leads to intermittent stalls and memory spikes at higher event rates. [src: rust/ingestor/src/ingestor.rs:L60, rust/ingestor/src/ingestor.rs:L62, rust/ingestor/src/ingestor.rs:L74]

The cost is higher implementation complexity and slower iteration for small parser tweaks. Cortex mitigates this by keeping normalization logic centralized and testable in one module while keeping service wiring explicit and compact.

Revisit trigger: if ingest event rate drops permanently to trivial volume and operational simplicity dominates throughput concerns, a simpler runtime could be reconsidered; however, this would need a concrete replacement for current checkpoint, backpressure, and watcher-reconcile behavior.

## ADR-003: Event Watcher Plus Reconcile Scanner

The scheduling model combines OS file events with periodic full-glob reconciliation. This decision explicitly rejects a watcher-only model. Filesystem watcher streams are low-latency but not a perfect reliability substrate; dropped or coalesced events are realistic in long-running local systems. [src: rust/ingestor/src/ingestor.rs:L187, rust/ingestor/src/ingestor.rs:L284]

In Cortex, watcher events drive fast path freshness while reconcile bounds correctness lag. Reconcile cost is small periodic filesystem scanning overhead, but it buys deterministic eventual coverage and removes dependence on watcher infallibility.

The alternative, watcher-only ingestion, has lower background cost but cannot guarantee eventual completeness when events are missed. For trace reconstruction workloads, silent omission is worse than slight periodic overhead.

Revisit trigger: if platform-specific watcher reliability can be proven and verified continuously with sequence-based guarantees, reconcile interval could be expanded significantly; full removal remains high risk unless an equivalent guarantee exists.

## ADR-004: Preserve Raw Payloads and Source Coordinates

The canonical model stores both extracted fields and full payload JSON, plus complete source coordinates. This is a conscious decision to prioritize reconstruction and future parser evolution over maximal storage compactness. [src: sql/001_schema.sql:L13, sql/001_schema.sql:L42, rust/ingestor/src/normalize.rs:L157]

The tempting alternative is aggressive projection and early pruning: keep only normalized text and a small set of typed fields. That approach reduces storage but destroys two critical capabilities. First, it removes the ability to diagnose parser/classification mistakes against exact source payloads. Second, it makes adapting to new payload forms difficult because historical rows no longer contain recoverable context.

Cortex chooses storage overhead to preserve forensic depth. This is aligned with system purpose: maintainers need to reconstruct conversations and tooling behavior, not only aggregate message counts.

Revisit trigger: if storage growth becomes a hard operational limit, introduce retention tiers that preserve canonical windows while archiving or compressing raw payload history, rather than dropping provenance fields globally.

## ADR-005: At-Least-Once Ingest with Replacing Semantics

Cortex does not attempt exactly-once ingestion across file append, truncation, and rotation scenarios. Instead it implements at-least-once processing and depends on deterministic event IDs plus `ReplacingMergeTree` convergence to yield logical idempotence. [src: rust/ingestor/src/normalize.rs:L100, sql/001_schema.sql:L46]

This decision is driven by practical file-system reality. Exactly-once requires heavyweight coordination and durable transaction markers that are brittle in local file event environments. At-least-once with generation-aware checkpoints is simpler and robust under common failure modes.

The tradeoff is transient duplicates before merges converge. This is acceptable because retrieval and analytics surfaces tolerate eventual replacement; correctness is preserved over time, and ingestion continuity is stronger under failure.

Revisit trigger: if downstream consumers require strict immediate uniqueness guarantees for transactional workflows, add read-time dedup strategies or specialized materialized latest-state views before considering ingestion model replacement.

## ADR-006: Incremental Sparse Indexing in ClickHouse

Sparse lexical structures are maintained continuously with materialized views and query-time BM25 scoring in MCP. This decision avoids rebuilding indexes in-process or scanning canonical event tables for each query. [src: sql/004_search_index.sql:L28, sql/004_search_index.sql:L100, rust/codex-mcp/src/main.rs:L410]

The alternative was in-memory lexical indexing inside MCP. That would simplify SQL but couple index correctness to process lifetime and require explicit rebuild logic on startup or schema changes. It also introduces synchronization complexity if multiple MCP instances run concurrently.

Current design pushes index maintenance into database writes and keeps MCP stateless over corpus structures. The tradeoff is write amplification from MV fanout and dependence on ClickHouse merge behavior. For this workload, the trade is favorable because ingestion already writes to ClickHouse and retrieval latency benefits from precomputed sparsity.

Revisit trigger: if query latency becomes dominated by SQL overhead despite sparse postings and tuned filters, evaluate hybrid approaches (for example cached top-K postings in process) while keeping ClickHouse as source of truth.

## ADR-007: Retrieval Defaults Favor Signal Over Completeness

MCP search defaults intentionally exclude many operational/tool lifecycle events and optionally exclude codex-mcp self-events. This decision optimizes agent answer quality by reducing lexical noise. [src: rust/codex-mcp/src/main.rs:L511, rust/codex-mcp/src/main.rs:L523, config/cortex.toml:L40-L41]

The alternative is maximal recall: include all event classes by default and let downstream agents filter. In practice this pushes noise handling to every client and leads to repeated poor ranking behavior where high-frequency operational tokens dominate.

Cortex defaults to conservative inclusion and makes broader retrieval opt-in via arguments. This preserves usability for common workflows while retaining flexibility for forensic modes.

Revisit trigger: if workloads shift toward tool-trace diagnostics as the dominant use case, default include policy may need to widen, but only with measured relevance impact using query/hit logs.

## ADR-008: Query and Hit Logging as First-Class Tables

Search telemetry is persisted by default into dedicated query and hit log tables, with optional async writes. This is a strategic decision to make relevance diagnostics and performance auditing possible from day one rather than bolting instrumentation on after quality regressions appear. [src: sql/004_search_index.sql:L180, rust/codex-mcp/src/main.rs:L650, rust/codex-mcp/src/main.rs:L689]

The cost is additional write volume per query. The benefit is long-term leverage: ranking regressions, filter policy debates, and tuning decisions can be informed by observed query distributions and hit outcomes rather than anecdote.

Revisit trigger: if query throughput grows enough that sync logging becomes a measurable latency contributor, enable async logging or move telemetry writes to a buffered pathway while preserving schema compatibility.

## Decision Summary

Across all decisions, the consistent pattern is this: Cortex spends complexity in deterministic ingestion semantics and schema design so that retrieval remains thin and operational debugging remains tractable. The system is intentionally not minimal in table count or code paths. It is minimal in hidden behavior. That distinction should remain the bar for future changes.
