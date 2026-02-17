# ClickHouse Data Model

## Modeling Goals

The ClickHouse schema is not just storage; it is the formal contract that binds ingestion correctness, retrieval freshness, and operator observability. The model has three goals that must hold simultaneously: retain source-level provenance for forensic replay, expose a deterministic logical conversation trace, and maintain sparse lexical structures incrementally as writes land. These goals are encoded directly in table engines, order keys, and view definitions rather than delegated to application-side conventions. [src: sql/001_schema.sql:L3, sql/002_views.sql:L61, sql/004_search_index.sql:L1]

A useful way to read the schema is to classify tables by lifecycle role. `raw_events` and canonical event tables hold ingestion truth. `ingest_*` tables hold runtime state and telemetry. `v_*` views define the reconstruction surface. `search_*` tables hold retrieval acceleration and feedback logs. This partitioning keeps responsibilities explicit and makes incidents easier to localize: storage defects, normalization defects, ordering defects, and retrieval defects each leave signatures in different strata. [src: sql/001_schema.sql:L3, sql/001_schema.sql:L80, sql/002_views.sql:L6, sql/004_search_index.sql:L180]

## Canonical Event Tables

`raw_events` is the immutable ingestion ledger. Each row records source file identity, source generation, line and byte offsets, top-level type, inferred session ID, full serialized JSON, and a hash. There is no attempt to deduplicate raw rows. This table exists so any downstream interpretation can be re-derived and audited against exact input bytes. [src: sql/001_schema.sql:L3, sql/001_schema.sql:L15]

`events` is the primary canonical event stream for modern and legacy records after classification. It is stored with `ReplacingMergeTree(event_version)` and sorted by session/time/source coordinates. The replacing engine choice acknowledges at-least-once ingestion: duplicates or superseded versions may appear transiently, but converged reads reflect the highest `event_version` per sort key. This is eventual idempotence, not immediate uniqueness. [src: sql/001_schema.sql:L23, sql/001_schema.sql:L73, sql/001_schema.sql:L88]

`event_links` stores normalized lineage edges (`parent_event`, `compacted_parent`, `tool_use_id`, and related link types) keyed by event/session coordinates. Keeping links in a separate replacing table avoids repeating wide event payload columns while preserving explicit parent/child and external-ID relationships. [src: sql/001_schema.sql:L92, sql/001_schema.sql:L103, sql/001_schema.sql:L108]

`tool_io` stores extracted tool input/output payloads keyed by `(session_id, tool_call_id, event_uid)` with replacing semantics. This keeps tool request/response blobs directly queryable without forcing every consumer to parse full `events.payload_json` rows. [src: sql/001_schema.sql:L112, sql/001_schema.sql:L118, sql/001_schema.sql:L134]

`ingest_errors` captures non-fatal parsing and normalization failures with source coordinates and raw fragments. This table is operationally important because it turns silent schema drift into measurable artifacts. A growing error trend is usually the earliest signal that upstream event shape changed in a way normalization no longer handles. [src: sql/001_schema.sql:L80, sql/001_schema.sql:L87]

`ingest_checkpoints` stores resumable file progress with replacing semantics on `updated_at`. The critical data is not only offset and line number but also inode and generation, which enables safe continuation across truncation and rotation. Checkpoint rows should be interpreted as a latest-state stream, not historical audit of every offset transition. [src: sql/001_schema.sql:L95, sql/001_schema.sql:L98, sql/001_schema.sql:L104]

`ingest_heartbeats` is a low-latency telemetry stream written by the sink loop. It exposes queue depth, files active/watched, cumulative write counters, flush latency, and last error. This table is the quickest way to answer whether ingestion is healthy, behind, or repeatedly failing writes, and should be consulted before log spelunking. [src: sql/003_ingest_heartbeats.sql:L1, sql/003_ingest_heartbeats.sql:L5, sql/003_ingest_heartbeats.sql:L11]

## View Layer and Trace Semantics

`v_all_events` projects `events` into a retrieval-friendly shape (`event_class`, `actor_role`, `call_id`, `phase`) and exposes compacted lineage as `compacted_parent_uid` via `origin_event_id`. The view does not enforce dedup; it preserves the same replacing-table convergence semantics as its source rows. [src: sql/002_views.sql:L6, sql/002_views.sql:L10, sql/002_views.sql:L32]

`v_conversation_trace` is the authoritative reconstruction interface. It derives `event_time` by parsing `record_ts` with fallback to `ingested_at`, then computes `event_order` using a stable ordered window over time and source coordinates. The fallback matters because some records can carry missing or unparsable timestamps; trace ordering remains deterministic regardless. [src: sql/002_views.sql:L72-73, sql/002_views.sql:L75]

`turn_seq` in `v_conversation_trace` is computed as cumulative count of `turn_context` markers, clamped to at least one. This gives a robust turn counter even for sessions that start with non-turn records or have sparse context markers. Turn derivation is therefore data-driven and reproducible, not dependent on client-side state machines. [src: sql/002_views.sql:L77, sql/002_views.sql:L80]

`v_turn_summary` and `v_session_summary` are convenience aggregations derived from the trace view. They provide counts and temporal boundaries useful for diagnostics, but they are intentionally lossy compared to the trace. When accuracy disputes occur, treat these summaries as derived projections and validate against `v_conversation_trace`. [src: sql/002_views.sql:L100, sql/002_views.sql:L116]

## Search Index Tables and Maintenance

`search_documents` is the document projection used for lexical retrieval. It captures event metadata, text payload, and `doc_len`, where `doc_len` is materialized from regex token extraction on lowercased text (`[a-z0-9_]+`). The table uses `ReplacingMergeTree(doc_version)` keyed by event UID, so document refresh follows the same eventual replacement posture as canonical tables. [src: sql/004_search_index.sql:L1, sql/004_search_index.sql:L22, sql/004_search_index.sql:L24]

`mv_search_documents_from_events` feeds `search_documents` from `events` while filtering out whitespace-only text. The projection carries `origin_event_id` as `compacted_parent_uid`, so retrieval candidates keep compacted lineage without a second expansion table. Empty payload rows remain available in canonical tables but are excluded from retrieval corpus. [src: sql/004_search_index.sql:L42, sql/004_search_index.sql:L49, sql/004_search_index.sql:L68]

`search_postings` stores sparse term-doc rows with term frequency, document length, and context metadata. It is partitioned by `cityHash64(term) % 32` and ordered by `(term, doc_id)`, optimizing term-constrained lookups and reducing scan footprint under high cardinality vocabularies. Postings are built by array-joining extracted tokens and grouping by `(term, doc)` with length bounds 2..64. [src: sql/004_search_index.sql:L82, sql/004_search_index.sql:L97, sql/004_search_index.sql:L129, sql/004_search_index.sql:L133]

`search_term_stats` and `search_corpus_stats` use `SummingMergeTree` to maintain incremental DF and corpus totals. They are not the sole source of truth; the MCP service includes fallback aggregate queries over base tables if stats are absent or incomplete. This dual path protects query behavior during schema bootstrap or temporary MV lag. [src: sql/004_search_index.sql:L147, sql/004_search_index.sql:L162, rust/codex-mcp/src/main.rs:L572, rust/codex-mcp/src/main.rs:L609]

Query telemetry is captured in `search_query_log` and `search_hit_log`, with optional external feedback storage in `search_interaction_log`. These tables allow relevance diagnostics and evaluation loops without changing retrieval response format. They are also useful for understanding workload shape, query latency distribution, and result sparsity patterns over time. [src: sql/004_search_index.sql:L180, sql/004_search_index.sql:L201, sql/004_search_index.sql:L220]

## Engine Choices and Their Consequences

The schema relies primarily on `MergeTree` and `ReplacingMergeTree`, each matched to a lifecycle pattern. Plain `MergeTree` is used for append-only ledgers and telemetry where every row should persist as written (`raw_events`, `ingest_errors`, `ingest_heartbeats`, query/hit logs). `ReplacingMergeTree` is used where at-least-once delivery can produce superseded rows and logical latest-state convergence is required (`events`, `event_links`, `tool_io`, `ingest_checkpoints`, `search_documents`, `search_postings`). `search_term_stats` and `search_corpus_stats` are exposed as views over `FINAL` reads, not separate summing tables. [src: sql/001_schema.sql:L19, sql/001_schema.sql:L88, sql/001_schema.sql:L108, sql/001_schema.sql:L134, sql/004_search_index.sql:L151, sql/004_search_index.sql:L161]

The operational implication is that freshness and uniqueness are different concepts. Newly inserted rows are immediately queryable, but replacement convergence still depends on background merges. Engineers must avoid assuming strict immediate dedup in high-churn windows. Where absolute latest-state reads are required, query patterns should account for engine behavior rather than presuming physical collapse has already occurred. [src: sql/001_schema.sql:L88, sql/004_search_index.sql:L33]

## Reconstruction Walkthrough

To reconstruct a full conversation deterministically, query `v_conversation_trace` filtered by session ID and ordered by `event_order`. This single view provides turn sequence, actor role, event class, payload type, and source reference, all in stable order. If a record’s semantics are disputed, use `source_ref` to locate originating file and compare against `raw_events.raw_json`. [src: sql/002_views.sql:L61, sql/002_views.sql:L76, sql/001_schema.sql:L13]

Tool call lineage is represented through `call_id` in `v_all_events` (mapped from `events.tool_call_id`) and event class pairings such as `tool_call` and `tool_result`. Compacted lineage is represented through `compacted_parent_uid` (mapped from `events.origin_event_id`) and optional `event_links` rows with `link_type='compacted_parent'`. Together, these fields support both conversational and execution-trace reconstruction without a separate expansion table. [src: sql/001_schema.sql:L47, sql/001_schema.sql:L49, sql/001_schema.sql:L103, sql/002_views.sql:L10, sql/002_views.sql:L25]

Token accounting payloads are preserved in `token_usage_json` rather than exploded into rigid columns. When token-level analytics are needed, parse JSON at query time into derived metrics. This keeps ingestion and schema evolution decoupled from provider-specific token metadata changes. [src: sql/001_schema.sql:L43, rust/ingestor/src/normalize.rs:L624]

## Schema Evolution Guidance

Treat SQL files as contract surfaces, not migration suggestions. The bootstrap path executes all bundled migrations in order through `moraine db migrate`. Any breaking schema change should include compatibility reasoning for existing views and consumers, otherwise operational startup can succeed while semantic correctness regresses.

When changing normalization fields that feed retrieval, evaluate the full chain: canonical columns, document projection MVs, posting generation, stats maintenance, and MCP filters. If tokenization or text projection semantics change, run `bin/backfill-search-index` to avoid mixed historical semantics across pre-change and post-change rows. [src: sql/004_search_index.sql:L28, sql/004_search_index.sql:L100, bin/backfill-search-index:L74]

When adding new event classes, ensure they are reflected consistently in both canonical classification and retrieval policy. MCP defaults intentionally exclude high-noise operational payload types and optionally exclude codex-mcp self-events; adding new classes without policy review can inflate recall noise and degrade ranking quality. [src: rust/codex-mcp/src/main.rs:L511, rust/codex-mcp/src/main.rs:L517, rust/codex-mcp/src/main.rs:L523]

The schema is compact enough that drift is usually introduced by small, local edits with broad downstream impact. Make those impacts explicit in docs and QC artifacts before changing production interpretation.
