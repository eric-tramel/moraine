# BM25 Indexing and Retrieval

## Retrieval Objective

Moraine retrieval is designed around one principle: ranking should be cheap at query time because expensive corpus transformation has already been paid incrementally at ingest time. This is the operational equivalent of [BM25S](https://bm25s.github.io/) thinking, but implemented with [ClickHouse](https://github.com/ClickHouse/ClickHouse) tables and materialized views instead of in-memory sparse matrices. The consequence is a thin MCP process that computes scores and formats responses while delegating index freshness and persistence to the database layer. [src: sql/004_search_index.sql:L28, sql/004_search_index.sql:L100, rust/codex-mcp/src/main.rs:L337]

The retrieval stack has three cooperating stages. Stage one is document projection from canonical events into `search_documents`. Stage two is sparse postings and corpus statistics maintenance. Stage three is query-time BM25 scoring with runtime filters in `codex-mcp`. Understanding this separation is essential for debugging: if search quality degrades, determine whether the issue is in projection, indexing, or ranking policy before tuning constants. [src: sql/004_search_index.sql:L1, sql/004_search_index.sql:L82, rust/codex-mcp/src/main.rs:L482]

## Index Construction in ClickHouse

`search_documents` is the document surface. It stores event UID, session metadata, class/type fields, payload JSON, and textual content plus computed `doc_len`. `doc_len` is materialized using regex token extraction over lowercased text. This materialization means average document length and BM25 denominator terms are cheap to compute later, with no repeated tokenization in the MCP process. [src: sql/004_search_index.sql:L1, sql/004_search_index.sql:L22]

Documents are produced by one materialized view, `mv_search_documents_from_events`, sourced from `events`. The projection maps canonical event fields into retrieval columns, carries `origin_event_id` as `compacted_parent_uid`, and filters out rows whose text is only whitespace. This keeps corpus size aligned with retrieval utility and avoids ranking overhead on structural events without textual payloads. [src: sql/004_search_index.sql:L42, sql/004_search_index.sql:L49, sql/004_search_index.sql:L68, sql/004_search_index.sql:L69]

`search_postings` is the sparse index. The MV explodes each document into tokens via `arrayJoin(extractAll(...))`, filters term lengths to 2..64, groups by `(term, doc)`, and stores `tf` along with document and context metadata. This table is partitioned by hashed term buckets and ordered by `(term, doc_id)`, making term-constrained scans efficient even as corpus grows. [src: sql/004_search_index.sql:L97, sql/004_search_index.sql:L100, sql/004_search_index.sql:L129, sql/004_search_index.sql:L133]

Two stats views are defined over indexed data. `search_term_stats` computes per-term document counts from `search_postings FINAL`, while `search_corpus_stats` computes corpus-wide document count and summed doc length from `search_documents FINAL`. This keeps stats derivation aligned with the current index state without separate additive-maintenance tables. [src: sql/004_search_index.sql:L151, sql/004_search_index.sql:L156, sql/004_search_index.sql:L161, sql/004_search_index.sql:L167]

## Query Processing in `codex-mcp`

Search starts with query tokenization using `[A-Za-z0-9_]+`, lowercasing, and length limits. Term count is capped by config (`max_query_terms`). The service preserves token order and term frequency in the tokenizer output, but current SQL scoring path treats each unique token once through term-level postings and IDF maps. Query validation rejects empty or non-searchable inputs early. [src: rust/codex-mcp/src/main.rs:L346, rust/codex-mcp/src/main.rs:L989, rust/codex-mcp/src/main.rs:L1000]

Runtime bounds are applied next. Requested limit is clamped to `max_results`, `min_should_match` is clamped to `[1, term_count]`, and default flags for tool-event inclusion and codex-mcp exclusion are applied from config. Optional `session_id` is validated against a strict safe-character regex before SQL generation, reducing injection and malformed-filter risks. [src: rust/codex-mcp/src/main.rs:L353, rust/codex-mcp/src/main.rs:L359, rust/codex-mcp/src/main.rs:L366, rust/codex-mcp/src/main.rs:L373, rust/codex-mcp/src/main.rs:L995]

The service fetches corpus totals and term DF values before building ranking SQL. Corpus stats are read from `search_corpus_stats`, with fallback aggregation from `search_documents` if stats are absent. DF values are read from `search_term_stats`, with fallback counts from `search_postings` for missing terms. These fallbacks make retrieval resilient during bootstrap and partial index repair. [src: rust/codex-mcp/src/main.rs:L578, rust/codex-mcp/src/main.rs:L582, rust/codex-mcp/src/main.rs:L588, rust/codex-mcp/src/main.rs:L597]

## BM25 Formula and SQL Realization

IDF is computed per query term in-process using an [Okapi BM25](https://en.wikipedia.org/wiki/Okapi_BM25)-style smoothing expression. For unseen terms (`df=0`), the service uses a high fallback IDF derived from corpus size; for seen terms it uses `ln(1 + ((N - df + 0.5)/(df + 0.5)))` with non-negative clamping. This keeps ranking numerically stable and prevents negative-term contributions from high-frequency terms. [src: rust/codex-mcp/src/main.rs:L398, rust/codex-mcp/src/main.rs:L401, rust/codex-mcp/src/main.rs:L405]

The SQL query embeds `k1`, `b`, `avgdl`, term array, and aligned IDF array in a `WITH` clause. For each posting row, term-specific IDF is selected with `transform(term, q_terms, q_idf, 0.0)`, and BM25 contribution is computed as `tf*(k1+1)/(tf + k1*(1-b+b*doc_len/avgdl))`. Contributions are summed per document; `matched_terms` and `score` filters are applied in `HAVING`, then results are ordered by score and limited. [src: rust/codex-mcp/src/main.rs:L532, rust/codex-mcp/src/main.rs:L551, rust/codex-mcp/src/main.rs:L554, rust/codex-mcp/src/main.rs:L564]

Because ranking is executed over postings constrained by query terms, dominant cost is posting fanout, not corpus cardinality. This is the key performance behavior that enables real-time local search at scale within one node, provided token normalization keeps term selectivity reasonable. [src: rust/codex-mcp/src/main.rs:L505, rust/codex-mcp/src/main.rs:L560]

## Retrieval Policy Filters

By default, retrieval excludes several operationally noisy payloads and prefers semantically meaningful event classes (`message`, `reasoning`, `event_msg`). When `include_tool_events` is false, additional payload-type exclusions remove lifecycle chatter such as `task_started` and `turn_aborted`. This default policy is tuned for agent consumption quality rather than maximal recall of low-signal events. [src: rust/codex-mcp/src/main.rs:L513, rust/codex-mcp/src/main.rs:L515, rust/codex-mcp/src/main.rs:L517]

A second policy filter optionally excludes codex-mcp self-reference. It removes rows whose payload mentions `codex-mcp` and rows with tool names `search` or `open`. This prevents retrieval loops where prior search/open traces dominate subsequent search results. The filter can be disabled when self-observation is intentionally desired. [src: rust/codex-mcp/src/main.rs:L523, rust/codex-mcp/src/main.rs:L525, config/moraine.toml:L41]

Session scoping is supported through exact `session_id` filtering in postings query conditions. In scoped mode, ranking is still BM25-based but corpus statistics remain global in current implementation. That means scores are comparable within scoped results for ranking order, but absolute values should not be interpreted as session-local calibrated relevance probabilities. [src: rust/codex-mcp/src/main.rs:L507, rust/codex-mcp/src/main.rs:L572]

## `open` Tool and Context Reconstruction

`open` resolves one event UID to a session and event order using `v_conversation_trace`, then fetches an ordered context window around that order. This is intentionally separate from lexical ranking and relies on the trace view’s deterministic ordering semantics. If UID is not found, the tool returns `found=false` instead of an error payload. [src: rust/codex-mcp/src/main.rs:L728, rust/codex-mcp/src/main.rs:L733, rust/codex-mcp/src/main.rs:L735]

Returned rows include both concise fields (actor, class, payload type, text) and full payload/token JSON for deep inspection. In prose mode, context is rendered in deterministic order and partitioned into before/target/after blocks to improve agent readability while preserving event order metadata. [src: rust/codex-mcp/src/main.rs:L745, rust/codex-mcp/src/main.rs:L755, rust/codex-mcp/src/main.rs:L905]

## Freshness and Rebuild Behavior

Steady-state freshness is push-driven: ingestor writes canonical rows, MVs update documents and postings, and MCP queries read latest committed state. No periodic full-corpus reindex is required for normal operation. This architecture is robust under continuous append workloads because index maintenance is amortized over ingest writes. [src: sql/004_search_index.sql:L28, sql/004_search_index.sql:L100, rust/codex-mcp/src/main.rs:L422]

For schema changes or index corruption repair, `bin/backfill-search-index` truncates search tables and rehydrates documents from canonical event tables. Postings and stats repopulate through MVs after inserts. Operators should run this explicitly after tokenization/projection changes to avoid mixed-semantics corpora. [src: bin/backfill-search-index:L74, bin/backfill-search-index:L79, bin/backfill-search-index:L81]

## Query and Interaction Logging

Each search writes a `search_query_log` row with normalized terms, filter settings, response latency, result count, and BM25 metadata (`docs`, `avgdl`, `k1`, `b`). Ranked results are written to `search_hit_log` with per-hit rank, score, and contextual metadata. These writes can be synchronous or async, controlled by config. [src: rust/codex-mcp/src/main.rs:L621, rust/codex-mcp/src/main.rs:L637, rust/codex-mcp/src/main.rs:L667, rust/codex-mcp/src/main.rs:L689]

`search_interaction_log` is reserved for external feedback capture and is not currently auto-populated by MCP. Keeping this table in baseline schema is strategic: it allows later relevance-learning loops to ingest click/selection/annotation events without schema migration pressure. [src: sql/004_search_index.sql:L220]

## Performance and Quality Tuning

High-impact knobs are `k1`, `b`, `min_should_match`, result limit, and inclusion filters. Raising `min_should_match` increases precision by requiring broader term overlap; lowering it increases recall for short queries or sparse terms. `k1` and `b` should be tuned against corpus characteristics, but defaults (`1.2`, `0.75`) are reasonable starting points for mixed conversational and tool text. [src: config/moraine.toml:L46-47, config/moraine.toml:L49]

If ranking quality looks noisy, first inspect corpus inputs, not formula constants. Common root causes are weak `text_content` extraction, inclusion of operational chatter, or stale/misaligned index tables after schema changes. Constants cannot recover information that never entered `search_documents` correctly. [src: sql/004_search_index.sql:L53, rust/ingestor/src/normalize.rs:L651, bin/backfill-search-index:L72]

If latency regresses, inspect posting fanout and query term shape. Extremely broad terms and long query token lists increase candidate set size. The max-query-terms cap provides a hard guardrail; if you raise it, do so with observed workload data and not by default. [src: rust/codex-mcp/src/main.rs:L346, rust/codex-mcp/src/main.rs:L1016, config/moraine.toml:L50]

## Known Limits

The current implementation uses simple regex tokenization with no stemming, lemmatization, phrase scoring, or field weighting. This keeps indexing fast and predictable for code-like and operational text, but leaves semantic recall on the table for morphology-heavy language domains. Advanced linguistic normalization can be layered later, but should only be introduced with explicit rebuild and relevance-evaluation plans. [src: sql/004_search_index.sql:L22, sql/004_search_index.sql:L129, rust/codex-mcp/src/main.rs:L989]

Score interpretation should remain relative, not absolute. BM25 scores are useful for rank ordering within one query, but cross-query score comparisons are weak without calibration. Downstream agents should prefer rank and contextual text over raw score thresholds unless query distributions are controlled.
