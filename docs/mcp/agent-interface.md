# Moraine MCP Interface

## Service Contract

`moraine-mcp` is a local, stateless [Model Context Protocol](https://modelcontextprotocol.io/) server that sits on top of ClickHouse-backed Moraine tables and exposes exactly two retrieval primitives to agent runtimes: lexical discovery (`search`) and trace reconstruction (`open`). It does not own ingestion, index construction, or background maintenance. Its contract boundary is intentionally narrow: accept JSON-RPC tool calls over stdio, execute bounded SQL reads against precomputed search structures and trace views, and return either agent-readable prose or full structured JSON. This keeps latency predictable, process startup cheap, and failure domains small enough that host runtimes can restart the process without any reindex step.

Configuration confirms this posture. ClickHouse endpoint, protocol version, result limits, context defaults, and BM25 parameters are loaded from TOML with concrete defaults, and startup fails fast on parse or ping failure. The service therefore has no hidden mutable state that can drift from corpus truth: either it can reach ClickHouse and answer from current tables, or it fails immediately and visibly. [src: crates/moraine-config/src/lib.rs:L66-L98, crates/moraine-config/src/lib.rs:L563-L566, crates/moraine-mcp-core/src/lib.rs:L671-L673, config/moraine.toml:L1-L50]

## JSON-RPC Session Lifecycle

The runtime is a long-lived async loop over newline-delimited JSON-RPC messages on stdin/stdout. Each non-empty line is parsed as `RpcRequest`; malformed lines are logged and skipped rather than terminating the server process. Supported base methods are `initialize`, `ping`, `tools/list`, and `tools/call`; unknown methods produce `-32601` responses when an `id` is present. Notifications such as `notifications/initialized` are accepted and ignored, which keeps compatibility with hosts that send startup chatter not requiring responses. [src: crates/moraine-mcp-core/src/lib.rs:L23-L29, crates/moraine-mcp-core/src/lib.rs:L215-L255, crates/moraine-mcp-core/src/lib.rs:L697-L710]

Initialization returns protocol and capability metadata sourced from runtime config and Cargo package version. Tool invocation is routed through typed argument decoding, and decode errors are surfaced as parameter errors (`-32602`) instead of ad hoc text. This separation between transport-level errors and tool-level errors is important for agent frameworks because they can decide whether to retry, revise arguments, or terminate based on standard JSON-RPC semantics. [src: crates/moraine-mcp-core/src/lib.rs:L216-L230, crates/moraine-mcp-core/src/lib.rs:L238-L250, crates/moraine-mcp-core/src/lib.rs:L332-L369]

Reference excerpt:

```rust
match req.method.as_str() {
    "initialize" => { /* protocol + capabilities */ }
    "ping" => id.map(|msg_id| rpc_ok(msg_id, json!({}))),
    "tools/list" => id.map(|msg_id| rpc_ok(msg_id, self.tools_list_result())),
    "tools/call" => { /* typed decode + call_tool */ }
    _ => id.map(|msg_id| rpc_err(msg_id, -32601, &format!("method not found: {}", req.method))),
}
```

[src: crates/moraine-mcp-core/src/lib.rs:L215-L255]

## Tool Definitions and Input Schemas

`tools/list` publishes two tools with explicit JSON Schema fragments: `search` and `open`. `search` requires `query`, supports optional bounds such as `limit`, `min_score`, `min_should_match`, filtering options (`session_id`, `include_tool_events`, `exclude_codex_mcp`), and `verbosity`. `open` requires `event_uid`, supports context window controls (`before`, `after`), and the same `verbosity` selector. The published schemas are the authoritative wire contract for hosts; any client-side wrappers should derive from this payload rather than duplicating assumptions in separate code paths. [src: crates/moraine-mcp-core/src/lib.rs:L258-L299]

The design intentionally keeps tool inventory minimal. Higher-level retrieval workflows are expected to be composed in the host runtime by calling `search`, selecting a candidate UID, and then calling `open` for surrounding evidence. This composition pattern is reflected directly in the prose formatter, which includes a suggested `open(event_uid=...)` continuation for each hit. [src: crates/moraine-mcp-core/src/lib.rs:L258-L299, crates/moraine-mcp-core/src/lib.rs:L507-L521]

Reference excerpt:

```rust
{
    "name": "search",
    "description": "BM25 lexical search over Moraine indexed conversation events.",
    "inputSchema": {
        "type": "object",
        "properties": {
            "query": { "type": "string" },
            "limit": { "type": "integer", "minimum": 1, "maximum": self.cfg.mcp.max_results },
            "verbosity": { "type": "string", "enum": ["prose", "full"], "default": "prose" }
        },
        "required": ["query"]
    }
}
```

[src: crates/moraine-mcp-core/src/lib.rs:L262-L281]

## Search Tool Execution Semantics

`search` begins by normalizing and validating request fields: query trimming, empty-query rejection, term tokenization with hard cap (`max_query_terms`), bounded `limit`, bounded `min_should_match`, optional session safety checks, and default filter controls from config. Query IDs are generated per call, and elapsed time is tracked from pre-tokenization to final payload assembly. These steps are not incidental bookkeeping; they are the service’s first-stage admission control and protect ClickHouse from pathological or malformed query shapes generated by upstream agents. [src: crates/moraine-conversations/src/clickhouse_repo.rs:L1131-L1168, crates/moraine-conversations/src/clickhouse_repo.rs:L1407-L1435, config/moraine.toml:L35-L50]

Ranking is BM25-like and implemented by combining in-process IDF preparation with SQL-side term scoring over `search_postings`. Corpus totals and term frequencies are sourced from stats tables when available and transparently fall back to base-table aggregation when stats are missing, so bootstrap and partial-repair states still return results. SQL generation constrains candidate rows via `p.term IN [...]`, optional session filters, event-class/payload filters, and optional codex-mcp self-exclusion, then computes score per document with configured `k1` and `b`. The result set is ordered by score and truncated by the requested limit. [src: crates/moraine-conversations/src/clickhouse_repo.rs:L381-L410, crates/moraine-conversations/src/clickhouse_repo.rs:L413-L509, crates/moraine-conversations/src/clickhouse_repo.rs:L1186-L1214]

Operationally, this means retrieval latency is tied to postings fanout and term selectivity rather than corpus-wide scans. The MCP process performs no full-table tokenization, no corpus rebuild, and no local index persistence; it simply compiles a bounded query against continuously maintained search tables. [src: crates/moraine-conversations/src/clickhouse_repo.rs:L413-L509, sql/004_search_index.sql:L82-L133, sql/004_search_index.sql:L147-L170]

Reference excerpt:

```rust
let query = args.query.trim();
if query.is_empty() {
    return Err(anyhow!("query cannot be empty"));
}
let terms_with_qf = tokenize_query(query, self.cfg.bm25.max_query_terms);
let min_should_match = args
    .min_should_match
    .unwrap_or(self.cfg.bm25.default_min_should_match)
    .max(1)
    .min(terms.len() as u16);
let query_sql = self.build_search_sql(/* terms, filters, bounds */)?;
let mut rows: Vec<SearchRow> = self.ch.query_json_rows(&query_sql).await?;
rows.sort_by(|a, b| b.score.total_cmp(&a.score));
```

[src: crates/moraine-conversations/src/clickhouse_repo.rs:L1132-L1158, crates/moraine-conversations/src/clickhouse_repo.rs:L1201-L1214]

## Open Tool Execution Semantics

`open` provides deterministic context reconstruction around one event UID. The implementation first validates `event_uid`, resolves target `(session_id, event_order, turn_seq)` from `v_conversation_trace`, then loads an ordered event window bounded by `before` and `after` offsets. If no target row exists, the response is a successful `found=false` payload rather than an exception, so hosts can branch on result state without treating misses as transport failures. [src: crates/moraine-conversations/src/clickhouse_repo.rs:L1031-L1053, crates/moraine-conversations/src/clickhouse_repo.rs:L1062-L1128]

Window rows include compact context fields and full payload/token JSON, preserving both quick readability and deep inspection paths. Ordering is normalized in memory before emission, and prose formatting then partitions rows into before/target/after blocks with stable event order, which is useful for agents that need immediate narrative context instead of raw arrays. [src: crates/moraine-conversations/src/clickhouse_repo.rs:L1065-L1116, crates/moraine-mcp-core/src/lib.rs:L526-L589]

The main architectural implication is that `open` depends on data-plane ordering contracts, not on search-rank ordering. Its fidelity therefore inherits from `v_conversation_trace` and source provenance in core tables, allowing a consistent answer to “what happened around this event” even when lexical ranking and trace chronology diverge. [src: sql/002_views.sql:L61-L80, sql/001_schema.sql:L27-L31]

Reference excerpt:

```rust
let target_query = format!(
    "SELECT session_id, event_order, turn_seq
     FROM moraine.v_conversation_trace
     WHERE event_uid = {}
     ORDER BY event_order DESC LIMIT 1 FORMAT JSONEachRow",
    sql_quote(event_uid)
);
let targets: Vec<OpenTargetRow> = self.ch.query_json_rows(&target_query).await?;
let Some(target) = targets.first() else {
    return Ok(json!({ "found": false, "event_uid": event_uid, "events": [] }));
};
```

[src: crates/moraine-conversations/src/clickhouse_repo.rs:L1042-L1050]

## Response Shapes and Verbosity

The tool envelope is explicitly dual-mode. In `full` mode, responses include both `content` text and `structuredContent` carrying the entire JSON payload. In default `prose` mode, responses return a concise text form intended for direct LLM consumption with minimal parsing burden. Error envelopes set `isError=true` and provide a single text payload. This makes downstream handling straightforward for both strict schema consumers and text-first agents. [src: crates/moraine-mcp-core/src/lib.rs:L452-L488]

Prose search output includes query metadata, hit count/latency, ranked hit summaries, snippet lines, and the next-step affordance to call `open`. Prose open output includes session and turn metadata, context-window settings, and ordered event blocks. Hosts that want deterministic machine transforms should request `full`; hosts optimizing for immediate model reasoning can stay on default `prose`. [src: crates/moraine-mcp-core/src/lib.rs:L490-L523, crates/moraine-mcp-core/src/lib.rs:L526-L589]

Reference excerpt:

```rust
fn tool_ok_full(payload: Value) -> Value {
    json!({
        "content": [{ "type": "text", "text": serde_json::to_string_pretty(&payload).unwrap_or_else(|_| "{}".to_string()) }],
        "structuredContent": payload,
        "isError": false
    })
}

fn tool_ok_prose(text: String) -> Value {
    json!({ "content": [{ "type": "text", "text": text }], "isError": false })
}
```

[src: crates/moraine-mcp-core/src/lib.rs:L452-L476]

## Safety and Failure Semantics

Input safety controls are strict but lightweight. Session and event filter values are validated with a safe-character regex before interpolation, SQL string literals are escaped, and unsupported characters produce deterministic request errors. Query tokenizer bounds prevent unbounded term lists, and limit clamps prevent oversized result sets even if hosts send extreme values. [src: crates/moraine-conversations/src/clickhouse_repo.rs:L214-L229, crates/moraine-conversations/src/clickhouse_repo.rs:L1146-L1164, crates/moraine-conversations/src/clickhouse_repo.rs:L1401-L1443]

Failure handling favors continuity. Parse failures of incoming request lines are logged and ignored; transport loop continues. Search/open execution errors are converted into tool error payloads instead of process termination. Telemetry writes to `search_query_log` and `search_hit_log` are best-effort in both sync and async modes: failures are warned but do not poison the user-facing response path. This behavior is deliberate because retrieval correctness should not hinge on observability table availability. [src: crates/moraine-mcp-core/src/lib.rs:L243-L247, crates/moraine-mcp-core/src/lib.rs:L705-L710, crates/moraine-conversations/src/clickhouse_repo.rs:L640-L734, sql/004_search_index.sql:L180-L219]

Client-side ClickHouse access is centralized in a small wrapper (`query_rows`, `insert_json_rows`, ping), with timeout and optional HTTP basic auth from config. This keeps surface area narrow and makes failure modes auditable to a small set of request paths. [src: crates/moraine-clickhouse/src/lib.rs:L43-L56, crates/moraine-clickhouse/src/lib.rs:L100-L129, crates/moraine-clickhouse/src/lib.rs:L181-L212]

Reference excerpt:

```rust
fn safe_value_re() -> &'static Regex {
    static SAFE_RE: OnceLock<Regex> = OnceLock::new();
    SAFE_RE.get_or_init(|| Regex::new(r"^[A-Za-z0-9._:@/-]{1,256}$").expect("valid safe-value regex"))
}

fn sql_quote(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}
```

[src: crates/moraine-conversations/src/clickhouse_repo.rs:L1401-L1405, crates/moraine-conversations/src/clickhouse_repo.rs:L1441-L1443]

## Integration Guidance for Agents

Integrate `moraine-mcp` as a colocated subprocess started by the host agent runtime. The recommended operational path is `bin/moraine run mcp --config <path>`, which keeps one command surface across local environments.

For host policy, default to `verbosity=prose` for first-pass retrieval, then issue targeted `verbosity=full` calls when the agent needs exact JSON fields (`payload_json`, `token_usage_json`, or source coordinates). Keep `exclude_codex_mcp=true` unless debugging MCP internals, and avoid raising `max_query_terms` without workload evidence because fanout cost rises rapidly on broad lexical tokens. [src: config/moraine.toml:L40-L50, crates/moraine-conversations/src/clickhouse_repo.rs:L1162-L1164, crates/moraine-conversations/src/clickhouse_repo.rs:L1407-L1424]

For deterministic behavior across environments, pin config in `config/moraine.toml` and keep MCP policy in the `[mcp]` section. This avoids drift between service-specific config files.

Reference excerpt:

```rust
#[derive(Debug, Args)]
struct RunArgs {
    #[arg(value_enum)]
    service: Service,
    #[arg(
        trailing_var_arg = true,
        allow_hyphen_values = true,
        num_args = 0..
    )]
    args: Vec<String>,
}
```

[src: apps/moraine/src/main.rs:L164-L173]

In practice, the interface should be treated as a strict retrieval edge service: keep it stateless, keep calls bounded, and let ClickHouse remain the durable truth for both content and retrieval telemetry.
