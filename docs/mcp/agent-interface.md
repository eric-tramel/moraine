# Cortex MCP Interface

## Service Contract

`cortex-mcp` is a local, stateless [Model Context Protocol](https://modelcontextprotocol.io/) server that sits on top of ClickHouse-backed Cortex tables and exposes exactly two retrieval primitives to agent runtimes: lexical discovery (`search`) and trace reconstruction (`open`). It does not own ingestion, index construction, or background maintenance. Its contract boundary is intentionally narrow: accept JSON-RPC tool calls over stdio, execute bounded SQL reads against precomputed search structures and trace views, and return either agent-readable prose or full structured JSON. This keeps latency predictable, process startup cheap, and failure domains small enough that host runtimes can restart the process without any reindex step.

Configuration confirms this posture. ClickHouse endpoint, protocol version, result limits, context defaults, and BM25 parameters are loaded from TOML with concrete defaults, and startup fails fast on parse or ping failure. The service therefore has no hidden mutable state that can drift from corpus truth: either it can reach ClickHouse and answer from current tables, or it fails immediately and visibly. [src: rust/codex-mcp/src/config.rs:L5-L65, rust/codex-mcp/src/config.rs:L194-L212, rust/codex-mcp/src/main.rs:L1075-L1081, config/codex-mcp.toml:L1-L25]

## JSON-RPC Session Lifecycle

The runtime is a long-lived async loop over newline-delimited JSON-RPC messages on stdin/stdout. Each non-empty line is parsed as `RpcRequest`; malformed lines are logged and skipped rather than terminating the server process. Supported base methods are `initialize`, `ping`, `tools/list`, and `tools/call`; unknown methods produce `-32601` responses when an `id` is present. Notifications such as `notifications/initialized` are accepted and ignored, which keeps compatibility with hosts that send startup chatter not requiring responses. [src: rust/codex-mcp/src/main.rs:L33-L39, rust/codex-mcp/src/main.rs:L216-L260, rust/codex-mcp/src/main.rs:L1084-L1110]

Initialization returns protocol and capability metadata sourced from runtime config and Cargo package version. Tool invocation is routed through typed argument decoding, and decode errors are surfaced as parameter errors (`-32602`) instead of ad hoc text. This separation between transport-level errors and tool-level errors is important for agent frameworks because they can decide whether to retry, revise arguments, or terminate based on standard JSON-RPC semantics. [src: rust/codex-mcp/src/main.rs:L220-L256, rust/codex-mcp/src/main.rs:L793-L810]

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

[src: rust/codex-mcp/src/main.rs:L219-L260]

## Tool Definitions and Input Schemas

`tools/list` publishes two tools with explicit JSON Schema fragments: `search` and `open`. `search` requires `query`, supports optional bounds such as `limit`, `min_score`, `min_should_match`, filtering options (`session_id`, `include_tool_events`, `exclude_codex_mcp`), and `verbosity`. `open` requires `event_uid`, supports context window controls (`before`, `after`), and the same `verbosity` selector. The published schemas are the authoritative wire contract for hosts; any client-side wrappers should derive from this payload rather than duplicating assumptions in separate code paths. [src: rust/codex-mcp/src/main.rs:L264-L307]

The design intentionally keeps tool inventory minimal. Higher-level retrieval workflows are expected to be composed in the host runtime by calling `search`, selecting a candidate UID, and then calling `open` for surrounding evidence. This composition pattern is reflected directly in the prose formatter, which includes a suggested `open(event_uid=...)` continuation for each hit. [src: rust/codex-mcp/src/main.rs:L867-L881]

Reference excerpt:

```rust
{
    "name": "search",
    "description": "BM25 lexical search over Cortex indexed conversation events.",
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

[src: rust/codex-mcp/src/main.rs:L267-L287]

## Search Tool Execution Semantics

`search` begins by normalizing and validating request fields: query trimming, empty-query rejection, term tokenization with hard cap (`max_query_terms`), bounded `limit`, bounded `min_should_match`, optional session safety checks, and default filter controls from config. Query IDs are generated per call, and elapsed time is tracked from pre-tokenization to final payload assembly. These steps are not incidental bookkeeping; they are the service’s first-stage admission control and protect ClickHouse from pathological or malformed query shapes generated by upstream agents. [src: rust/codex-mcp/src/main.rs:L337-L377, rust/codex-mcp/src/main.rs:L1000-L1028, config/codex-mcp.toml:L11-L25]

Ranking is BM25-like and implemented by combining in-process IDF preparation with SQL-side term scoring over `search_postings`. Corpus totals and term frequencies are sourced from stats tables when available and transparently fall back to base-table aggregation when stats are missing, so bootstrap and partial-repair states still return results. SQL generation constrains candidate rows via `p.term IN [...]`, optional session filters, event-class/payload filters, and optional codex-mcp self-exclusion, then computes score per document with configured `k1` and `b`. The result set is ordered by score and truncated by the requested limit. [src: rust/codex-mcp/src/main.rs:L379-L420, rust/codex-mcp/src/main.rs:L482-L570, rust/codex-mcp/src/main.rs:L572-L619, rust/codex-mcp/src/main.rs:L1034-L1049]

Operationally, this means retrieval latency is tied to postings fanout and term selectivity rather than corpus-wide scans. The MCP process performs no full-table tokenization, no corpus rebuild, and no local index persistence; it simply compiles a bounded query against continuously maintained search tables. [src: rust/codex-mcp/src/main.rs:L505-L566, sql/004_search_index.sql:L82-L133, sql/004_search_index.sql:L147-L170]

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

[src: rust/codex-mcp/src/main.rs:L337-L349, rust/codex-mcp/src/main.rs:L359-L364, rust/codex-mcp/src/main.rs:L410-L424]

## Open Tool Execution Semantics

`open` provides deterministic context reconstruction around one event UID. The implementation first validates `event_uid`, resolves target `(session_id, event_order, turn_seq)` from `v_conversation_trace`, then loads an ordered event window bounded by `before` and `after` offsets. If no target row exists, the response is a successful `found=false` payload rather than an exception, so hosts can branch on result state without treating misses as transport failures. [src: rust/codex-mcp/src/main.rs:L716-L740, rust/codex-mcp/src/main.rs:L745-L753]

Window rows include compact context fields and full payload/token JSON, preserving both quick readability and deep inspection paths. Ordering is normalized in memory before emission, and prose formatting then partitions rows into before/target/after blocks with stable event order, which is useful for agents that need immediate narrative context instead of raw arrays. [src: rust/codex-mcp/src/main.rs:L752-L789, rust/codex-mcp/src/main.rs:L886-L949]

The main architectural implication is that `open` depends on data-plane ordering contracts, not on search-rank ordering. Its fidelity therefore inherits from `v_conversation_trace` and source provenance in core tables, allowing a consistent answer to “what happened around this event” even when lexical ranking and trace chronology diverge. [src: sql/002_views.sql:L61-L80, sql/001_schema.sql:L27-L31]

Reference excerpt:

```rust
let target_query = format!(
    "SELECT session_id, event_order, turn_seq
     FROM cortex.v_conversation_trace
     WHERE event_uid = {}
     ORDER BY event_order DESC LIMIT 1 FORMAT JSONEachRow",
    sql_quote(event_uid)
);
let targets: Vec<OpenTargetRow> = self.ch.query_json_rows(&target_query).await?;
let Some(target) = targets.first() else {
    return Ok(json!({ "found": false, "event_uid": event_uid, "events": [] }));
};
```

[src: rust/codex-mcp/src/main.rs:L728-L740]

## Response Shapes and Verbosity

The tool envelope is explicitly dual-mode. In `full` mode, responses include both `content` text and `structuredContent` carrying the entire JSON payload. In default `prose` mode, responses return a concise text form intended for direct LLM consumption with minimal parsing burden. Error envelopes set `isError=true` and provide a single text payload. This makes downstream handling straightforward for both strict schema consumers and text-first agents. [src: rust/codex-mcp/src/main.rs:L812-L848]

Prose search output includes query metadata, hit count/latency, ranked hit summaries, snippet lines, and the next-step affordance to call `open`. Prose open output includes session and turn metadata, context-window settings, and ordered event blocks. Hosts that want deterministic machine transforms should request `full`; hosts optimizing for immediate model reasoning can stay on default `prose`. [src: rust/codex-mcp/src/main.rs:L850-L884, rust/codex-mcp/src/main.rs:L886-L962]

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

[src: rust/codex-mcp/src/main.rs:L812-L836]

## Safety and Failure Semantics

Input safety controls are strict but lightweight. Session and event filter values are validated with a safe-character regex before interpolation, SQL string literals are escaped, and unsupported characters produce deterministic request errors. Query tokenizer bounds prevent unbounded term lists, and limit clamps prevent oversized result sets even if hosts send extreme values. [src: rust/codex-mcp/src/main.rs:L373-L376, rust/codex-mcp/src/main.rs:L721-L723, rust/codex-mcp/src/main.rs:L994-L998, rust/codex-mcp/src/main.rs:L353-L364, rust/codex-mcp/src/main.rs:L1034-L1041]

Failure handling favors continuity. Parse failures of incoming request lines are logged and ignored; transport loop continues. Search/open execution errors are converted into tool error payloads instead of process termination. Telemetry writes to `search_query_log` and `search_hit_log` are best-effort in both sync and async modes: failures are warned but do not poison the user-facing response path. This behavior is deliberate because retrieval correctness should not hinge on observability table availability. [src: rust/codex-mcp/src/main.rs:L1096-L1103, rust/codex-mcp/src/main.rs:L247-L253, rust/codex-mcp/src/main.rs:L621-L714, sql/004_search_index.sql:L180-L219]

Client-side ClickHouse access is centralized in a small wrapper (`query_json_rows`, `insert_json_rows`, ping), with timeout and optional HTTP basic auth from config. This keeps surface area narrow and makes failure modes auditable to a small set of request paths. [src: rust/codex-mcp/src/clickhouse.rs:L14-L27, rust/codex-mcp/src/clickhouse.rs:L29-L70, rust/codex-mcp/src/clickhouse.rs:L81-L115]

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

[src: rust/codex-mcp/src/main.rs:L994-L998, rust/codex-mcp/src/main.rs:L1034-L1036]

## Integration Guidance for Agents

Integrate `cortex-mcp` as a colocated subprocess started by the host agent runtime. The recommended operational path is `bin/cortexctl run mcp --config <path>`, which keeps one command surface across local environments.

For host policy, default to `verbosity=prose` for first-pass retrieval, then issue targeted `verbosity=full` calls when the agent needs exact JSON fields (`payload_json`, `token_usage_json`, or source coordinates). Keep `exclude_codex_mcp=true` unless debugging MCP internals, and avoid raising `max_query_terms` without workload evidence because fanout cost rises rapidly on broad lexical tokens. [src: config/codex-mcp.toml:L15-L25, rust/codex-mcp/src/main.rs:L523-L526, rust/codex-mcp/src/main.rs:L1000-L1018]

For deterministic behavior across environments, pin config in `config/cortex.toml` and keep MCP policy in the `[mcp]` section. This avoids drift between service-specific config files.

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

[src: apps/cortexctl/src/main.rs:L164-L173]

In practice, the interface should be treated as a strict retrieval edge service: keep it stateless, keep calls bounded, and let ClickHouse remain the durable truth for both content and retrieval telemetry.
