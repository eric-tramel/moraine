use anyhow::{anyhow, Context, Result};
use cortex_clickhouse::ClickHouseClient;
use cortex_config::AppConfig;
use regex::Regex;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::{debug, warn};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Verbosity {
    Prose,
    Full,
}

impl Default for Verbosity {
    fn default() -> Self {
        Self::Prose
    }
}

#[derive(Debug, Deserialize)]
struct RpcRequest {
    #[serde(default)]
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Deserialize)]
struct ToolCallParams {
    name: String,
    #[serde(default)]
    arguments: Value,
}

#[derive(Debug, Deserialize)]
struct SearchArgs {
    query: String,
    #[serde(default)]
    limit: Option<u16>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    min_score: Option<f64>,
    #[serde(default)]
    min_should_match: Option<u16>,
    #[serde(default)]
    include_tool_events: Option<bool>,
    #[serde(default)]
    exclude_codex_mcp: Option<bool>,
    #[serde(default)]
    verbosity: Option<Verbosity>,
}

#[derive(Debug, Deserialize)]
struct OpenArgs {
    event_uid: String,
    #[serde(default)]
    before: Option<u16>,
    #[serde(default)]
    after: Option<u16>,
    #[serde(default)]
    verbosity: Option<Verbosity>,
}

#[derive(Debug, Deserialize)]
struct CorpusStatsRow {
    docs: u64,
    total_doc_len: u64,
}

#[derive(Debug, Deserialize)]
struct DfRow {
    term: String,
    df: u64,
}

#[derive(Debug, Deserialize)]
struct SearchRow {
    event_uid: String,
    session_id: String,
    source_name: String,
    provider: String,
    event_class: String,
    payload_type: String,
    actor_role: String,
    name: String,
    phase: String,
    source_ref: String,
    doc_len: u32,
    text_preview: String,
    score: f64,
    matched_terms: u64,
}

#[derive(Debug, Deserialize)]
struct OpenTargetRow {
    session_id: String,
    event_order: u64,
    turn_seq: u32,
}

#[derive(Debug, Deserialize)]
struct OpenContextRow {
    session_id: String,
    event_uid: String,
    event_order: u64,
    turn_seq: u32,
    event_time: String,
    actor_role: String,
    event_class: String,
    payload_type: String,
    call_id: String,
    name: String,
    phase: String,
    item_id: String,
    text_content: String,
    payload_json: String,
    token_usage_json: String,
    source_ref: String,
}

#[derive(Debug, Default, Deserialize)]
struct SearchProsePayload {
    #[serde(default)]
    query_id: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    stats: SearchProseStats,
    #[serde(default)]
    hits: Vec<SearchProseHit>,
}

#[derive(Debug, Default, Deserialize)]
struct SearchProseStats {
    #[serde(default)]
    took_ms: u64,
    #[serde(default)]
    result_count: u64,
}

#[derive(Debug, Default, Deserialize)]
struct SearchProseHit {
    #[serde(default)]
    rank: u64,
    #[serde(default)]
    event_uid: String,
    #[serde(default)]
    session_id: String,
    #[serde(default)]
    score: f64,
    #[serde(default)]
    event_class: String,
    #[serde(default)]
    payload_type: String,
    #[serde(default)]
    actor_role: String,
    #[serde(default)]
    text_preview: String,
}

#[derive(Debug, Default, Deserialize)]
struct OpenProsePayload {
    #[serde(default)]
    found: bool,
    #[serde(default)]
    event_uid: String,
    #[serde(default)]
    session_id: String,
    #[serde(default)]
    turn_seq: u32,
    #[serde(default)]
    target_event_order: u64,
    #[serde(default)]
    before: u16,
    #[serde(default)]
    after: u16,
    #[serde(default)]
    events: Vec<OpenProseEvent>,
}

#[derive(Debug, Default, Deserialize)]
struct OpenProseEvent {
    #[serde(default)]
    is_target: bool,
    #[serde(default)]
    event_order: u64,
    #[serde(default)]
    actor_role: String,
    #[serde(default)]
    event_class: String,
    #[serde(default)]
    payload_type: String,
    #[serde(default)]
    text_content: String,
}

#[derive(Clone)]
struct AppState {
    cfg: AppConfig,
    ch: ClickHouseClient,
}

impl AppState {
    async fn handle_request(&self, req: RpcRequest) -> Option<Value> {
        let id = req.id.clone();

        match req.method.as_str() {
            "initialize" => {
                let result = json!({
                    "protocolVersion": self.cfg.mcp.protocol_version,
                    "capabilities": {
                        "tools": {
                            "listChanged": false
                        }
                    },
                    "serverInfo": {
                        "name": "codex-mcp",
                        "version": env!("CARGO_PKG_VERSION")
                    }
                });

                id.map(|msg_id| rpc_ok(msg_id, result))
            }
            "ping" => id.map(|msg_id| rpc_ok(msg_id, json!({}))),
            "notifications/initialized" | "initialized" => None,
            "tools/list" => id.map(|msg_id| rpc_ok(msg_id, self.tools_list_result())),
            "tools/call" => {
                let Some(msg_id) = id else {
                    return None;
                };

                let parsed: Result<ToolCallParams> =
                    serde_json::from_value(req.params).context("invalid tools/call params payload");

                match parsed {
                    Ok(params) => {
                        let tool_result = match self.call_tool(params).await {
                            Ok(v) => v,
                            Err(err) => tool_error_result(err.to_string()),
                        };
                        Some(rpc_ok(msg_id, tool_result))
                    }
                    Err(err) => Some(rpc_err(msg_id, -32602, &format!("invalid params: {err}"))),
                }
            }
            _ => id.map(|msg_id| {
                rpc_err(msg_id, -32601, &format!("method not found: {}", req.method))
            }),
        }
    }

    fn tools_list_result(&self) -> Value {
        json!({
            "tools": [
                {
                    "name": "search",
                    "description": "BM25 lexical search over Cortex indexed conversation events.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "query": { "type": "string" },
                            "limit": { "type": "integer", "minimum": 1, "maximum": self.cfg.mcp.max_results },
                            "session_id": { "type": "string" },
                            "min_score": { "type": "number" },
                            "min_should_match": { "type": "integer", "minimum": 1 },
                            "include_tool_events": { "type": "boolean" },
                            "exclude_codex_mcp": { "type": "boolean" },
                            "verbosity": {
                                "type": "string",
                                "enum": ["prose", "full"],
                                "default": "prose"
                            }
                        },
                        "required": ["query"]
                    }
                },
                {
                    "name": "open",
                    "description": "Open one event by uid with surrounding conversation context.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "event_uid": { "type": "string" },
                            "before": { "type": "integer", "minimum": 0 },
                            "after": { "type": "integer", "minimum": 0 },
                            "verbosity": {
                                "type": "string",
                                "enum": ["prose", "full"],
                                "default": "prose"
                            }
                        },
                        "required": ["event_uid"]
                    }
                }
            ]
        })
    }

    async fn call_tool(&self, params: ToolCallParams) -> Result<Value> {
        match params.name.as_str() {
            "search" => {
                let args: SearchArgs = serde_json::from_value(params.arguments)
                    .context("search expects a JSON object with at least {\"query\": ...}")?;
                let verbosity = args.verbosity.unwrap_or_default();
                let payload = self.search(args).await?;
                match verbosity {
                    Verbosity::Full => Ok(tool_ok_full(payload)),
                    Verbosity::Prose => Ok(tool_ok_prose(format_search_prose(&payload)?)),
                }
            }
            "open" => {
                let args: OpenArgs = serde_json::from_value(params.arguments)
                    .context("open expects {\"event_uid\": ...}")?;
                let verbosity = args.verbosity.unwrap_or_default();
                let payload = self.open(args).await?;
                match verbosity {
                    Verbosity::Full => Ok(tool_ok_full(payload)),
                    Verbosity::Prose => Ok(tool_ok_prose(format_open_prose(&payload)?)),
                }
            }
            other => Err(anyhow!("unknown tool: {other}")),
        }
    }

    async fn search(&self, args: SearchArgs) -> Result<Value> {
        let query = args.query.trim();
        if query.is_empty() {
            return Err(anyhow!("query cannot be empty"));
        }

        let query_id = Uuid::new_v4().to_string();
        let started = Instant::now();

        let terms_with_qf = tokenize_query(query, self.cfg.bm25.max_query_terms);
        if terms_with_qf.is_empty() {
            return Err(anyhow!("query has no searchable terms"));
        }

        let terms: Vec<String> = terms_with_qf.iter().map(|(term, _)| term.clone()).collect();

        let limit = args
            .limit
            .unwrap_or(self.cfg.mcp.max_results)
            .max(1)
            .min(self.cfg.mcp.max_results);

        let min_should_match = args
            .min_should_match
            .unwrap_or(self.cfg.bm25.default_min_should_match)
            .max(1)
            .min(terms.len() as u16);

        let min_score = args.min_score.unwrap_or(self.cfg.bm25.default_min_score);
        let include_tool_events = args
            .include_tool_events
            .unwrap_or(self.cfg.mcp.default_include_tool_events);
        let exclude_codex_mcp = args
            .exclude_codex_mcp
            .unwrap_or(self.cfg.mcp.default_exclude_codex_mcp);

        if let Some(session_id) = args.session_id.as_deref() {
            if !is_safe_filter_value(session_id) {
                return Err(anyhow!("session_id contains unsupported characters"));
            }
        }

        let (docs, total_doc_len) = self.corpus_stats().await?;
        if docs == 0 {
            return Ok(json!({
                "query_id": query_id,
                "query": query,
                "terms": terms,
                "stats": {
                    "docs": 0,
                    "avgdl": 0.0,
                    "took_ms": started.elapsed().as_millis(),
                    "result_count": 0
                },
                "hits": []
            }));
        }

        let avgdl = (total_doc_len as f64 / docs as f64).max(1.0);
        let df_map = self.df_map(&terms, docs).await?;

        let mut idf_by_term = HashMap::<String, f64>::new();
        for term in &terms {
            let df = *df_map.get(term).unwrap_or(&0);
            let idf = if df == 0 {
                (1.0 + ((docs as f64 + 0.5) / 0.5)).ln()
            } else {
                let n = docs.max(df) as f64;
                (1.0 + ((n - df as f64 + 0.5) / (df as f64 + 0.5))).ln()
            };
            idf_by_term.insert(term.clone(), idf.max(0.0));
        }

        let query_sql = self.build_search_sql(
            &terms,
            &idf_by_term,
            avgdl,
            include_tool_events,
            exclude_codex_mcp,
            args.session_id.as_deref(),
            min_should_match,
            min_score,
            limit,
        )?;

        let mut rows: Vec<SearchRow> = self.ch.query_rows(&query_sql, None).await?;
        rows.sort_by(|a, b| b.score.total_cmp(&a.score));

        let took_ms = started.elapsed().as_millis() as u32;

        let hits: Vec<Value> = rows
            .iter()
            .enumerate()
            .map(|(idx, row)| {
                json!({
                    "rank": idx + 1,
                    "event_uid": row.event_uid,
                    "session_id": row.session_id,
                    "source_name": row.source_name,
                    "provider": row.provider,
                    "score": row.score,
                    "matched_terms": row.matched_terms,
                    "doc_len": row.doc_len,
                    "event_class": row.event_class,
                    "payload_type": row.payload_type,
                    "actor_role": row.actor_role,
                    "name": row.name,
                    "phase": row.phase,
                    "source_ref": row.source_ref,
                    "text_preview": row.text_preview
                })
            })
            .collect();

        let payload = json!({
            "query_id": query_id,
            "query": query,
            "terms": terms,
            "stats": {
                "docs": docs,
                "avgdl": avgdl,
                "took_ms": took_ms,
                "result_count": hits.len()
            },
            "hits": hits
        });

        self.log_search(
            &query_id,
            query,
            args.session_id.as_deref().unwrap_or(""),
            &terms,
            limit,
            min_should_match,
            min_score,
            include_tool_events,
            exclude_codex_mcp,
            took_ms,
            &rows,
            docs,
            avgdl,
        )
        .await;

        Ok(payload)
    }

    fn build_search_sql(
        &self,
        terms: &[String],
        idf_by_term: &HashMap<String, f64>,
        avgdl: f64,
        include_tool_events: bool,
        exclude_codex_mcp: bool,
        session_id: Option<&str>,
        min_should_match: u16,
        min_score: f64,
        limit: u16,
    ) -> Result<String> {
        if terms.is_empty() {
            return Err(anyhow!("cannot build search query with empty terms"));
        }

        let postings_table = self.table_ref("search_postings");
        let documents_table = self.table_ref("search_documents");
        let terms_array_sql = sql_array_strings(terms);
        let idf_vals: Vec<f64> = terms
            .iter()
            .map(|t| *idf_by_term.get(t).unwrap_or(&0.0))
            .collect();
        let idf_array_sql = sql_array_f64(&idf_vals);

        let mut where_clauses = vec![format!("p.term IN {}", terms_array_sql)];

        if let Some(sid) = session_id {
            where_clauses.push(format!("p.session_id = {}", sql_quote(sid)));
        }

        if include_tool_events {
            where_clauses.push("p.payload_type != 'token_count'".to_string());
        } else {
            where_clauses
                .push("p.event_class IN ('message', 'reasoning', 'event_msg')".to_string());
            where_clauses.push(
                "p.payload_type NOT IN ('token_count', 'task_started', 'task_complete', 'turn_aborted', 'item_completed')"
                    .to_string(),
            );
        }

        if exclude_codex_mcp {
            where_clauses
                .push("positionCaseInsensitiveUTF8(d.payload_json, 'codex-mcp') = 0".to_string());
            where_clauses.push("lowerUTF8(d.name) NOT IN ('search', 'open')".to_string());
        }

        let where_sql = where_clauses.join("\n  AND ");
        let k1 = self.cfg.bm25.k1.max(0.01);
        let b = self.cfg.bm25.b.clamp(0.0, 1.0);

        Ok(format!(
            "WITH
  {k1:.6} AS k1,
  {b:.6} AS b,
  greatest({avgdl:.6}, 1.0) AS avgdl,
  {terms_array_sql} AS q_terms,
  {idf_array_sql} AS q_idf
SELECT
  p.doc_id AS event_uid,
  any(p.session_id) AS session_id,
  any(p.source_name) AS source_name,
  any(p.provider) AS provider,
  any(p.event_class) AS event_class,
  any(p.payload_type) AS payload_type,
  any(p.actor_role) AS actor_role,
  any(p.name) AS name,
  any(p.phase) AS phase,
  any(p.source_ref) AS source_ref,
  any(p.doc_len) AS doc_len,
  leftUTF8(any(d.text_content), {preview}) AS text_preview,
  sum(
    transform(p.term, q_terms, q_idf, 0.0)
    *
    (
      (toFloat64(p.tf) * (k1 + 1.0))
      /
      (toFloat64(p.tf) + k1 * (1.0 - b + b * (toFloat64(p.doc_len) / avgdl)))
    )
  ) AS score,
  uniqExact(p.term) AS matched_terms
FROM {postings_table} AS p
ANY INNER JOIN {documents_table} AS d ON d.event_uid = p.doc_id
WHERE {where_sql}
GROUP BY p.doc_id
HAVING matched_terms >= {min_should_match} AND score >= {min_score:.6}
ORDER BY score DESC
LIMIT {limit}
FORMAT JSONEachRow",
            preview = self.cfg.mcp.preview_chars,
            postings_table = postings_table,
            documents_table = documents_table,
        ))
    }

    async fn corpus_stats(&self) -> Result<(u64, u64)> {
        let from_stats_query = format!(
            "SELECT toUInt64(ifNull(sum(docs), 0)) AS docs, toUInt64(ifNull(sum(total_doc_len), 0)) AS total_doc_len FROM {} FORMAT JSONEachRow",
            self.table_ref("search_corpus_stats")
        );
        let from_stats: Vec<CorpusStatsRow> = self.ch.query_rows(&from_stats_query, None).await?;

        if let Some(row) = from_stats.first() {
            if row.docs > 0 {
                return Ok((row.docs, row.total_doc_len));
            }
        }

        let fallback_query = format!(
            "SELECT toUInt64(count()) AS docs, toUInt64(ifNull(sum(doc_len), 0)) AS total_doc_len FROM {} FINAL WHERE doc_len > 0 FORMAT JSONEachRow",
            self.table_ref("search_documents")
        );
        let fallback: Vec<CorpusStatsRow> = self.ch.query_rows(&fallback_query, None).await?;
        if let Some(row) = fallback.first() {
            Ok((row.docs, row.total_doc_len))
        } else {
            Ok((0, 0))
        }
    }

    async fn df_map(&self, terms: &[String], docs: u64) -> Result<HashMap<String, u64>> {
        let terms_array = sql_array_strings(terms);
        let term_stats_table = self.table_ref("search_term_stats");
        let postings_table = self.table_ref("search_postings");
        let primary_query = format!(
            "SELECT term, toUInt64(sum(docs)) AS df FROM {term_stats_table} WHERE term IN {terms_array} GROUP BY term FORMAT JSONEachRow",
        );

        let mut map = HashMap::<String, u64>::new();

        let primary_rows: Vec<DfRow> = self.ch.query_rows(&primary_query, None).await?;
        for row in primary_rows {
            map.insert(row.term, row.df);
        }

        if !df_map_requires_recompute(terms, &map, docs) {
            return Ok(map);
        }

        let fallback_query = format!(
            "SELECT term, count() AS df FROM {postings_table} FINAL WHERE term IN {terms_array} GROUP BY term FORMAT JSONEachRow",
        );
        let fallback_rows: Vec<DfRow> = self.ch.query_rows(&fallback_query, None).await?;
        map.clear();
        for row in fallback_rows {
            map.insert(row.term, row.df);
        }

        Ok(map)
    }

    async fn log_search(
        &self,
        query_id: &str,
        raw_query: &str,
        session_hint: &str,
        terms: &[String],
        limit: u16,
        min_should_match: u16,
        min_score: f64,
        include_tool_events: bool,
        exclude_codex_mcp: bool,
        took_ms: u32,
        rows: &[SearchRow],
        docs: u64,
        avgdl: f64,
    ) {
        let metadata_json = match serde_json::to_string(&json!({
            "docs": docs,
            "avgdl": avgdl,
            "k1": self.cfg.bm25.k1,
            "b": self.cfg.bm25.b
        })) {
            Ok(v) => v,
            Err(err) => {
                warn!("failed to encode search metadata: {}", err);
                "{}".to_string()
            }
        };

        let query_row = json!({
            "query_id": query_id,
            "source": "codex-mcp",
            "session_hint": session_hint,
            "raw_query": raw_query,
            "normalized_terms": terms,
            "term_count": terms.len() as u16,
            "result_limit": limit,
            "min_should_match": min_should_match,
            "min_score": min_score,
            "include_tool_events": if include_tool_events { 1 } else { 0 },
            "exclude_codex_mcp": if exclude_codex_mcp { 1 } else { 0 },
            "response_ms": took_ms,
            "result_count": rows.len() as u16,
            "metadata_json": metadata_json,
        });

        let hit_rows: Vec<Value> = rows
            .iter()
            .enumerate()
            .map(|(idx, row)| {
                json!({
                    "query_id": query_id,
                    "rank": (idx + 1) as u16,
                    "event_uid": row.event_uid,
                    "session_id": row.session_id,
                    "source_name": row.source_name,
                    "provider": row.provider,
                    "score": row.score,
                    "matched_terms": row.matched_terms as u16,
                    "doc_len": row.doc_len,
                    "event_class": row.event_class,
                    "payload_type": row.payload_type,
                    "actor_role": row.actor_role,
                    "name": row.name,
                    "source_ref": row.source_ref,
                })
            })
            .collect();

        let ch = self.ch.clone();
        if self.cfg.mcp.async_log_writes {
            tokio::spawn(async move {
                if let Err(err) = ch.insert_json_rows("search_query_log", &[query_row]).await {
                    warn!("failed to write search_query_log: {}", err);
                }
                if !hit_rows.is_empty() {
                    if let Err(err) = ch.insert_json_rows("search_hit_log", &hit_rows).await {
                        warn!("failed to write search_hit_log: {}", err);
                    }
                }
            });
        } else {
            if let Err(err) = self
                .ch
                .insert_json_rows("search_query_log", &[query_row])
                .await
            {
                warn!("failed to write search_query_log: {}", err);
            }
            if !hit_rows.is_empty() {
                if let Err(err) = self.ch.insert_json_rows("search_hit_log", &hit_rows).await {
                    warn!("failed to write search_hit_log: {}", err);
                }
            }
        }
    }

    async fn open(&self, args: OpenArgs) -> Result<Value> {
        let event_uid = args.event_uid.trim();
        if event_uid.is_empty() {
            return Err(anyhow!("event_uid cannot be empty"));
        }
        if !is_safe_filter_value(event_uid) {
            return Err(anyhow!("event_uid contains unsupported characters"));
        }

        let before = args.before.unwrap_or(self.cfg.mcp.default_context_before);
        let after = args.after.unwrap_or(self.cfg.mcp.default_context_after);
        let trace_table = self.table_ref("v_conversation_trace");

        let target_query = format!(
            "SELECT session_id, event_order, turn_seq FROM {trace_table} WHERE event_uid = {} ORDER BY event_order DESC LIMIT 1 FORMAT JSONEachRow",
            sql_quote(event_uid)
        );

        let targets: Vec<OpenTargetRow> = self.ch.query_rows(&target_query, None).await?;
        let Some(target) = targets.first() else {
            return Ok(json!({
                "found": false,
                "event_uid": event_uid,
                "events": []
            }));
        };

        let lower = target.event_order.saturating_sub(before as u64).max(1);
        let upper = target.event_order + after as u64;

        let context_query = format!(
            "SELECT session_id, event_uid, event_order, turn_seq, toString(event_time) AS event_time, actor_role, event_class, payload_type, call_id, name, phase, item_id, text_content, payload_json, token_usage_json, source_ref FROM {trace_table} WHERE session_id = {} AND event_order BETWEEN {} AND {} ORDER BY event_order FORMAT JSONEachRow",
            sql_quote(&target.session_id),
            lower,
            upper
        );

        let mut rows: Vec<OpenContextRow> = self.ch.query_rows(&context_query, None).await?;
        rows.sort_by_key(|row| row.event_order);

        let events: Vec<Value> = rows
            .iter()
            .map(|row| {
                json!({
                    "is_target": row.event_uid == event_uid,
                    "session_id": row.session_id,
                    "event_uid": row.event_uid,
                    "event_order": row.event_order,
                    "turn_seq": row.turn_seq,
                    "event_time": row.event_time,
                    "actor_role": row.actor_role,
                    "event_class": row.event_class,
                    "payload_type": row.payload_type,
                    "call_id": row.call_id,
                    "name": row.name,
                    "phase": row.phase,
                    "item_id": row.item_id,
                    "source_ref": row.source_ref,
                    "text_content": row.text_content,
                    "payload_json": row.payload_json,
                    "token_usage_json": row.token_usage_json,
                })
            })
            .collect();

        Ok(json!({
            "found": true,
            "event_uid": event_uid,
            "session_id": target.session_id,
            "target_event_order": target.event_order,
            "turn_seq": target.turn_seq,
            "before": before,
            "after": after,
            "events": events,
        }))
    }

    fn table_ref(&self, table: &str) -> String {
        format!(
            "{}.{}",
            sql_identifier(&self.cfg.clickhouse.database),
            sql_identifier(table)
        )
    }
}

fn rpc_ok(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result
    })
}

fn rpc_err(id: Value, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message
        }
    })
}

fn tool_ok_full(payload: Value) -> Value {
    let text = serde_json::to_string_pretty(&payload).unwrap_or_else(|_| "{}".to_string());
    json!({
        "content": [
            {
                "type": "text",
                "text": text
            }
        ],
        "structuredContent": payload,
        "isError": false
    })
}

fn tool_ok_prose(text: String) -> Value {
    json!({
        "content": [
            {
                "type": "text",
                "text": text
            }
        ],
        "isError": false
    })
}

fn tool_error_result(message: String) -> Value {
    json!({
        "content": [
            {
                "type": "text",
                "text": message
            }
        ],
        "isError": true
    })
}

fn format_search_prose(payload: &Value) -> Result<String> {
    let parsed: SearchProsePayload =
        serde_json::from_value(payload.clone()).context("failed to parse search payload")?;

    let mut out = String::new();
    out.push_str(&format!("Search: \"{}\"\n", parsed.query));
    out.push_str(&format!("Query ID: {}\n", parsed.query_id));
    out.push_str(&format!(
        "Hits: {} ({} ms)\n",
        parsed.stats.result_count, parsed.stats.took_ms
    ));

    if parsed.hits.is_empty() {
        out.push_str("\nNo hits.");
        return Ok(out);
    }

    for hit in &parsed.hits {
        let kind = display_kind(&hit.event_class, &hit.payload_type);
        out.push_str(&format!(
            "\n{}) session={} score={:.4} kind={} role={}\n",
            hit.rank, hit.session_id, hit.score, kind, hit.actor_role
        ));

        let snippet = compact_text_line(&hit.text_preview, 220);
        if !snippet.is_empty() {
            out.push_str(&format!("   snippet: {}\n", snippet));
        }

        out.push_str(&format!("   event_uid: {}\n", hit.event_uid));
        out.push_str(&format!("   next: open(event_uid=\"{}\")\n", hit.event_uid));
    }

    Ok(out.trim_end().to_string())
}

fn format_open_prose(payload: &Value) -> Result<String> {
    let mut parsed: OpenProsePayload =
        serde_json::from_value(payload.clone()).context("failed to parse open payload")?;

    let mut out = String::new();
    out.push_str(&format!("Open event: {}\n", parsed.event_uid));

    if !parsed.found {
        out.push_str("Not found.");
        return Ok(out);
    }

    out.push_str(&format!("Session: {}\n", parsed.session_id));
    out.push_str(&format!("Turn: {}\n", parsed.turn_seq));
    out.push_str(&format!(
        "Context window: before={} after={}\n",
        parsed.before, parsed.after
    ));

    parsed.events.sort_by_key(|e| e.event_order);

    let mut before_events = Vec::new();
    let mut target_events = Vec::new();
    let mut after_events = Vec::new();

    for event in parsed.events {
        if event.is_target || event.event_order == parsed.target_event_order {
            target_events.push(event);
        } else if event.event_order < parsed.target_event_order {
            before_events.push(event);
        } else {
            after_events.push(event);
        }
    }

    out.push_str("\nBefore:\n");
    if before_events.is_empty() {
        out.push_str("- (none)\n");
    } else {
        for event in &before_events {
            append_open_event_line(&mut out, event);
        }
    }

    out.push_str("\nTarget:\n");
    if target_events.is_empty() {
        out.push_str("- (none)\n");
    } else {
        for event in &target_events {
            append_open_event_line(&mut out, event);
        }
    }

    out.push_str("\nAfter:\n");
    if after_events.is_empty() {
        out.push_str("- (none)");
    } else {
        for event in &after_events {
            append_open_event_line(&mut out, event);
        }
    }

    Ok(out.trim_end().to_string())
}

fn append_open_event_line(out: &mut String, event: &OpenProseEvent) {
    let kind = display_kind(&event.event_class, &event.payload_type);
    out.push_str(&format!(
        "- [{}] {} {}\n",
        event.event_order, event.actor_role, kind
    ));

    let text = compact_text_line(&event.text_content, 220);
    if !text.is_empty() {
        out.push_str(&format!("  {}\n", text));
    }
}

fn display_kind(event_class: &str, payload_type: &str) -> String {
    if payload_type.is_empty() || payload_type == event_class || payload_type == "unknown" {
        if event_class.is_empty() {
            "event".to_string()
        } else {
            event_class.to_string()
        }
    } else if event_class.is_empty() {
        payload_type.to_string()
    } else {
        format!("{} ({})", event_class, payload_type)
    }
}

fn compact_text_line(text: &str, max_chars: usize) -> String {
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= max_chars {
        return compact;
    }

    let mut trimmed: String = compact.chars().take(max_chars.saturating_sub(3)).collect();
    trimmed.push_str("...");
    trimmed
}

fn token_re() -> &'static Regex {
    static TOKEN_RE: OnceLock<Regex> = OnceLock::new();
    TOKEN_RE.get_or_init(|| Regex::new(r"[A-Za-z0-9_]+").expect("valid token regex"))
}

fn safe_value_re() -> &'static Regex {
    static SAFE_RE: OnceLock<Regex> = OnceLock::new();
    SAFE_RE
        .get_or_init(|| Regex::new(r"^[A-Za-z0-9._:@/-]{1,256}$").expect("valid safe-value regex"))
}

fn tokenize_query(text: &str, max_terms: usize) -> Vec<(String, u32)> {
    let mut order = Vec::<String>::new();
    let mut tf = HashMap::<String, u32>::new();

    for mat in token_re().find_iter(text) {
        let token = mat.as_str().to_ascii_lowercase();
        if token.len() < 2 || token.len() > 64 {
            continue;
        }

        if !tf.contains_key(&token) {
            order.push(token.clone());
        }
        let entry = tf.entry(token).or_insert(0);
        *entry += 1;

        if order.len() >= max_terms {
            break;
        }
    }

    order
        .into_iter()
        .map(|token| {
            let count = *tf.get(&token).unwrap_or(&1);
            (token, count)
        })
        .collect()
}

fn is_safe_filter_value(value: &str) -> bool {
    safe_value_re().is_match(value)
}

fn sql_quote(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

fn sql_identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

fn sql_array_strings(items: &[String]) -> String {
    let parts = items.iter().map(|item| sql_quote(item)).collect::<Vec<_>>();
    format!("[{}]", parts.join(","))
}

fn sql_array_f64(items: &[f64]) -> String {
    let parts = items
        .iter()
        .map(|v| format!("{:.12}", v))
        .collect::<Vec<_>>();
    format!("[{}]", parts.join(","))
}

fn df_map_requires_recompute(terms: &[String], df_map: &HashMap<String, u64>, docs: u64) -> bool {
    if df_map.len() != terms.len() {
        return true;
    }

    terms
        .iter()
        .any(|term| df_map.get(term).copied().unwrap_or(0) > docs)
}

pub async fn run_stdio(cfg: AppConfig) -> Result<()> {
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    ch.ping().await.context("clickhouse ping failed")?;

    let state = Arc::new(AppState { cfg, ch });

    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    let mut stdout = tokio::io::stdout();

    while let Some(line) = lines.next_line().await? {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        debug!("incoming rpc line: {}", line);

        let parsed = serde_json::from_str::<RpcRequest>(line);
        let req = match parsed {
            Ok(req) => req,
            Err(err) => {
                warn!("failed to parse rpc request: {}", err);
                continue;
            }
        };

        if let Some(resp) = state.handle_request(req).await {
            let payload = serde_json::to_vec(&resp)?;
            stdout.write_all(&payload).await?;
            stdout.write_all(b"\n").await?;
            stdout.flush().await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cortex_config::ClickHouseConfig;

    fn test_state(database: &str) -> AppState {
        let mut cfg = AppConfig::default();
        cfg.clickhouse = ClickHouseConfig {
            url: "http://127.0.0.1:8123".to_string(),
            database: database.to_string(),
            ..cfg.clickhouse
        };

        let ch = ClickHouseClient::new(cfg.clickhouse.clone()).expect("valid client config");
        AppState { cfg, ch }
    }

    #[test]
    fn tokenize_query_enforces_limits_and_counts() {
        let terms = tokenize_query("Hello hello world tool_use", 3);
        assert_eq!(terms.len(), 3);
        assert_eq!(terms[0], ("hello".to_string(), 2));
        assert_eq!(terms[1].0, "world");
    }

    #[test]
    fn safe_filter_value_validation() {
        assert!(is_safe_filter_value("session_123"));
        assert!(is_safe_filter_value("a/b.c:d@e-1"));
        assert!(!is_safe_filter_value("drop table;"));
    }

    #[test]
    fn sql_array_builders_escape_values() {
        let values = vec!["a".to_string(), "b'c".to_string()];
        let out = sql_array_strings(&values);
        assert!(out.contains("'a'"));
        assert!(out.contains("'b''c'"));
    }

    #[test]
    fn build_search_sql_uses_configured_database() {
        let state = test_state("alt_db");
        let terms = vec!["hello".to_string(), "world".to_string()];
        let mut idf = HashMap::new();
        idf.insert("hello".to_string(), 1.0);
        idf.insert("world".to_string(), 2.0);

        let sql = state
            .build_search_sql(&terms, &idf, 42.0, false, true, Some("s-123"), 1, 0.0, 10)
            .expect("search sql should build");

        assert!(sql.contains("FROM `alt_db`.`search_postings` AS p"));
        assert!(sql.contains("ANY INNER JOIN `alt_db`.`search_documents` AS d"));
        assert!(sql.contains("p.session_id = 's-123'"));
        assert!(sql.contains("p.event_class IN ('message', 'reasoning', 'event_msg')"));
        assert!(sql.contains("lowerUTF8(d.name) NOT IN ('search', 'open')"));
    }

    #[test]
    fn table_ref_escapes_identifiers() {
        let state = test_state("db`name");
        assert_eq!(state.table_ref("table`x"), "`db``name`.`table``x`");
    }

    #[test]
    fn df_map_recompute_when_terms_missing() {
        let terms = vec!["hello".to_string(), "world".to_string()];
        let mut df_map = HashMap::new();
        df_map.insert("hello".to_string(), 2);

        assert!(df_map_requires_recompute(&terms, &df_map, 10));
    }

    #[test]
    fn df_map_recompute_when_primary_df_exceeds_docs() {
        let terms = vec!["hello".to_string(), "world".to_string()];
        let mut df_map = HashMap::new();
        df_map.insert("hello".to_string(), 11);
        df_map.insert("world".to_string(), 2);

        assert!(df_map_requires_recompute(&terms, &df_map, 10));
    }

    #[test]
    fn df_map_skip_recompute_when_primary_stats_are_consistent() {
        let terms = vec!["hello".to_string(), "world".to_string()];
        let mut df_map = HashMap::new();
        df_map.insert("hello".to_string(), 4);
        df_map.insert("world".to_string(), 2);

        assert!(!df_map_requires_recompute(&terms, &df_map, 10));
    }
}
