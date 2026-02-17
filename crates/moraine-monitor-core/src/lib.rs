use anyhow::{anyhow, Result};
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, HeaderValue, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::time::Instant;

use moraine_clickhouse::ClickHouseClient;
use moraine_config::AppConfig;

#[derive(Clone)]
struct AppState {
    clickhouse: ClickHouseClient,
    static_dir: PathBuf,
}

#[derive(Deserialize)]
struct LimitQuery {
    limit: Option<u32>,
}

#[derive(Deserialize)]
struct AnalyticsQuery {
    range: Option<String>,
}

#[derive(Serialize)]
struct TableSummary {
    name: String,
    engine: String,
    is_temporary: u8,
    rows: u64,
}

pub async fn run_server(
    cfg: AppConfig,
    host: String,
    port: u16,
    static_dir: PathBuf,
) -> Result<()> {
    let clickhouse = ClickHouseClient::new(cfg.clickhouse)?;

    let state = AppState {
        clickhouse,
        static_dir,
    };

    let app = Router::new()
        .route("/api/health", get(api_health))
        .route("/api/status", get(api_status))
        .route("/api/analytics", get(api_analytics))
        .route("/api/tables", get(api_tables))
        .route("/api/web-searches", get(api_web_searches))
        .route("/api/tables/:table", get(api_table_rows))
        .fallback(get(static_fallback))
        .with_state(state.clone());

    let bind = format!("{}:{}", host, port)
        .parse::<SocketAddr>()
        .map_err(|err| anyhow!("invalid bind address: {err}"))?;

    println!("moraine-monitor running at http://{}", bind);
    println!("serving UI from {}", state.static_dir.display());

    let listener = tokio::net::TcpListener::bind(bind).await.map_err(|error| {
        if error.kind() == ErrorKind::AddrInUse {
            anyhow!(
                "failed to bind {bind}: address already in use. another monitor may already be running (including legacy cortex-monitor). stop it or rerun with `moraine run monitor -- --port <free-port>`"
            )
        } else {
            anyhow!("failed to bind {bind}: {error}")
        }
    })?;
    axum::serve(listener, app).await?;
    Ok(())
}

fn json_response<T: Serialize>(payload: T, status: StatusCode) -> Response {
    let mut response = Json(payload).into_response();
    *response.status_mut() = status;
    response
}

fn clickhouse_ping_payload(version: String, ping_result: Result<()>, ping_ms: f64) -> Value {
    match ping_result {
        Ok(()) => json!({
            "healthy": true,
            "version": version,
            "ping_ms": ping_ms,
            "error": Value::Null,
        }),
        Err(error) => json!({
            "healthy": false,
            "version": version,
            "ping_ms": ping_ms,
            "error": error.to_string(),
        }),
    }
}

async fn api_health(State(state): State<AppState>) -> Response {
    let start = Instant::now();
    let connection_stats = query_clickhouse_connections(&state)
        .await
        .unwrap_or_else(|error| json!({"total": Value::Null, "error": error.to_string()}));

    match state.clickhouse.ping().await {
        Ok(()) => match state.clickhouse.version().await {
            Ok(version) => {
                let body = json!({
                    "ok": true,
                    "url": state.clickhouse.config().url,
                    "database": state.clickhouse.config().database,
                    "version": version,
                    "ping_ms": (Instant::elapsed(&start).as_secs_f64() * 1000.0),
                    "connections": connection_stats,
                });
                json_response(body, StatusCode::OK)
            }
            Err(error) => json_response(
                json!({
                    "ok": false,
                    "url": state.clickhouse.config().url,
                    "database": state.clickhouse.config().database,
                    "error": error.to_string(),
                    "connections": connection_stats,
                }),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
        },
        Err(error) => json_response(
            json!({
                "ok": false,
                "url": state.clickhouse.config().url,
                "database": state.clickhouse.config().database,
                "error": error.to_string(),
                "connections": connection_stats,
            }),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
    }
}

async fn api_status(State(state): State<AppState>) -> Response {
    let db_exists = match state
        .clickhouse
        .query_rows::<Value>(
            &format!(
                "SELECT name FROM system.databases WHERE name = '{}'",
                escape_literal(&state.clickhouse.config().database)
            ),
            None,
        )
        .await
    {
        Ok(rows) => !rows.is_empty(),
        Err(_) => false,
    };

    let tables = if db_exists {
        match query_table_summaries(&state).await {
            Ok(value) => value,
            Err(error) => {
                return json_response(
                    json!({"ok": false, "error": error.to_string()}),
                    StatusCode::SERVICE_UNAVAILABLE,
                );
            }
        }
    } else {
        Vec::new()
    };

    let estimated_total_rows = tables.iter().map(|table| table.rows).sum::<u64>();
    let heartbeat = if db_exists {
        query_heartbeat(&state).await.unwrap_or_else(|_| {
            json!({"present": false, "alive": false, "latest": Value::Null, "age_seconds": Value::Null})
        })
    } else {
        json!({"present": false, "alive": false, "latest": Value::Null, "age_seconds": Value::Null})
    };

    let clickhouse_ping = if db_exists {
        match state.clickhouse.version().await {
            Ok(version) => {
                let start = Instant::now();
                let ping_result = state.clickhouse.ping().await;
                let ping_ms = Instant::elapsed(&start).as_secs_f64() * 1000.0;
                clickhouse_ping_payload(version, ping_result, ping_ms)
            }
            Err(error) => {
                json!({
                    "healthy": false,
                    "version": Value::Null,
                    "ping_ms": Value::Null,
                    "error": error.to_string(),
                })
            }
        }
    } else {
        json!({
            "healthy": false,
            "version": Value::Null,
            "ping_ms": Value::Null,
            "error": "database not found",
        })
    };

    json_response(
        json!({
            "ok": true,
            "clickhouse": {
                "url": state.clickhouse.config().url,
            "database": state.clickhouse.config().database,
            "healthy": clickhouse_ping["healthy"],
            "version": clickhouse_ping["version"],
            "ping_ms": clickhouse_ping["ping_ms"],
            "error": clickhouse_ping["error"],
            "connections": if db_exists {
                query_clickhouse_connections(&state)
                    .await
                    .unwrap_or_else(|error| json!({"total": Value::Null, "error": error.to_string()}))
            } else {
                json!({"total": Value::Null, "error": "database not found"})
            },
        },
        "database": {
            "exists": db_exists,
            "table_count": tables.len(),
            "estimated_total_rows": estimated_total_rows,
                "tables": tables,
            },
            "ingestor": heartbeat,
        }),
        StatusCode::OK,
    )
}

async fn api_tables(State(state): State<AppState>) -> Response {
    match query_table_summaries(&state).await {
        Ok(tables) => json_response(json!({"ok": true, "tables": tables}), StatusCode::OK),
        Err(error) => json_response(
            json!({"ok": false, "error": error.to_string()}),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
    }
}

async fn api_web_searches(
    Query(params): Query<LimitQuery>,
    State(state): State<AppState>,
) -> Response {
    #[derive(serde::Deserialize, serde::Serialize)]
    struct WebSearchRow {
        event_time: String,
        provider: String,
        source_name: String,
        session_id: String,
        model: String,
        action: String,
        search_query: String,
        result_url: String,
        source_ref: String,
    }

    let limit = params.limit.unwrap_or(100).clamp(1, 1000);
    let database = &state.clickhouse.config().database;
    let table = format!(
        "{}.{}",
        escape_identifier(database),
        escape_identifier("events")
    );

    let query = format!(
        "SELECT \
            toString(event_ts) AS event_time, \
            provider, \
            source_name, \
            session_id, \
            lowerUTF8(trim(BOTH ' ' FROM model)) AS model, \
            if(payload_type = 'web_search_call', op_kind, if(tool_name = 'WebFetch', 'open_page', if(tool_name = 'WebSearch', 'search', payload_type))) AS action, \
            if(length(JSONExtractString(payload_json, 'action', 'query')) > 0, \
               JSONExtractString(payload_json, 'action', 'query'), \
               if(length(JSONExtractString(payload_json, 'input', 'query')) > 0, \
                  JSONExtractString(payload_json, 'input', 'query'), \
                  if(length(JSONExtractString(payload_json, 'data', 'query')) > 0, \
                     JSONExtractString(payload_json, 'data', 'query'), \
                     text_content))) AS search_query, \
            if(length(JSONExtractString(payload_json, 'action', 'url')) > 0, \
               JSONExtractString(payload_json, 'action', 'url'), \
               JSONExtractString(payload_json, 'input', 'url')) AS result_url, \
            source_ref \
         FROM {table} \
         WHERE payload_type = 'web_search_call' \
            OR (payload_type = 'tool_use' AND tool_name IN ('WebSearch', 'WebFetch')) \
            OR payload_type = 'search_results_received' \
         ORDER BY event_ts DESC \
         LIMIT {limit}",
        table = table,
        limit = limit,
    );

    let rows = match state
        .clickhouse
        .query_rows::<WebSearchRow>(&query, None)
        .await
    {
        Ok(rows) => rows,
        Err(error) => {
            return json_response(
                json!({"ok": false, "error": format!("web search query failed: {error}")}),
                StatusCode::SERVICE_UNAVAILABLE,
            );
        }
    };

    json_response(
        json!({
            "ok": true,
            "table": "web_searches",
            "limit": limit,
            "schema": [
                {"name": "event_time", "type": "String", "default_expression": ""},
                {"name": "provider", "type": "String", "default_expression": ""},
                {"name": "source_name", "type": "String", "default_expression": ""},
                {"name": "session_id", "type": "String", "default_expression": ""},
                {"name": "model", "type": "String", "default_expression": ""},
                {"name": "action", "type": "String", "default_expression": ""},
                {"name": "search_query", "type": "String", "default_expression": ""},
                {"name": "result_url", "type": "String", "default_expression": ""},
                {"name": "source_ref", "type": "String", "default_expression": ""}
            ],
            "rows": rows,
        }),
        StatusCode::OK,
    )
}

#[derive(Clone, Copy)]
struct AnalyticsRange {
    key: &'static str,
    label: &'static str,
    window_seconds: u32,
    bucket_seconds: u32,
}

fn resolve_analytics_range(value: Option<&str>) -> AnalyticsRange {
    match value.unwrap_or("24h") {
        "15m" => AnalyticsRange {
            key: "15m",
            label: "Last 15m",
            window_seconds: 15 * 60,
            bucket_seconds: 60,
        },
        "1h" => AnalyticsRange {
            key: "1h",
            label: "Last 1h",
            window_seconds: 60 * 60,
            bucket_seconds: 5 * 60,
        },
        "6h" => AnalyticsRange {
            key: "6h",
            label: "Last 6h",
            window_seconds: 6 * 60 * 60,
            bucket_seconds: 15 * 60,
        },
        "24h" => AnalyticsRange {
            key: "24h",
            label: "Last 24h",
            window_seconds: 24 * 60 * 60,
            bucket_seconds: 60 * 60,
        },
        "7d" => AnalyticsRange {
            key: "7d",
            label: "Last 7d",
            window_seconds: 7 * 24 * 60 * 60,
            bucket_seconds: 6 * 60 * 60,
        },
        "30d" => AnalyticsRange {
            key: "30d",
            label: "Last 30d",
            window_seconds: 30 * 24 * 60 * 60,
            bucket_seconds: 24 * 60 * 60,
        },
        _ => AnalyticsRange {
            key: "24h",
            label: "Last 24h",
            window_seconds: 24 * 60 * 60,
            bucket_seconds: 60 * 60,
        },
    }
}

async fn api_analytics(
    Query(params): Query<AnalyticsQuery>,
    State(state): State<AppState>,
) -> Response {
    #[derive(serde::Deserialize, serde::Serialize)]
    struct TokenRow {
        bucket_unix: u64,
        model: String,
        tokens: u64,
    }

    #[derive(serde::Deserialize, serde::Serialize)]
    struct TurnRow {
        bucket_unix: u64,
        model: String,
        turns: u64,
    }

    #[derive(serde::Deserialize, serde::Serialize)]
    struct ConcurrentRow {
        bucket_unix: u64,
        concurrent_sessions: u64,
    }

    let range = resolve_analytics_range(params.range.as_deref());
    let database = &state.clickhouse.config().database;
    let table = format!(
        "{}.{}",
        escape_identifier(database),
        escape_identifier("events")
    );
    let model_expr = "if(lowerUTF8(trim(BOTH ' ' FROM model)) = 'codex', 'gpt-5.3-codex-xhigh', lowerUTF8(trim(BOTH ' ' FROM model)))";
    let model_expr_latest = "if(lowerUTF8(trim(BOTH ' ' FROM model_latest)) = 'codex', 'gpt-5.3-codex-xhigh', lowerUTF8(trim(BOTH ' ' FROM model_latest)))";
    let window_filter = format!(
        "event_ts >= now() - INTERVAL {} SECOND AND length(trim(BOTH ' ' FROM model)) > 0 AND lowerUTF8(trim(BOTH ' ' FROM model)) != '<synthetic>'",
        range.window_seconds
    );
    let generation_latest_filter = format!(
        "event_ts_latest >= now() - INTERVAL {} SECOND AND length(trim(BOTH ' ' FROM model_latest)) > 0 AND lowerUTF8(trim(BOTH ' ' FROM model_latest)) != '<synthetic>' AND output_tokens_latest > 0",
        range.window_seconds
    );
    let turn_filter = format!(
        "{} AND length(trim(BOTH ' ' FROM request_id)) > 0",
        window_filter
    );
    let concurrent_filter = format!(
        "event_ts >= now() - INTERVAL {} SECOND AND length(trim(BOTH ' ' FROM session_id)) > 0 AND (input_tokens > 0 OR output_tokens > 0 OR cache_read_tokens > 0 OR cache_write_tokens > 0)",
        range.window_seconds
    );

    let token_query = format!(
        "SELECT bucket_unix, model, toUInt64(sum(tokens)) AS tokens \
         FROM ( \
           SELECT bucket_unix, model, toUInt64(max(output_tokens_latest)) AS tokens \
           FROM ( \
             SELECT \
               toUInt64(toUnixTimestamp(toStartOfInterval(event_ts_latest, INTERVAL {bucket_seconds} SECOND))) AS bucket_unix, \
               {model_expr_latest} AS model, \
               provider_latest, \
               session_id_latest, \
               request_id_latest, \
               output_tokens_latest \
             FROM ( \
               SELECT \
                 event_uid, \
                 argMax(event_ts, event_version) AS event_ts_latest, \
                 argMax(model, event_version) AS model_latest, \
                 argMax(provider, event_version) AS provider_latest, \
                 argMax(session_id, event_version) AS session_id_latest, \
                 argMax(request_id, event_version) AS request_id_latest, \
                 argMax(output_tokens, event_version) AS output_tokens_latest \
               FROM {table} \
               GROUP BY event_uid \
             ) WHERE {generation_latest_filter} \
           ) \
           WHERE provider_latest = 'claude' AND length(trim(BOTH ' ' FROM request_id_latest)) > 0 \
           GROUP BY bucket_unix, model, session_id_latest, request_id_latest \
           UNION ALL \
           SELECT \
             toUInt64(toUnixTimestamp(toStartOfInterval(event_ts_latest, INTERVAL {bucket_seconds} SECOND))) AS bucket_unix, \
             {model_expr_latest} AS model, \
             toUInt64(output_tokens_latest) AS tokens \
           FROM ( \
             SELECT \
               event_uid, \
               argMax(event_ts, event_version) AS event_ts_latest, \
               argMax(model, event_version) AS model_latest, \
               argMax(provider, event_version) AS provider_latest, \
               argMax(session_id, event_version) AS session_id_latest, \
               argMax(request_id, event_version) AS request_id_latest, \
               argMax(output_tokens, event_version) AS output_tokens_latest \
             FROM {table} \
             GROUP BY event_uid \
           ) WHERE {generation_latest_filter} \
           AND NOT (provider_latest = 'claude' AND length(trim(BOTH ' ' FROM request_id_latest)) > 0) \
         ) \
         GROUP BY bucket_unix, model \
         ORDER BY bucket_unix ASC, model ASC",
        bucket_seconds = range.bucket_seconds,
        model_expr_latest = model_expr_latest,
        table = table,
        generation_latest_filter = generation_latest_filter,
    );

    let turns_query = format!(
        "SELECT \
            toUInt64(toUnixTimestamp(toStartOfInterval(event_ts, INTERVAL {bucket_seconds} SECOND))) AS bucket_unix, \
            {model_expr} AS model, \
            toUInt64(uniqExact(tuple(session_id, request_id))) AS turns \
         FROM {table} \
         WHERE {turn_filter} \
         GROUP BY bucket_unix, model \
         ORDER BY bucket_unix ASC, model ASC",
        bucket_seconds = range.bucket_seconds,
        model_expr = model_expr,
        table = table,
        turn_filter = turn_filter,
    );

    let concurrent_query = format!(
        "SELECT bucket_unix, toUInt64(uniqExact(session_stream_key)) AS concurrent_sessions \
         FROM ( \
           SELECT \
             toUInt64(toUnixTimestamp(toStartOfInterval(event_ts, INTERVAL {bucket_seconds} SECOND))) AS bucket_unix, \
             if(provider = 'claude' AND length(trim(BOTH ' ' FROM agent_run_id)) > 0, concat(session_id, '::', agent_run_id), session_id) AS session_stream_key \
           FROM {table} \
           WHERE {concurrent_filter} \
         ) \
         GROUP BY bucket_unix \
         ORDER BY bucket_unix ASC",
        bucket_seconds = range.bucket_seconds,
        table = table,
        concurrent_filter = concurrent_filter,
    );

    let token_rows = match state
        .clickhouse
        .query_rows::<TokenRow>(&token_query, None)
        .await
    {
        Ok(rows) => rows,
        Err(error) => {
            return json_response(
                json!({"ok": false, "error": format!("analytics token query failed: {error}")}),
                StatusCode::SERVICE_UNAVAILABLE,
            );
        }
    };

    let turn_rows = match state
        .clickhouse
        .query_rows::<TurnRow>(&turns_query, None)
        .await
    {
        Ok(rows) => rows,
        Err(error) => {
            return json_response(
                json!({"ok": false, "error": format!("analytics turns query failed: {error}")}),
                StatusCode::SERVICE_UNAVAILABLE,
            );
        }
    };

    let concurrent_rows = match state
        .clickhouse
        .query_rows::<ConcurrentRow>(&concurrent_query, None)
        .await
    {
        Ok(rows) => rows,
        Err(error) => {
            return json_response(
                json!({"ok": false, "error": format!("analytics concurrent-session query failed: {error}")}),
                StatusCode::SERVICE_UNAVAILABLE,
            );
        }
    };

    #[derive(serde::Deserialize)]
    struct WindowRow {
        now_unix: u64,
        from_unix: u64,
    }

    let window_query = format!(
        "SELECT toUInt64(toUnixTimestamp(max(event_ts))) AS now_unix, toUInt64(toUnixTimestamp(max(event_ts) - INTERVAL {} SECOND)) AS from_unix FROM {} WHERE {}",
        range.window_seconds,
        table,
        window_filter,
    );

    let (now_unix, from_unix) = match state
        .clickhouse
        .query_rows::<WindowRow>(&window_query, None)
        .await
    {
        Ok(rows) if !rows.is_empty() => {
            let row = &rows[0];
            (row.now_unix, row.from_unix)
        }
        _ => {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            (now, now.saturating_sub(range.window_seconds as u64))
        }
    };

    json_response(
        json!({
            "ok": true,
            "range": {
                "key": range.key,
                "label": range.label,
                "window_seconds": range.window_seconds,
                "bucket_seconds": range.bucket_seconds,
                "from_unix": from_unix,
                "to_unix": now_unix,
            },
            "series": {
                "tokens": token_rows,
                "turns": turn_rows,
                "concurrent_sessions": concurrent_rows,
            }
        }),
        StatusCode::OK,
    )
}

async fn query_table_summaries(state: &AppState) -> Result<Vec<TableSummary>> {
    #[derive(serde::Deserialize)]
    struct TableRow {
        name: String,
        engine: String,
        is_temporary: u8,
    }

    #[derive(serde::Deserialize)]
    struct PartsRow {
        table: String,
        rows: u64,
    }

    let database = &state.clickhouse.config().database;
    let tables = state
        .clickhouse
        .query_rows::<TableRow>(&format!(
            "SELECT name, engine, is_temporary FROM system.tables WHERE database = '{}' ORDER BY name",
            escape_literal(database)
        ), None)
        .await?;

    let parts = state
        .clickhouse
        .query_rows::<PartsRow>(&format!(
            "SELECT table, SUM(rows) AS rows FROM system.parts WHERE database = '{}' AND active GROUP BY table",
            escape_literal(database)
        ), None)
        .await
        .unwrap_or_default();

    let counts: HashMap<String, u64> = parts.into_iter().map(|row| (row.table, row.rows)).collect();

    let values = tables
        .into_iter()
        .map(|row| TableSummary {
            name: row.name.clone(),
            engine: row.engine,
            is_temporary: row.is_temporary,
            rows: counts.get(&row.name).copied().unwrap_or(0),
        })
        .collect();

    Ok(values)
}

async fn query_heartbeat(state: &AppState) -> Result<Value> {
    let database = &state.clickhouse.config().database;
    let present = state
        .clickhouse
        .query_rows::<Value>(
            &format!(
            "SELECT name FROM system.tables WHERE database = '{}' AND name = 'ingest_heartbeats'",
            escape_literal(database)
        ),
            None,
        )
        .await?;

    if present.is_empty() {
        return Ok(json!({
            "present": false,
            "alive": false,
            "latest": Value::Null,
            "age_seconds": Value::Null,
        }));
    }

    let query = format!(
        "SELECT ts, toUnixTimestamp64Milli(ts) AS ts_unix_ms, host, service_version, queue_depth, files_active, files_watched, rows_raw_written, rows_events_written, rows_errors_written, flush_latency_ms, append_to_visible_p50_ms, append_to_visible_p95_ms, last_error FROM {}.ingest_heartbeats ORDER BY ts DESC LIMIT 1",
        escape_identifier(database)
    );

    let latest = state
        .clickhouse
        .query_rows::<Value>(&query, None)
        .await?
        .into_iter()
        .next();

    let Some(latest) = latest else {
        return Ok(
            json!({"present": false, "alive": false, "latest": Value::Null, "age_seconds": Value::Null}),
        );
    };

    let age_seconds = latest
        .get("ts_unix_ms")
        .and_then(value_to_i64)
        .and_then(|ts_ms| {
            if ts_ms < 0 {
                return None;
            }

            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()?
                .as_millis() as i64;
            if now_ms < ts_ms {
                return Some(0);
            }

            Some(((now_ms - ts_ms) / 1000) as u64)
        });

    Ok(json!({
        "present": true,
        "latest": latest,
        "age_seconds": age_seconds,
        "alive": age_seconds.map(|age| age <= 30).unwrap_or(false),
    }))
}

async fn query_clickhouse_connections(state: &AppState) -> Result<Value> {
    #[derive(serde::Deserialize)]
    struct MetricRow {
        metric: String,
        value: u64,
    }

    let metrics = state
        .clickhouse
        .query_rows::<MetricRow>(
            "SELECT metric, value FROM system.metrics WHERE metric IN ('TCPConnection','HTTPConnection','MySQLConnection','PostgreSQLConnection','InterserverConnection')",
            None,
        )
        .await?;

    let mut tcp = 0_u64;
    let mut http = 0_u64;
    let mut mysql = 0_u64;
    let mut postgres = 0_u64;
    let mut interserver = 0_u64;

    for row in metrics {
        match row.metric.as_str() {
            "TCPConnection" => tcp = tcp.saturating_add(row.value),
            "HTTPConnection" => http = http.saturating_add(row.value),
            "MySQLConnection" => mysql = mysql.saturating_add(row.value),
            "PostgreSQLConnection" => postgres = postgres.saturating_add(row.value),
            "InterserverConnection" => interserver = interserver.saturating_add(row.value),
            _ => {}
        }
    }

    let total = tcp
        .saturating_add(http)
        .saturating_add(mysql)
        .saturating_add(postgres)
        .saturating_add(interserver);

    Ok(json!({
        "total": total,
        "tcp": tcp,
        "http": http,
        "mysql": mysql,
        "postgres": postgres,
        "interserver": interserver,
    }))
}

async fn api_table_rows(
    Path(table): Path<String>,
    Query(params): Query<LimitQuery>,
    State(state): State<AppState>,
) -> Response {
    if !is_safe_identifier(&table) {
        return json_response(
            json!({"ok": false, "error": "invalid table name"}),
            StatusCode::BAD_REQUEST,
        );
    }

    let limit = params.limit.unwrap_or(25).clamp(1, 500);
    let database = &state.clickhouse.config().database;

    #[derive(serde::Deserialize)]
    struct SchemaRow {
        name: String,
        #[serde(rename = "type")]
        type_name: String,
        default_expression: Option<String>,
    }

    let schema = match state
        .clickhouse
        .query_rows::<SchemaRow>(&format!(
            "SELECT name, type, default_expression FROM system.columns WHERE database = '{}' AND table = '{}' ORDER BY position",
            escape_literal(database),
            escape_literal(&table)
        ), None)
        .await
    {
        Ok(value) => value,
        Err(error) => {
            return json_response(
                json!({
                    "ok": false,
                    "error": format!("unable to read schema for table {table}: {error}"),
                }),
                StatusCode::SERVICE_UNAVAILABLE,
            );
        }
    };

    let preview_columns: Vec<String> = schema.iter().map(|entry| entry.name.clone()).collect();
    let rows_query = table_preview_rows_query(database, &table, &preview_columns, limit);

    let rows = match state
        .clickhouse
        .query_rows::<Value>(&rows_query, None)
        .await
    {
        Ok(value) => value,
        Err(error) => {
            return json_response(
                json!({
                    "ok": false,
                    "error": format!("unable to read table {table}: {error}"),
                }),
                StatusCode::SERVICE_UNAVAILABLE,
            );
        }
    };

    let schema_payload: Vec<Value> = schema
        .into_iter()
        .map(|entry| {
            json!({
                "name": entry.name,
                "type": entry.type_name,
                "default_expression": entry.default_expression.unwrap_or_default(),
            })
        })
        .collect();

    json_response(
        json!({
            "ok": true,
            "table": table,
            "limit": limit,
            "schema": schema_payload,
            "rows": rows,
        }),
        StatusCode::OK,
    )
}

async fn static_fallback(State(state): State<AppState>, uri: Uri) -> Response {
    let requested = uri.path();
    if requested.contains("..") {
        return json_response(
            json!({"ok": false, "error": "forbidden"}),
            StatusCode::FORBIDDEN,
        );
    }

    let file_path = if requested == "/" || requested.is_empty() {
        state.static_dir.join("index.html")
    } else {
        let mut target = state.static_dir.join(requested.trim_start_matches('/'));
        if target.is_dir() {
            target.push("index.html");
        }
        target
    };

    let canonical_root = match fs::canonicalize(&state.static_dir).await {
        Ok(path) => path,
        Err(error) => {
            return json_response(
                json!({"ok": false, "error": format!("static directory unavailable: {error}")}),
                StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
    };

    let canonical_file = match fs::canonicalize(&file_path).await {
        Ok(path) => path,
        Err(_) => {
            return json_response(
                json!({"ok": false, "error": "not found"}),
                StatusCode::NOT_FOUND,
            );
        }
    };

    if !canonical_file.starts_with(&canonical_root) {
        return json_response(
            json!({"ok": false, "error": "forbidden"}),
            StatusCode::FORBIDDEN,
        );
    }

    let bytes = match fs::read(&canonical_file).await {
        Ok(value) => value,
        Err(error) => {
            return json_response(
                json!({"ok": false, "error": format!("failed to read file: {error}")}),
                StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
    };

    let content_type = mime_guess::from_path(&canonical_file)
        .first_or_octet_stream()
        .essence_str()
        .to_string();

    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&content_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    response
}

fn is_safe_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) if first == '_' || first.is_ascii_alphabetic() => {}
        _ => return false,
    }

    chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn escape_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn escape_identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

fn table_preview_rows_query(
    database: &str,
    table: &str,
    column_names: &[String],
    limit: u32,
) -> String {
    let database = escape_identifier(database);
    let table = escape_identifier(table);

    if column_names.is_empty() {
        return format!("SELECT * FROM {database}.{table} LIMIT {limit}");
    }

    let order_by = column_names
        .iter()
        .map(|name| escape_identifier(name))
        .collect::<Vec<_>>()
        .join(", ");

    format!("SELECT * FROM {database}.{table} ORDER BY {order_by} LIMIT {limit}")
}

fn value_to_i64(value: &Value) -> Option<i64> {
    if let Some(n) = value.as_i64() {
        return Some(n);
    }
    if let Some(n) = value.as_u64() {
        return i64::try_from(n).ok();
    }
    value.as_str().and_then(|raw| raw.parse::<i64>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;

    #[test]
    fn identifier_safety_helper() {
        assert!(is_safe_identifier("events"));
        assert!(is_safe_identifier("_tmp_1"));
        assert!(!is_safe_identifier("1events"));
        assert!(!is_safe_identifier("events;drop"));
    }

    #[test]
    fn escaping_helpers() {
        assert_eq!(escape_literal("a'b"), "a''b");
        assert_eq!(escape_identifier("ev`ents"), "`ev``ents`");
    }

    #[test]
    fn table_preview_query_orders_by_schema_columns() {
        let query = table_preview_rows_query(
            "analytics",
            "events",
            &[
                "event_ts".to_string(),
                "session_id".to_string(),
                "event_id".to_string(),
            ],
            25,
        );
        assert_eq!(
            query,
            "SELECT * FROM `analytics`.`events` ORDER BY `event_ts`, `session_id`, `event_id` LIMIT 25"
        );
    }

    #[test]
    fn table_preview_query_escapes_identifiers() {
        let query = table_preview_rows_query("ana`lytics", "ev`ents", &["co`l".to_string()], 10);
        assert_eq!(
            query,
            "SELECT * FROM `ana``lytics`.`ev``ents` ORDER BY `co``l` LIMIT 10"
        );
    }

    #[test]
    fn table_preview_query_handles_empty_schema() {
        let columns: Vec<String> = Vec::new();
        let query = table_preview_rows_query("analytics", "events", &columns, 5);
        assert_eq!(query, "SELECT * FROM `analytics`.`events` LIMIT 5");
    }

    #[test]
    fn clickhouse_ping_payload_marks_healthy_when_ping_succeeds() {
        let payload = clickhouse_ping_payload("24.8".to_string(), Ok(()), 3.5);
        assert_eq!(payload["healthy"], json!(true));
        assert_eq!(payload["version"], json!("24.8"));
        assert_eq!(payload["ping_ms"], json!(3.5));
        assert_eq!(payload["error"], Value::Null);
    }

    #[test]
    fn clickhouse_ping_payload_marks_unhealthy_when_ping_fails() {
        let payload =
            clickhouse_ping_payload("24.8".to_string(), Err(anyhow!("ping failed")), 8.25);
        assert_eq!(payload["healthy"], json!(false));
        assert_eq!(payload["version"], json!("24.8"));
        assert_eq!(payload["ping_ms"], json!(8.25));
        assert_eq!(payload["error"], json!("ping failed"));
    }
}
