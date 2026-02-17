use crate::checkpoint::{checkpoint_key, merge_checkpoint};
use crate::heartbeat::host_name;
use crate::model::Checkpoint;
use crate::{
    DispatchState, Metrics, SinkMessage, WATCHER_BACKEND_MIXED, WATCHER_BACKEND_NATIVE,
    WATCHER_BACKEND_POLL,
};
use chrono::{DateTime, Utc};
use moraine_clickhouse::ClickHouseClient;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;
use tracing::{info, warn};

fn watcher_backend_label(value: u64) -> &'static str {
    match value {
        WATCHER_BACKEND_NATIVE => "native",
        WATCHER_BACKEND_POLL => "poll",
        WATCHER_BACKEND_MIXED => "mixed",
        _ => "unknown",
    }
}

fn saturating_u64_to_u32(value: u64) -> u32 {
    value.min(u32::MAX as u64) as u32
}

fn append_to_visible_percentile(sorted_latencies_ms: &[u64], quantile: f64) -> u64 {
    debug_assert!(!sorted_latencies_ms.is_empty());
    let rank = ((sorted_latencies_ms.len() as f64) * quantile).ceil() as usize;
    sorted_latencies_ms[rank.saturating_sub(1).min(sorted_latencies_ms.len() - 1)]
}

fn compute_append_to_visible_stats(
    raw_rows: &[Value],
    visible_at: DateTime<Utc>,
) -> Option<(u32, u32)> {
    let mut latencies_ms: Vec<u64> = raw_rows
        .iter()
        .filter_map(|row| row.get("record_ts").and_then(Value::as_str))
        .filter_map(|record_ts| DateTime::parse_from_rfc3339(record_ts).ok())
        .map(|record_ts| {
            visible_at
                .signed_duration_since(record_ts.with_timezone(&Utc))
                .num_milliseconds()
                .max(0) as u64
        })
        .collect();

    if latencies_ms.is_empty() {
        return None;
    }

    latencies_ms.sort_unstable();
    let p50 = append_to_visible_percentile(&latencies_ms, 0.50);
    let p95 = append_to_visible_percentile(&latencies_ms, 0.95);
    Some((saturating_u64_to_u32(p50), saturating_u64_to_u32(p95)))
}

pub(crate) fn spawn_sink_task(
    config: moraine_config::AppConfig,
    clickhouse: ClickHouseClient,
    checkpoints: Arc<RwLock<HashMap<String, Checkpoint>>>,
    metrics: Arc<Metrics>,
    mut rx: mpsc::Receiver<SinkMessage>,
    dispatch: Arc<Mutex<DispatchState>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut raw_rows = Vec::<Value>::new();
        let mut event_rows = Vec::<Value>::new();
        let mut link_rows = Vec::<Value>::new();
        let mut tool_rows = Vec::<Value>::new();
        let mut error_rows = Vec::<Value>::new();
        let mut checkpoint_updates = HashMap::<String, Checkpoint>::new();

        let flush_interval =
            Duration::from_secs_f64(config.ingest.flush_interval_seconds.max(0.05));
        let heartbeat_interval =
            Duration::from_secs_f64(config.ingest.heartbeat_interval_seconds.max(1.0));
        let retry_backoff =
            Duration::from_secs_f64((config.ingest.flush_interval_seconds * 2.0).max(0.25));

        let mut flush_tick = tokio::time::interval(flush_interval);
        let mut heartbeat_tick = tokio::time::interval(heartbeat_interval);
        let mut throttling_flush_retries = false;

        loop {
            if throttling_flush_retries
                && has_pending_data(
                    &raw_rows,
                    &event_rows,
                    &link_rows,
                    &tool_rows,
                    &error_rows,
                    &checkpoint_updates,
                )
            {
                if flush_pending(
                    &clickhouse,
                    &checkpoints,
                    &metrics,
                    &mut raw_rows,
                    &mut event_rows,
                    &mut link_rows,
                    &mut tool_rows,
                    &mut error_rows,
                    &mut checkpoint_updates,
                )
                .await
                {
                    throttling_flush_retries = false;
                    info!("flush retry succeeded; resuming sink intake");
                } else {
                    tokio::select! {
                        _ = tokio::time::sleep(retry_backoff) => {}
                        _ = heartbeat_tick.tick() => {
                            emit_heartbeat(&clickhouse, &metrics, &dispatch).await;
                        }
                    }
                }
                continue;
            }

            tokio::select! {
                maybe_msg = rx.recv() => {
                    match maybe_msg {
                        Some(SinkMessage::Batch(batch)) => {
                            raw_rows.extend(batch.raw_rows);
                            event_rows.extend(batch.event_rows);
                            link_rows.extend(batch.link_rows);
                            tool_rows.extend(batch.tool_rows);
                            error_rows.extend(batch.error_rows);
                            if let Some(cp) = batch.checkpoint {
                                merge_checkpoint(&mut checkpoint_updates, cp);
                            }

                            let total_rows = raw_rows.len() + event_rows.len() + link_rows.len() + tool_rows.len() + error_rows.len();
                            if total_rows >= config.ingest.batch_size {
                                if !flush_pending(
                                    &clickhouse,
                                    &checkpoints,
                                    &metrics,
                                    &mut raw_rows,
                                    &mut event_rows,
                                    &mut link_rows,
                                    &mut tool_rows,
                                    &mut error_rows,
                                    &mut checkpoint_updates,
                                ).await {
                                    if !throttling_flush_retries {
                                        warn!(
                                            "flush failed; pausing sink intake and retrying pending rows every {} ms",
                                            retry_backoff.as_millis()
                                        );
                                    }
                                    throttling_flush_retries = true;
                                }
                            }
                        }
                        None => break,
                    }
                }
                _ = flush_tick.tick() => {
                    if has_pending_data(&raw_rows, &event_rows, &link_rows, &tool_rows, &error_rows, &checkpoint_updates) {
                        if !flush_pending(
                            &clickhouse,
                            &checkpoints,
                            &metrics,
                            &mut raw_rows,
                            &mut event_rows,
                            &mut link_rows,
                            &mut tool_rows,
                            &mut error_rows,
                            &mut checkpoint_updates,
                        ).await {
                            if !throttling_flush_retries {
                                warn!(
                                    "flush failed; pausing sink intake and retrying pending rows every {} ms",
                                    retry_backoff.as_millis()
                                );
                            }
                            throttling_flush_retries = true;
                        }
                    }
                }
                _ = heartbeat_tick.tick() => {
                    emit_heartbeat(&clickhouse, &metrics, &dispatch).await;
                }
            }
        }

        if has_pending_data(
            &raw_rows,
            &event_rows,
            &link_rows,
            &tool_rows,
            &error_rows,
            &checkpoint_updates,
        ) {
            flush_pending(
                &clickhouse,
                &checkpoints,
                &metrics,
                &mut raw_rows,
                &mut event_rows,
                &mut link_rows,
                &mut tool_rows,
                &mut error_rows,
                &mut checkpoint_updates,
            )
            .await;
        }
    })
}

fn has_pending_data(
    raw_rows: &[Value],
    event_rows: &[Value],
    link_rows: &[Value],
    tool_rows: &[Value],
    error_rows: &[Value],
    checkpoint_updates: &HashMap<String, Checkpoint>,
) -> bool {
    !(raw_rows.is_empty()
        && event_rows.is_empty()
        && link_rows.is_empty()
        && tool_rows.is_empty()
        && error_rows.is_empty()
        && checkpoint_updates.is_empty())
}

async fn emit_heartbeat(
    clickhouse: &ClickHouseClient,
    metrics: &Arc<Metrics>,
    dispatch: &Arc<Mutex<DispatchState>>,
) {
    let files_active = {
        let state = dispatch.lock().expect("dispatch mutex poisoned");
        state.inflight.len() as u32
    };
    let files_watched = metrics.watcher_registrations.load(Ordering::Relaxed) as u32;
    let last_error = {
        metrics
            .last_error
            .lock()
            .expect("metrics last_error mutex poisoned")
            .clone()
    };
    let watcher_backend =
        watcher_backend_label(metrics.watcher_backend_state.load(Ordering::Relaxed));

    let heartbeat = json!({
        "host": host_name(),
        "service_version": env!("CARGO_PKG_VERSION"),
        "queue_depth": metrics.queue_depth.load(Ordering::Relaxed),
        "files_active": files_active,
        "files_watched": files_watched,
        "rows_raw_written": metrics.raw_rows_written.load(Ordering::Relaxed),
        "rows_events_written": metrics.event_rows_written.load(Ordering::Relaxed),
        "rows_errors_written": metrics.err_rows_written.load(Ordering::Relaxed),
        "flush_latency_ms": saturating_u64_to_u32(metrics.last_flush_ms.load(Ordering::Relaxed)),
        "append_to_visible_p50_ms": saturating_u64_to_u32(metrics.append_to_visible_p50_ms.load(Ordering::Relaxed)),
        "append_to_visible_p95_ms": saturating_u64_to_u32(metrics.append_to_visible_p95_ms.load(Ordering::Relaxed)),
        "watcher_backend": watcher_backend,
        "watcher_error_count": metrics.watcher_error_count.load(Ordering::Relaxed),
        "watcher_reset_count": metrics.watcher_reset_count.load(Ordering::Relaxed),
        "watcher_last_reset_unix_ms": metrics.watcher_last_reset_unix_ms.load(Ordering::Relaxed),
        "last_error": last_error,
    });

    if let Err(exc) = clickhouse
        .insert_json_rows("ingest_heartbeats", &[heartbeat])
        .await
    {
        warn!("heartbeat insert failed: {exc}");
    }
}

async fn flush_pending(
    clickhouse: &ClickHouseClient,
    checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
    metrics: &Arc<Metrics>,
    raw_rows: &mut Vec<Value>,
    event_rows: &mut Vec<Value>,
    link_rows: &mut Vec<Value>,
    tool_rows: &mut Vec<Value>,
    error_rows: &mut Vec<Value>,
    checkpoint_updates: &mut HashMap<String, Checkpoint>,
) -> bool {
    let started = Instant::now();

    let checkpoint_rows: Vec<Value> = checkpoint_updates
        .values()
        .map(|cp| {
            json!({
                "source_name": cp.source_name,
                "source_file": cp.source_file,
                "source_inode": cp.source_inode,
                "source_generation": cp.source_generation,
                "last_offset": cp.last_offset,
                "last_line_no": cp.last_line_no,
                "status": cp.status,
            })
        })
        .collect();

    let flush_result = async {
        clickhouse.insert_json_rows("raw_events", raw_rows).await?;
        clickhouse.insert_json_rows("events", event_rows).await?;
        clickhouse
            .insert_json_rows("event_links", link_rows)
            .await?;
        clickhouse.insert_json_rows("tool_io", tool_rows).await?;
        clickhouse
            .insert_json_rows("ingest_errors", error_rows)
            .await?;
        clickhouse
            .insert_json_rows("ingest_checkpoints", &checkpoint_rows)
            .await?;
        anyhow::Result::<()>::Ok(())
    }
    .await;

    match flush_result {
        Ok(()) => {
            metrics
                .raw_rows_written
                .fetch_add(raw_rows.len() as u64, Ordering::Relaxed);
            metrics
                .event_rows_written
                .fetch_add(event_rows.len() as u64, Ordering::Relaxed);
            metrics
                .err_rows_written
                .fetch_add(error_rows.len() as u64, Ordering::Relaxed);
            metrics
                .last_flush_ms
                .store(started.elapsed().as_millis() as u64, Ordering::Relaxed);
            if let Some((p50_ms, p95_ms)) = compute_append_to_visible_stats(raw_rows, Utc::now()) {
                metrics
                    .append_to_visible_p50_ms
                    .store(p50_ms as u64, Ordering::Relaxed);
                metrics
                    .append_to_visible_p95_ms
                    .store(p95_ms as u64, Ordering::Relaxed);
            }

            {
                let mut state = checkpoints.write().await;
                for cp in checkpoint_updates.values() {
                    let key = checkpoint_key(&cp.source_name, &cp.source_file);
                    state.insert(key, cp.clone());
                }
            }

            raw_rows.clear();
            event_rows.clear();
            link_rows.clear();
            tool_rows.clear();
            error_rows.clear();
            checkpoint_updates.clear();
            true
        }
        Err(exc) => {
            metrics.flush_failures.fetch_add(1, Ordering::Relaxed);
            *metrics
                .last_error
                .lock()
                .expect("metrics last_error mutex poisoned") = exc.to_string();
            warn!("flush failed: {exc}");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::RowBatch;
    use chrono::{DateTime, Utc};
    use serde_json::json;
    use tokio::time::timeout;

    fn single_row_batch(id: u64) -> SinkMessage {
        let mut batch = RowBatch::default();
        batch.raw_rows.push(json!({ "id": id }));
        SinkMessage::Batch(batch)
    }

    #[tokio::test]
    async fn failed_flush_throttles_sink_consumption() {
        let mut config = moraine_config::AppConfig::default();
        config.clickhouse.url = "http://127.0.0.1:1".to_string();
        config.clickhouse.timeout_seconds = 1.0;
        config.ingest.batch_size = 1;
        config.ingest.flush_interval_seconds = 0.05;
        config.ingest.heartbeat_interval_seconds = 60.0;

        let clickhouse = ClickHouseClient::new(config.clickhouse.clone())
            .expect("clickhouse client should initialize");
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());
        let dispatch = Arc::new(Mutex::new(DispatchState::default()));
        let (tx, rx) = mpsc::channel(1);

        let handle = spawn_sink_task(config, clickhouse, checkpoints, metrics, rx, dispatch);

        tx.send(single_row_batch(1))
            .await
            .expect("first send should succeed");
        tx.send(single_row_batch(2))
            .await
            .expect("second send should succeed");

        let third_send = timeout(Duration::from_millis(350), tx.send(single_row_batch(3))).await;
        assert!(
            third_send.is_err(),
            "third send should block while sink retries failed flushes"
        );

        handle.abort();
    }

    #[test]
    fn compute_append_to_visible_stats_uses_real_record_timestamps() {
        let visible_at = DateTime::parse_from_rfc3339("2026-02-17T00:00:10.000Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let raw_rows = vec![
            json!({"record_ts": "2026-02-17T00:00:00.000Z"}),
            json!({"record_ts": "2026-02-17T00:00:05.000Z"}),
            json!({"record_ts": "2026-02-17T00:00:09.000Z"}),
        ];

        let (p50, p95) = compute_append_to_visible_stats(&raw_rows, visible_at)
            .expect("expected percentile stats");

        assert_eq!(p50, 5_000);
        assert_eq!(p95, 10_000);
    }

    #[test]
    fn compute_append_to_visible_stats_returns_none_for_unparseable_rows() {
        let visible_at = DateTime::parse_from_rfc3339("2026-02-17T00:00:10.000Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let raw_rows = vec![
            json!({"record_ts": "not-a-timestamp"}),
            json!({"record_ts": ""}),
            json!({}),
        ];

        let stats = compute_append_to_visible_stats(&raw_rows, visible_at);
        assert!(stats.is_none());
    }

    #[test]
    fn compute_append_to_visible_stats_clamps_future_timestamps_to_zero() {
        let visible_at = DateTime::parse_from_rfc3339("2026-02-17T00:00:10.000Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let raw_rows = vec![json!({"record_ts": "2026-02-17T00:00:20.000Z"})];

        let (p50, p95) = compute_append_to_visible_stats(&raw_rows, visible_at)
            .expect("expected percentile stats");

        assert_eq!(p50, 0);
        assert_eq!(p95, 0);
    }
}
