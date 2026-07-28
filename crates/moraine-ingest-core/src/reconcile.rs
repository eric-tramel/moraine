use crate::dispatch::enqueue_work;
use crate::watch::enumerate_tracked_files;
use crate::{DispatchState, Metrics, WorkItem, WorkTrigger};
use moraine_config::{AppConfig, IngestSource};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

pub(crate) fn spawn_reconcile_task(
    config: AppConfig,
    sources: Vec<IngestSource>,
    process_tx: mpsc::Sender<WorkItem>,
    dispatch: Arc<Mutex<DispatchState>>,
    metrics: Arc<Metrics>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let interval = Duration::from_secs_f64(config.ingest.reconcile_interval_seconds.max(5.0));
        let mut ticker = tokio::time::interval(interval);

        loop {
            ticker.tick().await;
            for source in &sources {
                match enumerate_tracked_files(&source.glob, source.format) {
                    Ok(paths) => {
                        debug!(
                            "reconcile scanning {} files for source={} (format={})",
                            paths.len(),
                            source.name,
                            source.format
                        );
                        for path in paths {
                            enqueue_work(
                                WorkItem {
                                    source_name: source.name.clone(),
                                    harness: source.harness.clone(),
                                    format: source.format,
                                    source_glob: source.glob.clone(),
                                    path,
                                    // The only trigger a reconciliation sweep
                                    // slice may ever attach to (§2.2, §2.4).
                                    trigger: WorkTrigger::Reconcile,
                                },
                                &process_tx,
                                &dispatch,
                                &metrics,
                            )
                            .await;
                        }
                    }
                    Err(exc) => {
                        warn!("reconcile scan failed for source={}: {exc}", source.name);
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::spawn_reconcile_task;
    use crate::{DispatchState, Metrics, WorkTrigger};
    use moraine_config::{AppConfig, IngestSource, SourceFormat};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tokio::sync::mpsc;

    /// Issue #601 §2.4 / WI-03. A reconciliation sweep slice may attach only to
    /// a reconcile-driven poll, so the reconcile tick has to be the one place
    /// that stamps `WorkTrigger::Reconcile`. Nothing else may.
    ///
    /// Fails for: reverting `spawn_reconcile_task` to the default trigger.
    #[tokio::test]
    async fn reconcile_ticks_are_stamped_as_reconcile_triggered() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before unix epoch")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("moraine-reconcile-trigger-{nonce}"));
        std::fs::create_dir_all(&dir).expect("create reconcile fixture dir");
        let file = dir.join("session.jsonl");
        std::fs::write(&file, b"{}\n").expect("write reconcile fixture file");

        let source = IngestSource {
            name: "reconcile-trigger-test".to_string(),
            harness: "codex".to_string(),
            format: SourceFormat::Jsonl,
            glob: dir.join("*.jsonl").to_string_lossy().to_string(),
            enabled: true,
            watch_root: dir.to_string_lossy().to_string(),
        };
        let (process_tx, mut process_rx) = mpsc::channel(8);
        let task = spawn_reconcile_task(
            AppConfig::default(),
            vec![source],
            process_tx,
            Arc::new(Mutex::new(DispatchState::default())),
            Arc::new(Metrics::default()),
        );

        // `tokio::time::interval` fires its first tick immediately, so this
        // does not wait out `reconcile_interval_seconds`.
        let queued = tokio::time::timeout(Duration::from_secs(5), process_rx.recv())
            .await
            .expect("reconcile must enqueue on its first tick")
            .expect("reconcile channel stays open");
        assert_eq!(queued.trigger, WorkTrigger::Reconcile);
        task.abort();

        std::fs::remove_dir_all(&dir).ok();
    }
}
