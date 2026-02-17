use crate::dispatch::enqueue_work;
use crate::watch::enumerate_jsonl_files;
use crate::{DispatchState, Metrics, WorkItem};
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
                match enumerate_jsonl_files(&source.glob) {
                    Ok(paths) => {
                        debug!(
                            "reconcile scanning {} files for source={}",
                            paths.len(),
                            source.name
                        );
                        for path in paths {
                            enqueue_work(
                                WorkItem {
                                    source_name: source.name.clone(),
                                    provider: source.provider.clone(),
                                    path,
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
