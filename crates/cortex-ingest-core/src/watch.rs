use crate::{
    Metrics, WorkItem, WATCHER_BACKEND_MIXED, WATCHER_BACKEND_NATIVE, WATCHER_BACKEND_POLL,
    WATCHER_BACKEND_UNKNOWN,
};
use anyhow::{Context, Result};
use cortex_config::IngestSource;
use glob::glob;
use notify::{
    event::{EventKind, ModifyKind},
    Config as NotifyConfig, Event, PollWatcher, RecommendedWatcher, RecursiveMode, Watcher,
};
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{info, warn};

enum ActiveWatcher {
    Recommended(RecommendedWatcher),
    Poll(PollWatcher),
}

impl ActiveWatcher {
    fn watch(&mut self, path: &std::path::Path, mode: RecursiveMode) -> notify::Result<()> {
        match self {
            Self::Recommended(watcher) => watcher.watch(path, mode),
            Self::Poll(watcher) => watcher.watch(path, mode),
        }
    }
}

#[derive(Clone, Copy)]
enum WatcherBackend {
    Native,
    Poll,
}

impl WatcherBackend {
    fn state(self) -> u64 {
        match self {
            Self::Native => WATCHER_BACKEND_NATIVE,
            Self::Poll => WATCHER_BACKEND_POLL,
        }
    }
}

fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

struct WatchRegistration {
    metrics: Arc<Metrics>,
    registered: bool,
}

impl WatchRegistration {
    fn new(metrics: Arc<Metrics>) -> Self {
        Self {
            metrics,
            registered: false,
        }
    }

    fn mark_registered(&mut self) {
        if self.registered {
            return;
        }
        self.metrics
            .watcher_registrations
            .fetch_add(1, Ordering::Relaxed);
        self.registered = true;
    }
}

impl Drop for WatchRegistration {
    fn drop(&mut self) {
        if !self.registered {
            return;
        }
        self.metrics
            .watcher_registrations
            .fetch_sub(1, Ordering::Relaxed);
    }
}

fn record_backend(metrics: &Arc<Metrics>, backend: WatcherBackend) {
    let next = backend.state();
    let mut current = metrics.watcher_backend_state.load(Ordering::Relaxed);

    loop {
        let merged = match (current, next) {
            (WATCHER_BACKEND_UNKNOWN, value) => value,
            (value, next_value) if value == next_value => value,
            _ => WATCHER_BACKEND_MIXED,
        };

        match metrics.watcher_backend_state.compare_exchange(
            current,
            merged,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

fn record_watcher_error(metrics: &Arc<Metrics>, message: &str) {
    metrics.watcher_error_count.fetch_add(1, Ordering::Relaxed);
    *metrics
        .last_error
        .lock()
        .expect("metrics last_error mutex poisoned") = message.to_string();
}

fn record_rescan(metrics: &Arc<Metrics>) {
    metrics.watcher_reset_count.fetch_add(1, Ordering::Relaxed);
    metrics
        .watcher_last_reset_unix_ms
        .store(unix_ms_now(), Ordering::Relaxed);
}

fn event_requires_rescan(event: &Event) -> bool {
    event.paths.is_empty() || event.need_rescan()
}

fn event_is_relevant(kind: &EventKind) -> bool {
    match kind {
        EventKind::Any | EventKind::Create(_) => true,
        EventKind::Modify(modify_kind) => matches!(
            modify_kind,
            ModifyKind::Any | ModifyKind::Data(_) | ModifyKind::Name(_)
        ),
        _ => false,
    }
}

fn event_jsonl_paths(event: &Event) -> Vec<String> {
    let mut dedup = BTreeSet::<String>::new();
    for path in &event.paths {
        if path.extension().and_then(|s| s.to_str()) == Some("jsonl") {
            dedup.insert(path.to_string_lossy().to_string());
        }
    }
    dedup.into_iter().collect()
}

fn queue_rescan(
    glob_pattern: &str,
    source_name: &str,
    provider: &str,
    tx: &mpsc::UnboundedSender<WorkItem>,
    metrics: &Arc<Metrics>,
) {
    record_rescan(metrics);
    if let Ok(paths) = enumerate_jsonl_files(glob_pattern) {
        for path in paths {
            let _ = tx.send(WorkItem {
                source_name: source_name.to_string(),
                provider: provider.to_string(),
                path,
            });
        }
    }
}

pub(crate) fn spawn_watcher_threads(
    sources: Vec<IngestSource>,
    tx: mpsc::UnboundedSender<WorkItem>,
    metrics: Arc<Metrics>,
) -> Result<Vec<std::thread::JoinHandle<()>>> {
    let mut handles = Vec::<std::thread::JoinHandle<()>>::new();

    for source in sources {
        let source_name = source.name.clone();
        let provider = source.provider.clone();
        let glob_pattern = source.glob.clone();
        let watch_root = std::path::PathBuf::from(source.watch_root.clone());
        let tx_clone = tx.clone();
        let metrics_clone = metrics.clone();

        info!(
            "starting watcher on {} (source={}, provider={})",
            watch_root.display(),
            source_name,
            provider
        );

        let handle = std::thread::spawn(move || {
            let (event_tx, event_rx) = std::sync::mpsc::channel::<notify::Result<Event>>();
            let native_tx = event_tx.clone();
            let mut registration = WatchRegistration::new(metrics_clone.clone());

            let mut watcher = match notify::recommended_watcher(move |res| {
                let _ = native_tx.send(res);
            }) {
                Ok(watcher) => {
                    record_backend(&metrics_clone, WatcherBackend::Native);
                    info!("watcher backend native (source={})", source_name);
                    ActiveWatcher::Recommended(watcher)
                }
                Err(exc) => {
                    eprintln!(
                        "[cortex-rust] failed to create native watcher for {}: {exc}; falling back to poll watcher",
                        source_name
                    );
                    record_watcher_error(
                        &metrics_clone,
                        &format!("native watcher create failed for {}: {exc}", source_name),
                    );
                    let poll_config =
                        NotifyConfig::default().with_poll_interval(Duration::from_secs(2));
                    match PollWatcher::new(
                        move |res| {
                            let _ = event_tx.send(res);
                        },
                        poll_config,
                    ) {
                        Ok(watcher) => {
                            record_backend(&metrics_clone, WatcherBackend::Poll);
                            info!("watcher backend poll (source={})", source_name);
                            ActiveWatcher::Poll(watcher)
                        }
                        Err(poll_exc) => {
                            eprintln!(
                                "[cortex-rust] failed to create poll watcher for {}: {poll_exc}",
                                source_name
                            );
                            record_watcher_error(
                                &metrics_clone,
                                &format!(
                                    "poll watcher create failed for {}: {poll_exc}",
                                    source_name
                                ),
                            );
                            queue_rescan(
                                &glob_pattern,
                                &source_name,
                                &provider,
                                &tx_clone,
                                &metrics_clone,
                            );
                            return;
                        }
                    }
                }
            };

            if let Err(exc) = watcher.watch(watch_root.as_path(), RecursiveMode::Recursive) {
                eprintln!(
                    "[cortex-rust] failed to watch {} ({}): {exc}",
                    watch_root.display(),
                    source_name
                );
                record_watcher_error(
                    &metrics_clone,
                    &format!(
                        "watch root register failed for {}:{}: {exc}",
                        source_name,
                        watch_root.display()
                    ),
                );
                queue_rescan(
                    &glob_pattern,
                    &source_name,
                    &provider,
                    &tx_clone,
                    &metrics_clone,
                );
                return;
            }
            registration.mark_registered();

            loop {
                match event_rx.recv() {
                    Ok(Ok(event)) => {
                        if event_requires_rescan(&event) {
                            queue_rescan(
                                &glob_pattern,
                                &source_name,
                                &provider,
                                &tx_clone,
                                &metrics_clone,
                            );
                            continue;
                        }

                        if !event_is_relevant(&event.kind) {
                            continue;
                        }

                        for path in event_jsonl_paths(&event) {
                            let _ = tx_clone.send(WorkItem {
                                source_name: source_name.clone(),
                                provider: provider.clone(),
                                path,
                            });
                        }
                    }
                    Ok(Err(exc)) => {
                        eprintln!("[cortex-rust] watcher event error ({source_name}): {exc}");
                        record_watcher_error(
                            &metrics_clone,
                            &format!("watcher event error ({source_name}): {exc}"),
                        );
                        queue_rescan(
                            &glob_pattern,
                            &source_name,
                            &provider,
                            &tx_clone,
                            &metrics_clone,
                        );
                    }
                    Err(_) => break,
                }
            }
        });

        handles.push(handle);
    }

    Ok(handles)
}

pub(crate) fn enumerate_jsonl_files(glob_pattern: &str) -> Result<Vec<String>> {
    let mut files = Vec::<String>::new();
    for entry in glob(glob_pattern).with_context(|| format!("invalid glob: {}", glob_pattern))? {
        let path = match entry {
            Ok(path) => path,
            Err(exc) => {
                warn!("glob iteration error: {exc}");
                continue;
            }
        };

        if path.extension().and_then(|s| s.to_str()) == Some("jsonl") {
            files.push(path.to_string_lossy().to_string());
        }
    }
    files.sort();
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Metrics;
    use notify::{
        event::{CreateKind, DataChange, Flag, ModifyKind, RenameMode},
        EventKind,
    };
    use std::path::PathBuf;
    use std::sync::atomic::Ordering;

    #[test]
    fn rescan_events_require_reconcile() {
        let event = Event::new(EventKind::Other).set_flag(Flag::Rescan);
        assert!(event_requires_rescan(&event));
    }

    #[test]
    fn relevant_event_kinds_include_create_modify_data_and_rename() {
        assert!(event_is_relevant(&EventKind::Create(CreateKind::Any)));
        assert!(event_is_relevant(&EventKind::Modify(ModifyKind::Data(
            DataChange::Any
        ))));
        assert!(event_is_relevant(&EventKind::Modify(ModifyKind::Name(
            RenameMode::Any
        ))));
        assert!(!event_is_relevant(&EventKind::Remove(
            notify::event::RemoveKind::Any
        )));
    }

    #[test]
    fn jsonl_paths_are_deduped_and_filtered() {
        let mut event = Event::new(EventKind::Modify(ModifyKind::Data(DataChange::Any)));
        event.paths = vec![
            PathBuf::from("/tmp/a.jsonl"),
            PathBuf::from("/tmp/a.jsonl"),
            PathBuf::from("/tmp/b.txt"),
        ];

        let paths = event_jsonl_paths(&event);
        assert_eq!(paths, vec!["/tmp/a.jsonl".to_string()]);
    }

    #[test]
    fn watcher_registration_tracks_active_watches() {
        let metrics = Arc::new(Metrics::default());
        assert_eq!(metrics.watcher_registrations.load(Ordering::Relaxed), 0);

        {
            let mut registration = WatchRegistration::new(metrics.clone());
            registration.mark_registered();
            assert_eq!(metrics.watcher_registrations.load(Ordering::Relaxed), 1);
        }

        assert_eq!(metrics.watcher_registrations.load(Ordering::Relaxed), 0);
    }
}
