use crate::WorkItem;
use anyhow::{Context, Result};
use cortex_config::IngestSource;
use glob::glob;
use notify::{
    Config as NotifyConfig, Event, PollWatcher, RecommendedWatcher, RecursiveMode, Watcher,
};
use std::time::Duration;
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

fn queue_rescan(
    glob_pattern: &str,
    source_name: &str,
    provider: &str,
    tx: &mpsc::UnboundedSender<WorkItem>,
) {
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
) -> Result<Vec<std::thread::JoinHandle<()>>> {
    let mut handles = Vec::<std::thread::JoinHandle<()>>::new();

    for source in sources {
        let source_name = source.name.clone();
        let provider = source.provider.clone();
        let glob_pattern = source.glob.clone();
        let watch_root = std::path::PathBuf::from(source.watch_root.clone());
        let tx_clone = tx.clone();

        info!(
            "starting watcher on {} (source={}, provider={})",
            watch_root.display(),
            source_name,
            provider
        );

        let handle = std::thread::spawn(move || {
            let (event_tx, event_rx) = std::sync::mpsc::channel::<notify::Result<Event>>();
            let native_tx = event_tx.clone();

            let mut watcher = match notify::recommended_watcher(move |res| {
                let _ = native_tx.send(res);
            }) {
                Ok(watcher) => ActiveWatcher::Recommended(watcher),
                Err(exc) => {
                    eprintln!(
                        "[cortex-rust] failed to create native watcher for {}: {exc}; falling back to poll watcher",
                        source_name
                    );
                    let poll_config =
                        NotifyConfig::default().with_poll_interval(Duration::from_secs(2));
                    match PollWatcher::new(
                        move |res| {
                            let _ = event_tx.send(res);
                        },
                        poll_config,
                    ) {
                        Ok(watcher) => ActiveWatcher::Poll(watcher),
                        Err(poll_exc) => {
                            eprintln!(
                                "[cortex-rust] failed to create poll watcher for {}: {poll_exc}",
                                source_name
                            );
                            queue_rescan(&glob_pattern, &source_name, &provider, &tx_clone);
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
                queue_rescan(&glob_pattern, &source_name, &provider, &tx_clone);
                return;
            }

            loop {
                match event_rx.recv() {
                    Ok(Ok(event)) => {
                        let needs_rescan = event.paths.is_empty()
                            || format!("{:?}", event.kind).contains("Rescan");
                        if needs_rescan {
                            queue_rescan(&glob_pattern, &source_name, &provider, &tx_clone);
                            continue;
                        }

                        for path in event.paths {
                            let _ = tx_clone.send(WorkItem {
                                source_name: source_name.clone(),
                                provider: provider.clone(),
                                path: path.to_string_lossy().to_string(),
                            });
                        }
                    }
                    Ok(Err(exc)) => {
                        eprintln!("[cortex-rust] watcher event error ({source_name}): {exc}");
                        queue_rescan(&glob_pattern, &source_name, &provider, &tx_clone);
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
