use crate::{
    Metrics, WorkItem, WATCHER_BACKEND_MIXED, WATCHER_BACKEND_NATIVE, WATCHER_BACKEND_POLL,
    WATCHER_BACKEND_UNKNOWN,
};
use anyhow::{Context, Result};
use glob::glob;
use moraine_config::{map_tracked_path, IngestSource, SourceFormat};
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

/// Canonical tracked paths touched by a watcher event. Sidecar writes (e.g.
/// SQLite `-wal`/`-shm`) map to their canonical database path, and the
/// `BTreeSet` coalesces a burst touching base + sidecars into one work item.
fn event_tracked_paths(event: &Event, format: SourceFormat, source_glob: &str) -> Vec<String> {
    let mut dedup = BTreeSet::<String>::new();
    for path in &event.paths {
        if let Some(tracked) = map_tracked_path(format, source_glob, &path.to_string_lossy()) {
            dedup.insert(tracked_path_identity(format, &tracked));
        }
    }
    dedup.into_iter().collect()
}

/// Resolves symlinks for SQLite-polled sources so every ingestion entry point
/// agrees on one path per database. Backfill/reconcile paths come from the
/// config glob while watcher events report the symlink-resolved location
/// (macOS FSEvents turns `/var/...` into `/private/var/...`); without
/// canonicalization the same database gets two checkpoint keys and two sets
/// of event UIDs.
///
/// File-backed formats keep the as-reported path: checkpoint keys and event
/// UIDs embed `source_file`, so changing the identity of long-standing
/// sources (e.g. dotfiles-managed symlinked session dirs) would orphan every
/// existing checkpoint and re-ingest history under new UIDs. SQLite-polled
/// sources are new enough that they can share this stricter identity rule.
fn tracked_path_identity(format: SourceFormat, path: &str) -> String {
    if !matches!(
        format,
        SourceFormat::CursorSqlite | SourceFormat::OpenCodeSqlite
    ) {
        return path.to_string();
    }
    std::fs::canonicalize(path)
        .map(|resolved| resolved.to_string_lossy().to_string())
        .unwrap_or_else(|_| path.to_string())
}

/// Queues one poll per tracked path a watcher event touched.
///
/// This is the path **100 % of real filesystem events take**; `queue_rescan`
/// only covers watcher *recovery*. Both funnel through `WorkItem::watcher`, so
/// the crate holds exactly one `WorkTrigger::Watcher` literal and
/// `watcher_work_items_are_stamped_as_watcher_triggered` guards both paths.
/// Extracting this out of the receive loop is what makes the primary path
/// reachable from a test at all — inline in the loop it could only be exercised
/// through a live `notify` backend.
fn queue_watcher_event(
    event: &Event,
    glob_pattern: &str,
    source_name: &str,
    harness: &str,
    format: SourceFormat,
    tx: &mpsc::UnboundedSender<WorkItem>,
) {
    for path in event_tracked_paths(event, format, glob_pattern) {
        let _ = tx.send(WorkItem::watcher(
            source_name,
            harness,
            format,
            glob_pattern,
            path,
        ));
    }
}

fn queue_rescan(
    glob_pattern: &str,
    source_name: &str,
    harness: &str,
    format: SourceFormat,
    tx: &mpsc::UnboundedSender<WorkItem>,
    metrics: &Arc<Metrics>,
) {
    record_rescan(metrics);
    match enumerate_tracked_files(glob_pattern, format) {
        Ok(paths) => {
            for path in paths {
                // A rescan is watcher-driven recovery, not reconciliation.
                let _ = tx.send(WorkItem::watcher(
                    source_name,
                    harness,
                    format,
                    glob_pattern,
                    path,
                ));
            }
        }
        Err(exc) => {
            warn!(
                source = source_name,
                harness,
                format = %format.as_str(),
                glob_pattern,
                error = %exc,
                "watcher rescan failed to enumerate tracked files"
            );
            record_watcher_error(
                metrics,
                &format!(
                    "rescan enumerate failed for source={source_name} harness={harness} format={format} glob={glob_pattern}: {exc}"
                ),
            );
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
        let harness = source.harness.clone();
        let format = source.format;
        let glob_pattern = source.glob.clone();
        let watch_root = std::path::PathBuf::from(source.watch_root.clone());
        let tx_clone = tx.clone();
        let metrics_clone = metrics.clone();

        info!(
            "starting watcher on {} (source={}, harness={}, format={})",
            watch_root.display(),
            source_name,
            harness,
            format
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
                        "[moraine-rust] failed to create native watcher for {}: {exc}; falling back to poll watcher",
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
                                "[moraine-rust] failed to create poll watcher for {}: {poll_exc}",
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
                                &harness,
                                format,
                                &tx_clone,
                                &metrics_clone,
                            );
                            return;
                        }
                    }
                }
            };

            let recursive_mode = if format.uses_recursive_watch() {
                RecursiveMode::Recursive
            } else {
                RecursiveMode::NonRecursive
            };
            if let Err(exc) = watcher.watch(watch_root.as_path(), recursive_mode) {
                eprintln!(
                    "[moraine-rust] failed to watch {} ({}): {exc}",
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
                    &harness,
                    format,
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
                                &harness,
                                format,
                                &tx_clone,
                                &metrics_clone,
                            );
                            continue;
                        }

                        if !event_is_relevant(&event.kind) {
                            continue;
                        }

                        queue_watcher_event(
                            &event,
                            &glob_pattern,
                            &source_name,
                            &harness,
                            format,
                            &tx_clone,
                        );
                    }
                    Ok(Err(exc)) => {
                        eprintln!("[moraine-rust] watcher event error ({source_name}): {exc}");
                        record_watcher_error(
                            &metrics_clone,
                            &format!("watcher event error ({source_name}): {exc}"),
                        );
                        queue_rescan(
                            &glob_pattern,
                            &source_name,
                            &harness,
                            format,
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

pub(crate) fn enumerate_tracked_files(
    glob_pattern: &str,
    format: SourceFormat,
) -> Result<Vec<String>> {
    let mut files = Vec::<String>::new();
    for entry in glob(glob_pattern).with_context(|| format!("invalid glob: {}", glob_pattern))? {
        let path = match entry {
            Ok(path) => path,
            Err(exc) => {
                warn!("glob iteration error: {exc}");
                continue;
            }
        };

        // Enumeration keeps canonical files only: a glob that happens to
        // match a sidecar must not produce a duplicate work item.
        let lossy = path.to_string_lossy();
        if map_tracked_path(format, glob_pattern, &lossy).as_deref() == Some(lossy.as_ref()) {
            files.push(tracked_path_identity(format, &lossy));
        }
    }
    files.sort();
    Ok(files)
}

/// Enumerate a source's tracked files as one-shot **startup** polls.
///
/// This is the crate's only path from an `IngestSource` to a
/// `WorkTrigger::Startup` item, and all three producers go through it: the
/// startup backfill in `run_ingestor`, the tee router's backend-discovery
/// probe, and the tee router's targeted replay pass. Each of those used to
/// enumerate and then stamp inline, which left the *stamp* guarded and the
/// *choice of stamp* unguarded: `startup_work_items_are_stamped_as_startup_triggered`
/// pinned `WorkItem::startup` itself, so all three call sites could be
/// repointed at `WorkItem::watcher` with the whole suite green. That matters
/// most at the backfill site, where the item reaches `enqueue_work` and a
/// `Watcher` stamp is exactly what `arm_owed_reconcile`'s `Startup` refusal
/// (plan §7.1 D1c) exists to prevent — a startup poll made sweep-eligible by an
/// owed reconcile tick.
///
/// Same rule, and the same reason, as `queue_watcher_event`: fold the
/// enumeration and the stamp into one function so the crate holds one literal
/// per trigger and one test can hold it.
pub(crate) fn enumerate_startup_work_items(source: &IngestSource) -> Result<Vec<WorkItem>> {
    Ok(enumerate_tracked_files(&source.glob, source.format)?
        .into_iter()
        .map(|path| {
            WorkItem::startup(
                &source.name,
                &source.harness,
                source.format,
                &source.glob,
                path,
            )
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Metrics, WorkTrigger};
    use notify::{
        event::{CreateKind, DataChange, Flag, ModifyKind, RenameMode},
        EventKind,
    };
    use std::path::PathBuf;
    use std::sync::atomic::Ordering;
    use tokio::sync::mpsc;

    /// Issue #601 §2.4 / WI-03. A reconciliation sweep must never attach to a
    /// watcher-driven poll, so **both** watcher entry points have to stamp
    /// `Watcher`:
    ///
    /// - `queue_watcher_event` — the per-event fan-out that 100 % of real
    ///   filesystem events take;
    /// - `queue_rescan` — watcher *recovery* only (registration failure, event
    ///   error, or a backend rescan flag).
    ///
    /// The earlier version of this test covered the recovery path alone, so
    /// the stamp on the primary path was unguarded and could be flipped to
    /// `Reconcile` with the whole suite still green. Both now funnel through
    /// `WorkItem::watcher`.
    ///
    /// Fails for: stamping either path with anything but `Watcher`.
    #[test]
    fn watcher_work_items_are_stamped_as_watcher_triggered() {
        let nonce = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before unix epoch")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("moraine-watch-trigger-{nonce}"));
        std::fs::create_dir_all(&dir).expect("create watcher fixture dir");
        let file = dir.join("session.jsonl");
        std::fs::write(&file, b"{}\n").expect("write watcher fixture file");
        let glob_pattern = dir.join("*.jsonl").to_string_lossy().to_string();

        // The primary path: a data-change event naming the tracked file, the
        // exact shape `spawn_watcher_threads` forwards on every write.
        let (tx, mut rx) = mpsc::unbounded_channel::<WorkItem>();
        let event =
            Event::new(EventKind::Modify(ModifyKind::Data(DataChange::Any))).add_path(file.clone());
        assert!(
            event_is_relevant(&event.kind) && !event_requires_rescan(&event),
            "the fixture event must reach the per-event fan-out, or this guard \
             cannot fail"
        );
        queue_watcher_event(
            &event,
            &glob_pattern,
            "watch-trigger-test",
            "codex",
            SourceFormat::Jsonl,
            &tx,
        );
        let queued = rx
            .try_recv()
            .expect("a watcher event must enqueue the tracked file");
        assert_eq!(queued.trigger, WorkTrigger::Watcher);
        assert_eq!(queued.path, file.to_string_lossy());
        assert!(rx.try_recv().is_err(), "one event, one queued poll");

        // The recovery path.
        let (tx, mut rx) = mpsc::unbounded_channel::<WorkItem>();
        queue_rescan(
            &glob_pattern,
            "watch-trigger-test",
            "codex",
            SourceFormat::Jsonl,
            &tx,
            &Arc::new(Metrics::default()),
        );

        let queued = rx.try_recv().expect("rescan must enqueue the tracked file");
        assert_eq!(queued.trigger, WorkTrigger::Watcher);

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Issue #601 §2.4 / WI-03. The startup backfill, the tee router's
    /// backend-discovery probe and its replay pass all queue one-shot polls
    /// that must not pay sweep cost — `Startup` is documented as never
    /// sweep-eligible and nothing enforced that.
    ///
    /// An earlier revision of this test asserted on `WorkItem::startup`
    /// directly, which pins the **constructor** and not which constructor each
    /// site calls: all three sites were swappable to `WorkItem::watcher` with
    /// the whole suite green. It now drives `enumerate_startup_work_items`,
    /// which is the single path from a source to a startup item and the only
    /// thing those three sites call.
    ///
    /// Fails for: stamping anything but `Startup` in
    /// `enumerate_startup_work_items` — and therefore at every site that
    /// enumerates through it.
    #[test]
    fn startup_work_items_are_stamped_as_startup_triggered() {
        let nonce = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before unix epoch")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("moraine-startup-trigger-{nonce}"));
        std::fs::create_dir_all(&dir).expect("create startup fixture dir");
        std::fs::write(dir.join("first.jsonl"), b"{}\n").expect("write first fixture file");
        std::fs::write(dir.join("second.jsonl"), b"{}\n").expect("write second fixture file");
        let glob_pattern = dir.join("*.jsonl").to_string_lossy().to_string();

        let source = IngestSource {
            name: "startup-trigger-test".to_string(),
            harness: "codex".to_string(),
            enabled: true,
            glob: glob_pattern.clone(),
            watch_root: dir.to_string_lossy().to_string(),
            format: SourceFormat::Jsonl,
        };
        let items = enumerate_startup_work_items(&source).expect("enumerate startup work items");

        assert_eq!(items.len(), 2, "both fixture files must be enumerated");
        for work in &items {
            assert_eq!(work.trigger, WorkTrigger::Startup);
            assert_eq!(work.source_name, "startup-trigger-test");
            assert_eq!(work.source_glob, glob_pattern);
            // And it is still not sweep-eligible after coalescing with a
            // reconcile tick, which is the property the trigger exists to
            // carry.
            assert_eq!(
                work.trigger.merge(WorkTrigger::Reconcile),
                WorkTrigger::Startup
            );
        }

        std::fs::remove_dir_all(&dir).ok();
    }

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
    fn tracked_paths_are_deduped_and_filtered_by_extension() {
        let mut event = Event::new(EventKind::Modify(ModifyKind::Data(DataChange::Any)));
        event.paths = vec![
            PathBuf::from("/tmp/a.jsonl"),
            PathBuf::from("/tmp/a.jsonl"),
            PathBuf::from("/tmp/b.txt"),
            PathBuf::from("/tmp/c.json"),
        ];

        let jsonl = event_tracked_paths(&event, SourceFormat::Jsonl, "");
        assert_eq!(jsonl, vec!["/tmp/a.jsonl".to_string()]);

        let session_json = event_tracked_paths(&event, SourceFormat::SessionJson, "");
        assert_eq!(session_json, vec!["/tmp/c.json".to_string()]);
    }

    #[test]
    fn sqlite_sidecar_events_coalesce_to_canonical_db_path() {
        let mut event = Event::new(EventKind::Modify(ModifyKind::Data(DataChange::Any)));
        event.paths = vec![
            PathBuf::from("/tmp/User/globalStorage/state.vscdb"),
            PathBuf::from("/tmp/User/globalStorage/state.vscdb-wal"),
            PathBuf::from("/tmp/User/globalStorage/state.vscdb-shm"),
            PathBuf::from("/tmp/User/globalStorage/state.vscdb.backup"),
        ];

        let tracked = event_tracked_paths(&event, SourceFormat::CursorSqlite, "");
        assert_eq!(
            tracked,
            vec!["/tmp/User/globalStorage/state.vscdb".to_string()],
            "base + sidecars coalesce to one canonical path; backups are ignored"
        );
    }
    #[test]
    fn nac_events_only_track_the_literal_configured_database() {
        let configured = "/tmp/nac/[workspace]*/store?.db";
        let configured_glob = glob::Pattern::escape(configured);
        let mut event = Event::new(EventKind::Modify(ModifyKind::Data(DataChange::Any)));
        event.paths = vec![
            PathBuf::from(configured),
            PathBuf::from(format!("{configured}-wal")),
            PathBuf::from("/tmp/nac/[workspace]*/nested/store?.db"),
            PathBuf::from("/tmp/nac/store.db"),
        ];

        assert_eq!(
            event_tracked_paths(&event, SourceFormat::NacSqlite, &configured_glob),
            vec![configured.to_string()]
        );
    }

    #[cfg(unix)]
    #[test]
    fn nac_symlink_and_wal_events_keep_the_configured_database_identity() {
        let dir = std::env::temp_dir().join(format!(
            "moraine-watch-nac-symlink-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock before unix epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("create test dir");
        let target = dir.join("real-store.db");
        std::fs::write(&target, "").expect("write target");
        let link = dir.join("configured-store.db");
        std::os::unix::fs::symlink(&target, &link).expect("create symlink");
        let configured = link.to_string_lossy().to_string();
        let configured_glob = glob::Pattern::escape(&configured);

        let enumerated = enumerate_tracked_files(&configured_glob, SourceFormat::NacSqlite)
            .expect("enumerate NAC symlink");
        assert_eq!(enumerated, vec![configured.clone()]);

        let mut event = Event::new(EventKind::Modify(ModifyKind::Data(DataChange::Any)));
        event.paths = vec![PathBuf::from(format!("{configured}-wal"))];
        assert_eq!(
            event_tracked_paths(&event, SourceFormat::NacSqlite, &configured_glob),
            vec![configured]
        );

        let _ = std::fs::remove_file(&link);
        let _ = std::fs::remove_file(&target);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn kiro_transcript_and_metadata_events_coalesce_to_transcript_path() {
        let mut event = Event::new(EventKind::Modify(ModifyKind::Data(DataChange::Any)));
        event.paths = vec![
            PathBuf::from("/tmp/kiro/session-1.jsonl"),
            PathBuf::from("/tmp/kiro/session-1.json"),
            PathBuf::from("/tmp/kiro/session-1.tmp"),
        ];

        let tracked = event_tracked_paths(&event, SourceFormat::KiroSession, "");
        assert_eq!(tracked, vec!["/tmp/kiro/session-1.jsonl".to_string()]);
    }

    #[cfg(unix)]
    #[test]
    fn file_backed_formats_keep_symlinked_paths_as_reported() {
        // Checkpoint keys and event UIDs embed source_file: resolving
        // symlinks for long-standing file-backed sources would orphan every
        // existing checkpoint and duplicate history under new UIDs.
        let dir = std::env::temp_dir().join(format!(
            "moraine-watch-symlink-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock before unix epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("create test dir");
        let target = dir.join("real-session.jsonl");
        std::fs::write(&target, "{}\n").expect("write target");
        let link = dir.join("link-session.jsonl");
        std::os::unix::fs::symlink(&target, &link).expect("create symlink");
        let link_str = link.to_string_lossy().to_string();

        let mut event = Event::new(EventKind::Modify(ModifyKind::Data(DataChange::Any)));
        event.paths = vec![link.clone()];
        assert_eq!(
            event_tracked_paths(&event, SourceFormat::Jsonl, ""),
            vec![link_str.clone()],
            "jsonl watcher paths must not be symlink-resolved"
        );

        let glob_pattern = dir.join("link-*.jsonl").to_string_lossy().to_string();
        let enumerated =
            enumerate_tracked_files(&glob_pattern, SourceFormat::Jsonl).expect("enumerate");
        assert_eq!(
            enumerated,
            vec![link_str],
            "jsonl enumeration must not be symlink-resolved"
        );

        let _ = std::fs::remove_file(&link);
        let _ = std::fs::remove_file(&target);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn cowork_fixture_glob_discovers_transcripts_without_audit() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("fixtures/claude-cowork");
        let pattern = root
            .join("**/.claude/projects/**/*.jsonl")
            .to_string_lossy()
            .to_string();
        let files = enumerate_tracked_files(&pattern, SourceFormat::Jsonl)
            .expect("enumerate Cowork fixture");
        assert_eq!(files.len(), 2);
        assert!(files.iter().all(|path| !path.ends_with("audit.jsonl")));
        assert!(files
            .iter()
            .any(|path| path.ends_with("aaaaaaaa-1111-4333-8444-555555555555.jsonl")));
        assert!(files
            .iter()
            .any(|path| path.ends_with("bbbbbbbb-2222-4333-8444-555555555555.jsonl")));
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

    #[test]
    fn queue_rescan_records_enumeration_errors() {
        let metrics = Arc::new(Metrics::default());
        let (tx, mut rx) = mpsc::unbounded_channel();

        queue_rescan(
            "[",
            "source-alpha",
            "harness-alpha",
            SourceFormat::Jsonl,
            &tx,
            &metrics,
        );

        assert!(rx.try_recv().is_err());
        assert_eq!(metrics.watcher_reset_count.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.watcher_error_count.load(Ordering::Relaxed), 1);

        let last_error = metrics
            .last_error
            .lock()
            .expect("metrics last_error mutex poisoned")
            .clone();
        assert!(last_error.contains("rescan enumerate failed"));
        assert!(last_error.contains("source=source-alpha"));
        assert!(last_error.contains("harness=harness-alpha"));
        assert!(last_error.contains("glob=["));
    }
}
