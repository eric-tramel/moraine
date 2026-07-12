use anyhow::{Context, Result};
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::paths::RuntimePaths;
use crate::process::{
    clickhouse_supervisor_log_path, log_path, RollingLog, CLICKHOUSE_SUPERVISOR_LOG_ROTATIONS,
};
use crate::render::{LogsSnapshot, ServiceLogSection};
use crate::service::Service;

fn tail_lines(path: &Path, lines: usize) -> Result<Vec<String>> {
    const TAIL_READ_CHUNK_BYTES: usize = 8 * 1024;

    if lines == 0 {
        return Ok(Vec::new());
    }

    let mut file = fs::File::open(path)
        .with_context(|| format!("failed to read log file {}", path.display()))?;
    let mut position = file
        .metadata()
        .with_context(|| format!("failed to read log file {}", path.display()))?
        .len();

    let mut chunks: Vec<Vec<u8>> = Vec::new();
    let mut scratch = vec![0_u8; TAIL_READ_CHUNK_BYTES];
    let mut newline_count = 0usize;
    while position > 0 {
        let read_len = (position as usize).min(TAIL_READ_CHUNK_BYTES);
        position -= read_len as u64;
        file.seek(SeekFrom::Start(position))
            .with_context(|| format!("failed to read log file {}", path.display()))?;
        file.read_exact(&mut scratch[..read_len])
            .with_context(|| format!("failed to read log file {}", path.display()))?;
        newline_count += scratch[..read_len]
            .iter()
            .filter(|byte| **byte == b'\n')
            .count();
        chunks.push(scratch[..read_len].to_vec());
        if newline_count > lines {
            break;
        }
    }

    let total_len = chunks.iter().map(Vec::len).sum();
    let mut bytes = Vec::with_capacity(total_len);
    for chunk in chunks.iter().rev() {
        bytes.extend_from_slice(chunk);
    }

    let start = if position > 0 {
        bytes
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(bytes.len(), |idx| idx + 1)
    } else {
        0
    };
    let content = String::from_utf8_lossy(&bytes[start..]);
    let mut collected = content
        .lines()
        .rev()
        .take(lines)
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    collected.reverse();
    Ok(collected)
}

fn service_log_paths(paths: &RuntimePaths, service: Service) -> Vec<PathBuf> {
    if service != Service::ClickHouse {
        return vec![log_path(paths, service)];
    }

    let supervisor = clickhouse_supervisor_log_path(paths);
    let mut log_paths = (1..=CLICKHOUSE_SUPERVISOR_LOG_ROTATIONS)
        .rev()
        .map(|generation| RollingLog::rotation_path(&supervisor, generation))
        .filter(|path| path.exists())
        .collect::<Vec<_>>();
    log_paths.push(supervisor);
    log_paths.push(log_path(paths, service));
    log_paths
}

fn default_log_services() -> [Service; 3] {
    [Service::ClickHouse, Service::Ingest, Service::Backend]
}

fn is_not_found(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
    })
}

pub(super) fn collect_logs(
    paths: &RuntimePaths,
    service: Option<Service>,
    lines: usize,
) -> Result<LogsSnapshot> {
    let targets = match service {
        Some(svc) => vec![svc],
        None => default_log_services().to_vec(),
    };

    let mut sections = Vec::new();
    for svc in targets {
        for path in service_log_paths(paths, svc) {
            let path_string = path.display().to_string();
            if !path.exists() {
                sections.push(ServiceLogSection {
                    service: svc,
                    path: path_string,
                    exists: false,
                    lines: Vec::new(),
                });
                continue;
            }
            let retained_lines = match tail_lines(&path, lines) {
                Ok(lines) => lines,
                Err(err) if is_not_found(&err) => {
                    sections.push(ServiceLogSection {
                        service: svc,
                        path: path_string,
                        exists: false,
                        lines: Vec::new(),
                    });
                    continue;
                }
                Err(err) => return Err(err),
            };
            sections.push(ServiceLogSection {
                service: svc,
                path: path_string,
                exists: true,
                lines: retained_lines,
            });
        }
    }

    Ok(LogsSnapshot {
        requested_lines: lines,
        sections,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use moraine_config::AppConfig;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(name: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("moraine-{name}-{stamp}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    #[test]
    fn tail_lines_returns_last_n_without_trailing_newline() {
        let root = temp_dir("tail-lines-basic");
        let path = root.join("test.log");
        fs::write(&path, "one\ntwo\nthree").expect("write log");

        let lines = tail_lines(&path, 2).expect("tail lines");
        assert_eq!(lines, vec!["two".to_string(), "three".to_string()]);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn tail_lines_handles_utf8_chunk_boundary() {
        let root = temp_dir("tail-lines-utf8");
        let path = root.join("test.log");
        let prefix = "é".repeat(4500);
        let content = format!("{prefix}\nmiddle\ntail\n");
        fs::write(&path, content).expect("write log");

        let one = tail_lines(&path, 1).expect("tail one line");
        assert_eq!(one, vec!["tail".to_string()]);

        let two = tail_lines(&path, 2).expect("tail two lines");
        assert_eq!(two, vec!["middle".to_string(), "tail".to_string()]);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn missing_log_error_is_detected_through_context() {
        let root = temp_dir("tail-lines-missing");
        let err = tail_lines(&root.join("vanished.log"), 1).expect_err("missing log");
        assert!(is_not_found(&err), "{err:#}");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn tail_lines_remains_readable_when_rotation_splits_utf8() {
        let root = temp_dir("tail-lines-split-utf8");
        let path = root.join("test.log");
        fs::write(&path, [0xa9, b'\n', b't', b'a', b'i', b'l', b'\n'])
            .expect("write split utf8 log");

        let lines = tail_lines(&path, 2).expect("decode split log");
        assert_eq!(lines, vec!["�".to_string(), "tail".to_string()]);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn clickhouse_logs_include_supervisor_and_server_sections() {
        let root = temp_dir("clickhouse-log-sections");
        let mut cfg = AppConfig::default();
        cfg.runtime.root_dir = root.join("runtime").display().to_string();
        cfg.runtime.logs_dir = root.join("logs").display().to_string();
        let paths = crate::paths::runtime_paths(&cfg);
        fs::create_dir_all(&paths.logs_dir).expect("logs dir");
        fs::create_dir_all(paths.clickhouse_root.join("log")).expect("clickhouse log dir");
        let supervisor = clickhouse_supervisor_log_path(&paths);
        let server = log_path(&paths, Service::ClickHouse);
        fs::write(&supervisor, "state=exhausted\n").expect("supervisor log");
        fs::write(&server, "server line\n").expect("server log");

        let snapshot =
            collect_logs(&paths, Some(Service::ClickHouse), 10).expect("collect clickhouse logs");

        assert_eq!(snapshot.sections.len(), 2);
        assert_eq!(snapshot.sections[0].path, supervisor.display().to_string());
        assert_eq!(snapshot.sections[0].lines, ["state=exhausted"]);
        assert_eq!(snapshot.sections[1].path, server.display().to_string());
        assert_eq!(snapshot.sections[1].lines, ["server line"]);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn clickhouse_logs_include_retained_supervisor_rotations() {
        let root = temp_dir("clickhouse-rotated-log-sections");
        let mut cfg = AppConfig::default();
        cfg.runtime.root_dir = root.join("runtime").display().to_string();
        cfg.runtime.logs_dir = root.join("logs").display().to_string();
        let paths = crate::paths::runtime_paths(&cfg);
        fs::create_dir_all(&paths.logs_dir).expect("logs dir");
        fs::create_dir_all(paths.clickhouse_root.join("log")).expect("clickhouse log dir");
        let supervisor = clickhouse_supervisor_log_path(&paths);
        fs::write(
            RollingLog::rotation_path(&supervisor, 2),
            "oldest retained\n",
        )
        .expect("oldest supervisor rotation");
        fs::write(
            RollingLog::rotation_path(&supervisor, 1),
            "newer retained\n",
        )
        .expect("newer supervisor rotation");
        fs::write(&supervisor, "current supervisor\n").expect("current supervisor");
        fs::write(log_path(&paths, Service::ClickHouse), "server line\n").expect("server log");

        let snapshot =
            collect_logs(&paths, Some(Service::ClickHouse), 10).expect("collect retained logs");

        assert_eq!(snapshot.sections.len(), 4);
        assert_eq!(snapshot.sections[0].lines, ["oldest retained"]);
        assert_eq!(snapshot.sections[1].lines, ["newer retained"]);
        assert_eq!(snapshot.sections[2].lines, ["current supervisor"]);
        assert_eq!(snapshot.sections[3].lines, ["server line"]);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn default_logs_follow_managed_topology_order() {
        assert_eq!(
            default_log_services(),
            [Service::ClickHouse, Service::Ingest, Service::Backend]
        );
    }
}
