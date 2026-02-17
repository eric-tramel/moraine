use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct CliArgs {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub config_path: PathBuf,
    pub static_dir: PathBuf,
}

fn monitor_dist_candidate(root: &Path) -> PathBuf {
    root.join("web").join("monitor").join("dist")
}

fn find_monitor_dir(root: &Path) -> Option<PathBuf> {
    let candidate = monitor_dist_candidate(root);
    if candidate.exists() {
        Some(candidate)
    } else {
        None
    }
}

fn source_tree_static_dir() -> PathBuf {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .join("web")
        .join("monitor")
        .join("dist")
}

fn default_static_dir() -> PathBuf {
    if let Ok(value) = std::env::var("CORTEX_MONITOR_STATIC_DIR") {
        let configured = PathBuf::from(value);
        if configured.exists() {
            return configured;
        }
    }

    if let Ok(exe) = std::env::current_exe() {
        let exe = exe.canonicalize().unwrap_or(exe);
        if let Some(bin_dir) = exe.parent() {
            if let Some(bundle_root) = bin_dir.parent() {
                if let Some(found) = find_monitor_dir(bundle_root) {
                    return found;
                }
            }
        }
    }

    source_tree_static_dir()
}

fn usage() {
    eprintln!(
        "usage:
  cortex-monitor [--host <host>] [--port <port>] [--config <path>] [--static-dir <path>]
"
    );
}

pub fn parse_args() -> CliArgs {
    let mut host = None;
    let mut port = None;
    let mut config_path: Option<PathBuf> = None;
    let mut static_dir: Option<PathBuf> = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--host" => {
                if let Some(value) = args.next() {
                    host = Some(value);
                }
            }
            "--port" => {
                if let Some(value) = args.next() {
                    port = value.parse().ok();
                }
            }
            "--config" => {
                if let Some(value) = args.next() {
                    config_path = Some(PathBuf::from(value));
                }
            }
            "--static-dir" => {
                if let Some(value) = args.next() {
                    static_dir = Some(PathBuf::from(value));
                }
            }
            "-h" | "--help" | "help" => {
                usage();
                std::process::exit(0);
            }
            _ => {}
        }
    }

    CliArgs {
        host,
        port,
        config_path: cortex_config::resolve_monitor_config_path(config_path),
        static_dir: static_dir.unwrap_or_else(default_static_dir),
    }
}

#[cfg(test)]
mod tests {
    use super::find_monitor_dir;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(name: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "cortex-monitor-cli-{name}-{}-{stamp}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create temp root");
        root
    }

    #[test]
    fn find_monitor_dir_prefers_dist() {
        let root = temp_root("prefers-dist");
        let dist = root.join("web").join("monitor").join("dist");
        let monitor = root.join("web").join("monitor");
        fs::create_dir_all(&dist).expect("create dist");
        fs::create_dir_all(&monitor).expect("create monitor dir");

        let found = find_monitor_dir(&root);
        assert_eq!(found, Some(dist));

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn find_monitor_dir_requires_dist() {
        let root = temp_root("fallback-monitor-root");
        let monitor = root.join("web").join("monitor");
        fs::create_dir_all(&monitor).expect("create monitor dir");

        let found = find_monitor_dir(&root);
        assert_eq!(found, None);

        fs::remove_dir_all(root).expect("cleanup");
    }
}
