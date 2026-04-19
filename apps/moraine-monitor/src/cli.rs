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

const MONITOR_DIST_ENV_KEYS: &[&str] = &["MORAINE_MONITOR_DIST", "MORAINE_MONITOR_STATIC_DIR"];

fn env_override_static_dir_with_keys(keys: &[&str]) -> Option<PathBuf> {
    for key in keys {
        if let Ok(value) = std::env::var(key) {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                continue;
            }
            let configured = PathBuf::from(trimmed);
            if configured.exists() {
                return Some(configured);
            }
        }
    }
    None
}

fn default_static_dir() -> PathBuf {
    if let Some(configured) = env_override_static_dir_with_keys(MONITOR_DIST_ENV_KEYS) {
        return configured;
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
  moraine-monitor [--host <host>] [--port <port>] [--config <path>] [--static-dir <path>]
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
        config_path: moraine_config::resolve_monitor_config_path(config_path),
        static_dir: static_dir.unwrap_or_else(default_static_dir),
    }
}

#[cfg(test)]
mod tests {
    use super::{env_override_static_dir_with_keys, find_monitor_dir};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(name: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "moraine-monitor-cli-{name}-{}-{stamp}",
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

    #[test]
    fn env_override_returns_path_when_set_and_exists() {
        let root = temp_root("env-override-primary");
        let env_key = "MORAINE_MONITOR_DIST_TEST_PRIMARY";
        std::env::set_var(env_key, root.to_string_lossy().to_string());

        let found = env_override_static_dir_with_keys(&[env_key]);

        std::env::remove_var(env_key);
        fs::remove_dir_all(&root).ok();
        assert_eq!(found, Some(root));
    }

    #[test]
    fn env_override_none_when_unset() {
        let found = env_override_static_dir_with_keys(&["MORAINE_MONITOR_DIST_TEST_UNSET_KEY"]);
        assert!(found.is_none());
    }

    #[test]
    fn env_override_skips_missing_path_and_falls_through_to_alias() {
        let root = temp_root("env-override-alias");
        let primary = "MORAINE_MONITOR_DIST_TEST_ALIAS_PRIMARY";
        let alias = "MORAINE_MONITOR_DIST_TEST_ALIAS_SECONDARY";
        std::env::set_var(primary, "/tmp/moraine-monitor-dist-definitely-missing");
        std::env::set_var(alias, root.to_string_lossy().to_string());

        let found = env_override_static_dir_with_keys(&[primary, alias]);

        std::env::remove_var(primary);
        std::env::remove_var(alias);
        fs::remove_dir_all(&root).ok();
        assert_eq!(found, Some(root));
    }
}
