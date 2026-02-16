use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct CliArgs {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub config_path: PathBuf,
    pub static_dir: PathBuf,
}

fn default_static_dir() -> PathBuf {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .join("web")
        .join("monitor")
        .join("dist")
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
