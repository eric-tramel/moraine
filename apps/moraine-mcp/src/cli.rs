use anyhow::{anyhow, bail, Result};
use std::path::PathBuf;

/// How this `moraine-mcp` process should run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServeMode {
    /// Default for `moraine run mcp`: proxy to the shared central server when
    /// it is enabled and reachable, otherwise fall back to an embedded stdio
    /// server. The decision is made at runtime in `run_mcp_entry`.
    Stdio,
    /// `moraine-mcp --serve socket`: run the unified MCP socket and monitor
    /// HTTP backend. Launched by `moraine up`.
    Socket,
}

#[derive(Debug, Clone)]
pub struct CliArgs {
    pub config_path: PathBuf,
    pub serve_mode: ServeMode,
    /// Optional `--socket <path>` override for the central socket path,
    /// otherwise taken from `mcp.central_socket_path` in config.
    pub socket_override: Option<PathBuf>,
    /// Optional monitor bind host, valid only in socket/backend mode.
    pub host: Option<String>,
    /// Optional monitor bind port, valid only in socket/backend mode.
    pub port: Option<u16>,
    /// Optional monitor asset directory, valid only in socket/backend mode.
    pub static_dir: Option<PathBuf>,
    /// `--project-only`: restrict retrieval to sessions that originated from
    /// the directory this process was launched in. Forces an embedded server
    /// (the shared central server cannot scope per session) and is rejected
    /// in `--serve socket` mode.
    pub project_only: bool,
    /// Print usage and return successfully without loading config or serving.
    pub help: bool,
}

pub(crate) fn usage() {
    eprintln!(
        "usage:
  moraine-mcp [--config <path>] [--serve <stdio|socket>] [--socket <path>] [--host <host>] [--port <port>] [--static-dir <path>] [--project-only]

  --serve stdio   (default) serve over stdio: proxy to the central MCP
                  server if enabled and reachable, else run embedded.
  --serve socket  run the unified MCP socket + monitor HTTP backend.
  --socket <path> override the MCP Unix socket path.
  --host <host>   override the monitor HTTP bind host in socket mode.
  --port <port>   override the monitor HTTP bind port in socket mode.
  --static-dir <path>
                  override the built monitor UI directory in socket mode.
  --project-only  only retrieve sessions that originated from the current
                  working directory (runs embedded, never via the central
                  server).
"
    );
}

fn parse_args_from(mut args: impl Iterator<Item = String>) -> Result<CliArgs> {
    let mut config_path: Option<PathBuf> = None;
    let mut serve_mode = ServeMode::Stdio;
    let mut socket_override: Option<PathBuf> = None;
    let mut host = None;
    let mut port = None;
    let mut static_dir = None;
    let mut project_only = false;
    let mut help = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow!("--config requires a path"))?;
                config_path = Some(PathBuf::from(value));
            }
            "--serve" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow!("--serve requires a mode (stdio|socket)"))?;
                serve_mode = match value.as_str() {
                    "stdio" => ServeMode::Stdio,
                    "socket" => ServeMode::Socket,
                    // Reject unknown modes loudly rather than silently ignoring
                    // them, so a partial/mismatched upgrade fails visibly.
                    other => bail!("unknown --serve mode `{other}` (expected stdio or socket)"),
                };
            }
            "--socket" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow!("--socket requires a path"))?;
                socket_override = Some(PathBuf::from(value));
            }
            "--host" => {
                host = Some(
                    args.next()
                        .ok_or_else(|| anyhow!("--host requires a value"))?,
                );
            }
            "--port" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow!("--port requires a value"))?;
                port = Some(
                    value
                        .parse::<u16>()
                        .map_err(|_| anyhow!("invalid --port `{value}` (expected 0-65535)"))?,
                );
            }
            "--static-dir" => {
                static_dir = Some(PathBuf::from(
                    args.next()
                        .ok_or_else(|| anyhow!("--static-dir requires a path"))?,
                ));
            }
            "--project-only" => {
                project_only = true;
            }
            "-h" | "--help" | "help" => {
                help = true;
                break;
            }
            _ => {}
        }
    }

    if !help && project_only && serve_mode == ServeMode::Socket {
        bail!("--project-only cannot be combined with --serve socket: the shared central server serves sessions from every project");
    }
    if !help
        && serve_mode != ServeMode::Socket
        && (host.is_some() || port.is_some() || static_dir.is_some())
    {
        bail!("--host, --port, and --static-dir require --serve socket");
    }

    Ok(CliArgs {
        config_path: moraine_config::resolve_mcp_config_path(config_path),
        serve_mode,
        socket_override,
        host,
        port,
        static_dir,
        project_only,
        help,
    })
}

pub fn parse_args() -> Result<CliArgs> {
    parse_args_from(std::env::args().skip(1))
}

#[cfg(test)]
mod tests {
    use super::{parse_args_from, ServeMode};
    use std::path::PathBuf;

    #[test]
    fn parse_args_rejects_missing_config_path() {
        let err = parse_args_from(vec!["--config".to_string()].into_iter())
            .expect_err("missing --config value should error");
        assert_eq!(err.to_string(), "--config requires a path");
    }

    #[test]
    fn parse_args_uses_config_path_when_provided() {
        let parsed =
            parse_args_from(vec!["--config".to_string(), "/tmp/mcp.toml".to_string()].into_iter())
                .expect("valid config path should parse");
        assert_eq!(parsed.config_path, PathBuf::from("/tmp/mcp.toml"));
    }

    #[test]
    fn parse_args_defaults_to_stdio_mode() {
        let parsed = parse_args_from(std::iter::empty()).expect("empty args should parse");
        assert_eq!(parsed.serve_mode, ServeMode::Stdio);
        assert!(parsed.socket_override.is_none());
        assert!(parsed.host.is_none());
        assert!(parsed.port.is_none());
        assert!(parsed.static_dir.is_none());
        assert!(!parsed.help);
    }

    #[test]
    fn parse_args_accepts_serve_socket_and_socket_override() {
        let parsed = parse_args_from(
            vec![
                "--serve".to_string(),
                "socket".to_string(),
                "--socket".to_string(),
                "/tmp/custom.sock".to_string(),
            ]
            .into_iter(),
        )
        .expect("serve socket should parse");
        assert_eq!(parsed.serve_mode, ServeMode::Socket);
        assert_eq!(
            parsed.socket_override,
            Some(PathBuf::from("/tmp/custom.sock"))
        );
    }

    #[test]
    fn parse_args_accepts_monitor_overrides_in_socket_mode() {
        let parsed = parse_args_from(
            [
                "--host",
                "0.0.0.0",
                "--port",
                "9090",
                "--static-dir",
                "/tmp/monitor-dist",
                "--serve",
                "socket",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("backend monitor overrides should parse");

        assert_eq!(parsed.host.as_deref(), Some("0.0.0.0"));
        assert_eq!(parsed.port, Some(9090));
        assert_eq!(parsed.static_dir, Some(PathBuf::from("/tmp/monitor-dist")));
    }

    #[test]
    fn parse_args_rejects_monitor_overrides_in_stdio_mode() {
        let err = parse_args_from(["--port", "9090"].into_iter().map(str::to_string))
            .expect_err("monitor flags require backend mode");
        assert!(
            err.to_string().contains("require --serve socket"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn parse_args_rejects_invalid_monitor_port() {
        let err = parse_args_from(
            ["--serve", "socket", "--port", "70000"]
                .into_iter()
                .map(str::to_string),
        )
        .expect_err("out-of-range port should fail");
        assert!(
            err.to_string().contains("invalid --port `70000`"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn parse_args_rejects_unknown_serve_mode() {
        let err = parse_args_from(vec!["--serve".to_string(), "http".to_string()].into_iter())
            .expect_err("unknown serve mode should error");
        assert!(
            err.to_string().contains("unknown --serve mode `http`"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn parse_args_returns_help_without_exiting_or_validating_socket_flags() {
        let parsed = parse_args_from(["--port", "9090", "--help"].into_iter().map(str::to_string))
            .expect("help should return successfully");
        assert!(parsed.help);
    }

    #[test]
    fn parse_args_defaults_to_unscoped() {
        let parsed = parse_args_from(std::iter::empty()).expect("empty args should parse");
        assert!(!parsed.project_only);
    }

    #[test]
    fn parse_args_accepts_project_only_in_stdio_mode() {
        let parsed = parse_args_from(vec!["--project-only".to_string()].into_iter())
            .expect("project-only should parse");
        assert!(parsed.project_only);
        assert_eq!(parsed.serve_mode, ServeMode::Stdio);
    }

    #[test]
    fn parse_args_rejects_project_only_with_serve_socket() {
        let err = parse_args_from(
            vec![
                "--serve".to_string(),
                "socket".to_string(),
                "--project-only".to_string(),
            ]
            .into_iter(),
        )
        .expect_err("project-only with central server mode should error");
        assert!(
            err.to_string()
                .contains("--project-only cannot be combined with --serve socket"),
            "unexpected error: {err}"
        );
    }
}
