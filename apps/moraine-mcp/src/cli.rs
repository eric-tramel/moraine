use anyhow::{anyhow, bail, Result};
use std::path::PathBuf;

/// How this `moraine-mcp` process should run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServeMode {
    /// Default for `moraine run mcp`: proxy to the shared central server when
    /// it is enabled and reachable, otherwise fall back to an embedded stdio
    /// server. The decision is made at runtime in `run_mcp_entry`.
    Stdio,
    /// `moraine-mcp --serve socket`: become the shared central server,
    /// listening on a Unix domain socket. Launched by `moraine up`.
    Socket,
}

#[derive(Debug, Clone)]
pub struct CliArgs {
    pub config_path: PathBuf,
    pub serve_mode: ServeMode,
    /// Optional `--socket <path>` override for the central socket path,
    /// otherwise taken from `mcp.central_socket_path` in config.
    pub socket_override: Option<PathBuf>,
}

fn usage() {
    eprintln!(
        "usage:
  moraine-mcp [--config <path>] [--serve <stdio|socket>] [--socket <path>]

  --serve stdio   (default) serve over stdio: proxy to the central MCP
                  server if enabled and reachable, else run embedded.
  --serve socket  run the shared central MCP server on a Unix socket.
  --socket <path> override the central socket path.
"
    );
}

fn parse_args_from(mut args: impl Iterator<Item = String>) -> Result<CliArgs> {
    let mut config_path: Option<PathBuf> = None;
    let mut serve_mode = ServeMode::Stdio;
    let mut socket_override: Option<PathBuf> = None;

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
            "-h" | "--help" | "help" => {
                usage();
                std::process::exit(0);
            }
            _ => {}
        }
    }

    Ok(CliArgs {
        config_path: moraine_config::resolve_mcp_config_path(config_path),
        serve_mode,
        socket_override,
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
    fn parse_args_rejects_unknown_serve_mode() {
        let err = parse_args_from(vec!["--serve".to_string(), "http".to_string()].into_iter())
            .expect_err("unknown serve mode should error");
        assert!(
            err.to_string().contains("unknown --serve mode `http`"),
            "unexpected error: {err}"
        );
    }
}
