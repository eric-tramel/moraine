use anyhow::{anyhow, Result};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct CliArgs {
    pub config_path: PathBuf,
}

fn usage() {
    eprintln!(
        "usage:
  moraine-mcp [--config <path>]
"
    );
}

fn parse_args_from(mut args: impl Iterator<Item = String>) -> Result<CliArgs> {
    let mut config_path: Option<PathBuf> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow!("--config requires a path"))?;
                config_path = Some(PathBuf::from(value));
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
    })
}

pub fn parse_args() -> Result<CliArgs> {
    parse_args_from(std::env::args().skip(1))
}

#[cfg(test)]
mod tests {
    use super::parse_args_from;
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
}
