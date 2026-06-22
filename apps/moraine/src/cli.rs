use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::Serialize;
use std::path::PathBuf;

use crate::service::Service;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum OutputFormat {
    Auto,
    Rich,
    Plain,
    Json,
}

#[derive(Debug, Parser)]
#[command(
    name = "moraine",
    about = "Unified runtime control plane for Moraine services",
    version = env!("CARGO_PKG_VERSION")
)]
pub(crate) struct Cli {
    #[arg(long, global = true, value_name = "PATH")]
    pub(crate) config: Option<PathBuf>,
    #[arg(long, global = true, value_enum, default_value_t = OutputFormat::Auto)]
    pub(crate) output: OutputFormat,
    #[arg(long, global = true, default_value_t = false)]
    pub(crate) verbose: bool,
    #[command(subcommand)]
    pub(crate) command: CliCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum CliCommand {
    Up(UpArgs),
    Down,
    Status,
    Logs(LogsArgs),
    Db(DbArgs),
    Clickhouse(ClickhouseArgs),
    Config(ConfigArgs),
    Setup(SetupArgs),
    Run(RunArgs),
}

#[derive(Debug, Args)]
pub(crate) struct UpArgs {
    #[arg(long)]
    pub(crate) no_ingest: bool,
    #[arg(long)]
    pub(crate) monitor: bool,
    #[arg(long)]
    pub(crate) mcp: bool,
}

#[derive(Debug, Args)]
pub(crate) struct LogsArgs {
    #[arg(value_enum)]
    pub(crate) service: Option<Service>,
    #[arg(long, default_value_t = 200)]
    pub(crate) lines: usize,
}

#[derive(Debug, Args)]
pub(crate) struct DbArgs {
    #[command(subcommand)]
    pub(crate) command: DbCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum DbCommand {
    Migrate,
    Doctor,
}

#[derive(Debug, Args)]
pub(crate) struct ClickhouseArgs {
    #[command(subcommand)]
    pub(crate) command: ClickhouseCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum ClickhouseCommand {
    Install(ClickhouseInstallArgs),
    Status,
    Uninstall,
}

#[derive(Debug, Args)]
pub(crate) struct ClickhouseInstallArgs {
    #[arg(long)]
    pub(crate) force: bool,
    #[arg(long)]
    pub(crate) version: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct ConfigArgs {
    #[command(subcommand)]
    pub(crate) command: ConfigCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum ConfigCommand {
    Get(ConfigGetArgs),
}

#[derive(Debug, Args)]
pub(crate) struct ConfigGetArgs {
    #[arg(value_name = "KEY")]
    pub(crate) key: String,
}

#[derive(Debug, Args)]
pub(crate) struct SetupArgs {
    /// Accept non-interactive defaults. Does not enable MCP targets by itself.
    #[arg(long)]
    pub(crate) yes: bool,
    /// Show planned changes without writing files or running external commands.
    #[arg(long)]
    pub(crate) dry_run: bool,
    /// Skip config file creation, validation, and repair.
    #[arg(long, conflicts_with = "repair_config")]
    pub(crate) skip_config: bool,
    /// Skip MCP/plugin registration prompts and actions.
    #[arg(long, conflicts_with = "mcp_targets")]
    pub(crate) skip_mcp: bool,
    /// Repair an invalid config by backing it up and writing the default template.
    #[arg(long)]
    pub(crate) repair_config: bool,
    /// MCP/plugin target to configure. Repeat to select multiple targets.
    #[arg(long = "mcp-target", value_enum, value_name = "TARGET")]
    pub(crate) mcp_targets: Vec<SetupMcpTarget>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum SetupMcpTarget {
    ClaudeCode,
    Codex,
    Hermes,
    KimiCli,
    Cursor,
    PiCodingAgent,
}

#[derive(Debug, Args)]
pub(crate) struct RunArgs {
    #[arg(value_enum)]
    pub(crate) service: Service,
    #[arg(
        trailing_var_arg = true,
        allow_hyphen_values = true,
        num_args = 0..
    )]
    pub(crate) args: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clap_parses_clickhouse_install_flags() {
        let cli = Cli::parse_from([
            "moraine",
            "clickhouse",
            "install",
            "--version",
            "v25.12.5.44-stable",
            "--force",
        ]);
        match cli.command {
            CliCommand::Clickhouse(ClickhouseArgs {
                command: ClickhouseCommand::Install(install),
            }) => {
                assert!(install.force);
                assert_eq!(install.version.as_deref(), Some("v25.12.5.44-stable"));
            }
            _ => panic!("expected clickhouse install command"),
        }
    }

    #[test]
    fn clap_parses_config_get_key() {
        let cli = Cli::parse_from(["moraine", "config", "get", "clickhouse.url"]);
        match cli.command {
            CliCommand::Config(ConfigArgs {
                command: ConfigCommand::Get(get),
            }) => assert_eq!(get.key, "clickhouse.url"),
            _ => panic!("expected config get command"),
        }
    }

    #[test]
    fn clap_parses_setup_targets() {
        let cli = Cli::parse_from([
            "moraine",
            "setup",
            "--yes",
            "--dry-run",
            "--mcp-target",
            "codex",
            "--mcp-target",
            "claude-code",
        ]);
        match cli.command {
            CliCommand::Setup(setup) => {
                assert!(setup.yes);
                assert!(setup.dry_run);
                assert_eq!(
                    setup.mcp_targets,
                    vec![SetupMcpTarget::Codex, SetupMcpTarget::ClaudeCode]
                );
            }
            _ => panic!("expected setup command"),
        }
    }

    #[test]
    fn clap_rejects_setup_skip_mcp_with_target() {
        let err = Cli::try_parse_from(["moraine", "setup", "--skip-mcp", "--mcp-target", "codex"])
            .expect_err("conflicting setup mcp flags should fail");
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn clap_parses_run_passthrough_args() {
        let cli = Cli::parse_from([
            "moraine",
            "--output",
            "plain",
            "run",
            "mcp",
            "--",
            "--stdio",
            "--transport",
            "jsonrpc",
        ]);
        match cli.command {
            CliCommand::Run(run) => {
                assert_eq!(run.service, Service::Mcp);
                assert_eq!(
                    run.args,
                    vec![
                        "--stdio".to_string(),
                        "--transport".to_string(),
                        "jsonrpc".to_string(),
                    ]
                );
            }
            _ => panic!("expected run command"),
        }
    }
}
