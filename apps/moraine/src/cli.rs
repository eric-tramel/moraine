use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::Serialize;
use std::path::PathBuf;

use crate::service::Service;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
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
    Export(Box<ExportArgs>),
    Schema(SchemaArgs),
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
    /// Start the unified MCP socket and monitor HTTP backend.
    #[arg(long)]
    pub(crate) backend: bool,
    /// Deprecated compatibility alias for `--backend`.
    #[arg(long)]
    pub(crate) monitor: bool,
    /// Deprecated compatibility alias for `--backend`.
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
pub(crate) struct ExportArgs {
    #[command(subcommand)]
    pub(crate) command: ExportCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum ExportCommand {
    Events(ExportEventsArgs),
}

#[derive(Debug, Args)]
pub(crate) struct ExportEventsArgs {
    #[arg(long, value_enum, required = true)]
    pub(crate) format: ExportRowFormat,
    #[arg(long)]
    pub(crate) columns: Option<String>,
    #[arg(long, default_value_t = false)]
    pub(crate) include_sensitive: bool,
    #[arg(long)]
    pub(crate) limit: Option<usize>,
    #[arg(long, default_value_t = false)]
    pub(crate) all: bool,
    #[arg(long)]
    pub(crate) since: Option<String>,
    #[arg(long)]
    pub(crate) until: Option<String>,
    #[arg(long)]
    pub(crate) session_id: Vec<String>,
    #[arg(long)]
    pub(crate) harness: Vec<String>,
    #[arg(long)]
    pub(crate) source_name: Vec<String>,
    #[arg(long)]
    pub(crate) project_id: Vec<String>,
    #[arg(long)]
    pub(crate) cwd_prefix: Vec<String>,
    #[arg(long)]
    pub(crate) worktree_root: Vec<String>,
    #[arg(long)]
    pub(crate) repo_rel_path: Vec<String>,
    #[arg(long)]
    pub(crate) event_kind: Vec<String>,
    #[arg(long)]
    pub(crate) payload_type: Vec<String>,
    #[arg(long)]
    pub(crate) actor_kind: Vec<String>,
    #[arg(long)]
    pub(crate) model_name: Vec<String>,
    #[arg(long)]
    pub(crate) tool_name: Vec<String>,
    #[arg(long, default_value_t = false)]
    pub(crate) tool_error_only: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum ExportRowFormat {
    Jsonl,
}

#[derive(Debug, Args)]
pub(crate) struct SchemaArgs {
    #[command(subcommand)]
    pub(crate) command: SchemaCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum SchemaCommand {
    Analytics(SchemaAnalyticsArgs),
}

#[derive(Debug, Args)]
pub(crate) struct SchemaAnalyticsArgs {
    #[arg(long)]
    pub(crate) json: bool,
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
    #[command(hide = true)]
    Supervise,
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
    /// Accept non-interactive defaults, including all supported MCP/plugin targets.
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
    #[serde(rename = "opencode")]
    #[value(name = "opencode")]
    OpenCode,
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
    fn clap_parses_internal_clickhouse_supervisor() {
        let cli = Cli::parse_from(["moraine", "clickhouse", "supervise"]);
        assert!(matches!(
            cli.command,
            CliCommand::Clickhouse(ClickhouseArgs {
                command: ClickhouseCommand::Supervise,
            })
        ));
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
    fn clap_parses_export_events_flags() {
        let cli = Cli::parse_from([
            "moraine",
            "export",
            "events",
            "--format",
            "jsonl",
            "--since",
            "2026-06-01T00:00:00Z",
            "--until",
            "2026-06-15T00:00:00Z",
            "--harness",
            "codex",
            "--harness",
            "hermes",
            "--project-id",
            "agent-stuff",
            "--columns",
            "session_id,event_uid,event_ts,payload_json",
            "--include-sensitive",
            "--limit",
            "100",
        ]);
        match cli.command {
            CliCommand::Export(args) => match args.command {
                ExportCommand::Events(events) => {
                    assert_eq!(events.format, ExportRowFormat::Jsonl);
                    assert_eq!(events.since.as_deref(), Some("2026-06-01T00:00:00Z"));
                    assert_eq!(events.until.as_deref(), Some("2026-06-15T00:00:00Z"));
                    assert_eq!(events.harness, vec!["codex", "hermes"]);
                    assert_eq!(events.project_id, vec!["agent-stuff"]);
                    assert!(events.include_sensitive);
                    assert_eq!(events.limit, Some(100));
                }
            },
            _ => panic!("expected export events command"),
        }
    }

    #[test]
    fn clap_rejects_export_events_without_format() {
        let err = Cli::try_parse_from(["moraine", "export", "events", "--all"])
            .expect_err("export row format is required");
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn clap_parses_schema_analytics_json() {
        let cli = Cli::parse_from(["moraine", "schema", "analytics", "--json"]);
        match cli.command {
            CliCommand::Schema(SchemaArgs {
                command: SchemaCommand::Analytics(analytics),
            }) => assert!(analytics.json),
            _ => panic!("expected schema analytics command"),
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
            "opencode",
            "--mcp-target",
            "cursor",
            "--mcp-target",
            "pi-coding-agent",
            "--mcp-target",
            "claude-code",
            "--mcp-target",
            "hermes",
        ]);
        match cli.command {
            CliCommand::Setup(setup) => {
                assert!(setup.yes);
                assert!(setup.dry_run);
                assert_eq!(
                    setup.mcp_targets,
                    vec![
                        SetupMcpTarget::Codex,
                        SetupMcpTarget::OpenCode,
                        SetupMcpTarget::Cursor,
                        SetupMcpTarget::PiCodingAgent,
                        SetupMcpTarget::ClaudeCode,
                        SetupMcpTarget::Hermes,
                    ]
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
    fn clap_parses_backend_and_compatibility_up_flags() {
        let cli = Cli::parse_from(["moraine", "up", "--backend", "--monitor", "--mcp"]);
        match cli.command {
            CliCommand::Up(args) => {
                assert!(args.backend);
                assert!(args.monitor);
                assert!(args.mcp);
            }
            _ => panic!("expected up command"),
        }
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
