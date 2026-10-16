use clap::{ArgGroup, Args, Parser, Subcommand, ValueEnum};
use serde::Serialize;
use std::path::PathBuf;

use crate::service::{LogService, Service};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum OutputFormat {
    Auto,
    Rich,
    Plain,
    Json,
}

#[derive(Debug, Args)]
pub(crate) struct OutputArgs {
    /// Select automatic terminal output, rich text, plain text, or JSON.
    #[arg(long, value_enum, default_value_t = OutputFormat::Auto)]
    pub(crate) output: OutputFormat,
}

#[derive(Debug, Args)]
pub(crate) struct RenderArgs {
    #[command(flatten)]
    pub(crate) output: OutputArgs,
    /// Include diagnostic details.
    #[arg(long)]
    pub(crate) verbose: bool,
}

#[derive(Debug, Args)]
pub(crate) struct TreeOutputArgs {
    /// Select automatic terminal output, rich text, plain text, or JSON.
    #[arg(long, global = true, value_enum, default_value_t = OutputFormat::Auto)]
    pub(crate) output: OutputFormat,
}

#[derive(Debug, Args)]
pub(crate) struct TreeRenderArgs {
    #[command(flatten)]
    pub(crate) output: TreeOutputArgs,
    /// Include diagnostic details.
    #[arg(long, global = true)]
    pub(crate) verbose: bool,
}

#[derive(Debug, Parser)]
#[command(
    name = "moraine",
    about = "Run and inspect local Moraine services",
    version = moraine_config::BUILD_VERSION,
    after_long_help = "Examples:\n  moraine setup\n  moraine up\n  moraine status --output json\n  moraine logs ingest --lines 500"
)]
pub(crate) struct Cli {
    /// Use this Moraine configuration file.
    #[arg(long, global = true, value_name = "PATH")]
    pub(crate) config: Option<PathBuf>,
    #[command(subcommand)]
    pub(crate) command: CliCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum CliCommand {
    /// Configure Moraine and agent integrations.
    Setup(SetupArgs),
    /// Start the local ClickHouse, ingest, and unified backend services.
    Up(UpArgs),
    /// Stop managed Moraine services.
    Down(OutputArgs),
    /// Show service, database, and ingest health.
    Status(RenderArgs),
    /// Show recent managed-service logs.
    Logs(LogsArgs),
    /// Export normalized analytics data.
    Export(Box<ExportArgs>),
    /// Describe public data schemas.
    Schema(SchemaArgs),
    /// Inspect or migrate the Moraine database.
    Db(DbArgs),
    /// Manage the bundled ClickHouse installation.
    Clickhouse(ClickhouseArgs),
    /// Inspect resolved public configuration values.
    Config(ConfigArgs),
    /// Run one Moraine service in the foreground.
    Run(RunArgs),
}

#[derive(Debug, Args)]
pub(crate) struct UpArgs {
    #[command(flatten)]
    pub(crate) render: RenderArgs,
    /// Start the backend without the ingest watcher.
    #[arg(long)]
    pub(crate) no_ingest: bool,
}

#[derive(Debug, Args)]
pub(crate) struct LogsArgs {
    #[command(flatten)]
    pub(crate) output: OutputArgs,
    /// Managed service whose logs should be shown; omit to show all services.
    #[arg(value_enum)]
    pub(crate) service: Option<LogService>,
    /// Maximum lines to show from each log.
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
    /// Stream normalized event rows as JSONL.
    Events(ExportEventsArgs),
}

#[derive(Debug, Args)]
pub(crate) struct ExportEventsArgs {
    /// Comma-separated public column names, or `all`.
    #[arg(long, value_name = "NAME,...")]
    pub(crate) columns: Option<String>,
    /// Permit explicitly selected sensitive columns.
    #[arg(long)]
    pub(crate) include_sensitive: bool,
    /// Maximum number of rows to emit.
    #[arg(long)]
    pub(crate) limit: Option<usize>,
    /// Export without a filter.
    #[arg(long)]
    pub(crate) all: bool,
    /// Include events at or after this RFC3339 timestamp.
    #[arg(long, value_name = "RFC3339")]
    pub(crate) since: Option<String>,
    /// Include events before this RFC3339 timestamp.
    #[arg(long, value_name = "RFC3339")]
    pub(crate) until: Option<String>,
    /// Match this session ID exactly; repeat to match any listed ID.
    #[arg(long)]
    pub(crate) session_id: Vec<String>,
    /// Match this harness exactly; repeat to match any listed harness.
    #[arg(long)]
    pub(crate) harness: Vec<String>,
    /// Match this configured source name; repeat to match any listed source.
    #[arg(long)]
    pub(crate) source_name: Vec<String>,
    /// Match this project ID exactly; repeat to match any listed project.
    #[arg(long)]
    pub(crate) project_id: Vec<String>,
    /// Match this working directory or one of its descendants.
    #[arg(long)]
    pub(crate) cwd_prefix: Vec<String>,
    /// Match this worktree root exactly; repeat to match any listed root.
    #[arg(long)]
    pub(crate) worktree_root: Vec<String>,
    /// Match this repository-relative path exactly.
    #[arg(long)]
    pub(crate) repo_rel_path: Vec<String>,
    /// Match this normalized event kind exactly.
    #[arg(long)]
    pub(crate) event_kind: Vec<String>,
    /// Match this normalized payload type exactly.
    #[arg(long)]
    pub(crate) payload_type: Vec<String>,
    /// Match this normalized actor kind exactly.
    #[arg(long)]
    pub(crate) actor_kind: Vec<String>,
    /// Match this model name exactly.
    #[arg(long)]
    pub(crate) model_name: Vec<String>,
    /// Match this tool name exactly.
    #[arg(long)]
    pub(crate) tool_name: Vec<String>,
    /// Include only failed tool events.
    #[arg(long)]
    pub(crate) tool_error_only: bool,
}

#[derive(Debug, Args)]
pub(crate) struct SchemaArgs {
    #[command(subcommand)]
    pub(crate) command: SchemaCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum SchemaCommand {
    /// Print the analytics export schema as JSON.
    Analytics,
}

#[derive(Debug, Args)]
pub(crate) struct DbArgs {
    #[command(flatten)]
    pub(crate) output: TreeOutputArgs,
    #[command(subcommand)]
    pub(crate) command: DbCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum DbCommand {
    /// Apply pending database migrations.
    Migrate,
    /// Check ClickHouse connectivity and schema health.
    Doctor,
}

#[derive(Debug, Args)]
pub(crate) struct ClickhouseArgs {
    #[command(subcommand)]
    pub(crate) command: ClickhouseCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum ClickhouseCommand {
    /// Install the bundled ClickHouse binary.
    Install(ClickhouseInstallArgs),
    /// Show bundled ClickHouse installation and process state.
    Status(OutputArgs),
    /// Remove the bundled ClickHouse binary.
    Uninstall(OutputArgs),
    #[command(hide = true)]
    Supervise,
}

#[derive(Debug, Args)]
pub(crate) struct ClickhouseInstallArgs {
    #[command(flatten)]
    pub(crate) output: OutputArgs,
    /// Replace an existing managed binary.
    #[arg(long)]
    pub(crate) force: bool,
    /// Install this ClickHouse release instead of the configured default.
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
    /// Print one safe resolved configuration value.
    Get(ConfigGetArgs),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum ConfigKey {
    #[value(name = "backend.start_on_up")]
    BackendStartOnUp,
    #[value(name = "clickhouse.url")]
    ClickhouseUrl,
    #[value(name = "clickhouse.database")]
    ClickhouseDatabase,
}

impl ConfigKey {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::BackendStartOnUp => "backend.start_on_up",
            Self::ClickhouseUrl => "clickhouse.url",
            Self::ClickhouseDatabase => "clickhouse.database",
        }
    }
}

#[derive(Debug, Args)]
pub(crate) struct ConfigGetArgs {
    #[command(flatten)]
    pub(crate) output: OutputArgs,
    /// Safe public configuration key to print.
    #[arg(value_enum, value_name = "KEY")]
    pub(crate) key: ConfigKey,
}

#[derive(Debug, Args)]
#[command(
    after_long_help = "Examples:\n  moraine setup\n  moraine setup config --yes\n  moraine setup integrations codex --yes\n  moraine setup integrations --all --dry-run"
)]
pub(crate) struct SetupArgs {
    #[command(flatten)]
    pub(crate) render: TreeRenderArgs,
    #[command(subcommand)]
    pub(crate) command: Option<SetupCommand>,
}

#[derive(Debug, Subcommand)]
pub(crate) enum SetupCommand {
    /// Create, validate, or repair the Moraine config file.
    Config(SetupConfigArgs),
    /// Install agent harness plugins and MCP registrations.
    Integrations(SetupIntegrationsArgs),
}

#[derive(Debug, Args)]
pub(crate) struct SetupConfigArgs {
    /// Confirm non-interactive config changes.
    #[arg(long)]
    pub(crate) yes: bool,
    /// Show planned config changes without writing files.
    #[arg(long)]
    pub(crate) dry_run: bool,
    /// Back up and replace an invalid config with the default template.
    #[arg(long)]
    pub(crate) repair: bool,
}

#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("selection")
        .required(true)
        .multiple(false)
        .args(["targets", "all"])
))]
pub(crate) struct SetupIntegrationsArgs {
    /// Harness integration to configure. Pass multiple target names as needed.
    #[arg(value_enum, value_name = "TARGET", num_args = 1..)]
    pub(crate) targets: Vec<SetupMcpTarget>,
    /// Configure every supported harness integration.
    #[arg(long)]
    pub(crate) all: bool,
    /// Confirm non-interactive integration changes.
    #[arg(long)]
    pub(crate) yes: bool,
    /// Show planned integration changes without writing files or running commands.
    #[arg(long)]
    pub(crate) dry_run: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum SetupMcpTarget {
    #[serde(rename = "antigravity")]
    #[value(name = "antigravity", alias = "antigravity-ide")]
    Antigravity,
    ClaudeCode,
    Codex,
    Hermes,
    KiroCli,
    KimiCli,
    QwenCode,
    Nac,
    #[serde(rename = "opencode")]
    #[value(name = "opencode")]
    OpenCode,
    Cursor,
    PiCodingAgent,
    PrimeAgent,
    Omp,
}

#[derive(Debug, Args)]
pub(crate) struct RunArgs {
    /// Service to run in the foreground.
    #[arg(value_enum)]
    pub(crate) service: Service,
    /// Arguments forwarded to the service after `--`.
    #[arg(last = true, num_args = 0..)]
    pub(crate) args: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{error::ErrorKind, CommandFactory};

    #[test]
    fn clap_definition_is_internally_consistent() {
        Cli::command().debug_assert();
    }

    #[test]
    fn visible_commands_and_arguments_are_documented() {
        fn assert_documented(command: &clap::Command, path: &str) {
            for argument in command
                .get_arguments()
                .filter(|argument| !argument.is_hide_set())
            {
                assert!(
                    argument.get_help().is_some() || argument.get_long_help().is_some(),
                    "missing help for {path} argument {}",
                    argument.get_id()
                );
            }
            for subcommand in command
                .get_subcommands()
                .filter(|subcommand| !subcommand.is_hide_set())
            {
                let subcommand_path = format!("{path} {}", subcommand.get_name());
                assert!(
                    subcommand.get_about().is_some() || subcommand.get_long_about().is_some(),
                    "missing help for {subcommand_path}"
                );
                assert_documented(subcommand, &subcommand_path);
            }
        }

        assert_documented(&Cli::command(), "moraine");
    }

    #[test]
    fn version_reports_the_build_commit() {
        let rendered = Cli::command().render_version().to_string();
        assert!(
            rendered.contains(moraine_config::BUILD_VERSION),
            "version output must identify the built revision: {rendered}"
        );
    }

    #[test]
    fn root_help_prioritizes_and_describes_common_commands() {
        let help = Cli::command().render_long_help().to_string();
        for description in [
            "Configure Moraine and agent integrations",
            "Start the local ClickHouse, ingest, and unified backend services",
            "Stop managed Moraine services",
            "Show service, database, and ingest health",
            "Show recent managed-service logs",
        ] {
            assert!(help.contains(description), "missing help: {description}");
        }
        assert!(help.find("setup").unwrap() < help.find("export").unwrap());
        assert!(help.contains("moraine status --output json"));
    }

    #[test]
    fn clap_parses_clickhouse_install_flags() {
        let cli = Cli::parse_from([
            "moraine",
            "clickhouse",
            "install",
            "--version",
            "v25.12.5.44-stable",
            "--force",
            "--output",
            "json",
        ]);
        match cli.command {
            CliCommand::Clickhouse(ClickhouseArgs {
                command: ClickhouseCommand::Install(install),
            }) => {
                assert!(install.force);
                assert_eq!(install.version.as_deref(), Some("v25.12.5.44-stable"));
                assert_eq!(install.output.output, OutputFormat::Json);
            }
            _ => panic!("expected clickhouse install command"),
        }
    }

    #[test]
    fn clap_parses_internal_clickhouse_supervisor_without_render_flags() {
        let cli = Cli::parse_from(["moraine", "clickhouse", "supervise"]);
        assert!(matches!(
            cli.command,
            CliCommand::Clickhouse(ClickhouseArgs {
                command: ClickhouseCommand::Supervise,
            })
        ));
        assert!(
            Cli::try_parse_from(["moraine", "clickhouse", "supervise", "--output", "json"])
                .is_err()
        );
    }

    #[test]
    fn clap_lists_and_parses_safe_config_keys() {
        let cli = Cli::parse_from(["moraine", "config", "get", "clickhouse.url"]);
        match cli.command {
            CliCommand::Config(ConfigArgs {
                command: ConfigCommand::Get(get),
            }) => assert_eq!(get.key, ConfigKey::ClickhouseUrl),
            _ => panic!("expected config get command"),
        }

        let help = Cli::command()
            .find_subcommand_mut("config")
            .unwrap()
            .find_subcommand_mut("get")
            .unwrap()
            .render_help()
            .to_string();
        for key in [
            "backend.start_on_up",
            "clickhouse.url",
            "clickhouse.database",
        ] {
            assert!(help.contains(key), "missing config key {key}");
        }
    }

    #[test]
    fn clap_parses_export_events_without_a_format_switch() {
        let cli = Cli::parse_from([
            "moraine",
            "export",
            "events",
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
        assert!(
            Cli::try_parse_from(["moraine", "export", "events", "--all", "--format", "jsonl"])
                .is_err()
        );
    }

    #[test]
    fn clap_parses_schema_analytics_without_a_json_switch() {
        assert!(matches!(
            Cli::parse_from(["moraine", "schema", "analytics"]).command,
            CliCommand::Schema(SchemaArgs {
                command: SchemaCommand::Analytics,
            })
        ));
        assert!(Cli::try_parse_from(["moraine", "schema", "analytics", "--json"]).is_err());
    }

    #[test]
    fn clap_parses_explicit_setup_integrations() {
        let cli = Cli::parse_from([
            "moraine",
            "setup",
            "integrations",
            "codex",
            "opencode",
            "prime-agent",
            "--yes",
        ]);
        match cli.command {
            CliCommand::Setup(SetupArgs {
                command: Some(SetupCommand::Integrations(args)),
                ..
            }) => {
                assert!(args.yes);
                assert!(!args.all);
                assert_eq!(
                    args.targets,
                    vec![
                        SetupMcpTarget::Codex,
                        SetupMcpTarget::OpenCode,
                        SetupMcpTarget::PrimeAgent,
                    ]
                );
            }
            _ => panic!("expected setup integrations command"),
        }
    }

    #[test]
    fn clap_requires_exactly_one_setup_integration_selection() {
        let missing = Cli::try_parse_from(["moraine", "setup", "integrations", "--yes"])
            .expect_err("selection is required");
        assert_eq!(missing.kind(), ErrorKind::MissingRequiredArgument);

        let conflict = Cli::try_parse_from([
            "moraine",
            "setup",
            "integrations",
            "codex",
            "--all",
            "--dry-run",
        ])
        .expect_err("targets and all conflict");
        assert_eq!(conflict.kind(), ErrorKind::ArgumentConflict);

        let cli = Cli::parse_from([
            "moraine",
            "setup",
            "integrations",
            "--all",
            "--dry-run",
            "--output",
            "json",
        ]);
        assert!(matches!(
            cli.command,
            CliCommand::Setup(SetupArgs {
                render: TreeRenderArgs {
                    output: TreeOutputArgs {
                        output: OutputFormat::Json,
                    },
                    ..
                },
                command: Some(SetupCommand::Integrations(SetupIntegrationsArgs {
                    all: true,
                    dry_run: true,
                    ..
                })),
            })
        ));
    }

    #[test]
    fn renderer_flags_are_scoped_to_consuming_commands() {
        let accepted: &[&[&str]] = &[
            &["moraine", "up", "--output", "json", "--verbose"],
            &["moraine", "down", "--output", "json"],
            &["moraine", "status", "--output", "json", "--verbose"],
            &["moraine", "logs", "--output", "json"],
            &["moraine", "db", "--output", "json", "doctor"],
            &["moraine", "db", "doctor", "--output", "json"],
            &["moraine", "clickhouse", "install", "--output", "json"],
            &["moraine", "clickhouse", "status", "--output", "json"],
            &["moraine", "clickhouse", "uninstall", "--output", "json"],
            &[
                "moraine",
                "config",
                "get",
                "backend.start_on_up",
                "--output",
                "json",
            ],
            &[
                "moraine",
                "setup",
                "--output",
                "json",
                "--verbose",
                "config",
                "--dry-run",
            ],
            &[
                "moraine",
                "setup",
                "config",
                "--dry-run",
                "--output",
                "json",
                "--verbose",
            ],
        ];
        for argv in accepted {
            assert!(
                Cli::try_parse_from(*argv).is_ok(),
                "expected renderer arguments to parse: {argv:?}"
            );
        }

        let rejected: &[&[&str]] = &[
            &["moraine", "--output", "json", "status"],
            &["moraine", "--verbose", "status"],
            &["moraine", "down", "--verbose"],
            &["moraine", "logs", "--verbose"],
            &["moraine", "export", "events", "--output", "json"],
            &["moraine", "export", "events", "--verbose"],
            &["moraine", "schema", "analytics", "--output", "json"],
            &["moraine", "schema", "analytics", "--verbose"],
            &["moraine", "db", "doctor", "--verbose"],
            &["moraine", "clickhouse", "--output", "json", "status"],
            &["moraine", "clickhouse", "status", "--verbose"],
            &[
                "moraine",
                "config",
                "--output",
                "json",
                "get",
                "backend.start_on_up",
            ],
            &[
                "moraine",
                "config",
                "get",
                "backend.start_on_up",
                "--verbose",
            ],
            &["moraine", "run", "mcp", "--output", "json"],
            &["moraine", "run", "mcp", "--verbose"],
        ];
        for argv in rejected {
            assert!(
                Cli::try_parse_from(*argv).is_err(),
                "expected renderer arguments to be rejected: {argv:?}"
            );
        }
    }

    #[test]
    fn clap_rejects_removed_startup_selectors() {
        for flag in ["--backend", "--monitor", "--mcp"] {
            assert!(Cli::try_parse_from(["moraine", "up", flag]).is_err());
        }
    }

    #[test]
    fn clap_parses_run_passthrough_args() {
        let cli = Cli::parse_from([
            "moraine",
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
