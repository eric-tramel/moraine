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
    /// Deprecated compatibility flag; the backend now always starts.
    #[arg(long)]
    pub(crate) backend: bool,
    /// Deprecated compatibility flag; the backend now always starts.
    #[arg(long)]
    pub(crate) monitor: bool,
    /// Deprecated compatibility flag; the backend now always starts.
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
    /// Inspect and operate the canonical read indexes (issue #598).
    CoreIndex(CoreIndexArgs),
    /// Inspect storage ownership and plan physical reclamation (issue #603).
    Reclaim(ReclaimArgs),
}

#[derive(Debug, Args)]
pub(crate) struct ReclaimArgs {
    #[command(subcommand)]
    pub(crate) command: ReclaimCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum ReclaimCommand {
    /// Print per-bucket storage, disk headroom, the effective retention
    /// policy, and the reclaim ledger. Never destructive.
    Status(ReclaimStatusArgs),
    /// Dry run: the claim set and row/byte ESTIMATES for each scope. Writes
    /// nothing.
    Plan(ReclaimPlanArgs),
    /// Claim and execute a bounded reclaim. Refuses without --confirm, and
    /// refuses a scope that deletes user history unless the matching
    /// `[retention]` key is configured.
    Run(ReclaimRunArgs),
}

#[derive(Debug, Args)]
pub(crate) struct ReclaimStatusArgs {
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct ReclaimPlanArgs {
    /// Limit the dry run to one scope (`read_index_generation`,
    /// `canonical_generation`). Omit for all.
    #[arg(long)]
    pub(crate) scope: Option<String>,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct ReclaimRunArgs {
    /// The scope to reclaim (`read_index_generation`,
    /// `canonical_generation`). Required: there is deliberately no
    /// "everything".
    #[arg(long)]
    pub(crate) scope: String,
    /// Acknowledge the deletion. Without it the command prints exactly what
    /// would be deleted and exits non-zero.
    #[arg(long)]
    pub(crate) confirm: bool,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct CoreIndexArgs {
    #[command(subcommand)]
    pub(crate) command: CoreIndexCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum CoreIndexCommand {
    /// Print core-index/open-v2 readiness, backfill cursor age, overlap-audit
    /// outcome, and the active open-reader mode.
    Status,
    /// Truncate the canonical read indexes and re-run the backfill from scratch.
    Rebuild,
    /// Publish the one-way open-v2 reader flag for a Shared/multi-writer backend
    /// (or re-promote after a rebuild). Requires --force to confirm every reader
    /// is v2-capable.
    Promote(CoreIndexPromoteArgs),
}

#[derive(Debug, Args)]
pub(crate) struct CoreIndexPromoteArgs {
    /// Confirm that every open-tool consumer of this backend is v2-capable.
    /// Promotion switches ALL readers of a shared backend onto the canonical
    /// reader; a downlevel reader would fail. Required to publish.
    #[arg(long)]
    pub(crate) force: bool,
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
    KiroCli,
    KimiCli,
    QwenCode,
    Nac,
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
    fn clap_parses_core_index_subcommands() {
        let status = Cli::parse_from(["moraine", "db", "core-index", "status"]);
        assert!(matches!(
            status.command,
            CliCommand::Db(DbArgs {
                command: DbCommand::CoreIndex(CoreIndexArgs {
                    command: CoreIndexCommand::Status,
                }),
            })
        ));

        let rebuild = Cli::parse_from(["moraine", "db", "core-index", "rebuild"]);
        assert!(matches!(
            rebuild.command,
            CliCommand::Db(DbArgs {
                command: DbCommand::CoreIndex(CoreIndexArgs {
                    command: CoreIndexCommand::Rebuild,
                }),
            })
        ));

        // Promote defaults to unforced; --force flips the confirmation.
        let promote = Cli::parse_from(["moraine", "db", "core-index", "promote"]);
        match promote.command {
            CliCommand::Db(DbArgs {
                command:
                    DbCommand::CoreIndex(CoreIndexArgs {
                        command: CoreIndexCommand::Promote(args),
                    }),
            }) => assert!(!args.force),
            _ => panic!("expected core-index promote command"),
        }

        let promote_forced = Cli::parse_from(["moraine", "db", "core-index", "promote", "--force"]);
        match promote_forced.command {
            CliCommand::Db(DbArgs {
                command:
                    DbCommand::CoreIndex(CoreIndexArgs {
                        command: CoreIndexCommand::Promote(args),
                    }),
            }) => assert!(args.force),
            _ => panic!("expected forced core-index promote command"),
        }
    }

    /// The `--scope` help lists **every** scope, not the ones that happened to
    /// exist when it was written.
    ///
    /// `ReclaimScope::parse` accepts all of `ReclaimScope::ALL`, so a scope
    /// missing from this list is runnable and undiscoverable: an operator
    /// reading `--help` cannot find it. That is how the since-retired
    /// `mcp_open_retired_lineage` came to be a registered, default-on
    /// executor with a header table entry in `docs/configuration.md` and no
    /// mention in the one place an operator looks first.
    ///
    /// Asserted against the enum rather than a literal list, so registering a
    /// scope and forgetting the help text fails here rather than shipping.
    ///
    /// **Both `--scope`-bearing subcommands, because `run` is the one that
    /// deletes.** The previous revision of this test walked `plan` only while
    /// claiming the enum-driven guarantee for the whole surface — the same
    /// defect one level up: a guard whose *name* is broader than what it
    /// drives. `run --help` named **zero** scopes and stayed green, so the
    /// destructive subcommand was the undiscoverable one.
    ///
    /// MUTATION (executed 2026-07-31): drop `read_index_generation` from
    /// `ReclaimPlanArgs::scope`'s doc comment => FAILS here on `plan`.
    /// **Lower bound.**
    ///
    /// MUTATION (executed 2026-07-28): restore `ReclaimRunArgs::scope`'s doc
    /// comment to `The scope to reclaim. Required: there is deliberately no
    /// "everything".` => FAILS here on `run`, and passed the whole workspace
    /// before this test drove `run`. **Width.**
    #[test]
    fn the_scope_help_names_every_reclaim_scope() {
        use clap::CommandFactory;

        // The help an operator actually reads, not the root's: `render_long_help`
        // does not recurse into subcommands.
        for subcommand in [
            ["db", "reclaim", "plan"].as_slice(),
            ["db", "reclaim", "run"].as_slice(),
        ] {
            let mut root = Cli::command();
            let leaf = subcommand.iter().fold(&mut root, |command, name| {
                command
                    .find_subcommand_mut(*name)
                    .unwrap_or_else(|| panic!("`{name}` subcommand exists"))
            });
            let path = subcommand.join(" ");
            let help = leaf.render_long_help().to_string();
            for scope in moraine_clickhouse::ReclaimScope::ALL {
                assert!(
                    help.contains(scope.as_str()),
                    "`moraine {path} --help` never names `{}`, so an operator cannot \
                     discover a scope this build will happily run",
                    scope.as_str()
                );
            }
        }
    }

    #[test]
    fn clap_parses_reclaim_subcommands() {
        let status = Cli::parse_from(["moraine", "db", "reclaim", "status"]);
        match status.command {
            CliCommand::Db(DbArgs {
                command:
                    DbCommand::Reclaim(ReclaimArgs {
                        command: ReclaimCommand::Status(args),
                    }),
            }) => assert!(!args.json),
            _ => panic!("expected reclaim status command"),
        }

        let plan = Cli::parse_from([
            "moraine",
            "db",
            "reclaim",
            "plan",
            "--scope",
            "read_index_generation",
            "--json",
        ]);
        match plan.command {
            CliCommand::Db(DbArgs {
                command:
                    DbCommand::Reclaim(ReclaimArgs {
                        command: ReclaimCommand::Plan(args),
                    }),
            }) => {
                assert_eq!(args.scope.as_deref(), Some("read_index_generation"));
                assert!(args.json);
            }
            _ => panic!("expected reclaim plan command"),
        }

        // `run` must not be invocable without a scope: there is deliberately
        // no "reclaim everything".
        assert!(Cli::try_parse_from(["moraine", "db", "reclaim", "run"]).is_err());

        // And --confirm defaults off, so the ceremony cannot be skipped by
        // omission.
        let run = Cli::parse_from([
            "moraine",
            "db",
            "reclaim",
            "run",
            "--scope",
            "canonical_generation",
        ]);
        match run.command {
            CliCommand::Db(DbArgs {
                command:
                    DbCommand::Reclaim(ReclaimArgs {
                        command: ReclaimCommand::Run(args),
                    }),
            }) => {
                assert_eq!(args.scope, "canonical_generation");
                assert!(!args.confirm);
            }
            _ => panic!("expected reclaim run command"),
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
            "--mcp-target",
            "qwen-code",
            "--mcp-target",
            "kiro-cli",
            "--mcp-target",
            "nac",
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
                        SetupMcpTarget::QwenCode,
                        SetupMcpTarget::KiroCli,
                        SetupMcpTarget::Nac,
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
