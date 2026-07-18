use std::cell::OnceCell;
use std::collections::BTreeSet;
use std::env;
use std::iter;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde_json::{Map, Value};
use toml_edit::{value as toml_value, Table};

use super::{CommandSpec, ConfigTarget, McpPlan, McpPlanStep, SetupMcpTarget};

pub(super) mod nac;

const HERMES_PLUGIN_REMOTE_IDENTIFIER: &str = "eric-tramel/moraine/plugins/hermes-moraine";
const HERMES_PLUGIN_RELATIVE_PATH: &str = "plugins/hermes-moraine";
const KIRO_STEERING: &str = include_str!("kiro-steering.md");

#[derive(Debug, Clone)]
pub(super) struct SetupPathContext {
    pub(super) launch_cwd: PathBuf,
    pub(super) home: Option<PathBuf>,
    pub(super) xdg_config_home: Option<PathBuf>,
    pub(super) kiro_home: Option<PathBuf>,
    pub(super) nac_home: Option<PathBuf>,
    pub(super) nac_snapshot: OnceCell<std::result::Result<nac::ConfigSnapshot, String>>,
    pub(super) nac_expected_content: OnceCell<Vec<u8>>,
}

impl SetupPathContext {
    pub(super) fn from_env() -> Result<Self> {
        Ok(Self {
            launch_cwd: env::current_dir().context("failed to resolve setup launch directory")?,
            home: env::var_os("HOME").map(PathBuf::from),
            xdg_config_home: env::var_os("XDG_CONFIG_HOME").map(PathBuf::from),
            kiro_home: env::var_os("KIRO_HOME").map(PathBuf::from),
            nac_home: env::var_os("NAC_HOME").map(PathBuf::from),
            nac_snapshot: OnceCell::new(),
            nac_expected_content: OnceCell::new(),
        })
    }

    #[cfg(test)]
    pub(super) fn with_home(home: Option<PathBuf>) -> Self {
        Self {
            launch_cwd: PathBuf::from("/"),
            home,
            xdg_config_home: None,
            kiro_home: None,
            nac_home: None,
            nac_snapshot: OnceCell::new(),
            nac_expected_content: OnceCell::new(),
        }
    }

    fn resolve_from_launch(&self, path: &Path) -> PathBuf {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.launch_cwd.join(path)
        }
    }

    fn nac_home_resolution(&self) -> (PathBuf, bool) {
        if let Some(path) = &self.nac_home {
            return (self.resolve_from_launch(path), path.is_absolute());
        }
        if let Some(path) = &self.xdg_config_home {
            return (
                self.resolve_from_launch(path).join("nac"),
                path.is_absolute(),
            );
        }
        if let Some(path) = &self.home {
            return (
                self.resolve_from_launch(path).join(".config").join("nac"),
                path.is_absolute(),
            );
        }
        (self.launch_cwd.join(".nac"), false)
    }

    pub(super) fn nac_home_dir(&self) -> PathBuf {
        self.nac_home_resolution().0
    }

    pub(super) fn nac_config_path(&self) -> PathBuf {
        self.nac_home_dir().join("config.toml")
    }

    pub(super) fn nac_config_snapshot(&self) -> Result<nac::ConfigSnapshot> {
        self.nac_snapshot
            .get_or_init(|| {
                nac::ConfigSnapshot::read(self.nac_config_path()).map_err(|exc| format!("{exc:#}"))
            })
            .clone()
            .map_err(|message| anyhow::anyhow!(message))
    }

    pub(super) fn resolve_nac_store(&self) -> Result<nac::StoreResolution> {
        let snapshot = self.nac_config_snapshot()?;
        let (nac_home, stable_home) = self.nac_home_resolution();
        let store = snapshot.resolve_store(&self.launch_cwd, nac_home, stable_home)?;
        store.validate_paths()?;
        Ok(store)
    }

    pub(super) fn nac_manual_ingest_guidance(&self) -> Result<Option<String>> {
        self.resolve_nac_store()?.manual_guidance()
    }

    pub(super) fn mark_nac_mcp_applied(&self, config_target: &ConfigTarget) -> Result<()> {
        let snapshot = self.nac_config_snapshot()?;
        let prepared = snapshot.prepare_mcp_write(&mcp_run_args(config_target))?;
        snapshot.verify_current(Some(prepared.rendered()))?;
        let _ = self.nac_expected_content.set(prepared.rendered().to_vec());
        Ok(())
    }

    pub(super) fn verify_nac_snapshot_current(&self) -> Result<()> {
        let Some(snapshot) = self.nac_snapshot.get() else {
            return Ok(());
        };
        let snapshot = snapshot
            .clone()
            .map_err(|message| anyhow::anyhow!(message))?;
        snapshot.verify_current(
            self.nac_expected_content
                .get()
                .map(|content| content.as_slice()),
        )
    }
}

#[derive(Debug, Clone)]
pub(super) struct ResolvedIngestSource {
    name: String,
    harness: String,
    glob: String,
    watch_root: String,
    format: Option<String>,
    materialize: bool,
}

impl ResolvedIngestSource {
    pub(super) fn name(&self) -> &str {
        &self.name
    }

    pub(super) fn harness(&self) -> &str {
        &self.harness
    }
    pub(super) fn materializes(&self) -> bool {
        self.materialize
    }

    pub(super) fn to_table(&self, enabled: bool) -> Table {
        let mut table = Table::new();
        table["name"] = toml_value(&self.name);
        table["harness"] = toml_value(&self.harness);
        table["enabled"] = toml_value(enabled);
        table["glob"] = toml_value(&self.glob);
        table["watch_root"] = toml_value(&self.watch_root);
        if let Some(format) = &self.format {
            table["format"] = toml_value(format);
        }
        table
    }

    pub(super) fn reconcile_table(
        &self,
        table: &mut Table,
        enabled: bool,
    ) -> DefaultIngestSourceUpdate {
        let effective_enabled = enabled && self.materialize;
        let enabled_changed = set_bool(table, "enabled", effective_enabled);
        let mut metadata_changed = false;
        if self.materialize {
            metadata_changed |= set_str(table, "name", &self.name);
            metadata_changed |= set_str(table, "harness", &self.harness);
            metadata_changed |= set_str(table, "glob", &self.glob);
            metadata_changed |= set_str(table, "watch_root", &self.watch_root);
            metadata_changed |= match &self.format {
                Some(format) => set_str(table, "format", format),
                None => table.remove("format").is_some(),
            };
        }
        DefaultIngestSourceUpdate {
            enabled_changed,
            metadata_changed,
        }
    }
}

impl From<DefaultIngestSource> for ResolvedIngestSource {
    fn from(source: DefaultIngestSource) -> Self {
        Self {
            name: source.name.to_string(),
            harness: source.harness.to_string(),
            glob: source.glob.to_string(),
            watch_root: source.watch_root.to_string(),
            format: source.format.map(str::to_string),
            materialize: true,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct DefaultIngestSourceUpdate {
    pub(super) enabled_changed: bool,
    pub(super) metadata_changed: bool,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct DefaultIngestSource {
    name: &'static str,
    harness: &'static str,
    glob: &'static str,
    watch_root: &'static str,
    format: Option<&'static str>,
}

fn set_str(table: &mut Table, key: &str, expected: &str) -> bool {
    if table.get(key).and_then(toml_edit::Item::as_str) == Some(expected) {
        return false;
    }
    table[key] = toml_value(expected);
    true
}

fn set_bool(table: &mut Table, key: &str, expected: bool) -> bool {
    if table.get(key).and_then(toml_edit::Item::as_bool) == Some(expected) {
        return false;
    }
    table[key] = toml_value(expected);
    true
}

#[derive(Debug, Clone, Copy)]
enum ProbePaths {
    None,
    OpenCode,
    Cursor,
    Nac,
    Pi,
}

impl ProbePaths {
    fn paths(self, home: &Path) -> Vec<PathBuf> {
        match self {
            ProbePaths::None => Vec::new(),
            ProbePaths::OpenCode => vec![
                home.join(".config").join("opencode"),
                home.join(".local").join("share").join("opencode"),
            ],
            ProbePaths::Cursor => vec![
                home.join(".cursor"),
                home.join("Library")
                    .join("Application Support")
                    .join("Cursor"),
                home.join(".config").join("Cursor"),
            ],
            ProbePaths::Nac => vec![home.join(".config").join("nac")],
            ProbePaths::Pi => vec![home.join(".pi").join("agent")],
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct HarnessSpec {
    pub(super) target: SetupMcpTarget,
    label: &'static str,
    setup_kind: &'static str,
    programs: &'static [&'static str],
    probe_paths: ProbePaths,
    ingest_sources: &'static [DefaultIngestSource],
}

impl HarnessSpec {
    pub(super) fn label(self) -> &'static str {
        self.label
    }

    pub(super) fn setup_kind(self) -> &'static str {
        self.setup_kind
    }

    pub(super) fn program_candidates(self) -> &'static [&'static str] {
        self.programs
    }

    pub(super) fn default_probe_paths(self, paths: &SetupPathContext) -> Vec<PathBuf> {
        if matches!(self.probe_paths, ProbePaths::Nac) {
            vec![paths.nac_home_dir()]
        } else {
            paths
                .home
                .as_deref()
                .map(|home| self.probe_paths.paths(home))
                .unwrap_or_default()
        }
    }

    pub(super) fn ingest_sources(self) -> &'static [DefaultIngestSource] {
        self.ingest_sources
    }
}

const CODEX_INGEST: [DefaultIngestSource; 1] = [DefaultIngestSource {
    name: "codex",
    harness: "codex",
    glob: "~/.codex/sessions/**/*.jsonl",
    watch_root: "~/.codex/sessions",
    format: None,
}];

#[cfg(target_os = "macos")]
const CLAUDE_INGEST: [DefaultIngestSource; 2] = [
    DefaultIngestSource {
        name: "claude",
        harness: "claude-code",
        glob: "~/.claude/projects/**/*.jsonl",
        watch_root: "~/.claude/projects",
        format: None,
    },
    DefaultIngestSource {
        name: "claude-cowork",
        harness: "claude-code",
        glob: "~/Library/Application Support/Claude/local-agent-mode-sessions/**/.claude/projects/**/*.jsonl",
        watch_root: "~/Library/Application Support/Claude/local-agent-mode-sessions",
        format: None,
    },
];

#[cfg(not(target_os = "macos"))]
const CLAUDE_INGEST: [DefaultIngestSource; 1] = [DefaultIngestSource {
    name: "claude",
    harness: "claude-code",
    glob: "~/.claude/projects/**/*.jsonl",
    watch_root: "~/.claude/projects",
    format: None,
}];

const HERMES_INGEST: [DefaultIngestSource; 1] = [DefaultIngestSource {
    name: "hermes",
    harness: "hermes",
    glob: "~/.hermes/sessions/session_*.json",
    watch_root: "~/.hermes/sessions",
    format: Some("session_json"),
}];

const KIRO_INGEST: [DefaultIngestSource; 1] = [DefaultIngestSource {
    name: "kiro",
    harness: "kiro-cli",
    glob: "~/.kiro/sessions/cli/*.jsonl",
    watch_root: "~/.kiro/sessions/cli",
    format: Some("kiro_session"),
}];

const KIMI_INGEST: [DefaultIngestSource; 1] = [DefaultIngestSource {
    name: "kimi-cli",
    harness: "kimi-cli",
    glob: "~/.kimi/sessions/**/wire.jsonl",
    watch_root: "~/.kimi/sessions",
    format: Some("jsonl"),
}];

const QWEN_CODE_INGEST: [DefaultIngestSource; 1] = [DefaultIngestSource {
    name: "qwen-code",
    harness: "qwen-code",
    glob: "~/.qwen/projects/*/chats/*.jsonl",
    watch_root: "~/.qwen/projects",
    format: Some("jsonl"),
}];

const OPENCODE_INGEST: [DefaultIngestSource; 1] = [DefaultIngestSource {
    name: "opencode",
    harness: "opencode",
    glob: "~/.local/share/opencode/opencode*.db",
    watch_root: "~/.local/share/opencode",
    format: Some("opencode_sqlite"),
}];

#[cfg(target_os = "macos")]
const CURSOR_SQLITE_GLOB: &str = "~/Library/Application Support/Cursor/User/**/state.vscdb";
#[cfg(target_os = "macos")]
const CURSOR_SQLITE_ROOT: &str = "~/Library/Application Support/Cursor/User";
#[cfg(not(target_os = "macos"))]
const CURSOR_SQLITE_GLOB: &str = "~/.config/Cursor/User/**/state.vscdb";
#[cfg(not(target_os = "macos"))]
const CURSOR_SQLITE_ROOT: &str = "~/.config/Cursor/User";

const CURSOR_INGEST: [DefaultIngestSource; 2] = [
    DefaultIngestSource {
        name: "cursor",
        harness: "cursor",
        glob: "~/.cursor/projects/*/agent-transcripts/**/*.jsonl",
        watch_root: "~/.cursor/projects",
        format: Some("jsonl"),
    },
    DefaultIngestSource {
        name: "cursor-sqlite",
        harness: "cursor",
        glob: CURSOR_SQLITE_GLOB,
        watch_root: CURSOR_SQLITE_ROOT,
        format: Some("cursor_sqlite"),
    },
];

const PI_INGEST: [DefaultIngestSource; 2] = [
    DefaultIngestSource {
        name: "pi",
        harness: "pi-coding-agent",
        glob: "~/.pi/agent/sessions/**/*.jsonl",
        watch_root: "~/.pi/agent/sessions",
        format: Some("jsonl"),
    },
    DefaultIngestSource {
        name: "omp",
        harness: "pi-coding-agent",
        glob: "~/.omp/agent/sessions/**/*.jsonl",
        watch_root: "~/.omp/agent/sessions",
        format: Some("jsonl"),
    },
];

const NAC_INGEST: [DefaultIngestSource; 0] = [];
const SPECS: [HarnessSpec; 10] = [
    HarnessSpec {
        target: SetupMcpTarget::ClaudeCode,
        label: "Claude Code",
        setup_kind: "plugin",
        programs: &["claude"],
        probe_paths: ProbePaths::None,
        ingest_sources: &CLAUDE_INGEST,
    },
    HarnessSpec {
        target: SetupMcpTarget::Codex,
        label: "Codex",
        setup_kind: "plugin",
        programs: &["codex"],
        probe_paths: ProbePaths::None,
        ingest_sources: &CODEX_INGEST,
    },
    HarnessSpec {
        target: SetupMcpTarget::Hermes,
        label: "Hermes",
        setup_kind: "plugin",
        programs: &["hermes"],
        probe_paths: ProbePaths::None,
        ingest_sources: &HERMES_INGEST,
    },
    HarnessSpec {
        target: SetupMcpTarget::KiroCli,
        label: "Kiro CLI",
        setup_kind: "MCP + steering",
        programs: &["kiro-cli"],
        probe_paths: ProbePaths::None,
        ingest_sources: &KIRO_INGEST,
    },
    HarnessSpec {
        target: SetupMcpTarget::KimiCli,
        label: "Kimi CLI",
        setup_kind: "MCP",
        programs: &["kimi"],
        probe_paths: ProbePaths::None,
        ingest_sources: &KIMI_INGEST,
    },
    HarnessSpec {
        target: SetupMcpTarget::QwenCode,
        label: "Qwen Code",
        setup_kind: "MCP",
        programs: &["qwen"],
        probe_paths: ProbePaths::None,
        ingest_sources: &QWEN_CODE_INGEST,
    },
    HarnessSpec {
        target: SetupMcpTarget::Nac,
        label: "NAC",
        setup_kind: "MCP config",
        programs: &["nac"],
        probe_paths: ProbePaths::Nac,
        ingest_sources: &NAC_INGEST,
    },
    HarnessSpec {
        target: SetupMcpTarget::OpenCode,
        label: "OpenCode",
        setup_kind: "MCP config",
        programs: &["opencode"],
        probe_paths: ProbePaths::OpenCode,
        ingest_sources: &OPENCODE_INGEST,
    },
    HarnessSpec {
        target: SetupMcpTarget::Cursor,
        label: "Cursor",
        setup_kind: "MCP config",
        programs: &["cursor", "cursor-agent"],
        probe_paths: ProbePaths::Cursor,
        ingest_sources: &CURSOR_INGEST,
    },
    HarnessSpec {
        target: SetupMcpTarget::PiCodingAgent,
        label: "Pi Coding Agent",
        setup_kind: "MCP extension",
        programs: &["pi"],
        probe_paths: ProbePaths::Pi,
        ingest_sources: &PI_INGEST,
    },
];

pub(super) fn setup_targets() -> Vec<SetupMcpTarget> {
    SPECS.iter().map(|spec| spec.target).collect()
}

pub(super) fn spec(target: SetupMcpTarget) -> &'static HarnessSpec {
    SPECS
        .iter()
        .find(|spec| spec.target == target)
        .expect("every setup target has a harness spec")
}

pub(super) fn default_ingest_sources(
    target: SetupMcpTarget,
    paths: &SetupPathContext,
    enabled: bool,
) -> Result<Vec<ResolvedIngestSource>> {
    if target != SetupMcpTarget::Nac {
        let mut sources = spec(target)
            .ingest_sources()
            .iter()
            .copied()
            .map(ResolvedIngestSource::from)
            .collect::<Vec<_>>();
        if target == SetupMcpTarget::KiroCli {
            if let Some(kiro_home) = paths.kiro_home.as_deref() {
                let sessions_dir = kiro_home.join("sessions").join("cli");
                let source = sources
                    .first_mut()
                    .expect("Kiro CLI has one default ingest source");
                source.glob = sessions_dir.join("*.jsonl").to_string_lossy().into_owned();
                source.watch_root = sessions_dir.to_string_lossy().into_owned();
            }
        }
        return Ok(sources);
    }
    if !enabled {
        return Ok(vec![ResolvedIngestSource {
            name: "nac".to_string(),
            harness: "nac".to_string(),
            glob: String::new(),
            watch_root: String::new(),
            format: None,
            materialize: false,
        }]);
    }
    let store = paths.resolve_nac_store()?;
    let (glob, watch_root) = store.source_paths()?;
    Ok(vec![ResolvedIngestSource {
        name: "nac".to_string(),
        harness: "nac".to_string(),
        glob,
        watch_root,
        format: Some("nac_sqlite".to_string()),
        materialize: store.auto_ingest,
    }])
}

pub(super) fn mcp_plan(
    target: SetupMcpTarget,
    config_target: &ConfigTarget,
    paths: &SetupPathContext,
) -> Result<McpPlan> {
    let home = &paths.home;
    Ok(match target {
        SetupMcpTarget::ClaudeCode if config_target.requires_explicit_mcp_config() => {
            let mut args = vec![
                "mcp".to_string(),
                "add".to_string(),
                "--transport".to_string(),
                "stdio".to_string(),
                "--scope".to_string(),
                "user".to_string(),
                "moraine".to_string(),
                "--".to_string(),
                "moraine".to_string(),
                "run".to_string(),
                "mcp".to_string(),
                "--config".to_string(),
            ];
            args.push(config_target.path.display().to_string());
            let command = CommandSpec::new("claude", args);
            McpPlan::manual(
                target,
                format!(
                    "Claude Code plugin installs use the default Moraine config. For this custom config target, use manual MCP registration:\n{}",
                    command.display()
                ),
            )
        }
        SetupMcpTarget::ClaudeCode => McpPlan {
            target,
            action: super::McpAction::Execute,
            steps: vec![
                McpPlanStep::warn_and_continue(
                    CommandSpec::new(
                        "claude",
                        [
                            "plugin",
                            "marketplace",
                            "add",
                            "eric-tramel/moraine",
                            "--sparse",
                            ".claude-plugin",
                            "plugins",
                        ],
                    ),
                    "Claude marketplace add failed; continuing because the marketplace may already exist",
                )
                .with_progress(
                    "Adding Claude Code plugin marketplace",
                    "Claude Code marketplace ready",
                    "Claude Code marketplace already present or unavailable",
                    "Claude Code marketplace add failed",
                ),
                McpPlanStep::required(CommandSpec::new(
                    "claude",
                    ["plugin", "marketplace", "update", "moraine"],
                ))
                .with_progress(
                    "Refreshing Claude Code plugin marketplace",
                    "Claude Code marketplace refreshed",
                    "Claude Code marketplace refresh warning",
                    "Claude Code marketplace refresh failed",
                ),
                McpPlanStep::required(CommandSpec::new(
                    "claude",
                    ["plugin", "install", "moraine@moraine"],
                ))
                .with_progress(
                    "Installing Claude Code Moraine plugin",
                    "Claude Code plugin installed",
                    "Claude Code plugin install warning",
                    "Claude Code plugin install failed",
                ),
                McpPlanStep::required(CommandSpec::new(
                    "claude",
                    ["plugin", "update", "moraine@moraine"],
                ))
                .with_progress(
                    "Updating Claude Code Moraine plugin",
                    "Claude Code plugin updated",
                    "Claude Code plugin update warning",
                    "Claude Code plugin update failed",
                ),
                McpPlanStep::warn_and_continue(
                    CommandSpec::new("claude", ["mcp", "remove", "moraine", "--scope", "user"]),
                    "Existing manual Claude Code MCP registration could not be removed; continuing in case it was absent",
                )
                .with_progress(
                    "Cleaning up old Claude Code MCP registration",
                    "Old Claude Code MCP registration removed or absent",
                    "Old Claude Code MCP registration left unchanged",
                    "Old Claude Code MCP cleanup failed",
                ),
            ],
            config_writes: Vec::new(),
            managed_writes: Vec::new(),
            manual_snippet: None,
        },
        SetupMcpTarget::Codex if config_target.requires_explicit_mcp_config() => {
            let command = CommandSpec::new("codex", codex_args(config_target));
            McpPlan::manual(
                target,
                format!(
                    "Codex plugin installs use the default Moraine config. For this custom config target, use manual MCP registration:\n{}",
                    command.display()
                ),
            )
        }
        SetupMcpTarget::Codex => McpPlan {
            target,
            action: super::McpAction::Execute,
            steps: vec![
                McpPlanStep::warn_and_continue(
                    CommandSpec::new(
                        "codex",
                        [
                            "plugin",
                            "marketplace",
                            "add",
                            "eric-tramel/moraine",
                            "--sparse",
                            ".agents/plugins",
                            "--sparse",
                            "plugins/moraine",
                            "--sparse",
                            "plugins/moraine-dev",
                        ],
                    ),
                    "Codex marketplace add failed; continuing because the marketplace may already exist",
                )
                .with_progress(
                    "Adding Codex plugin marketplace",
                    "Codex marketplace ready",
                    "Codex marketplace already present or unavailable",
                    "Codex marketplace add failed",
                ),
                McpPlanStep::required(CommandSpec::new(
                    "codex",
                    ["plugin", "add", "moraine@moraine"],
                ))
                .with_progress(
                    "Installing Codex Moraine plugin",
                    "Codex plugin installed",
                    "Codex plugin install warning",
                    "Codex plugin install failed",
                ),
                McpPlanStep::warn_and_continue(
                    CommandSpec::new("codex", ["mcp", "remove", "moraine"]),
                    "Existing manual Codex MCP registration could not be removed; continuing in case it was absent",
                )
                .with_progress(
                    "Cleaning up old Codex MCP registration",
                    "Old Codex MCP registration removed or absent",
                    "Old Codex MCP registration left unchanged",
                    "Old Codex MCP cleanup failed",
                ),
            ],
            config_writes: Vec::new(),
            managed_writes: Vec::new(),
            manual_snippet: None,
        },
        SetupMcpTarget::Hermes if config_target.requires_explicit_mcp_config() => {
            let command = CommandSpec::new("hermes", hermes_args(config_target));
            McpPlan::manual(
                target,
                format!(
                    "Hermes plugin setup uses the default Moraine config. For this custom config target, use manual MCP registration:\n{}",
                    command.display()
                ),
            )
        }
        SetupMcpTarget::Hermes => McpPlan {
            target,
            action: super::McpAction::Execute,
            steps: vec![
                McpPlanStep::required(CommandSpec::new(
                    "hermes",
                    vec![
                        "plugins".to_string(),
                        "install".to_string(),
                        hermes_plugin_identifier(),
                        "--force".to_string(),
                        "--enable".to_string(),
                    ],
                ))
                .with_progress(
                    "Installing Hermes Moraine plugin",
                    "Hermes plugin installed",
                    "Hermes plugin install warning",
                    "Hermes plugin install failed",
                ),
                McpPlanStep::required_stdout(
                    CommandSpec::new("hermes", ["moraine", "setup", "--no-test"]),
                    "Moraine MCP setup complete",
                )
                .with_progress(
                    "Configuring Hermes Moraine MCP",
                    "Hermes MCP configured",
                    "Hermes MCP setup warning",
                    "Hermes MCP setup failed",
                ),
            ],
            config_writes: Vec::new(),
            managed_writes: Vec::new(),
            manual_snippet: None,
        },
        SetupMcpTarget::KiroCli => {
            let Some(moraine_command) = kiro_moraine_command() else {
                return Ok(McpPlan::manual(
                    target,
                    "Moraine could not resolve the absolute path of its running executable. Register Kiro MCP manually with an absolute, trusted path to the moraine CLI.".to_string(),
                ));
            };
            let registration_args = kiro_args(config_target, &moraine_command);

            let Some(kiro_home) = paths
                .kiro_home
                .clone()
                .or_else(|| home.as_ref().map(|home| home.join(".kiro")))
            else {
                let command = CommandSpec::new("kiro-cli", registration_args);
                return Ok(McpPlan::manual(
                    target,
                    format!(
                        "KIRO_HOME and HOME are not set, so Moraine cannot choose Kiro's global steering directory. Set one of them, then run:\n{}",
                        command.display()
                    ),
                ));
            };

            McpPlan {
                target,
                action: super::McpAction::Execute,
                steps: vec![McpPlanStep::required(CommandSpec::new(
                    "kiro-cli",
                    registration_args,
                ))
                .with_progress(
                    "Registering Moraine MCP in Kiro CLI",
                    "Kiro CLI MCP registered",
                    "Kiro CLI MCP registration warning",
                    "Kiro CLI MCP registration failed",
                )],
                config_writes: Vec::new(),
                managed_writes: vec![ManagedFileWrite::kiro_steering(&kiro_home)],
                manual_snippet: None,
            }
        }
        SetupMcpTarget::KimiCli => McpPlan::replace_registration(
            target,
            CommandSpec::new("kimi", ["mcp", "remove", "moraine"]),
            McpPlanStep::required(CommandSpec::new("kimi", kimi_args(config_target)))
                .with_progress(
                    "Registering Moraine MCP in Kimi CLI",
                    "Kimi CLI MCP registered",
                    "Kimi CLI MCP registration warning",
                    "Kimi CLI MCP registration failed",
                ),
        ),
        SetupMcpTarget::QwenCode => McpPlan {
            target,
            action: super::McpAction::Execute,
            steps: vec![
                McpPlanStep::required(CommandSpec::new("qwen", qwen_args(config_target)))
                    .with_progress(
                        "Registering Moraine MCP in Qwen Code",
                        "Qwen Code MCP registered",
                        "Qwen Code MCP registration warning",
                        "Qwen Code MCP registration failed",
                    ),
            ],
            config_writes: Vec::new(),
            managed_writes: Vec::new(),
            manual_snippet: None,
        },
        SetupMcpTarget::Nac => nac_mcp_plan(paths, config_target)?,
        SetupMcpTarget::OpenCode => McpPlan::write_config(
            target,
            home.as_ref()
                .map(|home| McpConfigWrite::opencode(home, config_target)),
            opencode_snippet(config_target),
        ),
        SetupMcpTarget::Cursor => McpPlan::write_config(
            target,
            home.as_ref()
                .map(|home| McpConfigWrite::cursor(home, config_target)),
            cursor_snippet(config_target),
        ),
        SetupMcpTarget::PiCodingAgent => {
            let mut plan = McpPlan::write_config(
                target,
                home.as_ref()
                    .map(|home| McpConfigWrite::pi(home, config_target)),
                pi_snippet(config_target),
            );
            if !plan.config_writes.is_empty() {
                plan.steps.push(
                    McpPlanStep::required(CommandSpec::new(
                        "pi",
                        ["install", "npm:pi-mcp-extension"],
                    ))
                    .with_progress(
                        "Installing Pi MCP extension",
                        "Pi MCP extension installed",
                        "Pi MCP extension install warning",
                        "Pi MCP extension install failed",
                    ),
                );
            }
            plan
        }
    })
}

fn nac_mcp_plan(paths: &SetupPathContext, config_target: &ConfigTarget) -> Result<McpPlan> {
    let snapshot = paths.nac_config_snapshot()?;
    let write = McpConfigWrite::nac(snapshot, config_target)?;
    Ok(McpPlan::write_config(
        SetupMcpTarget::Nac,
        Some(write),
        nac_mcp_snippet(config_target),
    ))
}

fn nac_mcp_snippet(config_target: &ConfigTarget) -> String {
    let args = mcp_run_args(config_target)
        .into_iter()
        .map(|arg| format!("\"{}\"", arg.replace('\\', "\\\\").replace('"', "\\\"")))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "[mcp_servers.moraine]\nenabled = true\ntransport = \"stdio\"\ncommand = \"moraine\"\nargs = [{args}]"
    )
}

#[derive(Debug, Clone)]
pub(super) struct ManagedFileWrite {
    path: PathBuf,
    label: &'static str,
    content: &'static str,
}

impl ManagedFileWrite {
    fn kiro_steering(kiro_home: &Path) -> Self {
        Self {
            path: kiro_home.join("steering").join("moraine.md"),
            label: "Kiro steering",
            content: KIRO_STEERING,
        }
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn label(&self) -> &'static str {
        self.label
    }

    pub(super) fn content(&self) -> &'static str {
        self.content
    }
}

pub(super) fn mcp_run_args(config_target: &ConfigTarget) -> Vec<String> {
    let mut args = vec!["run".to_string(), "mcp".to_string()];
    if config_target.requires_explicit_mcp_config() {
        args.push("--config".to_string());
        args.push(config_target.path.display().to_string());
    }
    args
}

pub(super) fn opencode_command(config_target: &ConfigTarget) -> Vec<String> {
    iter::once("moraine".to_string())
        .chain(mcp_run_args(config_target))
        .collect()
}

#[derive(Debug, Clone, Copy)]
pub(super) enum McpConfigFormat {
    Json,
    Jsonc,
    Toml,
}
#[derive(Debug, Clone)]
pub(super) struct McpConfigWrite {
    path: PathBuf,
    kind: McpConfigKind,
    command: Vec<String>,
    nac_write: Option<nac::PreparedMcpWrite>,
}

impl McpConfigWrite {
    pub(super) fn cursor(home: &Path, config_target: &ConfigTarget) -> Self {
        Self {
            path: home.join(".cursor").join("mcp.json"),
            kind: McpConfigKind::Cursor,
            command: mcp_run_args(config_target),
            nac_write: None,
        }
    }

    pub(super) fn pi(home: &Path, config_target: &ConfigTarget) -> Self {
        Self {
            path: home.join(".pi").join("agent").join("mcp.json"),
            kind: McpConfigKind::Pi,
            command: mcp_run_args(config_target),
            nac_write: None,
        }
    }

    pub(super) fn opencode(home: &Path, config_target: &ConfigTarget) -> Self {
        Self {
            path: home.join(".config").join("opencode").join("opencode.json"),
            kind: McpConfigKind::OpenCode,
            command: opencode_command(config_target),
            nac_write: None,
        }
    }

    pub(super) fn nac(snapshot: nac::ConfigSnapshot, config_target: &ConfigTarget) -> Result<Self> {
        let command = mcp_run_args(config_target);
        let nac_write = snapshot.prepare_mcp_write(&command)?;
        Ok(Self {
            path: nac_write.path().to_path_buf(),
            kind: McpConfigKind::Nac,
            command,
            nac_write: Some(nac_write),
        })
    }

    #[cfg(test)]
    pub(super) fn nac_path(path: PathBuf, config_target: &ConfigTarget) -> Result<Self> {
        Self::nac(nac::ConfigSnapshot::read(path)?, config_target)
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn label(&self) -> &'static str {
        self.kind.label()
    }

    pub(super) fn format(&self) -> McpConfigFormat {
        self.kind.format()
    }

    pub(super) fn nac_rendered(&self) -> Option<&[u8]> {
        self.nac_write.as_ref().map(|write| write.rendered())
    }

    pub(super) fn nac_is_unchanged(&self) -> bool {
        self.nac_write
            .as_ref()
            .is_some_and(nac::PreparedMcpWrite::is_unchanged)
    }

    pub(super) fn verify_nac_snapshot_current(&self) -> Result<()> {
        self.nac_write
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("{} is not a NAC MCP write", self.label()))?
            .verify_snapshot_current()
    }

    pub(super) fn merge_into(&self, root: &mut Map<String, Value>) -> Result<()> {
        match self.kind {
            McpConfigKind::Cursor => {
                let servers = object_entry_mut(root, "mcpServers")?;
                servers.insert("moraine".to_string(), self.server_value());
            }
            McpConfigKind::Pi => {
                let servers = object_entry_mut(root, "mcpServers")?;
                servers.insert("moraine".to_string(), self.server_value());
            }
            McpConfigKind::OpenCode => {
                root.entry("$schema".to_string())
                    .or_insert_with(|| serde_json::json!("https://opencode.ai/config.json"));
                let servers = object_entry_mut(root, "mcp")?;
                servers.insert("moraine".to_string(), self.server_value());
            }
            McpConfigKind::Nac => bail!("NAC MCP config uses TOML, not JSON"),
        }
        Ok(())
    }

    fn server_value(&self) -> Value {
        match self.kind {
            McpConfigKind::Cursor => serde_json::json!({
                "type": "stdio",
                "command": "moraine",
                "args": self.command.clone(),
            }),
            McpConfigKind::Pi => serde_json::json!({
                "transport": "stdio",
                "command": "moraine",
                "args": self.command.clone(),
                "lifecycle": "eager",
            }),
            McpConfigKind::OpenCode => serde_json::json!({
                "type": "local",
                "command": self.command.clone(),
                "enabled": true,
            }),
            McpConfigKind::Nac => Value::Null,
        }
    }

    pub(super) fn snippet_root(&self) -> Map<String, Value> {
        let mut root = Map::new();
        self.merge_into(&mut root)
            .expect("snippet roots are built from empty JSON objects");
        root
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum McpConfigKind {
    Cursor,
    Pi,
    OpenCode,
    Nac,
}

impl McpConfigKind {
    fn label(self) -> &'static str {
        match self {
            McpConfigKind::Cursor => "Cursor",
            McpConfigKind::Pi => "Pi",
            McpConfigKind::OpenCode => "OpenCode",
            McpConfigKind::Nac => "NAC",
        }
    }

    fn format(self) -> McpConfigFormat {
        match self {
            McpConfigKind::Cursor | McpConfigKind::Pi => McpConfigFormat::Json,
            McpConfigKind::OpenCode => McpConfigFormat::Jsonc,
            McpConfigKind::Nac => McpConfigFormat::Toml,
        }
    }
}

fn object_entry_mut<'a>(
    root: &'a mut Map<String, Value>,
    key: &str,
) -> Result<&'a mut Map<String, Value>> {
    let value = root
        .entry(key.to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    if !value.is_object() {
        bail!("{key} must be a JSON object");
    }
    Ok(value
        .as_object_mut()
        .expect("value was checked as a JSON object"))
}

fn codex_args(config_target: &ConfigTarget) -> Vec<String> {
    let mut args = vec![
        "mcp".to_string(),
        "add".to_string(),
        "moraine".to_string(),
        "--".to_string(),
        "moraine".to_string(),
    ];
    args.extend(mcp_run_args(config_target));
    args
}

pub(super) fn hermes_args(config_target: &ConfigTarget) -> Vec<String> {
    let mut args = vec![
        "mcp".to_string(),
        "add".to_string(),
        "moraine".to_string(),
        "--command".to_string(),
        "moraine".to_string(),
        "--args".to_string(),
    ];
    args.extend(mcp_run_args(config_target));
    args
}

pub(super) fn hermes_plugin_identifier() -> String {
    if let Some(source) = env::var_os("MORAINE_HERMES_PLUGIN_SOURCE")
        .and_then(|value| value.into_string().ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        return source;
    }

    hermes_local_plugin_identifier().unwrap_or_else(|| HERMES_PLUGIN_REMOTE_IDENTIFIER.to_string())
}

fn hermes_local_plugin_identifier() -> Option<String> {
    let mut seen = BTreeSet::new();
    let mut candidates = Vec::new();

    candidates.push(PathBuf::from(env!("CARGO_MANIFEST_DIR")));

    if let Ok(exe) = env::current_exe() {
        candidates.push(exe);
    }

    for candidate in candidates {
        let Some(root) = find_moraine_source_root(&candidate) else {
            continue;
        };
        let Ok(root) = root.canonicalize() else {
            continue;
        };
        if !seen.insert(root.clone()) {
            continue;
        }
        if let Some(identifier) = hermes_plugin_identifier_for_root(&root) {
            return Some(identifier);
        }
    }

    None
}

fn find_moraine_source_root(start: &Path) -> Option<PathBuf> {
    let mut current = if start.is_file() {
        start.parent()?.to_path_buf()
    } else {
        start.to_path_buf()
    };

    loop {
        if current.join("Cargo.toml").is_file()
            && current
                .join(HERMES_PLUGIN_RELATIVE_PATH)
                .join("plugin.yaml")
                .is_file()
        {
            return Some(current);
        }

        if !current.pop() {
            return None;
        }
    }
}

pub(super) fn hermes_plugin_identifier_for_root(root: &Path) -> Option<String> {
    let plugin = root.join(HERMES_PLUGIN_RELATIVE_PATH).join("plugin.yaml");
    if !plugin.is_file() {
        return None;
    }

    Some(format!(
        "{}#{}",
        file_url_for_path(root),
        HERMES_PLUGIN_RELATIVE_PATH
    ))
}

fn file_url_for_path(path: &Path) -> String {
    let mut raw = path.to_string_lossy().replace('\\', "/");
    if !raw.starts_with('/') {
        raw.insert(0, '/');
    }
    format!("file://{}", percent_encode_file_path(&raw))
}

fn percent_encode_file_path(path: &str) -> String {
    let mut encoded = String::with_capacity(path.len());
    for byte in path.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~' | b'/') {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

fn kimi_args(config_target: &ConfigTarget) -> Vec<String> {
    let mut args = vec![
        "mcp".to_string(),
        "add".to_string(),
        "--transport".to_string(),
        "stdio".to_string(),
        "moraine".to_string(),
        "--".to_string(),
        "moraine".to_string(),
    ];
    args.extend(mcp_run_args(config_target));
    args
}

fn qwen_args(config_target: &ConfigTarget) -> Vec<String> {
    let mut args = vec![
        "mcp".to_string(),
        "add".to_string(),
        "--scope".to_string(),
        "user".to_string(),
        "--transport".to_string(),
        "stdio".to_string(),
        "moraine".to_string(),
        "moraine".to_string(),
        "--".to_string(),
    ];
    args.extend(mcp_run_args(config_target));
    args
}

fn kiro_moraine_command() -> Option<String> {
    let executable = env::current_exe().ok()?;
    if !executable.is_absolute() {
        return None;
    }
    executable.into_os_string().into_string().ok()
}

pub(super) fn kiro_args(config_target: &ConfigTarget, moraine_command: &str) -> Vec<String> {
    let command_args = serde_json::to_string(&mcp_run_args(config_target))
        .expect("Moraine MCP arguments always serialize as JSON");
    vec![
        "mcp".to_string(),
        "add".to_string(),
        "--name".to_string(),
        "moraine".to_string(),
        "--scope".to_string(),
        "global".to_string(),
        "--command".to_string(),
        moraine_command.to_string(),
        "--args".to_string(),
        command_args,
        "--force".to_string(),
    ]
}

fn cursor_snippet(config_target: &ConfigTarget) -> String {
    snippet(
        "Add this server to ~/.cursor/mcp.json for global Cursor use or .cursor/mcp.json for a project",
        Value::Object(McpConfigWrite::cursor(Path::new("~"), config_target).snippet_root()),
    )
}

fn opencode_snippet(config_target: &ConfigTarget) -> String {
    snippet(
        "Add this server to ~/.config/opencode/opencode.json",
        Value::Object(McpConfigWrite::opencode(Path::new("~"), config_target).snippet_root()),
    )
}

fn pi_snippet(config_target: &ConfigTarget) -> String {
    format!(
        "Install the Pi MCP extension first:\npi install npm:pi-mcp-extension\n\nThen add this server to ~/.pi/agent/mcp.json:\n{}",
        serde_json::to_string_pretty(&Value::Object(
            McpConfigWrite::pi(Path::new("~"), config_target).snippet_root()
        ))
        .unwrap_or_else(|_| "{}".to_string())
    )
}

fn snippet(intro: &str, value: Value) -> String {
    format!(
        "{intro}:\n{}",
        serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string())
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "moraine-nac-setup-{name}-{}-{nonce}",
            std::process::id()
        ))
    }

    #[test]
    fn cursor_probes_include_linux_config_location() {
        let home = Path::new("/home/example");
        assert!(ProbePaths::Cursor
            .paths(home)
            .contains(&home.join(".config").join("Cursor")));
    }

    #[test]
    fn nac_home_obeys_explicit_and_xdg_precedence() {
        let home = PathBuf::from("/home/example");
        let xdg = PathBuf::from("/xdg");
        let explicit = PathBuf::from("/custom/nac");
        let mut paths = SetupPathContext {
            launch_cwd: PathBuf::from("/workspace"),
            home: Some(home),
            xdg_config_home: Some(xdg.clone()),
            nac_home: None,
            nac_snapshot: OnceCell::new(),
            nac_expected_content: OnceCell::new(),
        };
        assert_eq!(paths.nac_home_dir(), xdg.join("nac"));
        paths.xdg_config_home = None;
        assert_eq!(
            paths.nac_home_dir(),
            PathBuf::from("/home/example/.config/nac")
        );
        paths.xdg_config_home = Some(xdg);
        paths.nac_home = Some(explicit.clone());
        assert_eq!(paths.nac_home_dir(), explicit);
    }

    #[test]
    fn nac_default_store_becomes_an_absolute_ingest_source() {
        let nac_home = temp_dir("default-store");
        let paths = SetupPathContext {
            launch_cwd: PathBuf::from("/workspace"),
            home: None,
            xdg_config_home: None,
            nac_home: Some(nac_home.clone()),
            nac_snapshot: OnceCell::new(),
            nac_expected_content: OnceCell::new(),
        };
        let sources =
            default_ingest_sources(SetupMcpTarget::Nac, &paths, true).expect("resolve NAC source");
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].name, "nac");
        assert_eq!(sources[0].harness, "nac");
        assert_eq!(sources[0].glob, nac_home.join("store.db").to_string_lossy());
        assert_eq!(sources[0].watch_root, nac_home.to_string_lossy());
        assert_eq!(sources[0].format.as_deref(), Some("nac_sqlite"));
    }

    #[test]
    fn nac_source_uses_shared_literal_glob_escaping() {
        let root = temp_dir("glob-metacharacters");
        let nac_home = root.join("nac");
        fs::create_dir_all(&nac_home).expect("create NAC home");
        let store = root.join("[workspace]*?").join("store.db");
        fs::write(
            nac_home.join("config.toml"),
            format!(
                "[storage]\nstore_path = {:?}\n",
                store.to_str().expect("UTF-8 test path")
            ),
        )
        .expect("write NAC config");
        let paths = SetupPathContext {
            launch_cwd: root.clone(),
            home: None,
            xdg_config_home: None,
            nac_home: Some(nac_home),
            nac_snapshot: OnceCell::new(),
            nac_expected_content: OnceCell::new(),
        };

        let sources = default_ingest_sources(SetupMcpTarget::Nac, &paths, true)
            .expect("resolve escaped NAC source");
        assert_eq!(
            sources[0].glob,
            moraine_config::escape_literal_glob(store.to_str().expect("UTF-8 test path"))
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn nac_relative_store_is_not_silently_ingested() {
        let root = temp_dir("relative-store");
        let nac_home = root.join("config");
        fs::create_dir_all(&nac_home).expect("create NAC config dir");
        fs::write(
            nac_home.join("config.toml"),
            "[storage]\nstore_path = \"state/store.db\"\n",
        )
        .expect("write NAC config");
        let paths = SetupPathContext {
            launch_cwd: root.join("workspace"),
            home: None,
            xdg_config_home: None,
            nac_home: Some(nac_home),
            nac_snapshot: OnceCell::new(),
            nac_expected_content: OnceCell::new(),
        };
        let sources = default_ingest_sources(SetupMcpTarget::Nac, &paths, true)
            .expect("resolve relative NAC source");
        assert_eq!(sources.len(), 1);
        assert!(!sources[0].materializes());
        let manual = paths
            .nac_manual_ingest_guidance()
            .expect("resolve manual guidance")
            .expect("relative-store instructions");
        assert!(manual.contains("launch-directory-relative"));
        assert!(manual.contains("format = \"nac_sqlite\""));
        assert!(manual.contains("name = \"nac-workspace\""));
        assert!(manual.contains("unique source name"));
        assert!(!manual.contains("name = \"nac\"\n"));
        let expected_store = root.join("workspace/state/store.db");
        assert!(manual.contains(expected_store.to_string_lossy().as_ref()));
        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn missing_home_variables_use_launch_scoped_fallback() {
        let paths = SetupPathContext {
            launch_cwd: PathBuf::from("/workspace/project"),
            home: None,
            xdg_config_home: None,
            nac_home: None,
            nac_snapshot: OnceCell::new(),
            nac_expected_content: OnceCell::new(),
        };
        assert_eq!(
            paths.nac_home_dir(),
            PathBuf::from("/workspace/project/.nac")
        );
        let store = paths.resolve_nac_store().expect("resolve launch fallback");
        assert_eq!(
            store.path,
            PathBuf::from("/workspace/project/.nac/store.db")
        );
        assert!(!store.auto_ingest);
    }

    #[test]
    fn relative_nac_home_is_launch_scoped_and_requires_manual_ingest() {
        let paths = SetupPathContext {
            launch_cwd: PathBuf::from("/workspace/project"),
            home: Some(PathBuf::from("/home/example")),
            xdg_config_home: None,
            nac_home: Some(PathBuf::from("nac-state")),
            nac_snapshot: OnceCell::new(),
            nac_expected_content: OnceCell::new(),
        };
        assert_eq!(
            paths.nac_config_path(),
            PathBuf::from("/workspace/project/nac-state/config.toml")
        );
        let store = paths
            .resolve_nac_store()
            .expect("resolve relative NAC_HOME");
        assert_eq!(
            store.path,
            PathBuf::from("/workspace/project/nac-state/store.db")
        );
        assert!(!store.auto_ingest);
        assert_eq!(
            spec(SetupMcpTarget::Nac).default_probe_paths(&paths),
            vec![PathBuf::from("/workspace/project/nac-state")]
        );
    }

    #[test]
    fn absolute_store_override_is_safe_even_with_relative_nac_home() {
        let root = temp_dir("absolute-override");
        let launch = root.join("workspace");
        let nac_home = launch.join("relative-nac");
        fs::create_dir_all(&nac_home).expect("create relative NAC home");
        let absolute_store = root.join("durable/store.db");
        fs::write(
            nac_home.join("config.toml"),
            format!(
                "storage = {{ store_path = {:?} }}\n",
                absolute_store.to_string_lossy()
            ),
        )
        .expect("write inline storage config");
        let paths = SetupPathContext {
            launch_cwd: launch,
            home: None,
            xdg_config_home: None,
            nac_home: Some(PathBuf::from("relative-nac")),
            nac_snapshot: OnceCell::new(),
            nac_expected_content: OnceCell::new(),
        };
        let store = paths
            .resolve_nac_store()
            .expect("resolve absolute override");
        assert_eq!(store.path, absolute_store);
        assert!(store.auto_ingest);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn nac_store_config_is_parsed_once_per_setup_context() {
        let nac_home = temp_dir("parse-once");
        fs::create_dir_all(&nac_home).expect("create NAC home");
        let config_path = nac_home.join("config.toml");
        fs::write(
            &config_path,
            "[storage]\nstore_path = \"/first/store.db\"\n",
        )
        .expect("write first NAC config");
        let paths = SetupPathContext {
            launch_cwd: PathBuf::from("/workspace"),
            home: None,
            xdg_config_home: None,
            nac_home: Some(nac_home.clone()),
            nac_snapshot: OnceCell::new(),
            nac_expected_content: OnceCell::new(),
        };
        assert_eq!(
            paths.resolve_nac_store().expect("first resolution").path,
            PathBuf::from("/first/store.db")
        );
        fs::write(
            &config_path,
            "[storage]\nstore_path = \"/second/store.db\"\n",
        )
        .expect("rewrite NAC config");
        assert_eq!(
            paths.resolve_nac_store().expect("cached resolution").path,
            PathBuf::from("/first/store.db")
        );
        let _ = fs::remove_dir_all(nac_home);
    }

    #[test]
    fn nac_store_resolution_rejects_malformed_storage_without_guessing() {
        for (name, content, expected) in [
            (
                "bad-storage",
                "storage = 7\n",
                "storage must be a TOML table",
            ),
            (
                "bad-store-path",
                "[storage]\nstore_path = 7\n",
                "storage.store_path must be a TOML string",
            ),
            ("bad-toml", "storage = [\n", "is not valid NAC TOML"),
        ] {
            let nac_home = temp_dir(name);
            fs::create_dir_all(&nac_home).expect("create malformed NAC home");
            fs::write(nac_home.join("config.toml"), content).expect("write malformed NAC config");
            let paths = SetupPathContext {
                launch_cwd: PathBuf::from("/workspace"),
                home: None,
                xdg_config_home: None,
                nac_home: Some(nac_home.clone()),
                nac_snapshot: OnceCell::new(),
                nac_expected_content: OnceCell::new(),
            };
            let error = paths
                .resolve_nac_store()
                .expect_err("reject malformed NAC storage");
            assert!(
                format!("{error:#}").contains(expected),
                "unexpected error: {error:#}"
            );
            let _ = fs::remove_dir_all(nac_home);
        }
    }

    #[cfg(unix)]
    #[test]
    fn nac_source_rejects_non_utf8_resolved_paths() {
        let invalid_component = std::ffi::OsString::from_vec(vec![b'n', b'a', b'c', b'-', 0xff]);
        let nac_home = temp_dir("non-utf8-parent").join(invalid_component);
        let paths = SetupPathContext {
            launch_cwd: PathBuf::from("/workspace"),
            home: None,
            xdg_config_home: None,
            nac_home: Some(nac_home),
            nac_snapshot: OnceCell::new(),
            nac_expected_content: OnceCell::new(),
        };

        let error = default_ingest_sources(SetupMcpTarget::Nac, &paths, true)
            .expect_err("reject non-UTF-8 store/watch paths");
        assert!(format!("{error:#}").contains("not valid UTF-8"));
    }
}
