use std::iter;
use std::path::{Path, PathBuf};

use anyhow::{bail, Result};
use serde_json::{Map, Value};
use toml_edit::{value as toml_value, Table};

use super::{CommandSpec, ConfigTarget, McpPlan, McpPlanStep, SetupMcpTarget};

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

impl DefaultIngestSource {
    pub(super) fn name(self) -> &'static str {
        self.name
    }

    pub(super) fn harness(self) -> &'static str {
        self.harness
    }

    pub(super) fn to_table(self, enabled: bool) -> Table {
        let mut table = Table::new();
        table["name"] = toml_value(self.name);
        table["harness"] = toml_value(self.harness);
        table["enabled"] = toml_value(enabled);
        table["glob"] = toml_value(self.glob);
        table["watch_root"] = toml_value(self.watch_root);
        if let Some(format) = self.format {
            table["format"] = toml_value(format);
        }
        table
    }

    pub(super) fn reconcile_table(
        self,
        table: &mut Table,
        enabled: bool,
    ) -> DefaultIngestSourceUpdate {
        let enabled_changed = set_bool(table, "enabled", enabled);
        let mut metadata_changed = false;

        metadata_changed |= set_str(table, "name", self.name);
        metadata_changed |= set_str(table, "harness", self.harness);
        metadata_changed |= set_str(table, "glob", self.glob);
        metadata_changed |= set_str(table, "watch_root", self.watch_root);
        metadata_changed |= match self.format {
            Some(format) => set_str(table, "format", format),
            None => table.remove("format").is_some(),
        };

        DefaultIngestSourceUpdate {
            enabled_changed,
            metadata_changed,
        }
    }
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

    pub(super) fn default_probe_paths(self, home: &Path) -> Vec<PathBuf> {
        self.probe_paths.paths(home)
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

const KIMI_INGEST: [DefaultIngestSource; 1] = [DefaultIngestSource {
    name: "kimi-cli",
    harness: "kimi-cli",
    glob: "~/.kimi/sessions/**/wire.jsonl",
    watch_root: "~/.kimi/sessions",
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

const PI_INGEST: [DefaultIngestSource; 1] = [DefaultIngestSource {
    name: "pi",
    harness: "pi-coding-agent",
    glob: "~/.pi/agent/sessions/**/*.jsonl",
    watch_root: "~/.pi/agent/sessions",
    format: Some("jsonl"),
}];

const SPECS: [HarnessSpec; 7] = [
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
        setup_kind: "MCP",
        programs: &["hermes"],
        probe_paths: ProbePaths::None,
        ingest_sources: &HERMES_INGEST,
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

pub(super) fn default_ingest_sources(target: SetupMcpTarget) -> &'static [DefaultIngestSource] {
    spec(target).ingest_sources()
}

pub(super) fn mcp_plan(
    target: SetupMcpTarget,
    config_target: &ConfigTarget,
    home: Option<PathBuf>,
) -> McpPlan {
    match target {
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
                    ["plugin", "install", "moraine@moraine"],
                ))
                .with_progress(
                    "Installing Claude Code Moraine plugin",
                    "Claude Code plugin installed",
                    "Claude Code plugin install warning",
                    "Claude Code plugin install failed",
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
            manual_snippet: None,
        },
        SetupMcpTarget::Hermes => McpPlan::replace_registration(
            target,
            CommandSpec::new("hermes", ["mcp", "remove", "moraine"]),
            McpPlanStep::required_stdout(
                CommandSpec::new("hermes", hermes_args(config_target)).with_stdin("\n"),
                "tools enabled",
            )
            .with_progress(
                "Registering Moraine MCP in Hermes",
                "Hermes MCP tools enabled",
                "Hermes MCP registration warning",
                "Hermes MCP registration failed",
            ),
        ),
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
}

#[derive(Debug, Clone)]
pub(super) struct McpConfigWrite {
    path: PathBuf,
    kind: McpConfigKind,
    command: Vec<String>,
}

impl McpConfigWrite {
    pub(super) fn cursor(home: &Path, config_target: &ConfigTarget) -> Self {
        Self {
            path: home.join(".cursor").join("mcp.json"),
            kind: McpConfigKind::Cursor,
            command: mcp_run_args(config_target),
        }
    }

    pub(super) fn pi(home: &Path, config_target: &ConfigTarget) -> Self {
        Self {
            path: home.join(".pi").join("agent").join("mcp.json"),
            kind: McpConfigKind::Pi,
            command: mcp_run_args(config_target),
        }
    }

    pub(super) fn opencode(home: &Path, config_target: &ConfigTarget) -> Self {
        Self {
            path: home.join(".config").join("opencode").join("opencode.json"),
            kind: McpConfigKind::OpenCode,
            command: opencode_command(config_target),
        }
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
        }
    }

    fn snippet_root(&self) -> Map<String, Value> {
        let mut root = Map::new();
        self.merge_into(&mut root)
            .expect("snippet roots are built from empty JSON objects");
        root
    }
}

#[derive(Debug, Clone, Copy)]
enum McpConfigKind {
    Cursor,
    Pi,
    OpenCode,
}

impl McpConfigKind {
    fn label(self) -> &'static str {
        match self {
            McpConfigKind::Cursor => "Cursor",
            McpConfigKind::Pi => "Pi",
            McpConfigKind::OpenCode => "OpenCode",
        }
    }

    fn format(self) -> McpConfigFormat {
        match self {
            McpConfigKind::Cursor | McpConfigKind::Pi => McpConfigFormat::Json,
            McpConfigKind::OpenCode => McpConfigFormat::Jsonc,
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

fn hermes_args(config_target: &ConfigTarget) -> Vec<String> {
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
