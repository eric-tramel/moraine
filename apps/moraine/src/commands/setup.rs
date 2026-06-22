use anyhow::{bail, Context, Result};
use dialoguer::console::{style, Key, Style, Term};
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::BTreeSet;
use std::env;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, IsTerminal, Write};
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cli::{SetupArgs, SetupMcpTarget};
use crate::render::{CliOutput, OutputMode};
use toml_edit::{ArrayOfTables, DocumentMut, Item, Table};

mod harnesses;
use harnesses::{McpConfigFormat, McpConfigWrite};

const DEFAULT_CONFIG_TEMPLATE: &str = include_str!("../../../../config/moraine.toml");
const HOST_WIDE_ACCESS_WARNING: &str =
    "Selected harness integrations get host-wide Moraine session history access visible to your user.";

pub(super) fn handle(
    output: &CliOutput,
    raw_config: Option<PathBuf>,
    args: SetupArgs,
) -> Result<ExitCode> {
    let target = resolve_setup_config_target(raw_config)?;
    let interactive = can_prompt(output);
    let mut runner = RealCommandRunner;
    let report = run_setup(output, &args, target, interactive, &mut runner)?;
    render_report(output, &report)?;

    if report.success {
        Ok(ExitCode::SUCCESS)
    } else {
        Ok(ExitCode::from(1))
    }
}

fn can_prompt(output: &CliOutput) -> bool {
    !output.is_json() && std::io::stdin().is_terminal() && std::io::stderr().is_terminal()
}

fn run_setup(
    output: &CliOutput,
    args: &SetupArgs,
    target: ConfigTarget,
    interactive: bool,
    runner: &mut dyn CommandRunner,
) -> Result<SetupReport> {
    if output.is_json() && !args.yes && !args.dry_run {
        bail!("`moraine --output json setup` requires --yes or --dry-run");
    }

    let mut config = if args.skip_config {
        ConfigReport::skipped(&target.path, "skipped by --skip-config")
    } else {
        setup_config(args, &target, interactive)?
    };

    let mut mcp_targets = Vec::new();
    if !args.skip_mcp {
        let config_allows_mcp =
            args.skip_config || args.dry_run || config.status == SetupStatus::Ok;
        if config_allows_mcp {
            let selections = setup_target_selections(args, interactive, runner)?;
            if selections.apply_ingest && !args.skip_config && config.status == SetupStatus::Ok {
                match apply_ingest_selections_to_config(&target.path, &selections.targets) {
                    Ok(update) => config.apply_ingest_update(update),
                    Err(exc) => {
                        config = ConfigReport::error(
                            &target.path,
                            "ingest_update_failed",
                            &format!("failed to update ingest source selections: {exc}"),
                        );
                    }
                }
            }

            let harness_targets = selections.harness_targets();
            if config.status == SetupStatus::Ok || args.skip_config || args.dry_run {
                let targets_confirmed_by_selection = selections.confirmed_harness;
                let mut progress = SetupProgress::from_output(output);
                for mcp_target in harness_targets {
                    let report = setup_mcp_target_with_progress(
                        args,
                        &target,
                        mcp_target,
                        interactive,
                        targets_confirmed_by_selection,
                        &mut progress,
                        runner,
                    )?;
                    mcp_targets.push(report);
                }
                progress.finish();
            } else {
                for mcp_target in harness_targets {
                    mcp_targets.push(McpTargetReport::skipped(
                        mcp_target,
                        "config setup did not complete; skipping MCP/plugin setup",
                    ));
                }
            }
        } else {
            for mcp_target in dedup_targets(&args.mcp_targets) {
                mcp_targets.push(McpTargetReport::skipped(
                    mcp_target,
                    "config setup did not complete; skipping MCP/plugin setup",
                ));
            }
        }
    }

    let success = config.status != SetupStatus::Error
        && mcp_targets
            .iter()
            .all(|target| target.status != SetupStatus::Error);

    Ok(SetupReport {
        success,
        config,
        mcp_targets,
    })
}

#[derive(Debug, Clone)]
struct ConfigTarget {
    path: PathBuf,
    source: ConfigTargetSource,
}

impl ConfigTarget {
    fn requires_explicit_mcp_config(&self) -> bool {
        self.source != ConfigTargetSource::HomeDefault
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfigTargetSource {
    Cli,
    HomeDefault,
}

fn resolve_setup_config_target(raw_config: Option<PathBuf>) -> Result<ConfigTarget> {
    resolve_setup_config_target_with(raw_config, env::var_os("HOME"))
}

fn resolve_setup_config_target_with(
    raw_config: Option<PathBuf>,
    home: Option<std::ffi::OsString>,
) -> Result<ConfigTarget> {
    if let Some(path) = raw_config {
        return Ok(ConfigTarget {
            path: absolutize_config_path(path)?,
            source: ConfigTargetSource::Cli,
        });
    }

    if let Some(home) = home {
        return Ok(ConfigTarget {
            path: PathBuf::from(home).join(".moraine").join("config.toml"),
            source: ConfigTargetSource::HomeDefault,
        });
    }

    bail!("cannot choose setup config path: pass --config or set HOME");
}

fn absolutize_config_path(path: PathBuf) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path);
    }
    Ok(env::current_dir()
        .context("failed to resolve current directory for config path")?
        .join(path))
}

fn setup_config(
    args: &SetupArgs,
    target: &ConfigTarget,
    interactive: bool,
) -> Result<ConfigReport> {
    match inspect_config(&target.path) {
        ConfigState::Missing => {
            if args.dry_run {
                return Ok(ConfigReport::planned(
                    &target.path,
                    "would_create",
                    "default config would be written",
                ));
            }

            let should_write = args.yes
                || (interactive
                    && prompt_yes_no(
                        &format!("Write default Moraine config to {}?", target.path.display()),
                        true,
                    )?);
            if !should_write && !interactive {
                return Ok(ConfigReport::error(
                    &target.path,
                    "missing",
                    "config is missing; rerun with --yes to write the default template or --dry-run to preview",
                ));
            }
            if !should_write {
                return Ok(ConfigReport::skipped(
                    &target.path,
                    "user declined config creation",
                ));
            }

            write_default_config(&target.path)
                .with_context(|| format!("failed to write {}", target.path.display()))?;
            Ok(ConfigReport::ok(
                &target.path,
                "created",
                "default config written",
            ))
        }
        ConfigState::Valid => Ok(ConfigReport::ok(
            &target.path,
            "unchanged",
            "existing config loads successfully",
        )),
        ConfigState::Invalid(message) => {
            if args.dry_run {
                let action = if args.repair_config {
                    "would_repair"
                } else {
                    "needs_repair"
                };
                return Ok(ConfigReport::planned(&target.path, action, &message));
            }

            let should_repair = args.repair_config
                || (interactive
                    && prompt_yes_no(
                        &format!(
                            "Config {} is invalid. Back it up and write the default template?",
                            target.path.display()
                        ),
                        false,
                    )?);

            if !should_repair {
                return Ok(ConfigReport::error(
                    &target.path,
                    "invalid",
                    "config is invalid; rerun with --repair-config to back it up and write the default template",
                ));
            }

            let backup_path = repair_invalid_config(&target.path)
                .with_context(|| format!("failed to repair {}", target.path.display()))?;
            Ok(ConfigReport::ok_with_backup(
                &target.path,
                backup_path,
                "repaired",
                "invalid config backed up and default config written",
            ))
        }
        ConfigState::Unreadable(message) => Ok(ConfigReport::error(
            &target.path,
            "unreadable",
            &format!("config could not be read and will not be overwritten: {message}"),
        )),
    }
}

enum ConfigState {
    Missing,
    Valid,
    Invalid(String),
    Unreadable(String),
}

fn inspect_config(path: &Path) -> ConfigState {
    match path.try_exists() {
        Ok(false) => return ConfigState::Missing,
        Err(exc) => return ConfigState::Unreadable(exc.to_string()),
        Ok(true) => {}
    }

    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(exc) => return ConfigState::Unreadable(exc.to_string()),
    };
    if !metadata.is_file() {
        return ConfigState::Unreadable("path exists but is not a file".to_string());
    }
    if let Err(exc) = fs::read_to_string(path) {
        return ConfigState::Unreadable(exc.to_string());
    }
    match moraine_config::load_config(path) {
        Ok(_) => ConfigState::Valid,
        Err(exc) => ConfigState::Invalid(exc.to_string()),
    }
}

fn write_default_config(path: &Path) -> Result<()> {
    validate_template_content()?;
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    if path
        .try_exists()
        .with_context(|| format!("failed to inspect {}", path.display()))?
    {
        bail!("refusing to overwrite existing config {}", path.display());
    }

    write_template_atomic(path)
}

fn repair_invalid_config(path: &Path) -> Result<PathBuf> {
    validate_template_content()?;
    let backup_path = unique_backup_path(path)?;
    fs::rename(path, &backup_path).with_context(|| {
        format!(
            "failed to move {} to {}",
            path.display(),
            backup_path.display()
        )
    })?;

    if let Err(exc) = write_template_atomic(path) {
        let _ = fs::rename(&backup_path, path);
        return Err(exc);
    }

    Ok(backup_path)
}

fn write_template_atomic(path: &Path) -> Result<()> {
    let temp_path = create_sibling_template_file(path)?;
    let result = (|| {
        moraine_config::load_config(&temp_path).with_context(|| {
            format!(
                "embedded config template failed validation at {}",
                temp_path.display()
            )
        })?;

        if path
            .try_exists()
            .with_context(|| format!("failed to inspect {}", path.display()))?
        {
            bail!("refusing to overwrite existing config {}", path.display());
        }

        fs::hard_link(&temp_path, path).with_context(|| {
            format!(
                "failed to persist {} to {}",
                temp_path.display(),
                path.display()
            )
        })?;
        Ok(())
    })();
    let _ = fs::remove_file(&temp_path);
    result
}

fn validate_template_content() -> Result<()> {
    let temp_path = create_template_file_in(&env::temp_dir(), "moraine-setup-template")?;
    let result = moraine_config::load_config(&temp_path)
        .with_context(|| "embedded default config template failed validation");
    let _ = fs::remove_file(&temp_path);
    result.map(|_| ())
}

fn create_sibling_template_file(path: &Path) -> Result<PathBuf> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("config.toml");
    create_template_file_in(parent, &format!(".{file_name}.setup"))
}

fn create_template_file_in(parent: &Path, prefix: &str) -> Result<PathBuf> {
    for attempt in 0..100 {
        let temp_path = parent.join(format!(
            "{prefix}-{}-{}-{attempt}.tmp",
            std::process::id(),
            timestamp_suffix()
        ));
        match private_create_new_options().open(&temp_path) {
            Ok(mut file) => {
                if let Err(exc) = file.write_all(DEFAULT_CONFIG_TEMPLATE.as_bytes()) {
                    let _ = fs::remove_file(&temp_path);
                    return Err(exc)
                        .with_context(|| format!("failed to write {}", temp_path.display()));
                }
                if let Err(exc) = file.flush() {
                    let _ = fs::remove_file(&temp_path);
                    return Err(exc)
                        .with_context(|| format!("failed to flush {}", temp_path.display()));
                }
                return Ok(temp_path);
            }
            Err(exc) if exc.kind() == ErrorKind::AlreadyExists => continue,
            Err(exc) => {
                return Err(exc)
                    .with_context(|| format!("failed to create {}", temp_path.display()));
            }
        }
    }

    bail!(
        "failed to create a unique temporary config file in {}",
        parent.display()
    );
}

fn private_create_new_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        options.mode(0o600);
    }
    options
}

fn unique_backup_path(path: &Path) -> Result<PathBuf> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("config.toml");
    let base = parent.join(format!("{file_name}.bak.{}", timestamp_suffix()));
    if !base
        .try_exists()
        .with_context(|| format!("failed to inspect {}", base.display()))?
    {
        return Ok(base);
    }

    for idx in 2.. {
        let candidate = parent.join(format!("{file_name}.bak.{}-{idx}", timestamp_suffix()));
        if !candidate
            .try_exists()
            .with_context(|| format!("failed to inspect {}", candidate.display()))?
        {
            return Ok(candidate);
        }
    }
    unreachable!()
}

fn timestamp_suffix() -> String {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}-{}", duration.as_secs(), duration.subsec_nanos())
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct IngestSelectionUpdate {
    enabled_sources: usize,
    disabled_sources: usize,
    added_sources: usize,
    updated_sources: usize,
}

impl IngestSelectionUpdate {
    fn changed_sources(self) -> usize {
        self.enabled_sources + self.disabled_sources + self.added_sources + self.updated_sources
    }

    fn has_changes(self) -> bool {
        self.changed_sources() > 0
    }

    fn summary(self) -> String {
        if !self.has_changes() {
            return "ingest sources already matched selection".to_string();
        }

        let mut parts = Vec::new();
        if self.added_sources == 1 {
            parts.push("1 added".to_string());
        } else if self.added_sources > 1 {
            parts.push(format!("{} added", self.added_sources));
        }
        if self.enabled_sources == 1 {
            parts.push("1 enabled".to_string());
        } else if self.enabled_sources > 1 {
            parts.push(format!("{} enabled", self.enabled_sources));
        }
        if self.disabled_sources == 1 {
            parts.push("1 disabled".to_string());
        } else if self.disabled_sources > 1 {
            parts.push(format!("{} disabled", self.disabled_sources));
        }
        if self.updated_sources == 1 {
            parts.push("1 repaired".to_string());
        } else if self.updated_sources > 1 {
            parts.push(format!("{} repaired", self.updated_sources));
        }
        format!("ingest sources updated: {}", parts.join(", "))
    }
}

fn apply_ingest_selections_to_config(
    path: &Path,
    selections: &[SetupTargetSelection],
) -> Result<IngestSelectionUpdate> {
    let content =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut document = content
        .parse::<DocumentMut>()
        .with_context(|| format!("{} is not valid TOML", path.display()))?;
    let update = apply_ingest_selections_to_document(&mut document, selections)?;
    if update.has_changes() {
        write_toml_atomic(path, &document.to_string())?;
    }
    Ok(update)
}

fn apply_ingest_selections_to_document(
    document: &mut DocumentMut,
    selections: &[SetupTargetSelection],
) -> Result<IngestSelectionUpdate> {
    let mut update = IngestSelectionUpdate::default();

    for selection in selections {
        let enabled = selection.mode.configures_ingest();

        for setup_source in harnesses::default_ingest_sources(selection.target) {
            let source_index = ingest_source_index(document, *setup_source)?;

            match source_index {
                Some(source_idx) => {
                    let sources = ensure_ingest_sources_mut(document)?;
                    let source_table = sources
                        .get_mut(source_idx)
                        .expect("source index came from the same array");
                    let source_update = setup_source.reconcile_table(source_table, enabled);
                    if source_update.enabled_changed {
                        if enabled {
                            update.enabled_sources += 1;
                        } else {
                            update.disabled_sources += 1;
                        }
                    }
                    if source_update.metadata_changed {
                        update.updated_sources += 1;
                    }
                }
                None if enabled => {
                    let sources = ensure_ingest_sources_mut(document)?;
                    sources.push(setup_source.to_table(enabled));
                    update.added_sources += 1;
                }
                None => {}
            }
        }
    }

    Ok(update)
}

fn ingest_source_index(
    document: &mut DocumentMut,
    setup_source: harnesses::DefaultIngestSource,
) -> Result<Option<usize>> {
    let Some(sources) = ingest_sources_mut(document)? else {
        return Ok(None);
    };
    Ok(sources.iter().position(|source| {
        source.get("name").and_then(Item::as_str) == Some(setup_source.name())
            && source.get("harness").and_then(Item::as_str) == Some(setup_source.harness())
    }))
}

fn ensure_ingest_sources_mut(document: &mut DocumentMut) -> Result<&mut ArrayOfTables> {
    if document.as_table().get("ingest").is_none() {
        document["ingest"] = Item::Table(Table::new());
    }

    let ingest = document["ingest"]
        .as_table_mut()
        .ok_or_else(|| anyhow::anyhow!("ingest must be a TOML table"))?;
    if ingest.get("sources").is_none() {
        ingest["sources"] = Item::ArrayOfTables(ArrayOfTables::new());
    }

    ingest["sources"]
        .as_array_of_tables_mut()
        .ok_or_else(|| anyhow::anyhow!("ingest.sources must be an array of tables"))
}

fn ingest_sources_mut(document: &mut DocumentMut) -> Result<Option<&mut ArrayOfTables>> {
    let Some(ingest) = document.as_table_mut().get_mut("ingest") else {
        return Ok(None);
    };
    let ingest = ingest
        .as_table_mut()
        .ok_or_else(|| anyhow::anyhow!("ingest must be a TOML table"))?;
    let Some(sources) = ingest.get_mut("sources") else {
        return Ok(None);
    };
    Ok(Some(sources.as_array_of_tables_mut().ok_or_else(|| {
        anyhow::anyhow!("ingest.sources must be an array of tables")
    })?))
}

fn write_toml_atomic(path: &Path, content: &str) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("config.toml");
    for attempt in 0..100 {
        let temp_path = parent.join(format!(
            ".{file_name}.setup-{}-{}-{attempt}.tmp",
            std::process::id(),
            timestamp_suffix()
        ));
        match private_create_new_options().open(&temp_path) {
            Ok(mut file) => {
                let result = (|| {
                    file.write_all(content.as_bytes())
                        .with_context(|| format!("failed to write {}", temp_path.display()))?;
                    file.flush()
                        .with_context(|| format!("failed to flush {}", temp_path.display()))?;
                    moraine_config::load_config(&temp_path).with_context(|| {
                        format!(
                            "updated config failed validation at {}",
                            temp_path.display()
                        )
                    })?;
                    fs::rename(&temp_path, path).with_context(|| {
                        format!(
                            "failed to persist {} to {}",
                            temp_path.display(),
                            path.display()
                        )
                    })?;
                    Ok(())
                })();
                if result.is_err() {
                    let _ = fs::remove_file(&temp_path);
                }
                return result;
            }
            Err(exc) if exc.kind() == ErrorKind::AlreadyExists => continue,
            Err(exc) => {
                return Err(exc)
                    .with_context(|| format!("failed to create {}", temp_path.display()));
            }
        }
    }

    bail!(
        "failed to create a unique temporary TOML file in {}",
        parent.display()
    );
}

#[derive(Debug, Clone)]
struct SetupSelectionSet {
    targets: Vec<SetupTargetSelection>,
    apply_ingest: bool,
    confirmed_harness: bool,
}

impl SetupSelectionSet {
    fn harness_targets(&self) -> Vec<SetupMcpTarget> {
        self.targets
            .iter()
            .filter(|selection| selection.mode.configures_harness())
            .map(|selection| selection.target)
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SetupTargetSelection {
    target: SetupMcpTarget,
    mode: SetupSelectionMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetupSelectionMode {
    Off,
    IngestAndHarness,
    IngestOnly,
    HarnessOnly,
}

impl SetupSelectionMode {
    fn cycle(self) -> Self {
        match self {
            SetupSelectionMode::Off => SetupSelectionMode::IngestAndHarness,
            SetupSelectionMode::IngestAndHarness => SetupSelectionMode::IngestOnly,
            SetupSelectionMode::IngestOnly => SetupSelectionMode::HarnessOnly,
            SetupSelectionMode::HarnessOnly => SetupSelectionMode::Off,
        }
    }

    fn configures_ingest(self) -> bool {
        matches!(
            self,
            SetupSelectionMode::IngestAndHarness | SetupSelectionMode::IngestOnly
        )
    }

    fn configures_harness(self) -> bool {
        matches!(
            self,
            SetupSelectionMode::IngestAndHarness | SetupSelectionMode::HarnessOnly
        )
    }

    fn icon(self) -> &'static str {
        match self {
            SetupSelectionMode::Off => "○",
            SetupSelectionMode::IngestAndHarness => "●",
            SetupSelectionMode::IngestOnly => "◐",
            SetupSelectionMode::HarnessOnly => "◑",
        }
    }

    fn summary(self, target: SetupMcpTarget) -> String {
        match self {
            SetupSelectionMode::Off => "nothing".to_string(),
            SetupSelectionMode::IngestAndHarness => format!("ingest + {}", target.setup_kind()),
            SetupSelectionMode::IngestOnly => "ingest only".to_string(),
            SetupSelectionMode::HarnessOnly => format!("{} only", target.setup_kind()),
        }
    }
}

fn setup_target_selections(
    args: &SetupArgs,
    interactive: bool,
    runner: &dyn CommandRunner,
) -> Result<SetupSelectionSet> {
    if !args.mcp_targets.is_empty() {
        return Ok(SetupSelectionSet {
            targets: dedup_targets(&args.mcp_targets)
                .into_iter()
                .map(|target| SetupTargetSelection {
                    target,
                    mode: SetupSelectionMode::HarnessOnly,
                })
                .collect(),
            apply_ingest: false,
            confirmed_harness: false,
        });
    }

    if args.yes || args.dry_run || !interactive {
        return Ok(SetupSelectionSet {
            targets: Vec::new(),
            apply_ingest: false,
            confirmed_harness: false,
        });
    }

    let targets = harnesses::setup_targets();
    prompt_setup_target_selector(&targets, runner)
}

fn dedup_targets(targets: &[SetupMcpTarget]) -> Vec<SetupMcpTarget> {
    let mut seen = BTreeSet::new();
    targets
        .iter()
        .copied()
        .filter(|target| seen.insert(*target))
        .collect()
}

fn prompt_setup_target_selector(
    targets: &[SetupMcpTarget],
    runner: &dyn CommandRunner,
) -> Result<SetupSelectionSet> {
    let mut selections = targets
        .iter()
        .copied()
        .map(|target| SetupTargetSelection {
            target,
            mode: SetupSelectionMode::Off,
        })
        .collect::<Vec<_>>();
    let mut active = 0usize;
    let term = Term::stderr();
    let mut rendered_lines = 0usize;

    loop {
        if rendered_lines > 0 {
            term.clear_last_lines(rendered_lines).ok();
        }
        rendered_lines = render_setup_selector(&term, &selections, active, runner)
            .context("failed to render setup target selector")?;

        match term
            .read_key()
            .context("failed to read setup target selection")?
        {
            Key::ArrowUp => {
                active = active.checked_sub(1).unwrap_or(selections.len() - 1);
            }
            Key::ArrowDown => {
                active = (active + 1) % selections.len();
            }
            Key::Char(' ') => {
                selections[active].mode = selections[active].mode.cycle();
            }
            Key::Enter => {
                term.clear_last_lines(rendered_lines).ok();
                render_setup_selection_summary(&selections);
                return Ok(SetupSelectionSet {
                    confirmed_harness: selections
                        .iter()
                        .any(|selection| selection.mode.configures_harness()),
                    targets: selections,
                    apply_ingest: true,
                });
            }
            Key::Escape => {
                term.clear_last_lines(rendered_lines).ok();
                eprintln!(
                    "{} {} {} {}",
                    style("–").for_stderr().yellow(),
                    Style::new()
                        .for_stderr()
                        .bold()
                        .apply_to("Agent integrations"),
                    style("·").for_stderr().bright().black(),
                    Style::new()
                        .for_stderr()
                        .bright()
                        .black()
                        .apply_to("skipped")
                );
                return Ok(SetupSelectionSet {
                    targets: Vec::new(),
                    apply_ingest: false,
                    confirmed_harness: false,
                });
            }
            Key::CtrlC => bail!("setup target selection interrupted"),
            _ => {}
        }
    }
}

fn render_setup_selector(
    term: &Term,
    selections: &[SetupTargetSelection],
    active: usize,
    runner: &dyn CommandRunner,
) -> Result<usize> {
    term.write_line(&format!(
        "{} {} {}",
        style("?").for_stderr().yellow(),
        Style::new().for_stderr().bold().apply_to("Agent setup"),
        style("›").for_stderr().bright().black()
    ))?;
    term.write_line(&format!(
        "  {}",
        Style::new()
            .for_stderr()
            .bright()
            .black()
            .apply_to("Space cycles none → ingest + harness → ingest only → harness only. Enter applies. Esc skips.")
    ))?;
    term.write_line(&format!(
        "  {}",
        Style::new()
            .for_stderr()
            .yellow()
            .apply_to(HOST_WIDE_ACCESS_WARNING)
    ))?;

    for (idx, selection) in selections.iter().enumerate() {
        term.write_line(&format_setup_selector_row(
            *selection,
            idx == active,
            selection.target.is_available_for_setup(runner),
        ))?;
    }
    term.flush()?;
    Ok(selections.len() + 3)
}

fn format_setup_selector_row(
    selection: SetupTargetSelection,
    active: bool,
    harness_available: bool,
) -> String {
    let arrow = if active {
        style("❯").for_stderr().green().to_string()
    } else {
        " ".to_string()
    };
    let icon_style = match selection.mode {
        SetupSelectionMode::Off => Style::new().for_stderr().bright().black(),
        SetupSelectionMode::IngestAndHarness => Style::new().for_stderr().green().bold(),
        SetupSelectionMode::IngestOnly => Style::new().for_stderr().green(),
        SetupSelectionMode::HarnessOnly => Style::new().for_stderr().cyan(),
    };
    let label_style = if active {
        Style::new().for_stderr().white().bold()
    } else {
        Style::new().for_stderr()
    };
    let disabled = Style::new().for_stderr().bright().black();
    let ingest = if selection.mode.configures_ingest() {
        Style::new().for_stderr().green().apply_to("ingest")
    } else {
        disabled.apply_to("·")
    };
    let harness_style = if selection.mode.configures_harness() {
        if harness_available {
            Style::new().for_stderr().cyan()
        } else {
            Style::new().for_stderr().yellow()
        }
    } else {
        disabled
    };
    let harness = if selection.mode.configures_harness() {
        harness_style.apply_to(selection.target.setup_kind())
    } else {
        harness_style.apply_to("·")
    };
    let availability = if selection.mode.configures_harness() && !harness_available {
        format!(
            " {}",
            Style::new().for_stderr().yellow().apply_to("not detected")
        )
    } else {
        String::new()
    };

    format!(
        "{} {} {:<16} {:<8} {}{}",
        arrow,
        icon_style.apply_to(selection.mode.icon()),
        label_style.apply_to(selection.target.label()),
        ingest,
        harness,
        availability
    )
}

fn render_setup_selection_summary(selections: &[SetupTargetSelection]) {
    let selected = selections
        .iter()
        .filter(|selection| selection.mode != SetupSelectionMode::Off)
        .map(|selection| {
            format!(
                "{} {}",
                selection.target.label(),
                selection.mode.summary(selection.target)
            )
        })
        .collect::<Vec<_>>();

    eprint!(
        "{} {} {} ",
        style("✔").for_stderr().green(),
        Style::new().for_stderr().bold().apply_to("Agent setup"),
        style("·").for_stderr().bright().black()
    );
    if selected.is_empty() {
        eprintln!(
            "{}",
            Style::new()
                .for_stderr()
                .bright()
                .black()
                .apply_to("nothing selected")
        );
    } else {
        for (idx, selection) in selected.iter().enumerate() {
            if idx > 0 {
                eprint!(", ");
            }
            eprint!("{}", Style::new().for_stderr().green().apply_to(selection));
        }
        eprintln!();
    }
}

struct SetupProgress {
    enabled: bool,
    rich: bool,
    unicode: bool,
    started: bool,
}

impl SetupProgress {
    fn from_output(output: &CliOutput) -> Self {
        Self {
            enabled: !output.is_json() && std::io::stderr().is_terminal(),
            rich: output.mode == OutputMode::Rich,
            unicode: output.unicode,
            started: false,
        }
    }

    #[cfg(test)]
    fn disabled() -> Self {
        Self {
            enabled: false,
            rich: false,
            unicode: true,
            started: false,
        }
    }

    fn finish(&mut self) {
        if !self.enabled || !self.started {
            return;
        }
        eprintln!(
            "{} {}",
            self.progress_line("╰─", "`-", Style::new().bright().black()),
            self.dim("setup summary follows")
        );
    }

    fn target_start(&mut self, target: SetupMcpTarget, plan: &McpPlan) {
        if !self.enabled {
            return;
        }
        self.ensure_started();
        eprintln!(
            "{} {} {}",
            self.progress_line("├─", "+-", Style::new().bright().black()),
            self.bold_label(target.label()),
            self.dim(plan.target.setup_kind())
        );
    }

    fn target_success(&self, target: SetupMcpTarget) {
        if !self.enabled {
            return;
        }
        eprintln!(
            "   {} {}",
            self.mark("✓", "[ok]", Style::new().green()),
            self.dim(&format!("{} configured", target.label()))
        );
    }

    fn target_error(&self, target: SetupMcpTarget) {
        if !self.enabled {
            return;
        }
        eprintln!(
            "   {} {}",
            self.mark("✗", "[err]", Style::new().red()),
            self.dim(&format!("{} needs attention", target.label()))
        );
    }

    fn target_skipped(&self, target: SetupMcpTarget, reason: &str) {
        if !self.enabled {
            return;
        }
        eprintln!(
            "   {} {} {}",
            self.mark("–", "[-]", Style::new().yellow()),
            self.dim(target.label()),
            self.dim(reason)
        );
    }

    fn command_start(&self, step: &McpPlanStep) {
        if !self.enabled {
            return;
        }
        eprintln!(
            "   {} {}",
            self.mark("→", ">", Style::new().cyan()),
            self.label(step.progress_label)
        );
    }

    fn command_success(&self, step: &McpPlanStep) {
        if !self.enabled {
            return;
        }
        eprintln!(
            "   {} {}",
            self.mark("✓", "[ok]", Style::new().green()),
            self.dim(step.success_label)
        );
    }

    fn command_warning(&self, step: &McpPlanStep, warning: &str) {
        if !self.enabled {
            return;
        }
        eprintln!(
            "   {} {} {}",
            self.mark("!", "[warn]", Style::new().yellow()),
            self.dim(step.warning_label),
            self.dim(warning)
        );
    }

    fn command_error(&self, step: &McpPlanStep, error: &str) {
        if !self.enabled {
            return;
        }
        eprintln!(
            "   {} {} {}",
            self.mark("✗", "[err]", Style::new().red()),
            self.dim(step.error_label),
            self.dim(error)
        );
    }

    fn config_start(&self, write: &McpConfigWrite) {
        if !self.enabled {
            return;
        }
        eprintln!(
            "   {} {}",
            self.mark("→", ">", Style::new().cyan()),
            self.label(&format!(
                "Updating {} config at {}",
                write.label(),
                write.path().display()
            ))
        );
    }

    fn config_success(&self, write: &McpConfigWrite) {
        if !self.enabled {
            return;
        }
        eprintln!(
            "   {} {}",
            self.mark("✓", "[ok]", Style::new().green()),
            self.dim(&format!("Updated {}", write.path().display()))
        );
    }

    fn config_error(&self, write: &McpConfigWrite, error: &str) {
        if !self.enabled {
            return;
        }
        eprintln!(
            "   {} {} {}",
            self.mark("✗", "[err]", Style::new().red()),
            self.dim(&format!("Could not update {}", write.path().display())),
            self.dim(error)
        );
    }

    fn ensure_started(&mut self) {
        if self.started {
            return;
        }
        self.started = true;
        if self.rich {
            eprintln!();
            eprintln!(
                "{} {}",
                self.progress_line("╭─", ".-", Style::new().cyan()),
                Style::new()
                    .cyan()
                    .bold()
                    .for_stderr()
                    .apply_to("Installing agent integrations")
            );
        } else {
            eprintln!("Installing agent integrations");
        }
    }

    fn mark<'a>(&self, unicode: &'a str, ascii: &'a str, style: Style) -> impl fmt::Display + 'a {
        if self.rich {
            style
                .for_stderr()
                .apply_to(if self.unicode { unicode } else { ascii })
        } else {
            Style::new()
                .for_stderr()
                .apply_to(if self.unicode { unicode } else { ascii })
        }
    }

    fn label<'a>(&self, value: &'a str) -> impl fmt::Display + 'a {
        if self.rich {
            Style::new().white().for_stderr().apply_to(value)
        } else {
            Style::new().for_stderr().apply_to(value)
        }
    }

    fn bold_label<'a>(&self, value: &'a str) -> impl fmt::Display + 'a {
        if self.rich {
            Style::new().white().bold().for_stderr().apply_to(value)
        } else {
            Style::new().for_stderr().apply_to(value)
        }
    }

    fn dim<'a>(&self, value: &'a str) -> impl fmt::Display + 'a {
        if self.rich {
            Style::new().bright().black().for_stderr().apply_to(value)
        } else {
            Style::new().for_stderr().apply_to(value)
        }
    }

    fn progress_line<'a>(
        &self,
        unicode: &'a str,
        ascii: &'a str,
        style: Style,
    ) -> impl fmt::Display + 'a {
        if self.rich {
            style
                .for_stderr()
                .apply_to(if self.unicode { unicode } else { ascii })
        } else {
            Style::new()
                .for_stderr()
                .apply_to(if self.unicode { unicode } else { ascii })
        }
    }
}

#[cfg(test)]
fn setup_mcp_target(
    args: &SetupArgs,
    config_target: &ConfigTarget,
    target: SetupMcpTarget,
    interactive: bool,
    already_confirmed: bool,
    runner: &mut dyn CommandRunner,
) -> Result<McpTargetReport> {
    let mut progress = SetupProgress::disabled();
    setup_mcp_target_with_progress(
        args,
        config_target,
        target,
        interactive,
        already_confirmed,
        &mut progress,
        runner,
    )
}

fn setup_mcp_target_with_progress(
    args: &SetupArgs,
    config_target: &ConfigTarget,
    target: SetupMcpTarget,
    interactive: bool,
    already_confirmed: bool,
    progress: &mut SetupProgress,
    runner: &mut dyn CommandRunner,
) -> Result<McpTargetReport> {
    let plan = McpPlan::for_target(target, config_target);
    if args.dry_run {
        return Ok(McpTargetReport::planned(plan));
    }

    if matches!(plan.action, McpAction::ManualInstructions) {
        return Ok(McpTargetReport::manual(plan));
    }

    if !args.yes && interactive && !already_confirmed {
        eprintln!(
            "Moraine MCP gives {name} access to host-wide Moraine session history visible to your user.",
            name = target.label()
        );
        if !prompt_yes_no(&format!("Configure {} integration?", target.label()), false)? {
            return Ok(McpTargetReport::skipped(
                target,
                "user declined MCP/plugin setup",
            ));
        }
    } else if !args.yes && !interactive {
        return Ok(McpTargetReport::error(
            target,
            "non-interactive MCP setup requires --yes or --dry-run",
        ));
    }

    execute_mcp_plan_with_progress(plan, runner, progress)
}

#[cfg(test)]
fn execute_mcp_plan(plan: McpPlan, runner: &mut dyn CommandRunner) -> Result<McpTargetReport> {
    let mut progress = SetupProgress::disabled();
    execute_mcp_plan_with_progress(plan, runner, &mut progress)
}

fn execute_mcp_plan_with_progress(
    plan: McpPlan,
    runner: &mut dyn CommandRunner,
    progress: &mut SetupProgress,
) -> Result<McpTargetReport> {
    if plan.steps.is_empty() && plan.config_writes.is_empty() {
        return Ok(McpTargetReport::manual(plan));
    }
    let commands = plan.commands();
    progress.target_start(plan.target, &plan);

    if let Some(first_step) = plan.steps.first() {
        if !runner.command_exists(&first_step.command.program) {
            progress.target_skipped(
                plan.target,
                &format!("{} was not found on PATH", first_step.command.program),
            );
            return Ok(McpTargetReport::skipped(
                plan.target,
                &format!("{} was not found on PATH", first_step.command.program),
            ));
        }
    }

    let mut command_results = Vec::new();
    let mut warnings = Vec::new();
    let mut failed = None;

    for step in &plan.steps {
        progress.command_start(step);
        let result = match runner.run(&step.command) {
            Ok(result) => result,
            Err(exc) => {
                let error = exc.to_string();
                progress.command_error(step, &error);
                progress.target_error(plan.target);
                return Err(exc);
            }
        };
        if let Some(command_failure) = step.command_failure(&result) {
            match step.failure_policy {
                CommandFailurePolicy::WarnAndContinue(message) => {
                    progress.command_warning(step, message);
                    warnings.push(message.to_string())
                }
                CommandFailurePolicy::Required => {
                    progress.command_error(step, &command_failure);
                    failed = Some(command_failure);
                }
            }
        } else {
            progress.command_success(step);
        }
        command_results.push(result);

        if failed.is_some() {
            break;
        }
    }

    let mut config_files = Vec::new();
    if failed.is_none() {
        for write in &plan.config_writes {
            progress.config_start(write);
            match apply_mcp_config_write(write) {
                Ok(report) => {
                    progress.config_success(write);
                    config_files.push(report);
                }
                Err(exc) => {
                    let error = format!("failed to update {}: {exc}", write.path().display());
                    progress.config_error(write, &exc.to_string());
                    config_files.push(McpConfigFileReport::error(write, &exc.to_string()));
                    failed = Some(error);
                    break;
                }
            }
        }
    }

    if let Some(error) = failed {
        progress.target_error(plan.target);
        Ok(McpTargetReport {
            target: plan.target,
            action: plan.action,
            status: SetupStatus::Error,
            commands,
            config_files,
            manual_snippet: plan.manual_snippet,
            skipped_reason: None,
            warnings,
            error: Some(error),
            command_results,
        })
    } else {
        progress.target_success(plan.target);
        Ok(McpTargetReport {
            target: plan.target,
            action: plan.action,
            status: SetupStatus::Ok,
            commands,
            config_files,
            manual_snippet: plan.manual_snippet,
            skipped_reason: None,
            warnings,
            error: None,
            command_results,
        })
    }
}

fn prompt_yes_no(question: &str, default: bool) -> Result<bool> {
    let suffix = if default { "[Y/n]" } else { "[y/N]" };
    loop {
        eprint!("{question} {suffix} ");
        std::io::stderr().flush().ok();

        let mut input = String::new();
        std::io::stdin()
            .read_line(&mut input)
            .context("failed to read setup prompt response")?;
        match input.trim().to_ascii_lowercase().as_str() {
            "" => return Ok(default),
            "y" | "yes" => return Ok(true),
            "n" | "no" => return Ok(false),
            _ => eprintln!("please answer 'y' or 'n'."),
        }
    }
}

#[derive(Debug, Clone)]
struct McpPlan {
    target: SetupMcpTarget,
    action: McpAction,
    steps: Vec<McpPlanStep>,
    config_writes: Vec<McpConfigWrite>,
    manual_snippet: Option<String>,
}

impl McpPlan {
    fn for_target(target: SetupMcpTarget, config_target: &ConfigTarget) -> Self {
        Self::for_target_with_home(
            target,
            config_target,
            env::var_os("HOME").map(PathBuf::from),
        )
    }

    fn for_target_with_home(
        target: SetupMcpTarget,
        config_target: &ConfigTarget,
        home: Option<PathBuf>,
    ) -> Self {
        harnesses::mcp_plan(target, config_target, home)
    }

    fn replace_registration(
        target: SetupMcpTarget,
        remove_command: CommandSpec,
        add_step: McpPlanStep,
    ) -> Self {
        Self {
            target,
            action: McpAction::Execute,
            steps: vec![
                McpPlanStep::warn_and_continue(
                    remove_command,
                    "Existing MCP registration could not be removed; continuing in case it was absent",
                )
                .with_progress(
                    "Removing existing Moraine MCP registration",
                    "Existing MCP registration removed or absent",
                    "Existing MCP registration left unchanged",
                    "Existing MCP cleanup failed",
                ),
                add_step,
            ],
            config_writes: Vec::new(),
            manual_snippet: None,
        }
    }

    fn manual(target: SetupMcpTarget, snippet: String) -> Self {
        Self {
            target,
            action: McpAction::ManualInstructions,
            steps: Vec::new(),
            config_writes: Vec::new(),
            manual_snippet: Some(snippet),
        }
    }

    fn write_config(
        target: SetupMcpTarget,
        write: Option<McpConfigWrite>,
        fallback_snippet: String,
    ) -> Self {
        let Some(write) = write else {
            return Self::manual(
                target,
                format!("HOME is not set, so Moraine cannot choose a global MCP config path.\n{fallback_snippet}"),
            );
        };

        Self {
            target,
            action: McpAction::WriteConfig,
            steps: Vec::new(),
            config_writes: vec![write],
            manual_snippet: None,
        }
    }

    fn commands(&self) -> Vec<CommandSpec> {
        self.steps.iter().map(|step| step.command.clone()).collect()
    }

    fn planned_config_files(&self) -> Vec<McpConfigFileReport> {
        self.config_writes
            .iter()
            .map(McpConfigFileReport::planned)
            .collect()
    }
}

#[derive(Debug, Clone)]
struct McpPlanStep {
    command: CommandSpec,
    failure_policy: CommandFailurePolicy,
    success_stdout_contains: Option<&'static str>,
    progress_label: &'static str,
    success_label: &'static str,
    warning_label: &'static str,
    error_label: &'static str,
}

impl McpPlanStep {
    fn required(command: CommandSpec) -> Self {
        Self {
            command,
            failure_policy: CommandFailurePolicy::Required,
            success_stdout_contains: None,
            progress_label: "Running setup command",
            success_label: "Command completed",
            warning_label: "Command completed with warning",
            error_label: "Command failed",
        }
    }

    fn required_stdout(command: CommandSpec, marker: &'static str) -> Self {
        Self {
            command,
            failure_policy: CommandFailurePolicy::Required,
            success_stdout_contains: Some(marker),
            progress_label: "Running setup command",
            success_label: "Command completed",
            warning_label: "Command completed with warning",
            error_label: "Command failed",
        }
    }

    fn warn_and_continue(command: CommandSpec, message: &'static str) -> Self {
        Self {
            command,
            failure_policy: CommandFailurePolicy::WarnAndContinue(message),
            success_stdout_contains: None,
            progress_label: "Running optional setup command",
            success_label: "Optional command completed",
            warning_label: "Optional command skipped",
            error_label: "Optional command failed",
        }
    }

    fn with_progress(
        mut self,
        progress_label: &'static str,
        success_label: &'static str,
        warning_label: &'static str,
        error_label: &'static str,
    ) -> Self {
        self.progress_label = progress_label;
        self.success_label = success_label;
        self.warning_label = warning_label;
        self.error_label = error_label;
        self
    }

    fn command_failure(&self, result: &CommandRunReport) -> Option<String> {
        if !result.success {
            return Some(format!(
                "{} exited with status {}",
                self.command.display(),
                result
                    .status_code
                    .map(|code| code.to_string())
                    .unwrap_or_else(|| "unknown".to_string())
            ));
        }

        if let Some(marker) = self.success_stdout_contains {
            if !result.stdout.contains(marker) {
                return Some(format!(
                    "{} did not report successful setup",
                    self.command.display()
                ));
            }
        }

        None
    }
}

#[derive(Debug, Clone, Copy)]
enum CommandFailurePolicy {
    Required,
    WarnAndContinue(&'static str),
}

#[derive(Debug, Clone, Serialize)]
struct McpConfigFileReport {
    path: String,
    action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl McpConfigFileReport {
    fn planned(write: &McpConfigWrite) -> Self {
        Self {
            path: write.path().display().to_string(),
            action: "would_update".to_string(),
            error: None,
        }
    }

    fn written(write: &McpConfigWrite) -> Self {
        Self {
            path: write.path().display().to_string(),
            action: "updated".to_string(),
            error: None,
        }
    }

    fn error(write: &McpConfigWrite, error: &str) -> Self {
        Self {
            path: write.path().display().to_string(),
            action: "error".to_string(),
            error: Some(error.to_string()),
        }
    }
}

fn apply_mcp_config_write(write: &McpConfigWrite) -> Result<McpConfigFileReport> {
    let mut root = read_json_object_or_default(write.path(), write.format())?;
    write.merge_into(&mut root)?;
    write_json_atomic(write.path(), &Value::Object(root))?;
    Ok(McpConfigFileReport::written(write))
}

fn read_json_object_or_default(path: &Path, format: McpConfigFormat) -> Result<Map<String, Value>> {
    let content = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(exc) if exc.kind() == ErrorKind::NotFound => return Ok(Map::new()),
        Err(exc) => {
            return Err(exc).with_context(|| format!("failed to read {}", path.display()));
        }
    };

    if content.trim().is_empty() {
        return Ok(Map::new());
    }

    let value = match format {
        McpConfigFormat::Jsonc => read_jsonc_value(path, &content)?,
        McpConfigFormat::Json => serde_json::from_str(&content)
            .with_context(|| format!("{} is not valid JSON", path.display()))?,
    };
    match value {
        Value::Object(object) => Ok(object),
        _ => bail!("{} must contain a JSON object", path.display()),
    }
}

fn read_jsonc_value(path: &Path, content: &str) -> Result<Value> {
    jsonc_parser::parse_to_serde_value::<Value>(content, &Default::default())
        .with_context(|| format!("{} is not valid JSONC", path.display()))
}

fn write_json_atomic(path: &Path, value: &Value) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("mcp.json");
    for attempt in 0..100 {
        let temp_path = parent.join(format!(
            ".{file_name}.setup-{}-{}-{attempt}.tmp",
            std::process::id(),
            timestamp_suffix()
        ));
        match private_create_new_options().open(&temp_path) {
            Ok(mut file) => {
                let result = (|| {
                    serde_json::to_writer_pretty(&mut file, value).with_context(|| {
                        format!("failed to serialize JSON for {}", path.display())
                    })?;
                    file.write_all(b"\n")
                        .with_context(|| format!("failed to write {}", temp_path.display()))?;
                    file.flush()
                        .with_context(|| format!("failed to flush {}", temp_path.display()))?;
                    fs::rename(&temp_path, path).with_context(|| {
                        format!(
                            "failed to persist {} to {}",
                            temp_path.display(),
                            path.display()
                        )
                    })?;
                    Ok(())
                })();
                if result.is_err() {
                    let _ = fs::remove_file(&temp_path);
                }
                return result;
            }
            Err(exc) if exc.kind() == ErrorKind::AlreadyExists => continue,
            Err(exc) => {
                return Err(exc)
                    .with_context(|| format!("failed to create {}", temp_path.display()));
            }
        }
    }

    bail!(
        "failed to create a unique temporary JSON file in {}",
        parent.display()
    );
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct CommandSpec {
    program: String,
    args: Vec<String>,
    #[serde(skip)]
    stdin: Option<String>,
}

impl CommandSpec {
    fn new<I, S>(program: &str, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            program: program.to_string(),
            args: args.into_iter().map(Into::into).collect(),
            stdin: None,
        }
    }

    #[cfg(test)]
    fn with_stdin(mut self, input: impl Into<String>) -> Self {
        self.stdin = Some(input.into());
        self
    }

    fn display(&self) -> String {
        std::iter::once(self.program.as_str())
            .chain(self.args.iter().map(String::as_str))
            .map(shell_display_arg)
            .collect::<Vec<_>>()
            .join(" ")
    }
}

fn shell_display_arg(arg: &str) -> String {
    if arg
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || "-_./:@=".contains(ch))
    {
        arg.to_string()
    } else {
        format!("'{}'", arg.replace('\'', "'\\''"))
    }
}

trait CommandRunner {
    fn command_exists(&self, program: &str) -> bool;
    fn run(&mut self, command: &CommandSpec) -> Result<CommandRunReport>;
}

struct RealCommandRunner;

impl CommandRunner for RealCommandRunner {
    fn command_exists(&self, program: &str) -> bool {
        command_exists_on_path(program)
    }

    fn run(&mut self, command: &CommandSpec) -> Result<CommandRunReport> {
        let mut process = Command::new(&command.program);
        process.args(&command.args);
        let output = if let Some(stdin) = &command.stdin {
            let mut child = process
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .with_context(|| format!("failed to run {}", command.program))?;
            if let Some(mut child_stdin) = child.stdin.take() {
                child_stdin
                    .write_all(stdin.as_bytes())
                    .with_context(|| format!("failed to write stdin for {}", command.program))?;
            }
            child
                .wait_with_output()
                .with_context(|| format!("failed to wait for {}", command.program))?
        } else {
            process
                .output()
                .with_context(|| format!("failed to run {}", command.program))?
        };
        Ok(CommandRunReport {
            command: command.clone(),
            success: output.status.success(),
            status_code: output.status.code(),
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        })
    }
}

fn command_exists_on_path(program: &str) -> bool {
    let path = Path::new(program);
    if path.components().count() > 1 {
        return is_executable_file(path);
    }

    let Some(paths) = env::var_os("PATH") else {
        return false;
    };
    env::split_paths(&paths).any(|dir| is_executable_file(&dir.join(program)))
}

fn is_executable_file(path: &Path) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

#[derive(Debug, Clone, Serialize)]
struct CommandRunReport {
    command: CommandSpec,
    success: bool,
    status_code: Option<i32>,
    #[serde(skip_serializing_if = "String::is_empty")]
    stdout: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    stderr: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum SetupStatus {
    Ok,
    Planned,
    Skipped,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum McpAction {
    Execute,
    WriteConfig,
    ManualInstructions,
}

#[derive(Debug, Clone, Serialize)]
struct SetupReport {
    success: bool,
    config: ConfigReport,
    mcp_targets: Vec<McpTargetReport>,
}

fn report_for_json(report: &SetupReport, verbose: bool) -> SetupReport {
    let mut report = report.clone();
    if !verbose {
        for target in &mut report.mcp_targets {
            for result in &mut target.command_results {
                result.stdout.clear();
                result.stderr.clear();
            }
        }
    }
    report
}

#[derive(Debug, Clone, Serialize)]
struct ConfigReport {
    action: String,
    status: SetupStatus,
    path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    backup_path: Option<String>,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl ConfigReport {
    fn ok(path: &Path, action: &str, message: &str) -> Self {
        Self {
            action: action.to_string(),
            status: SetupStatus::Ok,
            path: path.display().to_string(),
            backup_path: None,
            message: message.to_string(),
            error: None,
        }
    }

    fn ok_with_backup(path: &Path, backup_path: PathBuf, action: &str, message: &str) -> Self {
        Self {
            action: action.to_string(),
            status: SetupStatus::Ok,
            path: path.display().to_string(),
            backup_path: Some(backup_path.display().to_string()),
            message: message.to_string(),
            error: None,
        }
    }

    fn planned(path: &Path, action: &str, message: &str) -> Self {
        Self {
            action: action.to_string(),
            status: SetupStatus::Planned,
            path: path.display().to_string(),
            backup_path: None,
            message: message.to_string(),
            error: None,
        }
    }

    fn skipped(path: &Path, message: &str) -> Self {
        Self {
            action: "skipped".to_string(),
            status: SetupStatus::Skipped,
            path: path.display().to_string(),
            backup_path: None,
            message: message.to_string(),
            error: None,
        }
    }

    fn error(path: &Path, action: &str, error: &str) -> Self {
        Self {
            action: action.to_string(),
            status: SetupStatus::Error,
            path: path.display().to_string(),
            backup_path: None,
            message: error.to_string(),
            error: Some(error.to_string()),
        }
    }

    fn apply_ingest_update(&mut self, update: IngestSelectionUpdate) {
        if update.has_changes() && self.action == "unchanged" {
            self.action = "updated".to_string();
        }
        self.message = format!("{}; {}", self.message, update.summary());
    }
}

#[derive(Debug, Clone, Serialize)]
struct McpTargetReport {
    target: SetupMcpTarget,
    action: McpAction,
    status: SetupStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    commands: Vec<CommandSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    config_files: Vec<McpConfigFileReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    manual_snippet: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    skipped_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    warnings: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    command_results: Vec<CommandRunReport>,
}

impl McpTargetReport {
    fn planned(plan: McpPlan) -> Self {
        Self {
            target: plan.target,
            action: plan.action,
            status: SetupStatus::Planned,
            commands: plan.commands(),
            config_files: plan.planned_config_files(),
            manual_snippet: plan.manual_snippet,
            skipped_reason: None,
            warnings: Vec::new(),
            error: None,
            command_results: Vec::new(),
        }
    }

    fn manual(plan: McpPlan) -> Self {
        Self {
            target: plan.target,
            action: plan.action,
            status: SetupStatus::Skipped,
            commands: Vec::new(),
            config_files: Vec::new(),
            manual_snippet: plan.manual_snippet,
            skipped_reason: Some("manual instructions only".to_string()),
            warnings: Vec::new(),
            error: None,
            command_results: Vec::new(),
        }
    }

    fn skipped(target: SetupMcpTarget, reason: &str) -> Self {
        Self {
            target,
            action: McpAction::Execute,
            status: SetupStatus::Skipped,
            commands: Vec::new(),
            config_files: Vec::new(),
            manual_snippet: None,
            skipped_reason: Some(reason.to_string()),
            warnings: Vec::new(),
            error: None,
            command_results: Vec::new(),
        }
    }

    fn error(target: SetupMcpTarget, error: &str) -> Self {
        Self {
            target,
            action: McpAction::Execute,
            status: SetupStatus::Error,
            commands: Vec::new(),
            config_files: Vec::new(),
            manual_snippet: None,
            skipped_reason: None,
            warnings: Vec::new(),
            error: Some(error.to_string()),
            command_results: Vec::new(),
        }
    }
}

fn render_report(output: &CliOutput, report: &SetupReport) -> Result<()> {
    if output.is_json() {
        println!(
            "{}",
            serde_json::to_string_pretty(&report_for_json(report, output.verbose))?
        );
        return Ok(());
    }

    let mut config_lines = vec![format!(
        "{} config  {}",
        status_mark(report.config.status, output.unicode),
        report.config.path
    )];
    config_lines.push(format!(
        "   {} - {}",
        report.config.action, report.config.message
    ));
    if let Some(backup_path) = &report.config.backup_path {
        config_lines.push(format!("   backup: {backup_path}"));
    }
    output.section("Moraine Setup", &config_lines);

    if report.mcp_targets.is_empty() {
        output.section(
            "Agent Integrations",
            &[format!(
                "{} none selected  run with --mcp-target <target> or rerun interactively",
                status_mark(SetupStatus::Skipped, output.unicode)
            )],
        );
        return Ok(());
    }

    let rows = report
        .mcp_targets
        .iter()
        .map(|target| {
            vec![
                format!(
                    "{} {} {}",
                    status_mark(target.status, output.unicode),
                    target.target.label(),
                    target.target.setup_kind()
                ),
                target_status_text(target).to_string(),
                target_detail(target),
            ]
        })
        .collect::<Vec<_>>();
    output.table("Agent Integrations", &["target", "status", "detail"], &rows);

    let mut detail_lines = Vec::new();
    for target in &report.mcp_targets {
        append_target_details(target, output, &mut detail_lines);
    }
    if !detail_lines.is_empty() {
        output.section("Integration Details", &detail_lines);
    }

    Ok(())
}

fn status_mark(status: SetupStatus, unicode: bool) -> &'static str {
    match (status, unicode) {
        (SetupStatus::Ok, true) => "✓",
        (SetupStatus::Planned, true) => "◇",
        (SetupStatus::Skipped, true) => "–",
        (SetupStatus::Error, true) => "✗",
        (SetupStatus::Ok, false) => "[ok]",
        (SetupStatus::Planned, false) => "[plan]",
        (SetupStatus::Skipped, false) => "[-]",
        (SetupStatus::Error, false) => "[err]",
    }
}

fn target_status_text(target: &McpTargetReport) -> &'static str {
    match target.status {
        SetupStatus::Ok => "configured",
        SetupStatus::Planned => "planned",
        SetupStatus::Skipped if target.manual_snippet.is_some() => "manual",
        SetupStatus::Skipped => "skipped",
        SetupStatus::Error => "error",
    }
}

fn target_detail(target: &McpTargetReport) -> String {
    if let Some(error) = &target.error {
        return truncate_for_table(error);
    }
    if target.manual_snippet.is_some() {
        return "manual instructions below".to_string();
    }
    if !target.warnings.is_empty() {
        return "completed with warnings".to_string();
    }
    if let Some(reason) = &target.skipped_reason {
        return truncate_for_table(reason);
    }
    let command_count = target.commands.len();
    let config_count = target.config_files.len();
    let mut parts = Vec::new();
    if command_count == 1 {
        parts.push("1 command".to_string());
    } else if command_count > 1 {
        parts.push(format!("{command_count} commands"));
    }
    if config_count == 1 {
        parts.push("1 config file".to_string());
    } else if config_count > 1 {
        parts.push(format!("{config_count} config files"));
    }
    if parts.is_empty() {
        return match target.status {
            SetupStatus::Ok => "ready".to_string(),
            SetupStatus::Planned => "no commands would run".to_string(),
            SetupStatus::Skipped => "no commands run".to_string(),
            SetupStatus::Error => "no commands completed".to_string(),
        };
    }
    let work = parts.join(" + ");
    match target.status {
        SetupStatus::Ok => format!("{work} completed"),
        SetupStatus::Planned => format!("{work} would update"),
        SetupStatus::Skipped => format!("{work} skipped"),
        SetupStatus::Error => format!("{work} attempted"),
    }
}

fn truncate_for_table(value: &str) -> String {
    let collapsed = value.split_whitespace().collect::<Vec<_>>().join(" ");
    const MAX: usize = 52;
    if collapsed.chars().count() <= MAX {
        return collapsed;
    }
    let mut truncated = collapsed
        .chars()
        .take(MAX.saturating_sub(3))
        .collect::<String>();
    truncated.push_str("...");
    truncated
}

fn append_target_details(target: &McpTargetReport, output: &CliOutput, lines: &mut Vec<String>) {
    let show_commands = output.verbose
        || target.status == SetupStatus::Planned
        || target.status == SetupStatus::Error
        || !target.warnings.is_empty();
    let show_config_files = !target.config_files.is_empty()
        && (output.verbose
            || target.status == SetupStatus::Ok
            || target.status == SetupStatus::Planned
            || target.status == SetupStatus::Error);
    let show_command_output = output.verbose && !target.command_results.is_empty();
    if !show_commands
        && !show_config_files
        && !show_command_output
        && target.manual_snippet.is_none()
        && target.warnings.is_empty()
        && target.error.is_none()
    {
        return;
    }

    if !lines.is_empty() {
        lines.push(String::new());
    }
    lines.push(format!("{}:", target.target.label()));
    if show_commands {
        for command in &target.commands {
            lines.push(format!("  $ {}", command.display()));
        }
    }
    if show_config_files {
        for config_file in &target.config_files {
            if let Some(error) = &config_file.error {
                lines.push(format!(
                    "  file: {} ({}, {error})",
                    config_file.path, config_file.action
                ));
            } else {
                lines.push(format!(
                    "  file: {} ({})",
                    config_file.path, config_file.action
                ));
            }
        }
    }
    if let Some(snippet) = &target.manual_snippet {
        for line in snippet.lines() {
            lines.push(format!("  {line}"));
        }
    }
    for warning in &target.warnings {
        lines.push(format!("  warning: {warning}"));
    }
    if let Some(error) = &target.error {
        lines.push(format!("  error: {error}"));
    }
    if output.verbose {
        for result in &target.command_results {
            if !result.stdout.trim().is_empty() {
                lines.push(format!(
                    "  {} stdout: {}",
                    result.command.program,
                    result.stdout.trim()
                ));
            }
            if !result.stderr.trim().is_empty() {
                lines.push(format!(
                    "  {} stderr: {}",
                    result.command.program,
                    result.stderr.trim()
                ));
            }
        }
    }
}

impl SetupMcpTarget {
    fn label(self) -> &'static str {
        harnesses::spec(self).label()
    }

    fn setup_kind(self) -> &'static str {
        harnesses::spec(self).setup_kind()
    }

    fn is_available_for_setup(self, runner: &dyn CommandRunner) -> bool {
        if self
            .program_candidates()
            .iter()
            .any(|program| runner.command_exists(program))
        {
            return true;
        }
        self.default_probe_paths().iter().any(|path| path.exists())
    }

    fn program_candidates(self) -> &'static [&'static str] {
        harnesses::spec(self).program_candidates()
    }

    fn default_probe_paths(self) -> Vec<PathBuf> {
        let Some(home) = env::var_os("HOME").map(PathBuf::from) else {
            return Vec::new();
        };
        harnesses::spec(self).default_probe_paths(&home)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render::OutputMode;
    use std::collections::{BTreeMap, BTreeSet};
    use toml_edit::value as toml_value;

    #[derive(Default)]
    struct FakeRunner {
        existing: BTreeSet<String>,
        responses: BTreeMap<String, CommandRunReport>,
        ran: Vec<CommandSpec>,
    }

    impl FakeRunner {
        fn with_existing(mut self, program: &str) -> Self {
            self.existing.insert(program.to_string());
            self
        }

        fn with_response(self, command: CommandSpec, success: bool, stderr: &str) -> Self {
            self.with_output_response(command, success, "", stderr)
        }

        fn with_output_response(
            mut self,
            command: CommandSpec,
            success: bool,
            stdout: &str,
            stderr: &str,
        ) -> Self {
            self.responses.insert(
                command.display(),
                CommandRunReport {
                    command,
                    success,
                    status_code: Some(if success { 0 } else { 1 }),
                    stdout: stdout.to_string(),
                    stderr: stderr.to_string(),
                },
            );
            self
        }
    }

    impl CommandRunner for FakeRunner {
        fn command_exists(&self, program: &str) -> bool {
            self.existing.contains(program)
        }

        fn run(&mut self, command: &CommandSpec) -> Result<CommandRunReport> {
            self.ran.push(command.clone());
            self.responses
                .get(&command.display())
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("no fake response for {}", command.display()))
        }
    }

    fn temp_path(label: &str) -> PathBuf {
        env::temp_dir().join(format!(
            "moraine-setup-test-{label}-{}-{}",
            std::process::id(),
            timestamp_suffix()
        ))
    }

    fn plain_output() -> CliOutput {
        CliOutput {
            mode: OutputMode::Plain,
            verbose: false,
            unicode: false,
            width: 100,
        }
    }

    fn sources_for_harness<'a>(document: &'a DocumentMut, harness: &str) -> Vec<&'a Table> {
        document["ingest"]["sources"]
            .as_array_of_tables()
            .expect("ingest sources")
            .iter()
            .filter(|source| source.get("harness").and_then(Item::as_str) == Some(harness))
            .collect()
    }

    fn source_enabled(document: &DocumentMut, name: &str) -> bool {
        document["ingest"]["sources"]
            .as_array_of_tables()
            .expect("ingest sources")
            .iter()
            .find(|source| source.get("name").and_then(Item::as_str) == Some(name))
            .expect("source exists")
            .get("enabled")
            .and_then(Item::as_bool)
            .unwrap_or(true)
    }

    fn source_value<'a>(document: &'a DocumentMut, name: &str, key: &str) -> Option<&'a str> {
        document["ingest"]["sources"]
            .as_array_of_tables()
            .expect("ingest sources")
            .iter()
            .find(|source| source.get("name").and_then(Item::as_str) == Some(name))
            .expect("source exists")
            .get(key)
            .and_then(Item::as_str)
    }

    fn push_source(
        document: &mut DocumentMut,
        name: &str,
        harness: &str,
        enabled: bool,
        glob: &str,
        watch_root: &str,
    ) {
        let mut table = Table::new();
        table["name"] = toml_value(name);
        table["harness"] = toml_value(harness);
        table["enabled"] = toml_value(enabled);
        table["glob"] = toml_value(glob);
        table["watch_root"] = toml_value(watch_root);
        document["ingest"]["sources"]
            .as_array_of_tables_mut()
            .expect("ingest sources")
            .push(table);
    }

    #[test]
    fn setup_config_target_prefers_cli_then_home() {
        let cli = resolve_setup_config_target_with(
            Some(PathBuf::from("/tmp/cli.toml")),
            Some("/home/test".into()),
        )
        .expect("cli target");
        assert_eq!(cli.path, PathBuf::from("/tmp/cli.toml"));
        assert_eq!(cli.source, ConfigTargetSource::Cli);

        let home =
            resolve_setup_config_target_with(None, Some("/home/test".into())).expect("home target");
        assert_eq!(home.path, PathBuf::from("/home/test/.moraine/config.toml"));
        assert_eq!(home.source, ConfigTargetSource::HomeDefault);
    }

    #[test]
    fn setup_config_target_absolutizes_relative_cli_paths() {
        let cwd = env::current_dir().expect("current dir");

        let cli = resolve_setup_config_target_with(
            Some(PathBuf::from("relative-cli.toml")),
            Some("/home/test".into()),
        )
        .expect("cli target");
        assert_eq!(cli.path, cwd.join("relative-cli.toml"));
    }

    #[test]
    fn setup_config_target_errors_without_home() {
        let err = resolve_setup_config_target_with(None, None).expect_err("missing home");
        assert!(err.to_string().contains("pass --config or set HOME"));
    }

    #[test]
    fn setup_selection_mode_cycles_through_ingest_and_harness_states() {
        assert_eq!(
            SetupSelectionMode::Off.cycle(),
            SetupSelectionMode::IngestAndHarness
        );
        assert_eq!(
            SetupSelectionMode::IngestAndHarness.cycle(),
            SetupSelectionMode::IngestOnly
        );
        assert_eq!(
            SetupSelectionMode::IngestOnly.cycle(),
            SetupSelectionMode::HarnessOnly
        );
        assert_eq!(
            SetupSelectionMode::HarnessOnly.cycle(),
            SetupSelectionMode::Off
        );
    }

    #[test]
    fn setup_selector_warning_discloses_host_wide_history_access() {
        assert!(HOST_WIDE_ACCESS_WARNING.contains("host-wide Moraine session history access"));
    }

    #[test]
    fn harness_targets_include_only_modes_that_configure_harnesses() {
        let selections = SetupSelectionSet {
            targets: vec![
                SetupTargetSelection {
                    target: SetupMcpTarget::Codex,
                    mode: SetupSelectionMode::IngestAndHarness,
                },
                SetupTargetSelection {
                    target: SetupMcpTarget::Hermes,
                    mode: SetupSelectionMode::IngestOnly,
                },
                SetupTargetSelection {
                    target: SetupMcpTarget::Cursor,
                    mode: SetupSelectionMode::HarnessOnly,
                },
                SetupTargetSelection {
                    target: SetupMcpTarget::ClaudeCode,
                    mode: SetupSelectionMode::Off,
                },
            ],
            apply_ingest: true,
            confirmed_harness: true,
        };

        assert_eq!(
            selections.harness_targets(),
            vec![SetupMcpTarget::Codex, SetupMcpTarget::Cursor]
        );
    }

    #[test]
    fn ingest_selection_disables_existing_sources_for_unselected_harnesses() {
        let mut document = DEFAULT_CONFIG_TEMPLATE
            .parse::<DocumentMut>()
            .expect("template parses");
        push_source(
            &mut document,
            "cursor-custom",
            "cursor",
            true,
            "~/custom/**/*.jsonl",
            "~/custom",
        );

        let update = apply_ingest_selections_to_document(
            &mut document,
            &[
                SetupTargetSelection {
                    target: SetupMcpTarget::Codex,
                    mode: SetupSelectionMode::IngestOnly,
                },
                SetupTargetSelection {
                    target: SetupMcpTarget::Cursor,
                    mode: SetupSelectionMode::HarnessOnly,
                },
                SetupTargetSelection {
                    target: SetupMcpTarget::Hermes,
                    mode: SetupSelectionMode::Off,
                },
            ],
        )
        .expect("apply ingest selections");

        assert_eq!(
            update,
            IngestSelectionUpdate {
                enabled_sources: 0,
                disabled_sources: 3,
                added_sources: 0,
                // The bundled template carries macOS Cursor SQLite paths. On Linux,
                // setup repairs that setup-owned source while disabling it.
                updated_sources: if cfg!(target_os = "macos") { 0 } else { 1 },
            }
        );
        assert!(source_enabled(&document, "codex"));
        assert!(!source_enabled(&document, "cursor"));
        assert!(!source_enabled(&document, "cursor-sqlite"));
        assert!(source_enabled(&document, "cursor-custom"));
        assert!(!source_enabled(&document, "hermes"));
    }

    #[test]
    fn ingest_selection_adds_default_sources_when_missing() {
        let mut document = "# minimal\n"
            .parse::<DocumentMut>()
            .expect("minimal config parses");

        let update = apply_ingest_selections_to_document(
            &mut document,
            &[
                SetupTargetSelection {
                    target: SetupMcpTarget::Cursor,
                    mode: SetupSelectionMode::IngestOnly,
                },
                SetupTargetSelection {
                    target: SetupMcpTarget::PiCodingAgent,
                    mode: SetupSelectionMode::HarnessOnly,
                },
            ],
        )
        .expect("apply ingest selections");

        assert_eq!(
            update,
            IngestSelectionUpdate {
                enabled_sources: 0,
                disabled_sources: 0,
                added_sources: 2,
                updated_sources: 0,
            }
        );
        assert_eq!(sources_for_harness(&document, "cursor").len(), 2);
        assert!(source_enabled(&document, "cursor"));
        assert!(source_enabled(&document, "cursor-sqlite"));
        assert!(sources_for_harness(&document, "pi-coding-agent").is_empty());

        let path = temp_path("ingest-added-config");
        fs::write(&path, document.to_string()).expect("write updated config");
        moraine_config::load_config(&path).expect("updated config loads");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn ingest_selection_reconciles_missing_setup_owned_sources() {
        let mut document = r#"
[ingest]

[[ingest.sources]]
name = "cursor"
harness = "cursor"
enabled = false
glob = "~/stale/**/*.jsonl"
watch_root = "~/stale"
format = "stale"

[[ingest.sources]]
name = "cursor-custom"
harness = "cursor"
enabled = false
glob = "~/custom/**/*.jsonl"
watch_root = "~/custom"
"#
        .parse::<DocumentMut>()
        .expect("partial cursor config parses");

        let update = apply_ingest_selections_to_document(
            &mut document,
            &[SetupTargetSelection {
                target: SetupMcpTarget::Cursor,
                mode: SetupSelectionMode::IngestOnly,
            }],
        )
        .expect("apply ingest selections");

        assert_eq!(
            update,
            IngestSelectionUpdate {
                enabled_sources: 1,
                disabled_sources: 0,
                added_sources: 1,
                updated_sources: 1,
            }
        );
        assert!(source_enabled(&document, "cursor"));
        assert!(source_enabled(&document, "cursor-sqlite"));
        assert!(!source_enabled(&document, "cursor-custom"));
        assert_eq!(
            source_value(&document, "cursor", "glob"),
            Some("~/.cursor/projects/*/agent-transcripts/**/*.jsonl")
        );
        assert_eq!(
            source_value(&document, "cursor", "watch_root"),
            Some("~/.cursor/projects")
        );
        assert_eq!(source_value(&document, "cursor", "format"), Some("jsonl"));
        assert_eq!(
            source_value(&document, "cursor-custom", "glob"),
            Some("~/custom/**/*.jsonl")
        );
    }

    #[test]
    fn writes_default_config_when_missing() {
        let dir = temp_path("missing-config");
        let path = dir.join("nested").join("config.toml");
        let args = SetupArgs {
            yes: true,
            dry_run: false,
            skip_config: false,
            skip_mcp: true,
            repair_config: false,
            mcp_targets: Vec::new(),
        };
        let report = setup_config(
            &args,
            &ConfigTarget {
                path: path.clone(),
                source: ConfigTargetSource::Cli,
            },
            false,
        )
        .expect("setup config");
        assert_eq!(report.status, SetupStatus::Ok);
        assert!(path.is_file());
        moraine_config::load_config(&path).expect("written config loads");
        #[cfg(unix)]
        assert_eq!(
            fs::metadata(&path)
                .expect("config metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn dry_run_does_not_write_missing_config() {
        let dir = temp_path("dry-run");
        let path = dir.join("config.toml");
        let args = SetupArgs {
            yes: false,
            dry_run: true,
            skip_config: false,
            skip_mcp: true,
            repair_config: false,
            mcp_targets: Vec::new(),
        };
        let report = setup_config(
            &args,
            &ConfigTarget {
                path: path.clone(),
                source: ConfigTargetSource::Cli,
            },
            false,
        )
        .expect("dry-run config");
        assert_eq!(report.status, SetupStatus::Planned);
        assert!(!path.exists());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn repair_invalid_config_creates_backup() {
        let dir = temp_path("repair");
        fs::create_dir_all(&dir).expect("create dir");
        let path = dir.join("config.toml");
        fs::write(&path, "not = [valid").expect("write invalid config");
        let args = SetupArgs {
            yes: true,
            dry_run: false,
            skip_config: false,
            skip_mcp: true,
            repair_config: true,
            mcp_targets: Vec::new(),
        };
        let report = setup_config(
            &args,
            &ConfigTarget {
                path: path.clone(),
                source: ConfigTargetSource::Cli,
            },
            false,
        )
        .expect("repair config");
        assert_eq!(report.status, SetupStatus::Ok);
        let backup = PathBuf::from(report.backup_path.expect("backup path"));
        assert!(backup.is_file());
        assert_eq!(
            fs::read_to_string(backup).expect("backup content"),
            "not = [valid"
        );
        moraine_config::load_config(&path).expect("repaired config loads");
        #[cfg(unix)]
        assert_eq!(
            fs::metadata(&path)
                .expect("config metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn valid_config_is_not_repaired_even_with_repair_flag() {
        let dir = temp_path("valid-no-repair");
        fs::create_dir_all(&dir).expect("create dir");
        let path = dir.join("config.toml");
        fs::write(&path, "# minimal\n").expect("write config");
        let args = SetupArgs {
            yes: true,
            dry_run: false,
            skip_config: false,
            skip_mcp: true,
            repair_config: true,
            mcp_targets: Vec::new(),
        };
        let report = setup_config(
            &args,
            &ConfigTarget {
                path: path.clone(),
                source: ConfigTargetSource::Cli,
            },
            false,
        )
        .expect("valid config");
        assert_eq!(report.action, "unchanged");
        assert!(report.backup_path.is_none());
        assert_eq!(
            fs::read_to_string(path).expect("config content"),
            "# minimal\n"
        );
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn codex_default_config_installs_plugin() {
        let target = ConfigTarget {
            path: PathBuf::from("/tmp/config.toml"),
            source: ConfigTargetSource::HomeDefault,
        };
        let plan = McpPlan::for_target(SetupMcpTarget::Codex, &target);
        let commands = plan.commands();
        assert_eq!(commands.len(), 3);
        assert_eq!(
            commands[0].args,
            vec![
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
            ]
        );
        assert_eq!(commands[1].args, vec!["plugin", "add", "moraine@moraine"]);
        assert_eq!(commands[2].args, vec!["mcp", "remove", "moraine"]);
    }

    #[test]
    fn claude_default_config_installs_plugin_and_cleans_manual_mcp() {
        let target = ConfigTarget {
            path: PathBuf::from("/tmp/config.toml"),
            source: ConfigTargetSource::HomeDefault,
        };
        let plan = McpPlan::for_target(SetupMcpTarget::ClaudeCode, &target);
        let commands = plan.commands();
        assert_eq!(commands.len(), 3);
        assert_eq!(
            commands[0].args,
            vec![
                "plugin",
                "marketplace",
                "add",
                "eric-tramel/moraine",
                "--sparse",
                ".claude-plugin",
                "plugins",
            ]
        );
        assert_eq!(
            commands[1].args,
            vec!["plugin", "install", "moraine@moraine"]
        );
        assert_eq!(
            commands[2].args,
            vec!["mcp", "remove", "moraine", "--scope", "user"]
        );
    }

    #[test]
    fn codex_custom_config_is_manual() {
        let target = ConfigTarget {
            path: PathBuf::from("/tmp/custom.toml"),
            source: ConfigTargetSource::Cli,
        };
        let plan = McpPlan::for_target(SetupMcpTarget::Codex, &target);
        assert_eq!(plan.action, McpAction::ManualInstructions);
        assert!(plan
            .manual_snippet
            .expect("manual snippet")
            .contains("--config /tmp/custom.toml"));
    }

    #[test]
    fn claude_custom_config_is_manual() {
        let target = ConfigTarget {
            path: PathBuf::from("/tmp/custom.toml"),
            source: ConfigTargetSource::Cli,
        };
        let plan = McpPlan::for_target(SetupMcpTarget::ClaudeCode, &target);
        assert_eq!(plan.action, McpAction::ManualInstructions);
        assert!(plan
            .manual_snippet
            .expect("manual snippet")
            .contains("--config /tmp/custom.toml"));
    }

    #[test]
    fn cursor_config_write_merges_global_mcp_json() {
        let home = temp_path("cursor-home");
        let cursor_dir = home.join(".cursor");
        fs::create_dir_all(&cursor_dir).expect("create cursor dir");
        let path = cursor_dir.join("mcp.json");
        fs::write(
            &path,
            r#"{"mcpServers":{"other":{"command":"node"}},"enabled":true}"#,
        )
        .expect("write existing cursor config");

        let target = ConfigTarget {
            path: PathBuf::from("/tmp/config.toml"),
            source: ConfigTargetSource::HomeDefault,
        };
        let write = McpConfigWrite::cursor(&home, &target);
        let report = apply_mcp_config_write(&write).expect("write cursor config");
        assert_eq!(report.action, "updated");

        let value: Value =
            serde_json::from_str(&fs::read_to_string(&path).expect("read cursor config"))
                .expect("cursor config json");
        assert_eq!(value["enabled"], true);
        assert_eq!(value["mcpServers"]["other"]["command"], "node");
        assert_eq!(value["mcpServers"]["moraine"]["type"], "stdio");
        assert_eq!(value["mcpServers"]["moraine"]["command"], "moraine");
        assert_eq!(
            value["mcpServers"]["moraine"]["args"],
            serde_json::json!(["run", "mcp"])
        );
        #[cfg(unix)]
        assert_eq!(
            fs::metadata(&path)
                .expect("cursor metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let _ = fs::remove_dir_all(home);
    }

    #[test]
    fn opencode_config_write_merges_global_config_with_custom_moraine_config() {
        let home = temp_path("opencode-home");
        let opencode_dir = home.join(".config").join("opencode");
        fs::create_dir_all(&opencode_dir).expect("create opencode dir");
        let path = opencode_dir.join("opencode.json");
        fs::write(
            &path,
            r#"{"theme":"system","mcp":{"other":{"type":"local","command":["node","server.js"]}}}"#,
        )
        .expect("write existing opencode config");

        let target = ConfigTarget {
            path: PathBuf::from("/tmp/custom.toml"),
            source: ConfigTargetSource::Cli,
        };
        let write = McpConfigWrite::opencode(&home, &target);
        apply_mcp_config_write(&write).expect("write opencode config");

        let value: Value =
            serde_json::from_str(&fs::read_to_string(&path).expect("read opencode config"))
                .expect("opencode config json");
        assert_eq!(value["theme"], "system");
        assert_eq!(value["$schema"], "https://opencode.ai/config.json");
        assert_eq!(value["mcp"]["other"]["type"], "local");
        assert_eq!(value["mcp"]["moraine"]["type"], "local");
        assert_eq!(value["mcp"]["moraine"]["enabled"], true);
        assert_eq!(
            value["mcp"]["moraine"]["command"],
            serde_json::json!(["moraine", "run", "mcp", "--config", "/tmp/custom.toml"])
        );
        let _ = fs::remove_dir_all(home);
    }

    #[test]
    fn opencode_config_write_accepts_jsonc_config() {
        let home = temp_path("opencode-jsonc-home");
        let opencode_dir = home.join(".config").join("opencode");
        fs::create_dir_all(&opencode_dir).expect("create opencode dir");
        let path = opencode_dir.join("opencode.json");
        fs::write(
            &path,
            r#"{
  // OpenCode config files are JSONC.
  "theme": "system",
  "mcp": {
    "other": {
      "type": "local",
      "command": ["node", "server.js"],
    },
  },
}"#,
        )
        .expect("write existing opencode config");

        let target = ConfigTarget {
            path: PathBuf::from("/tmp/config.toml"),
            source: ConfigTargetSource::HomeDefault,
        };
        let write = McpConfigWrite::opencode(&home, &target);
        apply_mcp_config_write(&write).expect("write opencode config");

        let value: Value =
            serde_json::from_str(&fs::read_to_string(&path).expect("read opencode config"))
                .expect("opencode config json");
        assert_eq!(value["theme"], "system");
        assert_eq!(
            value["mcp"]["other"]["command"],
            serde_json::json!(["node", "server.js"])
        );
        assert_eq!(value["mcp"]["moraine"]["type"], "local");
        assert_eq!(
            value["mcp"]["moraine"]["command"],
            serde_json::json!(["moraine", "run", "mcp"])
        );
        let _ = fs::remove_dir_all(home);
    }

    #[test]
    fn pi_plan_installs_extension_then_writes_config() {
        let home = temp_path("pi-home");
        let target = ConfigTarget {
            path: PathBuf::from("/tmp/config.toml"),
            source: ConfigTargetSource::HomeDefault,
        };
        let plan = McpPlan::for_target_with_home(
            SetupMcpTarget::PiCodingAgent,
            &target,
            Some(home.clone()),
        );
        assert_eq!(plan.action, McpAction::WriteConfig);
        let commands = plan.commands();
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].program, "pi");
        assert_eq!(commands[0].args, vec!["install", "npm:pi-mcp-extension"]);

        let mut runner =
            FakeRunner::default()
                .with_existing("pi")
                .with_response(commands[0].clone(), true, "");
        let report = execute_mcp_plan(plan, &mut runner).expect("execute pi plan");
        assert_eq!(report.status, SetupStatus::Ok);
        assert_eq!(runner.ran, commands);
        assert_eq!(report.config_files.len(), 1);

        let path = home.join(".pi").join("agent").join("mcp.json");
        let value: Value =
            serde_json::from_str(&fs::read_to_string(&path).expect("read pi config"))
                .expect("pi config json");
        assert_eq!(value["mcpServers"]["moraine"]["transport"], "stdio");
        assert_eq!(value["mcpServers"]["moraine"]["command"], "moraine");
        assert_eq!(
            value["mcpServers"]["moraine"]["args"],
            serde_json::json!(["run", "mcp"])
        );
        assert_eq!(value["mcpServers"]["moraine"]["lifecycle"], "eager");
        let _ = fs::remove_dir_all(home);
    }

    #[test]
    fn invalid_json_config_write_reports_error() {
        let home = temp_path("bad-cursor-home");
        let cursor_dir = home.join(".cursor");
        fs::create_dir_all(&cursor_dir).expect("create cursor dir");
        let path = cursor_dir.join("mcp.json");
        fs::write(&path, "not json").expect("write bad cursor config");

        let target = ConfigTarget {
            path: PathBuf::from("/tmp/config.toml"),
            source: ConfigTargetSource::HomeDefault,
        };
        let plan =
            McpPlan::for_target_with_home(SetupMcpTarget::Cursor, &target, Some(home.clone()));
        let mut runner = FakeRunner::default();
        let report = execute_mcp_plan(plan, &mut runner).expect("execute cursor plan");
        assert_eq!(report.status, SetupStatus::Error);
        assert!(report
            .error
            .as_deref()
            .expect("error")
            .contains("not valid JSON"));
        assert_eq!(report.config_files.len(), 1);
        assert_eq!(report.config_files[0].action, "error");
        assert!(runner.ran.is_empty());
        assert_eq!(
            fs::read_to_string(&path).expect("bad config content"),
            "not json"
        );
        let _ = fs::remove_dir_all(home);
    }

    #[test]
    fn dry_run_reports_planned_config_file_without_writing() {
        let home = temp_path("cursor-dry-run");
        let target = ConfigTarget {
            path: PathBuf::from("/tmp/config.toml"),
            source: ConfigTargetSource::HomeDefault,
        };
        let plan =
            McpPlan::for_target_with_home(SetupMcpTarget::Cursor, &target, Some(home.clone()));
        let report = McpTargetReport::planned(plan);
        assert_eq!(report.status, SetupStatus::Planned);
        assert_eq!(report.action, McpAction::WriteConfig);
        assert_eq!(report.config_files.len(), 1);
        assert_eq!(report.config_files[0].action, "would_update");
        assert_eq!(
            report.config_files[0].path,
            home.join(".cursor").join("mcp.json").display().to_string()
        );
        assert!(!home.exists());
    }

    #[test]
    fn hermes_default_config_installs_plugin_and_runs_setup() {
        let target = ConfigTarget {
            path: PathBuf::from("/tmp/config.toml"),
            source: ConfigTargetSource::HomeDefault,
        };
        let plan = McpPlan::for_target(SetupMcpTarget::Hermes, &target);
        let commands = plan.commands();
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].args[0], "plugins");
        assert_eq!(commands[0].args[1], "install");
        assert!(
            commands[0].args[2] == "eric-tramel/moraine/plugins/hermes-moraine"
                || (commands[0].args[2].starts_with("file://")
                    && commands[0].args[2].ends_with("#plugins/hermes-moraine"))
        );
        assert_eq!(commands[0].args[3], "--force");
        assert_eq!(commands[0].args[4], "--enable");
        assert_eq!(commands[1].args, vec!["moraine", "setup", "--no-test"]);
    }

    #[test]
    fn hermes_plugin_identifier_for_root_uses_local_checkout() {
        let root = temp_path("hermes plugin root");
        let plugin_dir = root.join("plugins").join("hermes-moraine");
        fs::create_dir_all(&plugin_dir).expect("create plugin dir");
        fs::write(plugin_dir.join("plugin.yaml"), "name: moraine\n").expect("write plugin yaml");

        let identifier =
            harnesses::hermes_plugin_identifier_for_root(&root).expect("local identifier");
        assert!(identifier.starts_with("file://"));
        assert!(identifier.ends_with("#plugins/hermes-moraine"));
        assert!(identifier.contains("hermes%20plugin%20root"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn hermes_plugin_identifier_for_root_ignores_missing_plugin() {
        let root = temp_path("missing-hermes-plugin");
        fs::create_dir_all(&root).expect("create root");

        assert!(harnesses::hermes_plugin_identifier_for_root(&root).is_none());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn hermes_custom_config_is_manual() {
        let target = ConfigTarget {
            path: PathBuf::from("/tmp/custom.toml"),
            source: ConfigTargetSource::Cli,
        };
        let plan = McpPlan::for_target(SetupMcpTarget::Hermes, &target);
        assert_eq!(plan.action, McpAction::ManualInstructions);
        assert!(plan
            .manual_snippet
            .expect("manual snippet")
            .contains("--config /tmp/custom.toml"));
    }

    #[test]
    fn hermes_plugin_setup_failure_is_reported() {
        let target = ConfigTarget {
            path: PathBuf::from("/tmp/config.toml"),
            source: ConfigTargetSource::HomeDefault,
        };
        let plan = McpPlan::for_target(SetupMcpTarget::Hermes, &target);
        let commands = plan.commands();
        let mut runner = FakeRunner::default()
            .with_existing("hermes")
            .with_response(commands[0].clone(), true, "")
            .with_output_response(
                commands[1].clone(),
                true,
                "Moraine MCP setup needs attention.",
                "",
            );
        let report = execute_mcp_plan(plan, &mut runner).expect("execute mcp");
        assert_eq!(report.status, SetupStatus::Error);
        assert!(report
            .error
            .as_deref()
            .expect("error")
            .contains("did not report successful setup"));
        assert_eq!(runner.ran, commands);
    }

    #[test]
    fn hermes_plugin_setup_success_is_accepted() {
        let target = ConfigTarget {
            path: PathBuf::from("/tmp/config.toml"),
            source: ConfigTargetSource::HomeDefault,
        };
        let plan = McpPlan::for_target(SetupMcpTarget::Hermes, &target);
        let commands = plan.commands();
        let mut runner = FakeRunner::default()
            .with_existing("hermes")
            .with_response(commands[0].clone(), true, "")
            .with_output_response(commands[1].clone(), true, "Moraine MCP setup complete.", "");
        let report = execute_mcp_plan(plan, &mut runner).expect("execute mcp");
        assert_eq!(report.status, SetupStatus::Ok);
        assert_eq!(runner.ran, commands);
    }

    #[test]
    fn hermes_manual_mcp_args_accept_tool_enable_prompt() {
        let target = ConfigTarget {
            path: PathBuf::from("/tmp/config.toml"),
            source: ConfigTargetSource::HomeDefault,
        };
        assert_eq!(
            harnesses::hermes_args(&target),
            vec![
                "mcp",
                "add",
                "moraine",
                "--command",
                "moraine",
                "--args",
                "run",
                "mcp",
            ]
        );
    }

    #[test]
    fn hermes_manual_mcp_cancelled_stdout_is_error_even_with_zero_status() {
        let target = ConfigTarget {
            path: PathBuf::from("/tmp/config.toml"),
            source: ConfigTargetSource::HomeDefault,
        };
        let plan = McpPlan::replace_registration(
            SetupMcpTarget::Hermes,
            CommandSpec::new("hermes", ["mcp", "remove", "moraine"]),
            McpPlanStep::required_stdout(
                CommandSpec::new("hermes", harnesses::hermes_args(&target)).with_stdin("\n"),
                "tools enabled",
            ),
        );
        let commands = plan.commands();
        let mut runner = FakeRunner::default()
            .with_existing("hermes")
            .with_response(commands[0].clone(), false, "server not found")
            .with_output_response(commands[1].clone(), true, "Cancelled.", "");
        let report = execute_mcp_plan(plan, &mut runner).expect("execute mcp");
        assert_eq!(report.status, SetupStatus::Error);
        assert!(report
            .error
            .as_deref()
            .expect("error")
            .contains("did not report successful setup"));
        assert_eq!(runner.ran, commands);
    }

    #[test]
    fn hermes_manual_mcp_args_include_custom_config() {
        let target = ConfigTarget {
            path: PathBuf::from("/tmp/custom.toml"),
            source: ConfigTargetSource::Cli,
        };
        assert_eq!(
            harnesses::hermes_args(&target),
            vec![
                "mcp",
                "add",
                "moraine",
                "--command",
                "moraine",
                "--args",
                "run",
                "mcp",
                "--config",
                "/tmp/custom.toml",
            ]
        );
    }

    #[test]
    fn explicit_noninteractive_mcp_requires_yes() {
        let args = SetupArgs {
            yes: false,
            dry_run: false,
            skip_config: true,
            skip_mcp: false,
            repair_config: false,
            mcp_targets: vec![SetupMcpTarget::Codex],
        };
        let mut runner = FakeRunner::default().with_existing("codex");
        let report = setup_mcp_target(
            &args,
            &ConfigTarget {
                path: PathBuf::from("/tmp/config.toml"),
                source: ConfigTargetSource::HomeDefault,
            },
            SetupMcpTarget::Codex,
            false,
            false,
            &mut runner,
        )
        .expect("mcp target");
        assert_eq!(report.status, SetupStatus::Error);
        assert!(runner.ran.is_empty());
    }

    #[test]
    fn fake_runner_records_codex_command() {
        let target = ConfigTarget {
            path: PathBuf::from("/tmp/config.toml"),
            source: ConfigTargetSource::HomeDefault,
        };
        let plan = McpPlan::for_target(SetupMcpTarget::Codex, &target);
        let commands = plan.commands();
        let mut runner = FakeRunner::default()
            .with_existing("codex")
            .with_response(commands[0].clone(), true, "")
            .with_response(commands[1].clone(), true, "")
            .with_response(commands[2].clone(), true, "");
        let report = execute_mcp_plan(plan, &mut runner).expect("execute mcp");
        assert_eq!(report.status, SetupStatus::Ok);
        assert_eq!(runner.ran, commands);
    }

    #[test]
    fn manual_remove_failure_continues_after_plugin_install() {
        let target = ConfigTarget {
            path: PathBuf::from("/tmp/config.toml"),
            source: ConfigTargetSource::HomeDefault,
        };
        let plan = McpPlan::for_target(SetupMcpTarget::Codex, &target);
        let commands = plan.commands();
        let mut runner = FakeRunner::default()
            .with_existing("codex")
            .with_response(commands[0].clone(), true, "")
            .with_response(commands[1].clone(), true, "")
            .with_response(commands[2].clone(), false, "server not found");
        let report = execute_mcp_plan(plan, &mut runner).expect("execute mcp");
        assert_eq!(report.status, SetupStatus::Ok);
        assert!(!report.warnings.is_empty());
        assert_eq!(runner.ran, commands);
    }

    #[test]
    fn plugin_install_failure_is_reported_as_error() {
        let target = ConfigTarget {
            path: PathBuf::from("/tmp/config.toml"),
            source: ConfigTargetSource::HomeDefault,
        };
        let plan = McpPlan::for_target(SetupMcpTarget::Codex, &target);
        let commands = plan.commands();
        let mut runner = FakeRunner::default()
            .with_existing("codex")
            .with_response(commands[0].clone(), true, "")
            .with_response(commands[1].clone(), false, "plugin install failed");
        let report = execute_mcp_plan(plan, &mut runner).expect("execute mcp");
        assert_eq!(report.status, SetupStatus::Error);
        assert!(report
            .error
            .as_deref()
            .expect("error")
            .contains(&commands[1].display()));
        assert_eq!(runner.ran, commands[..2]);
    }

    #[test]
    fn config_failure_skips_explicit_mcp_targets() {
        let dir = temp_path("invalid-skips-mcp");
        fs::create_dir_all(&dir).expect("create dir");
        let path = dir.join("config.toml");
        fs::write(&path, "not = [valid").expect("write invalid config");
        let args = SetupArgs {
            yes: true,
            dry_run: false,
            skip_config: false,
            skip_mcp: false,
            repair_config: false,
            mcp_targets: vec![SetupMcpTarget::Codex],
        };
        let mut runner = FakeRunner::default().with_existing("codex");
        let report = run_setup(
            &plain_output(),
            &args,
            ConfigTarget {
                path: path.clone(),
                source: ConfigTargetSource::Cli,
            },
            false,
            &mut runner,
        )
        .expect("setup report");
        assert!(!report.success);
        assert_eq!(report.config.status, SetupStatus::Error);
        assert_eq!(report.mcp_targets.len(), 1);
        assert_eq!(report.mcp_targets[0].status, SetupStatus::Skipped);
        assert!(runner.ran.is_empty());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn json_report_redacts_command_output_unless_verbose() {
        let command = CommandSpec::new("codex", ["mcp", "list"]);
        let report = SetupReport {
            success: true,
            config: ConfigReport::ok(Path::new("/tmp/config.toml"), "unchanged", "ok"),
            mcp_targets: vec![McpTargetReport {
                target: SetupMcpTarget::Codex,
                action: McpAction::Execute,
                status: SetupStatus::Ok,
                commands: vec![command.clone()],
                config_files: Vec::new(),
                manual_snippet: None,
                skipped_reason: None,
                warnings: Vec::new(),
                error: None,
                command_results: vec![CommandRunReport {
                    command,
                    success: true,
                    status_code: Some(0),
                    stdout: "account@example.test".to_string(),
                    stderr: "/sensitive/path".to_string(),
                }],
            }],
        };

        let redacted = report_for_json(&report, false);
        let result = &redacted.mcp_targets[0].command_results[0];
        assert!(result.stdout.is_empty());
        assert!(result.stderr.is_empty());

        let verbose = report_for_json(&report, true);
        let result = &verbose.mcp_targets[0].command_results[0];
        assert_eq!(result.stdout, "account@example.test");
        assert_eq!(result.stderr, "/sensitive/path");
    }
}
