use anyhow::{bail, Context, Result};
use dialoguer::console::{style, Key, Style, Term};
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::BTreeSet;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, IsTerminal, Write};
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cli::{SetupArgs, SetupMcpTarget};
use crate::render::CliOutput;
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
            let config_allows_ingest = config.status == SetupStatus::Ok
                || (args.dry_run && config.action == "would_migrate");
            if selections.apply_ingest && !args.skip_config && config_allows_ingest {
                let ingest_update = if args.dry_run {
                    preview_ingest_selections_for_config(&target.path, &selections.targets)
                } else {
                    apply_ingest_selections_to_config(&target.path, &selections.targets)
                };
                match ingest_update {
                    Ok(update) => config.apply_ingest_update(update, args.dry_run),
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
        ConfigState::Valid { backend_bind } => {
            migrate_backend_config(&target.path, &backend_bind, args.dry_run)
        }
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
    Valid { backend_bind: String },
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
        Ok(config) => ConfigState::Valid {
            backend_bind: config.backend.bind,
        },
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct BackendConfigMigration {
    materialized_start_on_up: bool,
    updated_start_on_up: bool,
    materialized_bind: bool,
    removed_monitor_host: bool,
    removed_monitor_alias: bool,
    removed_mcp_alias: bool,
    removed_central_alias: bool,
}

impl BackendConfigMigration {
    fn has_changes(&self) -> bool {
        self.materialized_start_on_up
            || self.updated_start_on_up
            || self.materialized_bind
            || self.removed_monitor_host
            || self.removed_launch_alias_count() > 0
    }

    fn removed_launch_alias_count(&self) -> usize {
        self.removed_monitor_alias as usize
            + self.removed_mcp_alias as usize
            + self.removed_central_alias as usize
    }

    fn summary(&self, dry_run: bool) -> String {
        let mut changes = Vec::new();
        if self.materialized_start_on_up {
            changes.push(format!(
                "backend.start_on_up=true {}",
                if dry_run {
                    "would be materialized"
                } else {
                    "materialized"
                }
            ));
        }
        if self.updated_start_on_up {
            changes.push(format!(
                "backend.start_on_up=true {}",
                if dry_run {
                    "would be updated"
                } else {
                    "updated"
                }
            ));
        }
        if self.materialized_bind {
            changes.push(format!(
                "backend.bind {}",
                if dry_run {
                    "would be materialized"
                } else {
                    "materialized"
                }
            ));
        }
        if self.removed_monitor_host {
            changes.push(format!(
                "obsolete monitor.host {}",
                if dry_run {
                    "would be removed"
                } else {
                    "removed"
                }
            ));
        }

        let removed = self.removed_launch_alias_count();
        if removed > 0 {
            changes.push(format!(
                "{removed} obsolete launch {} {}",
                if removed == 1 { "key" } else { "keys" },
                if dry_run {
                    "would be removed"
                } else {
                    "removed"
                }
            ));
        }
        changes.join("; ")
    }
}

fn migrate_backend_config(path: &Path, backend_bind: &str, dry_run: bool) -> Result<ConfigReport> {
    let mut document = read_config_document(path)?;
    let migration = apply_backend_config_migration_to_document(&mut document, backend_bind)?;
    if !migration.has_changes() {
        return Ok(ConfigReport::ok(
            path,
            "unchanged",
            "existing config loads successfully; backend config is canonical",
        ));
    }

    if dry_run {
        return Ok(ConfigReport::planned(
            path,
            "would_migrate",
            &migration.summary(true),
        ));
    }

    write_toml_atomic(path, &document.to_string())?;
    Ok(ConfigReport::ok(
        path,
        "migrated",
        &migration.summary(false),
    ))
}

fn apply_backend_config_migration_to_document(
    document: &mut DocumentMut,
    backend_bind: &str,
) -> Result<BackendConfigMigration> {
    let backend_start_value = document
        .as_table()
        .get("backend")
        .and_then(Item::as_table_like)
        .and_then(|backend| backend.get("start_on_up"))
        .and_then(Item::as_bool);
    let backend_start_explicit = backend_start_value.is_some();
    let updated_start_on_up = backend_start_value == Some(false);
    let backend_bind_explicit = document
        .as_table()
        .get("backend")
        .and_then(Item::as_table_like)
        .and_then(|backend| backend.get("bind"))
        .is_some();
    let legacy_monitor_host = take_config_table_key(document, "monitor", "host")?;
    let removed_monitor_host = legacy_monitor_host.is_some();

    if document.as_table().get("backend").is_none() {
        document["backend"] = Item::Table(Table::new());
    }
    if !backend_bind_explicit && legacy_monitor_host.is_some() {
        let backend = document
            .as_table_mut()
            .get_mut("backend")
            .expect("backend table was created above");
        if let Item::Value(toml_edit::Value::InlineTable(_)) = backend {
            let Item::Value(toml_edit::Value::InlineTable(inline)) = std::mem::take(backend) else {
                unreachable!("backend variant was checked above");
            };
            let decor = inline.decor().clone();
            let mut table = inline.into_table();
            *table.decor_mut() = decor;
            *backend = Item::Table(table);
        }
    }
    let backend = document
        .as_table_mut()
        .get_mut("backend")
        .and_then(Item::as_table_like_mut)
        .ok_or_else(|| anyhow::anyhow!("backend must be a TOML table"))?;
    if !backend_start_explicit || updated_start_on_up {
        let _ = backend.insert("start_on_up", toml_edit::value(true));
    }
    if !backend_bind_explicit {
        match legacy_monitor_host {
            Some((decor, item)) => {
                let key = toml_edit::Key::new("bind").with_leaf_decor(decor);
                let _ = backend.entry_format(&key).or_insert(item);
            }
            None => {
                let _ = backend.insert("bind", toml_edit::value(backend_bind));
            }
        }
    }

    let removed_monitor_alias =
        remove_config_table_key(document, "runtime", "start_monitor_on_up")?;
    let removed_mcp_alias = remove_config_table_key(document, "runtime", "start_mcp_on_up")?;
    let removed_central_alias = remove_config_table_key(document, "mcp", "start_central_on_up")?;

    Ok(BackendConfigMigration {
        materialized_start_on_up: !backend_start_explicit,
        updated_start_on_up,
        materialized_bind: !backend_bind_explicit,
        removed_monitor_host,
        removed_monitor_alias,
        removed_mcp_alias,
        removed_central_alias,
    })
}

fn take_config_table_key(
    document: &mut DocumentMut,
    table: &str,
    key: &str,
) -> Result<Option<(toml_edit::Decor, Item)>> {
    let Some(item) = document.as_table_mut().get_mut(table) else {
        return Ok(None);
    };
    let table = item
        .as_table_like_mut()
        .ok_or_else(|| anyhow::anyhow!("{table} must be a TOML table"))?;
    let decor = table
        .key(key)
        .map(|key| key.leaf_decor().clone())
        .unwrap_or_default();
    Ok(table.remove(key).map(|item| (decor, item)))
}

fn remove_config_table_key(document: &mut DocumentMut, table: &str, key: &str) -> Result<bool> {
    Ok(take_config_table_key(document, table, key)?.is_some())
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
        format!("ingest sources updated: {}", self.change_summary())
    }

    fn planned_summary(self) -> String {
        if !self.has_changes() {
            return "ingest sources already matched selection".to_string();
        }
        format!("ingest sources would be updated: {}", self.change_summary())
    }

    fn change_summary(self) -> String {
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
        parts.join(", ")
    }
}

fn preview_ingest_selections_for_config(
    path: &Path,
    selections: &[SetupTargetSelection],
) -> Result<IngestSelectionUpdate> {
    let mut document = read_config_document(path)?;
    apply_ingest_selections_to_document(&mut document, selections)
}

fn apply_ingest_selections_to_config(
    path: &Path,
    selections: &[SetupTargetSelection],
) -> Result<IngestSelectionUpdate> {
    let mut document = read_config_document(path)?;
    let update = apply_ingest_selections_to_document(&mut document, selections)?;
    if update.has_changes() {
        write_toml_atomic(path, &document.to_string())?;
    }
    Ok(update)
}

fn read_config_document(path: &Path) -> Result<DocumentMut> {
    let content =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    content
        .parse::<DocumentMut>()
        .with_context(|| format!("{} is not valid TOML", path.display()))
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
    Ok(sources
        .iter()
        .position(|source| source.get("name").and_then(Item::as_str) == Some(setup_source.name())))
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

    if args.yes || args.dry_run {
        return Ok(SetupSelectionSet {
            targets: default_setup_target_selections(),
            apply_ingest: true,
            confirmed_harness: true,
        });
    }

    if !interactive {
        return Ok(SetupSelectionSet {
            targets: Vec::new(),
            apply_ingest: false,
            confirmed_harness: false,
        });
    }

    let targets = harnesses::setup_targets();
    prompt_setup_target_selector(&targets, runner)
}

fn default_setup_target_selections() -> Vec<SetupTargetSelection> {
    setup_target_selection_rows(harnesses::setup_targets())
}

fn setup_target_selection_rows(
    targets: impl IntoIterator<Item = SetupMcpTarget>,
) -> Vec<SetupTargetSelection> {
    targets
        .into_iter()
        .map(|target| SetupTargetSelection {
            target,
            mode: SetupSelectionMode::IngestAndHarness,
        })
        .collect()
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
    let mut selections = setup_target_selection_rows(targets.iter().copied());
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
            .apply_to("All harnesses start selected. Space cycles ingest + harness → ingest only → harness only → none. Enter applies. Esc skips.")
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
    style: crate::progress::ProgressStyle,
    started: bool,
}

impl SetupProgress {
    fn from_output(output: &CliOutput) -> Self {
        Self {
            style: crate::progress::ProgressStyle::from_output(output),
            started: false,
        }
    }

    #[cfg(test)]
    fn disabled() -> Self {
        Self {
            style: crate::progress::ProgressStyle::disabled(),
            started: false,
        }
    }

    fn finish(&mut self) {
        if !self.style.enabled() || !self.started {
            return;
        }
        eprintln!(
            "{} {}",
            self.style.branch("╰─", "`-", Style::new().bright().black()),
            self.style.dim("setup summary follows")
        );
    }

    fn target_start(&mut self, target: SetupMcpTarget, plan: &McpPlan) {
        if !self.style.enabled() {
            return;
        }
        self.ensure_started();
        eprintln!(
            "{} {} {}",
            self.style.branch("├─", "+-", Style::new().bright().black()),
            self.style.bold_label(target.label()),
            self.style.dim(plan.target.setup_kind())
        );
    }

    fn target_success(&self, target: SetupMcpTarget) {
        if !self.style.enabled() {
            return;
        }
        eprintln!(
            "   {} {}",
            self.style.mark("✓", "[ok]", Style::new().green()),
            self.style.dim(&format!("{} configured", target.label()))
        );
    }

    fn target_error(&self, target: SetupMcpTarget) {
        if !self.style.enabled() {
            return;
        }
        eprintln!(
            "   {} {}",
            self.style.mark("✗", "[err]", Style::new().red()),
            self.style
                .dim(&format!("{} needs attention", target.label()))
        );
    }

    fn target_skipped(&self, target: SetupMcpTarget, reason: &str) {
        if !self.style.enabled() {
            return;
        }
        eprintln!(
            "   {} {} {}",
            self.style.mark("–", "[-]", Style::new().yellow()),
            self.style.dim(target.label()),
            self.style.dim(reason)
        );
    }

    fn command_start(&self, step: &McpPlanStep) {
        if !self.style.enabled() {
            return;
        }
        eprintln!(
            "   {} {}",
            self.style.mark("→", ">", Style::new().cyan()),
            self.style.label(step.progress_label)
        );
    }

    fn command_success(&self, step: &McpPlanStep) {
        if !self.style.enabled() {
            return;
        }
        eprintln!(
            "   {} {}",
            self.style.mark("✓", "[ok]", Style::new().green()),
            self.style.dim(step.success_label)
        );
    }

    fn command_warning(&self, step: &McpPlanStep, warning: &str) {
        if !self.style.enabled() {
            return;
        }
        eprintln!(
            "   {} {} {}",
            self.style.mark("!", "[warn]", Style::new().yellow()),
            self.style.dim(step.warning_label),
            self.style.dim(warning)
        );
    }

    fn command_error(&self, step: &McpPlanStep, error: &str) {
        if !self.style.enabled() {
            return;
        }
        eprintln!(
            "   {} {} {}",
            self.style.mark("✗", "[err]", Style::new().red()),
            self.style.dim(step.error_label),
            self.style.dim(error)
        );
    }

    fn config_start(&self, write: &McpConfigWrite) {
        if !self.style.enabled() {
            return;
        }
        eprintln!(
            "   {} {}",
            self.style.mark("→", ">", Style::new().cyan()),
            self.style.label(&format!(
                "Updating {} config at {}",
                write.label(),
                write.path().display()
            ))
        );
    }

    fn config_success(&self, write: &McpConfigWrite) {
        if !self.style.enabled() {
            return;
        }
        eprintln!(
            "   {} {}",
            self.style.mark("✓", "[ok]", Style::new().green()),
            self.style
                .dim(&format!("Updated {}", write.path().display()))
        );
    }

    fn config_error(&self, write: &McpConfigWrite, error: &str) {
        if !self.style.enabled() {
            return;
        }
        eprintln!(
            "   {} {} {}",
            self.style.mark("✗", "[err]", Style::new().red()),
            self.style
                .dim(&format!("Could not update {}", write.path().display())),
            self.style.dim(error)
        );
    }

    fn ensure_started(&mut self) {
        if self.started {
            return;
        }
        self.started = true;
        if self.style.rich() {
            eprintln!();
            eprintln!(
                "{} {}",
                self.style.branch("╭─", ".-", Style::new().cyan()),
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

    fn apply_ingest_update(&mut self, update: IngestSelectionUpdate, dry_run: bool) {
        if update.has_changes() {
            if dry_run || self.status == SetupStatus::Planned {
                if self.action == "unchanged" {
                    self.action = "would_update".to_string();
                }
                if self.status == SetupStatus::Ok {
                    self.status = SetupStatus::Planned;
                }
                self.message = format!("{}; {}", self.message, update.planned_summary());
                return;
            }

            if self.action == "unchanged" {
                self.action = "updated".to_string();
            }
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
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
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

    fn setup_config_for_test(path: &Path, dry_run: bool, repair_config: bool) -> ConfigReport {
        let args = SetupArgs {
            yes: true,
            dry_run,
            skip_config: false,
            skip_mcp: true,
            repair_config,
            mcp_targets: Vec::new(),
        };
        setup_config(
            &args,
            &ConfigTarget {
                path: path.to_path_buf(),
                source: ConfigTargetSource::Cli,
            },
            false,
        )
        .expect("setup config")
    }

    fn read_toml_document(path: &Path) -> DocumentMut {
        fs::read_to_string(path)
            .expect("read config")
            .parse::<DocumentMut>()
            .expect("parse config")
    }

    fn config_item<'a>(document: &'a DocumentMut, table: &str, key: &str) -> Option<&'a Item> {
        document
            .as_table()
            .get(table)
            .and_then(Item::as_table_like)
            .and_then(|table| table.get(key))
    }

    fn repo_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("repo root")
            .to_path_buf()
    }

    fn shared_plugin_mcp_config() -> Value {
        let shared_mcp_path = repo_root()
            .join("plugins")
            .join("moraine")
            .join(".mcp.json");
        serde_json::from_str(
            &fs::read_to_string(&shared_mcp_path).expect("read shared plugin mcp config"),
        )
        .expect("shared plugin mcp json")
    }

    fn shared_plugin_launcher() -> String {
        shared_plugin_mcp_config()["mcpServers"]["moraine"]["args"][2]
            .as_str()
            .expect("shared inline launcher script")
            .to_string()
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
    fn default_target_selections_enable_ingest_and_harnesses() {
        let selections = default_setup_target_selections();
        assert_eq!(
            selections
                .iter()
                .map(|selection| selection.target)
                .collect::<Vec<_>>(),
            harnesses::setup_targets()
        );
        assert!(selections
            .iter()
            .all(|selection| selection.mode == SetupSelectionMode::IngestAndHarness));
    }

    #[test]
    fn yes_without_explicit_targets_selects_all_default_harnesses() {
        let args = SetupArgs {
            yes: true,
            dry_run: false,
            skip_config: false,
            skip_mcp: false,
            repair_config: false,
            mcp_targets: Vec::new(),
        };
        let selections =
            setup_target_selections(&args, false, &FakeRunner::default()).expect("selections");

        assert!(selections.apply_ingest);
        assert!(selections.confirmed_harness);
        assert_eq!(selections.harness_targets(), harnesses::setup_targets(),);
        assert!(selections
            .targets
            .iter()
            .all(|selection| selection.mode == SetupSelectionMode::IngestAndHarness));
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
    fn claude_ingest_selection_reconciles_platform_sources_idempotently() {
        let mut document = "# minimal\n"
            .parse::<DocumentMut>()
            .expect("minimal config parses");
        let selection = |mode| SetupTargetSelection {
            target: SetupMcpTarget::ClaudeCode,
            mode,
        };

        let added = apply_ingest_selections_to_document(
            &mut document,
            &[selection(SetupSelectionMode::IngestOnly)],
        )
        .expect("add Claude ingest sources");
        let expected_sources = if cfg!(target_os = "macos") { 2 } else { 1 };
        assert_eq!(added.added_sources, expected_sources);
        assert_eq!(
            sources_for_harness(&document, "claude-code").len(),
            expected_sources
        );
        assert!(source_enabled(&document, "claude"));
        #[cfg(target_os = "macos")]
        {
            assert!(source_enabled(&document, "claude-cowork"));
            assert_eq!(
                source_value(&document, "claude-cowork", "glob"),
                Some(
                    "~/Library/Application Support/Claude/local-agent-mode-sessions/**/.claude/projects/**/*.jsonl"
                )
            );
        }

        let unchanged = apply_ingest_selections_to_document(
            &mut document,
            &[selection(SetupSelectionMode::IngestOnly)],
        )
        .expect("reapply Claude ingest selection");
        assert_eq!(unchanged, IngestSelectionUpdate::default());

        let disabled = apply_ingest_selections_to_document(
            &mut document,
            &[selection(SetupSelectionMode::Off)],
        )
        .expect("disable Claude ingest sources");
        assert_eq!(disabled.disabled_sources, expected_sources);
        assert!(!source_enabled(&document, "claude"));
        #[cfg(target_os = "macos")]
        assert!(!source_enabled(&document, "claude-cowork"));

        let reenabled = apply_ingest_selections_to_document(
            &mut document,
            &[selection(SetupSelectionMode::IngestOnly)],
        )
        .expect("re-enable Claude ingest sources");
        assert_eq!(reenabled.enabled_sources, expected_sources);
    }

    #[test]
    fn qwen_ingest_selection_repairs_setup_source_and_preserves_custom_sources() {
        let mut document = r#"
[[ingest.sources]]
name = "qwen-code"
harness = "codex"
enabled = false
glob = "~/stale/*.jsonl"
watch_root = "~/stale"

[[ingest.sources]]
name = "qwen-archive"
harness = "qwen-code"
enabled = false
glob = "~/archive/**/*.jsonl"
watch_root = "~/archive"
"#
        .parse::<DocumentMut>()
        .expect("fixture config parses");

        let update = apply_ingest_selections_to_document(
            &mut document,
            &[SetupTargetSelection {
                target: SetupMcpTarget::QwenCode,
                mode: SetupSelectionMode::IngestOnly,
            }],
        )
        .expect("reconcile Qwen source");

        assert_eq!(
            update,
            IngestSelectionUpdate {
                enabled_sources: 1,
                disabled_sources: 0,
                added_sources: 0,
                updated_sources: 1,
            }
        );
        assert_eq!(sources_for_harness(&document, "qwen-code").len(), 2);
        assert_eq!(
            source_value(&document, "qwen-code", "harness"),
            Some("qwen-code")
        );
        assert_eq!(
            source_value(&document, "qwen-code", "glob"),
            Some("~/.qwen/projects/*/chats/*.jsonl")
        );
        assert_eq!(
            source_value(&document, "qwen-code", "watch_root"),
            Some("~/.qwen/projects")
        );
        assert_eq!(
            source_value(&document, "qwen-code", "format"),
            Some("jsonl")
        );
        assert!(source_enabled(&document, "qwen-code"));
        assert!(!source_enabled(&document, "qwen-archive"));
        assert_eq!(
            source_value(&document, "qwen-archive", "glob"),
            Some("~/archive/**/*.jsonl")
        );

        let unchanged = apply_ingest_selections_to_document(
            &mut document,
            &[SetupTargetSelection {
                target: SetupMcpTarget::QwenCode,
                mode: SetupSelectionMode::IngestOnly,
            }],
        )
        .expect("reapply Qwen source");
        assert_eq!(unchanged, IngestSelectionUpdate::default());
    }

    #[test]
    fn ingest_selection_adds_omp_source_to_existing_pi_config() {
        let mut document = r#"
[ingest]

[[ingest.sources]]
name = "pi"
harness = "pi-coding-agent"
enabled = true
glob = "~/.pi/agent/sessions/**/*.jsonl"
watch_root = "~/.pi/agent/sessions"
format = "jsonl"
"#
        .parse::<DocumentMut>()
        .expect("existing pi config parses");

        let update = apply_ingest_selections_to_document(
            &mut document,
            &[SetupTargetSelection {
                target: SetupMcpTarget::PiCodingAgent,
                mode: SetupSelectionMode::IngestOnly,
            }],
        )
        .expect("apply pi ingest selection");

        assert_eq!(
            update,
            IngestSelectionUpdate {
                enabled_sources: 0,
                disabled_sources: 0,
                added_sources: 1,
                updated_sources: 0,
            }
        );
        assert!(source_enabled(&document, "pi"));
        assert!(source_enabled(&document, "omp"));
        assert_eq!(
            source_value(&document, "omp", "glob"),
            Some("~/.omp/agent/sessions/**/*.jsonl")
        );
        assert_eq!(
            source_value(&document, "omp", "watch_root"),
            Some("~/.omp/agent/sessions")
        );
        assert_eq!(source_value(&document, "omp", "format"), Some("jsonl"));

        let path = temp_path("ingest-adds-omp-to-pi-config");
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
        let cfg = moraine_config::load_config(&path).expect("written config loads");
        assert_eq!(cfg.backend.bind, "127.0.0.1");
        assert!(cfg.backend.auth_token.is_none());
        let document = read_toml_document(&path);
        assert_eq!(
            config_item(&document, "backend", "bind").and_then(Item::as_str),
            Some("127.0.0.1")
        );
        assert!(config_item(&document, "backend", "auth_token").is_none());
        assert!(config_item(&document, "monitor", "host").is_none());
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
    fn valid_config_is_migrated_without_repair_even_with_repair_flag() {
        let dir = temp_path("valid-no-repair");
        fs::create_dir_all(&dir).expect("create dir");
        let path = dir.join("config.toml");
        let original = "# minimal\n\n[backend]\nstart_on_up = false\n";
        fs::write(&path, original).expect("write config");
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
        assert_eq!(report.action, "migrated");
        assert!(report.backup_path.is_none());
        let content = fs::read_to_string(path).expect("config content");
        assert!(content.contains("# minimal"));
        let document = content
            .parse::<DocumentMut>()
            .expect("parse migrated config");
        assert_eq!(
            config_item(&document, "backend", "start_on_up").and_then(Item::as_bool),
            Some(true)
        );
        assert_eq!(
            config_item(&document, "backend", "bind").and_then(Item::as_str),
            Some("127.0.0.1")
        );
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn setup_migrates_monitor_and_central_backend_launch_aliases() {
        for (label, table, key) in [
            ("monitor", "runtime", "start_monitor_on_up"),
            ("central", "mcp", "start_central_on_up"),
        ] {
            let path = temp_path(&format!("legacy-backend-{label}"));
            let original = format!("# preserved heading\n\n[{table}]\n{key} = true\n");
            fs::write(&path, &original).expect("write legacy config");

            let report = setup_config_for_test(&path, false, false);

            assert_eq!(report.status, SetupStatus::Ok);
            assert_eq!(report.action, "migrated");
            assert!(report.backup_path.is_none());
            let document = read_toml_document(&path);
            assert_eq!(
                config_item(&document, "backend", "start_on_up").and_then(Item::as_bool),
                Some(true)
            );
            assert!(config_item(&document, "runtime", "start_monitor_on_up").is_none());
            assert!(config_item(&document, "runtime", "start_mcp_on_up").is_none());
            assert!(config_item(&document, "mcp", "start_central_on_up").is_none());
            assert!(fs::read_to_string(&path)
                .expect("migrated config")
                .contains("# preserved heading"));
            assert!(
                moraine_config::load_config(&path)
                    .expect("migrated config loads")
                    .backend
                    .start_on_up
            );
            let _ = fs::remove_file(path);
        }
    }

    #[test]
    fn setup_migrates_deprecated_runtime_mcp_launch_alias() {
        let path = temp_path("legacy-backend-runtime-mcp");
        fs::write(&path, "[runtime]\nstart_mcp_on_up = true\n")
            .expect("write deprecated alias config");

        let report = setup_config_for_test(&path, false, false);

        assert_eq!(report.status, SetupStatus::Ok);
        assert_eq!(report.action, "migrated");
        let document = read_toml_document(&path);
        assert_eq!(
            config_item(&document, "backend", "start_on_up").and_then(Item::as_bool),
            Some(true)
        );
        assert!(config_item(&document, "runtime", "start_mcp_on_up").is_none());
        assert!(
            moraine_config::load_config(&path)
                .expect("migrated config loads")
                .backend
                .start_on_up
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn setup_materializes_backend_on_without_legacy_keys() {
        let path = temp_path("backend-no-legacy");
        let original =
            "# minimal config\n\n[monitor]\n# preserve host comment\nhost = \"127.0.0.1\"\n";
        fs::write(&path, original).expect("write minimal config");

        let report = setup_config_for_test(&path, false, false);

        assert_eq!(report.action, "migrated");
        assert!(report.message.contains("backend.start_on_up=true"));
        assert!(report.message.contains("backend.bind"));
        assert!(report.message.contains("obsolete monitor.host removed"));
        let content = fs::read_to_string(&path).expect("migrated config");
        assert!(content.contains("# minimal config"));
        assert!(content.contains("# preserve host comment"));
        let document = content
            .parse::<DocumentMut>()
            .expect("parse migrated config");
        assert_eq!(
            config_item(&document, "backend", "start_on_up").and_then(Item::as_bool),
            Some(true)
        );
        assert_eq!(
            config_item(&document, "backend", "bind").and_then(Item::as_str),
            Some("127.0.0.1")
        );
        assert!(config_item(&document, "monitor", "host").is_none());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn setup_materializes_backend_on_when_all_legacy_aliases_are_false() {
        let path = temp_path("backend-all-aliases-false");
        fs::write(
            &path,
            "[runtime]\nstart_monitor_on_up = false\nstart_mcp_on_up = false\n\n[mcp]\nstart_central_on_up = false\n",
        )
        .expect("write legacy config");

        let report = setup_config_for_test(&path, false, false);

        assert_eq!(report.action, "migrated");
        let document = read_toml_document(&path);
        assert_eq!(
            config_item(&document, "backend", "start_on_up").and_then(Item::as_bool),
            Some(true)
        );
        assert!(config_item(&document, "runtime", "start_monitor_on_up").is_none());
        assert!(config_item(&document, "runtime", "start_mcp_on_up").is_none());
        assert!(config_item(&document, "mcp", "start_central_on_up").is_none());
        assert!(
            moraine_config::load_config(&path)
                .expect("migrated config loads")
                .backend
                .start_on_up
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn setup_enables_explicit_backend_and_removes_conflicting_aliases() {
        for explicit in [false, true] {
            let legacy = !explicit;
            let path = temp_path(&format!("backend-explicit-{explicit}"));
            fs::write(
                &path,
                format!(
                    "[backend]\nstart_on_up = {explicit}\n\n[runtime]\nstart_monitor_on_up = {legacy}\nstart_mcp_on_up = {legacy}\n\n[mcp]\nstart_central_on_up = {legacy}\n"
                ),
            )
            .expect("write conflicting config");

            let report = setup_config_for_test(&path, false, false);

            assert_eq!(report.action, "migrated");
            assert_eq!(
                report.message.contains("backend.start_on_up=true updated"),
                !explicit
            );
            assert!(report.message.contains("3 obsolete launch keys removed"));
            let document = read_toml_document(&path);
            assert_eq!(
                config_item(&document, "backend", "start_on_up").and_then(Item::as_bool),
                Some(true)
            );
            assert!(config_item(&document, "runtime", "start_monitor_on_up").is_none());
            assert!(config_item(&document, "runtime", "start_mcp_on_up").is_none());
            assert!(config_item(&document, "mcp", "start_central_on_up").is_none());
            assert!(
                moraine_config::load_config(&path)
                    .expect("migrated config loads")
                    .backend
                    .start_on_up
            );
            let _ = fs::remove_file(path);
        }
    }

    #[test]
    fn setup_preserves_auth_token_without_exposing_it_in_reports() {
        let path = temp_path("backend-auth-token-preserved");
        let secret = "issue-462-secret-must-not-appear-in-report";
        fs::write(
            &path,
            format!(
                "[backend]\nbind = \"0.0.0.0\"\nstart_on_up = true\nauth_token = \"{secret}\"\n\n[monitor]\nhost = \"127.0.0.1\"\n"
            ),
        )
        .expect("write token config");

        let report = setup_config_for_test(&path, false, false);

        assert_eq!(report.action, "migrated");
        assert!(!report.message.contains(secret));
        assert!(!format!("{report:?}").contains(secret));
        assert!(!serde_json::to_string(&report)
            .expect("serialize config report")
            .contains(secret));
        let cfg = moraine_config::load_config(&path).expect("migrated config loads");
        assert_eq!(cfg.backend.bind, "0.0.0.0");
        assert_eq!(cfg.backend.auth_token.as_deref(), Some(secret));
        let document = read_toml_document(&path);
        assert!(config_item(&document, "monitor", "host").is_none());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn setup_materializes_start_on_up_in_empty_backend_table() {
        let path = temp_path("backend-empty-table");
        fs::write(
            &path,
            "[backend] # preserve backend heading\n# preserve backend note\n",
        )
        .expect("write empty backend config");

        let report = setup_config_for_test(&path, false, false);

        assert_eq!(report.action, "migrated");
        let content = fs::read_to_string(&path).expect("migrated config");
        assert!(content.contains("[backend] # preserve backend heading"));
        assert!(content.contains("# preserve backend note"));
        let document = content
            .parse::<DocumentMut>()
            .expect("parse migrated config");
        assert_eq!(
            config_item(&document, "backend", "start_on_up").and_then(Item::as_bool),
            Some(true)
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn setup_backend_migration_dry_run_reports_without_writing() {
        let path = temp_path("backend-migration-dry-run");
        let original = "[runtime]\nstart_monitor_on_up = true\n";
        fs::write(&path, original).expect("write legacy config");

        let report = setup_config_for_test(&path, true, false);

        assert_eq!(report.status, SetupStatus::Planned);
        assert_eq!(report.action, "would_migrate");
        assert!(report.message.contains("would be materialized"));
        assert!(report.message.contains("would be removed"));
        assert_eq!(fs::read_to_string(&path).expect("config content"), original);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn setup_backend_migration_is_comment_preserving_and_idempotent() {
        let path = temp_path("backend-migration-idempotent");
        let original = r#"# top-level comment

[runtime] # runtime heading
# preserve logs comment
logs_dir = "/tmp/moraine-logs"
start_monitor_on_up = true

[monitor]
# preserve monitor comment
host = "127.0.0.1"
"#;
        fs::write(&path, original).expect("write legacy config");

        let first = setup_config_for_test(&path, false, false);
        let first_content = fs::read_to_string(&path).expect("first config content");
        let second = setup_config_for_test(&path, false, false);
        let second_content = fs::read_to_string(&path).expect("second config content");

        assert_eq!(first.action, "migrated");
        assert!(first.backup_path.is_none());
        assert_eq!(second.action, "unchanged");
        assert_eq!(first_content, second_content);
        assert!(second.backup_path.is_none());
        assert!(first_content.contains("# top-level comment"));
        assert!(first_content.contains("[runtime] # runtime heading"));
        assert!(first_content.contains("# preserve logs comment"));
        assert!(first_content.contains("logs_dir = \"/tmp/moraine-logs\""));
        assert!(first_content.contains("# preserve monitor comment"));
        assert!(!first_content.contains("start_monitor_on_up"));
        let document = first_content
            .parse::<DocumentMut>()
            .expect("parse migrated config");
        assert_eq!(
            config_item(&document, "backend", "bind").and_then(Item::as_str),
            Some("127.0.0.1")
        );
        assert!(config_item(&document, "monitor", "host").is_none());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn setup_migrates_legacy_host_into_inline_backend_without_losing_comments() {
        let path = temp_path("backend-inline-host-migration");
        let original = r#"backend = { start_on_up = false } # preserve inline backend comment

[monitor]
# preserve inline migration comment
host = "127.42.0.9"
"#;
        fs::write(&path, original).expect("write inline backend config");

        let report = setup_config_for_test(&path, false, false);

        assert_eq!(report.action, "migrated");
        let content = fs::read_to_string(&path).expect("migrated config");
        assert!(content.contains("# preserve inline migration comment"));
        assert!(content.contains("# preserve inline backend comment"));
        let document = content
            .parse::<DocumentMut>()
            .expect("parse migrated config");
        assert_eq!(
            config_item(&document, "backend", "start_on_up").and_then(Item::as_bool),
            Some(true)
        );
        assert_eq!(
            config_item(&document, "backend", "bind").and_then(Item::as_str),
            Some("127.42.0.9")
        );
        assert!(config_item(&document, "monitor", "host").is_none());
        moraine_config::load_config(&path).expect("migrated config loads");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn write_toml_atomic_validation_failure_preserves_original_and_cleans_temp_file() {
        let path = temp_path("backend-atomic-validation-failure");
        let original = "[backend]\nstart_on_up = false\n";
        fs::write(&path, original).expect("write original config");

        let result = write_toml_atomic(&path, "[backend]\nstart_on_up = [\n");

        assert!(result.is_err());
        assert_eq!(
            fs::read_to_string(&path).expect("original config"),
            original
        );
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("config file name");
        let temp_prefix = format!(".{file_name}.setup-");
        let leftovers = fs::read_dir(path.parent().expect("config parent"))
            .expect("read config parent")
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(&temp_prefix)
            })
            .collect::<Vec<_>>();
        assert!(
            leftovers.is_empty(),
            "temporary config files must be removed after validation failure"
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn dry_run_invalid_config_repair_does_not_write() {
        let path = temp_path("invalid-repair-dry-run");
        let original = "not = [valid";
        fs::write(&path, original).expect("write invalid config");

        let report = setup_config_for_test(&path, true, true);

        assert_eq!(report.status, SetupStatus::Planned);
        assert_eq!(report.action, "would_repair");
        assert!(report.backup_path.is_none());
        assert_eq!(fs::read_to_string(&path).expect("config content"), original);
        let _ = fs::remove_file(path);
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
    fn claude_default_config_installs_updates_and_cleans_manual_mcp() {
        let target = ConfigTarget {
            path: PathBuf::from("/tmp/config.toml"),
            source: ConfigTargetSource::HomeDefault,
        };
        let plan = McpPlan::for_target(SetupMcpTarget::ClaudeCode, &target);
        let commands = plan.commands();
        assert_eq!(commands.len(), 5);
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
            vec!["plugin", "marketplace", "update", "moraine"]
        );
        assert_eq!(
            commands[2].args,
            vec!["plugin", "install", "moraine@moraine"]
        );
        assert_eq!(
            commands[3].args,
            vec!["plugin", "update", "moraine@moraine"]
        );
        assert_eq!(
            commands[4].args,
            vec!["mcp", "remove", "moraine", "--scope", "user"]
        );
    }

    #[test]
    fn claude_marketplace_add_failure_continues_to_refresh() {
        let target = ConfigTarget {
            path: PathBuf::from("/tmp/config.toml"),
            source: ConfigTargetSource::HomeDefault,
        };
        let plan = McpPlan::for_target(SetupMcpTarget::ClaudeCode, &target);
        let commands = plan.commands();
        let mut runner = FakeRunner::default()
            .with_existing("claude")
            .with_response(commands[0].clone(), false, "marketplace already exists")
            .with_response(commands[1].clone(), true, "")
            .with_response(commands[2].clone(), true, "")
            .with_response(commands[3].clone(), true, "")
            .with_response(commands[4].clone(), true, "");

        let report = execute_mcp_plan(plan, &mut runner).expect("execute Claude plan");

        assert_eq!(report.status, SetupStatus::Ok);
        assert!(!report.warnings.is_empty());
        assert_eq!(runner.ran, commands);
    }

    #[test]
    fn claude_marketplace_update_failure_stops_setup() {
        let target = ConfigTarget {
            path: PathBuf::from("/tmp/config.toml"),
            source: ConfigTargetSource::HomeDefault,
        };
        let plan = McpPlan::for_target(SetupMcpTarget::ClaudeCode, &target);
        let commands = plan.commands();
        let mut runner = FakeRunner::default()
            .with_existing("claude")
            .with_response(commands[0].clone(), true, "")
            .with_response(commands[1].clone(), false, "marketplace update failed");

        let report = execute_mcp_plan(plan, &mut runner).expect("execute Claude plan");

        assert_eq!(report.status, SetupStatus::Error);
        assert!(report
            .error
            .as_deref()
            .expect("error")
            .contains(&commands[1].display()));
        assert_eq!(runner.ran, commands[..2]);
    }

    #[test]
    fn claude_plugin_update_failure_stops_before_cleanup() {
        let target = ConfigTarget {
            path: PathBuf::from("/tmp/config.toml"),
            source: ConfigTargetSource::HomeDefault,
        };
        let plan = McpPlan::for_target(SetupMcpTarget::ClaudeCode, &target);
        let commands = plan.commands();
        let mut runner = FakeRunner::default()
            .with_existing("claude")
            .with_response(commands[0].clone(), true, "")
            .with_response(commands[1].clone(), true, "")
            .with_response(commands[2].clone(), true, "")
            .with_response(commands[3].clone(), false, "plugin update failed");

        let report = execute_mcp_plan(plan, &mut runner).expect("execute Claude plan");

        assert_eq!(report.status, SetupStatus::Error);
        assert!(report
            .error
            .as_deref()
            .expect("error")
            .contains(&commands[3].display()));
        assert_eq!(runner.ran, commands[..4]);
    }

    #[test]
    fn plugin_mcp_configs_are_client_specific() {
        let repo_root = repo_root();

        let shared_mcp = shared_plugin_mcp_config();
        let shared_server = shared_mcp["mcpServers"]["moraine"]
            .as_object()
            .expect("shared moraine server object");
        assert_eq!(
            shared_server.get("command"),
            Some(&serde_json::json!("/bin/sh"))
        );
        assert!(shared_server.get("cwd").is_none());
        let shared_args = shared_server["args"].as_array().expect("shared args array");
        assert_eq!(shared_args.first(), Some(&serde_json::json!("-eu")));
        let shared_launcher = shared_args
            .get(2)
            .and_then(Value::as_str)
            .expect("shared inline launcher script");
        assert!(shared_launcher.contains("moraine plugin launch error: binary_untrusted"));
        assert!(shared_launcher.contains("exec \"$moraine_bin\" run mcp"));
        assert!(!shared_launcher.contains("scripts/launch.sh"));

        let codex_manifest_path = repo_root
            .join("plugins")
            .join("moraine")
            .join(".codex-plugin")
            .join("plugin.json");
        let codex_manifest: Value = serde_json::from_str(
            &fs::read_to_string(&codex_manifest_path).expect("read codex plugin manifest"),
        )
        .expect("codex plugin manifest json");
        assert_eq!(codex_manifest["mcpServers"], "./.mcp.json");

        let claude_manifest_path = repo_root
            .join("plugins")
            .join("moraine")
            .join(".claude-plugin")
            .join("plugin.json");
        let claude_manifest: Value = serde_json::from_str(
            &fs::read_to_string(&claude_manifest_path).expect("read claude plugin manifest"),
        )
        .expect("claude plugin manifest json");
        assert_eq!(claude_manifest["mcpServers"], "./.claude-mcp.json");

        let claude_mcp_path = repo_root
            .join("plugins")
            .join("moraine")
            .join(".claude-mcp.json");
        let claude_mcp: Value = serde_json::from_str(
            &fs::read_to_string(&claude_mcp_path).expect("read claude plugin mcp config"),
        )
        .expect("claude plugin mcp json");
        assert_eq!(
            claude_mcp["mcpServers"]["moraine"]["command"],
            "${CLAUDE_PLUGIN_ROOT}/scripts/launch.sh"
        );
        assert_eq!(
            claude_mcp["mcpServers"]["moraine"]["args"],
            serde_json::json!([])
        );
    }

    #[cfg(unix)]
    #[test]
    fn shared_plugin_launcher_rejects_relative_path_moraine() {
        let dir = temp_path("relative-path-launcher");
        let rel_bin = dir.join("rel-bin");
        fs::create_dir_all(&rel_bin).expect("create relative bin dir");
        let fake_moraine = rel_bin.join("moraine");
        fs::write(&fake_moraine, "#!/bin/sh\nexit 99\n").expect("write fake moraine");
        let mut permissions = fs::metadata(&fake_moraine)
            .expect("fake metadata")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&fake_moraine, permissions).expect("chmod fake moraine");

        let output = Command::new("/bin/sh")
            .arg("-eu")
            .arg("-c")
            .arg(shared_plugin_launcher())
            .current_dir(&dir)
            .env("PATH", "rel-bin")
            .output()
            .expect("run shared plugin launcher");

        assert_eq!(output.status.code(), Some(126));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("binary_untrusted"));
        assert!(stderr.contains("relative PATH entry"));
        let _ = fs::remove_dir_all(dir);
    }

    #[cfg(unix)]
    #[test]
    fn shared_plugin_launcher_execs_trusted_absolute_moraine() {
        let dir = temp_path("trusted-launcher");
        let project = dir.join("project");
        let bin = dir.join("bin");
        fs::create_dir_all(&project).expect("create project dir");
        fs::create_dir_all(&bin).expect("create bin dir");
        let fake_moraine = bin.join("moraine");
        fs::write(
            &fake_moraine,
            "#!/bin/sh\nprintf 'fake-moraine:%s:%s\\n' \"$1\" \"$2\"\n",
        )
        .expect("write fake moraine");
        let mut permissions = fs::metadata(&fake_moraine)
            .expect("fake metadata")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&fake_moraine, permissions).expect("chmod fake moraine");

        let output = Command::new("/bin/sh")
            .arg("-eu")
            .arg("-c")
            .arg(shared_plugin_launcher())
            .current_dir(&project)
            .env("PATH", &bin)
            .output()
            .expect("run shared plugin launcher");

        assert!(output.status.success());
        assert_eq!(
            String::from_utf8_lossy(&output.stdout),
            "fake-moraine:run:mcp\n"
        );
        assert!(output.stderr.is_empty());
        let _ = fs::remove_dir_all(dir);
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
    fn qwen_setup_detects_cli_and_builds_exact_registration_commands() {
        assert!(SetupMcpTarget::QwenCode
            .is_available_for_setup(&FakeRunner::default().with_existing("qwen")));
        assert!(!SetupMcpTarget::QwenCode.is_available_for_setup(&FakeRunner::default()));

        let default_target = ConfigTarget {
            path: PathBuf::from("/tmp/config.toml"),
            source: ConfigTargetSource::HomeDefault,
        };
        let default_plan = McpPlan::for_target(SetupMcpTarget::QwenCode, &default_target);
        let default_commands = default_plan.commands();
        assert_eq!(default_commands.len(), 1);
        assert_eq!(default_commands[0].program, "qwen");
        assert_eq!(
            default_commands[0].args,
            vec![
                "mcp",
                "add",
                "--scope",
                "user",
                "--transport",
                "stdio",
                "moraine",
                "moraine",
                "--",
                "run",
                "mcp",
            ]
        );
        assert!(!default_commands[0].args.iter().any(|arg| arg == "--trust"));

        let custom_target = ConfigTarget {
            path: PathBuf::from("/tmp/Qwen Config/config.toml"),
            source: ConfigTargetSource::Cli,
        };
        let custom_commands =
            McpPlan::for_target(SetupMcpTarget::QwenCode, &custom_target).commands();
        assert_eq!(
            custom_commands[0].args,
            vec![
                "mcp",
                "add",
                "--scope",
                "user",
                "--transport",
                "stdio",
                "moraine",
                "moraine",
                "--",
                "run",
                "mcp",
                "--config",
                "/tmp/Qwen Config/config.toml",
            ]
        );
    }

    #[test]
    fn qwen_dry_run_reports_command_without_execution() {
        let plan = McpPlan::for_target(
            SetupMcpTarget::QwenCode,
            &ConfigTarget {
                path: PathBuf::from("/tmp/config.toml"),
                source: ConfigTargetSource::HomeDefault,
            },
        );
        let report = McpTargetReport::planned(plan);
        assert_eq!(report.status, SetupStatus::Planned);
        assert_eq!(report.action, McpAction::Execute);
        assert_eq!(report.commands.len(), 1);
        assert!(report.commands[0].display().starts_with("qwen mcp add "));
    }

    #[test]
    fn qwen_missing_and_failing_cli_are_never_reported_as_success() {
        let args = SetupArgs {
            yes: true,
            dry_run: false,
            skip_config: true,
            skip_mcp: false,
            repair_config: false,
            mcp_targets: vec![SetupMcpTarget::QwenCode],
        };
        let target = ConfigTarget {
            path: PathBuf::from("/tmp/config.toml"),
            source: ConfigTargetSource::HomeDefault,
        };
        let missing = setup_mcp_target(
            &args,
            &target,
            SetupMcpTarget::QwenCode,
            false,
            true,
            &mut FakeRunner::default(),
        )
        .expect("missing Qwen report");
        assert_eq!(missing.status, SetupStatus::Skipped);

        let plan = McpPlan::for_target(SetupMcpTarget::QwenCode, &target);
        let commands = plan.commands();
        let mut runner = FakeRunner::default().with_existing("qwen").with_response(
            commands[0].clone(),
            false,
            "registration failed",
        );
        let failed = execute_mcp_plan(plan, &mut runner).expect("failed Qwen report");
        assert_eq!(failed.status, SetupStatus::Error);
        assert_eq!(runner.ran, commands);
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
    fn dry_run_without_explicit_targets_plans_all_default_harnesses() {
        let args = SetupArgs {
            yes: false,
            dry_run: true,
            skip_config: true,
            skip_mcp: false,
            repair_config: false,
            mcp_targets: Vec::new(),
        };
        let mut runner = FakeRunner::default();
        let report = run_setup(
            &plain_output(),
            &args,
            ConfigTarget {
                path: PathBuf::from("/tmp/config.toml"),
                source: ConfigTargetSource::HomeDefault,
            },
            false,
            &mut runner,
        )
        .expect("setup report");

        assert_eq!(
            report
                .mcp_targets
                .iter()
                .map(|target| target.target)
                .collect::<Vec<_>>(),
            harnesses::setup_targets()
        );
        assert!(report
            .mcp_targets
            .iter()
            .all(|target| target.status == SetupStatus::Planned));
        assert!(runner.ran.is_empty());
    }

    #[test]
    fn dry_run_previews_ingest_config_updates_without_writing_existing_config() {
        let dir = temp_path("dry-run-ingest-preview");
        fs::create_dir_all(&dir).expect("create dir");
        let path = dir.join("config.toml");
        let original = r#"
[ingest]

[[ingest.sources]]
name = "codex"
harness = "codex"
enabled = false
glob = "~/.codex/sessions/**/*.jsonl"
watch_root = "~/.codex/sessions"
"#;
        fs::write(&path, original).expect("write config");
        let args = SetupArgs {
            yes: false,
            dry_run: true,
            skip_config: false,
            skip_mcp: false,
            repair_config: false,
            mcp_targets: Vec::new(),
        };
        let mut runner = FakeRunner::default();
        let report = run_setup(
            &plain_output(),
            &args,
            ConfigTarget {
                path: path.clone(),
                source: ConfigTargetSource::HomeDefault,
            },
            false,
            &mut runner,
        )
        .expect("setup report");

        assert_eq!(report.config.status, SetupStatus::Planned);
        assert_eq!(report.config.action, "would_migrate");
        assert!(report
            .config
            .message
            .contains("ingest sources would be updated"));
        assert!(report
            .config
            .message
            .contains("backend.start_on_up=true would be materialized"));
        assert_eq!(fs::read_to_string(&path).expect("config content"), original);
        assert_eq!(
            report
                .mcp_targets
                .iter()
                .map(|target| target.target)
                .collect::<Vec<_>>(),
            harnesses::setup_targets()
        );
        assert!(report
            .mcp_targets
            .iter()
            .all(|target| target.status == SetupStatus::Planned));
        assert!(runner.ran.is_empty());
        let _ = fs::remove_dir_all(dir);
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
