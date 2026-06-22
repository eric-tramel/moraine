use anyhow::{bail, Context, Result};
use dialoguer::{
    console::{style, Style},
    theme::Theme,
    MultiSelect,
};
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

const DEFAULT_CONFIG_TEMPLATE: &str = include_str!("../../../../config/moraine.toml");

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

    let config = if args.skip_config {
        ConfigReport::skipped(&target.path, "skipped by --skip-config")
    } else {
        setup_config(args, &target, interactive)?
    };

    let mut mcp_targets = Vec::new();
    if !args.skip_mcp {
        let config_allows_mcp =
            args.skip_config || args.dry_run || config.status == SetupStatus::Ok;
        if config_allows_mcp {
            let selected = selected_mcp_targets(args, interactive, runner)?;
            let targets_confirmed_by_selection =
                args.mcp_targets.is_empty() && interactive && !args.yes && !args.dry_run;
            let mut progress = SetupProgress::from_output(output);
            for mcp_target in selected {
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

fn selected_mcp_targets(
    args: &SetupArgs,
    interactive: bool,
    runner: &dyn CommandRunner,
) -> Result<Vec<SetupMcpTarget>> {
    if !args.mcp_targets.is_empty() {
        return Ok(dedup_targets(&args.mcp_targets));
    }

    if args.yes || args.dry_run || !interactive {
        return Ok(Vec::new());
    }

    let available = setup_targets()
        .into_iter()
        .filter(|target| target.is_available_for_setup(runner))
        .collect::<Vec<_>>();
    prompt_mcp_target_checklist(&available)
}

fn dedup_targets(targets: &[SetupMcpTarget]) -> Vec<SetupMcpTarget> {
    let mut seen = BTreeSet::new();
    targets
        .iter()
        .copied()
        .filter(|target| seen.insert(*target))
        .collect()
}

fn setup_targets() -> [SetupMcpTarget; 7] {
    [
        SetupMcpTarget::ClaudeCode,
        SetupMcpTarget::Codex,
        SetupMcpTarget::Hermes,
        SetupMcpTarget::KimiCli,
        SetupMcpTarget::OpenCode,
        SetupMcpTarget::Cursor,
        SetupMcpTarget::PiCodingAgent,
    ]
}

fn prompt_mcp_target_checklist(available: &[SetupMcpTarget]) -> Result<Vec<SetupMcpTarget>> {
    if available.is_empty() {
        eprintln!("No supported agent harnesses were detected.");
        return Ok(Vec::new());
    }

    let items = available
        .iter()
        .map(|target| format!("{:<14} {}", target.label(), target.setup_kind()))
        .collect::<Vec<_>>();
    let defaults = vec![false; items.len()];
    let theme = SetupPromptTheme;
    let selected = MultiSelect::with_theme(&theme)
        .with_prompt(
            "Agent integrations - selected harnesses get host-wide Moraine session history access (Space toggles, Enter installs selected, Esc skips)",
        )
        .items(&items)
        .defaults(&defaults)
        .max_length(items.len())
        .interact_opt()
        .context("failed to read setup target selection")?
        .unwrap_or_default();

    Ok(selected_mcp_targets_from_indices(available, &selected))
}

fn selected_mcp_targets_from_indices(
    available: &[SetupMcpTarget],
    indices: &[usize],
) -> Vec<SetupMcpTarget> {
    indices
        .iter()
        .filter_map(|idx| available.get(*idx).copied())
        .collect()
}

struct SetupPromptTheme;

impl Theme for SetupPromptTheme {
    fn format_prompt(&self, f: &mut dyn fmt::Write, prompt: &str) -> fmt::Result {
        write!(
            f,
            "{} {} {}",
            style("?").for_stderr().yellow(),
            Style::new().for_stderr().bold().apply_to(prompt),
            style("›").for_stderr().bright().black()
        )
    }

    fn format_error(&self, f: &mut dyn fmt::Write, err: &str) -> fmt::Result {
        write!(
            f,
            "{} {}",
            style("✘").for_stderr().red(),
            Style::new().for_stderr().red().apply_to(err)
        )
    }

    fn format_multi_select_prompt_selection(
        &self,
        f: &mut dyn fmt::Write,
        prompt: &str,
        selections: &[&str],
    ) -> fmt::Result {
        write!(
            f,
            "{} {} {} ",
            style("✔").for_stderr().green(),
            Style::new().for_stderr().bold().apply_to(prompt),
            style("·").for_stderr().bright().black()
        )?;
        if selections.is_empty() {
            return write!(
                f,
                "{}",
                Style::new().for_stderr().bright().black().apply_to("none")
            );
        }
        for (idx, selection) in selections.iter().enumerate() {
            if idx > 0 {
                write!(f, ", ")?;
            }
            write!(
                f,
                "{}",
                Style::new().for_stderr().green().apply_to(selection)
            )?;
        }
        Ok(())
    }

    fn format_multi_select_prompt_item(
        &self,
        f: &mut dyn fmt::Write,
        text: &str,
        checked: bool,
        active: bool,
    ) -> fmt::Result {
        let arrow = if active {
            style("❯").for_stderr().green().to_string()
        } else {
            " ".to_string()
        };
        let marker = if checked {
            style("[x]").for_stderr().green().to_string()
        } else {
            style("[ ]").for_stderr().magenta().to_string()
        };
        if active {
            write!(
                f,
                "{} {} {}",
                arrow,
                marker,
                Style::new().for_stderr().cyan().bold().apply_to(text)
            )
        } else {
            write!(f, "{} {} {}", arrow, marker, text)
        }
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
                write.kind.label(),
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
                Self::manual(
                    target,
                    format!(
                        "Claude Code plugin installs use the default Moraine config. For this custom config target, use manual MCP registration:\n{}",
                        command.display()
                    ),
                )
            }
            SetupMcpTarget::ClaudeCode => Self {
                target,
                action: McpAction::Execute,
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
                        CommandSpec::new(
                            "claude",
                            ["mcp", "remove", "moraine", "--scope", "user"],
                        ),
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
                Self::manual(
                    target,
                    format!(
                        "Codex plugin installs use the default Moraine config. For this custom config target, use manual MCP registration:\n{}",
                        command.display()
                    ),
                )
            }
            SetupMcpTarget::Codex => Self {
                target,
                action: McpAction::Execute,
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
            SetupMcpTarget::Hermes => Self::replace_registration(
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
            SetupMcpTarget::KimiCli => Self::replace_registration(
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
            SetupMcpTarget::OpenCode => Self::write_config(
                target,
                home
                    .as_ref()
                    .map(|home| McpConfigWrite::opencode(home, config_target)),
                opencode_snippet(config_target),
            ),
            SetupMcpTarget::Cursor => Self::write_config(
                target,
                home.as_ref()
                    .map(|home| McpConfigWrite::cursor(home, config_target)),
                cursor_snippet(config_target),
            ),
            SetupMcpTarget::PiCodingAgent => {
                let mut plan = Self::write_config(
                    target,
                    home.as_ref()
                        .map(|home| McpConfigWrite::pi(home, config_target)),
                    pi_snippet(config_target),
                );
                if !plan.config_writes.is_empty() {
                    plan.steps.push(McpPlanStep::required(CommandSpec::new(
                        "pi",
                        ["install", "npm:pi-mcp-extension"],
                    ))
                    .with_progress(
                        "Installing Pi MCP extension",
                        "Pi MCP extension installed",
                        "Pi MCP extension install warning",
                        "Pi MCP extension install failed",
                    ));
                }
                plan
            }
        }
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

fn mcp_run_args(config_target: &ConfigTarget) -> Vec<String> {
    let mut args = vec!["run".to_string(), "mcp".to_string()];
    if config_target.requires_explicit_mcp_config() {
        args.push("--config".to_string());
        args.push(config_target.path.display().to_string());
    }
    args
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
    let snippet = serde_json::json!({
        "mcpServers": {
            "moraine": {
                "type": "stdio",
                "command": "moraine",
                "args": mcp_run_args(config_target),
            }
        }
    });
    format!(
        "Add this server to ~/.cursor/mcp.json for global Cursor use or .cursor/mcp.json for a project:\n{}",
        serde_json::to_string_pretty(&snippet).unwrap_or_else(|_| snippet.to_string())
    )
}

fn opencode_snippet(config_target: &ConfigTarget) -> String {
    let snippet = serde_json::json!({
        "$schema": "https://opencode.ai/config.json",
        "mcp": {
            "moraine": {
                "type": "local",
                "command": opencode_command(config_target),
                "enabled": true,
            }
        }
    });
    format!(
        "Add this server to ~/.config/opencode/opencode.json:\n{}",
        serde_json::to_string_pretty(&snippet).unwrap_or_else(|_| snippet.to_string())
    )
}

fn pi_snippet(config_target: &ConfigTarget) -> String {
    let snippet = serde_json::json!({
        "mcpServers": {
            "moraine": {
                "transport": "stdio",
                "command": "moraine",
                "args": mcp_run_args(config_target),
                "lifecycle": "eager",
            }
        }
    });
    format!(
        "Install the Pi MCP extension first:\npi install npm:pi-mcp-extension\n\nThen add this server to ~/.pi/agent/mcp.json:\n{}",
        serde_json::to_string_pretty(&snippet).unwrap_or_else(|_| snippet.to_string())
    )
}

fn opencode_command(config_target: &ConfigTarget) -> Vec<String> {
    std::iter::once("moraine".to_string())
        .chain(mcp_run_args(config_target))
        .collect()
}

#[derive(Debug, Clone)]
struct McpConfigWrite {
    path: PathBuf,
    kind: McpConfigKind,
    command: Vec<String>,
}

impl McpConfigWrite {
    fn cursor(home: &Path, config_target: &ConfigTarget) -> Self {
        Self {
            path: home.join(".cursor").join("mcp.json"),
            kind: McpConfigKind::Cursor,
            command: mcp_run_args(config_target),
        }
    }

    fn pi(home: &Path, config_target: &ConfigTarget) -> Self {
        Self {
            path: home.join(".pi").join("agent").join("mcp.json"),
            kind: McpConfigKind::Pi,
            command: mcp_run_args(config_target),
        }
    }

    fn opencode(home: &Path, config_target: &ConfigTarget) -> Self {
        Self {
            path: home.join(".config").join("opencode").join("opencode.json"),
            kind: McpConfigKind::OpenCode,
            command: opencode_command(config_target),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn merge_into(&self, root: &mut Map<String, Value>) -> Result<()> {
        match self.kind {
            McpConfigKind::Cursor => {
                let servers = object_entry_mut(root, "mcpServers")?;
                servers.insert(
                    "moraine".to_string(),
                    serde_json::json!({
                        "type": "stdio",
                        "command": "moraine",
                        "args": self.command.clone(),
                    }),
                );
            }
            McpConfigKind::Pi => {
                let servers = object_entry_mut(root, "mcpServers")?;
                servers.insert(
                    "moraine".to_string(),
                    serde_json::json!({
                        "transport": "stdio",
                        "command": "moraine",
                        "args": self.command.clone(),
                        "lifecycle": "eager",
                    }),
                );
            }
            McpConfigKind::OpenCode => {
                root.entry("$schema".to_string())
                    .or_insert_with(|| serde_json::json!("https://opencode.ai/config.json"));
                let servers = object_entry_mut(root, "mcp")?;
                servers.insert(
                    "moraine".to_string(),
                    serde_json::json!({
                        "type": "local",
                        "command": self.command.clone(),
                        "enabled": true,
                    }),
                );
            }
        }
        Ok(())
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
    let mut root = read_json_object_or_default(write.path())?;
    write.merge_into(&mut root)?;
    write_json_atomic(write.path(), &Value::Object(root))?;
    Ok(McpConfigFileReport::written(write))
}

fn read_json_object_or_default(path: &Path) -> Result<Map<String, Value>> {
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

    let value: Value = serde_json::from_str(&content)
        .with_context(|| format!("{} is not valid JSON", path.display()))?;
    match value {
        Value::Object(object) => Ok(object),
        _ => bail!("{} must contain a JSON object", path.display()),
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
        match self {
            SetupMcpTarget::ClaudeCode => "Claude Code",
            SetupMcpTarget::Codex => "Codex",
            SetupMcpTarget::Hermes => "Hermes",
            SetupMcpTarget::KimiCli => "Kimi CLI",
            SetupMcpTarget::OpenCode => "OpenCode",
            SetupMcpTarget::Cursor => "Cursor",
            SetupMcpTarget::PiCodingAgent => "Pi Coding Agent",
        }
    }

    fn setup_kind(self) -> &'static str {
        match self {
            SetupMcpTarget::ClaudeCode | SetupMcpTarget::Codex => "plugin",
            SetupMcpTarget::Hermes | SetupMcpTarget::KimiCli => "MCP",
            SetupMcpTarget::OpenCode | SetupMcpTarget::Cursor => "MCP config",
            SetupMcpTarget::PiCodingAgent => "MCP extension",
        }
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
        match self {
            SetupMcpTarget::ClaudeCode => &["claude"],
            SetupMcpTarget::Codex => &["codex"],
            SetupMcpTarget::Hermes => &["hermes"],
            SetupMcpTarget::KimiCli => &["kimi"],
            SetupMcpTarget::OpenCode => &["opencode"],
            SetupMcpTarget::Cursor => &["cursor", "cursor-agent"],
            SetupMcpTarget::PiCodingAgent => &["pi"],
        }
    }

    fn default_probe_paths(self) -> Vec<PathBuf> {
        let Some(home) = env::var_os("HOME").map(PathBuf::from) else {
            return Vec::new();
        };
        match self {
            SetupMcpTarget::OpenCode => vec![
                home.join(".config").join("opencode"),
                home.join(".local").join("share").join("opencode"),
            ],
            SetupMcpTarget::Cursor => vec![
                home.join(".cursor"),
                home.join("Library")
                    .join("Application Support")
                    .join("Cursor"),
                home.join(".config").join("Cursor"),
            ],
            SetupMcpTarget::PiCodingAgent => vec![home.join(".pi").join("agent")],
            _ => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render::OutputMode;
    use std::collections::{BTreeMap, BTreeSet};

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
    fn maps_mcp_target_checklist_indices() {
        let available = [
            SetupMcpTarget::ClaudeCode,
            SetupMcpTarget::Codex,
            SetupMcpTarget::Hermes,
        ];

        assert_eq!(
            selected_mcp_targets_from_indices(&available, &[1, 2, 9]),
            vec![SetupMcpTarget::Codex, SetupMcpTarget::Hermes]
        );
        assert_eq!(
            selected_mcp_targets_from_indices(&available, &[]),
            Vec::<SetupMcpTarget>::new()
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
    fn hermes_add_accepts_tool_enable_prompt() {
        let target = ConfigTarget {
            path: PathBuf::from("/tmp/config.toml"),
            source: ConfigTargetSource::HomeDefault,
        };
        let plan = McpPlan::for_target(SetupMcpTarget::Hermes, &target);
        let commands = plan.commands();
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].args, vec!["mcp", "remove", "moraine"]);
        assert_eq!(
            commands[1].args,
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
        assert_eq!(commands[1].stdin.as_deref(), Some("\n"));
    }

    #[test]
    fn hermes_add_cancelled_stdout_is_error_even_with_zero_status() {
        let target = ConfigTarget {
            path: PathBuf::from("/tmp/config.toml"),
            source: ConfigTargetSource::HomeDefault,
        };
        let plan = McpPlan::for_target(SetupMcpTarget::Hermes, &target);
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
