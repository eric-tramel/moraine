use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use crate::cli::RunArgs;
use crate::managed_clickhouse::run_foreground_clickhouse;
use crate::paths::{load_cfg, runtime_paths};
use crate::process::{
    lock_storage_writer_launch, pid_path, remove_pid_if_matches, require_service_binary,
    service_args_with_defaults, write_pid_exclusive, StorageGateGuard,
};
use crate::service::Service;

pub(super) async fn handle(global_config: Option<PathBuf>, run: RunArgs) -> Result<ExitCode> {
    let (inline_config, passthrough) = parse_config_flag(&run.args)?;
    let raw_config = inline_config.or(global_config);
    let (config_path, cfg) = load_cfg(raw_config)?;
    let paths = runtime_paths(&cfg);
    if run.service == Service::ClickHouse {
        return run_foreground_clickhouse(&cfg, &paths).await;
    }

    let writer_gate = lock_storage_writer_launch(&paths, run.service)?;
    let binary = require_service_binary(run.service, &paths)?;
    let args = service_args_with_defaults(
        run.service,
        config_path.as_path(),
        &cfg,
        &paths,
        &passthrough,
    );

    let mut command = Command::new(binary);
    command.args(args);
    if let Some((key, value)) = config_path.child_origin_environment() {
        command.env(key, value);
    }
    run_registered_foreground_child(
        command,
        &pid_path(&paths, run.service),
        run.service.name(),
        writer_gate,
    )
}

fn run_registered_foreground_child(
    mut command: Command,
    pid_file: &Path,
    service_name: &str,
    writer_gate: Option<StorageGateGuard>,
) -> Result<ExitCode> {
    if let Some(parent) = pid_file.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create PID directory {}", parent.display()))?;
    }
    let mut child = command
        .spawn()
        .map_err(anyhow::Error::from)
        .with_context(|| format!("failed to run {service_name}"))?;
    let child_pid = child.id();
    if let Err(error) = write_pid_exclusive(pid_file, child_pid) {
        let _ = child.kill();
        let _ = child.wait();
        return Err(error);
    }
    drop(writer_gate);
    let result = child
        .wait()
        .map_err(anyhow::Error::from)
        .with_context(|| format!("failed to wait for {service_name}"));
    remove_pid_if_matches(pid_file, child_pid);
    let status = result?;
    Ok(ExitCode::from(status.code().unwrap_or(1) as u8))
}

fn parse_config_flag(args: &[String]) -> Result<(Option<PathBuf>, Vec<String>)> {
    let mut raw_config = None;
    let mut rest = Vec::new();

    let mut i = 0usize;
    while i < args.len() {
        if args[i] == "--config" {
            if i + 1 >= args.len() {
                bail!("--config requires a path");
            }
            raw_config = Some(PathBuf::from(args[i + 1].clone()));
            i += 2;
            continue;
        }

        if let Some(path) = args[i].strip_prefix("--config=") {
            if path.is_empty() {
                bail!("--config requires a path");
            }
            raw_config = Some(PathBuf::from(path));
            i += 1;
            continue;
        }

        rest.push(args[i].clone());
        i += 1;
    }

    Ok((raw_config, rest))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_config_flag_preserves_inline_config_and_rest() {
        let args = vec![
            "--config".to_string(),
            "/tmp/moraine.toml".to_string(),
            "--stdio".to_string(),
        ];
        let (config, rest) = parse_config_flag(&args).expect("parse config");
        assert_eq!(config, Some(PathBuf::from("/tmp/moraine.toml")));
        assert_eq!(rest, vec!["--stdio".to_string()]);
    }

    #[test]
    fn parse_config_flag_supports_equals_form_and_argument_order() {
        let args = vec![
            "--config=/tmp/first.toml".to_string(),
            "--config".to_string(),
            "/tmp/second.toml".to_string(),
            "--host=127.0.0.1".to_string(),
        ];
        let (config, rest) = parse_config_flag(&args).expect("parse config");
        assert_eq!(config, Some(PathBuf::from("/tmp/second.toml")));
        assert_eq!(rest, vec!["--host=127.0.0.1".to_string()]);

        let err = parse_config_flag(&["--config=".to_string()]).expect_err("empty config");
        assert!(err.to_string().contains("--config requires a path"));
    }

    #[test]
    fn parse_config_flag_rejects_dangling_config() {
        let err = parse_config_flag(&["--config".to_string()]).expect_err("dangling config");
        assert!(err.to_string().contains("--config requires a path"));
    }

    #[cfg(unix)]
    #[test]
    fn foreground_child_is_registered_for_storage_cutover_barriers() {
        let root =
            std::env::temp_dir().join(format!("moraine-foreground-pid-{}", std::process::id()));
        let pid_file = root.join("run/ingest.pid");
        let mut command = Command::new("/bin/sh");
        command
            .env("PID_FILE", &pid_file)
            .args(["-c", "sleep 0.1; test \"$(cat \"$PID_FILE\")\" = \"$$\""]);

        let code = run_registered_foreground_child(command, &pid_file, "ingest", None)
            .expect("run registered foreground child");

        assert_eq!(code, ExitCode::SUCCESS);
        assert!(!pid_file.exists());
        let _ = std::fs::remove_dir_all(root);
    }
}
