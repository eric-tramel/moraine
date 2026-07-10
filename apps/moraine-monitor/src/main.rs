use std::env;
use std::ffi::{OsStr, OsString};
use std::io;
use std::path::PathBuf;
use std::process::{self, Command, ExitStatus};

const DEPRECATION_WARNING: &str =
    "warning: moraine-monitor is deprecated; delegating to `moraine-mcp --serve socket`";

fn backend_binary_name() -> OsString {
    let mut name = OsString::from("moraine-mcp");
    name.push(env::consts::EXE_SUFFIX);
    name
}

fn sibling_backend(binary_name: &OsStr) -> Option<PathBuf> {
    let sibling = env::current_exe().ok()?.parent()?.join(binary_name);
    sibling.is_file().then_some(sibling)
}

fn forwarded_args(args: impl IntoIterator<Item = OsString>) -> Result<Vec<OsString>, String> {
    let args: Vec<_> = args.into_iter().collect();
    let mut index = 0;
    let mut has_socket_mode = false;

    while index < args.len() {
        if args[index] == OsStr::new("--serve") {
            let mode = args
                .get(index + 1)
                .ok_or_else(|| "`--serve` requires the mode `socket`".to_string())?;
            if mode != OsStr::new("socket") {
                return Err(format!(
                    "moraine-monitor can only delegate with `--serve socket`, not `--serve {}`",
                    mode.to_string_lossy()
                ));
            }
            has_socket_mode = true;
            index += 2;
        } else {
            index += 1;
        }
    }

    if has_socket_mode {
        return Ok(args);
    }

    let mut forwarded = Vec::with_capacity(args.len() + 2);
    forwarded.push(OsString::from("--serve"));
    forwarded.push(OsString::from("socket"));
    forwarded.extend(args);
    Ok(forwarded)
}

fn backend_command(program: &OsStr, args: &[OsString]) -> Command {
    let mut command = Command::new(program);
    command.args(args);
    command
}

#[cfg(unix)]
fn run_backend(program: &OsStr, args: &[OsString]) -> io::Result<ExitStatus> {
    use std::os::unix::process::CommandExt;

    let mut command = backend_command(program, args);
    Err(command.exec())
}

#[cfg(not(unix))]
fn run_backend(program: &OsStr, args: &[OsString]) -> io::Result<ExitStatus> {
    backend_command(program, args).status()
}

fn delegate(args: &[OsString]) -> Result<ExitStatus, String> {
    let binary_name = backend_binary_name();
    let mut sibling_failure = None;

    if let Some(sibling) = sibling_backend(&binary_name) {
        match run_backend(sibling.as_os_str(), args) {
            Ok(status) => return Ok(status),
            Err(error) => sibling_failure = Some((sibling, error)),
        }
    }

    match run_backend(binary_name.as_os_str(), args) {
        Ok(status) => Ok(status),
        Err(path_error) => {
            if let Some((sibling, sibling_error)) = sibling_failure {
                Err(format!(
                    "failed to execute sibling `{}` ({sibling_error}); PATH fallback `{}` also failed ({path_error})",
                    sibling.display(),
                    binary_name.to_string_lossy()
                ))
            } else {
                Err(format!(
                    "failed to execute `{}` from PATH ({path_error})",
                    binary_name.to_string_lossy()
                ))
            }
        }
    }
}

fn exit_with_status(status: ExitStatus) -> ! {
    if let Some(code) = status.code() {
        process::exit(code);
    }

    eprintln!("error: unified backend terminated without an exit code");
    process::exit(1);
}

fn main() {
    eprintln!("{DEPRECATION_WARNING}");

    let args = forwarded_args(env::args_os().skip(1)).unwrap_or_else(|error| {
        eprintln!("error: {error}");
        process::exit(2);
    });
    let status = delegate(&args).unwrap_or_else(|error| {
        eprintln!("error: moraine-monitor could not start the unified backend: {error}");
        process::exit(1);
    });

    exit_with_status(status);
}

#[cfg(test)]
mod tests {
    use super::forwarded_args;
    use std::ffi::OsString;

    fn args(values: &[&str]) -> Vec<OsString> {
        values.iter().map(|value| OsString::from(*value)).collect()
    }

    #[test]
    fn forwards_legacy_arguments_with_socket_mode() {
        let forwarded = forwarded_args(args(&[
            "--host",
            "127.0.0.1",
            "--port",
            "7749",
            "--config",
            "/tmp/moraine.toml",
            "--static-dir",
            "/tmp/monitor",
        ]))
        .expect("legacy arguments should be forwarded");

        assert_eq!(
            forwarded,
            args(&[
                "--serve",
                "socket",
                "--host",
                "127.0.0.1",
                "--port",
                "7749",
                "--config",
                "/tmp/moraine.toml",
                "--static-dir",
                "/tmp/monitor",
            ])
        );
    }

    #[test]
    fn preserves_help_for_the_unified_backend() {
        let forwarded = forwarded_args(args(&["--help"])).expect("help should be forwarded");
        assert_eq!(forwarded, args(&["--serve", "socket", "--help"]));
    }

    #[test]
    fn does_not_duplicate_explicit_socket_mode() {
        let original = args(&["--config", "/tmp/moraine.toml", "--serve", "socket"]);
        let forwarded = forwarded_args(original.clone()).expect("socket mode should be accepted");
        assert_eq!(forwarded, original);
    }

    #[test]
    fn rejects_non_backend_serve_modes() {
        let error = forwarded_args(args(&["--serve", "stdio"]))
            .expect_err("the deprecated alias must never start a stdio owner");
        assert!(error.contains("can only delegate with `--serve socket`"));
    }
}
