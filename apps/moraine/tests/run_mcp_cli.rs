#![cfg(unix)]

//! `moraine run mcp` is a per-session stdio proxy. It must not claim the
//! leftover singleton `mcp.pid` that older launches wrote.

use std::fs;
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "moraine-run-mcp-cli-{name}-{}-{stamp}",
        std::process::id()
    ));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

fn write_executable(path: &Path, contents: &str) {
    fs::create_dir_all(path.parent().expect("script parent")).expect("create script parent");
    fs::write(path, contents).expect("write script");
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("make script executable");
}

struct Harness {
    root: PathBuf,
    config: PathBuf,
    holder: Option<Child>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(mut holder) = self.holder.take() {
            let _ = holder.kill();
            let _ = holder.wait();
        }
        let _ = fs::remove_dir_all(&self.root);
    }
}

impl Harness {
    fn new(name: &str, mcp_script: &str, ingest_script: &str) -> Self {
        let root = temp_dir(name);
        let runtime = root.join("runtime");
        let pids = runtime.join("run");
        fs::create_dir_all(&pids).expect("create pids dir");
        write_executable(&root.join("services/moraine-mcp"), mcp_script);
        write_executable(&root.join("services/moraine-ingest"), ingest_script);
        let config = root.join("config.toml");
        fs::write(
            &config,
            format!(
                r#"[clickhouse]
url = "http://127.0.0.1:1"
database = "moraine"
timeout_seconds = 1.0

[mcp]
central_socket_path = "{socket}"

[backend]
start_on_up = false

[runtime]
root_dir = "{runtime}"
logs_dir = "{logs}"
pids_dir = "{pids}"
service_bin_dir = "{services}"
managed_clickhouse_dir = "{managed}"
clickhouse_auto_install = false
"#,
                socket = root.join("mcp.sock").display(),
                runtime = runtime.display(),
                logs = runtime.join("logs").display(),
                pids = pids.display(),
                services = root.join("services").display(),
                managed = root.join("managed").display(),
            ),
        )
        .expect("write config");
        Self {
            root,
            config,
            holder: None,
        }
    }

    fn pid_file(&self, name: &str) -> PathBuf {
        self.root.join("runtime/run").join(name)
    }

    fn hold_live_pid(&mut self, name: &str) {
        let holder = Command::new("sleep")
            .arg("30")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start pid holder");
        fs::write(self.pid_file(name), format!("{}\n", holder.id())).expect("write live pid");
        self.holder = Some(holder);
    }

    fn command(&self, service: &str) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_moraine"));
        command
            .arg("--config")
            .arg(&self.config)
            .args(["run", service])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command
    }

    fn spawn(&self, service: &str) -> Child {
        self.command(service).spawn().expect("spawn moraine")
    }
}

fn wait_child(mut child: Child, timeout: Duration) -> Output {
    let started = Instant::now();
    loop {
        match child.try_wait().expect("try wait") {
            Some(status) => return collect_output(child, status),
            None if started.elapsed() >= timeout => {
                let _ = child.kill();
                let status = child.wait().expect("reap killed child");
                let output = collect_output(child, status);
                panic!(
                    "timed out waiting for moraine run\nstdout:\n{}\nstderr:\n{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            None => thread::sleep(Duration::from_millis(20)),
        }
    }
}

fn collect_output(mut child: Child, status: ExitStatus) -> Output {
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    if let Some(mut pipe) = child.stdout.take() {
        let _ = pipe.read_to_end(&mut stdout);
    }
    if let Some(mut pipe) = child.stderr.take() {
        let _ = pipe.read_to_end(&mut stderr);
    }
    Output {
        status,
        stdout,
        stderr,
    }
}

fn assert_success(output: &Output, what: &str) {
    assert!(
        output.status.success(),
        "{what} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn overlapping_sleep_script() -> &'static str {
    "#!/bin/sh\nsleep 0.4\nexit 0\n"
}

fn idle_script() -> &'static str {
    "#!/bin/sh\nexit 0\n"
}

#[test]
fn concurrent_run_mcp_succeeds_while_legacy_mcp_pid_is_live() {
    let mut harness = Harness::new("legacy-live", overlapping_sleep_script(), idle_script());
    harness.hold_live_pid("mcp.pid");
    let original = fs::read_to_string(harness.pid_file("mcp.pid")).expect("read leftover pid");

    let first_child = harness.spawn("mcp");
    let second_child = harness.spawn("mcp");
    let first = wait_child(first_child, Duration::from_secs(5));
    let second = wait_child(second_child, Duration::from_secs(5));

    assert_success(&first, "first moraine run mcp");
    assert_success(&second, "second moraine run mcp");
    assert_eq!(
        fs::read_to_string(harness.pid_file("mcp.pid")).expect("reread leftover pid"),
        original,
        "stdio proxies must not replace a live leftover mcp.pid"
    );
}

#[test]
fn concurrent_run_mcp_does_not_create_mcp_pid() {
    let harness = Harness::new("no-pid", overlapping_sleep_script(), idle_script());
    let first_child = harness.spawn("mcp");
    let second_child = harness.spawn("mcp");
    let first = wait_child(first_child, Duration::from_secs(5));
    let second = wait_child(second_child, Duration::from_secs(5));

    assert_success(&first, "first moraine run mcp");
    assert_success(&second, "second moraine run mcp");
    assert!(
        !harness.pid_file("mcp.pid").exists(),
        "per-session MCP must not claim mcp.pid"
    );
}

#[test]
fn overlapping_run_ingest_still_refuses_a_second_launcher() {
    let harness = Harness::new(
        "ingest-exclusive",
        idle_script(),
        overlapping_sleep_script(),
    );
    let first_child = harness.spawn("ingest");
    let pid_file = harness.pid_file("ingest.pid");
    let started = Instant::now();
    while !pid_file.exists() {
        if started.elapsed() > Duration::from_secs(2) {
            panic!("first ingest never claimed ingest.pid");
        }
        thread::sleep(Duration::from_millis(10));
    }

    let second = wait_child(harness.spawn("ingest"), Duration::from_secs(5));
    assert!(
        !second.status.success(),
        "second ingest launcher must fail while the first holds ingest.pid"
    );
    let stderr = String::from_utf8_lossy(&second.stderr);
    assert!(
        stderr.contains("another launcher owns this service"),
        "unexpected ingest refusal: {stderr}"
    );

    let first = wait_child(first_child, Duration::from_secs(5));
    assert_success(&first, "first ingest");
}

#[test]
fn run_mcp_forwards_child_exit_status() {
    let harness = Harness::new("exit-status", "#!/bin/sh\nexit 7\n", idle_script());
    let output = wait_child(harness.spawn("mcp"), Duration::from_secs(5));
    assert_eq!(output.status.code(), Some(7));
}
