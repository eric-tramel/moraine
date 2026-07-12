#![cfg(unix)]

use std::fs;
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const EVENT_TIMEOUT: Duration = Duration::from_secs(15);

fn temp_dir(name: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "moraine-clickhouse-supervision-{name}-{}-{stamp}",
        std::process::id()
    ));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

fn unused_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("reserve port")
        .local_addr()
        .expect("reserved port address")
        .port()
}

fn write_executable(path: &Path, contents: &str) {
    fs::create_dir_all(path.parent().expect("script parent")).expect("create script parent");
    fs::write(path, contents).expect("write script");
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("make script executable");
}

fn write_config(root: &Path, port: u16) -> PathBuf {
    let runtime = root.join("runtime");
    let config = root.join("config path with spaces.toml");
    fs::write(
        &config,
        format!(
            r#"[clickhouse]
url = "http://127.0.0.1:{port}"
database = "moraine"
timeout_seconds = 1.0

[mcp]
central_socket_path = "{}"

[backend]
start_on_up = false

[runtime]
root_dir = "{}"
logs_dir = "{}"
pids_dir = "{}"
service_bin_dir = "{}"
managed_clickhouse_dir = "{}"
clickhouse_start_timeout_seconds = 1.0
healthcheck_interval_ms = 100
clickhouse_auto_install = false
"#,
            root.join("mcp.sock").display(),
            runtime.display(),
            runtime.join("logs").display(),
            runtime.join("run").display(),
            root.join("services").display(),
            root.join("managed").display(),
        ),
    )
    .expect("write config");
    config
}

fn process_alive(pid: u32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("probe process")
        .success()
}

fn wait_for_file(path: &Path, timeout: Duration) -> String {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(contents) = fs::read_to_string(path) {
            if !contents.trim().is_empty() {
                return contents;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {}",
            path.display()
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn wait_for_retained_log(supervisor: &mut Child, path: &Path, needle: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let found = (0..=3).any(|generation| {
            let retained_path = if generation == 0 {
                path.to_path_buf()
            } else {
                let mut name = path.file_name().expect("log file name").to_os_string();
                name.push(format!(".{generation}"));
                path.with_file_name(name)
            };
            fs::read_to_string(retained_path)
                .map(|contents| contents.contains(needle))
                .unwrap_or(false)
        });
        if found {
            return;
        }
        assert!(
            supervisor.try_wait().expect("inspect supervisor").is_none(),
            "supervisor exited before logging `{needle}`"
        );
        assert!(
            Instant::now() < deadline,
            "timed out waiting for `{needle}` in retained segments for {}",
            path.display()
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn wait_for_child(child: &mut Child, timeout: Duration) -> ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().expect("inspect child") {
            return status;
        }
        assert!(Instant::now() < deadline, "timed out waiting for child");
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn failed_initial_readiness_rolls_back_supervisor_child_and_pid() {
    let root = temp_dir("startup-rollback");
    let raw_pid_path = root.join("raw.pid");
    let server = root.join("managed/bin/clickhouse-server");
    write_executable(
        &server,
        &format!(
            "#!/bin/sh\nprintf '%s\\n' \"$$\" > '{}'\nexec sleep 60\n",
            raw_pid_path.display()
        ),
    );
    let config = write_config(&root, unused_port());

    let output = Command::new(env!("CARGO_BIN_EXE_moraine"))
        .arg("--config")
        .arg(&config)
        .args(["up", "--no-ingest"])
        .output()
        .expect("run moraine up");

    assert!(!output.status.success(), "up unexpectedly succeeded");
    let raw_pid = wait_for_file(&raw_pid_path, Duration::from_secs(2))
        .trim()
        .parse::<u32>()
        .expect("raw pid");
    assert!(
        !process_alive(raw_pid),
        "raw child survived startup rollback"
    );
    let run_dir = root.join("runtime/run");
    assert!(!run_dir.join("clickhouse.pid").exists());
    for service in ["ingest.pid", "backend.pid", "monitor.pid", "mcp.pid"] {
        assert!(
            !run_dir.join(service).exists(),
            "{service} should not start"
        );
    }
    let _ = fs::remove_dir_all(root);
}

#[test]
fn sigterm_during_backoff_exits_without_replacement() {
    let root = temp_dir("backoff-shutdown");
    let count_path = root.join("generation-count");
    let server = root.join("managed/bin/clickhouse-server");
    write_executable(
        &server,
        &format!(
            "#!/bin/sh\ncount=0\n[ -f '{}' ] && count=$(cat '{}')\ncount=$((count + 1))\nprintf '%s\\n' \"$count\" > '{}'\nexit 42\n",
            count_path.display(),
            count_path.display(),
            count_path.display()
        ),
    );
    let config = write_config(&root, unused_port());
    let supervisor_log_path = root.join("runtime/logs/clickhouse-supervisor.log");
    let mut supervisor = Command::new(env!("CARGO_BIN_EXE_moraine"))
        .arg("--config")
        .arg(&config)
        .args(["clickhouse", "supervise"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start supervisor");

    wait_for_retained_log(
        &mut supervisor,
        &supervisor_log_path,
        "state=backoff",
        EVENT_TIMEOUT,
    );
    let status = Command::new("kill")
        .arg(supervisor.id().to_string())
        .status()
        .expect("signal supervisor");
    assert!(status.success());
    assert!(wait_for_child(&mut supervisor, EVENT_TIMEOUT).success());
    assert_eq!(
        fs::read_to_string(&count_path)
            .expect("generation count")
            .trim(),
        "1"
    );
    let _ = fs::remove_dir_all(root);
}
