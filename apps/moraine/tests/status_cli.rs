use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

fn temp_dir() -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "moraine-status-cli-read-error-{}-{stamp}",
        std::process::id()
    ));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

fn write_config(root: &Path) -> PathBuf {
    let runtime = root.join("runtime");
    let config = root.join("config.toml");
    fs::write(
        &config,
        format!(
            r#"[clickhouse]
url = "http://127.0.0.1:9"
database = "moraine"
timeout_seconds = 1.0

[runtime]
root_dir = "{}"
logs_dir = "{}"
pids_dir = "{}"
service_bin_dir = "{}"
managed_clickhouse_dir = "{}"
"#,
            runtime.display(),
            runtime.join("logs").display(),
            runtime.join("run").display(),
            root.join("services").display(),
            root.join("managed").display(),
        ),
    )
    .expect("write status config");
    config
}

fn run_status(config: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_moraine"))
        .args(["--output", "json", "--config"])
        .arg(config)
        .arg("status")
        .output()
        .expect("run moraine status")
}

#[test]
fn heartbeat_read_error_remains_visible_without_failing_status() {
    let root = temp_dir();
    let config = write_config(&root);

    let output = run_status(&config);
    assert!(
        output.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let status: Value = serde_json::from_slice(&output.stdout).expect("status JSON output");

    assert_eq!(status["doctor"]["clickhouse_healthy"], false);
    assert_eq!(status["heartbeat"]["state"], "error");
    assert!(
        status["heartbeat"]["message"]
            .as_str()
            .expect("heartbeat error message")
            .contains("backend error"),
        "{}",
        status["heartbeat"]
    );

    let _ = fs::remove_dir_all(root);
}
