use std::fs;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

const EXPORT_METADATA_SCHEMA_VERSION: &str = "moraine.analytics.export_metadata.v1";

fn temp_config_path() -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "moraine-export-cli-test-{}-{nanos}.toml",
        std::process::id()
    ))
}

#[test]
fn invalid_export_columns_emit_no_completion_metadata() {
    let config = temp_config_path();
    fs::write(
        &config,
        r#"
[clickhouse]
url = "http://127.0.0.1:9"
database = "moraine"
"#,
    )
    .expect("write temp config");

    let output = Command::new(env!("CARGO_BIN_EXE_moraine"))
        .arg("--config")
        .arg(&config)
        .args([
            "export",
            "events",
            "--format",
            "jsonl",
            "--all",
            "--metadata",
            "stderr",
            "--columns",
            "payload_json",
        ])
        .output()
        .expect("run moraine");
    let _ = fs::remove_file(config);

    assert!(!output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stdout.is_empty(), "stdout should be empty: {stdout}");
    assert!(stderr.contains("--include-sensitive"), "{stderr}");
    assert!(!stdout.contains(EXPORT_METADATA_SCHEMA_VERSION), "{stdout}");
    assert!(!stderr.contains(EXPORT_METADATA_SCHEMA_VERSION), "{stderr}");
}
