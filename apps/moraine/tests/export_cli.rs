use serde_json::{json, Value};
use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;
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

fn bundled_migration_versions() -> Vec<String> {
    let mut versions =
        fs::read_dir(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../sql"))
            .expect("read migrations")
            .filter_map(|entry| {
                let name = entry.ok()?.file_name().into_string().ok()?;
                let (version, _) = name.split_once('_')?;
                version
                    .chars()
                    .all(|ch| ch.is_ascii_digit())
                    .then(|| version.to_string())
            })
            .collect::<Vec<_>>();
    versions.sort();
    versions
}

fn read_request_target(stream: &mut std::net::TcpStream) -> String {
    let mut request = Vec::new();
    let mut buf = [0u8; 4096];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = stream.read(&mut buf).expect("read request");
        assert!(read > 0, "request ended before headers");
        request.extend_from_slice(&buf[..read]);
    }
    let header_end = request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("header terminator")
        + 4;
    let headers = String::from_utf8_lossy(&request[..header_end]).into_owned();
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().expect("content length"))
        })
        .unwrap_or(0);
    while request.len() < header_end + content_length {
        let read = stream.read(&mut buf).expect("read request body");
        assert!(read > 0, "request ended before body");
        request.extend_from_slice(&buf[..read]);
    }
    headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .expect("request target")
        .to_string()
}

fn query_param<'a>(target: &'a str, name: &str) -> Option<&'a str> {
    target.split_once('?')?.1.split('&').find_map(|pair| {
        pair.split_once('=')
            .filter(|(key, _)| *key == name)
            .map(|(_, value)| value)
    })
}

#[test]
fn export_uses_one_logical_owner_without_elapsed_deadline_params() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ClickHouse fixture");
    let address = listener.local_addr().expect("fixture address");
    let targets = Arc::new(Mutex::new(Vec::new()));
    let captured = targets.clone();
    let versions = bundled_migration_versions();
    let server = thread::spawn(move || {
        for index in 0..4 {
            let (mut stream, _) = listener.accept().expect("accept request");
            captured
                .lock()
                .expect("capture lock")
                .push(read_request_target(&mut stream));
            let body = match index {
                0 => "6".to_string(),
                1 => json!({"data": [{"exists": 1}]}).to_string(),
                2 => json!({
                    "data": versions
                        .iter()
                        .map(|version| json!({"version": version}))
                        .collect::<Vec<_>>()
                })
                .to_string(),
                3 => "{\"event_uid\":\"event-1\",\"event_ts\":1780317296789}\n".to_string(),
                _ => unreachable!(),
            };
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .expect("write response");
        }
    });

    let config = temp_config_path();
    fs::write(
        &config,
        format!(
            r#"
[clickhouse]
url = "http://{address}"
database = "moraine"
timeout_seconds = 1.0
"#
        ),
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
            "--columns",
            "event_uid,event_ts",
        ])
        .output()
        .expect("run export");
    let _ = fs::remove_file(config);
    server.join().expect("join fixture");

    assert!(
        output.status.success(),
        "export failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("event-1"));
    let metadata: Value = String::from_utf8_lossy(&output.stderr)
        .lines()
        .find_map(|line| serde_json::from_str(line).ok())
        .expect("completion metadata");
    let logical_id = metadata["query_id"].as_str().expect("logical query id");
    assert!(logical_id.starts_with("moraine-export-"), "{logical_id}");

    let targets = targets.lock().expect("targets lock");
    assert_eq!(targets.len(), 4);
    for target in targets.iter() {
        assert!(!target.contains("max_execution_time"), "{target}");
        assert!(
            !target.contains("timeout_before_checking_execution_speed"),
            "{target}"
        );
        assert!(!target.contains("timeout_overflow_mode"), "{target}");
        let child_id = query_param(target, "query_id").expect("transport query_id");
        assert!(
            child_id.starts_with(&format!("{logical_id}-")),
            "{child_id}"
        );
    }
}

#[test]
fn broken_pipe_cancels_export_owner_before_root_drain() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ClickHouse fixture");
    let address = listener.local_addr().expect("fixture address");
    let versions = bundled_migration_versions();
    let targets = Arc::new(Mutex::new(Vec::new()));
    let captured = targets.clone();
    let server = thread::spawn(move || {
        let mut export_child_id = String::new();
        for index in 0..8 {
            let (mut stream, _) = listener.accept().expect("accept request");
            let target = read_request_target(&mut stream);
            if index == 3 {
                export_child_id = query_param(&target, "query_id")
                    .expect("export child query_id")
                    .to_string();
            }
            captured.lock().expect("capture lock").push(target);
            let body = match index {
                0 => "6".to_string(),
                1 => json!({"data": [{"exists": 1}]}).to_string(),
                2 => json!({
                    "data": versions
                        .iter()
                        .map(|version| json!({"version": version}))
                        .collect::<Vec<_>>()
                })
                .to_string(),
                3 => format!("{{\"event_uid\":\"{}\"}}\n", "x".repeat(256 * 1024)),
                5 => format!("{{\"query_id\":\"{export_child_id}\"}}\n"),
                4 | 6 | 7 => String::new(),
                _ => unreachable!(),
            };
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .expect("write response");
        }
    });

    let config = temp_config_path();
    fs::write(
        &config,
        format!(
            r#"
[clickhouse]
url = "http://{address}"
database = "moraine"
timeout_seconds = 1.0
"#
        ),
    )
    .expect("write temp config");
    let mut child = Command::new(env!("CARGO_BIN_EXE_moraine"))
        .arg("--config")
        .arg(&config)
        .args([
            "export",
            "events",
            "--format",
            "jsonl",
            "--all",
            "--columns",
            "event_uid",
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn export");
    drop(child.stdout.take());
    let output = child.wait_with_output().expect("wait for export");
    let _ = fs::remove_file(config);
    server.join().expect("join fixture");

    assert!(
        output.status.success(),
        "broken pipe should be successful: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let targets = targets.lock().expect("targets lock");
    let child_id = query_param(&targets[3], "query_id").expect("export child id");
    let decoded_child_id = child_id;
    assert!(
        targets[4].contains("KILL+QUERY") || targets[4].contains("KILL%20QUERY"),
        "{}",
        targets[4]
    );
    assert!(targets[4].contains(decoded_child_id), "{}", targets[4]);
    assert!(targets[6].contains(decoded_child_id), "{}", targets[6]);
}
