use std::fs;
#[cfg(unix)]
use std::io::{Read, Write};
#[cfg(unix)]
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(unix)]
use std::thread;
#[cfg(unix)]
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

static NEXT_TEMP_DIR: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    let nonce = NEXT_TEMP_DIR.fetch_add(1, Ordering::Relaxed);
    let path =
        std::env::temp_dir().join(format!("moraine-st-{}-{stamp}-{nonce}", std::process::id()));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

fn write_config(root: &Path, monitor_port: u16) -> PathBuf {
    let runtime = root.join("runtime");
    let config = root.join("config.toml");
    fs::write(
        &config,
        format!(
            r#"[clickhouse]
url = "http://127.0.0.1:9"
database = "moraine"
timeout_seconds = 1.0

[mcp]
central_socket_path = "{}"

[monitor]
port = {monitor_port}

[backend]
bind = "127.0.0.1"
start_on_up = false

[runtime]
root_dir = "{}"
logs_dir = "{}"
pids_dir = "{}"
service_bin_dir = "{}"
managed_clickhouse_dir = "{}"
"#,
            root.join("backend.sock").display(),
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
fn run_plain_status(config: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_moraine"))
        .args(["--output", "plain", "--config"])
        .arg(config)
        .arg("status")
        .output()
        .expect("run plain moraine status")
}

#[cfg(unix)]
fn spawn_http_responses(
    responses: Vec<(&'static str, String)>,
) -> (u16, thread::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind HTTP endpoint");
    let port = listener.local_addr().expect("HTTP endpoint address").port();
    listener
        .set_nonblocking(true)
        .expect("nonblocking HTTP endpoint");
    let worker = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut requests = Vec::new();
        for (status, body) in responses {
            loop {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream
                            .set_nonblocking(false)
                            .expect("set HTTP fixture blocking mode");
                        stream
                            .set_read_timeout(Some(Duration::from_secs(1)))
                            .expect("set HTTP fixture read timeout");
                        let mut request = [0_u8; 2048];
                        let request_len = stream.read(&mut request).expect("read HTTP request");
                        requests
                            .push(String::from_utf8_lossy(&request[..request_len]).into_owned());
                        let response = format!(
                            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        stream
                            .write_all(response.as_bytes())
                            .expect("write HTTP response");
                        break;
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        if Instant::now() >= deadline {
                            return requests;
                        }
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => return requests,
                }
            }
        }
        requests
    });
    (port, worker)
}

#[cfg(unix)]
fn spawn_http_endpoint() -> (u16, thread::JoinHandle<Vec<String>>) {
    spawn_http_responses(vec![
        ("503 Service Unavailable", String::new()),
        ("503 Service Unavailable", String::new()),
    ])
}

#[cfg(unix)]
fn backend_row(status: &Value) -> &Value {
    status["services"]
        .as_array()
        .expect("services array")
        .iter()
        .find(|service| service["service"] == "backend")
        .expect("backend service row")
}

#[test]
fn heartbeat_read_error_remains_visible_without_failing_status() {
    let root = temp_dir();
    let config = write_config(&root, 0);

    let output = run_status(&config);
    assert!(
        output.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let status: Value = serde_json::from_slice(&output.stdout).expect("status JSON output");

    assert_eq!(status["doctor"]["clickhouse_healthy"], false);
    assert_eq!(status["data_source"], "direct_db");
    assert_eq!(status["heartbeat"]["state"], "error");
    assert!(
        status["heartbeat"]["message"]
            .as_str()
            .expect("heartbeat error message")
            .contains("backend error"),
        "{}",
        status["heartbeat"]
    );

    let plain_output = run_plain_status(&config);
    assert!(
        plain_output.status.success(),
        "plain status failed: {}",
        String::from_utf8_lossy(&plain_output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&plain_output.stdout).contains("source: direct DB"),
        "{}",
        String::from_utf8_lossy(&plain_output.stdout)
    );

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn status_prefers_canonical_daemon_api_and_preserves_json_schema() {
    let api_body = serde_json::json!({
        "ok": true,
        "clickhouse": {
            "url": "http://127.0.0.1:8123",
            "database": "api_db",
            "healthy": true,
            "version": "26.1.2.3",
            "ping_ms": 3,
            "error": null,
            "connections": {"total": 1}
        },
        "database": {
            "exists": true,
            "table_count": 1,
            "estimated_total_rows": 7,
            "tables": []
        },
        "ingestor": {
            "present": true,
            "alive": true,
            "latest": {
                "ts": "2026-07-10 12:34:56.789",
                "ts_unix_ms": 1783686896789_i64,
                "host": "fixture",
                "service_version": "0.6.4",
                "queue_depth": 17,
                "files_active": 2,
                "files_watched": 3,
                "rows_raw_written": 8,
                "rows_events_written": 7,
                "rows_errors_written": 1,
                "flush_latency_ms": 4,
                "append_to_visible_p50_ms": 5,
                "append_to_visible_p95_ms": 6,
                "last_error": ""
            },
            "age_seconds": 1
        }
    })
    .to_string();
    let (monitor_port, worker) =
        spawn_http_responses(vec![("200 OK", String::new()), ("200 OK", api_body)]);
    let root = temp_dir();
    let config = write_config(&root, monitor_port);

    let output = run_status(&config);
    assert!(
        output.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let status: Value = serde_json::from_slice(&output.stdout).expect("status JSON output");

    assert_eq!(status["data_source"], "daemon_api");
    assert_eq!(status["clickhouse_health_url"], "http://127.0.0.1:8123");
    assert_eq!(status["doctor"]["clickhouse_healthy"], true);
    assert_eq!(status["doctor"]["clickhouse_version"], "26.1.2.3");
    assert_eq!(status["doctor"]["database"], "api_db");
    assert_eq!(
        status["doctor"]["applied_migrations"],
        serde_json::json!([])
    );
    assert_eq!(status["heartbeat"]["state"], "available");
    assert_eq!(status["heartbeat"]["queue_depth"], 17);
    assert_eq!(status["heartbeat"]["watcher_backend"], "unknown");

    let requests = worker.join().expect("HTTP endpoint worker");
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert!(requests[0].starts_with("GET / HTTP/1.1"), "{}", requests[0]);
    assert!(
        requests[1].starts_with("GET /api/v1/status HTTP/1.1"),
        "{}",
        requests[1]
    );
    assert!(
        requests
            .iter()
            .all(|request| !request.contains("GET /api/status")),
        "{requests:?}"
    );
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn backend_status_distinguishes_endpoint_combinations_and_gates_monitor_url() {
    use std::os::unix::net::UnixListener;

    for (socket_live, http_live, managed, expected_state) in [
        (false, false, false, "stopped"),
        (true, false, false, "partial"),
        (false, true, false, "partial"),
        (true, true, false, "unmanaged"),
        (true, true, true, "running"),
    ] {
        let root = temp_dir();
        let socket_listener = socket_live
            .then(|| UnixListener::bind(root.join("backend.sock")).expect("bind MCP socket"));
        let (monitor_port, http_worker) = if http_live {
            let (port, worker) = spawn_http_endpoint();
            (port, Some(worker))
        } else {
            (0, None)
        };
        let config = write_config(&root, monitor_port);
        if managed {
            let run_dir = root.join("runtime/run");
            fs::create_dir_all(&run_dir).expect("create backend PID directory");
            fs::write(
                run_dir.join("backend.pid"),
                format!("{}\n", std::process::id()),
            )
            .expect("write backend PID");
        }

        let output = run_status(&config);
        assert!(
            output.status.success(),
            "status failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let status: Value = serde_json::from_slice(&output.stdout).expect("status JSON output");
        let backend = backend_row(&status);
        assert_eq!(backend["state"], expected_state);
        assert_eq!(backend["pid"].as_u64().is_some(), managed);
        assert_eq!(backend["socket_listening"], socket_live);
        assert_eq!(backend["http_listening"], http_live);
        assert_eq!(status["monitor_url"].is_string(), http_live);
        assert_eq!(status["data_source"], "direct_db");

        drop(socket_listener);
        if let Some(worker) = http_worker {
            worker.join().expect("HTTP endpoint worker");
        }
        let _ = fs::remove_dir_all(root);
    }
}

#[cfg(unix)]
#[test]
fn status_preserves_pid_files_when_processes_are_not_visible() {
    let root = temp_dir();
    let config = write_config(&root, 0);
    let run_dir = root.join("runtime/run");
    fs::create_dir_all(&run_dir).expect("create PID directory");
    let pid_bytes = b"2147483647\n";
    let pid_files = [
        "clickhouse.pid",
        "ingest.pid",
        "backend.pid",
        "monitor.pid",
        "mcp.pid",
    ];
    for pid_file in pid_files {
        fs::write(run_dir.join(pid_file), pid_bytes).expect("write PID file");
    }

    let output = run_status(&config);
    assert!(
        output.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let status: Value = serde_json::from_slice(&output.stdout).expect("status JSON output");

    let services = status["services"].as_array().expect("services array");
    for service_name in ["clickhouse", "ingest", "backend"] {
        let service = services
            .iter()
            .find(|service| service["service"] == service_name)
            .expect("service row");
        assert_eq!(service["state"], "stopped", "{service}");
        assert_eq!(service["pid"], Value::Null, "{service}");
    }
    let status_notes = status["status_notes"].as_array().expect("status notes");
    assert!(
        status_notes.iter().all(|note| !note
            .as_str()
            .expect("status note string")
            .contains("legacy managed")),
        "{status_notes:?}"
    );
    for pid_file in pid_files {
        assert_eq!(
            fs::read(run_dir.join(pid_file)).expect("read preserved PID file"),
            pid_bytes,
            "{pid_file} changed"
        );
    }

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn status_preserves_and_reports_visible_legacy_pid_files() {
    let root = temp_dir();
    let config = write_config(&root, 0);
    let run_dir = root.join("runtime/run");
    fs::create_dir_all(&run_dir).expect("create PID directory");
    let pid_bytes = format!("{}\n", std::process::id());
    for pid_file in ["monitor.pid", "mcp.pid"] {
        fs::write(run_dir.join(pid_file), &pid_bytes).expect("write legacy PID file");
    }

    let output = run_status(&config);
    assert!(
        output.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let status: Value = serde_json::from_slice(&output.stdout).expect("status JSON output");
    let status_notes = status["status_notes"].as_array().expect("status notes");
    for service_name in ["monitor", "MCP"] {
        let expected = format!(
            "legacy managed {service_name} process (pid {}) is still tracked; run `moraine down` before starting the unified backend",
            std::process::id()
        );
        assert!(
            status_notes.iter().any(|note| note == &expected),
            "missing {expected:?} in {status_notes:?}"
        );
    }
    for pid_file in ["monitor.pid", "mcp.pid"] {
        assert_eq!(
            fs::read_to_string(run_dir.join(pid_file)).expect("read preserved legacy PID file"),
            pid_bytes,
            "{pid_file} changed"
        );
    }

    let _ = fs::remove_dir_all(root);
}
