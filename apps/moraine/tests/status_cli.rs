use std::fs;
#[cfg(unix)]
use std::io::Write;
#[cfg(unix)]
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
#[cfg(unix)]
use std::thread;
#[cfg(unix)]
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

fn temp_dir() -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("moraine-st-{}-{stamp}", std::process::id()));
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
host = "127.0.0.1"
port = {monitor_port}

[backend]
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

#[cfg(unix)]
fn spawn_http_endpoint() -> (u16, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind HTTP endpoint");
    let port = listener.local_addr().expect("HTTP endpoint address").port();
    listener
        .set_nonblocking(true)
        .expect("nonblocking HTTP endpoint");
    let worker = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let _ = stream.write_all(
                        b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    );
                    return;
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return;
                    }
                    thread::sleep(Duration::from_millis(10));
                }
                Err(_) => return,
            }
        }
    });
    (port, worker)
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

        drop(socket_listener);
        if let Some(worker) = http_worker {
            worker.join().expect("HTTP endpoint worker");
        }
        let _ = fs::remove_dir_all(root);
    }
}
