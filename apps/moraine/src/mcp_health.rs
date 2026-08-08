use serde_json::{json, Value};
use std::time::Duration;

const MAX_MCP_RESPONSE_BYTES: u64 = 64 * 1024;
const MAX_MCP_VERSION_BYTES: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct McpHealthSnapshot {
    pub(crate) healthy: bool,
    pub(crate) protocol_version: Option<String>,
    pub(crate) server_version: Option<String>,
    pub(crate) error: Option<String>,
}

impl McpHealthSnapshot {
    fn healthy(protocol_version: String, server_version: String) -> Self {
        Self {
            healthy: true,
            protocol_version: Some(protocol_version),
            server_version: Some(server_version),
            error: None,
        }
    }

    pub(crate) fn unhealthy(error: impl Into<String>) -> Self {
        Self {
            healthy: false,
            protocol_version: None,
            server_version: None,
            error: Some(error.into()),
        }
    }
}

#[cfg(unix)]
fn io_diagnostic(action: &str, error: &std::io::Error) -> String {
    let detail = match error.kind() {
        std::io::ErrorKind::NotFound => "MCP socket is absent",
        std::io::ErrorKind::ConnectionRefused => "MCP socket refused the connection",
        std::io::ErrorKind::PermissionDenied => "MCP socket permission was denied",
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock => {
            "MCP backend did not respond before the timeout"
        }
        std::io::ErrorKind::BrokenPipe
        | std::io::ErrorKind::ConnectionAborted
        | std::io::ErrorKind::ConnectionReset
        | std::io::ErrorKind::UnexpectedEof => "MCP backend closed the connection",
        _ => "MCP socket I/O failed",
    };
    format!("{action}: {detail}")
}

#[cfg(unix)]
async fn write_message<W>(writer: &mut W, message: &Value, action: &str) -> Result<(), String>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;

    let mut encoded = serde_json::to_vec(message)
        .map_err(|_| format!("{action}: failed to encode MCP request"))?;
    encoded.push(b'\n');
    writer
        .write_all(&encoded)
        .await
        .map_err(|error| io_diagnostic(action, &error))?;
    writer
        .flush()
        .await
        .map_err(|error| io_diagnostic(action, &error))
}

#[cfg(unix)]
async fn read_response<R>(reader: &mut R, expected_id: i64, action: &str) -> Result<Value, String>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    use tokio::io::{AsyncBufReadExt, AsyncReadExt};

    let mut body = Vec::new();
    let bytes_read = reader
        .take(MAX_MCP_RESPONSE_BYTES + 1)
        .read_until(b'\n', &mut body)
        .await
        .map_err(|error| io_diagnostic(action, &error))?;
    if bytes_read == 0 {
        return Err(format!("{action}: MCP backend closed the connection"));
    }
    if bytes_read as u64 > MAX_MCP_RESPONSE_BYTES || body.last() != Some(&b'\n') {
        return Err(format!("{action}: MCP response exceeded the size limit"));
    }

    let response: Value =
        serde_json::from_slice(&body).map_err(|_| format!("{action}: invalid JSON response"))?;
    if response.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err(format!("{action}: invalid JSON-RPC version"));
    }
    if response.get("id") != Some(&json!(expected_id)) {
        return Err(format!("{action}: response id did not match the request"));
    }
    if response.get("error").is_some_and(|error| !error.is_null()) {
        return Err(format!("{action}: MCP backend returned a JSON-RPC error"));
    }
    Ok(response)
}

#[cfg(unix)]
fn validated_version(value: Option<&Value>, field: &str) -> Result<String, String> {
    let value = value
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("initialize handshake: missing {field}"))?;
    if value.len() > MAX_MCP_VERSION_BYTES || !value.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(format!("initialize handshake: invalid {field}"));
    }
    Ok(value.to_string())
}

#[cfg(unix)]
async fn probe_mcp_backend_inner(
    socket_path: &str,
    protocol_version: &str,
) -> Result<McpHealthSnapshot, String> {
    use tokio::io::BufReader;
    use tokio::net::UnixStream;

    let stream = UnixStream::connect(socket_path)
        .await
        .map_err(|error| io_diagnostic("connect", &error))?;
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    write_message(
        &mut write_half,
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": protocol_version,
                "capabilities": {},
                "clientInfo": {
                    "name": "moraine-cli-health",
                    "version": moraine_config::BUILD_VERSION
                }
            }
        }),
        "initialize handshake",
    )
    .await?;
    let initialize = read_response(&mut reader, 1, "initialize handshake").await?;
    let result = initialize
        .get("result")
        .and_then(Value::as_object)
        .ok_or_else(|| "initialize handshake: missing result".to_string())?;
    let negotiated_protocol = validated_version(result.get("protocolVersion"), "protocol version")?;
    let server_info = result
        .get("serverInfo")
        .and_then(Value::as_object)
        .ok_or_else(|| "initialize handshake: missing server info".to_string())?;
    if server_info.get("name").and_then(Value::as_str) != Some("moraine-mcp") {
        return Err("initialize handshake: unexpected MCP server identity".to_string());
    }
    let server_version = validated_version(server_info.get("version"), "server version")?;

    write_message(
        &mut write_half,
        &json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
            "params": {}
        }),
        "initialize notification",
    )
    .await?;
    write_message(
        &mut write_half,
        &json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "ping",
            "params": {}
        }),
        "MCP ping",
    )
    .await?;
    let ping = read_response(&mut reader, 2, "MCP ping").await?;
    if !ping.get("result").is_some_and(Value::is_object) {
        return Err("MCP ping: missing result".to_string());
    }

    Ok(McpHealthSnapshot::healthy(
        negotiated_protocol,
        server_version,
    ))
}

#[cfg(unix)]
pub(crate) async fn probe_mcp_backend(
    socket_path: &str,
    protocol_version: &str,
    timeout: Duration,
) -> McpHealthSnapshot {
    match tokio::time::timeout(
        timeout,
        probe_mcp_backend_inner(socket_path, protocol_version),
    )
    .await
    {
        Ok(Ok(health)) => health,
        Ok(Err(error)) => McpHealthSnapshot::unhealthy(error),
        Err(_) => McpHealthSnapshot::unhealthy("MCP health check timed out"),
    }
}

#[cfg(not(unix))]
pub(crate) async fn probe_mcp_backend(
    _socket_path: &str,
    _protocol_version: &str,
    _timeout: Duration,
) -> McpHealthSnapshot {
    McpHealthSnapshot::unhealthy("MCP backend health checks require a Unix socket")
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::net::UnixListener;
    use std::path::PathBuf;
    use std::thread;
    use std::time::{Instant, SystemTime, UNIX_EPOCH};

    fn socket_path(label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        PathBuf::from("/tmp").join(format!("mh-{label}-{}-{stamp}.sock", std::process::id()))
    }

    #[tokio::test]
    async fn initialize_and_ping_prove_backend_health() {
        let path = socket_path("healthy");
        let listener = UnixListener::bind(&path).expect("bind MCP fixture");
        let worker = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept probe");
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .expect("set fixture timeout");
            let mut reader = BufReader::new(stream.try_clone().expect("clone fixture stream"));
            let mut methods = Vec::new();
            for response in [
                json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "protocolVersion": "2025-03-26",
                        "capabilities": {"tools": {"listChanged": false}},
                        "serverInfo": {"name": "moraine-mcp", "version": "test-version"}
                    }
                }),
                json!({"jsonrpc": "2.0", "id": 2, "result": {}}),
            ] {
                let mut line = String::new();
                reader.read_line(&mut line).expect("read MCP request");
                let request: Value = serde_json::from_str(&line).expect("decode MCP request");
                methods.push(
                    request["method"]
                        .as_str()
                        .expect("request method")
                        .to_string(),
                );
                if request["method"] == "notifications/initialized" {
                    line.clear();
                    reader
                        .read_line(&mut line)
                        .expect("read ping after notification");
                    let ping: Value = serde_json::from_str(&line).expect("decode ping");
                    methods.push(ping["method"].as_str().expect("ping method").to_string());
                }
                serde_json::to_writer(&mut stream, &response).expect("encode MCP response");
                stream.write_all(b"\n").expect("write MCP response");
                stream.flush().expect("flush MCP response");
            }
            methods
        });

        let health = probe_mcp_backend(
            path.to_str().expect("UTF-8 socket path"),
            "2025-03-26",
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(
            health,
            McpHealthSnapshot {
                healthy: true,
                protocol_version: Some("2025-03-26".to_string()),
                server_version: Some("test-version".to_string()),
                error: None,
            }
        );
        assert_eq!(
            worker.join().expect("MCP fixture worker"),
            ["initialize", "notifications/initialized", "ping"]
        );
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn total_probe_deadline_rejects_drip_fed_response() {
        let path = socket_path("deadline");
        let listener = UnixListener::bind(&path).expect("bind MCP fixture");
        let worker = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept probe");
            let mut request = String::new();
            BufReader::new(stream.try_clone().expect("clone fixture stream"))
                .read_line(&mut request)
                .expect("read initialize request");
            for byte in br#"{"jsonrpc":"2.0","id":1,"result":{}}\n"# {
                if stream.write_all(std::slice::from_ref(byte)).is_err() {
                    break;
                }
                let _ = stream.flush();
                thread::sleep(Duration::from_millis(30));
            }
        });

        let started = Instant::now();
        let health = probe_mcp_backend(
            path.to_str().expect("UTF-8 socket path"),
            "2025-03-26",
            Duration::from_millis(100),
        )
        .await;
        assert_eq!(health.error.as_deref(), Some("MCP health check timed out"));
        assert!(started.elapsed() < Duration::from_secs(1));
        worker.join().expect("MCP fixture worker");
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn rejects_control_characters_in_reported_versions() {
        let path = socket_path("controls");
        let listener = UnixListener::bind(&path).expect("bind MCP fixture");
        let worker = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept probe");
            let mut request = String::new();
            BufReader::new(stream.try_clone().expect("clone fixture stream"))
                .read_line(&mut request)
                .expect("read initialize request");
            serde_json::to_writer(
                &mut stream,
                &json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "protocolVersion": "2025-03-26",
                        "serverInfo": {"name": "moraine-mcp", "version": "forged\nline"}
                    }
                }),
            )
            .expect("encode MCP response");
            stream.write_all(b"\n").expect("write MCP response");
        });

        let health = probe_mcp_backend(
            path.to_str().expect("UTF-8 socket path"),
            "2025-03-26",
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(
            health.error.as_deref(),
            Some("initialize handshake: invalid server version")
        );
        worker.join().expect("MCP fixture worker");
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn diagnostics_do_not_expose_socket_paths() {
        let missing = socket_path("missing");
        let health = probe_mcp_backend(
            missing.to_str().expect("UTF-8 socket path"),
            "2025-03-26",
            Duration::from_millis(100),
        )
        .await;
        let error = health.error.expect("health error");
        assert!(error.contains("socket is absent"), "{error}");
        assert!(!error.contains(missing.to_str().expect("UTF-8 socket path")));
    }
}
