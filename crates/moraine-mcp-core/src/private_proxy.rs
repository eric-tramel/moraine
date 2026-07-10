use anyhow::{anyhow, bail, Context, Result};
use moraine_config::AppConfig;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

pub const PRIVATE_ROUTE_MAX_LINE_BYTES: usize = 64 * 1024;
const PRIVATE_ROUTE_ID: &str = "moraine-route-v1";
const PRIVATE_ROUTE_METHOD: &str = "moraine/private/route";
const PRIVATE_ROUTE_VERSION: u64 = 1;
const PRIVATE_ROUTE_ERROR_CODE: i64 = -32000;
const PRIVATE_ROUTE_INCOMPATIBLE_CODE: i64 = -32001;

/// Maximum time a proxy waits for the daemon's private route response.
///
/// A named-backend schema probe performs at most two sequential ClickHouse
/// requests. The extra second covers routing and control-message overhead.
/// ClickHouse applies a one-second floor to every configured request timeout;
/// this calculation intentionally applies the same floor.
pub fn private_route_deadline(cfg: &AppConfig) -> Result<Duration> {
    let mut max_timeout_seconds = 1.0_f64;

    for (backend_name, backend) in cfg.backends.iter() {
        let seconds = backend.timeout_seconds;
        if !seconds.is_finite() {
            bail!(
                "backend '{backend_name}' timeout_seconds must be finite to derive the private route deadline"
            );
        }
        max_timeout_seconds = max_timeout_seconds.max(seconds.max(1.0));
    }

    // Preserve the AppConfig invariant defensively for programmatic configs
    // whose `backends` map may have been cleared or desynchronized.
    let default_seconds = cfg.clickhouse.timeout_seconds;
    if !default_seconds.is_finite() {
        bail!(
            "default backend timeout_seconds must be finite to derive the private route deadline"
        );
    }
    max_timeout_seconds = max_timeout_seconds.max(default_seconds.max(1.0));

    let deadline_seconds = max_timeout_seconds.mul_add(2.0, 1.0);
    if !deadline_seconds.is_finite() {
        bail!("private route deadline is out of range");
    }
    Duration::try_from_secs_f64(deadline_seconds)
        .map_err(|_| anyhow!("private route deadline is out of range"))
}

/// Opaque socket halves retained after the daemon acknowledges a private route.
///
/// Pass this directly to [`crate::run_proxy`]; exposing only the accepted
/// connection makes it impossible to begin consuming agent stdin before the
/// ACK barrier.
#[cfg(unix)]
#[derive(Debug)]
pub struct PrivateProxyConnection {
    reader: tokio::io::BufReader<tokio::net::unix::OwnedReadHalf>,
    writer: tokio::net::unix::OwnedWriteHalf,
}

#[cfg(unix)]
impl PrivateProxyConnection {
    pub(crate) fn into_parts(
        self,
    ) -> (
        tokio::io::BufReader<tokio::net::unix::OwnedReadHalf>,
        tokio::net::unix::OwnedWriteHalf,
    ) {
        (self.reader, self.writer)
    }
}

/// Result of the daemon-private cwd negotiation performed before proxying any
/// agent bytes.
///
/// `Incompatible` is safe for the app to replace with a locally selected
/// embedded server because no agent input has been consumed. `Rejected` is a
/// recognized route/build/skew failure from a new daemon and is fatal: falling
/// back would silently change the selected backend. Only `Accepted` carries a
/// connection that can be passed to [`crate::run_proxy`].
#[cfg(unix)]
#[derive(Debug)]
pub enum PrivateRouteNegotiation {
    Accepted(PrivateProxyConnection),
    Incompatible { reason: String },
    Rejected { message: String },
}

/// Negotiate a cwd-selected repository with a connected central daemon.
///
/// The request and response are bounded to 64 KiB and the complete exchange is
/// limited by `deadline`. Every pre-ACK transport/protocol failure is returned
/// as [`PrivateRouteNegotiation::Incompatible`]; only an exact structured route
/// error becomes [`PrivateRouteNegotiation::Rejected`].
#[cfg(unix)]
pub async fn negotiate_private_route(
    stream: tokio::net::UnixStream,
    cwd: &str,
    deadline: Duration,
) -> PrivateRouteNegotiation {
    use tokio::io::BufReader;

    let request = json!({
        "jsonrpc": "2.0",
        "id": PRIVATE_ROUTE_ID,
        "method": PRIVATE_ROUTE_METHOD,
        "params": {
            "version": PRIVATE_ROUTE_VERSION,
            "cwd": cwd,
        },
    });
    let mut payload = match serde_json::to_vec(&request) {
        Ok(payload) => payload,
        Err(error) => {
            return PrivateRouteNegotiation::Incompatible {
                reason: format!("failed to encode private route request: {error}"),
            };
        }
    };
    payload.push(b'\n');
    if payload.len() > PRIVATE_ROUTE_MAX_LINE_BYTES {
        return PrivateRouteNegotiation::Incompatible {
            reason: format!(
                "private route request exceeds {} bytes",
                PRIVATE_ROUTE_MAX_LINE_BYTES
            ),
        };
    }

    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let exchange = async {
        write_half
            .write_all(&payload)
            .await
            .context("failed to write private route request")?;
        write_half
            .flush()
            .await
            .context("failed to flush private route request")?;
        read_bounded_line(&mut reader)
            .await?
            .ok_or_else(|| anyhow!("daemon closed before the private route response"))
    };

    let response = match tokio::time::timeout(deadline, exchange).await {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => {
            return PrivateRouteNegotiation::Incompatible {
                reason: error.to_string(),
            };
        }
        Err(_) => {
            return PrivateRouteNegotiation::Incompatible {
                reason: format!(
                    "private route negotiation timed out after {:.3}s",
                    deadline.as_secs_f64()
                ),
            };
        }
    };

    match classify_client_response(&response) {
        ClientResponse::Accepted => PrivateRouteNegotiation::Accepted(PrivateProxyConnection {
            reader,
            writer: write_half,
        }),
        ClientResponse::Incompatible(reason) => PrivateRouteNegotiation::Incompatible { reason },
        ClientResponse::Rejected(message) => PrivateRouteNegotiation::Rejected { message },
    }
}

#[cfg(unix)]
enum ClientResponse {
    Accepted,
    Incompatible(String),
    Rejected(String),
}

#[cfg(unix)]
fn classify_client_response(line: &[u8]) -> ClientResponse {
    let response: Value = match serde_json::from_slice(line) {
        Ok(response) => response,
        Err(error) => {
            return ClientResponse::Incompatible(format!(
                "daemon returned malformed private route JSON: {error}"
            ));
        }
    };

    if response.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || response.get("id").and_then(Value::as_str) != Some(PRIVATE_ROUTE_ID)
    {
        return ClientResponse::Incompatible(
            "daemon returned a mismatched private route response".to_string(),
        );
    }

    if response.get("error").is_none()
        && response
            .get("result")
            .and_then(|result| result.get("version"))
            .and_then(Value::as_u64)
            == Some(PRIVATE_ROUTE_VERSION)
    {
        return ClientResponse::Accepted;
    }

    let Some(error) = response.get("error") else {
        return ClientResponse::Incompatible(
            "daemon returned an unrecognized private route response".to_string(),
        );
    };
    if error.get("code").and_then(Value::as_i64) == Some(-32601) {
        return ClientResponse::Incompatible(
            "daemon does not support private route negotiation".to_string(),
        );
    }

    let data = error.get("data");
    let version = data
        .and_then(|data| data.get("version"))
        .and_then(Value::as_u64);
    let kind = data
        .and_then(|data| data.get("kind"))
        .and_then(Value::as_str);
    let code = error.get("code").and_then(Value::as_i64);
    if version != Some(PRIVATE_ROUTE_VERSION) {
        return ClientResponse::Incompatible(
            "daemon returned a private route response for an unsupported version".to_string(),
        );
    }

    match (code, kind) {
        (Some(PRIVATE_ROUTE_ERROR_CODE), Some("route")) => ClientResponse::Rejected(
            error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("daemon rejected the private route")
                .to_string(),
        ),
        (Some(PRIVATE_ROUTE_INCOMPATIBLE_CODE), Some("incompatible")) => {
            ClientResponse::Incompatible(
                error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("daemon rejected the private route protocol version")
                    .to_string(),
            )
        }
        _ => ClientResponse::Incompatible(
            "daemon returned an unrecognized private route error".to_string(),
        ),
    }
}

#[cfg(unix)]
pub(crate) enum ServerFirstLine {
    Route { cwd: String },
    Incompatible,
    Raw,
}

#[cfg(unix)]
pub(crate) fn classify_server_first_line(line: &[u8]) -> ServerFirstLine {
    if line.len() > PRIVATE_ROUTE_MAX_LINE_BYTES {
        return ServerFirstLine::Raw;
    }
    let request: Value = match serde_json::from_slice(line) {
        Ok(request) => request,
        Err(_) => return ServerFirstLine::Raw,
    };

    if request.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || request.get("id").and_then(Value::as_str) != Some(PRIVATE_ROUTE_ID)
        || request.get("method").and_then(Value::as_str) != Some(PRIVATE_ROUTE_METHOD)
    {
        return ServerFirstLine::Raw;
    }

    let params = request.get("params");
    let version = params
        .and_then(|params| params.get("version"))
        .and_then(Value::as_u64);
    if version != Some(PRIVATE_ROUTE_VERSION) {
        return ServerFirstLine::Incompatible;
    }
    let Some(cwd) = params
        .and_then(|params| params.get("cwd"))
        .and_then(Value::as_str)
    else {
        return ServerFirstLine::Incompatible;
    };

    ServerFirstLine::Route {
        cwd: cwd.to_string(),
    }
}

#[cfg(unix)]
pub(crate) async fn write_ack<W>(writer: &mut W) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_control_response(
        writer,
        &json!({
            "jsonrpc": "2.0",
            "id": PRIVATE_ROUTE_ID,
            "result": { "version": PRIVATE_ROUTE_VERSION },
        }),
    )
    .await
}

#[cfg(unix)]
pub(crate) async fn write_route_error<W>(writer: &mut W, message: &str) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    // Eight KiB remains under the 64-KiB control-line limit even if every
    // input byte needs serde_json's six-byte `\u00XX` escape.
    const MAX_ROUTE_ERROR_MESSAGE_BYTES: usize = 8 * 1024;
    let message = if message.len() <= MAX_ROUTE_ERROR_MESSAGE_BYTES {
        std::borrow::Cow::Borrowed(message)
    } else {
        let mut end = MAX_ROUTE_ERROR_MESSAGE_BYTES - 3;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        std::borrow::Cow::Owned(format!("{}...", &message[..end]))
    };
    write_control_response(
        writer,
        &json!({
            "jsonrpc": "2.0",
            "id": PRIVATE_ROUTE_ID,
            "error": {
                "code": PRIVATE_ROUTE_ERROR_CODE,
                "message": message,
                "data": {
                    "kind": "route",
                    "version": PRIVATE_ROUTE_VERSION,
                },
            },
        }),
    )
    .await
}

#[cfg(unix)]
pub(crate) async fn write_incompatible_error<W>(writer: &mut W) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_control_response(
        writer,
        &json!({
            "jsonrpc": "2.0",
            "id": PRIVATE_ROUTE_ID,
            "error": {
                "code": PRIVATE_ROUTE_INCOMPATIBLE_CODE,
                "message": "unsupported private route protocol version",
                "data": {
                    "kind": "incompatible",
                    "version": PRIVATE_ROUTE_VERSION,
                },
            },
        }),
    )
    .await
}

#[cfg(unix)]
async fn write_control_response<W>(writer: &mut W, response: &Value) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut payload = serde_json::to_vec(response)?;
    payload.push(b'\n');
    if payload.len() > PRIVATE_ROUTE_MAX_LINE_BYTES {
        bail!(
            "private route response exceeds {} bytes",
            PRIVATE_ROUTE_MAX_LINE_BYTES
        );
    }
    writer.write_all(&payload).await?;
    writer.flush().await?;
    Ok(())
}

/// Read the first daemon-side line with the public MCP transport's historical
/// unbounded framing. Oversized lines are classified as raw MCP rather than as
/// private control messages, so the 64-KiB control bound never truncates a
/// legacy client's first request.
pub(crate) async fn read_server_first_line<R>(reader: &mut R) -> Result<Option<Vec<u8>>>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::new();
    let bytes = reader.read_until(b'\n', &mut line).await?;
    Ok((bytes != 0).then_some(line))
}

/// Read one line without ever allowing its allocation to exceed the private
/// control protocol's bound. The newline, when present, is retained.
pub(crate) async fn read_bounded_line<R>(reader: &mut R) -> Result<Option<Vec<u8>>>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::with_capacity(512);
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok((!line.is_empty()).then_some(line));
        }

        if let Some(newline) = available.iter().position(|byte| *byte == b'\n') {
            let consumed = newline + 1;
            if line.len() + consumed > PRIVATE_ROUTE_MAX_LINE_BYTES {
                bail!(
                    "private route control line exceeds {} bytes",
                    PRIVATE_ROUTE_MAX_LINE_BYTES
                );
            }
            line.extend_from_slice(&available[..consumed]);
            reader.consume(consumed);
            return Ok(Some(line));
        }

        if line.len() + available.len() > PRIVATE_ROUTE_MAX_LINE_BYTES {
            bail!(
                "private route control line exceeds {} bytes",
                PRIVATE_ROUTE_MAX_LINE_BYTES
            );
        }
        line.extend_from_slice(available);
        let consumed = available.len();
        reader.consume(consumed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    #[test]
    fn deadline_uses_effective_max_backend_timeout() {
        let mut cfg = AppConfig::default();
        assert_eq!(
            private_route_deadline(&cfg).expect("default deadline"),
            Duration::from_secs(61)
        );

        cfg.backends.insert(
            "slow".to_string(),
            moraine_config::ClickHouseConfig {
                timeout_seconds: 45.0,
                ..Default::default()
            },
        );
        assert_eq!(
            private_route_deadline(&cfg).expect("slow deadline"),
            Duration::from_secs(91)
        );

        cfg.clickhouse.timeout_seconds = 0.2;
        for backend in cfg.backends.values_mut() {
            backend.timeout_seconds = 0.2;
        }
        assert_eq!(
            private_route_deadline(&cfg).expect("floored deadline"),
            Duration::from_secs(3)
        );
    }

    #[test]
    fn deadline_rejects_non_finite_and_out_of_range_values() {
        let mut cfg = AppConfig::default();
        cfg.backends
            .get_mut("default")
            .expect("default backend")
            .timeout_seconds = f64::INFINITY;
        assert!(private_route_deadline(&cfg)
            .expect_err("infinite timeout must fail")
            .to_string()
            .contains("must be finite"));

        cfg.backends
            .get_mut("default")
            .expect("default backend")
            .timeout_seconds = f64::MAX;
        assert!(private_route_deadline(&cfg)
            .expect_err("overflowing timeout must fail")
            .to_string()
            .contains("out of range"));
    }

    #[tokio::test]
    async fn bounded_line_accepts_limit_and_rejects_one_byte_over() {
        let mut exact = vec![b'x'; PRIVATE_ROUTE_MAX_LINE_BYTES - 1];
        exact.push(b'\n');
        let mut exact_reader = BufReader::new(std::io::Cursor::new(exact.clone()));
        assert_eq!(
            read_bounded_line(&mut exact_reader)
                .await
                .expect("exact line"),
            Some(exact)
        );

        let mut oversized = vec![b'x'; PRIVATE_ROUTE_MAX_LINE_BYTES];
        oversized.push(b'\n');
        let mut oversized_reader = BufReader::new(std::io::Cursor::new(oversized));
        assert!(read_bounded_line(&mut oversized_reader)
            .await
            .expect_err("oversized line must fail")
            .to_string()
            .contains("exceeds"));
    }

    #[cfg(unix)]
    #[test]
    fn server_discriminates_only_exact_private_hello() {
        let exact = serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": "moraine-route-v1",
            "method": "moraine/private/route",
            "params": {"version": 1, "cwd": "/work/team"},
        }))
        .expect("exact hello");
        match classify_server_first_line(&exact) {
            ServerFirstLine::Route { cwd } => assert_eq!(cwd, "/work/team"),
            _ => panic!("exact hello must route"),
        }

        let unsupported = serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": "moraine-route-v1",
            "method": "moraine/private/route",
            "params": {"version": 2, "cwd": "/work/team"},
        }))
        .expect("unsupported hello");
        assert!(matches!(
            classify_server_first_line(&unsupported),
            ServerFirstLine::Incompatible
        ));

        let wrong_id = serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": "agent-request",
            "method": "moraine/private/route",
            "params": {"version": 1, "cwd": "/work/team"},
        }))
        .expect("raw request");
        assert!(matches!(
            classify_server_first_line(&wrong_id),
            ServerFirstLine::Raw
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn server_emits_exact_ack_and_structured_errors() {
        let mut ack = Vec::new();
        write_ack(&mut ack).await.expect("ack");
        assert_eq!(
            serde_json::from_slice::<Value>(&ack).expect("ack json"),
            json!({
                "jsonrpc": "2.0",
                "id": "moraine-route-v1",
                "result": {"version": 1},
            })
        );

        let mut route = Vec::new();
        write_route_error(&mut route, "backend 'team-ch' is older")
            .await
            .expect("route error");
        assert_eq!(
            serde_json::from_slice::<Value>(&route).expect("route json"),
            json!({
                "jsonrpc": "2.0",
                "id": "moraine-route-v1",
                "error": {
                    "code": -32000,
                    "message": "backend 'team-ch' is older",
                    "data": {"kind": "route", "version": 1},
                },
            })
        );

        let mut bounded_route = Vec::new();
        let escape_heavy_message = format!("backend team-ch: {}", "\u{1}".repeat(20_000));
        write_route_error(&mut bounded_route, &escape_heavy_message)
            .await
            .expect("oversized route error is truncated, not dropped");
        assert!(bounded_route.len() <= PRIVATE_ROUTE_MAX_LINE_BYTES);
        let bounded_route: Value =
            serde_json::from_slice(&bounded_route).expect("bounded route JSON");
        assert_eq!(bounded_route["error"]["data"]["kind"], json!("route"));
        assert!(bounded_route["error"]["message"]
            .as_str()
            .expect("bounded route message")
            .starts_with("backend team-ch"));

        let mut incompatible = Vec::new();
        write_incompatible_error(&mut incompatible)
            .await
            .expect("incompatible error");
        assert_eq!(
            serde_json::from_slice::<Value>(&incompatible).expect("incompatible json"),
            json!({
                "jsonrpc": "2.0",
                "id": "moraine-route-v1",
                "error": {
                    "code": -32001,
                    "message": "unsupported private route protocol version",
                    "data": {"kind": "incompatible", "version": 1},
                },
            })
        );
    }

    #[cfg(unix)]
    async fn negotiation_with_response(response: Value) -> PrivateRouteNegotiation {
        let (client, server) = tokio::net::UnixStream::pair().expect("socket pair");
        tokio::spawn(async move {
            let (read_half, mut write_half) = server.into_split();
            let mut reader = BufReader::new(read_half);
            let mut request = String::new();
            reader
                .read_line(&mut request)
                .await
                .expect("read private request");
            write_half
                .write_all(&serde_json::to_vec(&response).expect("response json"))
                .await
                .expect("write private response");
            write_half.write_all(b"\n").await.expect("write newline");
        });
        negotiate_private_route(client, "/work/team", Duration::from_secs(1)).await
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn client_classifies_old_incompatible_and_route_rejection_exactly() {
        let old = negotiation_with_response(json!({
            "jsonrpc": "2.0",
            "id": "moraine-route-v1",
            "error": {"code": -32601, "message": "method not found"},
        }))
        .await;
        assert!(matches!(old, PrivateRouteNegotiation::Incompatible { .. }));

        let wrong_version = negotiation_with_response(json!({
            "jsonrpc": "2.0",
            "id": "moraine-route-v1",
            "result": {"version": 2},
        }))
        .await;
        assert!(matches!(
            wrong_version,
            PrivateRouteNegotiation::Incompatible { .. }
        ));

        let rejected = negotiation_with_response(json!({
            "jsonrpc": "2.0",
            "id": "moraine-route-v1",
            "error": {
                "code": -32000,
                "message": "backend 'team-ch' is older",
                "data": {"kind": "route", "version": 1},
            },
        }))
        .await;
        match rejected {
            PrivateRouteNegotiation::Rejected { message } => {
                assert_eq!(message, "backend 'team-ch' is older")
            }
            _ => panic!("structured route error must be fatal"),
        }

        let wrong_code = negotiation_with_response(json!({
            "jsonrpc": "2.0",
            "id": "moraine-route-v1",
            "error": {
                "code": -32099,
                "message": "not the private route error contract",
                "data": {"kind": "route", "version": 1},
            },
        }))
        .await;
        assert!(matches!(
            wrong_code,
            PrivateRouteNegotiation::Incompatible { .. }
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn client_emits_exact_private_route_request() {
        let (client, server) = tokio::net::UnixStream::pair().expect("socket pair");
        let (request_tx, request_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (read_half, mut write_half) = server.into_split();
            let mut reader = BufReader::new(read_half);
            let mut request = String::new();
            reader
                .read_line(&mut request)
                .await
                .expect("read private request");
            request_tx
                .send(serde_json::from_str::<Value>(request.trim()).expect("request JSON"))
                .expect("capture request");
            write_half
                .write_all(
                    b"{\"jsonrpc\":\"2.0\",\"id\":\"moraine-route-v1\",\"result\":{\"version\":1}}\n",
                )
                .await
                .expect("write ACK");
        });

        let outcome = negotiate_private_route(client, "/work/team", Duration::from_secs(1)).await;
        assert!(matches!(outcome, PrivateRouteNegotiation::Accepted(_)));
        assert_eq!(
            request_rx.await.expect("captured request"),
            json!({
                "jsonrpc": "2.0",
                "id": "moraine-route-v1",
                "method": "moraine/private/route",
                "params": {"version": 1, "cwd": "/work/team"},
            })
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn delayed_ack_beyond_connect_timeout_remains_acceptable() {
        let (client, server) = tokio::net::UnixStream::pair().expect("socket pair");
        tokio::spawn(async move {
            let (read_half, mut write_half) = server.into_split();
            let mut reader = BufReader::new(read_half);
            let mut request = String::new();
            reader
                .read_line(&mut request)
                .await
                .expect("read private request");
            tokio::time::sleep(Duration::from_millis(300)).await;
            write_half
                .write_all(
                    b"{\"jsonrpc\":\"2.0\",\"id\":\"moraine-route-v1\",\"result\":{\"version\":1}}\n",
                )
                .await
                .expect("write delayed ACK");
        });

        let outcome = negotiate_private_route(client, "/work/team", Duration::from_secs(1)).await;
        assert!(matches!(outcome, PrivateRouteNegotiation::Accepted(_)));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn eof_malformed_response_and_deadline_are_incompatible() {
        let (eof_client, eof_server) = tokio::net::UnixStream::pair().expect("EOF socket pair");
        tokio::spawn(async move {
            let (read_half, _write_half) = eof_server.into_split();
            let mut reader = BufReader::new(read_half);
            let mut request = String::new();
            reader
                .read_line(&mut request)
                .await
                .expect("read EOF request");
        });
        let eof = negotiate_private_route(eof_client, "/work/team", Duration::from_secs(1)).await;
        assert!(matches!(eof, PrivateRouteNegotiation::Incompatible { .. }));

        let (malformed_client, malformed_server) =
            tokio::net::UnixStream::pair().expect("malformed socket pair");
        tokio::spawn(async move {
            let (read_half, mut write_half) = malformed_server.into_split();
            let mut reader = BufReader::new(read_half);
            let mut request = String::new();
            reader
                .read_line(&mut request)
                .await
                .expect("read malformed request");
            write_half
                .write_all(b"{not-json}\n")
                .await
                .expect("write malformed response");
        });
        let malformed =
            negotiate_private_route(malformed_client, "/work/team", Duration::from_secs(1)).await;
        assert!(matches!(
            malformed,
            PrivateRouteNegotiation::Incompatible { .. }
        ));

        let (timeout_client, timeout_server) =
            tokio::net::UnixStream::pair().expect("timeout socket pair");
        tokio::spawn(async move {
            let (read_half, _write_half) = timeout_server.into_split();
            let mut reader = BufReader::new(read_half);
            let mut request = String::new();
            reader
                .read_line(&mut request)
                .await
                .expect("read timeout request");
            tokio::time::sleep(Duration::from_millis(100)).await;
        });
        let timed_out =
            negotiate_private_route(timeout_client, "/work/team", Duration::from_millis(10)).await;
        match timed_out {
            PrivateRouteNegotiation::Incompatible { reason } => {
                assert!(reason.contains("timed out"), "unexpected reason: {reason}");
            }
            _ => panic!("private response deadline must be incompatible"),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn accepted_negotiation_retains_bytes_buffered_after_ack() {
        let (client, server) = tokio::net::UnixStream::pair().expect("socket pair");
        tokio::spawn(async move {
            let (read_half, mut write_half) = server.into_split();
            let mut reader = BufReader::new(read_half);
            let mut request = String::new();
            reader
                .read_line(&mut request)
                .await
                .expect("read private request");
            write_half
                .write_all(
                    b"{\"jsonrpc\":\"2.0\",\"id\":\"moraine-route-v1\",\"result\":{\"version\":1}}\nafter-ack\n",
                )
                .await
                .expect("write ack and data");
        });

        let accepted = negotiate_private_route(client, "/work/team", Duration::from_secs(1)).await;
        let PrivateRouteNegotiation::Accepted(connection) = accepted else {
            panic!("valid ACK must be accepted");
        };
        let (mut reader, _writer) = connection.into_parts();
        let mut line = String::new();
        reader.read_line(&mut line).await.expect("retained line");
        assert_eq!(line, "after-ack\n");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn oversized_client_response_is_incompatible() {
        let (client, server) = tokio::net::UnixStream::pair().expect("socket pair");
        tokio::spawn(async move {
            let (read_half, mut write_half) = server.into_split();
            let mut reader = BufReader::new(read_half);
            let mut request = String::new();
            reader
                .read_line(&mut request)
                .await
                .expect("read private request");
            write_half
                .write_all(&vec![b'x'; PRIVATE_ROUTE_MAX_LINE_BYTES + 1])
                .await
                .expect("write oversized response");
            write_half.write_all(b"\n").await.expect("write newline");
        });

        let outcome = negotiate_private_route(client, "/work/team", Duration::from_secs(1)).await;
        match outcome {
            PrivateRouteNegotiation::Incompatible { reason } => {
                assert!(reason.contains("exceeds"), "unexpected reason: {reason}");
            }
            _ => panic!("oversized response must be incompatible"),
        }
    }
}
