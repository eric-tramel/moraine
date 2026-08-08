use super::{
    dispatch_rpc_request, finish_request, ActiveRequest, McpDaemonState, RequestResult, RpcRequest,
    SUPPORTED_PROTOCOL_VERSIONS,
};
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::IpAddr;
use tokio::task::JoinSet;

const MCP_PROTOCOL_VERSION: &str = "mcp-protocol-version";

pub fn http_router(state: McpDaemonState) -> Router {
    Router::new()
        .route("/mcp", post(handle_mcp_post))
        .with_state(state)
}

async fn handle_mcp_post(
    State(state): State<McpDaemonState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !content_type_is_json(&headers) {
        return status_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "content type must be application/json",
        );
    }
    if !accepts_json(&headers) {
        return status_error(
            StatusCode::NOT_ACCEPTABLE,
            "accept must include application/json",
        );
    }
    if !origin_is_allowed(&headers) {
        return status_error(
            StatusCode::FORBIDDEN,
            "origin must name a loopback IP address",
        );
    }

    let payload = match serde_json::from_slice::<Value>(&body) {
        Ok(payload) => payload,
        Err(error) => {
            return status_error(
                StatusCode::BAD_REQUEST,
                &format!("invalid JSON-RPC request: {error}"),
            )
        }
    };
    if payload.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return status_error(StatusCode::BAD_REQUEST, "jsonrpc must be 2.0");
    }
    if !jsonrpc_shape_is_valid(&payload) {
        return status_error(StatusCode::BAD_REQUEST, "invalid JSON-RPC request shape");
    }
    let explicit_null_id = payload.get("id").is_some_and(Value::is_null);
    let mut request = match serde_json::from_value::<RpcRequest>(payload) {
        Ok(request) => request,
        Err(error) => {
            return status_error(
                StatusCode::BAD_REQUEST,
                &format!("invalid JSON-RPC request: {error}"),
            )
        }
    };
    if explicit_null_id {
        request.id = Some(Value::Null);
    }
    if !protocol_version_is_allowed(
        &headers,
        request.method == "initialize",
        &state.cfg.mcp.protocol_version,
    ) {
        return status_error(
            StatusCode::BAD_REQUEST,
            "missing or unsupported MCP-Protocol-Version",
        );
    }

    let app_state = match state.default_app_state().await {
        Ok(app_state) => app_state,
        Err(error) => {
            tracing::warn!("MCP HTTP default backend selection failed: {error:#}");
            return status_error(StatusCode::SERVICE_UNAVAILABLE, "MCP backend unavailable");
        }
    };
    let notification = request.id.is_none();
    let mut requests = JoinSet::<RequestResult>::new();
    let mut active = HashMap::<String, ActiveRequest>::new();
    let immediate = dispatch_rpc_request(
        &app_state,
        request,
        tokio::time::Instant::now(),
        &mut requests,
        &mut active,
    )
    .await;

    match immediate {
        Ok(Some(response)) => json_response(response),
        Ok(None) if notification => StatusCode::ACCEPTED.into_response(),
        Ok(None) => {
            while let Some(completed) = requests.join_next().await {
                if let Some(response) = finish_request(Some(completed), &mut active) {
                    return json_response(response);
                }
            }
            status_error(StatusCode::SERVICE_UNAVAILABLE, "MCP request was cancelled")
        }
        Err(error) => {
            tracing::warn!("MCP HTTP request dispatch failed: {error:#}");
            status_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "MCP request dispatch failed",
            )
        }
    }
}

fn content_type_is_json(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
}

fn jsonrpc_shape_is_valid(payload: &Value) -> bool {
    let Some(request) = payload.as_object() else {
        return false;
    };
    let id_is_valid = matches!(
        request.get("id"),
        None | Some(Value::Null | Value::String(_) | Value::Number(_))
    );
    let params_are_valid = matches!(
        request.get("params"),
        None | Some(Value::Object(_) | Value::Array(_))
    );
    id_is_valid && params_are_valid
}

fn accepts_json(headers: &HeaderMap) -> bool {
    headers
        .get_all(header::ACCEPT)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|entry| {
            let mut parts = entry.split(';');
            let media_type = parts.next().unwrap_or_default().trim();
            let disabled = parts.any(|parameter| {
                let (name, value) = parameter.split_once('=').unwrap_or((parameter, ""));
                name.trim().eq_ignore_ascii_case("q")
                    && value
                        .trim()
                        .parse::<f32>()
                        .is_ok_and(|quality| quality == 0.0)
            });
            media_type.eq_ignore_ascii_case("application/json") && !disabled
        })
}

fn origin_is_allowed(headers: &HeaderMap) -> bool {
    let Some(origin) = headers.get(header::ORIGIN) else {
        return true;
    };
    let Ok(origin) = origin.to_str() else {
        return false;
    };
    let Ok(uri) = origin.parse::<Uri>() else {
        return false;
    };
    if !matches!(uri.scheme_str(), Some("http" | "https")) {
        return false;
    }
    uri.host()
        .map(|host| {
            host.strip_prefix('[')
                .and_then(|inner| inner.strip_suffix(']'))
                .unwrap_or(host)
        })
        .and_then(|host| host.parse::<IpAddr>().ok())
        .is_some_and(|address| address.is_loopback())
}

fn protocol_version_is_allowed(headers: &HeaderMap, initialize: bool, configured: &str) -> bool {
    let Some(value) = headers.get(MCP_PROTOCOL_VERSION) else {
        return initialize;
    };
    value.to_str().ok().is_some_and(|version| {
        version == configured || SUPPORTED_PROTOCOL_VERSIONS.contains(&version)
    })
}

fn json_response(value: Value) -> Response {
    Json(value).into_response()
}

fn status_error(status: StatusCode, message: &str) -> Response {
    let mut response = Json(json!({"error": message})).into_response();
    *response.status_mut() = status;
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use crate::{run_socket_with_state, MAX_QUEUED_REQUESTS};
    use axum::body::to_bytes;
    use axum::body::Body;
    use axum::http::HeaderValue;
    use axum::http::Request;
    use moraine_config::AppConfig;
    use moraine_conversations::{
        BackendRepositoryRouter, ConversationRepository, InMemoryConversationRepository,
        QueryRuntime,
    };
    use std::sync::Arc;
    use tower::ServiceExt;

    fn test_router() -> Router {
        test_router_with_config(AppConfig::default())
    }

    fn test_router_with_config(cfg: AppConfig) -> Router {
        http_router(test_daemon_state_with_config(cfg))
    }

    fn test_daemon_state_with_config(cfg: AppConfig) -> McpDaemonState {
        let cfg = Arc::new(cfg);
        let repository: Arc<dyn ConversationRepository> =
            Arc::new(InMemoryConversationRepository::default());
        let backend_router = Arc::new(
            BackendRepositoryRouter::from_preloaded_for_testing(
                cfg.clone(),
                QueryRuntime::new(),
                [("default".to_string(), repository)],
            )
            .expect("test backend router"),
        );
        McpDaemonState::new(cfg, backend_router)
    }

    async fn wait_for_owner_count(runtime: &QueryRuntime, expected: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while runtime.active_owner_count() != expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "query owner count did not reach {expected}; current count is {}",
                runtime.active_owner_count()
            )
        });
    }

    #[cfg(unix)]
    fn unique_socket_path(tag: &str) -> std::path::PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        std::path::PathBuf::from("/tmp").join(format!(
            "moraine-http-{tag}-{}-{nonce}.sock",
            std::process::id()
        ))
    }

    #[cfg(unix)]
    async fn connect_test_socket(path: &std::path::Path) -> tokio::net::UnixStream {
        for _ in 0..100 {
            if let Ok(stream) = tokio::net::UnixStream::connect(path).await {
                return stream;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("failed to connect to test socket {}", path.display());
    }
    fn request(body: &str) -> Request<Body> {
        Request::post("/mcp")
            .header(header::CONTENT_TYPE, "application/json; charset=utf-8")
            .header(header::ACCEPT, "application/json, text/event-stream")
            .body(Body::from(body.to_string()))
            .expect("request")
    }

    fn protocol_request(body: &str) -> Request<Body> {
        let mut request = request(body);
        request
            .headers_mut()
            .insert(MCP_PROTOCOL_VERSION, HeaderValue::from_static("2025-06-18"));
        request
    }

    #[tokio::test]
    async fn initialize_negotiates_supported_protocol_and_notifications_are_empty() {
        for version in SUPPORTED_PROTOCOL_VERSIONS {
            let body = format!(
                r#"{{"jsonrpc":"2.0","id":1,"method":"initialize","params":{{"protocolVersion":"{version}"}}}}"#
            );
            let response = test_router()
                .oneshot(request(&body))
                .await
                .expect("initialize response");
            assert_eq!(response.status(), StatusCode::OK);
            let body = to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("initialize body");
            let payload: Value = serde_json::from_slice(&body).expect("initialize JSON");
            assert_eq!(payload["result"]["protocolVersion"], *version);
        }

        let response = test_router()
            .oneshot(request(
                r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"unsupported"}}"#,
            ))
            .await
            .expect("fallback initialize response");
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("fallback initialize body");
        let payload: Value = serde_json::from_slice(&body).expect("fallback initialize JSON");
        assert_eq!(payload["result"]["protocolVersion"], "2024-11-05");

        let response = test_router()
            .oneshot(
                request(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#)
                    .map(|body| body),
            )
            .await
            .expect("notification response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let mut notification = request(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#);
        notification
            .headers_mut()
            .insert(MCP_PROTOCOL_VERSION, HeaderValue::from_static("2025-06-18"));
        let response = test_router()
            .oneshot(notification)
            .await
            .expect("notification response");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("notification body")
            .is_empty());
    }

    #[tokio::test]
    async fn configured_protocol_version_is_advertised_and_accepted() {
        let mut cfg = AppConfig::default();
        cfg.mcp.protocol_version = "custom-version".to_string();
        let router = test_router_with_config(cfg);
        let response = router
            .clone()
            .oneshot(request(
                r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"unsupported"}}"#,
            ))
            .await
            .expect("initialize response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("initialize body");
        let payload: Value = serde_json::from_slice(&body).expect("initialize JSON");
        assert_eq!(payload["result"]["protocolVersion"], "custom-version");

        let mut ping = request(r#"{"jsonrpc":"2.0","id":2,"method":"ping"}"#);
        ping.headers_mut().insert(
            MCP_PROTOCOL_VERSION,
            HeaderValue::from_static("custom-version"),
        );
        let response = router.oneshot(ping).await.expect("ping response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn default_http_state_has_no_client_project_and_reuses_backend_prewarm_gate() {
        let state = test_daemon_state_with_config(AppConfig::default());
        let backend = state
            .router
            .default_repository()
            .await
            .expect("default backend");
        let expected_gate = state
            .prewarm_gates
            .for_backend(backend.backend_name())
            .expect("backend prewarm gate");
        let app_state = state.default_app_state().await.expect("default app state");

        assert!(app_state.launch_dir.is_none());
        assert!(Arc::ptr_eq(&app_state.prewarm_started, &expected_gate));
    }

    #[tokio::test]
    async fn dropping_http_request_releases_shared_query_owner() {
        let mut cfg = AppConfig::default();
        cfg.mcp.max_parallel_requests = Some(1);
        let state = test_daemon_state_with_config(cfg);
        let held_permit = state
            .request_admission
            .try_register()
            .expect("shared admission slot")
            .acquire()
            .await
            .expect("shared admission permit");
        let runtime = state.router.query_runtime();
        let request_task = tokio::spawn(http_router(state).oneshot(protocol_request(
            r#"{"jsonrpc":"2.0","id":"blocked","method":"tools/call","params":{"name":"search_sessions","arguments":{"query":"nothing"}}}"#,
        )));

        wait_for_owner_count(&runtime, 1).await;
        request_task.abort();
        assert!(request_task
            .await
            .expect_err("aborted HTTP request task")
            .is_cancelled());
        wait_for_owner_count(&runtime, 0).await;
        drop(held_permit);
    }

    #[tokio::test]
    async fn query_runtime_shutdown_cancels_active_http_request() {
        let mut cfg = AppConfig::default();
        cfg.mcp.max_parallel_requests = Some(1);
        let state = test_daemon_state_with_config(cfg);
        let held_permit = state
            .request_admission
            .try_register()
            .expect("shared admission slot")
            .acquire()
            .await
            .expect("shared admission permit");
        let runtime = state.router.query_runtime();
        let request_task =
            tokio::spawn(http_router(state).oneshot(protocol_request(
                r#"{"jsonrpc":"2.0","id":"shutdown","method":"tools/call","params":{"name":"search_sessions","arguments":{"query":"nothing"}}}"#,
            )));

        wait_for_owner_count(&runtime, 1).await;
        runtime.close_and_drain().await;
        let response = request_task
            .await
            .expect("HTTP request task")
            .expect("HTTP response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        wait_for_owner_count(&runtime, 0).await;
        drop(held_permit);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn socket_and_http_share_queue_capacity_and_http_reports_exhaustion() {
        use tokio::io::AsyncWriteExt;

        let mut cfg = AppConfig::default();
        cfg.mcp.max_parallel_requests = Some(1);
        let state = test_daemon_state_with_config(cfg);
        let held_permit = state
            .request_admission
            .try_register()
            .expect("shared admission slot")
            .acquire()
            .await
            .expect("shared admission permit");
        let runtime = state.router.query_runtime();
        let socket_path = unique_socket_path("shared-admission");
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server_path = socket_path.clone();
        let socket_state = state.clone();
        let server = tokio::spawn(async move {
            run_socket_with_state(socket_state, server_path, async {
                let _ = shutdown_rx.await;
            })
            .await
        });

        let mut clients = Vec::with_capacity(MAX_QUEUED_REQUESTS);
        for id in 0..MAX_QUEUED_REQUESTS {
            let mut stream = connect_test_socket(&socket_path).await;
            let request = format!(
                r#"{{"jsonrpc":"2.0","id":"socket-{id}","method":"tools/call","params":{{"name":"search_sessions","arguments":{{"query":"nothing"}}}}}}"#
            );
            stream
                .write_all(format!("{request}\n").as_bytes())
                .await
                .expect("write queued socket request");
            clients.push(stream);
        }
        wait_for_owner_count(&runtime, MAX_QUEUED_REQUESTS).await;

        let response = http_router(state)
            .oneshot(protocol_request(
                r#"{"jsonrpc":"2.0","id":"http-full","method":"tools/call","params":{"name":"search_sessions","arguments":{"query":"nothing"}}}"#,
            ))
            .await
            .expect("HTTP queue-full response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("HTTP queue-full body");
        let payload: Value = serde_json::from_slice(&body).expect("HTTP queue-full JSON");
        assert_eq!(payload["result"]["isError"], true);
        assert_eq!(
            payload["result"]["structuredContent"]["error"]["code"],
            "busy"
        );
        assert_eq!(
            payload["result"]["structuredContent"]["error"]["details"]["reason"],
            "queue_full"
        );

        drop(clients);
        shutdown_tx.send(()).expect("request socket shutdown");
        server
            .await
            .expect("socket server task")
            .expect("clean socket shutdown");
        wait_for_owner_count(&runtime, 0).await;
        drop(held_permit);
        assert!(!socket_path.exists());
    }

    #[tokio::test]
    async fn tools_list_uses_json_response() {
        let mut rpc = request(r#"{"jsonrpc":"2.0","id":"tools","method":"tools/list"}"#);
        rpc.headers_mut()
            .insert(MCP_PROTOCOL_VERSION, HeaderValue::from_static("2025-06-18"));
        let response = test_router().oneshot(rpc).await.expect("tools response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE),
            Some(&HeaderValue::from_static("application/json"))
        );
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("tools body");
        let payload: Value = serde_json::from_slice(&body).expect("tools JSON");
        assert!(payload["result"]["tools"].as_array().is_some());
    }

    #[tokio::test]
    async fn tools_call_uses_the_shared_default_repository() {
        let response = test_router()
            .oneshot(protocol_request(
                r#"{"jsonrpc":"2.0","id":"search","method":"tools/call","params":{"name":"search_sessions","arguments":{"query":"nothing"}}}"#,
            ))
            .await
            .expect("tools/call response");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get("mcp-session-id").is_none());
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("tools/call body");
        let payload: Value = serde_json::from_slice(&body).expect("tools/call JSON");
        assert_eq!(payload["id"], "search");
        assert_eq!(payload["result"]["isError"], false);
        assert_eq!(
            payload["result"]["structuredContent"]["tool"],
            "search_sessions"
        );
    }

    #[tokio::test]
    async fn concurrent_posts_complete_independently() {
        let router = test_router();
        let first = router.clone().oneshot(protocol_request(
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#,
        ));
        let second = router.oneshot(protocol_request(
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#,
        ));
        let (first, second) = tokio::join!(first, second);
        for response in [
            first.expect("first response"),
            second.expect("second response"),
        ] {
            assert_eq!(response.status(), StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn rejects_invalid_http_contract_before_dispatch() {
        let response = test_router()
            .oneshot(
                Request::post("/mcp")
                    .header(header::ACCEPT, "application/json")
                    .body(Body::from("{}"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);

        let response = test_router()
            .oneshot(
                Request::post("/mcp")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::ACCEPT, "text/event-stream")
                    .body(Body::from("{}"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_ACCEPTABLE);

        let response = test_router()
            .oneshot(
                Request::post("/mcp")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::ACCEPT, "application/json; q=0.0, text/event-stream")
                    .body(Body::from("{}"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_ACCEPTABLE);

        for origin in ["http://127.0.0.1:5173", "http://[::1]:5173"] {
            let mut loopback =
                request(r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#);
            loopback.headers_mut().insert(
                header::ORIGIN,
                HeaderValue::from_str(origin).expect("loopback origin"),
            );
            let response = test_router()
                .oneshot(loopback)
                .await
                .expect("loopback response");
            assert_eq!(response.status(), StatusCode::OK, "{origin}");
        }

        let mut invalid_origin =
            request(r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#);
        invalid_origin.headers_mut().insert(
            header::ORIGIN,
            HeaderValue::from_static("https://example.com"),
        );
        let response = test_router()
            .oneshot(invalid_origin)
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let response = test_router()
            .oneshot(request("{"))
            .await
            .expect("malformed response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = test_router()
            .oneshot(request(
                r#"{"jsonrpc":"1.0","id":1,"method":"initialize","params":{}}"#,
            ))
            .await
            .expect("invalid JSON-RPC response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        for body in [
            r#"{"jsonrpc":"2.0","id":true,"method":"initialize","params":{}}"#,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":"invalid"}"#,
        ] {
            let response = test_router()
                .oneshot(request(body))
                .await
                .expect("invalid request-shape response");
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{body}");
        }

        let response = test_router()
            .oneshot(protocol_request(
                r#"{"jsonrpc":"2.0","id":null,"method":"ping"}"#,
            ))
            .await
            .expect("null-id response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("null-id body");
        let payload: Value = serde_json::from_slice(&body).expect("null-id JSON");
        assert!(payload
            .as_object()
            .is_some_and(|object| object.contains_key("id")));
        assert!(payload["id"].is_null());

        let mut unsupported = request(r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#);
        unsupported
            .headers_mut()
            .insert(MCP_PROTOCOL_VERSION, HeaderValue::from_static("2000-01-01"));
        let response = test_router()
            .oneshot(unsupported)
            .await
            .expect("unsupported protocol response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = test_router()
            .oneshot(protocol_request(
                r#"{"jsonrpc":"2.0","id":"unknown","method":"unknown"}"#,
            ))
            .await
            .expect("JSON-RPC error response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("JSON-RPC error body");
        let payload: Value = serde_json::from_slice(&body).expect("JSON-RPC error JSON");
        assert_eq!(payload["error"]["code"], -32601);

        let response = test_router()
            .oneshot(Request::get("/mcp").body(Body::empty()).expect("request"))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }
}
