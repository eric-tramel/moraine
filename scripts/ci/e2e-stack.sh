#!/usr/bin/env bash
set -euo pipefail

E2E_SUCCESS=0

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "required command not found: $1" >&2
    exit 1
  fi
}

pick_open_port() {
  local python_bin="$1"
  "$python_bin" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

pick_clickhouse_http_port() {
  local python_bin="$1"
  "$python_bin" -c '
import random
import socket
import sys

TCP_OFFSET = 877
INTERSERVER_OFFSET = 886

for _ in range(500):
    http_port = random.randint(20000, 60000 - INTERSERVER_OFFSET)
    ports = [http_port, http_port + TCP_OFFSET, http_port + INTERSERVER_OFFSET]
    sockets = []
    try:
        for port in ports:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("127.0.0.1", port))
            sockets.append(sock)
    except OSError:
        for sock in sockets:
            sock.close()
        continue
    for sock in sockets:
        sock.close()
    print(http_port)
    sys.exit(0)

print("failed to find available ClickHouse ports", file=sys.stderr)
sys.exit(1)
'
}

json_ok_true() {
  local python_bin="$1"
  "$python_bin" -c 'import json,sys; data=json.load(sys.stdin); sys.exit(0 if data.get("ok") is True else 1)'
}

wait_for_endpoint_ok() {
  local python_bin="$1"
  local url="$2"
  local timeout_seconds="${3:-90}"
  local started
  started="$(date +%s)"

  while true; do
    local now
    now="$(date +%s)"
    if (( now - started >= timeout_seconds )); then
      echo "timed out waiting for endpoint: $url" >&2
      return 1
    fi

    local body=""
    if body="$(curl -sS --max-time 3 "$url" 2>/dev/null)"; then
      if printf '%s' "$body" | json_ok_true "$python_bin"; then
        return 0
      fi
    fi

    sleep 2
  done
}

wait_for_clickhouse_count() {
  local clickhouse_url="$1"
  local query="$2"
  local timeout_seconds="${3:-120}"
  local started
  started="$(date +%s)"

  while true; do
    local now
    now="$(date +%s)"
    if (( now - started >= timeout_seconds )); then
      echo "timed out waiting for ClickHouse query to become non-zero: $query" >&2
      return 1
    fi

    local count
    count="$(curl -sS --max-time 3 "$clickhouse_url" --data-binary "$query" 2>/dev/null | tr -d '[:space:]' || true)"
    if [[ -n "$count" && "$count" =~ ^[0-9]+$ ]] && (( count > 0 )); then
      return 0
    fi

    sleep 2
  done
}

cleanup_e2e() {
  local cortexctl_bin="$1"
  local config_path="$2"
  local runtime_root="$3"
  local tmp_root="$4"

  if [[ "${E2E_SUCCESS}" -ne 1 ]]; then
    echo "[e2e] failure diagnostics (tmp root: $tmp_root)" >&2
    if [[ -d "$runtime_root/logs" ]]; then
      find "$runtime_root/logs" -maxdepth 1 -type f -print >&2 || true
      for log in "$runtime_root/logs"/*.log; do
        [[ -f "$log" ]] || continue
        echo "--- tail $log ---" >&2
        tail -n 80 "$log" >&2 || true
      done
    fi
    for ch_log in \
      "$runtime_root/clickhouse/log/clickhouse-server.log" \
      "$runtime_root/clickhouse/log/clickhouse-server.err.log"; do
      if [[ -f "$ch_log" ]]; then
        echo "--- tail $ch_log ---" >&2
        tail -n 120 "$ch_log" >&2 || true
      fi
    done
  fi

  "$cortexctl_bin" down --config "$config_path" >/dev/null 2>&1 || true

  if [[ "${E2E_SUCCESS}" -eq 1 && "${KEEP_E2E_TMP:-0}" != "1" ]]; then
    rm -rf "$tmp_root"
  else
    echo "[e2e] keeping temp directory: $tmp_root" >&2
  fi
}

main() {
  local repo_root
  repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
  local cortexctl_bin="${CORTEXCTL_BIN:-$repo_root/target/debug/cortexctl}"
  local python_bin="${PYTHON_BIN:-python3}"
  local monitor_port="${MONITOR_PORT:-}"
  local base_keyword="${CORTEX_TEST_KEYWORD:-portable_ci_keyword}"
  base_keyword="${base_keyword//[^[:alnum:]_]/_}"
  base_keyword="$(printf '%s' "$base_keyword" | tr '[:upper:]' '[:lower:]')"
  if [[ -z "$base_keyword" ]]; then
    base_keyword="portable_ci_keyword"
  fi
  local run_stamp
  run_stamp="$(date +%s)_$$_$RANDOM"
  local codex_keyword="${base_keyword}_codex_${run_stamp}"
  local claude_keyword="${base_keyword}_claude_${run_stamp}"
  local clickhouse_database="cortex"
  local codex_session_suffix
  codex_session_suffix="$(printf '%06x%06x' "$RANDOM" "$RANDOM")"
  local codex_session_id="00000000-0000-4000-8000-${codex_session_suffix}"
  local claude_session_suffix
  claude_session_suffix="$(printf '%06x%06x' "$RANDOM" "$RANDOM")"
  local claude_session_id="00000000-0000-4000-8000-${claude_session_suffix}"
  local codex_trace_marker="mcp_codex_trace_marker_${run_stamp}"
  local claude_trace_marker="mcp_claude_trace_marker_${run_stamp}"

  need_cmd curl
  need_cmd "$python_bin"

  if [[ -z "$monitor_port" ]]; then
    monitor_port="$(pick_open_port "$python_bin")"
  fi

  local clickhouse_http_port
  clickhouse_http_port="$(pick_clickhouse_http_port "$python_bin")"
  local clickhouse_url="http://127.0.0.1:${clickhouse_http_port}"

  if [[ ! -x "$cortexctl_bin" ]]; then
    echo "missing cortexctl binary: $cortexctl_bin" >&2
    echo "build first: cargo build --workspace --locked" >&2
    exit 1
  fi

  local tmp_root
  tmp_root="$(mktemp -d)"
  local fixtures_root="$tmp_root/fixtures"
  local runtime_root="$tmp_root/runtime"
  local config_path="$tmp_root/cortex-ci.toml"
  local codex_fixture_file="$fixtures_root/codex/sessions/2026/02/16/session-${codex_session_id}.jsonl"
  local claude_fixture_file="$fixtures_root/claude/projects/e2e/session-${claude_session_id}.jsonl"

  mkdir -p "$(dirname "$codex_fixture_file")"
  mkdir -p "$(dirname "$claude_fixture_file")"
  mkdir -p "$runtime_root"

  cat > "$codex_fixture_file" <<EOF
{"timestamp":"2026-02-16T12:00:00.000Z","type":"session_meta","payload":{"id":"${codex_session_id}"}}
{"timestamp":"2026-02-16T12:00:01.000Z","type":"turn_context","payload":{"turn_id":"1","model":"gpt-5.3-codex"}}
{"timestamp":"2026-02-16T12:00:02.000Z","type":"response_item","payload":{"type":"message","role":"user","id":"msg-user-${run_stamp}","content":[{"type":"text","text":"local e2e codex user prompt ${codex_keyword}"}],"phase":"completed"}}
{"timestamp":"2026-02-16T12:00:03.000Z","type":"response_item","payload":{"type":"message","role":"assistant","id":"msg-assistant-${run_stamp}","content":[{"type":"text","text":"local e2e codex assistant reply ${codex_keyword} ${codex_trace_marker}"}],"phase":"completed"}}
EOF

  cat > "$claude_fixture_file" <<EOF
{"type":"user","sessionId":"${claude_session_id}","uuid":"claude-user-${run_stamp}","timestamp":"2026-02-16T12:00:04.000Z","message":{"role":"user","content":[{"type":"text","text":"local e2e claude user prompt ${claude_keyword}"}]}}
{"type":"assistant","sessionId":"${claude_session_id}","uuid":"claude-assistant-${run_stamp}","parentUuid":"claude-user-${run_stamp}","requestId":"req-${run_stamp}","timestamp":"2026-02-16T12:00:05.000Z","message":{"model":"claude-opus-4-5-20251101","role":"assistant","usage":{"input_tokens":9,"output_tokens":5,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"service_tier":"standard"},"content":[{"type":"text","text":"local e2e claude assistant reply ${claude_keyword} ${claude_trace_marker}"}]}}
EOF

  cat > "$config_path" <<EOF
[clickhouse]
url = "${clickhouse_url}"
database = "${clickhouse_database}"

[monitor]
host = "127.0.0.1"
port = ${monitor_port}

[ingest]
backfill_on_start = true
heartbeat_interval_seconds = 1.0
flush_interval_seconds = 0.2
reconcile_interval_seconds = 2.0
state_dir = "${tmp_root}/ingest-state"

[[ingest.sources]]
name = "ci-codex"
provider = "codex"
enabled = true
glob = "${fixtures_root}/codex/sessions/**/*.jsonl"
watch_root = "${fixtures_root}/codex/sessions"

[[ingest.sources]]
name = "ci-claude"
provider = "claude"
enabled = true
glob = "${fixtures_root}/claude/projects/**/*.jsonl"
watch_root = "${fixtures_root}/claude/projects"

[runtime]
root_dir = "${runtime_root}"
logs_dir = "logs"
pids_dir = "run"
service_bin_dir = "${repo_root}/target/debug"
managed_clickhouse_dir = "${runtime_root}/managed-clickhouse"
clickhouse_auto_install = true
clickhouse_start_timeout_seconds = 90.0
start_monitor_on_up = true
start_mcp_on_up = false
EOF

  trap "cleanup_e2e \"$cortexctl_bin\" \"$config_path\" \"$runtime_root\" \"$tmp_root\"" EXIT

  echo "[e2e] run id: ${run_stamp}"
  echo "[e2e] clickhouse url: ${clickhouse_url}"
  echo "[e2e] clickhouse db: ${clickhouse_database}"
  echo "[e2e] codex fixture: ${codex_fixture_file}"
  echo "[e2e] claude fixture: ${claude_fixture_file}"

  echo "[e2e] installing managed ClickHouse"
  "$cortexctl_bin" clickhouse install --config "$config_path"

  echo "[e2e] starting stack"
  "$cortexctl_bin" up --config "$config_path"

  echo "[e2e] waiting for monitor health"
  wait_for_endpoint_ok "$python_bin" "http://127.0.0.1:${monitor_port}/api/health" 120

  echo "[e2e] waiting for ingest heartbeat + indexed content"
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.ingest_heartbeats" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_documents WHERE positionCaseInsensitiveUTF8(text_content, '${codex_keyword}') > 0" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_postings WHERE term = '${codex_keyword}'" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_documents WHERE positionCaseInsensitiveUTF8(text_content, '${claude_keyword}') > 0" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_postings WHERE term = '${claude_keyword}'" 120

  echo "[e2e] checking monitor API routes"
  for path in /api/health /api/status /api/analytics /api/web-searches; do
    local body
    body="$(curl -fsS "http://127.0.0.1:${monitor_port}${path}")"
    printf '%s' "$body" | json_ok_true "$python_bin"
  done

  echo "[e2e] checking MCP initialize/tools/search/open (codex)"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --cortexctl "$cortexctl_bin" \
    --config "$config_path" \
    --query "$codex_keyword" \
    --expect-session-id "$codex_session_id" \
    --expect-source-file "$codex_fixture_file" \
    --expect-open-text "$codex_trace_marker"

  echo "[e2e] checking MCP initialize/tools/search/open (claude)"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --cortexctl "$cortexctl_bin" \
    --config "$config_path" \
    --query "$claude_keyword" \
    --expect-session-id "$claude_session_id" \
    --expect-source-file "$claude_fixture_file" \
    --expect-open-text "$claude_trace_marker"

  echo "[e2e] final status"
  "$cortexctl_bin" status --config "$config_path"
  E2E_SUCCESS=1
}

main "$@"
