#!/usr/bin/env bash
set -euo pipefail

E2E_SUCCESS=0

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "required command not found: $1" >&2
    exit 1
  fi
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
  local query="$1"
  local timeout_seconds="${2:-120}"
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
    count="$(curl -sS --max-time 3 "http://127.0.0.1:8123" --data-binary "$query" 2>/dev/null | tr -d '[:space:]' || true)"
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
  local monitor_port="${MONITOR_PORT:-18080}"
  local keyword="${CORTEX_TEST_KEYWORD:-portable_ci_keyword}"

  need_cmd curl
  need_cmd "$python_bin"

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
  local fixture_file="$fixtures_root/codex/sessions/2026/02/16/session-ci.jsonl"

  mkdir -p "$(dirname "$fixture_file")"
  mkdir -p "$runtime_root"

  cat > "$fixture_file" <<EOF
{"timestamp":"2026-02-16T12:00:00.000Z","type":"response_item","payload":{"type":"message","role":"assistant","id":"msg-ci-1","content":[{"type":"text","text":"${keyword} alpha beta gamma"}],"phase":"completed"}}
EOF

  cat > "$config_path" <<EOF
[monitor]
host = "127.0.0.1"
port = ${monitor_port}

[ingest]
backfill_on_start = true
heartbeat_interval_seconds = 1.0
flush_interval_seconds = 0.2
reconcile_interval_seconds = 2.0

[[ingest.sources]]
name = "ci-codex"
provider = "codex"
enabled = true
glob = "${fixtures_root}/codex/sessions/**/*.jsonl"
watch_root = "${fixtures_root}/codex/sessions"

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

  echo "[e2e] installing managed ClickHouse"
  "$cortexctl_bin" clickhouse install --config "$config_path"

  echo "[e2e] starting stack"
  "$cortexctl_bin" up --config "$config_path"

  echo "[e2e] waiting for monitor health"
  wait_for_endpoint_ok "$python_bin" "http://127.0.0.1:${monitor_port}/api/health" 120

  echo "[e2e] waiting for ingest heartbeat + indexed content"
  wait_for_clickhouse_count "SELECT count() FROM cortex.ingest_heartbeats" 120
  wait_for_clickhouse_count "SELECT count() FROM cortex.search_documents WHERE positionCaseInsensitiveUTF8(text_content, '${keyword}') > 0" 120
  wait_for_clickhouse_count "SELECT count() FROM cortex.search_postings WHERE term = '${keyword}'" 120

  echo "[e2e] checking monitor API routes"
  for path in /api/health /api/status /api/analytics /api/web-searches; do
    local body
    body="$(curl -fsS "http://127.0.0.1:${monitor_port}${path}")"
    printf '%s' "$body" | json_ok_true "$python_bin"
  done

  echo "[e2e] checking MCP initialize/tools/search/open"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --cortexctl "$cortexctl_bin" \
    --config "$config_path" \
    --query "$keyword"

  echo "[e2e] final status"
  "$cortexctl_bin" status --config "$config_path"
  E2E_SUCCESS=1
}

main "$@"
