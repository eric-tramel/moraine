#!/usr/bin/env bash
set -euo pipefail

# Cache behavior assertions below inspect info-level backend markers. Keep the
# child service log level deterministic instead of inheriting a quieter host
# setting such as RUST_LOG=warn.
export RUST_LOG=info

E2E_SUCCESS=0

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "required command not found: $1" >&2
    exit 1
  fi
}

monitor_frontend_needs_build() {
  local repo_root="$1"
  local dist_index="$repo_root/web/monitor/dist/index.html"
  if [[ ! -f "$dist_index" ]]; then
    return 0
  fi

  local watch_paths=(
    "$repo_root/web/monitor/src"
    "$repo_root/web/monitor/index.html"
    "$repo_root/web/monitor/package.json"
    "$repo_root/web/monitor/tsconfig.app.json"
    "$repo_root/web/monitor/tsconfig.node.json"
    "$repo_root/web/monitor/vite.config.ts"
  )

  local path
  for path in "${watch_paths[@]}"; do
    if [[ -d "$path" ]]; then
      if find "$path" -type f -newer "$dist_index" -print -quit | grep -q .; then
        return 0
      fi
    elif [[ -f "$path" && "$path" -nt "$dist_index" ]]; then
      return 0
    fi
  done

  return 1
}

ensure_monitor_frontend() {
  local repo_root="$1"
  local dist_dir="$repo_root/web/monitor/dist"
  if ! monitor_frontend_needs_build "$repo_root"; then
    return 0
  fi

  need_cmd bun
  (
    cd "$repo_root/web/monitor"
    bun install --frozen-lockfile
    bun run build
  )
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

assert_monitor_api_alias() {
  local python_bin="$1"
  local base_url="$2"
  local tmp_root="$3"
  local canonical_path="$4"
  local legacy_path="$5"
  local canonical_body_file
  local legacy_body_file
  canonical_body_file="$(mktemp "$tmp_root/canonical-api.XXXXXX")"
  legacy_body_file="$(mktemp "$tmp_root/legacy-api.XXXXXX")"

  local canonical_status
  local legacy_status
  # Deliberately omit --location: compatibility aliases must respond directly,
  # not pass by redirecting curl to the canonical route.
  if ! canonical_status="$(curl -sS --max-time 30 -o "$canonical_body_file" -w '%{http_code}' -- "${base_url}${canonical_path}")"; then
    echo "[e2e] canonical monitor request failed: ${canonical_path}" >&2
    rm -f "$canonical_body_file" "$legacy_body_file"
    return 1
  fi
  if ! legacy_status="$(curl -sS --max-time 30 -o "$legacy_body_file" -w '%{http_code}' -- "${base_url}${legacy_path}")"; then
    echo "[e2e] legacy monitor request failed: ${legacy_path}" >&2
    rm -f "$canonical_body_file" "$legacy_body_file"
    return 1
  fi

  if [[ "$canonical_status" != "$legacy_status" ]]; then
    echo "[e2e] monitor alias status mismatch: ${legacy_path} (${legacy_status}) != ${canonical_path} (${canonical_status})" >&2
    rm -f "$canonical_body_file" "$legacy_body_file"
    return 1
  fi

  if [[ "$canonical_status" != "200" ]]; then
    echo "[e2e] monitor alias returned unexpected status: ${canonical_path} (${canonical_status})" >&2
    rm -f "$canonical_body_file" "$legacy_body_file"
    return 1
  fi
  if ! json_ok_true "$python_bin" <"$canonical_body_file"; then
    echo "[e2e] canonical monitor response is not successful JSON: ${canonical_path}" >&2
    rm -f "$canonical_body_file" "$legacy_body_file"
    return 1
  fi
  if ! json_ok_true "$python_bin" <"$legacy_body_file"; then
    echo "[e2e] legacy monitor response is not successful JSON: ${legacy_path}" >&2
    rm -f "$canonical_body_file" "$legacy_body_file"
    return 1
  fi

  rm -f "$canonical_body_file" "$legacy_body_file"
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

clickhouse_scalar() {
  local clickhouse_url="$1"
  local query="$2"

  curl -fsS --max-time 10 "$clickhouse_url" --data-binary "$query" | tr -d '\r\n'
}

wait_for_clickhouse_scalar_stable() {
  local clickhouse_url="$1"
  local query="$2"
  local timeout_seconds="${3:-30}"
  local required_observations="${4:-3}"
  local started
  local previous=""
  local stable_observations=0
  started="$(date +%s)"

  while true; do
    local now
    now="$(date +%s)"
    if (( now - started >= timeout_seconds )); then
      echo "timed out waiting for stable ClickHouse scalar: $query" >&2
      return 1
    fi

    local actual
    actual="$(clickhouse_scalar "$clickhouse_url" "$query" 2>/dev/null || true)"
    if [[ -n "$actual" && "$actual" == "$previous" ]]; then
      stable_observations=$((stable_observations + 1))
    else
      previous="$actual"
      stable_observations=1
    fi
    if (( stable_observations >= required_observations )); then
      printf '%s' "$actual"
      return 0
    fi

    sleep 2
  done
}

assert_clickhouse_scalar() {
  local clickhouse_url="$1"
  local label="$2"
  local query="$3"
  local expected="$4"
  local actual

  actual="$(clickhouse_scalar "$clickhouse_url" "$query")"
  if [[ "$actual" != "$expected" ]]; then
    echo "[e2e] ClickHouse assertion failed: $label" >&2
    echo "[e2e] expected: $expected" >&2
    echo "[e2e] actual:   $actual" >&2
    echo "[e2e] query:    $query" >&2
    return 1
  fi
}

assert_clickhouse_count() {
  local clickhouse_url="$1"
  local label="$2"
  local query="$3"
  local expected="$4"

  assert_clickhouse_scalar "$clickhouse_url" "$label" "$query" "$expected"
}

read_live_pid_file() {
  local path="$1"
  local label="$2"
  local pid=""

  if [[ ! -f "$path" ]]; then
    echo "[e2e] missing ${label} PID file: $path" >&2
    return 1
  fi
  IFS= read -r pid < "$path" || true
  if [[ ! "$pid" =~ ^[0-9]+$ ]]; then
    echo "[e2e] invalid ${label} PID in $path: ${pid:-<empty>}" >&2
    return 1
  fi
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "[e2e] ${label} PID is not running: $pid" >&2
    return 1
  fi
  printf '%s\n' "$pid"
}

assert_process_binary() {
  local pid="$1"
  local expected_binary="$2"
  local label="$3"
  local command_line

  if ! command_line="$(ps -ww -p "$pid" -o command= 2>/dev/null)"; then
    echo "[e2e] could not inspect ${label} process command for PID $pid" >&2
    return 1
  fi
  case "$command_line" in
    "$expected_binary" | "$expected_binary "*) ;;
    *)
      echo "[e2e] ${label} PID $pid is not executing the expected binary" >&2
      echo "[e2e] expected: $expected_binary" >&2
      echo "[e2e] actual:   $command_line" >&2
      return 1
      ;;
  esac
}

assert_canonical_pid_layout() {
  local pids_dir="$1"
  local service
  for service in clickhouse ingest backend; do
    read_live_pid_file "$pids_dir/${service}.pid" "$service" >/dev/null
  done

  local retired
  for retired in monitor.pid mcp.pid; do
    if [[ -e "$pids_dir/$retired" ]]; then
      echo "[e2e] retired PID file must not exist: $pids_dir/$retired" >&2
      return 1
    fi
  done

  local pid_path
  for pid_path in "$pids_dir"/*.pid; do
    [[ -f "$pid_path" ]] || continue
    case "${pid_path##*/}" in
      clickhouse.pid | ingest.pid | backend.pid) ;;
      *)
        echo "[e2e] unexpected managed PID file: $pid_path" >&2
        return 1
        ;;
    esac
  done
}

assert_frontend_asset_served() {
  local python_bin="$1"
  local base_url="$2"
  local work_dir="$3"
  local index_path="$work_dir/frontend-index.html"
  local asset_path="$work_dir/frontend-asset"
  local asset_url

  curl -fsS --max-time 10 "$base_url/" -o "$index_path"
  asset_url="$("$python_bin" - "$index_path" "$base_url" <<'PY'
from html.parser import HTMLParser
from pathlib import Path
import sys
from urllib.parse import urljoin, urlsplit

index_path, base_url = sys.argv[1:3]
document = Path(index_path).read_text(encoding="utf-8")
if "<html" not in document.lower() and "<!doctype html" not in document.lower():
    raise SystemExit(f"{base_url}/ did not return an HTML document")


class AssetParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.references: list[str] = []

    def handle_starttag(self, tag, attrs) -> None:
        attribute = "src" if tag in {"script", "img"} else "href" if tag == "link" else None
        if attribute is None:
            return
        for name, value in attrs:
            if name == attribute and value:
                self.references.append(value)


parser = AssetParser()
parser.feed(document)
base = urlsplit(base_url)
for reference in parser.references:
    resolved = urlsplit(urljoin(f"{base_url}/", reference))
    if resolved.scheme not in {"http", "https"} or resolved.netloc != base.netloc:
        continue
    if resolved.path in {"", "/"}:
        continue
    print(resolved.geturl())
    break
else:
    raise SystemExit(f"{base_url}/ did not reference a same-origin static asset")
PY
)"
  curl -fsS --max-time 10 "$asset_url" -o "$asset_path"
  if [[ ! -s "$asset_path" ]]; then
    echo "[e2e] referenced frontend asset was empty: $asset_url" >&2
    return 1
  fi
}

wait_for_mcp_cache_sequence() {
  local python_bin="$1"
  local backend_log="$2"
  local baseline_bytes="$3"
  local timeout_seconds="${4:-20}"

  "$python_bin" - "$backend_log" "$baseline_bytes" "$timeout_seconds" <<'PY'
from pathlib import Path
import re
import sys
import time

log_path = Path(sys.argv[1])
baseline_bytes = int(sys.argv[2])
deadline = time.monotonic() + float(sys.argv[3])
ansi = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
hit_field = re.compile(r"\bcache_hit=(true|false)\b")
relevant: list[str] = []

while time.monotonic() < deadline:
    relevant = []
    observed: list[bool] = []
    if log_path.is_file():
        payload = log_path.read_bytes()
        if len(payload) < baseline_bytes:
            raise SystemExit(
                f"{log_path} shrank below the cache-event baseline: "
                f"{len(payload)} < {baseline_bytes}"
            )
        for raw_line in payload[baseline_bytes:].decode(
            "utf-8", errors="replace"
        ).splitlines():
            line = ansi.sub("", raw_line)
            if "mcp_search_cache" not in line:
                continue
            relevant.append(line)
            hit_match = hit_field.search(line)
            if hit_match is None:
                raise SystemExit(
                    "post-baseline mcp_search_cache marker omitted boolean cache_hit: "
                    f"{line}"
                )
            observed.append(hit_match.group(1) == "true")
    if len(observed) >= 2:
        if observed[:2] == [False, True]:
            raise SystemExit(0)
        raise SystemExit(
            "post-baseline mcp_search_cache markers were not ordered miss then hit: "
            f"{observed}; lines={relevant[-10:]}"
        )
    time.sleep(0.2)

raise SystemExit(
    "timed out waiting for post-baseline mcp_search_cache miss/hit markers in "
    f"{log_path}; lines={relevant[-10:]}"
)
PY
}

unix_socket_connectable() {
  local python_bin="$1"
  local socket_path="$2"

  "$python_bin" - "$socket_path" <<'PY'
import socket
import sys

client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
client.settimeout(0.5)
try:
    client.connect(sys.argv[1])
except OSError:
    raise SystemExit(1)
finally:
    client.close()
PY
}

wait_for_backend_crash() {
  local python_bin="$1"
  local backend_pid="$2"
  local socket_path="$3"
  local http_url="$4"
  local timeout_seconds="${5:-30}"
  local started
  started="$(date +%s)"

  while true; do
    local pid_live=0
    local socket_present=0
    local socket_connectable=0
    local http_live=0
    kill -0 "$backend_pid" 2>/dev/null && pid_live=1
    [[ -S "$socket_path" ]] && socket_present=1
    if unix_socket_connectable "$python_bin" "$socket_path"; then
      socket_connectable=1
    fi
    if curl -fsS --max-time 1 "$http_url/" >/dev/null 2>&1; then
      http_live=1
    fi

    if (( pid_live == 0 && socket_present == 1 && socket_connectable == 0 && http_live == 0 )); then
      return 0
    fi

    local now
    now="$(date +%s)"
    if (( now - started >= timeout_seconds )); then
      echo "[e2e] timed out waiting for abrupt backend loss" >&2
      echo "[e2e] pid=$backend_pid pid_live=$pid_live socket_present=$socket_present socket_connectable=$socket_connectable http=$http_live" >&2
      return 1
    fi
    sleep 0.2
  done
}

cleanup_e2e() {
  local moraine_bin="$1"
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

  "$moraine_bin" down --config "$config_path" >/dev/null 2>&1 || true

  if [[ "${E2E_SUCCESS}" -eq 1 && "${KEEP_E2E_TMP:-0}" != "1" ]]; then
    rm -rf "$tmp_root"
  else
    echo "[e2e] keeping temp directory: $tmp_root" >&2
  fi
}

main() {
  local repo_root
  repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
  local moraine_bin="${MORAINECTL_BIN:-$repo_root/target/debug/moraine}"
  local service_bin_dir="${MORAINE_SERVICE_BIN_DIR:-$repo_root/target/debug}"
  local python_bin="${PYTHON_BIN:-python3}"
  local monitor_port="${MONITOR_PORT:-}"
  local base_keyword="${MORAINE_TEST_KEYWORD:-portable_ci_keyword}"
  base_keyword="${base_keyword//[^[:alnum:]_]/_}"
  base_keyword="$(printf '%s' "$base_keyword" | tr '[:upper:]' '[:lower:]')"
  if [[ -z "$base_keyword" ]]; then
    base_keyword="portable_ci_keyword"
  fi
  local run_stamp
  run_stamp="$(date +%s)_$$_$RANDOM"
  local codex_keyword="${base_keyword}_codex_${run_stamp}"
  local claude_keyword="${base_keyword}_claude_${run_stamp}"
  local kimi_keyword="${base_keyword}_kimi_${run_stamp}"
  local qwen_keyword="${base_keyword}_qwen_${run_stamp}"
  local kiro_keyword="${base_keyword}_kiro_${run_stamp}"
  local cursor_keyword="${base_keyword}_cursor_${run_stamp}"
  local cursor_fallback_keyword="${base_keyword}_cursor_fallback_${run_stamp}"
  local cursor_sqlite_keyword="${base_keyword}_cursorsqlite_${run_stamp}"
  local nac_keyword="${base_keyword}_nac_${run_stamp}"
  local nac_live_keyword="${base_keyword}_nac_live_${run_stamp}"
  local nac_worker_keyword="${base_keyword}_nac_worker_${run_stamp}"
  local nac_replacement_keyword="${base_keyword}_nac_replacement_${run_stamp}"
  local nac_mcp_sentinel="${base_keyword}_nac_mcp_only_${run_stamp}"
  local nac_remote_keyword="${base_keyword}_nac_remote_${run_stamp}"
  local cursor_sqlite_live_keyword="${base_keyword}_cursorsqlite_live_${run_stamp}"
  local pi_keyword="${base_keyword}_pi_${run_stamp}"
  local hermes_keyword="${base_keyword}_hermes_trajectory_${run_stamp}"
  local hermes_session_keyword="${base_keyword}_hermes_session_${run_stamp}"
  local clickhouse_database="moraine"
  local routed_clickhouse_database="moraine_routed"
  local codex_session_suffix
  codex_session_suffix="$(printf '%06x%06x' "$RANDOM" "$RANDOM")"
  local codex_session_id="00000000-0000-4000-8000-${codex_session_suffix}"
  local codex_multi_path_session_id="00000000-0000-4000-8001-${codex_session_suffix}"
  local codex_nested_path_session_id="00000000-0000-4000-8002-${codex_session_suffix}"
  local nac_session_id="nac-parent-${run_stamp}"
  local nac_local_worker_id="nac-local-worker-${run_stamp}"
  local nac_remote_worker_id="nac-remote-worker-${run_stamp}"
  local nac_tool_call_id="nac-tool-${run_stamp}"
  local nac_secret_sentinel="NAC_FIXTURE_SECRET_${run_stamp}"
  local claude_session_suffix
  claude_session_suffix="$(printf '%06x%06x' "$RANDOM" "$RANDOM")"
  local claude_session_id="00000000-0000-4000-8000-${claude_session_suffix}"
  local kimi_session_id="kimi-${run_stamp}"
  local kimi_raw_session_id="kimi-cli:${kimi_session_id}"
  local qwen_session_id="qwen-${run_stamp}"
  local kiro_session_suffix
  kiro_session_suffix="$(printf '%06x%06x' "$RANDOM" "$RANDOM")"
  local kiro_session_id="00000000-0000-4000-8000-${kiro_session_suffix}"
  local kiro_session_title="Kiro e2e ${kiro_keyword}"
  local kiro_updated_session_title="Updated Kiro e2e ${kiro_keyword}"
  local kiro_cwd="/workspace/kiro-e2e"
  local cursor_session_suffix
  cursor_session_suffix="$(printf '%06x%06x' "$RANDOM" "$RANDOM")"
  local cursor_session_id="00000000-0000-4000-8000-${cursor_session_suffix}"
  local cursor_fallback_session_suffix
  cursor_fallback_session_suffix="$(printf '%06x%06x' "$RANDOM" "$RANDOM")"
  local cursor_fallback_session_id="00000000-0000-4000-8000-${cursor_fallback_session_suffix}"
  # The cursor_sqlite session id is the composer uuid: every event the poller
  # synthesizes for a composerData/bubbleId row carries it as session_id.
  local cursor_sqlite_session_suffix
  cursor_sqlite_session_suffix="$(printf '%06x%06x' "$RANDOM" "$RANDOM")"
  local cursor_sqlite_session_id="00000000-0000-4000-8000-${cursor_sqlite_session_suffix}"
  local cursor_sqlite_session_title="Cursor sqlite e2e ${cursor_sqlite_keyword}"
  local pi_session_suffix
  pi_session_suffix="$(printf '%06x%06x' "$RANDOM" "$RANDOM")"
  local pi_session_id="00000000-0000-4000-8000-${pi_session_suffix}"
  local hermes_session_id="session_${run_stamp}"
  local hermes_raw_session_id="hermes:${hermes_session_id}"
  local codex_trace_marker="mcp_codex_trace_marker_${run_stamp}"
  local claude_trace_marker="mcp_claude_trace_marker_${run_stamp}"
  local kimi_trace_marker="mcp_kimi_trace_marker_${run_stamp}"
  local qwen_trace_marker="mcp_qwen_trace_marker_${run_stamp}"
  local kiro_trace_marker="mcp_kiro_trace_marker_${run_stamp}"
  local cursor_trace_marker="mcp_cursor_trace_marker_${run_stamp}"
  local cursor_sqlite_trace_marker="mcp_cursor_sqlite_trace_marker_${run_stamp}"
  local pi_trace_marker="mcp_pi_trace_marker_${run_stamp}"
  local hermes_trace_marker="mcp_hermes_trace_marker_${run_stamp}"
  local hermes_session_trace_marker="mcp_hermes_session_trace_marker_${run_stamp}"
  local qwen_self_keyword="mcp_qwen_self_keyword_${run_stamp}"

  need_cmd curl
  need_cmd "$python_bin"
  need_cmd ps

  # Cache assertions below consume info-level events from the managed backend.
  # Keep the test deterministic when the caller has a stricter global filter.
  export RUST_LOG=info

  if [[ -z "$monitor_port" ]]; then
    monitor_port="$(pick_open_port "$python_bin")"
  fi

  local clickhouse_http_port
  clickhouse_http_port="$(pick_clickhouse_http_port "$python_bin")"
  local clickhouse_url="http://127.0.0.1:${clickhouse_http_port}"

  if [[ ! -x "$moraine_bin" ]]; then
    echo "missing moraine binary: $moraine_bin" >&2
    echo "build first: cargo build --workspace --locked" >&2
    exit 1
  fi

  if [[ ! -d "$service_bin_dir" ]]; then
    echo "missing service binary directory: $service_bin_dir" >&2
    exit 1
  fi
  service_bin_dir="$(cd "$service_bin_dir" && pwd -P)"
  export MORAINE_SERVICE_BIN_DIR="$service_bin_dir"
  local expected_ingest_bin="$service_bin_dir/moraine-ingest"
  local expected_backend_bin="$service_bin_dir/moraine-mcp"
  if [[ ! -x "$expected_ingest_bin" ]]; then
    echo "missing ingest service binary: $expected_ingest_bin" >&2
    exit 1
  fi
  if [[ ! -x "$expected_backend_bin" ]]; then
    echo "missing backend service binary: $expected_backend_bin" >&2
    exit 1
  fi

  local tmp_root
  tmp_root="$(mktemp -d)"
  # Resolve symlinks (macOS mktemp returns /var/..., a symlink to
  # /private/var/...) so the fixture paths planted in the config match the
  # symlink-resolved paths macOS FSEvents reports to the watcher: the
  # cursor-sqlite live-update assertions count distinct event rows and
  # would see duplicates if backfill and watcher keyed the same database
  # under two source_file spellings.
  tmp_root="$(cd "$tmp_root" && pwd -P)"
  local fixtures_root="$tmp_root/fixtures"
  local runtime_root="$tmp_root/runtime"
  local config_path="$tmp_root/moraine-ci.toml"
  local routed_bootstrap_config_path="$tmp_root/moraine-routed-bootstrap.toml"
  local codex_fixture_file="$fixtures_root/codex/sessions/2026/02/16/session-${codex_session_id}.jsonl"
  local claude_fixture_file="$fixtures_root/claude/projects/e2e/session-${claude_session_id}.jsonl"
  local kimi_fixture_file="$fixtures_root/kimi/sessions/${kimi_session_id}/wire.jsonl"
  local qwen_fixture_file="$fixtures_root/qwen/projects/e2e/chats/${qwen_session_id}.jsonl"
  local kiro_fixture_file="$fixtures_root/kiro/sessions/${kiro_session_id}.jsonl"
  local kiro_metadata_file="$fixtures_root/kiro/sessions/${kiro_session_id}.json"
  local cursor_fixture_file="$fixtures_root/cursor/projects/e2e/agent-transcripts/${cursor_session_id}/${cursor_session_id}.jsonl"
  local cursor_fallback_fixture_file="$fixtures_root/cursor/projects/e2e-fallback/agent-transcripts/${cursor_fallback_session_id}/${cursor_fallback_session_id}.jsonl"
  local cursor_sqlite_fixture_file="$fixtures_root/cursor-sqlite/User/globalStorage/state.vscdb"
  local pi_fixture_file="$fixtures_root/pi/agent/sessions/--tmp-moraine-e2e--/2026-02-16T12-00-08-000Z_${pi_session_id}.jsonl"
  local hermes_fixture_file="$fixtures_root/hermes/trajectories/001-${run_stamp}.jsonl"
  local hermes_session_fixture_file="$fixtures_root/hermes/sessions/${hermes_session_id}.json"
  local nac_fixture_dir="$fixtures_root/nac"
  local nac_fixture_file="$nac_fixture_dir/store.db"
  local nac_project_dir="$tmp_root/nac-project"
  # Codex records the launch cwd in session_meta. Keep this as a plain Git
  # repository with no .moraine.toml so bundled MCP project identity and ingest
  # normalization exercise the default, unconfigured repository path.
  local codex_project_dir="$tmp_root/codex-project"
  mkdir -p "$codex_project_dir/.git"
  # The directory the claude fixture session "originated from": real Claude
  # Code records a cwd on every message line, and `--project-only` scoping
  # keys off it. Must be a real directory so mcp_smoke.py can launch the
  # MCP server from inside it.
  local claude_project_dir="$tmp_root/claude-project"
  mkdir -p "$claude_project_dir"
  mkdir -p "$nac_project_dir"
  printf '[workspace]\nresolver = "2"\n' > "$nac_project_dir/Cargo.toml"

  mkdir -p "$(dirname "$codex_fixture_file")"
  mkdir -p "$(dirname "$claude_fixture_file")"
  mkdir -p "$(dirname "$kimi_fixture_file")"
  mkdir -p "$(dirname "$cursor_fixture_file")"
  mkdir -p "$(dirname "$qwen_fixture_file")"
  mkdir -p "$(dirname "$kiro_fixture_file")"
  mkdir -p "$(dirname "$cursor_fallback_fixture_file")"
  mkdir -p "$(dirname "$cursor_sqlite_fixture_file")"
  mkdir -p "$(dirname "$pi_fixture_file")"
  mkdir -p "$(dirname "$hermes_fixture_file")"
  mkdir -p "$(dirname "$hermes_session_fixture_file")"
  mkdir -p "$nac_fixture_dir"
  mkdir -p "$runtime_root"

  cat > "$codex_fixture_file" <<EOF
{"timestamp":"2026-02-16T12:00:00.000Z","type":"session_meta","payload":{"id":"${codex_session_id}","cwd":"${codex_project_dir}","source":"vscode"}}
{"timestamp":"2026-02-16T12:00:01.000Z","type":"turn_context","payload":{"turn_id":"1","model":"gpt-5.3-codex"}}
{"timestamp":"2026-02-16T12:00:02.000Z","type":"response_item","payload":{"type":"message","role":"user","id":"msg-user-${run_stamp}","content":[{"type":"text","text":"local e2e codex user prompt ${codex_keyword}"}],"phase":"completed"}}
{"timestamp":"2026-02-16T12:00:03.000Z","type":"response_item","parent_id":"msg-user-${run_stamp}","payload":{"type":"message","role":"assistant","id":"msg-assistant-${run_stamp}","content":[{"type":"text","text":"local e2e codex assistant reply ${codex_keyword} ${codex_trace_marker}"}],"phase":"final_answer"}}
{"timestamp":"2026-02-16T12:00:03.003Z","type":"event_msg","payload":{"type":"agent_message","turn_id":"1","message":"local e2e codex assistant reply ${codex_keyword} ${codex_trace_marker}","phase":"final_answer","status":"completed"}}
{"timestamp":"2026-02-16T12:00:03.500Z","type":"response_item","payload":{"type":"function_call","call_id":"codex-tool-${run_stamp}","name":"Read","arguments":"{\"path\":\"Cargo.toml\"}"}}
{"timestamp":"2026-02-16T12:00:03.750Z","type":"response_item","payload":{"type":"function_call_output","call_id":"codex-tool-${run_stamp}","output":"workspace = true"}}
{"timestamp":"2026-02-16T12:00:03.900Z","type":"event_msg","payload":{"type":"token_count","turn_id":"1","info":{"last_token_usage":{"input_tokens":17,"output_tokens":4,"cached_input_tokens":3,"cache_creation_input_tokens":2,"output_tokens_details":{"reasoning_tokens":1}}},"rate_limits":{"limit_name":"GPT-5.3-Codex-Spark","limit_id":"codex_e2e","plan_type":"pro"}}}
EOF

  cat > "$claude_fixture_file" <<EOF
{"type":"user","sessionId":"${claude_session_id}","uuid":"claude-user-${run_stamp}","cwd":"${claude_project_dir}","timestamp":"2026-02-16T12:00:04.000Z","message":{"role":"user","content":[{"type":"text","text":"local e2e claude user prompt ${claude_keyword}"}]}}
{"type":"assistant","sessionId":"${claude_session_id}","uuid":"claude-assistant-${run_stamp}","cwd":"${claude_project_dir}","parentUuid":"claude-user-${run_stamp}","requestId":"req-${run_stamp}","agentId":"agent-${run_stamp}","agentName":"primary","teamName":"root","timestamp":"2026-02-16T12:00:05.000Z","message":{"model":"claude-opus-4-5-20251101","role":"assistant","usage":{"input_tokens":9,"output_tokens":5,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"service_tier":"standard"},"content":[{"type":"thinking","thinking":"I should inspect the manifest."},{"type":"tool_use","id":"claude-tool-${run_stamp}","name":"Read","input":{"path":"Cargo.toml"}},{"type":"text","text":"local e2e claude assistant reply ${claude_keyword} ${claude_trace_marker}"}]}}
{"type":"user","sessionId":"${claude_session_id}","uuid":"claude-tool-result-${run_stamp}","cwd":"${claude_project_dir}","parentUuid":"claude-assistant-${run_stamp}","parentToolUseID":"claude-tool-${run_stamp}","toolUseID":"claude-tool-${run_stamp}","sourceToolAssistantUUID":"claude-assistant-${run_stamp}","sourceToolUseID":"claude-tool-${run_stamp}","timestamp":"2026-02-16T12:00:05.500Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"claude-tool-${run_stamp}","content":[{"type":"text","text":"workspace = true"}],"is_error":false}]}}
EOF

  cat > "$kimi_fixture_file" <<EOF
{"type":"metadata","protocol_version":"1.9"}
{"timestamp":1771243204.000000,"message":{"type":"TurnBegin","payload":{"user_input":"local e2e kimi user prompt ${kimi_keyword}"}}}
{"timestamp":1771243204.500000,"message":{"type":"StepBegin","payload":{"n":1}}}
{"timestamp":1771243205.000000,"message":{"type":"ContentPart","payload":{"type":"think","think":"Need to inspect files first."}}}
{"timestamp":1771243205.500000,"message":{"type":"ContentPart","payload":{"type":"text","text":"local e2e kimi assistant reply ${kimi_keyword} ${kimi_trace_marker}"}}}
{"timestamp":1771243206.000000,"message":{"type":"ToolCall","payload":{"type":"function","id":"kimi-tool-${run_stamp}","function":{"name":"ReadFile","arguments":"{\"path\":\"manifest.json\"}"},"extras":null}}}
{"timestamp":1771243206.500000,"message":{"type":"ToolResult","payload":{"tool_call_id":"kimi-tool-${run_stamp}","return_value":{"is_error":false,"output":"{\"ok\":true}","message":"Read file","display":[],"extras":null}}}}
{"timestamp":1771243207.000000,"message":{"type":"StatusUpdate","payload":{"context_usage":0.1,"context_tokens":100,"max_context_tokens":1000,"token_usage":{"input_other":10,"output":5,"input_cache_read":2,"input_cache_creation":1},"message_id":"chatcmpl-${run_stamp}","plan_mode":false,"mcp_status":null}}}
{"timestamp":1771243207.500000,"message":{"type":"SubagentEvent","payload":{"agent_id":"sub-${run_stamp}","event":{"type":"ContentPart","payload":{"type":"text","text":"sub-agent echo"}}}}}
EOF

  cat > "$qwen_fixture_file" <<EOF
{"uuid":"qwen-user-${run_stamp}","parentUuid":null,"sessionId":"${qwen_session_id}","timestamp":"2026-02-16T12:00:30.000Z","type":"user","cwd":"${tmp_root}/qwen-project","version":"0.19.11","gitBranch":"main","message":{"role":"user","parts":[{"text":"local e2e qwen user prompt ${qwen_keyword}"}]}}
{"uuid":"qwen-assistant-${run_stamp}","parentUuid":"qwen-user-${run_stamp}","sessionId":"${qwen_session_id}","timestamp":"2026-02-16T12:00:31.000Z","type":"assistant","cwd":"${tmp_root}/qwen-project","version":"0.19.11","gitBranch":"main","model":"Qwen3-Coder-Plus","message":{"role":"model","parts":[{"text":"I should search Moraine before replying.","thought":true},{"text":"local e2e qwen assistant reply ${qwen_keyword} ${qwen_trace_marker}"},{"functionCall":{"id":"qwen-call-search-${run_stamp}","name":"mcp__moraine__search_sessions","args":{"query":"${qwen_self_keyword}"}}}]},"usageMetadata":{"promptTokenCount":120,"candidatesTokenCount":80,"cachedContentTokenCount":20,"thoughtsTokenCount":7,"toolUsePromptTokenCount":5,"totalTokenCount":200,"promptTokensDetails":[{"modality":"TEXT","tokenCount":91},{"modality":"IMAGE","tokenCount":4}],"candidatesTokensDetails":[{"modality":"TEXT","tokenCount":62},{"modality":"AUDIO","tokenCount":11}]}}
{"uuid":"qwen-result-${run_stamp}","parentUuid":"qwen-assistant-${run_stamp}","sessionId":"${qwen_session_id}","timestamp":"2026-02-16T12:00:32.000Z","type":"tool_result","cwd":"${tmp_root}/qwen-project","version":"0.19.11","gitBranch":"main","message":{"role":"user","parts":[{"functionResponse":{"id":"qwen-response-${run_stamp}","name":"mcp__moraine__search_sessions","response":{"output":"Three matching sessions."}}}]},"toolCallResult":{"callId":"qwen-call-search-${run_stamp}","status":"success","durationMs":25}}
{"uuid":"qwen-fail-call-${run_stamp}","parentUuid":"qwen-result-${run_stamp}","sessionId":"${qwen_session_id}","timestamp":"2026-02-16T12:00:33.000Z","type":"assistant","cwd":"${tmp_root}/qwen-project","version":"0.19.11","gitBranch":"main","model":"Qwen3-Coder-Plus","message":{"role":"model","parts":[{"functionCall":{"id":"qwen-call-fail-${run_stamp}","name":"shell","args":{"command":"false"}}}]}}
{"uuid":"qwen-fail-result-${run_stamp}","parentUuid":"qwen-fail-call-${run_stamp}","sessionId":"${qwen_session_id}","timestamp":"2026-02-16T12:00:34.000Z","type":"tool_result","cwd":"${tmp_root}/qwen-project","version":"0.19.11","gitBranch":"main","message":{"role":"user","parts":[{"functionResponse":{"id":"qwen-fail-response-${run_stamp}","name":"shell","response":{"error":"command failed"}}}]},"toolCallResult":{"callId":"qwen-call-fail-${run_stamp}","status":"failed","durationMs":42,"error":{"message":"command failed"}}}
{"uuid":"qwen-title-${run_stamp}","parentUuid":"qwen-fail-result-${run_stamp}","sessionId":"${qwen_session_id}","timestamp":"2026-02-16T12:00:35.000Z","type":"system","subtype":"custom_title","cwd":"${tmp_root}/qwen-project","version":"0.19.11","gitBranch":"main","systemPayload":{"customTitle":"Qwen e2e ${run_stamp}","titleSource":"model"}}
{"uuid":"qwen-compression-${run_stamp}","parentUuid":"qwen-title-${run_stamp}","sessionId":"${qwen_session_id}","timestamp":"2026-02-16T12:00:36.000Z","type":"system","subtype":"chat_compression","cwd":"${tmp_root}/qwen-project","version":"0.19.11","gitBranch":"main","systemPayload":{"info":{"originalTokenCount":4096,"newTokenCount":1024,"compressionStatus":"completed","triggerReason":"token_limit"},"compressedHistory":[{"role":"user","parts":[{"text":"must remain raw only"}]}]}}
{"uuid":"qwen-abandoned-${run_stamp}","parentUuid":"qwen-compression-${run_stamp}","sessionId":"${qwen_session_id}","timestamp":"2026-02-16T12:00:37.000Z","type":"assistant","cwd":"${tmp_root}/qwen-project","version":"0.19.11","gitBranch":"main","model":"Qwen3-Coder-Plus","message":{"role":"model","parts":[{"text":"Qwen abandoned branch ${run_stamp}"}]}}
{"uuid":"qwen-rewind-${run_stamp}","parentUuid":"qwen-compression-${run_stamp}","sessionId":"${qwen_session_id}","timestamp":"2026-02-16T12:00:38.000Z","type":"system","subtype":"rewind","cwd":"${tmp_root}/qwen-project","version":"0.19.11","gitBranch":"main","systemPayload":{"truncatedCount":1,"targetUuid":"qwen-compression-${run_stamp}"}}
{"uuid":"qwen-replacement-user-${run_stamp}","parentUuid":"qwen-compression-${run_stamp}","sessionId":"${qwen_session_id}","timestamp":"2026-02-16T12:00:39.000Z","type":"user","subtype":"mid_turn_user_message","cwd":"${tmp_root}/qwen-project","version":"0.19.11","gitBranch":"main","message":{"role":"user","parts":[{"text":"Qwen replacement branch prompt ${run_stamp}"}]}}
{"uuid":"qwen-replacement-assistant-${run_stamp}","parentUuid":"qwen-replacement-user-${run_stamp}","sessionId":"${qwen_session_id}","timestamp":"2026-02-16T12:00:40.000Z","type":"assistant","cwd":"${tmp_root}/qwen-project","version":"0.19.11","gitBranch":"main","model":"Qwen3-Coder-Plus","message":{"role":"model","parts":[{"text":"Qwen replacement branch answer ${run_stamp}"}]}}
{"uuid":"qwen-unknown-${run_stamp}","parentUuid":"qwen-replacement-assistant-${run_stamp}","sessionId":"${qwen_session_id}","timestamp":"2026-02-16T12:00:41.000Z","type":"future_record","cwd":"${tmp_root}/qwen-project","version":"0.19.11","payload":{"text":"unknown must remain raw"}}
{"uuid":"qwen-snapshot-${run_stamp}","parentUuid":"qwen-unknown-${run_stamp}","sessionId":"${qwen_session_id}","timestamp":"2026-02-16T12:00:42.000Z","type":"system","subtype":"file_history_snapshot","cwd":"${tmp_root}/qwen-project","version":"0.19.11","systemPayload":{"snapshots":[{"path":"secret.txt","content":"snapshot must remain raw"}]}}
{"uuid":"qwen-binary-${run_stamp}","parentUuid":"qwen-snapshot-${run_stamp}","sessionId":"${qwen_session_id}","timestamp":"2026-02-16T12:00:43.000Z","type":"user","cwd":"${tmp_root}/qwen-project","version":"0.19.11","message":{"role":"user","parts":[{"inlineData":{"mimeType":"image/png","data":"iVBORw0KGgoAAAANSUhEUgAAAAEAAAAB"}}]}}
{"uuid":"qwen-bad-ts-${run_stamp}","parentUuid":"qwen-replacement-assistant-${run_stamp}","sessionId":"${qwen_session_id}","timestamp":"not-a-timestamp","type":"user","cwd":"${tmp_root}/qwen-project","version":"0.19.11","message":{"role":"user","parts":[{"text":"Qwen malformed timestamp neighbor ${run_stamp}"}]}}
EOF

  # Kiro stores each CLI session as a paired JSONL transcript and JSON sidecar.
  # The sidecar supplies session metadata and is rewritten independently.
  cat > "$kiro_fixture_file" <<EOF
{"version":"v1","kind":"Prompt","data":{"message_id":"kiro-user-${run_stamp}","content":[{"kind":"text","data":"local e2e kiro user prompt ${kiro_keyword}"}],"meta":{"timestamp":1771243208}}}
{"version":"v1","kind":"AssistantMessage","data":{"message_id":"kiro-assistant-${run_stamp}","content":[{"kind":"thinking","data":{"text":"I should inspect the target file."}},{"kind":"text","data":"I will inspect the target file."},{"kind":"toolUse","data":{"toolUseId":"kiro-tool-${run_stamp}","name":"read_file","input":{"path":"/workspace/kiro-e2e.txt"},"modelId":"claude-sonnet-4"}}]}}
{"version":"v1","kind":"ToolResults","data":{"message_id":"kiro-tool-result-${run_stamp}","content":[{"kind":"toolResult","data":{"toolUseId":"kiro-tool-${run_stamp}","status":"success","content":[{"kind":"text","data":"kiro tool output"}]}}],"results":{"kiro-tool-${run_stamp}":{"tool":{"kind":{"BuiltIn":{"Read":{}}}}}}}}
{"version":"v1","kind":"AssistantMessage","data":{"message_id":"kiro-final-${run_stamp}","content":[{"kind":"text","data":"local e2e kiro assistant reply ${kiro_keyword} ${kiro_trace_marker}"}]}}
{"version":"v1","kind":"Compaction","data":{"summary":"Kiro E2E session compacted after inspecting the target file.","strategy":"summary"}}
EOF

  cat > "$kiro_metadata_file" <<EOF
{
  "session_id": "${kiro_session_id}",
  "cwd": "${kiro_cwd}",
  "title": "${kiro_session_title}",
  "created_at": "2026-02-16T12:00:08Z",
  "updated_at": "2026-02-16T12:00:13Z",
  "session_state": {
    "agent_name": "kiro_default",
    "rts_model_state": {
      "model_info": {"model_id": "claude-sonnet-4"}
    },
    "conversation_metadata": {
      "user_turn_metadatas": [
        {"input_token_count": 23, "output_token_count": 11, "metering_usage": [{"value": 0.75, "unit": "credit", "unitPlural": "credits"}]}
      ]
    }
  }
}
EOF

  # Cursor Agent transcripts observed under ~/.cursor/projects/.../agent-transcripts
  # use role/message envelopes. This deterministic fixture stays inside the
  # MCP smoke test window; the small fixture below covers timestamp-less files.
  cat > "$cursor_fixture_file" <<EOF
{"timestamp":"2026-02-16T12:00:10.000Z","role":"user","message":{"content":[{"type":"text","text":"local e2e cursor user prompt ${cursor_keyword}"}]}}
{"timestamp":"2026-02-16T12:00:11.000Z","role":"assistant","message":{"content":[{"type":"text","text":"I will inspect the target file."},{"type":"tool_use","id":"cursor-tool-${run_stamp}","name":"Read","input":{"path":"/workspace/cursor-e2e.txt","filename":"cursor-e2e.txt","symbol":"cursorE2e"}}]}}
{"timestamp":"2026-02-16T12:00:12.000Z","role":"tool","message":{"content":[{"type":"tool_result","tool_use_id":"cursor-tool-${run_stamp}","content":[{"type":"text","text":"cursor tool output"}],"is_error":false}]}}
{"timestamp":"2026-02-16T12:00:13.000Z","role":"assistant","message":{"content":[{"type":"text","text":"local e2e cursor assistant reply ${cursor_keyword} ${cursor_trace_marker}"}]}}
EOF

  # Cursor JSONL files can omit per-row timestamps. This file proves ingest
  # still assigns a usable event timestamp from the append-only file metadata.
  cat > "$cursor_fallback_fixture_file" <<EOF
{"role":"user","message":{"content":[{"type":"text","text":"local e2e cursor fallback prompt ${cursor_fallback_keyword}"}]}}
EOF

  # Cursor's IDE-side history lives in a SQLite kv store (state.vscdb), not
  # JSONL; the cursor_sqlite format polls it (issue #361). Planted via
  # Python's stdlib sqlite3 because the sqlite3 CLI is not guaranteed on CI
  # hosts, and committed in full before 'moraine up' so the first poll sees a
  # schema-complete database. Row shapes mirror
  # fixtures/cursor/state-vscdb-kv.jsonl: one composerData row plus a user
  # bubble, a thinking bubble, and a completed tool bubble.
  "$python_bin" - \
    "$cursor_sqlite_fixture_file" \
    "$cursor_sqlite_session_id" \
    "$cursor_sqlite_session_title" \
    "local e2e cursor sqlite user prompt ${cursor_sqlite_keyword} ${cursor_sqlite_trace_marker}" \
    "cursor-sqlite-tool-${run_stamp}" <<'PY'
import json
import sqlite3
import sys

db_path, composer_id, session_name, user_text, tool_call_id = sys.argv[1:6]

user_bubble = "aaaaaaaa-1111-4111-8111-111111111111"
thinking_bubble = "bbbbbbbb-2222-4222-8222-222222222222"
tool_bubble = "cccccccc-3333-4333-8333-333333333333"

composer = {
    "_v": 16,
    "composerId": composer_id,
    "name": session_name,
    "subtitle": "Edited cursor-sqlite-e2e.txt",
    "unifiedMode": "agent",
    "agentBackend": "cursor-agent",
    "status": "completed",
    "createdAt": 1771243220000,
    "lastUpdatedAt": 1771243222000,
    "workspaceIdentifier": {"id": "e2e-window", "uri": {"fsPath": "/workspace"}},
    "fullConversationHeadersOnly": [
        {"bubbleId": user_bubble, "type": 1},
        {"bubbleId": thinking_bubble, "type": 2},
        {"bubbleId": tool_bubble, "type": 2},
    ],
}
rows = {
    f"composerData:{composer_id}": composer,
    f"bubbleId:{composer_id}:{user_bubble}": {
        "_v": 3,
        "type": 1,
        "bubbleId": user_bubble,
        "createdAt": "2026-02-16T12:00:20.000Z",
        "requestId": "11111111-aaaa-4aaa-8aaa-111111111111",
        "text": user_text,
        "richText": json.dumps(
            {
                "type": "doc",
                "content": [
                    {
                        "type": "paragraph",
                        "content": [{"type": "text", "text": user_text}],
                    }
                ],
            }
        ),
        "modelInfo": {"modelName": "default"},
    },
    f"bubbleId:{composer_id}:{thinking_bubble}": {
        "_v": 3,
        "type": 2,
        "bubbleId": thinking_bubble,
        "createdAt": "2026-02-16T12:00:21.000Z",
        "capabilityType": 30,
        "thinkingStyle": 2,
        "thinking": {
            "text": "**Planning the edit**\n\nInspect the target file first.",
            "signature": "",
        },
        "thinkingDurationMs": 1200,
    },
    f"bubbleId:{composer_id}:{tool_bubble}": {
        "_v": 3,
        "type": 2,
        "bubbleId": tool_bubble,
        "createdAt": "2026-02-16T12:00:22.000Z",
        "capabilityType": 15,
        "toolFormerData": {
            "tool": 38,
            "toolIndex": 0,
            "name": "edit_file_v2",
            "modelCallId": "",
            "toolCallId": tool_call_id,
            "status": "completed",
            "params": json.dumps(
                {
                    "relativeWorkspacePath": "/workspace/cursor-sqlite-e2e.txt",
                    "noCodeblock": True,
                }
            ),
            "rawArgs": "{}",
            "result": json.dumps({"afterContentId": "composer.content.e2e"}),
            "additionalData": {"status": "completed", "startedAtMs": 1771243222000},
            "userDecision": "accepted",
        },
    },
}

connection = sqlite3.connect(db_path)
connection.execute(
    "CREATE TABLE cursorDiskKV (key TEXT UNIQUE ON CONFLICT REPLACE, value BLOB)"
)
connection.executemany(
    "INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)",
    # str (TEXT storage class) to match real Cursor: it writes JSON as TEXT
    # despite the BLOB-declared column.
    [(key, json.dumps(value)) for key, value in rows.items()],
)
connection.commit()
connection.close()
PY

  cat > "$pi_fixture_file" <<EOF
{"type":"session","version":3,"id":"${pi_session_id}","timestamp":"2026-02-16T12:00:08.000Z","cwd":"${tmp_root}"}
{"type":"model_change","id":"pi-model-${run_stamp}","parentId":null,"timestamp":"2026-02-16T12:00:08.100Z","provider":"openai","modelId":"gpt-5.2-chat-latest"}
{"type":"thinking_level_change","id":"pi-thinking-${run_stamp}","parentId":"pi-model-${run_stamp}","timestamp":"2026-02-16T12:00:08.200Z","thinkingLevel":"high"}
{"type":"message","id":"pi-user-${run_stamp}","parentId":"pi-thinking-${run_stamp}","timestamp":"2026-02-16T12:00:09.000Z","message":{"role":"user","content":[{"type":"text","text":"local e2e pi user prompt ${pi_keyword}"}],"timestamp":1771243209000}}
{"type":"message","id":"pi-assistant-${run_stamp}","parentId":"pi-user-${run_stamp}","timestamp":"2026-02-16T12:00:10.000Z","message":{"role":"assistant","content":[{"type":"thinking","thinking":"Need to inspect package metadata."},{"type":"text","text":"local e2e pi assistant reply ${pi_keyword} ${pi_trace_marker}"},{"type":"toolCall","id":"pi-tool-${run_stamp}","name":"read","arguments":{"path":"Cargo.toml"}}],"api":"chat","provider":"openai","model":"gpt-5.2-chat-latest","usage":{"input":16,"output":8,"cacheRead":2,"cacheWrite":1,"totalTokens":27,"cost":{"input":0.001,"output":0.002,"cacheRead":0.0001,"cacheWrite":0.0002,"total":0.0033}},"stopReason":"toolUse","timestamp":1771243210000}}
{"type":"message","id":"pi-tool-result-${run_stamp}","parentId":"pi-assistant-${run_stamp}","timestamp":"2026-02-16T12:00:11.000Z","message":{"role":"toolResult","toolCallId":"pi-tool-${run_stamp}","toolName":"read","content":[{"type":"text","text":"workspace = true"}],"isError":false,"timestamp":1771243211000}}
{"type":"session_info","id":"pi-info-${run_stamp}","parentId":"pi-tool-result-${run_stamp}","timestamp":"2026-02-16T12:00:12.000Z","name":"Pi e2e ${run_stamp}"}
EOF

  # Hermes ShareGPT trajectory: one completed rollout per line. Exercises a
  # vendor/model prefix (`anthropic/claude-sonnet-4.6`), reasoning, tool
  # call/result rows, and an assistant text turn carrying the MCP trace marker.
  cat > "$hermes_fixture_file" <<EOF
{"timestamp":"2026-02-16T12:00:06.000000","model":"anthropic/claude-sonnet-4.6","prompt_index":1,"completed":true,"partial":false,"api_calls":1,"conversations":[{"from":"human","value":"local e2e hermes trajectory user prompt ${hermes_keyword}"},{"from":"gpt","value":"<think>draft the answer</think>\n<tool_call>{\"tool_call_id\":\"hermes-tool-${run_stamp}\",\"name\":\"weather\",\"arguments\":{\"location\":\"Boston, MA\"}}</tool_call>"},{"from":"tool","value":"<tool_response>{\"tool_call_id\":\"hermes-tool-${run_stamp}\",\"name\":\"weather\",\"content\":{\"forecast\":\"rain\"}}</tool_response>"},{"from":"gpt","value":"local e2e hermes trajectory assistant reply ${hermes_keyword} ${hermes_trace_marker}"}]}
EOF

  cat > "$hermes_session_fixture_file" <<EOF
{
  "session_id": "${hermes_session_id}",
  "model": "claude-opus-4-6",
  "base_url": "https://api.anthropic.com",
  "platform": "cli",
  "session_start": "2026-02-16T12:00:08.000000",
  "last_updated": "2026-02-16T12:00:09.000000",
  "system_prompt": "You are Hermes, a helpful CLI agent.",
  "tools": [
    {
      "type": "function",
      "function": {
        "name": "shell",
        "description": "Run a shell command",
        "parameters": {
          "type": "object",
          "properties": {
            "cmd": {"type": "string"}
          },
          "required": ["cmd"]
        }
      }
    }
  ],
  "message_count": 4,
  "messages": [
    {
      "role": "user",
      "content": "local e2e hermes session user prompt ${hermes_session_keyword}"
    },
    {
      "role": "assistant",
      "content": "",
      "reasoning": "I should run date before answering.",
      "finish_reason": "tool_calls",
      "tool_calls": [
        {
          "id": "hermes-session-tool-${run_stamp}",
          "type": "function",
          "function": {
            "name": "shell",
            "arguments": "{\"cmd\":\"date\"}"
          }
        }
      ]
    },
    {
      "role": "tool",
      "tool_call_id": "hermes-session-tool-${run_stamp}",
      "name": "shell",
      "content": "Mon Feb 16 12:00:09 UTC 2026"
    },
    {
      "role": "assistant",
      "content": "local e2e hermes session assistant reply ${hermes_session_keyword} ${hermes_session_trace_marker}",
      "finish_reason": "stop"
    }
  ]
}
EOF

  "$python_bin" - \
    "$nac_fixture_file" \
    "$nac_session_id" \
    "$nac_local_worker_id" \
    "$nac_remote_worker_id" \
    "$nac_project_dir" \
    "$nac_keyword" \
    "$nac_worker_keyword" \
    "$nac_mcp_sentinel" \
    "$nac_remote_keyword" \
    "$nac_secret_sentinel" \
    "$nac_tool_call_id" <<'PY'
import json
import sqlite3
import sys

(
    db_path,
    parent_id,
    local_worker_id,
    remote_worker_id,
    cwd,
    keyword,
    worker_keyword,
    mcp_sentinel,
    remote_keyword,
    secret,
    tool_call_id,
) = sys.argv[1:12]

connection = sqlite3.connect(db_path)
connection.executescript(
    """
    PRAGMA foreign_keys = ON;
    PRAGMA journal_mode = WAL;

    CREATE TABLE sessions (
        session_id TEXT PRIMARY KEY,
        cwd TEXT NOT NULL,
        model TEXT NOT NULL,
        base_url TEXT NOT NULL,
        backend TEXT NOT NULL,
        reasoning_effort TEXT,
        sandbox_json TEXT,
        messages_json TEXT NOT NULL,
        api_key_env TEXT,
        extra_headers_json TEXT,
        last_response_duration_ms INTEGER,
        previous_response_duration_ms INTEGER,
        response_durations_json TEXT,
        token_usages_json TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        host_id TEXT
    );
    CREATE TABLE episodes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        thread_name TEXT NOT NULL,
        session_id TEXT NOT NULL,
        action TEXT NOT NULL,
        content TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
    );
    CREATE INDEX idx_episodes_session_id_id ON episodes(session_id, id);
    """
)

messages = [
    {
        "role": "user",
        "content": f"inspect the nac canonical stack marker {keyword}",
    },
    {
        "role": "assistant",
        "content": f"nac parent response for {keyword}",
        "reasoning_text": "bounded fixture reasoning",
    },
    {
        "role": "assistant",
        "content": "",
        "tool_calls": [
            {
                "id": tool_call_id,
                "type": "function",
                "function": {
                    "name": "mcp__moraine__search",
                    "arguments": json.dumps(
                        {"query": mcp_sentinel, "path": "Cargo.toml"},
                        separators=(",", ":"),
                    ),
                },
            }
        ],
    },
    {
        "role": "tool",
        "content": f"canonical tool result for {mcp_sentinel}",
        "tool_call_id": tool_call_id,
        "name": "mcp__moraine__search",
    },
]
connection.execute(
    """
    INSERT INTO sessions (
        session_id, cwd, model, base_url, backend, reasoning_effort,
        sandbox_json, messages_json, api_key_env, extra_headers_json,
        last_response_duration_ms, previous_response_duration_ms,
        response_durations_json, token_usages_json,
        created_at, updated_at, host_id
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
    (
        parent_id,
        cwd,
        "z-ai/glm-5.2",
        "https://openrouter.ai/api/v1",
        "together-chat",
        "high",
        json.dumps({"enabled": False}),
        json.dumps(messages),
        secret,
        json.dumps({"Authorization": f"Bearer {secret}"}),
        2100,
        1800,
        json.dumps([2100]),
        json.dumps(
            [
                {
                    "input_tokens": 13,
                    "output_tokens": 5,
                    "cache_read_tokens": 2,
                    "cache_write_tokens": 1,
                    "reasoning_tokens": 3,
                }
            ]
        ),
        "2026-02-16 12:00:05.000000000",
        "2026-02-16 12:00:08.000000000",
        None,
    ),
)
connection.execute(
    """
    INSERT INTO sessions (
        session_id, cwd, model, base_url, backend, messages_json,
        created_at, updated_at, host_id
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
    (
        remote_worker_id,
        "/remote/private/path",
        "z-ai/glm-5.2",
        "https://openrouter.ai/api/v1",
        "together-chat",
        json.dumps(
            [
                {"role": "user", "content": f"remote question {remote_keyword}"},
                {"role": "assistant", "content": f"remote answer {remote_keyword}"},
            ]
        ),
        "2026-02-16 12:00:11.000000000",
        "2026-02-16 12:00:12.000000000",
        "remote-host",
    ),
)
connection.executemany(
    """
    INSERT INTO episodes (thread_name, session_id, action, content, created_at)
    VALUES (?, ?, ?, ?, ?)
    """,
    [
        (
            local_worker_id,
            parent_id,
            "search repository",
            "local worker action complete",
            "2026-02-16 12:00:09",
        ),
        (
            local_worker_id,
            parent_id,
            "report result",
            f"local worker result for {worker_keyword}",
            "2026-02-16 12:00:10",
        ),
        (
            remote_worker_id,
            remote_worker_id,
            f"remote action {remote_keyword}",
            f"remote response {remote_keyword}",
            "2026-02-16 12:00:11",
        ),
    ],
)
connection.commit()
connection.close()
PY

  local nac_canonical_db
  nac_canonical_db="$("$python_bin" - "$nac_fixture_file" <<'PY'
from pathlib import Path
import sys

print(Path(sys.argv[1]).resolve())
PY
)"
  local nac_normalized_session_id
  nac_normalized_session_id="$("$python_bin" - "$nac_canonical_db" "$nac_session_id" <<'PY'
from hashlib import sha256
import sys

db_path, raw_session_id = sys.argv[1:3]
material = f"ci-nac\n{db_path}\n1".encode()
namespace = sha256(material).hexdigest()[:16]
print(f"nac:{namespace}:{raw_session_id}")
PY
)"
  local nac_normalized_local_worker_id
  nac_normalized_local_worker_id="$nac_normalized_session_id:nac-worker:$($python_bin - "$nac_local_worker_id" <<'PY'
from hashlib import sha256
import sys

print(sha256(sys.argv[1].encode()).hexdigest()[:16])
PY
)"

  cat > "$config_path" <<EOF
[backends.default]
url = "${clickhouse_url}"
database = "${clickhouse_database}"

[backends.routed]
url = "${clickhouse_url}"
database = "${routed_clickhouse_database}"

[[routes]]
dir = "${claude_project_dir}/**"
backend = "routed"
mode = "mirror"

[backend]
bind = "127.0.0.1"
start_on_up = false

[monitor]
port = ${monitor_port}

[identity]
author = "routed-e2e"

[ingest]
backfill_on_start = true
heartbeat_interval_seconds = 1.0
flush_interval_seconds = 0.2
reconcile_interval_seconds = 2.0
state_dir = "${tmp_root}/ingest-state"

[[ingest.sources]]
name = "ci-codex"
harness = "codex"
enabled = true
glob = "${fixtures_root}/codex/sessions/**/*.jsonl"
watch_root = "${fixtures_root}/codex/sessions"

[[ingest.sources]]
name = "ci-claude"
harness = "claude-code"
enabled = true
glob = "${fixtures_root}/claude/projects/**/*.jsonl"
watch_root = "${fixtures_root}/claude/projects"

[[ingest.sources]]
name = "ci-kimi"
harness = "kimi-cli"
enabled = true
glob = "${fixtures_root}/kimi/sessions/**/wire.jsonl"
watch_root = "${fixtures_root}/kimi/sessions"

[[ingest.sources]]
name = "qwen-code"
harness = "qwen-code"
enabled = true
glob = "${fixtures_root}/qwen/projects/*/chats/*.jsonl"
watch_root = "${fixtures_root}/qwen/projects"
format = "jsonl"

[[ingest.sources]]
name = "ci-kiro"
harness = "kiro-cli"
format = "kiro_session"
enabled = true
glob = "${fixtures_root}/kiro/sessions/*.jsonl"
watch_root = "${fixtures_root}/kiro/sessions"

[[ingest.sources]]
name = "ci-cursor"
harness = "cursor"
enabled = true
glob = "${fixtures_root}/cursor/projects/*/agent-transcripts/**/*.jsonl"
watch_root = "${fixtures_root}/cursor/projects"

# cursor_sqlite polls Cursor's state.vscdb kv store instead of tailing JSONL.
# The format ships disabled by default (issue #361: Cursor publishes no
# stable local-DB contract), so the e2e opts in explicitly here against a
# fixture database.
[[ingest.sources]]
name = "cursor-sqlite"
harness = "cursor"
format = "cursor_sqlite"
enabled = true
glob = "${fixtures_root}/cursor-sqlite/User/**/state.vscdb"
watch_root = "${fixtures_root}/cursor-sqlite/User"

[[ingest.sources]]
name = "ci-pi"
harness = "pi-coding-agent"
format = "jsonl"
enabled = true
glob = "${fixtures_root}/pi/agent/sessions/**/*.jsonl"
watch_root = "${fixtures_root}/pi/agent/sessions"

[[ingest.sources]]
name = "ci-hermes-trajectory"
harness = "hermes"
enabled = true
glob = "${fixtures_root}/hermes/trajectories/**/*.jsonl"
watch_root = "${fixtures_root}/hermes/trajectories"

[[ingest.sources]]
name = "ci-hermes-session"
harness = "hermes"
format = "session_json"
enabled = true
glob = "${fixtures_root}/hermes/sessions/*.json"
watch_root = "${fixtures_root}/hermes/sessions"

[[ingest.sources]]
name = "ci-nac"
harness = "nac"
enabled = true
glob = "${nac_fixture_file}"
watch_root = "${nac_fixture_dir}"
format = "nac_sqlite"

[mcp]
# Exercise the unified backend end to end: the canonical backend launch switch
# makes 'moraine up' host both the central MCP socket and monitor HTTP surface.
# mcp_smoke.py must proxy through it until the explicit crash check below.

use_central_server = true

[runtime]
root_dir = "${runtime_root}"
logs_dir = "logs"
pids_dir = "run"
service_bin_dir = "${service_bin_dir}"
managed_clickhouse_dir = "${runtime_root}/managed-clickhouse"
clickhouse_auto_install = true
clickhouse_start_timeout_seconds = 90.0
EOF

  cat > "$routed_bootstrap_config_path" <<EOF
[clickhouse]
url = "${clickhouse_url}"
database = "${routed_clickhouse_database}"

[backend]
start_on_up = false

[monitor]
port = ${monitor_port}

[runtime]
root_dir = "${runtime_root}"
logs_dir = "logs"
pids_dir = "run"
service_bin_dir = "${service_bin_dir}"
managed_clickhouse_dir = "${runtime_root}/managed-clickhouse"
clickhouse_auto_install = true
clickhouse_start_timeout_seconds = 90.0
EOF

  trap "cleanup_e2e \"$moraine_bin\" \"$config_path\" \"$runtime_root\" \"$tmp_root\"" EXIT

  echo "[e2e] run id: ${run_stamp}"
  echo "[e2e] clickhouse url: ${clickhouse_url}"
  echo "[e2e] clickhouse db: ${clickhouse_database}"
  echo "[e2e] codex fixture: ${codex_fixture_file}"
  echo "[e2e] claude fixture: ${claude_fixture_file}"
  echo "[e2e] kimi fixture: ${kimi_fixture_file}"
  echo "[e2e] kiro fixture: ${kiro_fixture_file}"
  echo "[e2e] cursor fixture: ${cursor_fixture_file}"
  echo "[e2e] cursor fallback fixture: ${cursor_fallback_fixture_file}"
  echo "[e2e] cursor sqlite fixture: ${cursor_sqlite_fixture_file}"
  echo "[e2e] pi fixture: ${pi_fixture_file}"
  echo "[e2e] hermes fixture: ${hermes_fixture_file}"
  echo "[e2e] hermes session fixture: ${hermes_session_fixture_file}"
  echo "[e2e] nac sqlite fixture: ${nac_fixture_file}"

  echo "[e2e] installing managed ClickHouse"
  "$moraine_bin" clickhouse install --config "$config_path"

  echo "[e2e] ensuring monitor frontend assets"
  ensure_monitor_frontend "$repo_root"

  echo "[e2e] bootstrapping distinct named-backend schema"
  "$moraine_bin" up --config "$routed_bootstrap_config_path" --no-ingest
  "$moraine_bin" down --config "$routed_bootstrap_config_path"

  echo "[e2e] starting stack"
  "$moraine_bin" up --config "$config_path"

  echo "[e2e] checking canonical managed process topology"
  local pids_dir="$runtime_root/run"
  local backend_pid_file="$pids_dir/backend.pid"
  local ingest_pid_file="$pids_dir/ingest.pid"
  local clickhouse_pid_file="$pids_dir/clickhouse.pid"
  local backend_socket="$pids_dir/mcp.sock"
  local backend_log="$runtime_root/logs/backend.log"
  local daemon_tools_snapshot="$tmp_root/tools-daemon.json"
  local embedded_tools_snapshot="$tmp_root/tools-embedded.json"
  assert_canonical_pid_layout "$pids_dir"
  local backend_pid
  local ingest_pid
  local clickhouse_pid
  backend_pid="$(read_live_pid_file "$backend_pid_file" "backend")"
  ingest_pid="$(read_live_pid_file "$ingest_pid_file" "ingest")"
  clickhouse_pid="$(read_live_pid_file "$clickhouse_pid_file" "clickhouse")"
  echo "[e2e] checking managed service executable paths"
  assert_process_binary "$ingest_pid" "$expected_ingest_bin" "ingest"
  assert_process_binary "$backend_pid" "$expected_backend_bin" "backend"

  local moraine_version
  moraine_version="$("$moraine_bin" --version)"
  moraine_version="${moraine_version##* }"
  if [[ -z "$moraine_version" || "$moraine_version" == *" "* ]]; then
    echo "[e2e] could not resolve Moraine version for ClickHouse attribution" >&2
    return 1
  fi
  local expected_backend_ua="moraine-backend/${moraine_version} (pid=${backend_pid})"
  local expected_ingest_ua="moraine-ingest/${moraine_version} (pid=${ingest_pid})"

  echo "[e2e] waiting for monitor health"
  wait_for_endpoint_ok "$python_bin" "http://127.0.0.1:${monitor_port}/api/v1/health" 120

  echo "[e2e] checking unified backend HTML + referenced static asset"
  assert_frontend_asset_served "$python_bin" "http://127.0.0.1:${monitor_port}" "$tmp_root"

  echo "[e2e] waiting for ingest heartbeat + indexed content"
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.ingest_heartbeats" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_documents WHERE positionCaseInsensitiveUTF8(text_content, '${codex_keyword}') > 0" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_postings WHERE term = '${codex_keyword}'" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_documents WHERE positionCaseInsensitiveUTF8(text_content, '${claude_keyword}') > 0" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_postings WHERE term = '${claude_keyword}'" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${routed_clickhouse_database}.search_documents WHERE positionCaseInsensitiveUTF8(text_content, '${claude_keyword}') > 0" 120
  assert_clickhouse_count "$clickhouse_url" "named backend excludes default-only codex session" "SELECT count() FROM ${routed_clickhouse_database}.search_documents WHERE positionCaseInsensitiveUTF8(text_content, '${codex_keyword}') > 0" "0"
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_documents WHERE positionCaseInsensitiveUTF8(text_content, '${kimi_keyword}') > 0" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_postings WHERE term = '${kimi_keyword}'" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_documents WHERE positionCaseInsensitiveUTF8(text_content, '${qwen_keyword}') > 0" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_postings WHERE term = '${qwen_keyword}'" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_documents WHERE positionCaseInsensitiveUTF8(text_content, '${kiro_keyword}') > 0" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_postings WHERE term = '${kiro_keyword}'" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.tool_io WHERE source_name = 'ci-kiro' AND tool_call_id = 'kiro-tool-${run_stamp}'" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_documents WHERE positionCaseInsensitiveUTF8(text_content, '${cursor_keyword}') > 0" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_postings WHERE term = '${cursor_keyword}'" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.event_links WHERE source_name = 'ci-cursor' AND linked_external_id = '/workspace/cursor-e2e.txt'" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.tool_io WHERE source_name = 'ci-cursor' AND tool_call_id = 'cursor-tool-${run_stamp}'" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.events WHERE source_name = 'ci-cursor' AND session_id = '${cursor_fallback_session_id}' AND positionCaseInsensitiveUTF8(text_content, '${cursor_fallback_keyword}') > 0" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_documents WHERE positionCaseInsensitiveUTF8(text_content, '${cursor_sqlite_keyword}') > 0" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_postings WHERE term = '${cursor_sqlite_keyword}'" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.raw_events WHERE source_name = 'cursor-sqlite' AND positionCaseInsensitiveUTF8(raw_json, '${cursor_sqlite_keyword}') > 0" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.tool_io WHERE source_name = 'cursor-sqlite' AND tool_call_id = 'cursor-sqlite-tool-${run_stamp}'" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_documents WHERE positionCaseInsensitiveUTF8(text_content, '${pi_keyword}') > 0" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_postings WHERE term = '${pi_keyword}'" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_documents WHERE positionCaseInsensitiveUTF8(text_content, '${hermes_keyword}') > 0" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_postings WHERE term = '${hermes_keyword}'" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_documents WHERE positionCaseInsensitiveUTF8(text_content, '${hermes_session_keyword}') > 0" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_postings WHERE term = '${hermes_session_keyword}'" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_documents WHERE positionCaseInsensitiveUTF8(text_content, '${nac_keyword}') > 0" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_postings WHERE term = '${nac_keyword}'" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.tool_io WHERE source_name = 'ci-nac' AND tool_call_id = '${nac_tool_call_id}'" 120

  echo "[e2e] checking ClickHouse normalized ingest rows"
  assert_clickhouse_count "$clickhouse_url" "non-Qwen ingest errors" "SELECT count() FROM ${clickhouse_database}.ingest_errors WHERE source_name != 'qwen-code'" "0"
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.ingest_errors WHERE source_name = 'qwen-code' AND error_kind = 'timestamp_parse_error'" 120
  assert_clickhouse_count "$clickhouse_url" "Qwen malformed timestamp logical error" "SELECT uniqExact(tuple(source_file, source_generation, source_line_no, error_kind)) FROM ${clickhouse_database}.ingest_errors WHERE source_name = 'qwen-code' AND error_kind = 'timestamp_parse_error'" "1"

  assert_clickhouse_count "$clickhouse_url" "codex unique raw rows" "SELECT uniqExact(raw_json_hash) FROM ${clickhouse_database}.raw_events WHERE source_name = 'ci-codex'" "8"
  assert_clickhouse_count "$clickhouse_url" "codex event rows" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-codex'" "8"
  assert_clickhouse_count "$clickhouse_url" "codex link rows" "SELECT count() FROM ${clickhouse_database}.event_links FINAL WHERE source_name = 'ci-codex' AND link_type = 'parent_event' AND linked_external_id = 'msg-user-${run_stamp}'" "1"
  assert_clickhouse_count "$clickhouse_url" "codex tool rows" "SELECT count() FROM ${clickhouse_database}.tool_io FINAL WHERE source_name = 'ci-codex' AND tool_call_id = 'codex-tool-${run_stamp}'" "2"
  assert_clickhouse_count "$clickhouse_url" "codex tool request fields" "SELECT count() FROM ${clickhouse_database}.tool_io FINAL WHERE source_name = 'ci-codex' AND tool_call_id = 'codex-tool-${run_stamp}' AND tool_name = 'Read' AND tool_phase = 'request'" "1"
  assert_clickhouse_count "$clickhouse_url" "codex plain-git event identity" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-codex' AND event_kind = 'tool_call' AND project_id != '' AND worktree_root = '${codex_project_dir}'" "1"
  assert_clickhouse_count "$clickhouse_url" "codex plain-git tool identity" "SELECT count() FROM ${clickhouse_database}.tool_io FINAL WHERE source_name = 'ci-codex' AND tool_call_id = 'codex-tool-${run_stamp}' AND tool_phase = 'request' AND project_id != '' AND repo_rel_path = 'Cargo.toml' AND worktree_root = '${codex_project_dir}'" "1"
  assert_clickhouse_count "$clickhouse_url" "codex harness/session fields" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-codex' AND harness = 'codex' AND session_id = '${codex_session_id}'" "8"
  assert_clickhouse_count "$clickhouse_url" "codex model fields" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-codex' AND model IN ('gpt-5.3-codex', 'gpt-5.3-codex-spark')" "7"
  assert_clickhouse_count "$clickhouse_url" "codex token buckets" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-codex' AND payload_type = 'token_count' AND input_tokens = 17 AND output_tokens = 4 AND cache_read_tokens = 3 AND cache_write_tokens = 2 AND token_usage_buckets['input_text'] = 12 AND token_usage_buckets['output_text'] = 3 AND token_usage_buckets['reasoning'] = 1" "1"

  # Simulate rows ingested before normalized project fields existed. The MCP
  # smoke below must recover only this retained, structured path + cwd evidence
  # and map it back to the launch repository without resetting checkpoints.
  clickhouse_scalar "$clickhouse_url" "INSERT INTO ${clickhouse_database}.tool_io SELECT * REPLACE ('' AS project_id, '' AS repo_rel_path, '' AS worktree_root, event_version + 100 AS event_version) FROM ${clickhouse_database}.tool_io FINAL WHERE source_name = 'ci-codex' AND tool_call_id = 'codex-tool-${run_stamp}' AND tool_phase = 'request'" >/dev/null
  clickhouse_scalar "$clickhouse_url" "INSERT INTO ${clickhouse_database}.events SELECT * REPLACE ('' AS project_id, '' AS repo_rel_path, '' AS worktree_root, event_version + 100 AS event_version) FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-codex' AND event_kind = 'tool_call' AND tool_call_id = 'codex-tool-${run_stamp}'" >/dev/null
  assert_clickhouse_count "$clickhouse_url" "codex legacy tool identity fixture" "SELECT count() FROM ${clickhouse_database}.tool_io FINAL WHERE source_name = 'ci-codex' AND tool_call_id = 'codex-tool-${run_stamp}' AND tool_phase = 'request' AND project_id = '' AND repo_rel_path = '' AND worktree_root = ''" "1"

  # Negative legacy fixtures: a top-level path plus nested path evidence, and
  # a nested-only path, may match the file tail but must never invent a root.
  clickhouse_scalar "$clickhouse_url" "INSERT INTO ${clickhouse_database}.tool_io SELECT * REPLACE ('${codex_multi_path_session_id}' AS session_id, concat(event_uid, '-multi-path') AS event_uid, 'codex-multi-path-${run_stamp}' AS tool_call_id, '{\"path\":\"Cargo.toml\",\"options\":{\"file_path\":\"other.rs\"}}' AS input_json, '' AS project_id, '' AS repo_rel_path, '' AS worktree_root, event_version + 200 AS event_version) FROM ${clickhouse_database}.tool_io FINAL WHERE source_name = 'ci-codex' AND tool_call_id = 'codex-tool-${run_stamp}' AND tool_phase = 'request'" >/dev/null
  clickhouse_scalar "$clickhouse_url" "INSERT INTO ${clickhouse_database}.events SELECT * REPLACE ('${codex_multi_path_session_id}' AS session_id, concat(event_uid, '-multi-path') AS event_uid, 'codex-multi-path-${run_stamp}' AS tool_call_id, '' AS project_id, '' AS repo_rel_path, '' AS worktree_root, event_version + 200 AS event_version) FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-codex' AND event_kind = 'tool_call' AND tool_call_id = 'codex-tool-${run_stamp}'" >/dev/null
  clickhouse_scalar "$clickhouse_url" "INSERT INTO ${clickhouse_database}.tool_io SELECT * REPLACE ('${codex_nested_path_session_id}' AS session_id, concat(event_uid, '-nested-path') AS event_uid, 'codex-nested-path-${run_stamp}' AS tool_call_id, '{\"options\":{\"path\":\"Cargo.toml\"}}' AS input_json, '' AS project_id, '' AS repo_rel_path, '' AS worktree_root, event_version + 300 AS event_version) FROM ${clickhouse_database}.tool_io FINAL WHERE source_name = 'ci-codex' AND tool_call_id = 'codex-tool-${run_stamp}' AND tool_phase = 'request'" >/dev/null
  clickhouse_scalar "$clickhouse_url" "INSERT INTO ${clickhouse_database}.events SELECT * REPLACE ('${codex_nested_path_session_id}' AS session_id, concat(event_uid, '-nested-path') AS event_uid, 'codex-nested-path-${run_stamp}' AS tool_call_id, '' AS project_id, '' AS repo_rel_path, '' AS worktree_root, event_version + 300 AS event_version) FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-codex' AND event_kind = 'tool_call' AND tool_call_id = 'codex-tool-${run_stamp}'" >/dev/null

  assert_clickhouse_count "$clickhouse_url" "claude unique raw rows" "SELECT uniqExact(raw_json_hash) FROM ${clickhouse_database}.raw_events WHERE source_name = 'ci-claude'" "3"
  assert_clickhouse_count "$clickhouse_url" "claude event rows" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-claude'" "5"
  # Every claude event carries the record-level cwd in the native column;
  # --project-only scoping reads it to resolve the session's origin directory.
  assert_clickhouse_count "$clickhouse_url" "claude cwd column populated" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-claude' AND cwd = '${claude_project_dir}'" "5"
  assert_clickhouse_count "$clickhouse_url" "claude non-Git directory identity populated" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-claude' AND startsWith(project_id, 'dir:') AND worktree_root = '${claude_project_dir}'" "5"
  assert_clickhouse_count "$clickhouse_url" "claude link rows" "SELECT count() FROM ${clickhouse_database}.event_links FINAL WHERE source_name = 'ci-claude'" "6"
  assert_clickhouse_count "$clickhouse_url" "claude tool rows" "SELECT count() FROM ${clickhouse_database}.tool_io FINAL WHERE source_name = 'ci-claude' AND tool_call_id = 'claude-tool-${run_stamp}'" "2"
  assert_clickhouse_count "$clickhouse_url" "claude non-Git tool path normalized" "SELECT count() FROM ${clickhouse_database}.tool_io FINAL WHERE source_name = 'ci-claude' AND tool_call_id = 'claude-tool-${run_stamp}' AND tool_phase = 'request' AND startsWith(project_id, 'dir:') AND worktree_root = '${claude_project_dir}' AND repo_rel_path = 'Cargo.toml'" "1"
  assert_clickhouse_count "$clickhouse_url" "claude harness/session fields" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-claude' AND harness = 'claude-code' AND inference_provider = 'anthropic' AND session_id = '${claude_session_id}'" "5"
  assert_clickhouse_count "$clickhouse_url" "claude model fields" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-claude' AND model = 'claude-opus-4-5-20251101'" "3"
  assert_clickhouse_count "$clickhouse_url" "claude token buckets" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-claude' AND actor_kind = 'assistant' AND input_tokens = 9 AND output_tokens = 5 AND token_usage_buckets['input_text'] = 9 AND token_usage_buckets['output_text'] = 5" "3"

  assert_clickhouse_count "$clickhouse_url" "kimi unique raw rows" "SELECT uniqExact(raw_json_hash) FROM ${clickhouse_database}.raw_events WHERE source_name = 'ci-kimi'" "8"
  assert_clickhouse_count "$clickhouse_url" "kimi event rows" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-kimi'" "7"
  assert_clickhouse_count "$clickhouse_url" "kimi link rows" "SELECT count() FROM ${clickhouse_database}.event_links FINAL WHERE source_name = 'ci-kimi'" "0"
  assert_clickhouse_count "$clickhouse_url" "kimi tool rows" "SELECT count() FROM ${clickhouse_database}.tool_io FINAL WHERE source_name = 'ci-kimi' AND tool_call_id = 'kimi-tool-${run_stamp}'" "2"
  assert_clickhouse_count "$clickhouse_url" "kimi domain fields" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-kimi' AND harness = 'kimi-cli' AND inference_provider = 'moonshot' AND session_id = '${kimi_raw_session_id}' AND model = 'kimi-cli'" "7"
  assert_clickhouse_count "$clickhouse_url" "kimi token buckets" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-kimi' AND payload_type = 'token_count' AND input_tokens = 13 AND output_tokens = 5 AND cache_read_tokens = 2 AND cache_write_tokens = 1 AND token_usage_buckets['input_text'] = 10 AND token_usage_buckets['output_text'] = 5 AND token_usage_buckets['input_cache_read'] = 2 AND token_usage_buckets['input_cache_write'] = 1" "1"

  # raw_events is append-only and its sink is intentionally at-least-once; count
  # persisted record identities rather than physical retry copies.
  assert_clickhouse_count "$clickhouse_url" "Qwen logical raw rows" "SELECT count() FROM (SELECT source_file, source_generation, source_line_no, source_offset, raw_json_hash FROM ${clickhouse_database}.raw_events WHERE source_name = 'qwen-code' GROUP BY source_file, source_generation, source_line_no, source_offset, raw_json_hash)" "15"
  assert_clickhouse_count "$clickhouse_url" "Qwen event rows" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'qwen-code'" "14"
  assert_clickhouse_count "$clickhouse_url" "Qwen parent links" "SELECT count() FROM ${clickhouse_database}.event_links FINAL WHERE source_name = 'qwen-code' AND link_type = 'parent_event'" "13"
  assert_clickhouse_count "$clickhouse_url" "Qwen raw-only records" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'qwen-code' AND item_id IN ('qwen-unknown-${run_stamp}', 'qwen-snapshot-${run_stamp}', 'qwen-binary-${run_stamp}')" "0"
  assert_clickhouse_count "$clickhouse_url" "Qwen domain fields" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'qwen-code' AND harness = 'qwen-code' AND session_id = '${qwen_session_id}' AND cwd = '${tmp_root}/qwen-project'" "14"
  assert_clickhouse_count "$clickhouse_url" "Qwen model without provider" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'qwen-code' AND model = 'qwen3-coder-plus' AND inference_provider = ''" "13"
  assert_clickhouse_count "$clickhouse_url" "Qwen token accounting" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'qwen-code' AND item_id = 'qwen-assistant-${run_stamp}' AND event_kind = 'reasoning' AND input_tokens = 120 AND output_tokens = 80 AND cache_read_tokens = 20 AND cache_write_tokens = 0 AND token_usage_buckets['input_text'] = 91 AND token_usage_buckets['output_text'] = 62 AND token_usage_buckets['input_image'] = 4 AND token_usage_buckets['output_audio'] = 11 AND token_usage_buckets['reasoning'] = 7 AND token_usage_buckets['server_tool_use'] = 5" "1"
  assert_clickhouse_count "$clickhouse_url" "Qwen title projection" "SELECT count() FROM ${clickhouse_database}.mcp_open_sessions FINAL WHERE session_id = '${qwen_session_id}' AND title = 'Qwen e2e ${run_stamp}'" "1"
  assert_clickhouse_count "$clickhouse_url" "Qwen reasoning row" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'qwen-code' AND item_id = 'qwen-assistant-${run_stamp}' AND event_kind = 'reasoning' AND has_reasoning = 1" "1"
  assert_clickhouse_count "$clickhouse_url" "Qwen projected assistant part order" "SELECT uniqExact(tuple(event_ordinal, event_class)) FROM ${clickhouse_database}.mcp_open_events FINAL WHERE session_id = '${qwen_session_id}' AND item_id = 'qwen-assistant-${run_stamp}' AND ((event_ordinal = 2 AND event_class = 'reasoning') OR (event_ordinal = 3 AND event_class = 'message') OR (event_ordinal = 4 AND event_class = 'tool_call'))" "3"
  assert_clickhouse_count "$clickhouse_url" "Qwen tool request and response rows" "SELECT count() FROM ${clickhouse_database}.tool_io FINAL WHERE source_name = 'qwen-code' AND tool_call_id IN ('qwen-call-search-${run_stamp}', 'qwen-call-fail-${run_stamp}')" "4"
  assert_clickhouse_count "$clickhouse_url" "Qwen failed tool response" "SELECT count() FROM ${clickhouse_database}.tool_io FINAL WHERE source_name = 'qwen-code' AND tool_call_id = 'qwen-call-fail-${run_stamp}' AND tool_phase = 'response' AND tool_error = 1" "1"
  assert_clickhouse_count "$clickhouse_url" "Qwen rewind event" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'qwen-code' AND item_id = 'qwen-rewind-${run_stamp}' AND op_kind = 'rewind'" "1"
  assert_clickhouse_count "$clickhouse_url" "Qwen abandoned branch parent" "SELECT count() FROM ${clickhouse_database}.event_links FINAL WHERE source_name = 'qwen-code' AND linked_external_id = 'qwen-compression-${run_stamp}' AND event_uid IN (SELECT event_uid FROM ${clickhouse_database}.events FINAL WHERE item_id = 'qwen-abandoned-${run_stamp}')" "1"
  assert_clickhouse_count "$clickhouse_url" "Qwen replacement branch parent" "SELECT count() FROM ${clickhouse_database}.event_links FINAL WHERE source_name = 'qwen-code' AND linked_external_id = 'qwen-replacement-user-${run_stamp}' AND event_uid IN (SELECT event_uid FROM ${clickhouse_database}.events FINAL WHERE item_id = 'qwen-replacement-assistant-${run_stamp}')" "1"
  assert_clickhouse_count "$clickhouse_url" "Qwen both rewind branches preserved" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'qwen-code' AND (position(text_content, 'Qwen abandoned branch ${run_stamp}') > 0 OR position(text_content, 'Qwen replacement branch answer ${run_stamp}') > 0)" "2"
  assert_clickhouse_count "$clickhouse_url" "Qwen compression is bounded" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'qwen-code' AND item_id = 'qwen-compression-${run_stamp}' AND event_kind = 'summary' AND text_content = '' AND position(payload_json, 'compressedHistory') = 0" "1"
  assert_clickhouse_count "$clickhouse_url" "Qwen malformed timestamp neighbor retained" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'qwen-code' AND item_id = 'qwen-bad-ts-${run_stamp}'" "1"
  assert_clickhouse_count "$clickhouse_url" "Qwen qualified Moraine mode" "SELECT count() FROM ${clickhouse_database}.mcp_open_sessions FINAL WHERE session_id = '${qwen_session_id}' AND mode = 'mcp_internal'" "1"
  assert_clickhouse_count "$clickhouse_url" "kiro unique raw rows" "SELECT uniqExact(raw_json_hash) FROM ${clickhouse_database}.raw_events WHERE source_name = 'ci-kiro'" "6"
  assert_clickhouse_count "$clickhouse_url" "kiro event rows" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-kiro'" "8"
  assert_clickhouse_count "$clickhouse_url" "kiro link rows" "SELECT count() FROM ${clickhouse_database}.event_links FINAL WHERE source_name = 'ci-kiro'" "0"
  assert_clickhouse_count "$clickhouse_url" "kiro tool rows" "SELECT count() FROM ${clickhouse_database}.tool_io FINAL WHERE source_name = 'ci-kiro' AND tool_call_id = 'kiro-tool-${run_stamp}'" "2"
  assert_clickhouse_count "$clickhouse_url" "kiro file path searchable" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-kiro' AND event_kind = 'tool_call' AND position(text_content, '/workspace/kiro-e2e.txt') > 0" "1"
  assert_clickhouse_count "$clickhouse_url" "kiro domain fields" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-kiro' AND harness = 'kiro-cli' AND inference_provider = 'kiro' AND session_id = '${kiro_session_id}' AND model = 'claude-sonnet-4' AND cwd = '${kiro_cwd}'" "8"
  assert_clickhouse_count "$clickhouse_url" "kiro metadata fields and credits" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-kiro' AND event_kind = 'session_meta' AND JSONExtractString(payload_json, 'title') = '${kiro_session_title}' AND input_tokens = 23 AND output_tokens = 11 AND token_usage_native_units['credits'] = 0.75" "1"

  # Kiro rewrites only the JSON sidecar when title and token metadata change.
  # The watcher must refresh session_meta without replaying the JSONL transcript.
  echo "[e2e] kiro sidecar update: refreshing metadata without transcript changes"
  cat > "$kiro_metadata_file" <<EOF
{
  "session_id": "${kiro_session_id}",
  "cwd": "${kiro_cwd}",
  "title": "${kiro_updated_session_title}",
  "created_at": "2026-02-16T12:00:08Z",
  "updated_at": "2026-02-16T12:00:14Z",
  "session_state": {
    "agent_name": "kiro_default",
    "rts_model_state": {
      "model_info": {"model_id": "claude-sonnet-4"}
    },
    "conversation_metadata": {
      "user_turn_metadatas": [
        {"input_token_count": 31, "output_token_count": 13, "metering_usage": [{"value": 1.25, "unit": "credit", "unitPlural": "credits"}]}
      ]
    }
  }
}
EOF

  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-kiro' AND event_kind = 'session_meta' AND JSONExtractString(payload_json, 'title') = '${kiro_updated_session_title}' AND input_tokens = 31 AND output_tokens = 13 AND token_usage_native_units['credits'] = 1.25" 120
  assert_clickhouse_count "$clickhouse_url" "kiro unique raw rows after sidecar update" "SELECT uniqExact(raw_json_hash) FROM ${clickhouse_database}.raw_events WHERE source_name = 'ci-kiro'" "7"
  assert_clickhouse_count "$clickhouse_url" "kiro event rows after sidecar update" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-kiro'" "8"
  assert_clickhouse_count "$clickhouse_url" "kiro session_meta collapses on re-emit" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-kiro' AND event_kind = 'session_meta'" "1"
  assert_clickhouse_count "$clickhouse_url" "Kiro ingest errors after sidecar update" "SELECT count() FROM ${clickhouse_database}.ingest_errors WHERE source_name = 'ci-kiro'" "0"

  assert_clickhouse_count "$clickhouse_url" "cursor unique raw rows" "SELECT uniqExact(raw_json_hash) FROM ${clickhouse_database}.raw_events WHERE source_name = 'ci-cursor'" "5"
  assert_clickhouse_count "$clickhouse_url" "cursor event rows" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-cursor'" "6"
  assert_clickhouse_count "$clickhouse_url" "cursor file link rows" "SELECT count() FROM ${clickhouse_database}.event_links FINAL WHERE source_name = 'ci-cursor' AND linked_external_id = '/workspace/cursor-e2e.txt'" "1"
  assert_clickhouse_count "$clickhouse_url" "cursor file path searchable" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-cursor' AND event_kind = 'tool_call' AND position(text_content, '/workspace/cursor-e2e.txt') > 0" "1"
  assert_clickhouse_count "$clickhouse_url" "cursor tool rows" "SELECT count() FROM ${clickhouse_database}.tool_io FINAL WHERE source_name = 'ci-cursor' AND tool_call_id = 'cursor-tool-${run_stamp}'" "2"
  assert_clickhouse_count "$clickhouse_url" "cursor domain fields" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-cursor' AND harness = 'cursor' AND inference_provider = 'cursor' AND session_id = '${cursor_session_id}'" "5"
  assert_clickhouse_count "$clickhouse_url" "cursor timestamp fallback" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-cursor' AND session_id = '${cursor_fallback_session_id}' AND event_ts > toDateTime64('2026-01-01', 3)" "1"

  # cursor_sqlite first poll: 4 changed kv rows synthesize 5 events — one
  # session_meta (composer), one user message, one reasoning, and the
  # completed tool bubble's tool_call + tool_result pair.
  assert_clickhouse_count "$clickhouse_url" "cursor sqlite unique raw rows" "SELECT uniqExact(raw_json_hash) FROM ${clickhouse_database}.raw_events WHERE source_name = 'cursor-sqlite'" "4"
  assert_clickhouse_count "$clickhouse_url" "cursor sqlite event rows" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'cursor-sqlite'" "5"
  assert_clickhouse_count "$clickhouse_url" "cursor sqlite event kinds" "SELECT uniqExact(event_kind) FROM ${clickhouse_database}.events FINAL WHERE source_name = 'cursor-sqlite' AND event_kind IN ('session_meta', 'message', 'reasoning', 'tool_call', 'tool_result')" "5"
  assert_clickhouse_count "$clickhouse_url" "cursor sqlite tool rows" "SELECT count() FROM ${clickhouse_database}.tool_io FINAL WHERE source_name = 'cursor-sqlite' AND tool_call_id = 'cursor-sqlite-tool-${run_stamp}' AND tool_name = 'edit_file_v2'" "2"
  assert_clickhouse_count "$clickhouse_url" "cursor sqlite domain fields" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'cursor-sqlite' AND harness = 'cursor' AND inference_provider = 'cursor' AND session_id = '${cursor_sqlite_session_id}'" "5"
  assert_clickhouse_count "$clickhouse_url" "cursor sqlite session title" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'cursor-sqlite' AND event_kind = 'session_meta' AND JSONExtractString(payload_json, 'title') = '${cursor_sqlite_session_title}'" "1"

  # Live update: Cursor mutates state.vscdb in place while a conversation
  # runs. Append an assistant bubble (growing the composer's header list) to
  # prove the watcher/sidecar/reconcile path re-polls a changed database, and
  # that the re-emitted composer collapses onto its stable event UID.
  echo "[e2e] cursor sqlite live update: appending an assistant bubble"
  "$python_bin" - \
    "$cursor_sqlite_fixture_file" \
    "$cursor_sqlite_session_id" \
    "local e2e cursor sqlite assistant reply ${cursor_sqlite_live_keyword}" <<'PY'
import json
import sqlite3
import sys

db_path, composer_id, reply_text = sys.argv[1:4]
reply_bubble = "dddddddd-4444-4444-8444-444444444444"

connection = sqlite3.connect(db_path, timeout=30)
row = connection.execute(
    "SELECT value FROM cursorDiskKV WHERE key = ?",
    (f"composerData:{composer_id}",),
).fetchone()
composer = json.loads(row[0])
composer["fullConversationHeadersOnly"].append({"bubbleId": reply_bubble, "type": 2})
composer["lastUpdatedAt"] = 1771243224000
bubble = {
    "_v": 3,
    "type": 2,
    "bubbleId": reply_bubble,
    "createdAt": "2026-02-16T12:00:24.000Z",
    "text": reply_text,
    "turnDurationMs": 1800,
}
connection.executemany(
    "INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)",
    [
        (f"composerData:{composer_id}", json.dumps(composer)),
        (f"bubbleId:{composer_id}:{reply_bubble}", json.dumps(bubble)),
    ],
)
connection.commit()
connection.close()
PY

  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.events WHERE source_name = 'cursor-sqlite' AND positionCaseInsensitiveUTF8(text_content, '${cursor_sqlite_live_keyword}') > 0" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.raw_events WHERE source_name = 'cursor-sqlite' AND positionCaseInsensitiveUTF8(raw_json, '${cursor_sqlite_live_keyword}') > 0" 120
  # Two kv rows changed (new bubble + mutated composer), so two new raw rows;
  # the events table gains only the assistant message because the composer's
  # session_meta re-emits under the same event UID and ReplacingMergeTree
  # collapses it.
  assert_clickhouse_count "$clickhouse_url" "cursor sqlite unique raw rows after live update" "SELECT uniqExact(raw_json_hash) FROM ${clickhouse_database}.raw_events WHERE source_name = 'cursor-sqlite'" "6"
  assert_clickhouse_count "$clickhouse_url" "cursor sqlite event rows after live update" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'cursor-sqlite'" "6"
  assert_clickhouse_count "$clickhouse_url" "cursor sqlite session_meta collapses on re-emit" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'cursor-sqlite' AND event_kind = 'session_meta'" "1"

  assert_clickhouse_count "$clickhouse_url" "pi unique raw rows" "SELECT uniqExact(raw_json_hash) FROM ${clickhouse_database}.raw_events WHERE source_name = 'ci-pi'" "7"
  assert_clickhouse_count "$clickhouse_url" "pi event rows" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-pi'" "9"
  assert_clickhouse_count "$clickhouse_url" "pi link rows" "SELECT count() FROM ${clickhouse_database}.event_links FINAL WHERE source_name = 'ci-pi' AND link_type = 'parent_event'" "7"
  assert_clickhouse_count "$clickhouse_url" "pi tool rows" "SELECT count() FROM ${clickhouse_database}.tool_io FINAL WHERE source_name = 'ci-pi' AND tool_call_id = 'pi-tool-${run_stamp}'" "2"
  assert_clickhouse_count "$clickhouse_url" "pi harness/session fields" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-pi' AND harness = 'pi-coding-agent' AND session_id = '${pi_session_id}'" "9"
  assert_clickhouse_count "$clickhouse_url" "pi provider fields" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-pi' AND inference_provider = 'openai'" "4"
  assert_clickhouse_count "$clickhouse_url" "pi model fields" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-pi' AND model = 'gpt-5.2-chat-latest'" "6"
  assert_clickhouse_count "$clickhouse_url" "pi token buckets" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-pi' AND payload_type = 'thinking' AND input_tokens = 19 AND output_tokens = 8 AND cache_read_tokens = 2 AND cache_write_tokens = 1 AND token_usage_buckets['input_text'] = 16 AND token_usage_buckets['output_text'] = 8 AND token_usage_buckets['input_cache_read'] = 2 AND token_usage_buckets['input_cache_write'] = 1" "1"

  assert_clickhouse_count "$clickhouse_url" "hermes trajectory unique raw rows" "SELECT uniqExact(raw_json_hash) FROM ${clickhouse_database}.raw_events WHERE source_name = 'ci-hermes-trajectory'" "1"
  assert_clickhouse_count "$clickhouse_url" "hermes trajectory event rows" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-hermes-trajectory'" "6"
  assert_clickhouse_count "$clickhouse_url" "hermes trajectory link rows" "SELECT count() FROM ${clickhouse_database}.event_links FINAL WHERE source_name = 'ci-hermes-trajectory'" "0"
  assert_clickhouse_count "$clickhouse_url" "hermes trajectory tool rows" "SELECT count() FROM ${clickhouse_database}.tool_io FINAL WHERE source_name = 'ci-hermes-trajectory' AND tool_call_id = 'hermes-tool-${run_stamp}' AND tool_name = 'weather'" "2"
  assert_clickhouse_count "$clickhouse_url" "hermes trajectory domain fields" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-hermes-trajectory' AND harness = 'hermes' AND inference_provider = 'anthropic' AND model = 'claude-sonnet-4.6'" "6"
  # NAC initial snapshot: one seven-record local parent, one three-record
  # remote parent, one five-record local worker, and one three-record remote
  # worker. Remote durable history is ingested without local project
  # attribution; forbidden credential/host columns never enter storage.
  assert_clickhouse_count "$clickhouse_url" "nac unique raw rows" "SELECT uniqExact(raw_json_hash) FROM ${clickhouse_database}.raw_events WHERE source_name = 'ci-nac'" "18"
  assert_clickhouse_count "$clickhouse_url" "nac event rows" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-nac'" "18"
  assert_clickhouse_count "$clickhouse_url" "nac parent event rows" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-nac' AND session_id = '${nac_normalized_session_id}'" "7"
  assert_clickhouse_count "$clickhouse_url" "nac local worker rows" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-nac' AND agent_label = '${nac_local_worker_id}'" "5"
  assert_clickhouse_count "$clickhouse_url" "nac remote metadata rows" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-nac' AND event_kind = 'session_meta' AND JSONExtractString(payload_json, 'cwd_scope') = 'remote'" "2"
  assert_clickhouse_count "$clickhouse_url" "nac remote rows have no local project attribution" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-nac' AND JSONExtractString(payload_json, 'cwd_scope') = 'remote' AND project_id = '' AND worktree_root = ''" "6"
  assert_clickhouse_count "$clickhouse_url" "nac remote bodies retained in raw storage" "SELECT uniqExact(raw_json_hash) FROM ${clickhouse_database}.raw_events WHERE source_name = 'ci-nac' AND position(raw_json, '${nac_remote_keyword}') > 0" "4"
  assert_clickhouse_count "$clickhouse_url" "nac remote bodies retained in normalized storage" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-nac' AND (position(text_content, '${nac_remote_keyword}') > 0 OR position(payload_json, '${nac_remote_keyword}') > 0)" "4"
  assert_clickhouse_count "$clickhouse_url" "nac credentials absent from raw storage" "SELECT count() FROM ${clickhouse_database}.raw_events WHERE source_name = 'ci-nac' AND position(raw_json, '${nac_secret_sentinel}') > 0" "0"
  assert_clickhouse_count "$clickhouse_url" "nac credentials absent from normalized storage" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-nac' AND (position(text_content, '${nac_secret_sentinel}') > 0 OR position(payload_json, '${nac_secret_sentinel}') > 0)" "0"
  assert_clickhouse_count "$clickhouse_url" "nac credentials absent from search" "SELECT count() FROM ${clickhouse_database}.search_documents WHERE positionCaseInsensitiveUTF8(text_content, '${nac_secret_sentinel}') > 0" "0"
  assert_clickhouse_count "$clickhouse_url" "nac provider and model fields" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-nac' AND inference_provider = 'openrouter' AND model = 'z-ai/glm-5.2'" "18"
  assert_clickhouse_count "$clickhouse_url" "nac token buckets" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-nac' AND actor_kind = 'assistant' AND input_tokens = 13 AND output_tokens = 5 AND cache_read_tokens = 2 AND cache_write_tokens = 1 AND token_usage_buckets['input_text'] = 10 AND token_usage_buckets['output_text'] = 2 AND token_usage_buckets['reasoning'] = 3" "1"
  assert_clickhouse_count "$clickhouse_url" "nac response duration" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-nac' AND actor_kind = 'assistant' AND latency_ms = 2100" "1"
  assert_clickhouse_count "$clickhouse_url" "nac canonical tool rows" "SELECT count() FROM ${clickhouse_database}.tool_io FINAL WHERE source_name = 'ci-nac' AND tool_call_id = '${nac_tool_call_id}' AND tool_name = 'search'" "2"
  assert_clickhouse_count "$clickhouse_url" "nac qualified tool provenance" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-nac' AND tool_call_id = '${nac_tool_call_id}' AND JSONExtractString(payload_json, 'raw_tool_name') = 'mcp__moraine__search'" "2"
  assert_clickhouse_count "$clickhouse_url" "nac worker parent links" "SELECT count() FROM ${clickhouse_database}.event_links FINAL WHERE source_name = 'ci-nac' AND link_type = 'subagent_parent'" "2"
  assert_clickhouse_count "$clickhouse_url" "nac tool response link" "SELECT count() FROM ${clickhouse_database}.event_links FINAL WHERE source_name = 'ci-nac' AND link_type = 'tool_use_id'" "1"

  echo "[e2e] nac sqlite live update: appending an assistant message"
  "$python_bin" - "$nac_fixture_file" "$nac_session_id" "$nac_live_keyword" <<'PY'
import json
import sqlite3
import sys

db_path, session_id, live_keyword = sys.argv[1:4]
connection = sqlite3.connect(db_path, timeout=30)
messages = json.loads(
    connection.execute(
        "SELECT messages_json FROM sessions WHERE session_id = ?",
        (session_id,),
    ).fetchone()[0]
)
messages.append({"role": "assistant", "content": f"nac live reply {live_keyword}"})
connection.execute(
    "UPDATE sessions SET messages_json = ?, updated_at = ? WHERE session_id = ?",
    (json.dumps(messages), "2026-02-16 12:00:20.000000000", session_id),
)
connection.commit()
connection.close()
PY
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.events WHERE source_name = 'ci-nac' AND positionCaseInsensitiveUTF8(text_content, '${nac_live_keyword}') > 0" 120
  assert_clickhouse_count "$clickhouse_url" "nac unique raw rows after live update" "SELECT uniqExact(raw_json_hash) FROM ${clickhouse_database}.raw_events WHERE source_name = 'ci-nac'" "20"
  assert_clickhouse_count "$clickhouse_url" "nac event rows after live update" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-nac'" "19"
  assert_clickhouse_count "$clickhouse_url" "nac parent rows after live update" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-nac' AND session_id = '${nac_normalized_session_id}'" "8"
  assert_clickhouse_count "$clickhouse_url" "nac parent metadata collapses on live update" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-nac' AND session_id = '${nac_normalized_session_id}' AND event_kind = 'session_meta'" "1"

  echo "[e2e] nac sqlite config-only update"
  "$python_bin" - "$nac_fixture_file" "$nac_session_id" <<'PY'
import sqlite3
import sys

connection = sqlite3.connect(sys.argv[1], timeout=30)
connection.execute(
    "UPDATE sessions SET reasoning_effort = 'low', updated_at = ? WHERE session_id = ?",
    ("2026-02-16 12:00:21.000000000", sys.argv[2]),
)
connection.commit()
connection.close()
PY
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-nac' AND session_id = '${nac_normalized_session_id}' AND event_kind = 'session_meta' AND JSONExtractString(payload_json, 'reasoning_effort') = 'low'" 120
  assert_clickhouse_count "$clickhouse_url" "nac config-only update preserves canonical event count" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-nac'" "19"

  echo "[e2e] nac sqlite prior-message replacement"
  "$python_bin" - "$nac_fixture_file" "$nac_session_id" <<'PY'
import json
import sqlite3
import sys

connection = sqlite3.connect(sys.argv[1], timeout=30)
messages = json.loads(
    connection.execute(
        "SELECT messages_json FROM sessions WHERE session_id = ?",
        (sys.argv[2],),
    ).fetchone()[0]
)
messages[0]["content"] += " NAC_PRIOR_MESSAGE_REPLACED"
connection.execute(
    "UPDATE sessions SET messages_json = ?, updated_at = ? WHERE session_id = ?",
    (json.dumps(messages), "2026-02-16 12:00:22.000000000", sys.argv[2]),
)
connection.commit()
connection.close()
PY
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-nac' AND session_id = '${nac_normalized_session_id}' AND position(text_content, 'NAC_PRIOR_MESSAGE_REPLACED') > 0" 120
  assert_clickhouse_count "$clickhouse_url" "nac prior-message replacement preserves canonical event count" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-nac'" "19"
  assert_clickhouse_count "$clickhouse_url" "nac prior-message replacement reuses one coordinate" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-nac' AND session_id = '${nac_normalized_session_id}' AND source_offset = 1 AND position(text_content, 'NAC_PRIOR_MESSAGE_REPLACED') > 0" "1"

  echo "[e2e] nac sqlite truncation preserves archived history"
  "$python_bin" - "$nac_fixture_file" "$nac_session_id" <<'PY'
import json
import sqlite3
import sys

connection = sqlite3.connect(sys.argv[1], timeout=30)
messages = json.loads(
    connection.execute(
        "SELECT messages_json FROM sessions WHERE session_id = ?",
        (sys.argv[2],),
    ).fetchone()[0]
)
messages.pop()
connection.execute(
    "UPDATE sessions SET messages_json = ?, updated_at = ? WHERE session_id = ?",
    (json.dumps(messages), "2026-02-16 12:00:23.000000000", sys.argv[2]),
)
connection.commit()
connection.close()
PY
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-nac' AND session_id = '${nac_normalized_session_id}' AND event_kind = 'session_meta' AND JSONExtractString(payload_json, 'updated_at') = '2026-02-16T12:00:23.000000Z'" 120
  assert_clickhouse_count "$clickhouse_url" "nac truncated message remains archived" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-nac' AND positionCaseInsensitiveUTF8(text_content, '${nac_live_keyword}') > 0" "1"
  assert_clickhouse_count "$clickhouse_url" "nac truncation preserves canonical event count" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-nac'" "19"
  local nac_heartbeat_before_noop
  nac_heartbeat_before_noop="$(clickhouse_scalar "$clickhouse_url" "SELECT toString(max(ts)) FROM ${clickhouse_database}.ingest_heartbeats")"
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.ingest_heartbeats WHERE ts > parseDateTime64BestEffort('${nac_heartbeat_before_noop}') AND queue_depth = 0 AND files_active = 0" 120

  # A stat-only change must be consumed by the volatile poll state without
  # appending raw history or persisting a durable checkpoint.
  local nac_raw_rows_before_noop
  local nac_checkpoint_updated_at_before_noop
  local nac_heartbeat_before_touch
  # The normalized truncation row can become visible before every raw insert
  # from the same queued scan. Establish the no-op baseline only after the raw
  # count has remained unchanged across consecutive poll intervals.
  nac_raw_rows_before_noop="$(wait_for_clickhouse_scalar_stable "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.raw_events WHERE source_name = 'ci-nac'" 30 3)"
  nac_checkpoint_updated_at_before_noop="$(clickhouse_scalar "$clickhouse_url" "SELECT toString(max(updated_at)) FROM ${clickhouse_database}.ingest_checkpoints WHERE source_name = 'ci-nac'")"
  nac_heartbeat_before_touch="$(clickhouse_scalar "$clickhouse_url" "SELECT toString(max(ts)) FROM ${clickhouse_database}.ingest_heartbeats")"
  "$python_bin" - "$nac_fixture_file" <<'PY'
import os
import sys

os.utime(sys.argv[1], None)
PY
  sleep 3
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.ingest_heartbeats WHERE ts > parseDateTime64BestEffort('${nac_heartbeat_before_touch}') AND queue_depth = 0 AND files_active = 0" 120
  assert_clickhouse_scalar "$clickhouse_url" "nac stat-only no-op writes no raw rows" "SELECT count() FROM ${clickhouse_database}.raw_events WHERE source_name = 'ci-nac'" "$nac_raw_rows_before_noop"
  assert_clickhouse_scalar "$clickhouse_url" "nac stat-only no-op writes no durable checkpoint" "SELECT toString(max(updated_at)) FROM ${clickhouse_database}.ingest_checkpoints WHERE source_name = 'ci-nac'" "$nac_checkpoint_updated_at_before_noop"

  # Removing the highest episode forces a high-water reset. The source-side
  # deletion is archival: previously indexed remote metadata and durable bodies
  # remain retrievable.
  echo "[e2e] nac sqlite source deletion follows archival policy"
  "$python_bin" - "$nac_fixture_file" "$nac_remote_worker_id" <<'PY'
import sqlite3
import sys

connection = sqlite3.connect(sys.argv[1], timeout=30)
connection.execute(
    "DELETE FROM episodes WHERE session_id = ?",
    (sys.argv[2],),
)
connection.commit()
connection.close()
PY
  sleep 3
  assert_clickhouse_count "$clickhouse_url" "nac deleted remote metadata remains archived" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-nac' AND event_kind = 'session_meta' AND JSONExtractString(payload_json, 'cwd_scope') = 'remote'" "2"
  assert_clickhouse_count "$clickhouse_url" "nac deleted remote body remains archived" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-nac' AND (position(text_content, '${nac_remote_keyword}') > 0 OR position(payload_json, '${nac_remote_keyword}') > 0)" "4"


  assert_clickhouse_count "$clickhouse_url" "hermes session unique raw rows" "SELECT uniqExact(raw_json_hash) FROM ${clickhouse_database}.raw_events WHERE source_name = 'ci-hermes-session'" "5"
  assert_clickhouse_count "$clickhouse_url" "hermes session event rows" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-hermes-session'" "6"
  assert_clickhouse_count "$clickhouse_url" "hermes session link rows" "SELECT count() FROM ${clickhouse_database}.event_links FINAL WHERE source_name = 'ci-hermes-session'" "0"
  assert_clickhouse_count "$clickhouse_url" "hermes session tool rows" "SELECT count() FROM ${clickhouse_database}.tool_io FINAL WHERE source_name = 'ci-hermes-session' AND tool_call_id = 'hermes-session-tool-${run_stamp}' AND tool_name = 'shell'" "2"
  assert_clickhouse_count "$clickhouse_url" "hermes session domain fields" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-hermes-session' AND harness = 'hermes' AND inference_provider = 'anthropic' AND session_id = '${hermes_raw_session_id}' AND model = 'claude-opus-4-6'" "6"
  assert_clickhouse_count "$clickhouse_url" "token bucket map keys on all events" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE NOT hasAll(mapKeys(token_usage_buckets), ['input_text', 'output_text', 'input_cache_read', 'input_cache_write', 'reasoning'])" "0"

  local hermes_trajectory_session_id
  hermes_trajectory_session_id="$(clickhouse_scalar "$clickhouse_url" "SELECT any(session_id) FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-hermes-trajectory'")"
  if [[ -z "$hermes_trajectory_session_id" ]]; then
    echo "[e2e] failed to resolve Hermes trajectory session id" >&2
    return 1
  fi

  echo "[e2e] checking canonical monitor API routes"
  local path
  for path in \
    "/api/v1/health" \
    "/api/v1/status" \
    "/api/v1/analytics?range=24h" \
    "/api/v1/tables" \
    "/api/v1/web-searches?limit=25" \
    "/api/v1/tables/v_conversation_trace?limit=500" \
    "/api/v1/sessions?since=all&limit=200"; do
    local body
    body="$(curl -fsS "http://127.0.0.1:${monitor_port}${path}")"
    printf '%s' "$body" | json_ok_true "$python_bin"
  done

  echo "[e2e] checking monitor API capabilities"
  local applied_schema_level
  applied_schema_level="$(clickhouse_scalar "$clickhouse_url" "SELECT max(version) FROM ${clickhouse_database}.schema_migrations")"
  local expected_server_version
  expected_server_version="$("$moraine_bin" --version)"
  expected_server_version="${expected_server_version##* }"
  local capabilities_body
  capabilities_body="$(curl -fsS "http://127.0.0.1:${monitor_port}/api/v1/capabilities")"
  printf '%s' "$capabilities_body" | "$python_bin" -c '
import json
import sys

data = json.load(sys.stdin)
expected_server_version, expected_schema_level = sys.argv[1:]
expected_features = {
    "analytics": True,
    "sessions": True,
    "table_inspection": True,
    "web_searches": True,
}
valid = (
    isinstance(data, dict)
    and set(data) == {
        "ok",
        "server_version",
        "schema_migration_level",
        "features",
    }
    and data.get("ok") is True
    and data.get("server_version") == expected_server_version
    and data.get("schema_migration_level") == expected_schema_level
    and data.get("features") == expected_features
)
if not valid:
    print(
        "unexpected monitor capabilities payload: "
        f"expected server_version={expected_server_version!r}, "
        f"schema_migration_level={expected_schema_level!r}, "
        f"features={expected_features!r}; got {data!r}",
        file=sys.stderr,
    )
    raise SystemExit(1)
' "$expected_server_version" "$applied_schema_level"

  local monitor_api_url="http://127.0.0.1:${monitor_port}"
  echo "[e2e] checking one-release monitor API aliases"
  assert_monitor_api_alias "$python_bin" "$monitor_api_url" "$tmp_root" \
    "/api/v1/health" "/api/health"
  assert_monitor_api_alias "$python_bin" "$monitor_api_url" "$tmp_root" \
    "/api/v1/status" "/api/status"
  assert_monitor_api_alias "$python_bin" "$monitor_api_url" "$tmp_root" \
    "/api/v1/analytics?range=24h" "/api/analytics?range=24h"
  assert_monitor_api_alias "$python_bin" "$monitor_api_url" "$tmp_root" \
    "/api/v1/tables" "/api/tables"
  assert_monitor_api_alias "$python_bin" "$monitor_api_url" "$tmp_root" \
    "/api/v1/web-searches?limit=25" "/api/web-searches?limit=25"
  assert_monitor_api_alias "$python_bin" "$monitor_api_url" "$tmp_root" \
    "/api/v1/tables/v_conversation_trace?limit=500" \
    "/api/tables/v_conversation_trace?limit=500"
  assert_monitor_api_alias "$python_bin" "$monitor_api_url" "$tmp_root" \
    "/api/v1/sessions?since=all&limit=200" \
    "/api/sessions?since=all&limit=200"

  local sessions_body
  sessions_body="$(curl -fsS "http://127.0.0.1:${monitor_port}/api/v1/sessions?since=all&limit=200")"
  printf '%s' "$sessions_body" | "$python_bin" -c 'import json,sys; data=json.load(sys.stdin); sid=sys.argv[1]; ok=any(s.get("id")==sid and s.get("harness",{}).get("id")=="cursor" for s in data.get("sessions", [])); sys.exit(0 if ok else 1)' "$cursor_session_id"
  printf '%s' "$sessions_body" | "$python_bin" -c 'import json,sys; data=json.load(sys.stdin); sid=sys.argv[1]; ok=any(s.get("id")==sid and s.get("harness",{}).get("id")=="pi-coding-agent" for s in data.get("sessions", [])); sys.exit(0 if ok else 1)' "$pi_session_id"
  printf '%s' "$sessions_body" | "$python_bin" -c 'import json,sys; data=json.load(sys.stdin); sid=sys.argv[1]; ok=any(s.get("id")==sid and s.get("harness",{}).get("id")=="kiro-cli" for s in data.get("sessions", [])); sys.exit(0 if ok else 1)' "$kiro_session_id"

  # Regression for #388: turn_seq is computed by a running user-message window
  # over the events ReplacingMergeTree. Re-ingestion can briefly leave a
  # duplicate physical row live (same sort key, higher event_version) before a
  # background merge collapses it. v_all_events now reads `events FINAL`, so the
  # turn counter must not over-count that duplicate; otherwise search_sessions
  # mints a turn:<session>:<seq> handle that open() cannot resolve once the
  # merge fires (the original flake). We plant the duplicate ourselves and prove
  # the turn count is unchanged while the duplicate is demonstrably still live.
  echo "[e2e] checking turn_seq is stable against an un-merged duplicate event (#388)"
  local codex_turns_before
  codex_turns_before="$(clickhouse_scalar "$clickhouse_url" "SELECT max(turn_seq) FROM ${clickhouse_database}.v_conversation_trace WHERE session_id = '${codex_session_id}'")"
  # Stop background merges on `events` before planting the duplicate so the two
  # parts stay physically un-merged for the duration of these assertions. A
  # background ReplacingMergeTree merge is the only thing that collapses the
  # non-FINAL row count, and it fires nondeterministically — without this, the
  # "live (un-merged)" assertion below races the merge and flakes (it has been
  # observed to read 1 instead of 2). Observing the pre-merge state is the whole
  # point: it is exactly what #388 guards against. FINAL still dedups at read
  # time, so the FINAL / turn-count assertions are unaffected by the freeze.
  clickhouse_scalar "$clickhouse_url" "SYSTEM STOP MERGES ${clickhouse_database}.events" >/dev/null
  local codex_live_user_messages_before
  codex_live_user_messages_before="$(clickhouse_scalar "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.events WHERE source_name = 'ci-codex' AND session_id = '${codex_session_id}' AND actor_kind = 'user' AND event_kind = 'message'")"
  if [[ ! "$codex_live_user_messages_before" =~ ^[0-9]+$ ]] || (( codex_live_user_messages_before < 1 )); then
    echo "[e2e] expected at least one live codex user-message row before duplicate insert, got: ${codex_live_user_messages_before}" >&2
    return 1
  fi
  local codex_live_user_messages_after
  codex_live_user_messages_after="$((codex_live_user_messages_before + 1))"
  assert_clickhouse_count "$clickhouse_url" "codex user-message row collapses under FINAL before duplicate insert" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-codex' AND session_id = '${codex_session_id}' AND actor_kind = 'user' AND event_kind = 'message'" "1"
  # Plant a duplicate of the codex user message in a fresh, un-merged part:
  # SELECT * preserves ingested_at (so it lands in the same partition and shares
  # the sort key), REPLACE bumps event_version so FINAL keeps exactly one row.
  clickhouse_scalar "$clickhouse_url" "INSERT INTO ${clickhouse_database}.events SELECT * REPLACE (event_version + 1 AS event_version) FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-codex' AND session_id = '${codex_session_id}' AND actor_kind = 'user' AND event_kind = 'message'" >/dev/null
  assert_clickhouse_count "$clickhouse_url" "codex duplicate user-message row increases live physical count" "SELECT count() FROM ${clickhouse_database}.events WHERE source_name = 'ci-codex' AND session_id = '${codex_session_id}' AND actor_kind = 'user' AND event_kind = 'message'" "$codex_live_user_messages_after"
  assert_clickhouse_count "$clickhouse_url" "codex duplicate collapses under FINAL" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-codex' AND session_id = '${codex_session_id}' AND actor_kind = 'user' AND event_kind = 'message'" "1"
  assert_clickhouse_scalar "$clickhouse_url" "codex turn_seq unchanged by un-merged duplicate" "SELECT max(turn_seq) FROM ${clickhouse_database}.v_conversation_trace WHERE session_id = '${codex_session_id}'" "$codex_turns_before"
  assert_clickhouse_scalar "$clickhouse_url" "codex session summary turn count unchanged by un-merged duplicate" "SELECT total_turns FROM ${clickhouse_database}.v_session_summary WHERE session_id = '${codex_session_id}'" "$codex_turns_before"
  # Re-enable merges now that the pre-merge assertions have passed.
  clickhouse_scalar "$clickhouse_url" "SYSTEM START MERGES ${clickhouse_database}.events" >/dev/null
  # This fixture mutation bypasses the ingest sink, which normally refreshes
  # the MCP read model after every event batch. Reconcile the dirty session so
  # the retrieval smoke below exercises the same clean projection contract as
  # production ingestion.
  "$moraine_bin" db migrate --config "$config_path" >/dev/null

  # `/api/v1/sessions` intentionally keeps its dashboard shape without
  # eventCount. Derive the monitor-side canonical count through the existing
  # trace preview route, then compare both that count and sessions.endedAt with
  # MCP below.
  echo "[e2e] checking monitor/repository session count + last-activity parity"
  sessions_body="$(curl -fsS "http://127.0.0.1:${monitor_port}/api/v1/sessions?since=all&limit=200")"
  printf '%s' "$sessions_body" | "$python_bin" -c 'import json,sys; data=json.load(sys.stdin); sid=sys.argv[1]; expected=int(sys.argv[2]); session=next((s for s in data.get("sessions", []) if s.get("id")==sid), None); ok=session is not None and session.get("endedAt")==expected; sys.exit(0 if ok else 1)' "$codex_session_id" "1771243203900"
  local trace_body
  trace_body="$(curl -fsS "http://127.0.0.1:${monitor_port}/api/v1/tables/v_conversation_trace?limit=500")"
  printf '%s' "$trace_body" | "$python_bin" -c 'import json,sys; data=json.load(sys.stdin); sid=sys.argv[1]; expected=int(sys.argv[2]); count=sum(1 for row in data.get("rows", []) if row.get("session_id")==sid); sys.exit(0 if count==expected else 1)' "$codex_session_id" "8"

  # Capture a server-clock-bounded query_log window after `moraine up` and its
  # status probe have exited. Direct curl assertions are excluded below by
  # User-Agent, so every service-owned SELECT in this window must come from the
  # long-lived backend PID rather than a transient CLI/test repository.
  local query_log_start
  query_log_start="$(clickhouse_scalar "$clickhouse_url" "SELECT toString(now64(6))")"
  local cache_log_baseline
  cache_log_baseline="$("$python_bin" - "$backend_log" <<'PY'
from pathlib import Path
import sys

log_path = Path(sys.argv[1])
print(log_path.stat().st_size if log_path.is_file() else 0)
PY
)"

  echo "[e2e] checking first fresh stdio MCP process (codex cache miss)"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --working-dir "$codex_project_dir" \
    --query "$codex_keyword" \
    --expect-session-id "$codex_session_id" \
    --expect-open-text "$codex_trace_marker" \
    --expect-matching-search-hits "1" \
    --expect-event-count "8" \
    --expect-updated-at "2026-02-16T12:00:03.900Z" \
    --file-attention-path "Cargo.toml" \
    --file-attention-expect-absent-session-id "$claude_session_id" \
    --file-attention-expect-absent-session-id "$pi_session_id" \
    --file-attention-expect-absent-session-id "$codex_multi_path_session_id" \
    --file-attention-expect-absent-session-id "$codex_nested_path_session_id" \
    --write-tools-snapshot "$daemon_tools_snapshot"

  echo "[e2e] checking second fresh stdio MCP process (identical codex cache hit)"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --working-dir "$codex_project_dir" \
    --query "$codex_keyword" \
    --expect-session-id "$codex_session_id" \
    --expect-open-text "$codex_trace_marker" \
    --expect-matching-search-hits "1" \
    --expect-event-count "8" \
    --expect-updated-at "2026-02-16T12:00:03.900Z" \
    --file-attention-path "Cargo.toml" \
    --file-attention-expect-absent-session-id "$claude_session_id" \
    --file-attention-expect-absent-session-id "$pi_session_id" \
    --file-attention-expect-absent-session-id "$codex_multi_path_session_id" \
    --file-attention-expect-absent-session-id "$codex_nested_path_session_id" \
    --write-tools-snapshot "$daemon_tools_snapshot"

  echo "[e2e] checking shared-daemon MCP cache miss -> hit markers"
  wait_for_mcp_cache_sequence "$python_bin" "$backend_log" "$cache_log_baseline" 20

  local routed_cache_log_baseline
  routed_cache_log_baseline="$("$python_bin" - "$backend_log" <<'PY'
from pathlib import Path
import sys

log_path = Path(sys.argv[1])
print(log_path.stat().st_size if log_path.is_file() else 0)
PY
)"

  echo "[e2e] checking first named-backend MCP route through daemon"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --working-dir "$claude_project_dir" \
    --query "$claude_keyword" \
    --expect-session-id "$claude_session_id" \
    --expect-open-text "$claude_trace_marker" \
    --file-attention-path "Cargo.toml"

  echo "[e2e] checking named-backend HTTP route through shared daemon router"
  routed_sessions_body="$(curl -fsS \
    -H "X-Moraine-Project-Dir: ${claude_project_dir}" \
    "http://127.0.0.1:${monitor_port}/api/v1/sessions?since=all&limit=200")"
  printf '%s' "$routed_sessions_body" | "$python_bin" -c 'import json,sys; data=json.load(sys.stdin); present=sys.argv[1]; absent=sys.argv[2]; ids={row.get("id") for row in data.get("sessions", [])}; sys.exit(0 if present in ids and absent not in ids else 1)' "$claude_session_id" "$codex_session_id"
  routed_health_body="$(curl -fsS \
    -H "X-Moraine-Project-Dir: ${claude_project_dir}" \
    "http://127.0.0.1:${monitor_port}/api/v1/health")"
  printf '%s' "$routed_health_body" | "$python_bin" -c 'import json,sys; data=json.load(sys.stdin); sys.exit(0 if data.get("database")==sys.argv[1] else 1)' "$routed_clickhouse_database"

  echo "[e2e] checking named-backend repository/cache reuse on second MCP route"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --working-dir "$claude_project_dir" \
    --query "$claude_keyword" \
    --expect-session-id "$claude_session_id" \
    --expect-open-text "$claude_trace_marker" \
    --file-attention-path "Cargo.toml"
  wait_for_mcp_cache_sequence "$python_bin" "$backend_log" "$routed_cache_log_baseline" 20

  echo "[e2e] checking named route excludes default-only content"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --working-dir "$claude_project_dir" \
    --query "$codex_keyword" \
    --expect-no-results \
    --expect-session-id "$claude_session_id" \
    --expect-absent-session-id "$codex_session_id"

  # The ingest heartbeat interval is one second. Leave enough room inside this
  # same bounded window for a fresh ingest INSERT, then force query_log durable
  # before asserting exact role + PID attribution.
  sleep 3
  local query_log_end
  query_log_end="$(clickhouse_scalar "$clickhouse_url" "SELECT toString(now64(6))")"
  clickhouse_scalar "$clickhouse_url" "SYSTEM FLUSH LOGS" >/dev/null
  local query_log_window="event_time_microseconds >= parseDateTime64BestEffort('${query_log_start}') AND event_time_microseconds < parseDateTime64BestEffort('${query_log_end}')"
  local attributed_service_filter="startsWith(http_user_agent, 'moraine-') AND NOT startsWith(http_user_agent, 'moraine-clickhouse/') AND NOT startsWith(http_user_agent, 'moraine-conversations/')"

  echo "[e2e] checking bounded ClickHouse query ownership"
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' AND ${query_log_window} AND query_kind = 'Select' AND http_user_agent = '${expected_backend_ua}'" 30
  assert_clickhouse_count "$clickhouse_url" "service SELECTs use the backend or ingest UA/PID" "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' AND ${query_log_window} AND query_kind = 'Select' AND (${attributed_service_filter}) AND http_user_agent NOT IN ('${expected_backend_ua}', '${expected_ingest_ua}')" "0"
  assert_clickhouse_count "$clickhouse_url" "named read schema handshake runs once despite MCP/HTTP reuse" "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' AND ${query_log_window} AND http_user_agent = '${expected_backend_ua}' AND position(query, 'system.tables') > 0 AND position(query, 'schema_migrations') > 0" "1"
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' AND ${query_log_window} AND query_kind = 'Insert' AND http_user_agent = '${expected_ingest_ua}'" 30
  local file_attention_root_insert_prefix="INSERT INTO \`${clickhouse_database}\`.\`file_attention_project_roots\`"
  local routed_file_attention_root_insert_prefix="INSERT INTO \`${routed_clickhouse_database}\`.\`file_attention_project_roots\`"
  local allowed_backend_root_insert="http_user_agent = '${expected_backend_ua}' AND (startsWith(query, '${file_attention_root_insert_prefix}') OR startsWith(query, '${routed_file_attention_root_insert_prefix}'))"
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' AND ${query_log_window} AND query_kind = 'Insert' AND (${allowed_backend_root_insert})" 30
  assert_clickhouse_count "$clickhouse_url" "service INSERTs use the ingest UA/PID except backend project-root mappings" "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' AND ${query_log_window} AND query_kind = 'Insert' AND (${attributed_service_filter}) AND http_user_agent != '${expected_ingest_ua}' AND NOT (${allowed_backend_root_insert})" "0"

  echo "[e2e] checking MCP initialize/tools/search_sessions/open/list_sessions (claude)"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --query "$claude_keyword" \
    --expect-session-id "$claude_session_id" \
    --expect-open-text "$claude_trace_marker" \
    --file-attention-path "Cargo.toml"

  echo "[e2e] checking MCP initialize/tools/search_sessions/open/list_sessions (kimi)"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --query "$kimi_keyword" \
    --expect-session-id "$kimi_raw_session_id" \
    --expect-open-text "$kimi_trace_marker" \
    --file-attention-path "manifest.json"

  echo "[e2e] checking MCP exact harness/source filters (qwen)"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --query "$qwen_keyword" \
    --expect-session-id "$qwen_session_id" \
    --expect-open-text "$qwen_trace_marker" \
    --harness-filter "qwen-code" \
    --source-filter "qwen-code"

  echo "[e2e] checking Qwen-qualified Moraine calls are excluded from default search"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --query "$qwen_self_keyword" \
    --expect-session-id "$qwen_session_id" \
    --expect-no-results \
    --event-type "tool_call" \
    --harness-filter "qwen-code" \
    --source-filter "qwen-code"
  echo "[e2e] checking MCP initialize/tools/search_sessions/open/list_sessions (kiro)"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --query "$kiro_keyword" \
    --expect-session-id "$kiro_session_id" \
    --expect-open-text "$kiro_trace_marker" \
    --expect-event-count "8" \
    --file-attention-path "/workspace/kiro-e2e.txt"

  echo "[e2e] checking MCP initialize/tools/search_sessions/open/list_sessions (cursor)"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --query "$cursor_keyword" \
    --expect-session-id "$cursor_session_id" \
    --expect-open-text "$cursor_trace_marker" \
    --file-attention-path "/workspace/cursor-e2e.txt"

  echo "[e2e] checking MCP initialize/tools/search_sessions/open/list_sessions (cursor sqlite)"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --query "$cursor_sqlite_keyword" \
    --expect-session-id "$cursor_sqlite_session_id" \
    --expect-open-text "$cursor_sqlite_trace_marker" \
    --file-attention-path "/workspace/cursor-sqlite-e2e.txt"

  echo "[e2e] checking MCP initialize/tools/search_sessions/open/list_sessions (nac sqlite)"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --working-dir "$nac_project_dir" \
    --query "$nac_keyword" \
    --expect-session-id "$nac_normalized_session_id" \
    --expect-open-text "NAC_PRIOR_MESSAGE_REPLACED" \
    --expect-event-count "8" \
    --expect-updated-at "2026-02-16T12:00:05.000Z" \
    --expect-mode "mcp_internal" \
    --file-attention-path "Cargo.toml"

  echo "[e2e] checking NAC internal MCP result is excluded from default search"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --working-dir "$nac_project_dir" \
    --query "$nac_mcp_sentinel" \
    --expect-no-results \
    --expect-session-id "$nac_normalized_session_id" \
    --expect-event-count "8" \
    --expect-updated-at "2026-02-16T12:00:05.000Z" \
    --expect-mode "mcp_internal"

  echo "[e2e] checking NAC worker search/open/list behavior"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --working-dir "$nac_project_dir" \
    --query "$nac_worker_keyword" \
    --expect-session-id "$nac_normalized_local_worker_id" \
    --expect-open-text "$nac_worker_keyword" \
    --expect-event-count "5"

  echo "[e2e] nac sqlite same-path database replacement"
  "$python_bin" - "$nac_fixture_file" "$nac_session_id" "$nac_replacement_keyword" <<'PY'
import json
import os
import sqlite3
import sys

db_path, session_id, marker = sys.argv[1:4]
replacement = f"{db_path}.replacement"
try:
    os.remove(replacement)
except FileNotFoundError:
    pass

source = sqlite3.connect(db_path, timeout=30)
target = sqlite3.connect(replacement, timeout=30)
source.backup(target)
source.close()
messages = json.loads(
    target.execute(
        "SELECT messages_json FROM sessions WHERE session_id = ?",
        (session_id,),
    ).fetchone()[0]
)
messages.append({"role": "assistant", "content": f"replacement generation {marker}"})
target.execute(
    "UPDATE sessions SET messages_json = ?, updated_at = ? WHERE session_id = ?",
    (json.dumps(messages), "2026-02-16 12:00:24.000000000", session_id),
)
target.commit()
target.close()
os.replace(replacement, db_path)
PY
  local nac_replacement_session_id
  nac_replacement_session_id="$($python_bin - "$nac_canonical_db" "$nac_session_id" <<'PY'
from hashlib import sha256
import sys

db_path, raw_session_id = sys.argv[1:3]
material = f"ci-nac\n{db_path}\n2".encode()
namespace = sha256(material).hexdigest()[:16]
print(f"nac:{namespace}:{raw_session_id}")
PY
)"
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-nac' AND source_generation = 2 AND session_id = '${nac_replacement_session_id}' AND position(text_content, '${nac_replacement_keyword}') > 0" 120
  assert_clickhouse_count "$clickhouse_url" "nac replacement advances source generation" "SELECT count() FROM ${clickhouse_database}.ingest_checkpoints WHERE source_name = 'ci-nac' AND source_generation = 2" "1"
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM (SELECT session_id, generation, dirty_revision, total_events FROM ${clickhouse_database}.mcp_open_sessions FINAL WHERE session_id = '${nac_replacement_session_id}') AS projected INNER JOIN (SELECT session_id, dirty_revision FROM ${clickhouse_database}.mcp_open_dirty_sessions FINAL WHERE session_id = '${nac_replacement_session_id}') AS dirty USING (session_id) WHERE projected.total_events = 8 AND projected.dirty_revision = dirty.dirty_revision" 120
  wait_for_clickhouse_scalar_stable "$clickhouse_url" "SELECT concat(toString(slot), ':', toString(generation), ':', toString(source_revision), ':', toString(dirty_revision), ':', toString(total_events)) FROM ${clickhouse_database}.mcp_open_sessions FINAL WHERE session_id = '${nac_replacement_session_id}' LIMIT 1" 60 5 >/dev/null
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --working-dir "$nac_project_dir" \
    --query "$nac_replacement_keyword" \
    --expect-session-id "$nac_replacement_session_id" \
    --expect-open-text "$nac_replacement_keyword" \
    --expect-event-count "8"

  echo "[e2e] checking MCP initialize/tools/search_sessions/open/list_sessions (pi)"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --query "$pi_keyword" \
    --expect-session-id "$pi_session_id" \
    --expect-open-text "$pi_trace_marker" \
    --file-attention-path "Cargo.toml"

  # Hermes synthesizes its own `hermes:<uid>` session id, so we do not pin
  # --expect-session-id; source file + trace marker are enough to prove the
  # row round-tripped from fixture through ingest to MCP search_sessions/open/list_sessions.
  echo "[e2e] checking MCP initialize/tools/search_sessions/open/list_sessions (hermes)"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --query "$hermes_trace_marker" \
    --expect-session-id "$hermes_trajectory_session_id" \
    --expect-open-text "$hermes_trace_marker"

  echo "[e2e] checking MCP initialize/tools/search_sessions/open/list_sessions (hermes session_json)"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --query "$hermes_session_keyword" \
    --expect-session-id "$hermes_raw_session_id" \
    --expect-open-text "$hermes_session_trace_marker"

  # --project-only: launched from the claude fixture's recorded cwd, the MCP
  # server must serve that session normally while sessions from other
  # directories (including the separate Codex Git repository) are invisible
  # to search/list and answer not_found on open.
  echo "[e2e] checking MCP --project-only serves in-scope session and hides others"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --query "$claude_keyword" \
    --expect-session-id "$claude_session_id" \
    --expect-open-text "$claude_trace_marker" \
    --project-dir "$claude_project_dir" \
    --expect-absent-session-id "$codex_session_id" \
    --expect-absent-session-id "$pi_session_id"

  # Search for an out-of-scope keyword returns nothing, while list_sessions
  # still surfaces the in-scope claude session (--expect-session-id) — so an
  # empty search proves scoping, not a dead backend.
  echo "[e2e] checking MCP --project-only returns no hits for out-of-scope keyword"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --query "$codex_keyword" \
    --expect-no-results \
    --expect-session-id "$claude_session_id" \
    --project-dir "$claude_project_dir" \
    --expect-absent-session-id "$codex_session_id"


  echo "[e2e] checking full-stack status prefers the daemon v1 API"
  local daemon_status_path="$tmp_root/daemon-status.json"
  "$moraine_bin" --output json --config "$config_path" status > "$daemon_status_path"
  "$python_bin" - "$daemon_status_path" "$backend_pid" "$monitor_port" <<'PY'
import json
from pathlib import Path
import sys

status = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
expected_backend_pid = int(sys.argv[2])
expected_monitor_url = f"http://127.0.0.1:{int(sys.argv[3])}"
services = {
    row.get("service"): row
    for row in status.get("services", [])
    if isinstance(row, dict) and isinstance(row.get("service"), str)
}

if status.get("data_source") != "daemon_api":
    raise SystemExit(f"full-stack status did not prefer daemon API: {status.get('data_source')!r}")
backend = services.get("backend")
if (
    not isinstance(backend, dict)
    or backend.get("state") != "running"
    or backend.get("pid") != expected_backend_pid
    or backend.get("socket_listening") is not True
    or backend.get("http_listening") is not True
):
    raise SystemExit(f"full-stack status did not report the backend running: {backend!r}")
doctor = status.get("doctor")
if (
    not isinstance(doctor, dict)
    or doctor.get("clickhouse_healthy") is not True
    or doctor.get("database_exists") is not True
):
    raise SystemExit(f"full-stack API status did not report a healthy database: {doctor!r}")
heartbeat = status.get("heartbeat")
if not isinstance(heartbeat, dict) or heartbeat.get("state") != "available":
    raise SystemExit(f"full-stack API status did not report an available heartbeat: {heartbeat!r}")
if status.get("monitor_url") != expected_monitor_url:
    raise SystemExit(
        f"full-stack status monitor URL mismatch: {status.get('monitor_url')!r}"
    )
PY

  echo "[e2e] killing only the unified backend (crash/fallback scenario)"
  kill -KILL "$backend_pid"
  wait_for_backend_crash \
    "$python_bin" \
    "$backend_pid" \
    "$backend_socket" \
    "http://127.0.0.1:${monitor_port}" \
    30

  echo "[e2e] checking database + ingest survived backend termination"
  local remaining_ingest_pid
  local remaining_clickhouse_pid
  remaining_ingest_pid="$(read_live_pid_file "$ingest_pid_file" "ingest after backend KILL")"
  remaining_clickhouse_pid="$(read_live_pid_file "$clickhouse_pid_file" "clickhouse after backend KILL")"
  if [[ "$remaining_ingest_pid" != "$ingest_pid" ]]; then
    echo "[e2e] ingest PID changed across backend KILL: $ingest_pid -> $remaining_ingest_pid" >&2
    return 1
  fi
  if [[ "$remaining_clickhouse_pid" != "$clickhouse_pid" ]]; then
    echo "[e2e] ClickHouse PID changed across backend KILL: $clickhouse_pid -> $remaining_clickhouse_pid" >&2
    return 1
  fi
  assert_clickhouse_scalar "$clickhouse_url" "database remains queryable after backend KILL" "SELECT 1" "1"

  echo "[e2e] checking full MCP smoke through embedded fallback"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --working-dir "$codex_project_dir" \
    --query "$codex_keyword" \
    --expect-session-id "$codex_session_id" \
    --expect-open-text "$codex_trace_marker" \
    --expect-matching-search-hits "1" \
    --expect-event-count "8" \
    --expect-updated-at "2026-02-16T12:00:03.900Z" \
    --file-attention-path "Cargo.toml" \
    --file-attention-expect-absent-session-id "$claude_session_id" \
    --file-attention-expect-absent-session-id "$pi_session_id" \
    --file-attention-expect-absent-session-id "$codex_multi_path_session_id" \
    --file-attention-expect-absent-session-id "$codex_nested_path_session_id" \
    --require-embedded-fallback \
    --write-tools-snapshot "$embedded_tools_snapshot"

  echo "[e2e] checking named-backend MCP route through embedded fallback"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --working-dir "$claude_project_dir" \
    --query "$claude_keyword" \
    --expect-session-id "$claude_session_id" \
    --expect-open-text "$claude_trace_marker" \
    --expect-absent-session-id "$codex_session_id" \
    --require-embedded-fallback

  echo "[e2e] comparing daemon and embedded canonical tools/list snapshots"
  "$python_bin" - "$daemon_tools_snapshot" "$embedded_tools_snapshot" <<'PY'
from hashlib import sha256
from pathlib import Path
import sys

daemon_path, embedded_path = map(Path, sys.argv[1:3])
daemon = daemon_path.read_bytes()
embedded = embedded_path.read_bytes()
if daemon != embedded:
    raise SystemExit(
        "canonical tools/list snapshots differ byte-for-byte: "
        f"daemon={sha256(daemon).hexdigest()} ({len(daemon)} bytes), "
        f"embedded={sha256(embedded).hexdigest()} ({len(embedded)} bytes)"
    )
PY

  echo "[e2e] checking final JSON status after abrupt backend loss"
  local final_status_path="$tmp_root/final-status.json"
  "$moraine_bin" --output json --config "$config_path" status > "$final_status_path"
  "$python_bin" - "$final_status_path" "$ingest_pid" "$clickhouse_pid" <<'PY'
import json
from pathlib import Path
import sys

status = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
if status.get("data_source") != "direct_db":
    raise SystemExit(
        f"backend-down status did not use direct DB fallback: {status.get('data_source')!r}"
    )
expected_ingest_pid = int(sys.argv[2])
expected_clickhouse_pid = int(sys.argv[3])
services = {
    row.get("service"): row
    for row in status.get("services", [])
    if isinstance(row, dict) and isinstance(row.get("service"), str)
}

backend = services.get("backend")
if backend != {
    "service": "backend",
    "pid": None,
    "state": "stopped",
    "socket_listening": False,
    "http_listening": False,
}:
    raise SystemExit(f"final status did not report a fully stopped backend: {backend!r}")

ingest = services.get("ingest")
if (
    not isinstance(ingest, dict)
    or ingest.get("state") != "running"
    or ingest.get("pid") != expected_ingest_pid
):
    raise SystemExit(f"final status did not report the original ingest PID running: {ingest!r}")

clickhouse = services.get("clickhouse")
if (
    not isinstance(clickhouse, dict)
    or clickhouse.get("state") != "running"
    or clickhouse.get("pid") != expected_clickhouse_pid
):
    raise SystemExit(
        f"final status did not report the original ClickHouse PID running: {clickhouse!r}"
    )

doctor = status.get("doctor")
if (
    not isinstance(doctor, dict)
    or doctor.get("clickhouse_healthy") is not True
    or doctor.get("database_exists") is not True
):
    raise SystemExit(f"final status did not report a healthy database: {doctor!r}")

heartbeat = status.get("heartbeat")
if not isinstance(heartbeat, dict) or heartbeat.get("state") != "available":
    raise SystemExit(f"final status did not report an available heartbeat: {heartbeat!r}")
if status.get("monitor_url") is not None:
    raise SystemExit(f"final status retained a stopped monitor URL: {status.get('monitor_url')!r}")
PY
  cat "$final_status_path"
  echo "[e2e] stopping remaining services and checking all-down status"
  local all_down_transition_path="$tmp_root/all-down-transition.json"
  local all_down_status_path="$tmp_root/all-down-status.json"
  "$moraine_bin" --output json --config "$config_path" down > "$all_down_transition_path"
  "$moraine_bin" --output json --config "$config_path" status > "$all_down_status_path"
  "$python_bin" - "$all_down_status_path" <<'PY'
import json
from pathlib import Path
import sys

status = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
services = {
    row.get("service"): row
    for row in status.get("services", [])
    if isinstance(row, dict) and isinstance(row.get("service"), str)
}

if status.get("data_source") != "direct_db":
    raise SystemExit(f"all-down status did not use direct DB: {status.get('data_source')!r}")
for service in ("backend", "ingest", "clickhouse"):
    row = services.get(service)
    if (
        not isinstance(row, dict)
        or row.get("state") != "stopped"
        or row.get("pid") is not None
    ):
        raise SystemExit(f"all-down status did not report {service} stopped: {row!r}")
doctor = status.get("doctor")
if not isinstance(doctor, dict) or doctor.get("clickhouse_healthy") is not False:
    raise SystemExit(f"all-down status did not preserve database diagnosis: {doctor!r}")
heartbeat = status.get("heartbeat")
if not isinstance(heartbeat, dict) or heartbeat.get("state") != "error":
    raise SystemExit(f"all-down status did not preserve heartbeat failure: {heartbeat!r}")
if status.get("monitor_url") is not None:
    raise SystemExit(f"all-down status retained a monitor URL: {status.get('monitor_url')!r}")
PY
  cat "$all_down_status_path"

  E2E_SUCCESS=1
}

main "$@"
