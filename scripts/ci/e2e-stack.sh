#!/usr/bin/env bash
set -euo pipefail

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
  local cursor_keyword="${base_keyword}_cursor_${run_stamp}"
  local cursor_fallback_keyword="${base_keyword}_cursor_fallback_${run_stamp}"
  local cursor_sqlite_keyword="${base_keyword}_cursorsqlite_${run_stamp}"
  local cursor_sqlite_live_keyword="${base_keyword}_cursorsqlite_live_${run_stamp}"
  local pi_keyword="${base_keyword}_pi_${run_stamp}"
  local hermes_keyword="${base_keyword}_hermes_trajectory_${run_stamp}"
  local hermes_session_keyword="${base_keyword}_hermes_session_${run_stamp}"
  local clickhouse_database="moraine"
  local codex_session_suffix
  codex_session_suffix="$(printf '%06x%06x' "$RANDOM" "$RANDOM")"
  local codex_session_id="00000000-0000-4000-8000-${codex_session_suffix}"
  local claude_session_suffix
  claude_session_suffix="$(printf '%06x%06x' "$RANDOM" "$RANDOM")"
  local claude_session_id="00000000-0000-4000-8000-${claude_session_suffix}"
  local kimi_session_id="kimi-${run_stamp}"
  local kimi_raw_session_id="kimi-cli:${kimi_session_id}"
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
  local cursor_trace_marker="mcp_cursor_trace_marker_${run_stamp}"
  local cursor_sqlite_trace_marker="mcp_cursor_sqlite_trace_marker_${run_stamp}"
  local pi_trace_marker="mcp_pi_trace_marker_${run_stamp}"
  local hermes_trace_marker="mcp_hermes_trace_marker_${run_stamp}"
  local hermes_session_trace_marker="mcp_hermes_session_trace_marker_${run_stamp}"

  need_cmd curl
  need_cmd "$python_bin"

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
  local codex_fixture_file="$fixtures_root/codex/sessions/2026/02/16/session-${codex_session_id}.jsonl"
  local claude_fixture_file="$fixtures_root/claude/projects/e2e/session-${claude_session_id}.jsonl"
  local kimi_fixture_file="$fixtures_root/kimi/sessions/${kimi_session_id}/wire.jsonl"
  local cursor_fixture_file="$fixtures_root/cursor/projects/e2e/agent-transcripts/${cursor_session_id}/${cursor_session_id}.jsonl"
  local cursor_fallback_fixture_file="$fixtures_root/cursor/projects/e2e-fallback/agent-transcripts/${cursor_fallback_session_id}/${cursor_fallback_session_id}.jsonl"
  local cursor_sqlite_fixture_file="$fixtures_root/cursor-sqlite/User/globalStorage/state.vscdb"
  local pi_fixture_file="$fixtures_root/pi/agent/sessions/--tmp-moraine-e2e--/2026-02-16T12-00-08-000Z_${pi_session_id}.jsonl"
  local hermes_fixture_file="$fixtures_root/hermes/trajectories/001-${run_stamp}.jsonl"
  local hermes_session_fixture_file="$fixtures_root/hermes/sessions/${hermes_session_id}.json"
  # The directory the claude fixture session "originated from": real Claude
  # Code records a cwd on every message line, and `--project-only` scoping
  # keys off it. Must be a real directory so mcp_smoke.py can launch the
  # MCP server from inside it.
  local claude_project_dir="$tmp_root/claude-project"
  mkdir -p "$claude_project_dir"

  mkdir -p "$(dirname "$codex_fixture_file")"
  mkdir -p "$(dirname "$claude_fixture_file")"
  mkdir -p "$(dirname "$kimi_fixture_file")"
  mkdir -p "$(dirname "$cursor_fixture_file")"
  mkdir -p "$(dirname "$cursor_fallback_fixture_file")"
  mkdir -p "$(dirname "$cursor_sqlite_fixture_file")"
  mkdir -p "$(dirname "$pi_fixture_file")"
  mkdir -p "$(dirname "$hermes_fixture_file")"
  mkdir -p "$(dirname "$hermes_session_fixture_file")"
  mkdir -p "$runtime_root"

  cat > "$codex_fixture_file" <<EOF
{"timestamp":"2026-02-16T12:00:00.000Z","type":"session_meta","payload":{"id":"${codex_session_id}"}}
{"timestamp":"2026-02-16T12:00:01.000Z","type":"turn_context","payload":{"turn_id":"1","model":"gpt-5.3-codex"}}
{"timestamp":"2026-02-16T12:00:02.000Z","type":"response_item","payload":{"type":"message","role":"user","id":"msg-user-${run_stamp}","content":[{"type":"text","text":"local e2e codex user prompt ${codex_keyword}"}],"phase":"completed"}}
{"timestamp":"2026-02-16T12:00:03.000Z","type":"response_item","parent_id":"msg-user-${run_stamp}","payload":{"type":"message","role":"assistant","id":"msg-assistant-${run_stamp}","content":[{"type":"text","text":"local e2e codex assistant reply ${codex_keyword} ${codex_trace_marker}"}],"phase":"completed"}}
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

[mcp]
# Exercise the shared central server end to end: 'moraine up' starts the
# (n.b. no backticks in this heredoc: it is unquoted, so they would execute)
# daemon and the mcp_smoke.py runs below proxy through it (falling back to an
# embedded server if the socket is not yet up). Both are the defaults; pinned
# here so the e2e's coverage of the proxy path is explicit, not incidental.
use_central_server = true
start_central_on_up = true

[runtime]
root_dir = "${runtime_root}"
logs_dir = "logs"
pids_dir = "run"
service_bin_dir = "${repo_root}/target/debug"
managed_clickhouse_dir = "${runtime_root}/managed-clickhouse"
clickhouse_auto_install = true
clickhouse_start_timeout_seconds = 90.0
start_monitor_on_up = true
EOF

  trap "cleanup_e2e \"$moraine_bin\" \"$config_path\" \"$runtime_root\" \"$tmp_root\"" EXIT

  echo "[e2e] run id: ${run_stamp}"
  echo "[e2e] clickhouse url: ${clickhouse_url}"
  echo "[e2e] clickhouse db: ${clickhouse_database}"
  echo "[e2e] codex fixture: ${codex_fixture_file}"
  echo "[e2e] claude fixture: ${claude_fixture_file}"
  echo "[e2e] kimi fixture: ${kimi_fixture_file}"
  echo "[e2e] cursor fixture: ${cursor_fixture_file}"
  echo "[e2e] cursor fallback fixture: ${cursor_fallback_fixture_file}"
  echo "[e2e] cursor sqlite fixture: ${cursor_sqlite_fixture_file}"
  echo "[e2e] pi fixture: ${pi_fixture_file}"
  echo "[e2e] hermes fixture: ${hermes_fixture_file}"
  echo "[e2e] hermes session fixture: ${hermes_session_fixture_file}"

  echo "[e2e] installing managed ClickHouse"
  "$moraine_bin" clickhouse install --config "$config_path"

  echo "[e2e] ensuring monitor frontend assets"
  ensure_monitor_frontend "$repo_root"

  echo "[e2e] starting stack"
  "$moraine_bin" up --config "$config_path"

  echo "[e2e] waiting for monitor health"
  wait_for_endpoint_ok "$python_bin" "http://127.0.0.1:${monitor_port}/api/health" 120

  echo "[e2e] waiting for ingest heartbeat + indexed content"
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.ingest_heartbeats" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_documents WHERE positionCaseInsensitiveUTF8(text_content, '${codex_keyword}') > 0" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_postings WHERE term = '${codex_keyword}'" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_documents WHERE positionCaseInsensitiveUTF8(text_content, '${claude_keyword}') > 0" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_postings WHERE term = '${claude_keyword}'" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_documents WHERE positionCaseInsensitiveUTF8(text_content, '${kimi_keyword}') > 0" 120
  wait_for_clickhouse_count "$clickhouse_url" "SELECT count() FROM ${clickhouse_database}.search_postings WHERE term = '${kimi_keyword}'" 120
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

  echo "[e2e] checking ClickHouse normalized ingest rows"
  assert_clickhouse_count "$clickhouse_url" "ingest errors" "SELECT count() FROM ${clickhouse_database}.ingest_errors" "0"

  assert_clickhouse_count "$clickhouse_url" "codex unique raw rows" "SELECT uniqExact(raw_json_hash) FROM ${clickhouse_database}.raw_events WHERE source_name = 'ci-codex'" "7"
  assert_clickhouse_count "$clickhouse_url" "codex event rows" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-codex'" "7"
  assert_clickhouse_count "$clickhouse_url" "codex link rows" "SELECT count() FROM ${clickhouse_database}.event_links FINAL WHERE source_name = 'ci-codex' AND link_type = 'parent_event' AND linked_external_id = 'msg-user-${run_stamp}'" "1"
  assert_clickhouse_count "$clickhouse_url" "codex tool rows" "SELECT count() FROM ${clickhouse_database}.tool_io FINAL WHERE source_name = 'ci-codex' AND tool_call_id = 'codex-tool-${run_stamp}'" "2"
  assert_clickhouse_count "$clickhouse_url" "codex tool request fields" "SELECT count() FROM ${clickhouse_database}.tool_io FINAL WHERE source_name = 'ci-codex' AND tool_call_id = 'codex-tool-${run_stamp}' AND tool_name = 'Read' AND tool_phase = 'request'" "1"
  assert_clickhouse_count "$clickhouse_url" "codex harness/session fields" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-codex' AND harness = 'codex' AND session_id = '${codex_session_id}'" "7"
  assert_clickhouse_count "$clickhouse_url" "codex model fields" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-codex' AND model IN ('gpt-5.3-codex', 'gpt-5.3-codex-spark')" "6"
  assert_clickhouse_count "$clickhouse_url" "codex token buckets" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-codex' AND payload_type = 'token_count' AND input_tokens = 17 AND output_tokens = 4 AND cache_read_tokens = 3 AND cache_write_tokens = 2 AND token_usage_buckets['input_text'] = 12 AND token_usage_buckets['output_text'] = 3 AND token_usage_buckets['reasoning'] = 1" "1"

  assert_clickhouse_count "$clickhouse_url" "claude unique raw rows" "SELECT uniqExact(raw_json_hash) FROM ${clickhouse_database}.raw_events WHERE source_name = 'ci-claude'" "3"
  # 5 content events + 1 synthetic session_meta carrying the recorded cwd,
  # injected once per session (on the first cwd-bearing record) by ingest
  # dispatch so --project-only scoping can see the claude session's origin.
  assert_clickhouse_count "$clickhouse_url" "claude event rows" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-claude'" "6"
  assert_clickhouse_count "$clickhouse_url" "claude session_meta cwd" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-claude' AND event_kind = 'session_meta' AND JSONExtractString(payload_json, 'cwd') != ''" "1"
  assert_clickhouse_count "$clickhouse_url" "claude link rows" "SELECT count() FROM ${clickhouse_database}.event_links FINAL WHERE source_name = 'ci-claude'" "6"
  assert_clickhouse_count "$clickhouse_url" "claude tool rows" "SELECT count() FROM ${clickhouse_database}.tool_io FINAL WHERE source_name = 'ci-claude' AND tool_call_id = 'claude-tool-${run_stamp}'" "2"
  assert_clickhouse_count "$clickhouse_url" "claude harness/session fields" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-claude' AND harness = 'claude-code' AND inference_provider = 'anthropic' AND session_id = '${claude_session_id}'" "6"
  assert_clickhouse_count "$clickhouse_url" "claude model fields" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-claude' AND model = 'claude-opus-4-5-20251101'" "3"
  assert_clickhouse_count "$clickhouse_url" "claude token buckets" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-claude' AND actor_kind = 'assistant' AND input_tokens = 9 AND output_tokens = 5 AND token_usage_buckets['input_text'] = 9 AND token_usage_buckets['output_text'] = 5" "3"

  assert_clickhouse_count "$clickhouse_url" "kimi unique raw rows" "SELECT uniqExact(raw_json_hash) FROM ${clickhouse_database}.raw_events WHERE source_name = 'ci-kimi'" "8"
  assert_clickhouse_count "$clickhouse_url" "kimi event rows" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-kimi'" "7"
  assert_clickhouse_count "$clickhouse_url" "kimi link rows" "SELECT count() FROM ${clickhouse_database}.event_links FINAL WHERE source_name = 'ci-kimi'" "0"
  assert_clickhouse_count "$clickhouse_url" "kimi tool rows" "SELECT count() FROM ${clickhouse_database}.tool_io FINAL WHERE source_name = 'ci-kimi' AND tool_call_id = 'kimi-tool-${run_stamp}'" "2"
  assert_clickhouse_count "$clickhouse_url" "kimi domain fields" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-kimi' AND harness = 'kimi-cli' AND inference_provider = 'moonshot' AND session_id = '${kimi_raw_session_id}' AND model = 'kimi-cli'" "7"
  assert_clickhouse_count "$clickhouse_url" "kimi token buckets" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-kimi' AND payload_type = 'token_count' AND input_tokens = 13 AND output_tokens = 5 AND cache_read_tokens = 2 AND cache_write_tokens = 1 AND token_usage_buckets['input_text'] = 10 AND token_usage_buckets['output_text'] = 5 AND token_usage_buckets['input_cache_read'] = 2 AND token_usage_buckets['input_cache_write'] = 1" "1"

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

  assert_clickhouse_count "$clickhouse_url" "hermes session unique raw rows" "SELECT uniqExact(raw_json_hash) FROM ${clickhouse_database}.raw_events WHERE source_name = 'ci-hermes-session'" "5"
  assert_clickhouse_count "$clickhouse_url" "hermes session event rows" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-hermes-session'" "6"
  assert_clickhouse_count "$clickhouse_url" "hermes session link rows" "SELECT count() FROM ${clickhouse_database}.event_links FINAL WHERE source_name = 'ci-hermes-session'" "0"
  assert_clickhouse_count "$clickhouse_url" "hermes session tool rows" "SELECT count() FROM ${clickhouse_database}.tool_io FINAL WHERE source_name = 'ci-hermes-session' AND tool_call_id = 'hermes-session-tool-${run_stamp}' AND tool_name = 'shell'" "2"
  assert_clickhouse_count "$clickhouse_url" "hermes session domain fields" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-hermes-session' AND harness = 'hermes' AND inference_provider = 'anthropic' AND session_id = '${hermes_raw_session_id}' AND model = 'claude-opus-4-6'" "6"
  # 52 prior events + 1 synthetic claude cwd session_meta (it carries the
  # canonical all-zero token bucket map like every other normalized event).
  assert_clickhouse_count "$clickhouse_url" "token bucket map keys on all events" "SELECT count() FROM ${clickhouse_database}.events FINAL WHERE hasAll(mapKeys(token_usage_buckets), ['input_text', 'output_text', 'input_cache_read', 'input_cache_write', 'reasoning'])" "53"

  local hermes_trajectory_session_id
  hermes_trajectory_session_id="$(clickhouse_scalar "$clickhouse_url" "SELECT any(session_id) FROM ${clickhouse_database}.events FINAL WHERE source_name = 'ci-hermes-trajectory'")"
  if [[ -z "$hermes_trajectory_session_id" ]]; then
    echo "[e2e] failed to resolve Hermes trajectory session id" >&2
    return 1
  fi

  echo "[e2e] checking monitor API routes"
  for path in /api/health /api/status /api/analytics /api/web-searches /api/sessions; do
    local body
    body="$(curl -fsS "http://127.0.0.1:${monitor_port}${path}")"
    printf '%s' "$body" | json_ok_true "$python_bin"
  done
  local sessions_body
  sessions_body="$(curl -fsS "http://127.0.0.1:${monitor_port}/api/sessions?since=all&limit=200")"
  printf '%s' "$sessions_body" | "$python_bin" -c 'import json,sys; data=json.load(sys.stdin); sid=sys.argv[1]; ok=any(s.get("id")==sid and s.get("harness",{}).get("id")=="cursor" for s in data.get("sessions", [])); sys.exit(0 if ok else 1)' "$cursor_session_id"
  printf '%s' "$sessions_body" | "$python_bin" -c 'import json,sys; data=json.load(sys.stdin); sid=sys.argv[1]; ok=any(s.get("id")==sid and s.get("harness",{}).get("id")=="pi-coding-agent" for s in data.get("sessions", [])); sys.exit(0 if ok else 1)' "$pi_session_id"

  echo "[e2e] checking MCP initialize/tools/search_sessions/open/list_sessions (codex)"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --query "$codex_keyword" \
    --expect-session-id "$codex_session_id" \
    --expect-open-text "$codex_trace_marker"

  echo "[e2e] checking MCP initialize/tools/search_sessions/open/list_sessions (claude)"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --query "$claude_keyword" \
    --expect-session-id "$claude_session_id" \
    --expect-open-text "$claude_trace_marker"

  echo "[e2e] checking MCP initialize/tools/search_sessions/open/list_sessions (kimi)"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --query "$kimi_keyword" \
    --expect-session-id "$kimi_raw_session_id" \
    --expect-open-text "$kimi_trace_marker"

  echo "[e2e] checking MCP initialize/tools/search_sessions/open/list_sessions (cursor)"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --query "$cursor_keyword" \
    --expect-session-id "$cursor_session_id" \
    --expect-open-text "$cursor_trace_marker"

  echo "[e2e] checking MCP initialize/tools/search_sessions/open/list_sessions (cursor sqlite)"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --query "$cursor_sqlite_keyword" \
    --expect-session-id "$cursor_sqlite_session_id" \
    --expect-open-text "$cursor_sqlite_trace_marker"

  echo "[e2e] checking MCP initialize/tools/search_sessions/open/list_sessions (pi)"
  "$python_bin" "$repo_root/scripts/ci/mcp_smoke.py" \
    --moraine "$moraine_bin" \
    --config "$config_path" \
    --query "$pi_keyword" \
    --expect-session-id "$pi_session_id" \
    --expect-open-text "$pi_trace_marker"

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
  # directories (pi: cwd=$tmp_root) or with no recorded cwd (codex) are
  # invisible to search/list and answer not_found on open.
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

  if [[ "${RUN_REPLAY_BENCH_SMOKE:-0}" == "1" ]]; then
    echo "[e2e] checking replay benchmark script (dry-run)"
    "$python_bin" "$repo_root/scripts/bench/replay_search_latency.py" \
      --config "$config_path" \
      --window 24h \
      --top-n 2 \
      --dry-run
  fi

  echo "[e2e] final status"
  "$moraine_bin" status --config "$config_path"
  E2E_SUCCESS=1
}

main "$@"
