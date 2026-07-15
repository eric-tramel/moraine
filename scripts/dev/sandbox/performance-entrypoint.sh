#!/usr/bin/env bash
# Minimal measured-service supervisor for the fixed-resource benchmark.
set -euo pipefail

BIN_DIR=/opt/moraine/bin
CONFIG=/sandbox/moraine.toml
STATE_DIR=/home/moraine/.moraine
STATUS_FILE=${STATE_DIR}/performance-central.json
CH_URL="http://${SANDBOX_CLICKHOUSE_HOST:-clickhouse}:${SANDBOX_CLICKHOUSE_PORT:-8123}"
central_pid=""
ingest_pid=""
shutting_down=0

log() { printf '[performance] %s\n' "$*" >&2; }
die() { printf '[performance] ERROR: %s\n' "$*" >&2; exit 1; }

for binary in moraine moraine-ingest moraine-mcp; do
    [[ -x "${BIN_DIR}/${binary}" ]] || die "immutable release binary missing: ${binary}"
done
[[ -f "$CONFIG" ]] || die "benchmark config missing: ${CONFIG}"
mkdir -p "$STATE_DIR"
rm -f "$STATUS_FILE" "${STATUS_FILE}.tmp"

write_central_status() {
    local generation="$1"
    printf '{"schema_version":"moraine-performance-central-v1","cache_generation":"%s","central_container_pid":%s,"ingest_container_pid":%s}\n' \
        "$generation" "$central_pid" "$ingest_pid" >"${STATUS_FILE}.tmp"
    chmod 0600 "${STATUS_FILE}.tmp"
    mv "${STATUS_FILE}.tmp" "$STATUS_FILE"
}

start_central() {
    if [[ -n "$central_pid" ]] && kill -0 "$central_pid" 2>/dev/null; then
        return
    fi
    local cache_generation
    cache_generation="$(cat /proc/sys/kernel/random/uuid)"
    "${BIN_DIR}/moraine-mcp" --config "$CONFIG" --serve socket --host 0.0.0.0 --port 8080 &
    central_pid=$!
    write_central_status "$cache_generation"
    log "central generation ${cache_generation} started"
}

stop_central() {
    if [[ -n "$central_pid" ]] && kill -0 "$central_pid" 2>/dev/null; then
        kill -TERM "$central_pid"
        wait "$central_pid" 2>/dev/null || true
    fi
    central_pid=""
    rm -f "$STATUS_FILE" "${STATUS_FILE}.tmp"
}

shutdown() {
    local rc="${1:-0}"
    if (( shutting_down )); then
        return
    fi
    shutting_down=1
    trap - TERM INT USR1 USR2
    stop_central
    if [[ -n "$ingest_pid" ]] && kill -0 "$ingest_pid" 2>/dev/null; then
        kill -TERM "$ingest_pid" 2>/dev/null || true
    fi
    if [[ -n "$ingest_pid" ]]; then
        wait "$ingest_pid" 2>/dev/null || true
    fi
    exit "$rc"
}
trap 'shutdown 143' TERM
trap 'shutdown 130' INT
trap 'stop_central' USR1
trap 'start_central' USR2

# This bounded readiness probe and schema setup occur once, before suite timing.
# TTR signals only the central child; ingest and this container remain alive.
for attempt in $(seq 1 120); do
    if curl -fsS --max-time 1 "${CH_URL}/ping" >/dev/null 2>&1; then
        break
    fi
    (( attempt < 120 )) || die "ClickHouse did not become ready before service start"
    sleep 1
done
"${BIN_DIR}/moraine" db migrate --config "$CONFIG" >/dev/null

"${BIN_DIR}/moraine-ingest" --config "$CONFIG" &
ingest_pid=$!
start_central

while true; do
    set +e
    if [[ -n "$central_pid" ]]; then
        wait -n "$central_pid" "$ingest_pid"
    else
        wait "$ingest_pid"
    fi
    rc=$?
    set -e
    if (( shutting_down )); then
        exit "$rc"
    fi
    if ! kill -0 "$ingest_pid" 2>/dev/null; then
        log "ingest exited unexpectedly (status ${rc})"
        shutdown "$rc"
    fi
    if [[ -n "$central_pid" ]] && ! kill -0 "$central_pid" 2>/dev/null; then
        log "central exited unexpectedly (status ${rc})"
        shutdown "$rc"
    fi
done
