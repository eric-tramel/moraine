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

shutdown() {
    local rc="${1:-0}"
    if (( shutting_down )); then
        return
    fi
    shutting_down=1
    trap - TERM INT
    for pid in "$central_pid" "$ingest_pid"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done
    for pid in "$central_pid" "$ingest_pid"; do
        if [[ -n "$pid" ]]; then
            wait "$pid" 2>/dev/null || true
        fi
    done
    rm -f "$STATUS_FILE" "${STATUS_FILE}.tmp"
    exit "$rc"
}
trap 'shutdown 143' TERM
trap 'shutdown 130' INT

# This bounded readiness probe occurs before suite timing. There is no Docker
# healthcheck or periodic probe during measurement.
for attempt in $(seq 1 120); do
    if curl -fsS --max-time 1 "${CH_URL}/ping" >/dev/null 2>&1; then
        break
    fi
    (( attempt < 120 )) || die "ClickHouse did not become ready before service start"
    sleep 1
done

# Schema setup is owned by the frozen arm binary and occurs before any measured
# boundary. A fresh ClickHouse volume has no tables until migrations run.
"${BIN_DIR}/moraine" db migrate --config "$CONFIG" >/dev/null

cache_generation="$(cat /proc/sys/kernel/random/uuid)"
"${BIN_DIR}/moraine-ingest" --config "$CONFIG" &
ingest_pid=$!
"${BIN_DIR}/moraine-mcp" --config "$CONFIG" --serve socket --host 0.0.0.0 --port 8080 &
central_pid=$!

# Container-local identities are paired with host PIDs/starttimes by the
# ownership-checked `moraine-sandbox benchmark-service status` operation.
printf '{"schema_version":"moraine-performance-central-v1","cache_generation":"%s","central_container_pid":%s,"ingest_container_pid":%s}\n' \
    "$cache_generation" "$central_pid" "$ingest_pid" >"${STATUS_FILE}.tmp"
chmod 0600 "${STATUS_FILE}.tmp"
mv "${STATUS_FILE}.tmp" "$STATUS_FILE"
log "central generation ${cache_generation} started"

set +e
wait -n "$central_pid" "$ingest_pid"
rc=$?
set -e
if (( ! shutting_down )); then
    log "measured service exited unexpectedly (status ${rc})"
fi
shutdown "$rc"
