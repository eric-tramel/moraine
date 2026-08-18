#!/usr/bin/env bash
# Moraine dev sandbox — container entrypoint. See RFC #232.
#
# Run by tini as PID 1 child. Expects:
#   /repo              — worktree source tree       (ro, host bind mount)
#   /opt/moraine/bin   — named volume, populated by this script's initial
#                        cargo build from /repo (or prior boot's output)
#   /opt/moraine/web   — monitor dist assets       (ro, host bind mount)
#   /sandbox           — generated moraine.toml    (ro, host bind mount)
#   /home/moraine/.moraine — named volume for runtime state (rw)
#   /home/moraine/.cargo   — named volume, cargo registry + git cache
#   /home/moraine/target   — named volume, CARGO_TARGET_DIR
#   /home/moraine/.cache/sccache — host bind mount, shared sccache disk cache
# and that a sibling `clickhouse` service in the same compose project is up
# and healthy on http://${SANDBOX_CLICKHOUSE_HOST:-clickhouse}:${SANDBOX_CLICKHOUSE_PORT:-8123}.
#
# Boot-build behavior:
#   The sandbox is a dev environment — agents iterate on /repo inside a
#   running container (cargo build / test / clippy etc.), with sccache
#   backing the shared host cache dir. On first boot (or when SANDBOX_REBUILD=1),
#   this script compiles the workspace before starting moraine services. On
#   subsequent boots the binaries are still in the `binaries` named volume and
#   the build is skipped unless SANDBOX_REBUILD=1.

set -euo pipefail

MORAINE_BIN=/opt/moraine/bin/moraine
MORAINE_CONFIG_PATH=/sandbox/moraine.toml
LOG_DIR=/home/moraine/.moraine/logs
RUN_DIR=/home/moraine/.moraine/run
CH_HOST="${SANDBOX_CLICKHOUSE_HOST:-clickhouse}"
CH_PORT="${SANDBOX_CLICKHOUSE_PORT:-8123}"
CH_URL="http://${CH_HOST}:${CH_PORT}"
STORAGE_ENGINE="${SANDBOX_STORAGE_ENGINE:-clickhouse}"

log()  { printf '[sandbox] %s\n' "$*"; }
warn() { printf '[sandbox] WARN: %s\n' "$*" >&2; }
die()  { printf '[sandbox] ERROR: %s\n' "$*" >&2; exit 1; }

dump_logs() {
    if ! compgen -G "$LOG_DIR/*.log" >/dev/null 2>&1; then
        warn "no log files under $LOG_DIR to display"
        return 0
    fi
    warn "--- last 80 lines of $LOG_DIR/*.log ---"
    # shellcheck disable=SC2012
    tail -n 80 "$LOG_DIR"/*.log >&2 || true
    warn "--- end log dump ---"
}

graceful_shutdown() {
    log "signal received, running 'moraine down'"
    if [[ -n "${tail_pid:-}" ]] && kill -0 "$tail_pid" 2>/dev/null; then
        kill "$tail_pid" 2>/dev/null || true
    fi
    "$MORAINE_BIN" down --config "$MORAINE_CONFIG_PATH" || true
    if [[ -n "${ch_sentinel_pid:-}" ]] && kill -0 "$ch_sentinel_pid" 2>/dev/null; then
        kill "$ch_sentinel_pid" 2>/dev/null || true
    fi
    exit 0
}

trap graceful_shutdown TERM INT

# ---------------------------------------------------------------------------
# Pre-flight checks (worktree + config, not binaries)
# ---------------------------------------------------------------------------

if [[ ! -f "$MORAINE_CONFIG_PATH" ]]; then
    die "sandbox config not found at $MORAINE_CONFIG_PATH — host CLI should generate and mount it"
fi

if [[ ! -f /repo/Cargo.toml ]]; then
    die "/repo/Cargo.toml missing — host CLI should bind-mount the worktree at /repo"
fi

if [[ ! -f "/opt/moraine/web/index.html" ]]; then
    warn "monitor assets not found at /opt/moraine/web/index.html — the monitor will not serve UI assets"
fi

mkdir -p "$LOG_DIR" "$RUN_DIR"

# ---------------------------------------------------------------------------
# Boot-build: compile the workspace if binaries aren't already present.
#
# On first boot the `binaries` named volume is empty. We cargo-build all four
# workspace binaries and install them at /opt/moraine/bin/. Subsequent boots
# reuse the volume's contents unless SANDBOX_REBUILD=1 is set. sccache wraps
# rustc so even "cold" builds benefit from the shared host cache for entries
# that target the same triple.
#
# CARGO_TARGET_DIR and CARGO_HOME are set in the image env; both are mapped
# to named volumes so subsequent incremental builds from `moraine-sandbox
# shell` are fast.
# ---------------------------------------------------------------------------

needs_build=0
if [[ "${SANDBOX_REBUILD:-0}" == "1" ]]; then
    log "SANDBOX_REBUILD=1 — forcing fresh cargo build"
    needs_build=1
elif [[ ! -x "$MORAINE_BIN" ]]; then
    log "no prior binaries at /opt/moraine/bin — running initial cargo build"
    needs_build=1
else
    log "reusing binaries from prior boot (set SANDBOX_REBUILD=1 to force rebuild)"
fi

if (( needs_build )); then
    log "cd /repo && cargo build --workspace --locked"
    log "(cold sccache can take several minutes; warm cache is seconds)"
    build_log="$LOG_DIR/bootstrap-build.log"
    : >"$build_log"
    if ! ( cd /repo && cargo build --workspace --locked ) 2>&1 | tee -a "$build_log"; then
        warn "cargo build failed — tail of $build_log:"
        tail -n 40 "$build_log" >&2 || true
        die "initial cargo build failed; resolve errors and relaunch the sandbox"
    fi
    log "installing built binaries to /opt/moraine/bin"
    install -m 0755 \
        "${CARGO_TARGET_DIR}/debug/moraine" \
        "${CARGO_TARGET_DIR}/debug/moraine-ingest" \
        "${CARGO_TARGET_DIR}/debug/moraine-monitor" \
        "${CARGO_TARGET_DIR}/debug/moraine-mcp" \
        /opt/moraine/bin/ || die "failed to install binaries from ${CARGO_TARGET_DIR}/debug/"
    log "initial build complete"
fi

for helper in moraine-ingest moraine-monitor moraine-mcp; do
    if [[ ! -x "/opt/moraine/bin/$helper" ]]; then
        die "$helper still missing from /opt/moraine/bin after build"
    fi
done

# ---------------------------------------------------------------------------
# Wait for the compose-managed ClickHouse to be reachable
# ---------------------------------------------------------------------------

if [[ "$STORAGE_ENGINE" == "clickhouse" ]]; then
    log "waiting for clickhouse at ${CH_URL}"
    for attempt in $(seq 1 60); do
        if curl -fsS --max-time 2 "${CH_URL}/ping" >/dev/null 2>&1; then
            log "clickhouse reachable after ${attempt} attempt(s)"
            break
        fi
        if (( attempt == 60 )); then
            die "clickhouse did not become reachable at ${CH_URL} within 60s"
        fi
        sleep 1
    done
fi

# ---------------------------------------------------------------------------
# Fake the ClickHouse pid file so `moraine up` skips starting its own.
#
# moraine up treats a running pid at $runtime.root/run/clickhouse.pid as
# "already running" and short-circuits start_clickhouse(). We spawn a
# long-lived sentinel and register its pid, so moraine never attempts to
# download or exec clickhouse-server. The real ClickHouse lives in the
# sibling compose service; connectivity is through clickhouse.url in
# moraine.toml (set to http://clickhouse:8123 by the host CLI).
# ---------------------------------------------------------------------------

if [[ "$STORAGE_ENGINE" == "clickhouse" ]]; then
    (sleep infinity) &
    ch_sentinel_pid=$!
    echo "$ch_sentinel_pid" > "$RUN_DIR/clickhouse.pid"
    log "registered clickhouse sentinel pid ${ch_sentinel_pid} (real CH is in sibling container)"
fi

# ---------------------------------------------------------------------------
# Bring the moraine services up
# ---------------------------------------------------------------------------

log "bringing up moraine stack with storage engine ${STORAGE_ENGINE}"
log "running: moraine up --config $MORAINE_CONFIG_PATH"
if ! "$MORAINE_BIN" up --config "$MORAINE_CONFIG_PATH"; then
    warn "'moraine up' failed"
    dump_logs
    exit 1
fi

log "moraine stack is up"
log "  monitor (container): http://127.0.0.1:8080"
if [[ "$STORAGE_ENGINE" == "clickhouse" ]]; then
    log "  clickhouse (compose DNS): ${CH_URL}"
else
    log "  storage: DuckDB under the isolated state volume"
fi
log "(the host CLI prints the matching host-published URLs)"

# ---------------------------------------------------------------------------
# Keep PID 1 alive and surface logs via `docker logs`
# ---------------------------------------------------------------------------

# Pre-create a placeholder so `tail -F` has at least one file to watch even if
# moraine hasn't flushed its first log yet.
: >>"$LOG_DIR/.sandbox-entrypoint.log"

tail -F "$LOG_DIR"/*.log &
tail_pid=$!

wait "$tail_pid"
