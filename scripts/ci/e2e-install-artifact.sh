#!/usr/bin/env bash
set -euo pipefail

E2E_INSTALL_SUCCESS=0
TMP_ROOT=""
SERVER_PID=""
SERVER_LOG=""

usage() {
  cat <<'EOF'
usage: scripts/ci/e2e-install-artifact.sh <target-triple>

example:
  scripts/ci/e2e-install-artifact.sh aarch64-apple-darwin
EOF
}

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

wait_for_local_server() {
  local base_url="$1"
  local timeout_seconds="${2:-30}"
  local started
  started="$(date +%s)"

  while true; do
    local now
    now="$(date +%s)"
    if (( now - started >= timeout_seconds )); then
      echo "timed out waiting for local artifact server: $base_url" >&2
      return 1
    fi

    if curl -fsS --max-time 2 "$base_url/" >/dev/null 2>&1; then
      return 0
    fi

    sleep 1
  done
}

cleanup_e2e_install() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi

  if [[ -z "$TMP_ROOT" ]]; then
    return
  fi

  if [[ "$E2E_INSTALL_SUCCESS" -ne 1 ]]; then
    echo "[e2e-install] failure diagnostics (tmp root: $TMP_ROOT)" >&2
    if [[ -n "$SERVER_LOG" && -f "$SERVER_LOG" ]]; then
      echo "--- tail $SERVER_LOG ---" >&2
      tail -n 120 "$SERVER_LOG" >&2 || true
    fi
  fi

  if [[ "$E2E_INSTALL_SUCCESS" -eq 1 && "${KEEP_E2E_TMP:-0}" != "1" ]]; then
    rm -rf "$TMP_ROOT"
  else
    echo "[e2e-install] keeping temp directory: $TMP_ROOT" >&2
  fi
}

main() {
  if [[ $# -ne 1 ]]; then
    usage
    exit 64
  fi

  local target="$1"
  local repo_root
  repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
  local package_script="$repo_root/scripts/package-cortexctl-release.sh"
  local install_script="$repo_root/scripts/install-cortexctl.sh"
  local e2e_script="$repo_root/scripts/ci/e2e-stack.sh"
  local python_bin="${PYTHON_BIN:-python3}"
  local bun_bin=""
  local run_stamp
  run_stamp="$(date +%s)_$$_$RANDOM"
  local bundle_version="ci-e2e-${run_stamp}"

  need_cmd curl
  need_cmd "$python_bin"
  need_cmd tar
  need_cmd bash

  if command -v bun >/dev/null 2>&1; then
    bun_bin="$(command -v bun)"
  fi

  TMP_ROOT="$(mktemp -d)"
  local dist_dir="$TMP_ROOT/dist"
  local isolated_home="$TMP_ROOT/home"
  SERVER_LOG="$TMP_ROOT/artifact-server.log"

  echo "[e2e-install] packaging release bundle for target: $target"
  "$package_script" "$target" "$dist_dir"

  local artifact_port
  artifact_port="$(pick_open_port "$python_bin")"
  local asset_base_url="http://127.0.0.1:${artifact_port}"

  echo "[e2e-install] starting local artifact server: $asset_base_url"
  "$python_bin" -m http.server "$artifact_port" --bind 127.0.0.1 --directory "$dist_dir" >"$SERVER_LOG" 2>&1 &
  SERVER_PID="$!"
  wait_for_local_server "$asset_base_url" 30

  echo "[e2e-install] installing via install script from local artifacts"
  export HOME="$isolated_home"
  mkdir -p "$HOME"
  local base_path="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
  if [[ -n "$bun_bin" ]]; then
    base_path="$(dirname "$bun_bin"):$base_path"
  fi
  export PATH="$base_path"
  "$install_script" \
    --asset-base-url "$asset_base_url" \
    --version "$bundle_version" \
    --skip-clickhouse

  export PATH="$HOME/.local/bin:$PATH"
  local installed_cortexctl="$HOME/.local/bin/cortexctl"
  if [[ ! -x "$installed_cortexctl" ]]; then
    echo "missing installed cortexctl binary: $installed_cortexctl" >&2
    exit 1
  fi

  echo "[e2e-install] running functional stack + MCP smoke with installed cortexctl"
  unset CORTEX_SOURCE_TREE_MODE
  CORTEXCTL_BIN="$installed_cortexctl" PYTHON_BIN="$python_bin" bash "$e2e_script"
  E2E_INSTALL_SUCCESS=1
}

trap cleanup_e2e_install EXIT
main "$@"
