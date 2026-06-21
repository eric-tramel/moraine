#!/usr/bin/env bash
set -euo pipefail

E2E_INSTALL_SUCCESS=0
TMP_ROOT=""

usage() {
  cat <<'EOF'
usage: scripts/ci/e2e-install-artifact.sh <target-triple>

example:
  scripts/ci/e2e-install-artifact.sh x86_64-apple-darwin
  scripts/ci/e2e-install-artifact.sh aarch64-apple-darwin
EOF
}

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "required command not found: $1" >&2
    exit 1
  fi
}

cleanup_e2e_install() {
  if [[ -z "$TMP_ROOT" ]]; then
    return
  fi

  if [[ "$E2E_INSTALL_SUCCESS" -ne 1 ]]; then
    echo "[e2e-install] failure diagnostics (tmp root: $TMP_ROOT)" >&2
    if [[ -d "$TMP_ROOT/dist" ]]; then
      echo "--- artifacts in $TMP_ROOT/dist ---" >&2
      find "$TMP_ROOT/dist" -maxdepth 1 -type f -print >&2 || true
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
  local package_script="$repo_root/scripts/package-moraine-release.sh"
  local install_script="$repo_root/scripts/install.sh"
  local e2e_script="$repo_root/scripts/ci/e2e-stack.sh"
  local python_bin="${PYTHON_BIN:-python3}"
  local run_stamp
  run_stamp="$(date +%s)_$$_$RANDOM"
  local bundle_version="ci-e2e-${run_stamp}"

  need_cmd curl
  need_cmd "$python_bin"
  need_cmd tar
  need_cmd bash

  TMP_ROOT="$(mktemp -d)"
  local dist_dir="$TMP_ROOT/dist"
  local isolated_home="$TMP_ROOT/home"

  echo "[e2e-install] packaging release bundle for target: $target"
  "$package_script" "$target" "$dist_dir"

  local asset_base_url="file://$dist_dir"

  echo "[e2e-install] installing via install script from local artifacts"
  export HOME="$isolated_home"
  mkdir -p "$HOME"
  local base_path="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
  export PATH="$base_path"
  MORAINE_INSTALL_ASSET_BASE_URL="$asset_base_url" \
    MORAINE_INSTALL_VERSION="$bundle_version" \
    MORAINE_INSTALL_SKIP_CLICKHOUSE=1 \
    MORAINE_INSTALL_DIR="$HOME/.local/bin" \
    bash "$install_script"

  export PATH="$HOME/.local/bin:$PATH"
  local installed_moraine="$HOME/.local/bin/moraine"
  if [[ ! -x "$installed_moraine" ]]; then
    echo "missing installed moraine binary: $installed_moraine" >&2
    exit 1
  fi
  local installed_monitor_index="$HOME/.local/web/monitor/dist/index.html"
  if [[ ! -f "$installed_monitor_index" ]]; then
    echo "missing installed monitor assets: $installed_monitor_index" >&2
    exit 1
  fi
  local installed_config="$HOME/.moraine/config.toml"
  if [[ ! -f "$installed_config" ]]; then
    echo "missing installed config bootstrap: $installed_config" >&2
    exit 1
  fi
  "$installed_moraine" config get clickhouse.url --config "$installed_config" >/dev/null

  echo "[e2e-install] running functional stack + MCP smoke with installed moraine"
  unset MORAINE_SOURCE_TREE_MODE
  MORAINECTL_BIN="$installed_moraine" PYTHON_BIN="$python_bin" bash "$e2e_script"
  E2E_INSTALL_SUCCESS=1
}

trap cleanup_e2e_install EXIT
main "$@"
