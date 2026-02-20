#!/usr/bin/env sh
set -eu

usage() {
  cat <<'USAGE'
Install Moraine binaries from release bundle artifacts.

usage:
  scripts/install.sh [--help]

configuration (environment variables):
  MORAINE_INSTALL_REPO             GitHub repository hosting release assets
                                   (default: eric-tramel/moraine)
  MORAINE_INSTALL_VERSION          Release tag to install (default: latest)
  MORAINE_INSTALL_ASSET_BASE_URL   Base URL hosting moraine-bundle-<target>.tar.gz and .sha256
                                   (requires MORAINE_INSTALL_VERSION to be set to a tag)
  MORAINE_INSTALL_SKIP_CLICKHOUSE  Skip managed ClickHouse install when set to 1/true/yes/on

  MORAINE_INSTALL_DIR              Explicit install bin directory (highest precedence)
  XDG_BIN_HOME                     Install bin directory when MORAINE_INSTALL_DIR is unset
  XDG_DATA_HOME                    Fallback install location is "$(dirname XDG_DATA_HOME)/bin"
  HOME                             Final fallback install dir: ~/.local/bin
  XDG_CONFIG_HOME                  Receipt root (default: ~/.config)

install directory precedence:
  MORAINE_INSTALL_DIR > XDG_BIN_HOME > $(dirname XDG_DATA_HOME)/bin > ~/.local/bin

examples:
  scripts/install.sh
  MORAINE_INSTALL_VERSION=v0.2.1 scripts/install.sh
  MORAINE_INSTALL_ASSET_BASE_URL=http://127.0.0.1:8080 \
    MORAINE_INSTALL_VERSION=ci-e2e scripts/install.sh
  MORAINE_INSTALL_DIR="$HOME/bin" MORAINE_INSTALL_SKIP_CLICKHOUSE=1 scripts/install.sh
USAGE
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "required command not found: $1"
    exit 1
  fi
}

is_truthy() {
  case "${1:-}" in
    1|[Tt][Rr][Uu][Ee]|[Yy][Ee][Ss]|[Oo][Nn])
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

resolve_install_dir() {
  if [ -n "${MORAINE_INSTALL_DIR:-}" ]; then
    printf '%s\n' "$MORAINE_INSTALL_DIR"
    return
  fi

  if [ -n "${XDG_BIN_HOME:-}" ]; then
    printf '%s\n' "$XDG_BIN_HOME"
    return
  fi

  if [ -n "${XDG_DATA_HOME:-}" ]; then
    printf '%s/bin\n' "$(dirname "$XDG_DATA_HOME")"
    return
  fi

  printf '%s/.local/bin\n' "${HOME:?HOME is not set}"
}

resolve_receipt_path() {
  config_home="${XDG_CONFIG_HOME:-${HOME:?HOME is not set}/.config}"
  printf '%s/moraine/install-receipt.json\n' "$config_home"
}

resolve_runtime_config_path() {
  if [ -n "${MORAINE_CONFIG:-}" ]; then
    printf '%s\n' "$MORAINE_CONFIG"
    return
  fi

  printf '%s/.moraine/config.toml\n' "${HOME:?HOME is not set}"
}

write_minimal_default_config() {
  path="$1"

  cat >"$path" <<'CONFIG'
# Moraine default config.
# Values omitted here are filled by built-in defaults.
CONFIG
}

bootstrap_runtime_config() {
  config_path="$1"
  template_path="$2"

  if [ -f "$config_path" ]; then
    return 0
  fi

  config_dir="$(dirname "$config_path")"
  if ! mkdir -p "$config_dir"; then
    echo "warning: failed to create config directory: $config_dir"
    return 1
  fi

  if [ -f "$template_path" ]; then
    if ! cp "$template_path" "$config_path"; then
      echo "warning: failed to write config from bundle template: $config_path"
      return 1
    fi
    echo "wrote default config: $config_path"
    return 0
  fi

  if ! write_minimal_default_config "$config_path"; then
    echo "warning: failed to write minimal config: $config_path"
    return 1
  fi

  echo "wrote minimal default config: $config_path"
  return 0
}

checksum_of() {
  path="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$path" | awk '{print $1}'
    return
  fi
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$path" | awk '{print $1}'
    return
  fi
  if command -v openssl >/dev/null 2>&1; then
    openssl dgst -sha256 "$path" | awk '{print $NF}'
    return
  fi
  echo "no checksum command available for $path" >&2
  exit 1
}

manifest_checksum() {
  manifest="$1"
  key="$2"
  awk -v key="$key" '
    $0 ~ "\"" key "\"" {
      if (match($0, /"[0-9A-Fa-f]+"/)) {
        value = substr($0, RSTART + 1, RLENGTH - 2)
        print value
        exit
      }
    }
  ' "$manifest"
}

verify_checksum() {
  archive="$1"
  checksum_file="$2"

  if command -v sha256sum >/dev/null 2>&1; then
    (cd "$(dirname "$archive")" && sha256sum -c "$checksum_file")
    return
  fi

  if command -v shasum >/dev/null 2>&1; then
    expected="$(awk '{print $1}' "$checksum_file")"
    actual="$(shasum -a 256 "$archive" | awk '{print $1}')"
    if [ "$expected" != "$actual" ]; then
      echo "checksum mismatch for $archive"
      exit 1
    fi
    return
  fi

  if command -v openssl >/dev/null 2>&1; then
    expected="$(awk '{print $1}' "$checksum_file")"
    actual="$(openssl dgst -sha256 "$archive" | awk '{print $NF}')"
    if [ "$expected" != "$actual" ]; then
      echo "checksum mismatch for $archive"
      exit 1
    fi
    return
  fi

  echo "unable to verify checksum for $archive: no sha256sum/shasum/openssl found" >&2
  exit 1
}

detect_target_triple() {
  os="$(uname -s)"
  arch="$(uname -m)"

  case "$os" in
    Darwin) os="apple-darwin" ;;
    Linux) os="unknown-linux-gnu" ;;
    *)
      echo "unsupported OS: $os (supported: Darwin, Linux)"
      exit 1
      ;;
  esac

  case "$arch" in
    x86_64|amd64) arch="x86_64" ;;
    arm64|aarch64) arch="aarch64" ;;
    *)
      echo "unsupported architecture: $arch (supported: x86_64, arm64/aarch64)"
      exit 1
      ;;
  esac

  printf '%s\n' "${arch}-${os}"
}

fetch_latest_tag() {
  repo="$1"
  api_url="https://api.github.com/repos/${repo}/releases/latest"

  if ! response="$(curl -fsSL "$api_url")"; then
    echo "failed to resolve latest release tag from $api_url"
    exit 1
  fi

  tag="$(printf '%s\n' "$response" | awk -F '"' '/"tag_name"[[:space:]]*:/ { print $4; exit }')"
  if [ -z "$tag" ]; then
    echo "failed to parse latest release tag from $api_url"
    exit 1
  fi

  printf '%s\n' "$tag"
}

json_escape() {
  printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'
}

write_install_receipt() {
  receipt_path="$1"
  installed_at_utc="$2"
  repo="$3"
  requested_version="$4"
  resolved_version="$5"
  target="$6"
  install_dir="$7"
  asset_url="$8"
  checksum_url="$9"

  mkdir -p "$(dirname "$receipt_path")"

  cat >"$receipt_path" <<RECEIPT
{
  "schema_version": 1,
  "installed_at_utc": "$(json_escape "$installed_at_utc")",
  "repo": "$(json_escape "$repo")",
  "requested_version": "$(json_escape "$requested_version")",
  "resolved_version": "$(json_escape "$resolved_version")",
  "target": "$(json_escape "$target")",
  "install_dir": "$(json_escape "$install_dir")",
  "asset_url": "$(json_escape "$asset_url")",
  "checksum_url": "$(json_escape "$checksum_url")",
  "binaries": [
    "moraine",
    "moraine-ingest",
    "moraine-monitor",
    "moraine-mcp"
  ]
}
RECEIPT
}

if [ "$#" -gt 0 ]; then
  if [ "$#" -eq 1 ] && { [ "$1" = "-h" ] || [ "$1" = "--help" ]; }; then
    usage
    exit 0
  fi

  echo "unknown argument(s): $*" >&2
  echo "install.sh is configured via environment variables; run with --help for details" >&2
  exit 64
fi

repo="${MORAINE_INSTALL_REPO:-eric-tramel/moraine}"
asset_base_url="${MORAINE_INSTALL_ASSET_BASE_URL:-}"
requested_version="${MORAINE_INSTALL_VERSION:-latest}"
version="$requested_version"
install_dir="$(resolve_install_dir)"
receipt_path="$(resolve_receipt_path)"
runtime_config_path="$(resolve_runtime_config_path)"
skip_clickhouse=0
bins="moraine moraine-ingest moraine-monitor moraine-mcp"

if is_truthy "${MORAINE_INSTALL_SKIP_CLICKHOUSE:-}"; then
  skip_clickhouse=1
fi

if [ -n "$asset_base_url" ]; then
  asset_base_url=${asset_base_url%/}
  if [ "$version" = "latest" ]; then
    echo "MORAINE_INSTALL_VERSION <tag> is required when MORAINE_INSTALL_ASSET_BASE_URL is set"
    usage
    exit 64
  fi
fi

require_cmd curl
require_cmd tar

target="$(detect_target_triple)"
if [ "$version" = "latest" ]; then
  version="$(fetch_latest_tag "$repo")"
fi

asset_name="moraine-bundle-${target}.tar.gz"
checksum_name="moraine-bundle-${target}.sha256"
if [ -n "$asset_base_url" ]; then
  asset_url="${asset_base_url}/${asset_name}"
  checksum_url="${asset_base_url}/${checksum_name}"
else
  asset_url="https://github.com/${repo}/releases/download/${version}/${asset_name}"
  checksum_url="https://github.com/${repo}/releases/download/${version}/${checksum_name}"
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' 0 HUP INT TERM

echo "installing Moraine ${version} for ${target}"
echo "downloading: ${asset_url}"
if ! curl -fL "$asset_url" -o "$tmp_dir/$asset_name"; then
  echo "failed to download release asset: $asset_url"
  if [ -n "$asset_base_url" ]; then
    echo "verify that the artifact is available at:"
    echo "  $asset_url"
  else
    echo "verify that repo/tag/target exists in GitHub Releases:"
    echo "  https://github.com/${repo}/releases/tag/${version}"
  fi
  exit 1
fi
if ! curl -fL "$checksum_url" -o "$tmp_dir/$checksum_name"; then
  echo "failed to download release checksum: $checksum_url"
  if [ -n "$asset_base_url" ]; then
    echo "verify that the checksum is available at:"
    echo "  $checksum_url"
  else
    echo "verify that repo/tag/target exists in GitHub Releases:"
    echo "  https://github.com/${repo}/releases/tag/${version}"
  fi
  exit 1
fi

verify_checksum "$tmp_dir/$asset_name" "$tmp_dir/$checksum_name"

extract_dir="$tmp_dir/extracted"
mkdir -p "$extract_dir"
tar -xzf "$tmp_dir/$asset_name" -C "$extract_dir"

if [ ! -f "$extract_dir/manifest.json" ]; then
  echo "archive did not contain required manifest: manifest.json"
  exit 1
fi

manifest_target="$(awk -F '"' '/"target"[[:space:]]*:/ { print $4; exit }' "$extract_dir/manifest.json")"
if [ -n "$manifest_target" ] && [ "$manifest_target" != "$target" ]; then
  echo "bundle target mismatch: expected $target, manifest has $manifest_target"
  exit 1
fi

for bin in $bins; do
  if [ ! -f "$extract_dir/bin/$bin" ]; then
    echo "archive did not contain required binary: bin/$bin"
    exit 1
  fi
  expected_sum="$(manifest_checksum "$extract_dir/manifest.json" "bin/$bin")"
  if [ -n "$expected_sum" ]; then
    actual_sum="$(checksum_of "$extract_dir/bin/$bin")"
    if [ "$expected_sum" != "$actual_sum" ]; then
      echo "manifest checksum mismatch for bin/$bin"
      echo "expected: $expected_sum"
      echo "actual:   $actual_sum"
      exit 1
    fi
  fi
done

if [ -f "$extract_dir/config/moraine.toml" ]; then
  expected_sum="$(manifest_checksum "$extract_dir/manifest.json" "config/moraine.toml")"
  if [ -n "$expected_sum" ]; then
    actual_sum="$(checksum_of "$extract_dir/config/moraine.toml")"
    if [ "$expected_sum" != "$actual_sum" ]; then
      echo "manifest checksum mismatch for config/moraine.toml"
      echo "expected: $expected_sum"
      echo "actual:   $actual_sum"
      exit 1
    fi
  fi
fi

mkdir -p "$install_dir"
for bin in $bins; do
  cp "$extract_dir/bin/$bin" "$install_dir/$bin"
  chmod +x "$install_dir/$bin"
done

monitor_dist_source="$extract_dir/web/monitor/dist"
if [ ! -d "$monitor_dist_source" ]; then
  echo "archive did not contain required monitor assets: web/monitor/dist"
  exit 1
fi

install_root="$(dirname "$install_dir")"
monitor_dist_dir="$install_root/web/monitor/dist"
mkdir -p "$(dirname "$monitor_dist_dir")"
rm -rf "$monitor_dist_dir"
cp -R "$monitor_dist_source" "$monitor_dist_dir"

for bin in $bins; do
  if ! "$install_dir/$bin" --help >/dev/null 2>&1; then
    echo "installed binary failed health check (--help): $install_dir/$bin"
    exit 1
  fi
done

installed_at_utc="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
write_install_receipt \
  "$receipt_path" \
  "$installed_at_utc" \
  "$repo" \
  "$requested_version" \
  "$version" \
  "$target" \
  "$install_dir" \
  "$asset_url" \
  "$checksum_url"

echo "installed binaries to: $install_dir"
echo "installed monitor assets to: $monitor_dist_dir"
echo "wrote install receipt: $receipt_path"

if ! bootstrap_runtime_config "$runtime_config_path" "$extract_dir/config/moraine.toml"; then
  echo "warning: unable to bootstrap config at $runtime_config_path"
fi

case ":${PATH:-}:" in
  *":$install_dir:"*)
    ;;
  *)
    echo
    echo "note: $install_dir is not currently on PATH."
    echo "add this to your shell profile:"
    echo "  export PATH=\"$install_dir:\$PATH\""
    ;;
esac

if [ "$skip_clickhouse" -ne 1 ]; then
  echo
  echo "installing managed ClickHouse..."
  if ! "$install_dir/moraine" clickhouse install --config "$runtime_config_path"; then
    echo "warning: managed ClickHouse install failed."
    echo "you can retry with:"
    echo "  moraine clickhouse install --config \"$runtime_config_path\""
  fi
fi

echo
echo "verify install:"
echo "  moraine --help"
echo "  moraine status"
