#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Install Cortex binaries from GitHub Releases.

usage:
  scripts/install-cortexctl.sh --repo <owner/repo> [options]

options:
  --repo <owner/repo>   GitHub repository hosting release assets (required)
  --version <tag>       Release tag (default: latest)
  --install-dir <path>  Destination directory for binary (default: ~/.local/bin)
  --lib-dir <path>      Destination root for versioned bundle (default: ~/.local/lib/cortex)
  --skip-clickhouse     Do not auto-install managed ClickHouse
  --force               Replace existing binary without prompting
  -h, --help            Show help

examples:
  scripts/install-cortexctl.sh --repo eric-tramel/cortex
  scripts/install-cortexctl.sh --repo eric-tramel/cortex --version v0.1.0
EOF
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "required command not found: $1"
    exit 1
  fi
}

checksum_of() {
  local path="$1"
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
  local manifest="$1"
  local key="$2"
  grep -E "\"${key}\"[[:space:]]*:" "$manifest" \
    | head -n 1 \
    | sed -n 's/.*:[[:space:]]*"\([0-9a-fA-F]\+\)".*/\1/p'
}

verify_checksum() {
  local archive="$1"
  local checksum_file="$2"

  if command -v sha256sum >/dev/null 2>&1; then
    (cd "$(dirname "$archive")" && sha256sum -c "$checksum_file")
    return
  fi

  if command -v shasum >/dev/null 2>&1; then
    local expected actual
    expected="$(awk '{print $1}' "$checksum_file")"
    actual="$(shasum -a 256 "$archive" | awk '{print $1}')"
    if [[ "$expected" != "$actual" ]]; then
      echo "checksum mismatch for $archive"
      exit 1
    fi
    return
  fi

  if command -v openssl >/dev/null 2>&1; then
    local expected actual
    expected="$(awk '{print $1}' "$checksum_file")"
    actual="$(openssl dgst -sha256 "$archive" | awk '{print $NF}')"
    if [[ "$expected" != "$actual" ]]; then
      echo "checksum mismatch for $archive"
      exit 1
    fi
    return
  fi

  echo "warning: no sha256sum/shasum/openssl found; skipping checksum verification" >&2
}

detect_target_triple() {
  local os arch
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

  echo "${arch}-${os}"
}

fetch_latest_tag() {
  local repo="$1"
  local api_url="https://api.github.com/repos/${repo}/releases/latest"
  local tag

  tag="$(curl -fsSL "$api_url" | sed -n 's/^[[:space:]]*"tag_name":[[:space:]]*"\([^"]\+\)".*$/\1/p' | head -n 1)"
  if [[ -z "$tag" ]]; then
    echo "failed to resolve latest release tag from $api_url"
    exit 1
  fi

  echo "$tag"
}

repo=""
version="latest"
install_dir="${HOME}/.local/bin"
lib_dir="${HOME}/.local/lib/cortex"
skip_clickhouse=0
force=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)
      repo="${2:-}"
      shift 2
      ;;
    --version)
      version="${2:-}"
      shift 2
      ;;
    --install-dir)
      install_dir="${2:-}"
      shift 2
      ;;
    --lib-dir)
      lib_dir="${2:-}"
      shift 2
      ;;
    --skip-clickhouse)
      skip_clickhouse=1
      shift
      ;;
    --force)
      force=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1"
      usage
      exit 64
      ;;
  esac
done

if [[ -z "$repo" ]]; then
  echo "--repo is required"
  usage
  exit 64
fi

require_cmd curl
require_cmd tar

target="$(detect_target_triple)"
if [[ "$version" == "latest" ]]; then
  version="$(fetch_latest_tag "$repo")"
fi

asset_name="cortex-bundle-${target}.tar.gz"
checksum_name="cortex-bundle-${target}.sha256"
asset_url="https://github.com/${repo}/releases/download/${version}/${asset_name}"
checksum_url="https://github.com/${repo}/releases/download/${version}/${checksum_name}"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

echo "installing Cortex ${version} for ${target}"
echo "downloading: ${asset_url}"
if ! curl -fL "$asset_url" -o "$tmp_dir/$asset_name"; then
  echo "failed to download release asset: $asset_name"
  echo "verify that repo/tag/target exists in GitHub Releases:"
  echo "  https://github.com/${repo}/releases/tag/${version}"
  exit 1
fi
if ! curl -fL "$checksum_url" -o "$tmp_dir/$checksum_name"; then
  echo "failed to download release checksum: $checksum_name"
  echo "verify that repo/tag/target exists in GitHub Releases:"
  echo "  https://github.com/${repo}/releases/tag/${version}"
  exit 1
fi

verify_checksum "$tmp_dir/$asset_name" "$tmp_dir/$checksum_name"

extract_dir="$tmp_dir/extracted"
mkdir -p "$extract_dir"
tar -xzf "$tmp_dir/$asset_name" -C "$extract_dir"

if [[ ! -f "$extract_dir/manifest.json" ]]; then
  echo "archive did not contain required manifest: manifest.json"
  exit 1
fi

manifest_target="$(sed -n 's/^[[:space:]]*"target"[[:space:]]*:[[:space:]]*"\([^"]\+\)".*$/\1/p' "$extract_dir/manifest.json" | head -n 1)"
if [[ -n "$manifest_target" && "$manifest_target" != "$target" ]]; then
  echo "bundle target mismatch: expected $target, manifest has $manifest_target"
  exit 1
fi

for bin in cortexctl cortex-ingest cortex-monitor cortex-mcp; do
  if [[ ! -f "$extract_dir/bin/$bin" ]]; then
    echo "archive did not contain required binary: bin/$bin"
    exit 1
  fi
  expected_sum="$(manifest_checksum "$extract_dir/manifest.json" "bin/$bin")"
  if [[ -n "$expected_sum" ]]; then
    actual_sum="$(checksum_of "$extract_dir/bin/$bin")"
    if [[ "$expected_sum" != "$actual_sum" ]]; then
      echo "manifest checksum mismatch for bin/$bin"
      echo "expected: $expected_sum"
      echo "actual:   $actual_sum"
      exit 1
    fi
  fi
done

release_dir="$lib_dir/$version/$target"
current_link="$lib_dir/current"

if [[ -e "$release_dir" && "$force" -ne 1 ]]; then
  echo "destination already exists: $release_dir"
  echo "re-run with --force to replace it"
  exit 1
fi

rm -rf "$release_dir"
mkdir -p "$release_dir"
cp -R "$extract_dir"/. "$release_dir/"

mkdir -p "$lib_dir"
ln -sfn "$release_dir" "$current_link"

mkdir -p "$install_dir"
for bin in cortexctl cortex-ingest cortex-monitor cortex-mcp; do
  ln -sfn "$current_link/bin/$bin" "$install_dir/$bin"
done

for bin in cortexctl cortex-ingest cortex-monitor cortex-mcp; do
  if ! "$current_link/bin/$bin" --help >/dev/null 2>&1; then
    echo "installed binary failed health check (--help): $current_link/bin/$bin"
    exit 1
  fi
done

echo "installed bundle: $release_dir"
echo "active bundle: $current_link"
echo "linked binaries in: $install_dir"

if [[ ":$PATH:" != *":$install_dir:"* ]]; then
  echo
  echo "note: $install_dir is not currently on PATH."
  echo "add this to your shell profile:"
  echo "  export PATH=\"$install_dir:\$PATH\""
fi

if [[ "$skip_clickhouse" -ne 1 ]]; then
  echo
  echo "installing managed ClickHouse..."
  if ! "$current_link/bin/cortexctl" clickhouse install; then
    echo "warning: managed ClickHouse install failed."
    echo "you can retry with:"
    echo "  cortexctl clickhouse install"
  fi
fi

echo
echo "verify install:"
echo "  cortexctl --help"
echo "  cortexctl status"
