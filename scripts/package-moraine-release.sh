#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: scripts/package-moraine-release.sh <target-triple> [output-dir]

examples:
  scripts/package-moraine-release.sh x86_64-unknown-linux-gnu dist
  scripts/package-moraine-release.sh aarch64-apple-darwin dist
EOF
}

sha256_file() {
  local path="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$path" | awk '{print $1}'
    return
  fi
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$path" | awk '{print $1}'
    return
  fi
  echo "neither sha256sum nor shasum found; cannot compute checksum for $path" >&2
  exit 1
}

build_monitor_frontend() {
  local monitor_dir="$PROJECT_ROOT/web/monitor"
  echo "building monitor frontend with bun"
  (
    cd "$monitor_dir"
    bun install --frozen-lockfile
    bun run build
  )
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage
  exit 64
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo is not installed or not on PATH"
  exit 1
fi

if ! command -v bun >/dev/null 2>&1; then
  echo "bun is not installed or not on PATH"
  exit 1
fi

TARGET="$1"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${2:-$PROJECT_ROOT/dist}"

mkdir -p "$OUTPUT_DIR"

build_monitor_frontend

if [[ ! -d "$PROJECT_ROOT/web/monitor/dist" ]]; then
  echo "expected built frontend assets at $PROJECT_ROOT/web/monitor/dist"
  exit 1
fi

cargo_build_native() {
  cargo build --manifest-path "$PROJECT_ROOT/Cargo.toml" --release --locked --target "$TARGET" \
    -p moraine \
    -p moraine-ingest \
    -p moraine-monitor \
    -p moraine-mcp
}

# `MORAINE_CARGO_CONTAINER`: optional Docker image that wraps the Rust
# build so the resulting binaries link against the container's glibc
# floor, not the host's. Used by the linux release legs to honour the
# wheel's `manylinux_2_28_*` platform tag — ubuntu-24.04 runners ship
# glibc 2.39, which leaked into v0.4.2rc1 and broke every non-latest
# linux distro (see issue #246). Unset by default so host builds and
# the macOS release leg are unchanged.
cargo_build_in_container() {
  local image="$1"
  echo "cargo build for $TARGET inside container image: $image"
  if ! command -v docker >/dev/null 2>&1; then
    echo "MORAINE_CARGO_CONTAINER is set but docker is not on PATH" >&2
    exit 1
  fi

  # `HOME=/workspace/.cargo-container-home` keeps rustup's on-disk state
  # inside the mounted worktree so it survives across invocations (cache
  # the stable toolchain + target) and is owned by the host user. Cargo
  # target dir is the project's default `target/<target>/`, unchanged.
  docker run --rm \
    -v "$PROJECT_ROOT:/workspace" \
    -w /workspace \
    --user "$(id -u):$(id -g)" \
    -e HOME=/workspace/.cargo-container-home \
    -e CARGO_TERM_COLOR=never \
    "$image" bash -c '
      set -euo pipefail
      export RUSTUP_HOME="$HOME/rustup"
      export CARGO_HOME="$HOME/cargo"
      mkdir -p "$RUSTUP_HOME" "$CARGO_HOME"
      if [[ ! -x "$CARGO_HOME/bin/cargo" ]]; then
        echo "installing rustup + stable toolchain inside container..."
        curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | \
          sh -s -- --default-toolchain stable --profile minimal -y
      fi
      export PATH="$CARGO_HOME/bin:$PATH"
      rustup target add "'"$TARGET"'"
      cargo build --manifest-path /workspace/Cargo.toml --release --locked --target "'"$TARGET"'" \
        -p moraine -p moraine-ingest -p moraine-monitor -p moraine-mcp
    '
}

if [[ -n "${MORAINE_CARGO_CONTAINER:-}" ]]; then
  cargo_build_in_container "$MORAINE_CARGO_CONTAINER"
else
  cargo_build_native
fi

for bin in moraine moraine-ingest moraine-monitor moraine-mcp; do
  if [[ ! -x "$PROJECT_ROOT/target/$TARGET/release/$bin" ]]; then
    echo "expected built binary at $PROJECT_ROOT/target/$TARGET/release/$bin"
    exit 1
  fi
done

ARCHIVE_NAME="moraine-bundle-$TARGET.tar.gz"
ARCHIVE_PATH="$OUTPUT_DIR/$ARCHIVE_NAME"
CHECKSUM_PATH="$OUTPUT_DIR/moraine-bundle-$TARGET.sha256"

STAGE_DIR="$(mktemp -d)"
trap 'rm -rf "$STAGE_DIR"' EXIT

mkdir -p "$STAGE_DIR/bin" "$STAGE_DIR/config" "$STAGE_DIR/web/monitor"

cp "$PROJECT_ROOT/target/$TARGET/release/moraine" "$STAGE_DIR/bin/moraine"
cp "$PROJECT_ROOT/target/$TARGET/release/moraine-ingest" "$STAGE_DIR/bin/moraine-ingest"
cp "$PROJECT_ROOT/target/$TARGET/release/moraine-monitor" "$STAGE_DIR/bin/moraine-monitor"
cp "$PROJECT_ROOT/target/$TARGET/release/moraine-mcp" "$STAGE_DIR/bin/moraine-mcp"
cp "$PROJECT_ROOT/config/moraine.toml" "$STAGE_DIR/config/moraine.toml"
cp -R "$PROJECT_ROOT/web/monitor/dist" "$STAGE_DIR/web/monitor/dist"

build_timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
git_commit="$(git -C "$PROJECT_ROOT" rev-parse --verify HEAD 2>/dev/null || echo "unknown")"
rustc_version="$(rustc -V 2>/dev/null || echo "unknown")"
checksum_moraine="$(sha256_file "$STAGE_DIR/bin/moraine")"
checksum_ingest="$(sha256_file "$STAGE_DIR/bin/moraine-ingest")"
checksum_monitor="$(sha256_file "$STAGE_DIR/bin/moraine-monitor")"
checksum_mcp="$(sha256_file "$STAGE_DIR/bin/moraine-mcp")"
checksum_config="$(sha256_file "$STAGE_DIR/config/moraine.toml")"

cat > "$STAGE_DIR/manifest.json" <<EOF
{
  "bundle_format_version": 1,
  "target": "$TARGET",
  "version": "${GITHUB_REF_NAME:-dev}",
  "build": {
    "timestamp_utc": "$build_timestamp",
    "git_commit": "$git_commit",
    "rustc": "$rustc_version"
  },
  "binaries": [
    "moraine",
    "moraine-ingest",
    "moraine-monitor",
    "moraine-mcp"
  ],
  "checksums": {
    "bin/moraine": "$checksum_moraine",
    "bin/moraine-ingest": "$checksum_ingest",
    "bin/moraine-monitor": "$checksum_monitor",
    "bin/moraine-mcp": "$checksum_mcp",
    "config/moraine.toml": "$checksum_config"
  },
  "web_assets": [
    "web/monitor/dist"
  ]
}
EOF

tar -C "$STAGE_DIR" -czf "$ARCHIVE_PATH" .
echo "$(sha256_file "$ARCHIVE_PATH")  $ARCHIVE_NAME" > "$CHECKSUM_PATH"

echo "packaged: $ARCHIVE_PATH"
echo "checksum: $CHECKSUM_PATH"
