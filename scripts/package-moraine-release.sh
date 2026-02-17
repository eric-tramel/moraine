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

cargo build --manifest-path "$PROJECT_ROOT/Cargo.toml" --release --locked --target "$TARGET" \
  -p moraine \
  -p moraine-ingest \
  -p moraine-monitor \
  -p moraine-mcp

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

mkdir -p "$STAGE_DIR/bin" "$STAGE_DIR/web/monitor"

cp "$PROJECT_ROOT/target/$TARGET/release/moraine" "$STAGE_DIR/bin/moraine"
cp "$PROJECT_ROOT/target/$TARGET/release/moraine-ingest" "$STAGE_DIR/bin/moraine-ingest"
cp "$PROJECT_ROOT/target/$TARGET/release/moraine-monitor" "$STAGE_DIR/bin/moraine-monitor"
cp "$PROJECT_ROOT/target/$TARGET/release/moraine-mcp" "$STAGE_DIR/bin/moraine-mcp"
cp -R "$PROJECT_ROOT/web/monitor/dist" "$STAGE_DIR/web/monitor/dist"

build_timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
git_commit="$(git -C "$PROJECT_ROOT" rev-parse --verify HEAD 2>/dev/null || echo "unknown")"
rustc_version="$(rustc -V 2>/dev/null || echo "unknown")"
checksum_moraine="$(sha256_file "$STAGE_DIR/bin/moraine")"
checksum_ingest="$(sha256_file "$STAGE_DIR/bin/moraine-ingest")"
checksum_monitor="$(sha256_file "$STAGE_DIR/bin/moraine-monitor")"
checksum_mcp="$(sha256_file "$STAGE_DIR/bin/moraine-mcp")"

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
    "bin/moraine-mcp": "$checksum_mcp"
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
