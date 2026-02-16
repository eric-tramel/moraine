#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: scripts/package-cortexctl-release.sh <target-triple> [output-dir]

examples:
  scripts/package-cortexctl-release.sh x86_64-unknown-linux-gnu dist
  scripts/package-cortexctl-release.sh aarch64-apple-darwin dist
EOF
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage
  exit 64
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo is not installed or not on PATH"
  exit 1
fi

TARGET="$1"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${2:-$PROJECT_ROOT/dist}"

mkdir -p "$OUTPUT_DIR"

cargo build --manifest-path "$PROJECT_ROOT/Cargo.toml" --release --locked --target "$TARGET" \
  -p cortexctl \
  -p cortex-ingest \
  -p cortex-monitor \
  -p cortex-mcp

for bin in cortexctl cortex-ingest cortex-monitor cortex-mcp; do
  if [[ ! -x "$PROJECT_ROOT/target/$TARGET/release/$bin" ]]; then
    echo "expected built binary at $PROJECT_ROOT/target/$TARGET/release/$bin"
    exit 1
  fi
done

ARCHIVE_NAME="cortex-bundle-$TARGET.tar.gz"
ARCHIVE_PATH="$OUTPUT_DIR/$ARCHIVE_NAME"
CHECKSUM_PATH="$OUTPUT_DIR/cortex-bundle-$TARGET.sha256"

STAGE_DIR="$(mktemp -d)"
trap 'rm -rf "$STAGE_DIR"' EXIT

mkdir -p "$STAGE_DIR/bin" "$STAGE_DIR/web"

cp "$PROJECT_ROOT/target/$TARGET/release/cortexctl" "$STAGE_DIR/bin/cortexctl"
cp "$PROJECT_ROOT/target/$TARGET/release/cortex-ingest" "$STAGE_DIR/bin/cortex-ingest"
cp "$PROJECT_ROOT/target/$TARGET/release/cortex-monitor" "$STAGE_DIR/bin/cortex-monitor"
cp "$PROJECT_ROOT/target/$TARGET/release/cortex-mcp" "$STAGE_DIR/bin/cortex-mcp"
cp -R "$PROJECT_ROOT/web/monitor" "$STAGE_DIR/web/monitor"

cat > "$STAGE_DIR/manifest.json" <<EOF
{
  "target": "$TARGET",
  "version": "${GITHUB_REF_NAME:-dev}",
  "binaries": [
    "cortexctl",
    "cortex-ingest",
    "cortex-monitor",
    "cortex-mcp"
  ],
  "web_assets": [
    "web/monitor"
  ]
}
EOF

tar -C "$STAGE_DIR" -czf "$ARCHIVE_PATH" .

if command -v sha256sum >/dev/null 2>&1; then
  sha256sum "$ARCHIVE_PATH" | awk -v archive="$ARCHIVE_NAME" '{print $1 "  " archive}' > "$CHECKSUM_PATH"
elif command -v shasum >/dev/null 2>&1; then
  shasum -a 256 "$ARCHIVE_PATH" | awk -v archive="$ARCHIVE_NAME" '{print $1 "  " archive}' > "$CHECKSUM_PATH"
else
  echo "neither sha256sum nor shasum found; cannot produce checksum file"
  exit 1
fi

echo "packaged: $ARCHIVE_PATH"
echo "checksum: $CHECKSUM_PATH"
