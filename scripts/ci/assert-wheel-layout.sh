#!/usr/bin/env bash
set -euo pipefail

# Assert that a moraine wheel contains the members the exec shim and
# downstream consumers rely on. Used by ci-packaging.yml but runnable
# locally for quick iteration.

usage() {
  cat <<'EOF'
usage: scripts/ci/assert-wheel-layout.sh <path/to/moraine-X.Y.Z-py3-none-<plat>.whl>

Verifies the wheel contains:
  - moraine_cli/__init__.py (exec shim)
  - moraine_cli/_version.py
  - moraine_cli/_binaries/{moraine,moraine-ingest,moraine-monitor,moraine-mcp}
  - moraine_cli/_data/config/moraine.toml
  - moraine_cli/_data/web/monitor/dist/index.html
  - moraine_cli/_data/manifest.json
  - moraine-<version>.dist-info/{METADATA,WHEEL,entry_points.txt,RECORD}

Does NOT unpack — just inspects the zip directory.
EOF
}

if [[ $# -ne 1 ]]; then
  usage
  exit 64
fi

wheel="$1"
if [[ ! -f "$wheel" ]]; then
  echo "wheel not found: $wheel" >&2
  exit 1
fi

# Capture zip member list once, grep against it below.
members="$(unzip -l "$wheel" | awk 'NR>3 && $NF !~ /^-+$/ {print $NF}')"

require_member() {
  local name="$1"
  if ! printf '%s\n' "$members" | grep -Fqx "$name"; then
    echo "wheel is missing required member: $name" >&2
    exit 1
  fi
}

require_prefix() {
  local prefix="$1"
  if ! printf '%s\n' "$members" | grep -q "^${prefix}"; then
    echo "wheel is missing any member under: $prefix" >&2
    exit 1
  fi
}

require_member "moraine_cli/__init__.py"
require_member "moraine_cli/_version.py"
require_member "moraine_cli/_binaries/moraine"
require_member "moraine_cli/_binaries/moraine-ingest"
require_member "moraine_cli/_binaries/moraine-monitor"
require_member "moraine_cli/_binaries/moraine-mcp"
require_member "moraine_cli/_data/config/moraine.toml"
require_member "moraine_cli/_data/web/monitor/dist/index.html"
require_member "moraine_cli/_data/manifest.json"
require_prefix "moraine_cli-"     # dist-info directory (PEP 427 normalizes moraine-cli → moraine_cli)
require_prefix "moraine_cli/_data/web/monitor/dist/"

# dist-info files
dist_info="$(printf '%s\n' "$members" | grep -E '^moraine_cli-.*\.dist-info/' | head -1 | awk -F/ '{print $1}')"
if [[ -z "$dist_info" ]]; then
  echo "wheel has no <name>.dist-info/ directory" >&2
  exit 1
fi
require_member "${dist_info}/METADATA"
require_member "${dist_info}/WHEEL"
require_member "${dist_info}/entry_points.txt"
require_member "${dist_info}/RECORD"

# Executable bit check: the binaries must be stored with mode 0o100755 in
# the zip external_attr upper bits so pip preserves +x on unpack. This
# regression is easy to introduce and silently breaks console-script exec
# on some extractors (see history: we got burned by this during the #223
# smoke test).
python3 - "$wheel" <<'PY'
import sys, zipfile
wheel = sys.argv[1]
missing = []
with zipfile.ZipFile(wheel) as zf:
    for info in zf.infolist():
        if "_binaries/" in info.filename and not info.filename.endswith("/"):
            mode = (info.external_attr >> 16) & 0xFFFF
            if mode != 0o100755:
                missing.append((info.filename, oct(mode)))
if missing:
    for name, mode in missing:
        sys.stderr.write(f"bundled binary {name} has external_attr mode {mode}; expected 0o100755\n")
    sys.exit(1)
PY

echo "wheel layout OK: $wheel"
