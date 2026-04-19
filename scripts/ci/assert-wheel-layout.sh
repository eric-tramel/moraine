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

# GLIBC floor check for manylinux wheels. Parse the glibc version from
# the wheel's platform tag (e.g. `manylinux_2_28_x86_64` → 2.28), extract
# each bundled binary into a temp dir, and run `readelf -V` to confirm
# no GLIBC_X.Y symbol exceeds the advertised floor. This regression is
# what shipped in v0.4.2rc1 (#246): ubuntu-24.04 runner leaked GLIBC_2.39
# symbols into wheels tagged manylinux_2_17. The check is skipped for
# non-manylinux wheels (macosx, etc.) and when readelf is unavailable.
wheel_basename="$(basename "$wheel")"
if [[ "$wheel_basename" =~ manylinux_([0-9]+)_([0-9]+)_ ]]; then
  floor_major="${BASH_REMATCH[1]}"
  floor_minor="${BASH_REMATCH[2]}"
  if ! command -v readelf >/dev/null 2>&1; then
    echo "readelf not found; skipping glibc floor check on manylinux wheel" >&2
  else
    echo "checking bundled binaries honour GLIBC_<=${floor_major}.${floor_minor} floor"
    tmpdir="$(mktemp -d)"
    trap 'rm -rf "$tmpdir"' EXIT
    python3 -m zipfile -e "$wheel" "$tmpdir" >/dev/null
    violations=()
    for bin in "$tmpdir"/moraine_cli/_binaries/*; do
      [[ -f "$bin" ]] || continue
      # readelf -V lists versioned symbol needs; extract the highest
      # GLIBC_X.Y entry for this binary.
      max="$(readelf -V "$bin" 2>/dev/null \
        | grep -oE 'GLIBC_[0-9]+\.[0-9]+' \
        | sort -u \
        | awk -F'[_.]' '{print $2"."$3}' \
        | sort -t. -k1,1n -k2,2n \
        | tail -1)"
      if [[ -z "$max" ]]; then
        continue  # static or no glibc deps
      fi
      bin_major="${max%%.*}"
      bin_minor="${max##*.}"
      if (( bin_major > floor_major )) \
         || (( bin_major == floor_major && bin_minor > floor_minor )); then
        violations+=("$(basename "$bin"): requires GLIBC_${max}, wheel tag allows <= ${floor_major}.${floor_minor}")
      fi
    done
    if (( ${#violations[@]} > 0 )); then
      echo "wheel's platform tag is a lie — bundled binaries need a newer glibc than the tag advertises:" >&2
      printf '  %s\n' "${violations[@]}" >&2
      echo "(see issue #246; fix by wrapping cargo in a manylinux build container)" >&2
      exit 1
    fi
  fi
fi

echo "wheel layout OK: $wheel"
