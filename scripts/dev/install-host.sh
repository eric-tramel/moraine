#!/usr/bin/env bash
set -euo pipefail

# Build the current checkout for the host target and install it to the host,
# taking over wherever the active `moraine` resolves. This is the dev-facing,
# real-host sibling of scripts/ci/e2e-install-artifact.sh: it reuses the exact
# release path (scripts/package-moraine-release.sh -> scripts/install.sh) so a
# dev build exercises the same install mechanism end users get — not a parallel
# bespoke installer.
#
# Install mode is auto-selected from the active `moraine`:
#   * bin  — a real installed binary (the install.sh / curl flavor, e.g.
#            ~/.local/bin/moraine). We package a bundle and run install.sh with
#            a file:// asset URL + MORAINE_INSTALL_DIR set to that directory.
#   * uv   — a `uv tool install moraine-cli` shim (interpreter under
#            ~/.local/share/uv/tools/...). uv tool envs have no pip, so the
#            take-over is to rebuild the host wheel and `uv tool install --force`.
#   * pip  — a plain pip/venv `console_scripts` shim (moraine = moraine_cli:...)
#            whose env has pip. Rebuild the host wheel and
#            `pip install --force-reinstall` it into that environment.
#
# uv/pip both reuse scripts/build-python-wheels.py — the same wheel users get
# from PyPI, rebuilt from this checkout.

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PACKAGE_SCRIPT="$PROJECT_ROOT/scripts/package-moraine-release.sh"
INSTALL_SCRIPT="$PROJECT_ROOT/scripts/install.sh"
WHEEL_SCRIPT="$PROJECT_ROOT/scripts/build-python-wheels.py"
# Persisted (gitignored) output dir so --skip-build can reuse a prior bundle.
DIST_DIR="$PROJECT_ROOT/target/dev-install"

WITH_CLICKHOUSE=0
SKIP_BUILD=0
FORCE_DIR=""

usage() {
  cat <<'USAGE'
Build the current checkout for the host target and install it to the host,
taking over the active `moraine` on your PATH.

usage:
  scripts/dev/install-host.sh [options]
  make install [INSTALL_ARGS="..."]

options:
  --with-clickhouse   Also run the managed ClickHouse install (default: skip;
                      a working stack already has ClickHouse).
  --dir <path>        Force a `bin` install into <path> instead of detecting the
                      active install location.
  --skip-build        Reuse the bundle from a previous run in target/dev-install
                      instead of rebuilding (fast re-install).
  -h, --help          Show this help.

what it does:
  1. Builds a release bundle for the host target via
     scripts/package-moraine-release.sh (native; bun monitor build + cargo
     --release for the 4 binaries).
  2. Detects where `moraine` currently resolves and installs over it:
       - real binary   -> scripts/install.sh with a file:// asset URL
       - uv tool shim   -> rebuild host wheel + uv tool install --force
       - pip/venv shim  -> rebuild host wheel + pip install --force-reinstall
  3. Prints what was installed and how to revert / restart the stack.
USAGE
}

die() {
  echo "error: $*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1${2:+ ($2)}"
}

# Portable realpath for a possibly-symlinked file (BSD readlink has no -f).
resolve_path() {
  local p="$1" target
  while [ -L "$p" ]; do
    target="$(readlink "$p")"
    case "$target" in
      /*) p="$target" ;;
      *) p="$(cd "$(dirname "$p")" && pwd)/$target" ;;
    esac
  done
  printf '%s\n' "$p"
}

parse_args() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --with-clickhouse) WITH_CLICKHOUSE=1 ;;
      --skip-build) SKIP_BUILD=1 ;;
      --dir)
        shift || die "--dir requires a path"
        FORCE_DIR="${1:-}"
        [ -n "$FORCE_DIR" ] || die "--dir requires a path"
        ;;
      --dir=*) FORCE_DIR="${1#--dir=}" ;;
      -h|--help) usage; exit 0 ;;
      *) die "unknown argument: $1 (see --help)" ;;
    esac
    shift
  done
}

# Build with the rustup toolchain (which tracks CI's dtolnay/rust-toolchain@stable),
# not whatever bare `cargo` is first on PATH. On this repo's dev hosts that is
# often a Nix-provided cargo that can lag stable — and the release deps need a
# current rustc (e.g. libsqlite3-sys 0.38.1 uses cfg_select!, rustc >= 1.95).
# package-moraine-release.sh calls bare `cargo`, so we select the toolchain here
# rather than only in the Makefile, so a direct script invocation also works.
# Mirrors the Makefile's CARGO_ENV. Escape hatch: MORAINE_INSTALL_USE_RUSTUP=0.
setup_rust_toolchain() {
  [ "${MORAINE_INSTALL_USE_RUSTUP:-1}" = "1" ] || return 0
  command -v rustup >/dev/null 2>&1 || return 0
  local rustup_bin
  rustup_bin="$(dirname "$(command -v rustup)")"
  export PATH="$rustup_bin:$PATH"
  export RUSTUP_TOOLCHAIN="${RUSTUP_TOOLCHAIN:-${RUST_TOOLCHAIN:-stable}}"
  echo "[install-host] using rustup toolchain: $RUSTUP_TOOLCHAIN ($(cargo --version 2>/dev/null))" >&2
}

host_triple() {
  rustc -vV 2>/dev/null | sed -n 's/^host: //p'
}

version_label() {
  local sha dirty=""
  sha="$(git -C "$PROJECT_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
  if [ -n "$(git -C "$PROJECT_ROOT" status --porcelain --untracked-files=no 2>/dev/null)" ]; then
    dirty="-dirty"
  fi
  printf 'dev-%s%s\n' "$sha" "$dirty"
}

crate_version() {
  # First `version = "x.y.z"` in the moraine app crate manifest.
  sed -n 's/^version[[:space:]]*=[[:space:]]*"\(.*\)".*/\1/p' \
    "$PROJECT_ROOT/apps/moraine/Cargo.toml" | head -n1
}

# Echoes "<mode> <target>" where target is the install dir (bin) or env python (pip).
detect_target() {
  if [ -n "$FORCE_DIR" ]; then
    printf 'bin %s\n' "$FORCE_DIR"
    return
  fi

  local active
  active="$(command -v moraine 2>/dev/null || true)"
  if [ -z "$active" ]; then
    printf 'bin %s\n' "$HOME/.local/bin"
    return
  fi

  active="$(resolve_path "$active")"

  local ftype
  ftype="$(file -b "$active" 2>/dev/null || echo unknown)"
  case "$ftype" in
    *Mach-O*|*ELF*)
      printf 'bin %s\n' "$(cd "$(dirname "$active")" && pwd)"
      ;;
    *[Pp]ython*)
      # console_scripts shim — derive the env interpreter from its shebang.
      local shebang py
      shebang="$(sed -n '1s/^#![[:space:]]*//p' "$active")"
      py="${shebang%% *}"
      if [ -z "$py" ]; then
        py="$(dirname "$active")/python3"
      fi
      case "$py" in
        */uv/tools/*)
          # uv tool env (no pip) — take over with `uv tool install --force`.
          printf 'uv %s\n' "$py"
          ;;
        *)
          [ -x "$py" ] || die "detected a pip shim at $active but no env python ($py); pass --dir to force a bin install"
          printf 'pip %s\n' "$py"
          ;;
      esac
      ;;
    *script*)
      # A shell wrapper (e.g. the in-repo bin/moraine dev wrapper). Don't
      # clobber it; fall back to a fresh ~/.local/bin install.
      echo "note: active moraine ($active) is a source-tree wrapper; installing to ~/.local/bin (it may stay shadowed on PATH)." >&2
      printf 'bin %s\n' "$HOME/.local/bin"
      ;;
    *)
      printf 'bin %s\n' "$(cd "$(dirname "$active")" && pwd)"
      ;;
  esac
}

build_bundle() {
  local triple="$1" label="$2" bundle
  bundle="$DIST_DIR/moraine-bundle-$triple.tar.gz"

  if [ "$SKIP_BUILD" = "1" ]; then
    [ -f "$bundle" ] || die "--skip-build set but no bundle at $bundle; run once without --skip-build first"
    echo "[install-host] reusing bundle: $bundle" >&2
    return
  fi

  mkdir -p "$DIST_DIR"
  echo "[install-host] building release bundle for $triple (label $label)" >&2
  # GITHUB_REF_NAME stamps the bundle manifest's version field with our label.
  GITHUB_REF_NAME="$label" "$PACKAGE_SCRIPT" "$triple" "$DIST_DIR" >&2
}

install_bin() {
  local triple="$1" label="$2" target_dir="$3" skip_ch
  skip_ch=1
  [ "$WITH_CLICKHOUSE" = "1" ] && skip_ch=0

  mkdir -p "$target_dir" 2>/dev/null || die "install dir is not writable: $target_dir (try --dir <path>)"

  echo "[install-host] installing to $target_dir via scripts/install.sh (clickhouse skip=$skip_ch)" >&2
  MORAINE_INSTALL_ASSET_BASE_URL="file://$DIST_DIR" \
    MORAINE_INSTALL_VERSION="$label" \
    MORAINE_INSTALL_DIR="$target_dir" \
    MORAINE_INSTALL_SKIP_CLICKHOUSE="$skip_ch" \
    sh "$INSTALL_SCRIPT"
}

# Builds the host wheel from the current bundle; echoes the wheel path on stdout.
build_host_wheel() {
  local triple="$1"
  local bundle wheels pep wheel
  bundle="$DIST_DIR/moraine-bundle-$triple.tar.gz"
  wheels="$DIST_DIR/wheels"
  # Wheel filenames can't carry the `dev-<sha>` hyphen; use a PEP 440 dev
  # version derived from the crate version. The git sha is in the report.
  pep="$(crate_version).dev0"

  require_cmd python3
  echo "[install-host] building host wheel ($pep)" >&2
  rm -rf "$wheels"
  python3 "$WHEEL_SCRIPT" --target "$triple" --bundle "$bundle" --out-dir "$wheels" --version "$pep" >&2
  wheel="$(ls "$wheels"/*.whl 2>/dev/null | head -n1)"
  [ -n "$wheel" ] || die "wheel build produced no .whl in $wheels"
  printf '%s\n' "$wheel"
}

report_moraine() {
  echo "  moraine: $(command -v moraine 2>/dev/null || echo '?')"
  "$(command -v moraine 2>/dev/null || true)" --version 2>/dev/null || true
}

install_uv() {
  local triple="$1" label="$2" wheel
  require_cmd uv
  wheel="$(build_host_wheel "$triple")"
  echo "[install-host] reinstalling uv tool from $wheel" >&2
  uv tool install --force "$wheel" >&2

  echo
  echo "installed dev build as uv tool: $label ($triple)"
  echo "  wheel:  $wheel"
  report_moraine
  echo "  revert: uv tool install --force moraine-cli"
}

install_pip() {
  local triple="$1" label="$2" env_python="$3" wheel
  wheel="$(build_host_wheel "$triple")"
  echo "[install-host] force-reinstalling into $env_python" >&2
  "$env_python" -m pip install --force-reinstall --no-deps "$wheel" >&2

  echo
  echo "installed dev build into pip env: $label ($triple)"
  echo "  python: $env_python"
  echo "  wheel:  $wheel"
  report_moraine
  echo "  revert: pip install --force-reinstall moraine-cli"
}

main() {
  parse_args "$@"

  setup_rust_toolchain
  require_cmd rustc "install the Rust toolchain"
  require_cmd cargo "install the Rust toolchain"
  require_cmd git
  [ "$SKIP_BUILD" = "1" ] || require_cmd bun "needed to build the monitor frontend"

  local triple label
  triple="$(host_triple)"
  [ -n "$triple" ] || die "could not determine host target triple from rustc -vV"
  label="$(version_label)"

  local detected mode target
  detected="$(detect_target)"
  mode="${detected%% *}"
  target="${detected#* }"

  build_bundle "$triple" "$label"

  case "$mode" in
    bin)
      install_bin "$triple" "$label" "$target"
      echo
      echo "installed dev build: $label ($triple)"
      echo "  moraine: $(command -v moraine 2>/dev/null || echo "$target/moraine")"
      "$(command -v moraine 2>/dev/null || echo "$target/moraine")" --version 2>/dev/null || true
      ;;
    uv)
      install_uv "$triple" "$label"
      ;;
    pip)
      install_pip "$triple" "$label" "$target"
      ;;
    *)
      die "internal: unknown install mode '$mode'"
      ;;
  esac

  echo
  echo "if the stack is running, restart it to pick up the new binaries:"
  echo "  moraine down && moraine up && moraine status"
}

main "$@"
