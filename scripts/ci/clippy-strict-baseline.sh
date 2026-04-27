#!/usr/bin/env bash
set -euo pipefail

# Enforce strict clippy while keeping current repo violations as an explicit baseline.
# Keep this list minimal and remove entries as code is cleaned up.
CLIPPY_BASELINE_FLAGS=(
  -Aclippy::collapsible_if
  -Aclippy::field_reassign_with_default
  -Aclippy::if_same_then_else
  -Aclippy::too_many_arguments
)

if [[ -n "${MORAINE_CARGO:-}" ]]; then
  # shellcheck disable=SC2206
  CARGO_CMD=(${MORAINE_CARGO})
elif [[ -n "${CARGO_CMD:-}" ]]; then
  # shellcheck disable=SC2206
  CARGO_CMD=(${CARGO_CMD})
elif command -v rustup >/dev/null 2>&1; then
  rustup_bin="$(dirname "$(command -v rustup)")"
  export PATH="${rustup_bin}:${PATH}"
  export RUSTUP_TOOLCHAIN="${RUST_TOOLCHAIN:-stable}"
  CARGO_CMD=(cargo)
else
  CARGO_CMD=(cargo)
fi

echo "[clippy] using: ${CARGO_CMD[*]}"
"${CARGO_CMD[@]}" clippy --workspace --all-targets --locked -- -Dwarnings "${CLIPPY_BASELINE_FLAGS[@]}"
