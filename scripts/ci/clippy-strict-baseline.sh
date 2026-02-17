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

cargo clippy --workspace --all-targets --locked -- -Dwarnings "${CLIPPY_BASELINE_FLAGS[@]}"
