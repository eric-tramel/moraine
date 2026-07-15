#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$repo_root"

mode="${MORAINE_AUTORESEARCH_MODE:-inner}"
sandbox_id="${MORAINE_AUTORESEARCH_SANDBOX_ID:-sb-a0735e}"
container="moraine-sandbox-${sandbox_id}"

ensure_sandbox() {
  if [[ "$(docker inspect --format '{{.State.Running}}' "$container" 2>/dev/null || true)" != "true" ]]; then
    scripts/dev/sandbox/moraine-sandbox up --id "$sandbox_id" --quiet >/dev/null
  fi
}

case "$mode" in
  inner)
    ensure_sandbox
    docker exec -w /repo "$container" \
      cargo test -p moraine-conversations --lib --release --locked \
      clickhouse_repo::tests::autoresearch_retrieval_benchmark -- \
      --exact --ignored --nocapture
    ;;
  e2e)
    baseline="${MORAINE_AUTORESEARCH_BASELINE:?set MORAINE_AUTORESEARCH_BASELINE to a clean baseline worktree}"
    output="${MORAINE_AUTORESEARCH_OUTPUT:?set MORAINE_AUTORESEARCH_OUTPUT to a new result directory}"
    pairs="${MORAINE_AUTORESEARCH_PAIRS:-1}"
    python3 scripts/bench/performance_suite.py run \
      --mode local \
      --profile full \
      --pairs "$pairs" \
      --baseline "$baseline" \
      --candidate "$repo_root" \
      --output "$output"
    python3 scripts/bench/performance_suite.py autoresearch-metrics \
      "$output/local-comparison.json"
    ;;
  *)
    echo "unsupported MORAINE_AUTORESEARCH_MODE: $mode (expected inner or e2e)" >&2
    exit 2
    ;;
esac
