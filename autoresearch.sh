#!/usr/bin/env bash
set -euo pipefail

sandbox_id="${MORAINE_AUTORESEARCH_SANDBOX_ID:-sb-a0735e}"
container="moraine-sandbox-${sandbox_id}"
results_dir="/tmp/autoresearch-concurrent-results"
oracle="/tmp/autoresearch-concurrent-oracle.json"

if [[ "$(docker inspect --format '{{.State.Running}}' "$container" 2>/dev/null || true)" != "true" ]]; then
  scripts/dev/sandbox/moraine-sandbox up --id "$sandbox_id" --quiet >/dev/null
fi

docker exec -w /repo "$container" \
  cargo build -p moraine-mcp --release --locked --quiet

docker exec "$container" \
  python3 /repo/scripts/bench/seed_concurrent_mcp_benchmark.py seed \
  --clickhouse-url http://clickhouse:8123 \
  --database moraine \
  --documents 100000 \
  --measured-cases 64 \
  --recovery-cases 2 \
  --oracle-json "$oracle"

docker exec "$container" rm -rf "$results_dir"
docker exec -w /repo "$container" \
  python3 scripts/bench/concurrent_mcp_retrieval.py \
  --moraine-mcp /home/moraine/target/release/moraine-mcp \
  --clickhouse-url http://clickhouse:8123 \
  --database moraine \
  --oracle-json "$oracle" \
  --concurrency 8 \
  --concurrency 16 \
  --reps 5 \
  --profile full \
  --startup-timeout-seconds 10 \
  --request-timeout-seconds 20 \
  --run-timeout-seconds 600 \
  --max-processes 64 \
  --output-dir "$results_dir"

docker exec -w /repo "$container" \
  python3 scripts/bench/autoresearch_retrieval_metrics.py \
  "$results_dir/concurrent-mcp-retrieval-n8-full.json" \
  "$results_dir/concurrent-mcp-retrieval-n16-full.json"
