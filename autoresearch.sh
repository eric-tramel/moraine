#!/usr/bin/env bash
set -euo pipefail

export RUST_BACKTRACE=0
export RUST_TEST_THREADS=1

cargo test \
  -p moraine-conversations \
  --lib \
  --release \
  --locked \
  clickhouse_repo::tests::autoresearch_retrieval_benchmark \
  -- \
  --exact \
  --ignored \
  --nocapture \
  --test-threads=1
