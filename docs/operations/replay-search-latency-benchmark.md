# Replay Search Latency Benchmark

## Purpose

Use `scripts/bench/replay_search_latency.py` to replay real, high-latency search requests from telemetry and measure current lookup latency against that baseline.

The benchmark script is a self-declared `uv` script and replays through the local maturin-built `moraine-conversations` Python package (`ConversationClient.search_events_json`) so the run exercises the in-repo search implementation directly.

The script:

- selects top `N` rows from `moraine.search_query_log` by `response_ms` in a time window,
- runs `maturin develop` for `bindings/python/moraine_conversations` (unless skipped),
- replays each query through `moraine_conversations.ConversationClient.search_events_json`,
- runs warmup and measured repeats per query,
- reports per-query and aggregate latency stats,
- records timeout/error counts,
- optionally writes a JSON artifact.

## Example Usage

Default benchmark (top 20 in last 24h):

```bash
uv run --script scripts/bench/replay_search_latency.py \
  --config config/moraine.toml
```

Custom window/sample and JSON output:

```bash
uv run --script scripts/bench/replay_search_latency.py \
  --config config/moraine.toml \
  --window 7d \
  --top-n 40 \
  --warmup 1 \
  --repeats 5 \
  --timeout-seconds 20 \
  --output-json /tmp/replay-latency.json
```

Inspect selected workload without replay:

```bash
uv run --script scripts/bench/replay_search_latency.py \
  --config config/moraine.toml \
  --window 24h \
  --top-n 20 \
  --dry-run
```

## CLI Flags

- `--config <path>`: Moraine config file used for ClickHouse connectivity.
- `--window <duration>`: telemetry lookback window. Supported suffixes: `s`, `m`, `h`, `d`, `w`.
- `--top-n <int>`: number of highest-latency rows to select.
- `--warmup <int>`: warmup runs per selected query.
- `--repeats <int>`: measured runs per selected query.
- `--timeout-seconds <int>`: timeout for each replayed search request.
- `--skip-maturin-develop`: skip local binding rebuild before replay.
- `--output-json <path>`: write machine-readable benchmark output.
- `--print-sql`: print generated ClickHouse selection SQL.
- `--dry-run`: select and validate rows, but skip replay.

Defaults:

- `window=24h`
- `top_n=20`
- `warmup=1`
- `repeats=5`
- `timeout_seconds=20`

## Output

Console output includes:

- selection metadata and selected time range,
- per-query table with baseline vs replay (`p50`, `p95`, `min`, `max`, `delta_p50`),
- aggregate summary (`min`, `p50`, `p95`, `p99`, `max`, `avg`),
- timeout/error totals.

JSON output includes:

- `meta` (timestamp, git SHA, config path, parameters),
- `selected_queries` (baseline rows + replay eligibility),
- `replay_results` (samples, per-query stats, failures),
- `aggregate` (overall stats + counts),
- `failures` (timeout/error totals).

Exit code behavior:

- `0`: all replay attempts succeeded.
- non-zero: fatal setup error, empty selection window, or replay timeout/error failures.

## Troubleshooting

- No rows selected:
  - increase `--window` (for example `7d`),
  - verify recent `search_query_log` writes are present.
- Local binding build/import failure:
  - ensure `uv`, Python, Rust/Cargo, and a C toolchain are available,
  - run `uv run --script scripts/bench/replay_search_latency.py --config ...` so `maturin` is auto-installed,
  - if iterating rapidly and the package is already built in the environment, use `--skip-maturin-develop`.
- Replay timeouts:
  - raise `--timeout-seconds`,
  - reduce `--top-n`/`--repeats` for quicker local checks,
  - inspect ClickHouse logs for transient slowness.
- Invalid selected rows:
  - run `--dry-run` to inspect skip reasons,
  - confirm telemetry fields (`result_limit`, `min_should_match`, flags) are valid.
