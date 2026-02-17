# Build and Operations

## Scope

This runbook describes local single-machine operation using the Rust workspace and `moraine` as the primary lifecycle interface.

## Build

```bash
cd ~/src/moraine
cargo build --workspace
```

This produces binaries for:

- `moraine`
- `moraine-ingest`
- `moraine-monitor`
- `moraine-mcp`

## Install Runtime Binaries From Source

### Cargo install

```bash
git clone https://github.com/eric-tramel/moraine.git ~/src/moraine
cd ~/src/moraine
for crate in moraine moraine-ingest moraine-monitor moraine-mcp; do
  cargo install --path "apps/$crate" --locked
done
```

Or install directly from GitHub without cloning:

```bash
for bin in moraine moraine-ingest moraine-monitor moraine-mcp; do
  cargo install --git https://github.com/eric-tramel/moraine.git \
    --package "$bin" \
    --bin "$bin" \
    --locked
done
```

### Prebuilt release binary

```bash
curl -fsSL https://raw.githubusercontent.com/eric-tramel/moraine/main/scripts/install-moraine.sh \
  | bash
export PATH="$HOME/.local/bin:$PATH"
```

Default binary symlink location is `~/.local/bin`. Versioned bundles are installed under `~/.local/lib/moraine/<tag>/<target>` with `~/.local/lib/moraine/current` as active symlink.

The installer fetches a full bundle (`moraine`, `moraine-ingest`, `moraine-monitor`, `moraine-mcp`) and installs managed ClickHouse automatically by default.

Install paths:

- Bundle root: `~/.local/lib/moraine/<tag>/<target>`
- Active symlink: `~/.local/lib/moraine/current`
- Command symlinks: `~/.local/bin`
- Managed ClickHouse root: `~/.local/lib/moraine/clickhouse/current`

## Publish prebuilt binaries

Tag-driven GitHub Actions release workflow:

1. Push a semantic tag (example: `v0.1.1`).
2. Workflow `.github/workflows/release-moraine.yml` builds:
   - `x86_64-unknown-linux-gnu`
   - `aarch64-unknown-linux-gnu`
   - `aarch64-apple-darwin`
3. Uploads `moraine-bundle-<target>.tar.gz` plus `moraine-bundle-<target>.sha256` to the tag release.

Each bundle includes `manifest.json` with target/version metadata, per-binary checksums, and build metadata.

Multiplatform functional CI (`.github/workflows/ci-functional.yml`) also packages per-target bundles and validates `scripts/install-moraine.sh` by installing from a local artifact server before running the stack + MCP smoke test.

## Config model

Use one shared config schema at `config/moraine.toml`.

Resolution precedence:

1. `--config <path>`
2. env override (`MORAINE_CONFIG`, plus `MORAINE_MCP_CONFIG` for MCP)
3. `~/.moraine/config.toml` (if present)
4. repo default `config/moraine.toml`

## Start stack

```bash
cd ~/src/moraine
bin/moraine up
bin/moraine up --output rich
```

`moraine up` does the following:

1. Starts ClickHouse process.
2. Waits for DB health.
3. Applies versioned migrations through `moraine-clickhouse` (`schema_migrations` ledger).
4. Starts ingest and optional services from `runtime` config.

It auto-installs managed ClickHouse when missing and `runtime.clickhouse_auto_install=true`.

`moraine clickhouse status` reports managed install state, active binary source (`managed` vs `PATH`), installed version, and checksum state.

## DB lifecycle

```bash
cd ~/src/moraine
bin/moraine db migrate
bin/moraine db doctor
bin/moraine db doctor --output json
```

`db doctor` checks:

- ClickHouse health/version.
- Database existence.
- Applied vs pending migrations.
- Required table presence.

## Service entrypoints

```bash
cd ~/src/moraine
bin/moraine run ingest
bin/moraine run monitor
bin/moraine run mcp
bin/moraine run clickhouse
```

## Replay latency benchmark

Use the replay benchmark to measure current MCP `search` latency against recent worst-case telemetry:

```bash
python3 scripts/bench/replay_search_latency.py --config config/moraine.toml
```

For workload inspection only, run with `--dry-run`.
Detailed options, output fields, and troubleshooting are in `operations/replay-search-latency-benchmark.md`.

## Status, logs, shutdown

```bash
cd ~/src/moraine
bin/moraine status
bin/moraine status --output rich --verbose
bin/moraine status --output json
bin/moraine logs
bin/moraine logs ingest --lines 200 --output plain
bin/moraine down
```

Status includes process state, DB health/schema checks, and latest ingest heartbeat metrics.

All subcommands support output control:

- `--output auto|rich|plain|json` (default `auto`, rich on TTY).
- `--verbose` for expanded diagnostics in rich/plain output.

## Legacy scripts

Legacy lifecycle aliases remain as fail-fast migration stubs with a `moraine` replacement hint:

- `bin/start-clickhouse`
- `bin/init-db`
- `bin/status`
- `bin/stop-all`

Legacy wrappers remain only as fail-fast stubs:

- `bin/start-ingestor` -> `bin/moraine up`
- `bin/run-codex-mcp` -> `bin/moraine run mcp`
- `bin/moraine-monitor` -> `bin/moraine run monitor`

## Failure triage

1. If `up` fails before migration, run `bin/moraine db doctor` and inspect ClickHouse logs via `bin/moraine logs clickhouse`.
2. If ingest stalls, run `bin/moraine status` and confirm heartbeat recency/queue depth.
3. If monitor APIs degrade, run `bin/moraine run monitor` in foreground for direct error output.
4. If MCP retrieval degrades, verify `search_*` tables in doctor output and rerun `db migrate`.
