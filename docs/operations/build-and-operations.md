# Build and Operations

## Scope

This runbook describes local single-machine operation using the Rust workspace and `cortexctl` as the primary lifecycle interface.

## Build

```bash
cd ~/src/cortex
cargo build --workspace
```

This produces binaries for:

- `cortexctl`
- `cortex-ingest`
- `cortex-monitor`
- `cortex-mcp`

## Install `cortexctl`

### Cargo install

```bash
git clone https://github.com/eric-tramel/cortex.git ~/src/cortex
cd ~/src/cortex
cargo install --path apps/cortexctl --locked
```

Or install directly from GitHub without cloning:

```bash
cargo install --git https://github.com/eric-tramel/cortex.git \
  --package cortexctl \
  --bin cortexctl \
  --locked
```

### Prebuilt release binary

```bash
curl -fsSL https://raw.githubusercontent.com/eric-tramel/cortex/main/scripts/install-cortexctl.sh \
  | bash -s -- --repo eric-tramel/cortex
```

Default binary symlink location is `~/.local/bin`. Versioned bundles are installed under `~/.local/lib/cortex/<tag>/<target>` with `~/.local/lib/cortex/current` as active symlink.

The installer fetches a full bundle (`cortexctl`, `cortex-ingest`, `cortex-monitor`, `cortex-mcp`) and installs managed ClickHouse automatically by default.

## Publish prebuilt binaries

Tag-driven GitHub Actions release workflow:

1. Push a semantic tag (example: `v0.1.0`).
2. Workflow `.github/workflows/release-cortexctl.yml` builds:
   - `x86_64-unknown-linux-gnu`
   - `aarch64-unknown-linux-gnu`
   - `x86_64-apple-darwin`
   - `aarch64-apple-darwin`
3. Uploads `cortex-bundle-<target>.tar.gz` plus `cortex-bundle-<target>.sha256` to the tag release.

## Config model

Use one shared config schema at `config/cortex.toml`.

Resolution precedence:

1. `--config <path>`
2. env override (`CORTEX_CONFIG`, plus `CORTEX_MCP_CONFIG` for MCP)
3. `~/.cortex/config.toml` (if present)
4. repo default `config/cortex.toml`

## Start stack

```bash
cd ~/src/cortex
bin/cortexctl up
```

`cortexctl up` does the following:

1. Starts ClickHouse process.
2. Waits for DB health.
3. Applies versioned migrations through `cortex-clickhouse` (`schema_migrations` ledger).
4. Starts ingest and optional services from `runtime` config.

It auto-installs managed ClickHouse when missing and `runtime.clickhouse_auto_install=true`.

## DB lifecycle

```bash
cd ~/src/cortex
bin/cortexctl db migrate
bin/cortexctl db doctor
```

`db doctor` checks:

- ClickHouse health/version.
- Database existence.
- Applied vs pending migrations.
- Required table presence.

## Service entrypoints

```bash
cd ~/src/cortex
bin/cortexctl run ingest
bin/cortexctl run monitor --host 127.0.0.1 --port 8090
bin/cortexctl run mcp
bin/cortexctl run clickhouse
```

## Boot services

```bash
cd ~/src/cortex
bin/cortexctl service install
bin/cortexctl service status
bin/cortexctl service uninstall
```

- macOS: installs user LaunchAgents under `~/Library/LaunchAgents`.
- Linux: installs user systemd units under `~/.config/systemd/user`.

## Status, logs, shutdown

```bash
cd ~/src/cortex
bin/cortexctl status
bin/cortexctl logs
bin/cortexctl logs ingest --lines 200
bin/cortexctl down
```

Status includes process state, DB health/schema checks, and latest ingest heartbeat metrics.

## Legacy scripts

Legacy shell entrypoints are removed and now fail fast with a `cortexctl` replacement hint:

- `bin/start-clickhouse`
- `bin/init-db`
- `bin/start-ingestor`
- `bin/status`
- `bin/stop-all`
- `bin/run-codex-mcp`
- `bin/cortex-monitor`

## Failure triage

1. If `up` fails before migration, run `bin/cortexctl db doctor` and inspect ClickHouse logs via `bin/cortexctl logs clickhouse`.
2. If ingest stalls, run `bin/cortexctl status` and confirm heartbeat recency/queue depth.
3. If monitor APIs degrade, run `bin/cortexctl run monitor` in foreground for direct error output.
4. If MCP retrieval degrades, verify `search_*` tables in doctor output and rerun `db migrate`.
