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

## Install Runtime Binaries From Source

### Cargo install

```bash
git clone https://github.com/eric-tramel/cortex.git ~/src/cortex
cd ~/src/cortex
for crate in cortexctl cortex-ingest cortex-monitor cortex-mcp; do
  cargo install --path "apps/$crate" --locked
done
```

Or install directly from GitHub without cloning:

```bash
for bin in cortexctl cortex-ingest cortex-monitor cortex-mcp; do
  cargo install --git https://github.com/eric-tramel/cortex.git \
    --package "$bin" \
    --bin "$bin" \
    --locked
done
```

### Prebuilt release binary

```bash
curl -fsSL https://raw.githubusercontent.com/eric-tramel/cortex/main/scripts/install-cortexctl.sh \
  | bash
export PATH="$HOME/.local/bin:$PATH"
```

Default binary symlink location is `~/.local/bin`. Versioned bundles are installed under `~/.local/lib/cortex/<tag>/<target>` with `~/.local/lib/cortex/current` as active symlink.

The installer fetches a full bundle (`cortexctl`, `cortex-ingest`, `cortex-monitor`, `cortex-mcp`) and installs managed ClickHouse automatically by default.

Install paths:

- Bundle root: `~/.local/lib/cortex/<tag>/<target>`
- Active symlink: `~/.local/lib/cortex/current`
- Command symlinks: `~/.local/bin`
- Managed ClickHouse root: `~/.local/lib/cortex/clickhouse/current`

## Publish prebuilt binaries

Tag-driven GitHub Actions release workflow:

1. Push a semantic tag (example: `v0.1.1`).
2. Workflow `.github/workflows/release-cortexctl.yml` builds:
   - `x86_64-unknown-linux-gnu`
   - `aarch64-unknown-linux-gnu`
   - `aarch64-apple-darwin`
3. Uploads `cortex-bundle-<target>.tar.gz` plus `cortex-bundle-<target>.sha256` to the tag release.

Each bundle includes `manifest.json` with target/version metadata, per-binary checksums, and build metadata.

Multiplatform functional CI (`.github/workflows/ci-functional.yml`) also packages per-target bundles and validates `scripts/install-cortexctl.sh` by installing from a local artifact server before running the stack + MCP smoke test.

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
bin/cortexctl up --output rich
```

`cortexctl up` does the following:

1. Starts ClickHouse process.
2. Waits for DB health.
3. Applies versioned migrations through `cortex-clickhouse` (`schema_migrations` ledger).
4. Starts ingest and optional services from `runtime` config.

It auto-installs managed ClickHouse when missing and `runtime.clickhouse_auto_install=true`.

`cortexctl clickhouse status` reports managed install state, active binary source (`managed` vs `PATH`), installed version, and checksum state.

## DB lifecycle

```bash
cd ~/src/cortex
bin/cortexctl db migrate
bin/cortexctl db doctor
bin/cortexctl db doctor --output json
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
bin/cortexctl run monitor
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
- Linux linger is checked during install. If linger is unavailable, Cortex prints the exact remediation command (`sudo loginctl enable-linger <user>`).

## Status, logs, shutdown

```bash
cd ~/src/cortex
bin/cortexctl status
bin/cortexctl status --output rich --verbose
bin/cortexctl status --output json
bin/cortexctl logs
bin/cortexctl logs ingest --lines 200 --output plain
bin/cortexctl down
```

Status includes process state, DB health/schema checks, and latest ingest heartbeat metrics.

All subcommands support output control:

- `--output auto|rich|plain|json` (default `auto`, rich on TTY).
- `--verbose` for expanded diagnostics in rich/plain output.

## Legacy scripts

Legacy lifecycle aliases remain as fail-fast migration stubs with a `cortexctl` replacement hint:

- `bin/start-clickhouse`
- `bin/init-db`
- `bin/status`
- `bin/stop-all`

Legacy service wrappers are retired (no longer shipped):

- `bin/start-ingestor` -> `bin/cortexctl up`
- `bin/run-codex-mcp` -> `bin/cortexctl run mcp`
- `bin/cortex-monitor` -> `bin/cortexctl run monitor`

## Failure triage

1. If `up` fails before migration, run `bin/cortexctl db doctor` and inspect ClickHouse logs via `bin/cortexctl logs clickhouse`.
2. If ingest stalls, run `bin/cortexctl status` and confirm heartbeat recency/queue depth.
3. If monitor APIs degrade, run `bin/cortexctl run monitor` in foreground for direct error output.
4. If MCP retrieval degrades, verify `search_*` tables in doctor output and rerun `db migrate`.
