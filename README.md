# cortex

Cortex is a local Rust workspace for ingesting Codex/Claude session logs into ClickHouse, serving monitor APIs/UI, and exposing MCP retrieval tools.

## Quickstart

Install `cortexctl` (prebuilt binary, recommended):

```bash
curl -fsSL https://raw.githubusercontent.com/eric-tramel/cortex/main/scripts/install-cortexctl.sh \
  | bash -s -- --repo eric-tramel/cortex
```

Then start and check the local stack:

```bash
cortexctl up
cortexctl status
```

The installer sets up all Cortex binaries (`cortexctl`, `cortex-ingest`, `cortex-monitor`, `cortex-mcp`) and auto-installs a managed ClickHouse build by default.

Install layout:

- `~/.local/lib/cortex/<tag>/<target>/bin` (versioned bundle)
- `~/.local/lib/cortex/current` (active symlink)
- `~/.local/bin/cortexctl` (command symlink)
- `~/.local/lib/cortex/clickhouse/current` (managed ClickHouse)

Enable launch-on-boot services:

```bash
cortexctl service install
cortexctl service status
```

## Prerequisites

- Rust toolchain (`cargo`, `rustc`) if you want to build from source.

`clickhouse-server` on `PATH` is optional. `cortexctl up` auto-installs managed ClickHouse when `runtime.clickhouse_auto_install=true` (default).

## Platform support

| OS | Architectures | Notes |
| --- | --- | --- |
| macOS | `x86_64`, `aarch64` | user `launchd` services |
| Linux | `x86_64`, `aarch64` | user `systemd` services (linger recommended) |

## Alternative installs

Install from source with Cargo:

```bash
gh repo clone eric-tramel/cortex ~/src/cortex
cd ~/src/cortex
cargo install --path apps/cortexctl --locked
```

Install from GitHub with Cargo (no clone):

```bash
cargo install --git https://github.com/eric-tramel/cortex.git \
  --package cortexctl \
  --bin cortexctl \
  --locked
```

## Build

```bash
cd ~/src/cortex
cargo build --workspace
```

## Config

Single config schema: `config/cortex.toml`.

Config resolution order across binaries:

1. `--config <path>`
2. env override (`CORTEX_CONFIG`; MCP also checks `CORTEX_MCP_CONFIG` first)
3. `~/.cortex/config.toml` (if present)
4. repo default `config/cortex.toml`

## Runtime management (`cortexctl`)

Use `cortexctl` for local stack lifecycle.

```bash
cd ~/src/cortex

# Start ClickHouse, run migrations, and start configured services
bin/cortexctl up

# Status: process state + DB health + migration/schema checks + ingest heartbeat
bin/cortexctl status

# Managed ClickHouse lifecycle
bin/cortexctl clickhouse status
bin/cortexctl clickhouse install --version v25.12.5.44-stable
bin/cortexctl clickhouse install --force
bin/cortexctl clickhouse uninstall

# DB lifecycle
bin/cortexctl db migrate
bin/cortexctl db doctor

# Foreground service entrypoints
bin/cortexctl run ingest
bin/cortexctl run monitor --host 127.0.0.1 --port 8090
bin/cortexctl run mcp

# Logs and shutdown
bin/cortexctl logs
bin/cortexctl down

# Boot service management
bin/cortexctl service install
bin/cortexctl service status
bin/cortexctl service uninstall
```

Linux pre-login startup note: enable linger for your user if needed.

```bash
sudo loginctl enable-linger "$USER"
```

## Compatibility notes

Legacy script commands (`bin/start-clickhouse`, `bin/init-db`, `bin/start-ingestor`, `bin/status`, `bin/stop-all`, `bin/run-codex-mcp`, `bin/cortex-monitor`) are removed and now exit with migration guidance to `cortexctl`.

Source-tree fallback mode is opt-in. Set `CORTEX_SOURCE_TREE_MODE=1` to allow `cortexctl run ...` to resolve `target/debug/*` binaries and repo `web/monitor` assets.

## Docs

```bash
cd ~/src/cortex
make docs-qc
make docs-build
make docs-serve
```

Open `http://127.0.0.1:8000`.
