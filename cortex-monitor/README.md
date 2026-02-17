# Cortex Monitor

Local UI to inspect the `cortex` ClickHouse database, now backed by a Rust server.

Note: `cortex-monitor/backend` is a legacy reference-only tree. Authoritative monitor runtime code is in `apps/cortex-monitor` and `crates/cortex-monitor-core`.

## Run

```bash
cd /Users/eric/src/cortex
bin/cortex-monitor --host 127.0.0.1 --port 8080
```

Open:

```txt
http://127.0.0.1:8080
```

Optional flags:

- `--host` (default: `127.0.0.1`)
- `--port` (default: `8080`)
- `--config` path to `cortex.toml`-style config
- `--static-dir` path to web assets (defaults to `web/monitor/dist`)

Environment helpers:

- `CORTEX_CONFIG` to point at a config file (overridden by `--config`)
- `CORTEX_MONITOR_CARGO` to override the `cargo` binary

## Frontend build

The monitor UI source lives under `web/monitor` and is built with Vite.
Release bundles include prebuilt web assets, so running installed binaries does not require Bun.
The `cortex-monitor/web` directory has been removed.

```bash
cd /Users/eric/src/cortex/web/monitor
bun install
bun run build
```

Serve custom assets by passing `--static-dir` if needed.

## API endpoints

- `GET /api/health` – ClickHouse ping/version
- `GET /api/status` – database and ingestor status summary
- `GET /api/analytics?range=24h` – model analytics (token usage and turns by time bucket)
- `GET /api/tables` – table list and estimated row counts
- `GET /api/tables/:name?limit=25` – schema and sample rows

Supported analytics ranges:

- `15m`
- `1h`
- `6h`
- `24h`
- `7d`
- `30d`
