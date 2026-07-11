# Moraine Monitor

The unified Rust backend serves the MCP socket, monitor HTTP API, and static UI together from one shared repository and cache set.

Note: `moraine-monitor/backend` is a legacy reference-only tree. Authoritative monitor runtime code is in `apps/moraine-monitor` and `crates/moraine-monitor-core`.

## Run

```bash
cd /Users/eric/src/moraine
bin/moraine run backend -- --host 127.0.0.1 --port 8080
```

The `moraine-monitor` executable is a deprecated compatibility alias for the
same unified backend process. It does not start a separate monitor service.

Open:

```txt
http://127.0.0.1:8080
```

Optional flags:

- `--host` (default: `127.0.0.1`)
- `--port` (default: `8080`)
- `--config` path to `moraine.toml`-style config
- `--static-dir` path to web assets (defaults to `web/monitor/dist`)

Environment helpers:

- `MORAINE_CONFIG` to point at a config file (overridden by `--config`)
- `MORAINE_SOURCE_TREE_MODE=1` to opt into source-tree binary fallback when running from a checkout

## Frontend build

The monitor UI source lives under `web/monitor` and is built with Vite.
Release bundles include prebuilt web assets, so running installed binaries does not require Bun.
The `moraine-monitor/web` directory has been removed.

```bash
cd /Users/eric/src/moraine/web/monitor
bun install
bun run build
```

Serve custom assets by passing `--static-dir` if needed.

## API endpoints

The canonical dashboard API is versioned under `/api/v1`:

- `GET /api/v1/capabilities` – server version, applied schema migration level, and feature flags
- `GET /api/v1/health` – ClickHouse ping/version
- `GET /api/v1/status` – database and ingestor status summary
- `GET /api/v1/analytics?range=24h` – model analytics (token usage and turns by time bucket)
- `GET /api/v1/tables` – table list and estimated row counts
- `GET /api/v1/tables/:name?limit=25` – schema and sample rows
- `GET /api/v1/web-searches?limit=100` – normalized web-search activity
- `GET /api/v1/sessions?since=30d&limit=50` – session analytics

The former `/api/*` dashboard paths remain direct aliases for one release.
New clients must use `/api/v1`. See
[`docs/monitor-http-api.md`](../docs/monitor-http-api.md) for the complete
contract, query bounds, compatibility matrix, and error behavior.

Supported analytics ranges:

- `15m`
- `1h`
- `6h`
- `24h`
- `7d`
- `30d`
