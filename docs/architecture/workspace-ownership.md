# Workspace Ownership and Boundaries

## Workspace structure

Moraine is organized as a Rust workspace with explicit app/core boundaries.

- Apps (`apps/*`) are transport/runtime entrypoints.
- Core crates (`crates/*-core`) hold service domain logic.
- Shared infra crates (`moraine-config`, `moraine-clickhouse`) hold cross-cutting concerns.

Legacy source trees remain in-repo only as historical reference snapshots and are non-authoritative:

- `rust/ingestor` -> `apps/moraine-ingest` + `crates/moraine-ingest-core`
- `rust/codex-mcp` -> `apps/moraine-mcp` + `crates/moraine-mcp-core`
- `moraine-monitor/backend` -> `apps/moraine-monitor` + `crates/moraine-monitor-core`

## Ownership map

### `apps/moraine-ingest`

- Owns ingest process startup and CLI.
- Delegates ingest internals to `moraine-ingest-core`.

### `crates/moraine-ingest-core`

- Owns filesystem watch/debounce/reconcile dispatch.
- Owns sink flushing, checkpoints, and heartbeat writes.
- Owns normalization/model pipeline internals.

### `apps/moraine-monitor`

- Owns monitor CLI/runtime entrypoint.
- Delegates query/domain behavior to `moraine-monitor-core`.
- Serves static assets from `web/monitor/dist` (built from `web/monitor`).

### `crates/moraine-monitor-core`

- Owns monitor API/query behavior (`health`, `status`, `analytics`, `tables`, `web-searches`).
- Owns SQL safety helpers and DTO shaping.

### `apps/moraine-mcp`

- Owns MCP stdio runtime/CLI entrypoint.
- Delegates protocol/tool behavior to `moraine-mcp-core`.

### `crates/moraine-mcp-core`

- Owns MCP JSON-RPC handling, tool routing (`search`, `open`), SQL building, and formatting.

### `apps/moraine`

- Owns local runtime orchestration and ClickHouse lifecycle.
- Owns process supervision (pid/log management) for local services.
- Owns DB migration/doctor command surface.

### `crates/moraine-config`

- Owns canonical config schema (`clickhouse`, `ingest`, `mcp`, `monitor`, `runtime`).
- Owns config path resolution semantics.

### `crates/moraine-clickhouse`

- Owns shared ClickHouse HTTP client.
- Owns migration runner + migration ledger (`schema_migrations`).
- Owns doctor report contract.

## Boundary rules

1. App crates must stay thin and avoid embedding service domain SQL/format/business logic.
2. Core crates may depend on shared crates but not on app crates.
3. Shared crates must not depend on service-specific core crates.
4. Changes to shared config or ClickHouse APIs should be made once in shared crates and consumed by all services.
5. Do not add new runtime logic under `rust/*` or `moraine-monitor/backend`; treat those paths as legacy reference-only trees.
