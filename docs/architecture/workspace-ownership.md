# Workspace Ownership and Boundaries

## Workspace structure

Cortex is organized as a Rust workspace with explicit app/core boundaries.

- Apps (`apps/*`) are transport/runtime entrypoints.
- Core crates (`crates/*-core`) hold service domain logic.
- Shared infra crates (`cortex-config`, `cortex-clickhouse`) hold cross-cutting concerns.

## Ownership map

### `apps/cortex-ingest`

- Owns ingest process startup and CLI.
- Delegates ingest internals to `cortex-ingest-core`.

### `crates/cortex-ingest-core`

- Owns filesystem watch/debounce/reconcile dispatch.
- Owns sink flushing, checkpoints, and heartbeat writes.
- Owns normalization/model pipeline internals.

### `apps/cortex-monitor`

- Owns monitor CLI/runtime entrypoint.
- Delegates query/domain behavior to `cortex-monitor-core`.
- Serves static assets from `web/monitor/dist` (built from `web/monitor`).

### `crates/cortex-monitor-core`

- Owns monitor API/query behavior (`health`, `status`, `analytics`, `tables`, `web-searches`).
- Owns SQL safety helpers and DTO shaping.

### `apps/cortex-mcp`

- Owns MCP stdio runtime/CLI entrypoint.
- Delegates protocol/tool behavior to `cortex-mcp-core`.

### `crates/cortex-mcp-core`

- Owns MCP JSON-RPC handling, tool routing (`search`, `open`), SQL building, and formatting.

### `apps/cortexctl`

- Owns local runtime orchestration and ClickHouse lifecycle.
- Owns process supervision (pid/log management) for local services.
- Owns DB migration/doctor command surface.

### `crates/cortex-config`

- Owns canonical config schema (`clickhouse`, `ingest`, `mcp`, `monitor`, `runtime`).
- Owns config path resolution semantics.

### `crates/cortex-clickhouse`

- Owns shared ClickHouse HTTP client.
- Owns migration runner + migration ledger (`schema_migrations`).
- Owns doctor report contract.

## Boundary rules

1. App crates must stay thin and avoid embedding service domain SQL/format/business logic.
2. Core crates may depend on shared crates but not on app crates.
3. Shared crates must not depend on service-specific core crates.
4. Changes to shared config or ClickHouse APIs should be made once in shared crates and consumed by all services.
