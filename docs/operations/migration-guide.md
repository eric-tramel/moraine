# Migration Guide: Legacy Scripts to `cortexctl`

This guide maps removed script commands to the new `cortexctl` command contracts.

## Command mapping

- `bin/start-clickhouse` -> `bin/cortexctl up --no-ingest`
- `bin/init-db` -> `bin/cortexctl db migrate`
- `bin/start-ingestor` -> `bin/cortexctl up`
- `bin/status` -> `bin/cortexctl status`
- `bin/stop-all` -> `bin/cortexctl down`
- `bin/run-codex-mcp` -> `bin/cortexctl run mcp`
- `bin/cortex-monitor` -> `bin/cortexctl run monitor`

Each removed script now exits with a migration message instead of invoking legacy behavior.

## Runtime changes

1. Runtime supervision is now in Rust (`cortexctl`), not shell scripts.
2. ClickHouse schema application is versioned and tracked via `schema_migrations`.
3. One shared config schema is used for all services (`config/cortex.toml`).
4. `cortexctl run ingest|monitor|mcp` resolves installed binaries first; source-tree fallback is opt-in via `CORTEX_SOURCE_TREE_MODE=1`.

## Recommended workflow

```bash
cd ~/src/cortex
cargo build --workspace
bin/cortexctl up
bin/cortexctl status
```

For DB checks:

```bash
bin/cortexctl db migrate
bin/cortexctl db doctor
```
