# Migration Guide: Legacy Scripts to `cortexctl`

This guide maps historical script commands to the `cortexctl` command contracts.

## Command mapping

- `bin/start-clickhouse` -> `bin/cortexctl up --no-ingest`
- `bin/init-db` -> `bin/cortexctl db migrate`
- `bin/status` -> `bin/cortexctl status`
- `bin/stop-all` -> `bin/cortexctl down`
- `bin/start-ingestor` -> `bin/cortexctl up` (wrapper retired)
- `bin/run-codex-mcp` -> `bin/cortexctl run mcp` (wrapper retired)
- `bin/cortex-monitor` -> `bin/cortexctl run monitor` (wrapper retired)

Legacy lifecycle aliases (`start-clickhouse`, `init-db`, `status`, `stop-all`) remain as fail-fast migration stubs. Service wrappers (`start-ingestor`, `run-codex-mcp`, `cortex-monitor`) are retired to keep the command surface focused on `cortexctl`.

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
