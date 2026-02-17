# Migration Guide: Legacy Scripts to `moraine`

This guide maps historical script commands to the `moraine` command contracts.

## Command mapping

- `bin/start-clickhouse` -> `bin/moraine up --no-ingest`
- `bin/init-db` -> `bin/moraine db migrate`
- `bin/status` -> `bin/moraine status`
- `bin/stop-all` -> `bin/moraine down`

Legacy lifecycle aliases (`start-clickhouse`, `init-db`, `status`, `stop-all`) remain as fail-fast migration stubs. Service wrapper scripts are retired; use `bin/moraine run ingest|monitor|mcp` directly.

## Runtime changes

1. Runtime supervision is now in Rust (`moraine`), not shell scripts.
2. ClickHouse schema application is versioned and tracked via `schema_migrations`.
3. One shared config schema is used for all services (`config/moraine.toml`).
4. `moraine run ingest|monitor|mcp` resolves installed binaries first; source-tree fallback is opt-in via `MORAINE_SOURCE_TREE_MODE=1`.

## Recommended workflow

```bash
cd ~/src/moraine
cargo build --workspace
bin/moraine up
bin/moraine status
```

For DB checks:

```bash
bin/moraine db migrate
bin/moraine db doctor
```
