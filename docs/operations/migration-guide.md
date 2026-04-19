# Migration Guide: Legacy Scripts to `moraine`

This guide maps historical script commands to the `moraine` command contracts.

## Updating an existing install

`moraine update` has been removed. Updates are now handled by your package manager:

- `uv tool install moraine-cli` (recommended) — installs, and `uv tool upgrade moraine-cli` keeps it current. The PyPI distribution is named `moraine-cli`; the installed entrypoint is `moraine`.
- `cargo install --force --locked moraine` — for source installs.
- Re-run `scripts/install.sh` — for existing curl-based installs. The installer will overwrite the binaries in place.

Installs from prior versions leave a stale `${XDG_CONFIG_HOME:-~/.config}/moraine/install-receipt.json` file; it is no longer read or written and can be deleted at will.

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
