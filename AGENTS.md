# Repository Guidelines

## Project Structure & Module Organization
This repository is a Rust workspace for local Cortex services and shared libraries.

- `apps/`: binary crates (`cortexctl`, `cortex-ingest`, `cortex-monitor`, `cortex-mcp`).
- `crates/`: shared libraries (config, ClickHouse client, ingest/monitor/MCP core logic).
- `sql/`: ordered schema and migration SQL (`001_...sql`, `002_...sql`, etc.).
- `config/`: default runtime/config templates (`cortex.toml`, ClickHouse XML).
- `scripts/`: CI helpers, packaging, docs tooling.
- `docs/`: MkDocs source; generated site output goes to `site/`.
- `web/monitor/`: static monitor UI assets served by monitor service.

Prefer adding new runtime logic under `apps/` + `crates/` (not legacy `rust/` or `cortex-monitor/` paths).

## Build, Test, and Development Commands
- `cargo build --workspace --locked`: build all workspace crates.
- `cargo test --workspace --locked`: run unit/integration tests across workspace.
- `cargo fmt --all -- --check`: enforce formatting (matches CI).
- `bash scripts/ci/e2e-stack.sh`: run functional stack + MCP smoke test.
- `bin/cortexctl up` / `bin/cortexctl status` / `bin/cortexctl down`: local stack lifecycle.
- `make docs-build` / `make docs-serve`: MkDocs build/serve.

## Coding Style & Naming Conventions
Use Rust 2021 idioms and keep code `rustfmt`-clean.

- Naming: `snake_case` for modules/functions/files, `PascalCase` for types/traits, `SCREAMING_SNAKE_CASE` for constants.
- Keep binaries thin; move reusable logic into `crates/*`.
- For shell scripts, follow existing strict mode (`set -euo pipefail`) and clear error messages.

## Testing Guidelines
Run `cargo test --workspace --locked` before opening a PR. CI also runs `scripts/ci/e2e-stack.sh`, so changes affecting ingest, monitor, MCP, or ClickHouse flows should be validated with that script locally when possible. Place tests close to the code they verify (`#[cfg(test)]` modules or crate-level integration tests).

## Development: Worktrees
When the user asks you to take on new development work, 
check out a fresh worktree with an appropriate name for your work to prevent multi-agent collision.

## Writing PRs
History uses concise, Conventional-Commit-like subjects such as:
- `feat: ...`
- `feat(monitor): ...`
- `chore: ...`
- `CI: ...`

Use imperative, scoped subjects and keep each commit focused. PRs should include:
- what changed and why,
- operational impact (config/migrations/service behavior),
- validation steps run (commands + outcomes),
- linked issue(s) when applicable.
