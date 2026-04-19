# Repository Guidelines

## Project Structure & Module Organization
This repository is a Rust workspace for local Moraine services and shared libraries.

- `apps/`: binary crates (`moraine`, `moraine-ingest`, `moraine-monitor`, `moraine-mcp`).
- `crates/`: shared libraries (config, ClickHouse client, ingest/monitor/MCP core logic).
- `sql/`: ordered schema and migration SQL (`001_...sql`, `002_...sql`, etc.).
- `config/`: default runtime/config templates (`moraine.toml`, ClickHouse XML).
- `scripts/`: CI helpers, packaging, docs tooling.
- `docs/`: MkDocs source; generated site output goes to `site/`.
- `web/monitor/`: static monitor UI assets served by monitor service.

Prefer adding new runtime logic under `apps/` + `crates/` (not legacy `rust/` or `moraine-monitor/` paths).

## Build, Test, and Development Commands
- `cargo build --workspace --locked`: build all workspace crates.
- `cargo test --workspace --locked`: run unit/integration tests across workspace.
- `cargo fmt --all -- --check`: enforce formatting (matches CI).
- `bash scripts/ci/e2e-stack.sh`: run functional stack + MCP smoke test.
- `bin/moraine up` / `bin/moraine status` / `bin/moraine down`: local stack lifecycle.
- `make docs-build` / `make docs-serve`: MkDocs build/serve.
- `make hooks-install`: enable the repo-managed git hooks (`.githooks/pre-commit` runs `cargo fmt --check` and the same clippy strict baseline CI uses). One-time per clone; bypass a single commit with `SKIP_PRECOMMIT=1 git commit ...` or `git commit --no-verify`.

## Coding Style & Naming Conventions
Use Rust 2021 idioms and keep code `rustfmt`-clean.

- Naming: `snake_case` for modules/functions/files, `PascalCase` for types/traits, `SCREAMING_SNAKE_CASE` for constants.
- Keep binaries thin; move reusable logic into `crates/*`.
- For shell scripts, follow existing strict mode (`set -euo pipefail`) and clear error messages.

## Testing Guidelines
Run `cargo test --workspace --locked` before opening a PR. CI also runs `scripts/ci/e2e-stack.sh`, so changes affecting ingest, monitor, MCP, or ClickHouse flows should be validated with that script locally when possible. Place tests close to the code they verify (`#[cfg(test)]` modules or crate-level integration tests).

For changes that touch ingest, MCP, monitor, or ClickHouse schema, run them inside a dev sandbox rather than against your host install. The sandbox is isolated from your live `~/.moraine/`; the host stack is not.

## Dev sandbox (required for QA of ingest/monitor/MCP/schema)

The sandbox is a long-lived linux container that mounts your current worktree at `/repo` read-only, `cargo build`s the workspace on first boot (wrapped by sccache sharing the host's cache), then runs the moraine stack against a sibling-compose ClickHouse. Iterate inside via `moraine-sandbox shell` — cargo / rustc / rustup / sccache are all on `PATH`, and `CARGO_TARGET_DIR` is volume-backed so subsequent builds are incremental. See [docs/development/sandbox.md](docs/development/sandbox.md) for the full reference.

### Typical agent flow

```bash
# 1. Boot. Capture the id with --quiet so you can't lose it to output
#    truncation. First boot cold is ~2 min; warm is ~30 s.
id=$(scripts/dev/sandbox/moraine-sandbox up --quiet)
echo "sandbox: $id"

# 2. Shell in and iterate. cargo check / cargo test / cargo clippy all just
#    work. sccache is RUSTC_WRAPPER, so builds share a cache with every
#    other sandbox and with host cargo (for matching target triples).
scripts/dev/sandbox/moraine-sandbox shell "$id"
# inside the container:
#   cd /repo
#   cargo check --workspace --locked
#   cargo test -p <crate> --locked
#   cargo clippy --workspace --all-targets -- -D warnings

# 3. Validate behavior against the printed monitor URL.
port=$(scripts/dev/sandbox/moraine-sandbox status "$id" | awk -F: '/^\[sandbox\] monitor/{print $NF}')
curl -fsS "http://127.0.0.1:${port}/api/health"

# 4. Tear down before reporting task complete. Leftover sandboxes leak
#    ClickHouse data and consume host ports.
scripts/dev/sandbox/moraine-sandbox down "$id"
```

**Important**: do NOT pipe `moraine-sandbox up` through `tail` / `head` to
limit output — the `[sandbox] up: sb-xxxxxx` summary line can fall off the
end of the buffer, at which point you've booted a sandbox you can no longer
identify (and, since you assume it belongs to another agent when it shows
up in `list`, you boot a *second* one and leak the first). Use `--quiet`
(id-only on stdout) or pass `--id sb-xxxxxx` yourself so you always know
which sandbox is yours.

### Commands

| Command | What it does |
|---|---|
| `moraine-sandbox up [--id <id>] [--rebuild] [--mount-host-sessions] [--quiet\|-q]` | Boot. Builds the workspace on first run; `--rebuild` forces re-compile; `--quiet` prints only the sandbox id on stdout (progress goes to stderr) so the id is safe under piping/truncation. |
| `moraine-sandbox shell [<id>]` | `docker exec` an interactive bash as user `moraine`. |
| `moraine-sandbox logs [<id>] [-f]` | Tail container logs (includes the bootstrap cargo build output). |
| `moraine-sandbox status [<id>]` | Summary block + `docker compose ps`. |
| `moraine-sandbox list` | One-line-per-sandbox table. |
| `moraine-sandbox down <id>` / `down --all` | Stop, remove the container + all named volumes + the `/tmp/moraine-sandbox-<id>/` config dir. |

### Agent-driven MCP smoke test

For a higher-level sanity check than `cargo test` can give, use `scripts/dev/sandbox/agent-smoke-e2e` (self-documented; run with `--help`). It boots a fresh sandbox, runs the `/agent-smoke-e2e` skill (see `.claude/skills/agent-smoke-e2e/SKILL.md`) against Opus/Sonnet/Haiku in sequence, and aggregates a PASS/FAIL matrix across every `moraine-mcp` tool. Use it before cutting a release, or when you've touched anything in `moraine-mcp-core` / the MCP tool surface. Requires `ANTHROPIC_API_KEY` in the environment.

### Isolation worktrees (spawned via the Agent SDK's `isolation: "worktree"`)

If you were spawned into a temporary worktree (e.g. `/Users/.../.claude/worktrees/agent-<id>/`), its branch may be **`main`**, not the branch the parent session is working on. The Agent SDK currently branches isolation worktrees from the repo root's `HEAD`, not the calling process's current branch.

**Before you start:** compare `git -C <your-worktree> rev-parse HEAD` against `git -C /Users/eric/src/moraine/.claude/worktrees/*/ rev-parse HEAD` for any sibling worktree whose branch name matches the task you were given. If they differ, the sibling worktree (the parent session's working copy) is the authoritative source for in-progress code. Invoke `scripts/dev/sandbox/moraine-sandbox` from there — the sandbox will bind-mount *that* worktree at `/repo`, which is what you actually want to test.

### Things to know

- `/repo` is **read-only**. Cargo writes go to a `CARGO_TARGET_DIR` volume at `/home/moraine/target`, not into your worktree.
- Each sandbox has its own set of named volumes (`binaries`, `cargo-home`, `cargo-target`, `state`, `clickhouse-data`). Two sandboxes from two worktrees run in parallel with no coordination.
- The host's `$SCCACHE_DIR` (`~/.cache/sccache` by default) is bind-mounted rw. Cache entries are content-addressed by compiler + target + source, so darwin (host cargo) and linux (container cargo) entries coexist without conflict, and one sandbox's cache hits every other sandbox's lookups.
- ClickHouse runs as a **sibling** compose service reachable at `http://clickhouse:8123` from inside moraine; moraine does not manage it inside its own container.
- Exactly one shared runtime image (`moraine-sandbox-runtime:latest`, ~2.2 GB) serves every sandbox on the host. Rebuilding it is only needed when `scripts/dev/sandbox/{Dockerfile,entrypoint.sh}` change — normal code edits in the worktree just get picked up by `--rebuild` (or a fresh `up`).

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
