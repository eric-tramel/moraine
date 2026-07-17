# moraine-sandbox

Per-developer containerized moraine stack for isolated testing against a
worktree's code. See RFC #232.

## Prerequisites

- `docker` and the `docker compose` v2 plugin (Docker Desktop on macOS,
  Docker Engine + compose plugin on Linux).
- `bash`, `python3` — used by the host CLI for port picking and state probes.
- `bun` — required when the monitor frontend under `web/monitor/dist/` is
  stale (the CLI rebuilds it on `up`).
- Rust toolchain for binaries under test:
    - Linux host: `cargo` (default `cargo build --workspace --locked`).
    - macOS host: `cross` (the default strategy cross-compiles to the host's
      Linux triple). Alternatively pass `--build-in-container` for a
      hermetic build inside Docker.

## Quick start

```bash
# Bring up a fresh sandbox with a random id; prints monitor + clickhouse URLs.
scripts/dev/sandbox/moraine-sandbox up

# Optionally mount your host session archives (read-only), including Codex,
# Claude Code, Cursor (Agent transcripts + SQLite state), Hermes, Kimi, and Kiro
# when their default directories exist:
scripts/dev/sandbox/moraine-sandbox up --mount-host-sessions

# Interactive shell inside the running container.
scripts/dev/sandbox/moraine-sandbox shell <id>

# Follow container logs.
scripts/dev/sandbox/moraine-sandbox logs <id> -f

# List running sandboxes.
scripts/dev/sandbox/moraine-sandbox list

# Tear down (container, named volume, /tmp config dir).
scripts/dev/sandbox/moraine-sandbox down <id>
scripts/dev/sandbox/moraine-sandbox down --all
```

Agents: always call `down` before reporting your task complete.

## Host session mounts (`--mount-host-sessions`)

Each mount is read-only and can be overridden with the matching env var;
directories that don't exist on the host fall back to a `/dev/null`
placeholder and their ingest source is skipped.

| Host default | Container path | Override |
|---|---|---|
| `~/.codex/sessions` | `/host/codex/sessions` | `SANDBOX_CODEX_SESSIONS_DIR` |
| `~/.claude/projects` | `/host/claude/projects` | `SANDBOX_CLAUDE_PROJECTS_DIR` |
| `~/.hermes/sessions` | `/host/hermes/sessions` | `SANDBOX_HERMES_SESSIONS_DIR` |
| `~/.kimi/sessions` | `/host/kimi/sessions` | `SANDBOX_KIMI_SESSIONS_DIR` |
| `~/.kiro/sessions/cli` | `/host/kiro/sessions` | `SANDBOX_KIRO_SESSIONS_DIR` |
| `~/.cursor/projects` | `/host/cursor/projects` | `SANDBOX_CURSOR_PROJECTS_DIR` |
| `~/Library/Application Support/Cursor/User` (macOS) or `~/.config/Cursor/User` (Linux) | `/host/cursor/state` | `SANDBOX_CURSOR_STATE_DIR` |

The Cursor state mount feeds the `cursor_sqlite` ingest format
(`state.vscdb` SQLite databases). That format is opt-in / disabled by
default in production configs (RFC #361 — Cursor has no stable local-DB
contract), but the generated sandbox config enables it on purpose: the
sandbox exists to exercise it.

## Where state lives

- Host config dir: `/tmp/moraine-sandbox-<id>/` — generated `moraine.toml`
  and (when no host mounts are used) empty fixture directories.
- Named Docker volume: `moraine-sandbox-<id>_state` — ClickHouse data,
  ingest state, logs. Removed on `down`.
- Docker compose project: `moraine-sandbox-<id>`.

Nothing touches `~/.moraine/` on the host. Host mounts, when enabled, are
read-only.

## NOT a distribution channel

The sandbox is a dev and agent-testing tool. The image is never published,
there is no `moraine` CLI subcommand for containers, and Docker is not a
supported runtime for end users. Production install remains `install.sh` /
PyPI — see issue #219. If you find yourself reaching for this to run
moraine "for real", stop; use `install.sh` instead.

## Agent-driven smoke test (`agent-smoke-e2e`)

`scripts/dev/sandbox/agent-smoke-e2e` boots a fresh sandbox, runs the Claude
Code CLI inside the container (headless, `--dangerously-skip-permissions`),
and drives the `/agent-smoke-e2e` skill at
[`.claude/skills/agent-smoke-e2e/SKILL.md`](../../../.claude/skills/agent-smoke-e2e/SKILL.md).
The skill exercises the public retrieval workflow (`search_sessions`, `open`,
and `list_sessions`) and emits a pass/fail matrix.

The driver runs the skill **three times** — once each against Opus 4.7,
Sonnet 4.6, and Haiku 4.5 — sharing a single sandbox boot. Each model gets
a fresh `moraine-mcp` subprocess (claude spins one up per invocation), so
runs are independent from the server's perspective. The driver aggregates
the three verdicts; exit 0 requires all three to PASS. Override the model
set with `MORAINE_SMOKE_MODELS="id1 id2 ..."` for ad-hoc runs.

```bash
# One-shot: boot, run opus/sonnet/haiku in sequence, report, tear down.
export ANTHROPIC_API_KEY=sk-ant-...
scripts/dev/sandbox/agent-smoke-e2e

# Only one model, e.g. during a regression bisect:
MORAINE_SMOKE_MODELS="claude-sonnet-4-6" scripts/dev/sandbox/agent-smoke-e2e
```

Complements `scripts/ci/mcp_smoke.py` (raw JSON-RPC) by driving the MCP
tools through an actual agent — catches regressions in tool discovery,
argument validation, and LLM-shaped payloads that the raw protocol smoke
doesn't exercise.

`ANTHROPIC_API_KEY` is required. On macOS, `claude login` stores creds in
Keychain and they're unreachable from the Linux container — so exporting an
API key is the supported path for this smoke test.

## See also

- `scripts/ci/e2e-stack.sh` — the non-interactive CI gate with synthetic
  fixtures. Complementary to this sandbox, not replaced by it.
- `scripts/ci/mcp_smoke.py` — raw JSON-RPC smoke against `moraine-mcp`.
  `agent-smoke-e2e` is the agent-level companion.
- RFC: `gh issue view 232`.
