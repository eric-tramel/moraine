---
name: moraine-start-work
description: Start Moraine repository development work safely. Use when an agent takes on new code, docs, test, CI, packaging, or release work in this repo; when it must create or verify an isolated branch/worktree; when sibling agent collisions are possible; or before editing files for a Moraine contributor task.
---

# Moraine Start Work

## Overview

Use this workflow to start a Moraine contributor task with the same orientation every time: verify the checkout, isolate the branch, read governing instructions, and choose the right validation path before editing.

## Start Checklist

Run these from the repository root unless the current harness already supplied a trusted workspace root:

```bash
pwd
git rev-parse --show-toplevel
git status --short --branch
git worktree list --porcelain
find .. -name AGENTS.md -print
```

Read every `AGENTS.md` that governs the files you will edit. If the user refers to previous work, another agent, an unexplained decision, a branch you cannot see, or a failure from another session, use the Moraine MCP tools directly before making assumptions. Prefer `search_sessions` for keyword search, `list_sessions` for time or active-session browsing, `open` for expansion, and `file_attention` for file-specific history.

## Branch And Worktree

Use an isolated branch for development work. Prefer the branch prefix requested by the user; otherwise use `codex/`.

If the harness already placed you in an isolated worktree, create or verify a branch there:

```bash
git switch -c codex/<short-task-name>
```

If you are in the main checkout and need to start a new worktree, create one with a task-specific name:

```bash
git worktree add ../moraine-worktrees/<short-task-name> -b codex/<short-task-name> main
```

When spawned into a temporary agent worktree, compare your `HEAD` with any sibling worktree whose branch name matches the task. If the sibling has newer in-progress code, treat that sibling as authoritative and run sandbox validation from that worktree.

## Before Editing

Use `rg` and `rg --files` for repository discovery. Identify the narrowest owner boundary for the change:

- Runtime binaries live under `apps/`.
- Shared logic lives under `crates/`.
- Schema changes live under `sql/`.
- Runtime/config templates live under `config/`.
- Developer docs live under `docs/development/`.
- Static monitor UI assets live under `web/monitor/`.

Do not revert changes you did not make. If `git status` shows existing edits, inspect enough to avoid overwriting them and mention any relevant overlap to the user.

## Validation Choice

Run focused local checks for narrow docs or metadata changes. For Rust changes, default to:

```bash
cargo fmt --all -- --check
cargo test --workspace --locked
```

For ingest, MCP, monitor, ClickHouse schema, source-format, or stack-behavior changes, use `$moraine-sandbox-qa` before reporting the task complete.

End with the changed paths, validation commands, and any validation you intentionally skipped.
