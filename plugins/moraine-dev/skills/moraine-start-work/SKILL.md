---
name: moraine-start-work
description: Start Moraine repository development work safely. Use when an agent takes on new code, docs, test, CI, packaging, or release work in this repo; when it must create or verify an isolated branch/worktree; when sibling agent collisions are possible; or before editing files for a Moraine contributor task.
---

# Moraine Start Work

## Overview

Use this workflow to start a Moraine contributor task with the same orientation every time: verify the checkout, isolate the branch, read governing instructions, and choose the right validation path before editing.

## Follow-On Workflow

After the start checklist and branch/worktree setup, run the rest of the contributor workflow in this order:

1. Use `$moraine-dev:crystallize` to turn rough or ambiguous input into a ready-to-implement local plan.
2. Implement the scoped change from that plan or, when the task is already concrete, from the user's explicit request.
3. Use `$moraine-dev:code-review` and iterate fixes until the review is clean or every remaining item is explicitly rejected or deferred with rationale.
4. Use `$moraine-dev:moraine-sandbox-qa` for stack-facing changes and iterate fixes until sandbox QA is clean. For changes that do not need sandbox QA, record why it was skipped.
5. Use `$moraine-dev:moraine-author-pr` to draft the PR title and description from the final diff, evidence, and validation results.

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

## Validation Choice

Run focused local checks for narrow docs or metadata changes. For Rust changes, default to:

```bash
cargo fmt --all -- --check
cargo test --workspace --locked
```

For ingest, MCP, monitor, ClickHouse schema, source-format, or stack-behavior changes, use `$moraine-dev:moraine-sandbox-qa` before reporting the task complete.

End with the changed paths, validation commands, and any validation you intentionally skipped.
