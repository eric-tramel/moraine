---
name: moraine-start-work
description: Start Moraine repository development work safely. Use when an agent takes on new code, docs, test, CI, packaging, or release work in this repo; when it must create or verify an isolated branch/worktree; when sibling agent collisions are possible; or before editing files for a Moraine contributor task.
---

# Moraine Start Work

## Overview

Use this workflow to start a Moraine contributor task with the same orientation every time: verify the checkout, isolate the branch, read governing instructions, and choose the right validation path before editing.

Using `$moraine-dev:code-review` in Codex or `/code-review` in Kiro is an
explicit user request for delegated reviewer subagents. You have permission to
launch that review wave when the follow-on workflow reaches that step.

## Harness Invocation

Use the invocation form native to the current harness:

| Workflow | Codex | Kiro |
| --- | --- | --- |
| `crystallize` | `$moraine-dev:crystallize` | `/crystallize` |
| `code-review` | `$moraine-dev:code-review` | `/code-review` |
| `moraine-sandbox-qa` | `$moraine-dev:moraine-sandbox-qa` | `/moraine-sandbox-qa` |
| `moraine-author-pr` | `$moraine-dev:moraine-author-pr` | `/moraine-author-pr` |

## Follow-On Workflow

After the start checklist and branch/worktree setup, run the rest of the contributor workflow in this order:

1. Use the current harness's `crystallize` invocation to turn rough or ambiguous input into a ready-to-implement local plan.
2. Implement the scoped change from that plan or, when the task is already concrete, from the user's explicit request.
3. Use the current harness's `code-review` invocation and iterate fixes until the review is clean or every remaining item is explicitly rejected or deferred with rationale.
4. Use the current harness's `moraine-sandbox-qa` invocation for stack-facing changes and iterate fixes until sandbox QA is clean.
5. Use the current harness's `moraine-author-pr` invocation to create a PR.

## Start Checklist

Run these from the repository root unless the current harness already supplied a trusted workspace root:

```bash
pwd
git rev-parse --show-toplevel
git status --short --branch
git worktree list --porcelain
find .. -name AGENTS.md -print
```

If the user refers to previous work, another agent, an unexplained decision, a branch you cannot see, or a failure from another session, use the Moraine MCP tools directly before making assumptions.

## Branch And Worktree

Use an isolated branch for development work. Choose a repository-appropriate
prefix such as `feat/`, `fix/`, or `docs/`; branch names describe the work, not
the harness running it.

If the harness already placed you in an isolated worktree, create or verify a branch there:

```bash
branch="feat/<short-task-name>" # replace with the repository-appropriate prefix
git switch -c "$branch"
```

If you are in the main checkout and need to start a new worktree, create one with a task-specific name:

```bash
branch="feat/<short-task-name>" # replace with the repository-appropriate prefix
git worktree add ../moraine-worktrees/<short-task-name> -b "$branch" main
```

When spawned into a temporary agent worktree, compare your `HEAD` with any sibling worktree whose branch name matches the task. If the sibling has newer in-progress code, treat that sibling as authoritative and run sandbox validation from that worktree.

## Validation Description

Favor narrative description of what you tested instead of bullet points.
