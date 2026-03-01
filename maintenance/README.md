# Maintenance Automation

`run_multiagent_repo_audit.sh` orchestrates a multi-agent repository maintenance flow:

1. Shard source files and run parallel `gpt-5.3-codex-spark` audits.
2. Run one `gpt-5.3-codex` pass to dedupe/compile findings.
3. Run one `gpt-5.3-codex-spark` session per finding to open GitHub issues using current `P0/P1/P2` and issue format conventions.
4. Run one final `gpt-5.3-codex` xhigh session to mark issue relationships (`blocked by`, `blocks`, `relates`) across created issues.

## Quick Start

```bash
maintenance/run_multiagent_repo_audit.sh --yes
```

## Safe Dry Run

```bash
maintenance/run_multiagent_repo_audit.sh --dry-run
```

## Common Options

- `--shards N`: number of shard review agents.
- `--review-parallel N`: concurrent shard reviewers.
- `--issue-parallel N`: concurrent issue creator agents.
- `--run-dir PATH`: artifact/log directory (default in `/tmp`).
- `--max-files N`: cap reviewed files (useful for smoke tests).
- `--sandbox-mode MODE`: `bypass` (default) or Codex sandbox mode.
- `--review-model`, `--dedupe-model`, `--issue-model`: override model names.
- `--relationship-model`: override final relationship linker model.
- `--relationship-effort`: relationship linker reasoning effort (`minimal|low|medium|high|xhigh`, default `xhigh`).

## Outputs

- `maintenance/REPORT.md`: deduplicated findings.
- `maintenance/ISSUES_CREATED.md`: one line per issue-creation result.
- `maintenance/ISSUE_RELATIONSHIPS.md`: one line per relationship-linking result.
- run artifacts in `/tmp/moraine-maintenance-<timestamp>` (or `--run-dir`).

## Issue Worker Orchestrator

`run_issue_worker.sh` orchestrates issue-fix workers:

1. Optionally filter by a comma-separated label union (`--tag-union`).
2. Select issues by priority (`P0 > P1 > P2 > unlabeled`, then lowest issue number).
3. Claim each selected issue with `status/in-progress`.
4. Launch worker executions with a 20-second stagger between launches.
5. Use `--parallel N` (default `1`) to control concurrent workers.
6. Use `--continuous` to keep refilling worker slots until no eligible issues remain.

Quick start (no tag filter):

```bash
maintenance/run_issue_worker.sh
```

Quick start (with tag filter):

```bash
maintenance/run_issue_worker.sh --tag-union "area/config,area/security"
```

One-shot parallel batch (up to 3 workers):

```bash
maintenance/run_issue_worker.sh --parallel 3
```

Continuous pool (keep up to 3 workers active until issue queue is empty):

```bash
maintenance/run_issue_worker.sh --parallel 3 --continuous
```

Dry run (select only, no claim or Codex launch, no tag filter):

```bash
maintenance/run_issue_worker.sh --dry-run
```

Dry run (with tag filter):

```bash
maintenance/run_issue_worker.sh --tag-union "area/config,area/security" --dry-run
```

Main outputs:

- No run artifacts are retained.
- The script uses a temporary run directory under `/tmp` and removes it on exit.
- Durable state is kept in GitHub issue/PR updates only.

## Merge Train Orchestrator

`run_merge_train.sh` orchestrates a manual merge train for pull requests:

1. Select queued PRs on a base branch (`main` by default).
2. Validate readiness (draft status, optional approval, required checks, mergeability).
3. Merge ready PRs sequentially.
4. For behind/conflicting PRs, launch a Codex repair session in a fresh worktree to integrate latest base branch and push the updated head branch.
5. Stop after the first repair attempt by default so one run does not trigger a full repair cascade.

Quick start (queue label `merge-queue`):

```bash
maintenance/run_merge_train.sh --yes
```

Dry run:

```bash
maintenance/run_merge_train.sh --dry-run
```

Process all open PRs for `main` (no queue label filter):

```bash
maintenance/run_merge_train.sh --all-open --yes
```

Common options:

- `--queue-label LABEL`: queue selector label (default `merge-queue`).
- `--all-open`: ignore queue label and consider all open PRs for the base branch.
- `--max-merges N`: cap merges performed in one run.
- `--merge-method METHOD`: `squash|merge|rebase`.
- `--require-approval`: require `reviewDecision=APPROVED` before merge.
- `--no-repair`: disable Codex-based repair for behind/conflicting PRs.
- `--continue-after-repair`: process additional PRs after a repair attempt (default is to stop after the first repair to avoid repair cascades).
- `--repair-gate CMD`: validation command run after repair (default `cargo test --workspace --locked`).
- `--cleanup-worktrees`: remove successful repair worktrees at the end.

Main outputs:

- `maintenance/MERGE_TRAIN_REPORT.md`: run summary with per-PR actions.
- Run logs/artifacts in `/tmp/moraine-merge-train-<timestamp>` (or `--run-dir`).
