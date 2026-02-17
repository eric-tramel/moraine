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
