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
