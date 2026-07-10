---
name: moraine-sandbox-qa
description: Run isolated Moraine dev sandbox validation. Use when testing ingest, MCP, monitor, ClickHouse schema, source formats, stack behavior, or any change where QA must not touch the developer's host ~/.moraine install; use before reporting completion for stack-facing changes.
---

# Moraine Sandbox QA

## Overview

Use the dev sandbox for stack-facing QA so validation runs against an isolated Moraine state directory and sibling ClickHouse service instead of the host install.

## When To Use

Use this workflow for changes touching:

- `crates/moraine-ingest-core/`, ingest source formats, or fixtures.
- `crates/moraine-mcp-core/`, `apps/moraine-mcp/`, or MCP tool behavior.
- `crates/moraine-monitor-core/`, `apps/moraine-monitor/`, or `web/monitor/`.
- `sql/`, ClickHouse migrations, config templates, or stack lifecycle behavior.
- Release or smoke-test work that needs end-to-end behavior.

For docs-only or narrow metadata changes, normal local checks are usually enough.

## Boot

Boot from the authoritative worktree for the change. Use `--quiet`; do not pipe `moraine-sandbox up` through `tail` or `head`.

```bash
id=$(scripts/dev/sandbox/moraine-sandbox up --quiet)
echo "sandbox: $id"
scripts/dev/sandbox/moraine-sandbox status "$id"
```

If you spawned from an isolation worktree, compare with the parent or sibling feature worktree first. Boot the sandbox from the worktree that contains the actual in-progress code.

## Validate

Use the sandbox shell for cargo and stack checks:

```bash
scripts/dev/sandbox/moraine-sandbox shell "$id"
cd /repo
cargo fmt --all -- --check
cargo test -p <crate> --locked
cargo test --workspace --locked
cargo clippy --workspace --all-targets -- -D warnings
```

Choose focused tests first, then broaden according to risk. For changes that affect ingest, MCP, monitor routes, or schema behavior, run the functional stack smoke test when feasible:

```bash
bash scripts/ci/e2e-stack.sh
```

Check the monitor health endpoint using the sandbox-reported port:

```bash
port=$(scripts/dev/sandbox/moraine-sandbox status "$id" | awk -F: '/^\[sandbox\] monitor/{print $NF}')
curl -fsS "http://127.0.0.1:${port}/api/v1/health"
```

For MCP tool surface changes or release-level confidence, consider `scripts/dev/sandbox/agent-smoke-e2e --help`. It requires `ANTHROPIC_API_KEY`.

## Teardown

Always tear down the sandbox before reporting the task complete:

```bash
scripts/dev/sandbox/moraine-sandbox down "$id"
```

Final reporting should include the sandbox id, commands run, outcomes, and confirmation that `down "$id"` succeeded. If sandbox QA was expected but not run, explain why.
