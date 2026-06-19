---
name: moraine-author-pr
description: Author Moraine pull request titles and descriptions. Use when drafting, revising, or reviewing a PR body for this repository; when preparing a PR summary after local changes; when documenting user-facing features, bug fixes, performance optimizations, validation, changelog impact, or operational impact; or when ensuring PR text follows repository conventions without AI attribution.
---

# Moraine Author PR

## Overview

Use this workflow to write PR titles and descriptions that are reviewable, evidence-backed, and consistent across agents.

## Title Rules

Use a concise imperative title. Prefer the repository's Conventional-Commit-like style:

```text
feat(monitor): add token trend filters
fix(ingest): preserve cursor sqlite session ids
docs: document agent contributor workflows
chore: tighten sandbox cleanup handling
```

Do not include AI attribution in the title or body. Do not add generated-by footers, co-author trailers for agents, or notes that the PR was written by an AI tool.

## Body Shape

Default to these sections:

```markdown
## What

- ...

## Why

- ...

## Usage

- ...

## Changelog

- ...

## Validation

- ...
```

Keep sections short when the PR is small. Add focused extra sections only when useful, such as `## Bug Evidence`, `## Performance`, `## Operational Impact`, or `## References`.

## What

State the concrete change in reviewer-facing terms. Mention files or subsystems when that helps review scope.

Good:

```markdown
## What

- Adds a repo-local `moraine-dev` plugin.
- Vendors `$moraine-dev:moraine-start-work`, `$moraine-dev:moraine-author-pr`, and `$moraine-dev:moraine-sandbox-qa` skills.
- Documents local installation in `docs/development/agent-contributor-workflows.md`.
```

## Why

Explain the reason for the change, not only the implementation. Include code, commands, queries, or before/after examples when they make the motivation concrete.

Example:

````markdown
## Why

Agents were repeating PR body structure differently across sessions. A shared
skill gives each agent the same review contract:

```text
What -> Why -> Usage -> Changelog -> Validation
```

That makes it easier for reviewers to find behavior, impact, and test evidence.
````

## Usage

For user-facing features, demonstrate the feature in the PR description. Use the most natural form for the change: CLI commands, config snippets, API calls, screenshots, curl examples, or monitor UI steps.

Examples:

````markdown
## Usage

Run the local plugin marketplace from a checkout:

```bash
codex plugin marketplace add .
```

Then enable `moraine-dev` and invoke:

```text
Use $moraine-dev:moraine-author-pr to draft this PR.
```
````

````markdown
## Usage

Example config:

```toml
[mcp]
retrieval_scope = "project_only"
```

With this setting, MCP search only returns sessions from the current project.
````

Skip usage only when the PR is purely internal and no user, operator, or downstream developer action changes. If skipped, say `No user-facing usage change.`

## Changelog

Summarize the externally meaningful change. Use `None` when there is no release-note-worthy impact.

Good:

```markdown
## Changelog

- Adds developer-only PR authoring guidance for Moraine contributor agents.
- No end-user runtime behavior change.
```

## Validation

List exact commands and outcomes. Include skipped validation with a reason.

Example:

```markdown
## Validation

- `python3 -m json.tool .agents/plugins/marketplace.json` passed.
- `quick_validate.py plugins/moraine-dev/skills/moraine-author-pr` passed.
- `make docs-build` passed.
- Rust tests not run; docs/plugin metadata only.
```

For stack-facing changes, include sandbox validation details or state why sandbox QA was not run.

## Bug Fixes

Bug fix PRs must include specific evidence that the bug was fixed. Provide at least:

- The failing behavior or reproducer before the change.
- The root cause in concrete terms.
- The code path that changed.
- The command, test, fixture, or manual check that now passes.

Example:

````markdown
## Bug Evidence

Before this PR, `cursor_sqlite` records with missing workspace paths produced
empty project filters:

```text
search_sessions(project_only=true) -> 0 results
```

The fix normalizes the workspace path before writing the session row. The new
fixture covers the missing-path case, and `cargo test -p moraine-ingest-core
cursor_sqlite --locked` now passes.
````

## Performance

Performance optimization PRs must include baseline and PR metrics. Include:

- Hardware or environment when relevant.
- Dataset or workload size.
- Command or measurement method.
- Baseline metric.
- PR metric.
- Delta and caveats.

Example:

````markdown
## Performance

Workload: 100k normalized events, local ClickHouse, warm cache.

| Metric | Baseline | This PR | Delta |
| --- | ---: | ---: | ---: |
| `search_sessions` p50 | 182 ms | 121 ms | -33.5% |
| `search_sessions` p95 | 441 ms | 286 ms | -35.1% |

Command:

```bash
bash scripts/bench/search-sessions.sh --events 100000 --warm-cache
```
````

Do not describe a change as a performance improvement without metrics. If metrics are not available, phrase it as a refactor or preparation work instead.
