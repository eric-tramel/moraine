---
name: moraine-author-pr
description: Create Moraine pull requests on GitHub. Use when the branch is ready to open a PR; when the user asks to create, push, or open a PR; after local changes are committed; or when documenting user-facing features, bug fixes, performance optimizations, validation, changelog impact, or operational impact in a PR that follows repository conventions without AI attribution.
---

# Moraine Author PR

## Overview

The goal is to **create the PR on GitHub**, not just draft title and body text in chat. The skill is complete when the PR exists on GitHub and you return its URL.

Always create a ready-for-review PR. Never create a GitHub Draft PR, never pass `--draft`, never set `draft: true`, and never emit an `isDraft=true` PR directive. If the branch is not ready for reviewer attention, stop and explain what remains instead of opening a Draft PR.

Write PR titles and descriptions that are reviewable, evidence-backed, and consistent across agents. Default bodies use `Description`, `Usage`, `Changelog`, and `Validation` sections. Base the summary on all commits in the branch, not only the latest one.

If the branch is not ready — uncommitted work, failing tests the user expects fixed first, or the user asked only to revise an existing PR description — say what is blocking creation and what remains. Do not pretend a PR was opened.

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
## Description

**What.** [One sentence description].

**Why.** [One sentence description].

[Multi-paragraph description]

## Usage

[How users or developers will see this change]

## Changelog

- ...

## Validation

[Narrative description of how this was validated]
```

Keep sections short when the PR is small. Add focused extra sections only when useful, such as `## Bug Evidence`, `## Performance`, `## Operational Impact`, or `## References`.

## Description

Open with bold one-line **What.** and **Why.** summaries, then expand in prose, bullets, or examples as needed.

State the concrete change in reviewer-facing terms. Mention files or subsystems when that helps review scope. Explain the reason for the change, not only the implementation. Include code, commands, queries, or before/after examples when they make the motivation concrete.

Good:

```markdown
## Description

**What.** Adds a repo-local `moraine-dev` plugin with contributor agent skills.

**Why.** Agents were repeating PR body structure differently across sessions; a shared skill gives each agent the same review contract.

- Vendors `$moraine-dev:moraine-start-work`, `$moraine-dev:moraine-author-pr`,
  and `$moraine-dev:moraine-sandbox-qa` in Codex, with `/moraine-start-work`,
  `/moraine-author-pr`, and `/moraine-sandbox-qa` equivalents in Kiro.
- Documents local installation in `docs/development/agent-contributor-workflows.md`.

That makes it easier for reviewers to find behavior, impact, and test evidence.
```

## Usage

For user-facing features, demonstrate the feature in the PR description. Use the most natural form for the change: CLI commands, config snippets, API calls, screenshots, curl examples, or monitor UI steps.

Examples:

````markdown
## Usage

Run the local Codex plugin marketplace from a checkout:

```bash
codex plugin marketplace add .
```

Then enable `moraine-dev` and invoke:

```text
Use $moraine-dev:moraine-author-pr to create this PR on GitHub.
```

Or launch the repository-local Kiro agent:

```bash
kiro-cli --agent moraine-dev
```

Then invoke the same shared skill:

```text
/moraine-author-pr
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

Write a short narrative that names exact commands and outcomes. Include skipped validation with a reason.

Example:

```markdown
## Validation

Validated plugin metadata with `python3 -m json.tool .agents/plugins/marketplace.json` and `quick_validate.py plugins/moraine-dev/skills/moraine-author-pr`. Ran `make docs-build` successfully. Rust tests were not run; change is docs and plugin metadata only.
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
