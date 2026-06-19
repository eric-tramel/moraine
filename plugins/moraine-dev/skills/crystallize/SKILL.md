---
name: crystallize
description: Turn a rough feature, bug, refactor, architecture, documentation, or operations idea into a ready-to-implement local plan file. Use when the user asks to crystallize, shape, refine, plan, spec, or de-risk ambiguous work before implementation; the workflow researches the codebase, public prior art, and relevant history, debates the proposed approach with subagents, and writes a final plan with no open questions.
---

# Crystallize

## Overview

Use this skill to convert rough input into an implementation plan that another agent can execute without needing the original discussion. The output is a single ignored local Markdown file under the gitignored `plans/` directory.

Do not implement the plan while crystallizing. Inspect, research, and reason; only write the plan artifact.

## Plan Artifact

Create `plans/` if it does not exist:

```bash
mkdir -p plans
```

Choose a unique filename:

- Derive a lowercase hyphenated slug from the rough input.
- Prefix it with a UTC timestamp from `date -u +%Y%m%d-%H%M%S`.
- Use `plans/<timestamp>-<slug>.md`.
- If the file already exists, append `-2`, `-3`, etc.

Keep `plans/` and every plan file ignored and untracked. Do not add them to git, do not create a tracked placeholder, and do not include plan files in commits.

## Research Wave

Launch parallel subagents before drafting the plan. Give each subagent the raw user input, any linked issue or document text, and explicit permission to inspect the workspace. Do not include your intended solution.

Use these research roles unless a role is plainly irrelevant:

- `CodebaseResearch`: identify affected crates, modules, APIs, tests, docs, and local patterns. Return concrete file paths and constraints.
- `HistoryResearch`: use Moraine MCP tools directly when prior or active agent context may matter. Search past sessions by keywords, inspect relevant sessions, and return only actionable history.
- `PublicPriorArtResearch`: research public implementations, official docs, standards, libraries, and comparable approaches. Prefer primary sources and include links when external sources influence the plan.
- `RiskResearch`: identify correctness, security, performance, migration, compatibility, rollout, and testing risks.

Subagent prompt shape:

```text
Research this rough implementation idea for Moraine.
Focus only on <role>.
Return evidence, concrete file paths or source links, constraints, and recommended plan implications.
Do not draft the whole plan and do not assume my preferred approach.

Rough input:
<raw user input>
```

Wait for every required research subagent to finish. If a subagent fails, retry once with a narrower prompt. If a role is still unavailable, perform that research yourself before drafting.

## Draft Plan

Synthesize the research into a concrete plan. Resolve ambiguities yourself using evidence and repository conventions. Do not ask the user open-ended questions unless the request would require an external decision that cannot be inferred safely; if that happens, stop before writing the final plan.

The draft must include implementation steps, affected files, validation, risks, and acceptance criteria. It must be specific enough that another agent can start from the plan file alone.

## Debate Wave

Before finalizing, launch a second parallel wave of subagents to argue against the draft plan. Provide the draft and the research summary, not private reasoning.

Use these debate roles:

- `PlanSkeptic`: find bugs, missing requirements, invalid assumptions, and vague steps.
- `PlanSimplifier`: force the minimal viable approach and remove unnecessary generalization.
- `PlanIdiomaticReviewer`: check language, framework, library, and repo idioms.
- `PlanScopeGuard`: reject scope creep and unrelated cleanup.
- `PlanValidationReviewer`: verify the validation strategy would prove the change works.

Debate prompt shape:

```text
Attack this implementation plan for Moraine from the <role> perspective.
Be direct. Identify blockers, weak assumptions, unnecessary scope, and missing validation.
Return required changes to make the plan ready to implement. If it is ready, say so.

Draft plan:
<draft plan>

Research summary:
<research summary>
```

Resolve every substantive objection. If debate agents disagree, decide the best course using evidence, repo conventions, and the user's original objective. Prefer the `PlanScopeGuard` when other roles request broader behavior that crosses the task boundary.

Run targeted follow-up only with the debate subagents whose objections required interpretation or whose facet changed after revision. Do not start a whole new debate wave unless the draft was fundamentally replaced.

## Final Plan Requirements

The final plan must have no open questions, no `TBD`, no unresolved alternatives, and no "ask the user" steps. Every uncertainty must be converted into a decision, assumption, or explicit non-goal.

Write the plan file with this structure:

```markdown
# <short plan title>

Status: Ready to implement
Created: <UTC timestamp>
Source input: <brief quote or summary of the rough input>

## Objective

<one concrete outcome>

## Context

- Codebase findings:
- History findings:
- Public prior art:
- Constraints:

## Decisions

- <decision and rationale>

## Non-Goals

- <explicitly out-of-scope work>

## Implementation Plan

1. <step>
   - Files:
   - Change:
   - Tests:

## Validation

- <commands, checks, fixtures, manual QA, or docs build steps>

## Risks And Mitigations

- <risk>: <mitigation>

## Acceptance Criteria

- <observable completion criterion>

## Implementation Handoff

<concise prompt an implementation agent can use to execute the plan>
```

Include exact file paths where possible. Include commands with expected outcomes. For stack-facing Moraine changes, include sandbox validation. For public prior art that materially influenced the plan, include source links in `Context`.

Before finishing, run:

```bash
git check-ignore -v plans/<plan-file>.md
```

The expected result is that root `.gitignore` ignores the plan file through the `plans/` rule. In the final response, report the plan path and state that it was intentionally left ignored and untracked.
