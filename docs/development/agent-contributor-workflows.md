# Agent Contributor Workflows

This repository vendors developer-only agent workflows as a local plugin. These
skills are for contributors and automation agents working on Moraine itself;
they are not part of the end-user Moraine runtime.

## Layout

```text
.agents/plugins/marketplace.json
plugins/moraine-dev/
  .codex-plugin/plugin.json
  skills/
    crystallize/
    code-review/
    code-review-completeness/
    code-review-correctness/
    code-review-elegance/
    code-review-idomatic/
    code-review-scope/
    code-review-security-review/
    code-review-yagni/
    moraine-author-pr/
    moraine-start-work/
    moraine-sandbox-qa/
```

The marketplace entry points to `./plugins/moraine-dev`, and the
plugin manifest exposes `./skills/`.

## Install Locally

From the repository root:

```bash
codex plugin marketplace add .
```

Then enable or install `moraine-dev` from the Codex plugin UI. Other
agent harnesses that understand `SKILL.md` directories can consume the same
skill folders directly.

Codex exposes plugin skills with the plugin namespace, so use the
`$moraine-dev:<skill-name>` form in prompts.

## Skills

| Skill | Purpose |
| --- | --- |
| `$moraine-dev:crystallize` | Turn rough input into an untracked, ready-to-implement plan under `plans/`. |
| `$moraine-dev:code-review` | Coordinate one review wave across all code-review personas and targeted follow-up. |
| `$moraine-dev:moraine-author-pr` | Draft PR titles and descriptions with standard evidence and validation sections. |
| `$moraine-dev:moraine-start-work` | Start development work with branch/worktree, instruction, and validation checks. |
| `$moraine-dev:moraine-sandbox-qa` | Run stack-facing QA in the isolated dev sandbox and tear it down afterward. |

## Planning

Use `$moraine-dev:crystallize` when the user has a rough idea, feature sketch,
bug report, or architecture direction that needs research and debate before
implementation. The skill writes a uniquely named Markdown plan under a local
`plans/` directory and leaves that directory untracked. Do not commit generated
plan files unless a maintainer explicitly asks for a specific plan artifact to
be versioned.

## Review Personas

Use `$moraine-dev:code-review` when you want the whole review set. It launches one subagent
per persona, tracks those sessions, integrates their feedback, and follows up
only with the sessions that need another look. Each review persona is a focused
skill that should report findings only for its facet unless another issue is a
direct blocker.

| Persona | Skill | Review facet |
| --- | --- | --- |
| CodeReviewElegance | `$moraine-dev:code-review-elegance` | Minimal design, leverage, and simplifying abstractions. |
| CodeReviewIdomatic | `$moraine-dev:code-review-idomatic` | Idiomatic language, standard library, ecosystem, and repo patterns. |
| CodeReviewCorrectness | `$moraine-dev:code-review-correctness` | Bugs, regressions, edge cases, and behavioral correctness. |
| CodeReviewCompleteness | `$moraine-dev:code-review-completeness` | Whether the PR satisfies its stated objectives and linked requirements. |
| CodeReviewSecurityReview | `$moraine-dev:code-review-security-review` | Security, secrets, dependency risk, injection, and trust boundaries. |
| CodeReviewYAGNI | `$moraine-dev:code-review-yagni` | Overengineering, premature generalization, and speculative defenses. |
| CodeReviewScope | `$moraine-dev:code-review-scope` | Scope control, unrelated changes, and review-lane separation. |

Historical and active session lookup does not need a skill. Use the Moraine MCP
tools directly: `search_sessions`, `list_sessions`, `open`, and
`file_attention`.

## Maintenance

Keep these skills focused on repeatable contributor actions. Do not add product
documentation, user guidance, or runtime behavior here.

When changing a skill, validate it with Codex's skill validator when available:

```bash
python3 "${CODEX_HOME:-$HOME/.codex}/skills/.system/skill-creator/scripts/quick_validate.py" \
  plugins/moraine-dev/skills/<skill-name>
```

Also keep `AGENTS.md` aligned with the installed skill names so agents discover
the workflows before starting work.
