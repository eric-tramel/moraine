# Agent Contributor Workflows

This repository vendors developer-only Agent Skills through a Codex plugin and
a repository-local Kiro CLI agent. Both harnesses use the same `SKILL.md`
workflow contracts. These workflows are for contributors and automation agents
working on Moraine itself; they are not part of the end-user Moraine runtime.

## Layout

```text
.agents/plugins/marketplace.json
.kiro/agents/moraine-dev.json
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
    release/
    moraine-start-work/
    moraine-sandbox-qa/
```

The Codex marketplace is named `moraine`. It exposes the end-user `moraine`
runtime plugin and the contributor-only `moraine-dev` workflow plugin. The
developer helper installs only `moraine-dev`; it does not automatically install
the end-user runtime MCP plugin.

The `moraine-dev` marketplace entry points to `./plugins/moraine-dev`, and the
plugin manifest exposes `./skills/`.

Kiro's workspace agent loads those same workflow files directly with a
`skill://plugins/moraine-dev/skills/*/SKILL.md` resource. Kiro reads skill
metadata at startup, loads full instructions on demand, and exposes each skill
as a slash command.

The shared skill bodies preserve harness-native invocation forms instead of
assuming one client: `$moraine-dev:<skill>` for Codex, `/<skill>` for Kiro,
and the discovered skill name for other clients that implement open Agent
Skills.

## Codex

### Install Locally

From the repository root:

```bash
make agent-plugins-install
```

This registers the Codex marketplace from the configured Git remote, replaces a
stale marketplace entry, syncs the marketplace snapshot from `origin/main`, and
installs or refreshes the `moraine-dev` plugin listed there. This intentionally
uses the merged `main` branch as the source of truth so agents running from
stale feature worktrees still pick up the latest developer workflows.

When changing the plugin itself, test the unmerged checkout explicitly:

```bash
make agent-plugins-install AGENT_PLUGINS_SOURCE=current
```

Other agent harnesses that understand `SKILL.md` directories can consume the
same skill folders directly.

Codex exposes plugin skills with the plugin namespace, so use the
`$moraine-dev:<skill-name>` form in prompts.

## Kiro

### Repository Agent

Kiro CLI discovers `.kiro/agents/moraine-dev.json` when it runs from this
checkout. Validate and launch it from the repository root:

```bash
kiro-cli agent validate --path .kiro/agents/moraine-dev.json
kiro-cli --agent moraine-dev
```

The workspace agent loads `AGENTS.md`, the shared Agent Skills, and a dedicated
`moraine run mcp` server. It does not edit `~/.kiro`, depend on the global Kiro
MCP registration, or affect Kiro sessions outside this repository. Custom Kiro
agents do not load skills automatically, so the `skill://` resource in the
agent configuration is required.

Invoke workflows directly:

```text
/moraine-start-work
/code-review
/moraine-sandbox-qa
/moraine-author-pr
/release X.Y.Z
```

## Skills

| Workflow | Codex | Kiro | Purpose |
| --- | --- | --- | --- |
| `crystallize` | `$moraine-dev:crystallize` | `/crystallize` | Turn rough input into an ignored, ready-to-implement plan under `plans/`. |
| `code-review` | `$moraine-dev:code-review` | `/code-review` | Coordinate one review wave across all code-review personas and targeted follow-up. |
| `moraine-author-pr` | `$moraine-dev:moraine-author-pr` | `/moraine-author-pr` | Draft PR titles and descriptions with standard evidence and validation sections. |
| `release` | `$moraine-dev:release` | `/release` | Cut and publish a Moraine release from a target version. |
| `moraine-start-work` | `$moraine-dev:moraine-start-work` | `/moraine-start-work` | Start development work with branch/worktree, instruction, and validation checks. |
| `moraine-sandbox-qa` | `$moraine-dev:moraine-sandbox-qa` | `/moraine-sandbox-qa` | Run stack-facing QA in the isolated dev sandbox and tear it down afterward. |

## Planning

Use `$moraine-dev:crystallize` in Codex, `/crystallize` in Kiro, or the
discovered `crystallize` skill in another Agent Skills harness when the user
has a rough idea, feature sketch, bug report, or architecture direction that
needs research and debate before implementation. The skill writes a uniquely
named Markdown plan under a local gitignored `plans/` directory. Do not commit
generated plan files unless a maintainer explicitly asks for a specific plan
artifact to be versioned.

## Review Personas

Use `$moraine-dev:code-review` in Codex, `/code-review` in Kiro, or the
discovered `code-review` skill in another Agent Skills harness for the whole
review set. Invoking the skill is an explicit request for delegated multi-agent
review; agents should not ask for separate permission to spawn reviewer
subagents, and should not replace the review wave with a local single-agent
review. If subagent tooling is unavailable, report the delegated review as
blocked. The coordinator launches one subagent per persona, tracks those
sessions, integrates their feedback, and follows up only with sessions that
need another look.

| Persona | Codex | Kiro | Review facet |
| --- | --- | --- | --- |
| CodeReviewElegance | `$moraine-dev:code-review-elegance` | `/code-review-elegance` | Minimal design, leverage, and simplifying abstractions. |
| CodeReviewIdomatic | `$moraine-dev:code-review-idomatic` | `/code-review-idomatic` | Idiomatic language, standard library, ecosystem, and repo patterns. |
| CodeReviewCorrectness | `$moraine-dev:code-review-correctness` | `/code-review-correctness` | Bugs, regressions, edge cases, and behavioral correctness. |
| CodeReviewCompleteness | `$moraine-dev:code-review-completeness` | `/code-review-completeness` | Whether the PR satisfies its stated objectives and linked requirements. |
| CodeReviewSecurityReview | `$moraine-dev:code-review-security-review` | `/code-review-security-review` | Security, secrets, dependency risk, injection, and trust boundaries. |
| CodeReviewYAGNI | `$moraine-dev:code-review-yagni` | `/code-review-yagni` | Overengineering, premature generalization, and speculative defenses. |
| CodeReviewScope | `$moraine-dev:code-review-scope` | `/code-review-scope` | Scope control, unrelated changes, and review-lane separation. |

Historical and active session lookup does not need a skill. Use the Moraine MCP
tools directly: `search_sessions`, `list_sessions`, `open`, and
`file_attention`.

## Releases

Use `$moraine-dev:release X.Y.Z` in Codex, `/release X.Y.Z` in Kiro, or the
discovered `release` skill in another Agent Skills harness. It owns the release
goal, version bump, release PR, annotated tag, GitHub release notes,
tag-triggered workflow verification, and PyPI verification.

## Maintenance

Keep these skills focused on repeatable contributor actions. Do not add product
documentation, user guidance, or runtime behavior here.

When changing a shared skill, validate it against the Agent Skills format:

```bash
uv run --with pyyaml python \
  "${CODEX_HOME:-$HOME/.codex}/skills/.system/skill-creator/scripts/quick_validate.py" \
  plugins/moraine-dev/skills/<skill-name>
```

When changing Kiro integration, validate the agent schema and inspect loaded
context:

```bash
kiro-cli agent validate --path .kiro/agents/moraine-dev.json
kiro-cli agent list
kiro-cli --agent moraine-dev
# In the session, run /context show and verify the Moraine skills are listed.
```

Also keep `AGENTS.md` and this page aligned with the installed workflow names
so agents discover the workflows before starting work.
