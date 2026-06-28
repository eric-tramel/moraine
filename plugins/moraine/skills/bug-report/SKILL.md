---
name: bug-report
description: Help prepare and, only with explicit user approval, file a sanitized Moraine bug report for reproducible defects in Moraine itself.
---

# Bug Report

Use this skill when the user hit a likely Moraine defect and wants help turning
it into a useful GitHub issue. A Moraine bug is a reproducible problem in the
Moraine CLI, ingest, monitor, MCP server, setup flow, packaging, documentation,
or bundled plugins.

Do not use this skill for general support, one-off local setup help, unrelated
agent-harness issues, or private debugging that cannot be generalized into a
Moraine repository issue. Troubleshoot those locally instead, and file a report
only if the evidence points to a fixable Moraine defect.

## Destination

The Moraine repository is:

```text
https://github.com/eric-tramel/moraine
```

New bug reports go through GitHub Issues:

```text
https://github.com/eric-tramel/moraine/issues/new
```

If a GitHub issue tool, browser, or `gh` is available, use it only after the
user explicitly confirms the final redacted report. If no posting tool is
available, provide the issue URL and the prepared report body.

## Consent Boundary

Never silently post an issue on the user's behalf. Even if the user says "file
it", first show the final sanitized title and body, then ask for explicit
confirmation before creating the issue. If the user declines, stop after
providing the draft.

Do not upload logs, configs, screenshots, session transcripts, or command output
without the same explicit confirmation.

## Privacy Rules

All bug reports must anonymize PII and host-specific details before they leave
the user's machine.

Redact or generalize:

- Usernames, hostnames, emails, organization names, repo names, client names, and
  internal project names.
- Exact absolute paths such as home directories, workspace roots, socket paths,
  and temp directories.
- Secrets, tokens, API keys, cookies, auth headers, database URLs, and credentials.
- Private prompts, agent transcripts, file contents, logs, or screenshots unless
  the user explicitly approves a minimal sanitized excerpt.
- Exact host environment details that fingerprint the machine. Prefer broad
  labels like "macOS on Apple Silicon", "Linux x86_64", "Homebrew install", or
  "uv tool install" instead of full hardware, hostname, local account, or full
  directory layouts.

Use placeholders such as `<home>`, `<repo>`, `<project>`, `<host>`,
`<workspace>`, `<token>`, and `<private transcript omitted>`.

## Triage

Before drafting a report, determine whether the issue belongs in the Moraine
repo:

1. Identify the affected Moraine surface: CLI, setup, ingest source, monitor,
   MCP search, ClickHouse schema, packaging, documentation, Codex plugin,
   Claude Code plugin, Hermes plugin, or another bundled integration.
2. Check whether the behavior is reproducible or has enough evidence to debug.
3. Separate Moraine defects from user-specific environment problems. A missing
   local dependency, local permissions issue, custom shell setup, broken external
   harness install, or private repo configuration is usually support unless
   Moraine handles it incorrectly or gives a misleading failure.
4. Prefer a small sanitized reproducer over broad environment dumps.

## Report Structure

Draft reports in this shape:

```markdown
## Summary

One or two sentences describing the Moraine bug and affected surface.

## Affected Area

- Moraine component: CLI | setup | ingest | monitor | MCP | packaging | docs | plugin
- Harness, if relevant: Codex | Claude Code | Hermes | OpenCode | Cursor | Kimi CLI | Pi Coding Agent | other
- Install method, if relevant: release bundle | uv tool | Homebrew | source checkout | unknown

## Version

- Moraine version: output of `moraine --version`, if available
- Broad platform: sanitized OS and architecture category only

## Steps To Reproduce

1. Minimal first step.
2. Minimal second step.
3. Command or action that triggers the bug, with paths and private names replaced.

## Expected Behavior

What Moraine should have done.

## Actual Behavior

What happened instead, including the sanitized error message or symptom.

## Sanitized Evidence

Short redacted excerpts only. Replace private values with placeholders.

## Workaround

Known workaround, if any.

## Notes

Any suspected regression range, related issue, or additional context that does
not expose private data.
```

## Evidence Collection

Ask before running commands that may read local state. Prefer narrow commands
whose output can be summarized and redacted, such as:

```bash
moraine --version
moraine status
moraine logs --lines 100
```

When logs are useful, include only the shortest sanitized excerpt that explains
the failure. Do not include full session content or full config files. Summarize
config shape instead of posting exact values.

If prior agent context matters and Moraine MCP tools are available, use the
current harness's Moraine search tools to recover the relevant session or error,
then apply the same redaction rules before drafting the issue.

## Posting Workflow

1. Draft a sanitized title and body using the report structure.
2. Point out any assumptions or missing reproduction details.
3. Ask the user whether they want to post it to GitHub.
4. If they say yes, show the final redacted report and ask for explicit posting
   confirmation.
5. Create the GitHub issue only after that confirmation, then return the issue
   URL.
6. If they do not confirm, leave them with the draft and the new-issue URL.
