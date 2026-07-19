---
name: release
description: Cut and publish Moraine releases from a version argument such as /release X.Y.Z in Kiro or a request to use $moraine-dev:release in Codex. Use when Codex or Kiro is asked to run Moraine's release process, bump release-managed versions, create and merge the release PR, push the vX.Y.Z tag, update GitHub release notes for a general audience, verify the release-moraine workflow, and confirm the PyPI moraine-cli package. In Codex, start or continue a durable goal for the release.
---

# Release

Run Moraine releases end to end with the shared `moraine-dev` contributor
workflow. Invoke it as `$moraine-dev:release X.Y.Z` in Codex or `/release
X.Y.Z` in Kiro. Both forms mean: publish `vX.Y.Z`, not just prepare a plan.

## Goal Contract

In Codex, make the release a durable goal before doing release work:

- If `create_goal` is available and there is no active goal, call it with:
  `Cut Moraine vX.Y.Z, including version bump PR, merged code, annotated repo tag, GitHub release notes, release workflow verification, and PyPI package verification.`
- If a goal already exists, continue inside it and keep it current with
  `update_plan`.
- Do not call `update_goal(status="complete")` until all public release
  evidence exists: merged PR, pushed tag, successful workflow, GitHub release
  body/assets, and PyPI `moraine-cli` artifacts.

In Kiro, maintain an explicit checklist in the session. Do not mark the release
complete until the same public release evidence exists.

## Preconditions

Normalize the argument first:

- `X.Y.Z` and `vX.Y.Z` both mean `VERSION=X.Y.Z` and `TAG=vX.Y.Z`.
- Refuse ambiguous input, missing versions, or a target older than the latest
  stable GitHub release.

Before editing:

1. Read the active `AGENTS.md`.
2. Use Moraine session search if available. Start broad, then narrow:
   - `release process Moraine PyPI GitHub tag`
   - `v0.5.4 release workflow pypi release notes`
   - `package-moraine-release release-moraine gh release edit`
3. Inspect `.github/workflows/release-moraine.yml`.
4. Read `.claude/skills/release-notes/SKILL.md` only as the house-format
   reference for the GitHub release body.
5. Verify tooling:
   - `gh auth status`
   - `git fetch origin --prune --tags`
   - `gh repo view --json nameWithOwner,defaultBranchRef,url`
6. Prove the target is unused:
   - `git tag --list "$TAG"` returns nothing.
   - `gh release view "$TAG"` fails with not found.
   - `curl -fsSL "https://pypi.org/pypi/moraine-cli/$VERSION/json"` fails.

## Branch And Context

Do release edits in a dedicated worktree from fresh `origin/main` unless the
user explicitly says otherwise. Set `branch` using the current harness's
convention:

| Harness | Branch value |
| --- | --- |
| Codex | `codex/release-$TAG` |
| Kiro | `kiro/release-$TAG` |

```bash
worktree_root="${MORAINE_WORKTREE_ROOT:-../moraine-worktrees}"
mkdir -p "$worktree_root"
git worktree add -b "$branch" "$worktree_root/release-$TAG" origin/main
```

Gather the changes since the previous stable release:

```bash
prev_tag="$(gh release list --limit 20 --json tagName,isDraft,isPrerelease -q \
  '[.[] | select(.isDraft==false and .isPrerelease==false) | .tagName] | .[0]')"
git log --oneline --decorate "$prev_tag"..origin/main
gh pr list --state merged --base main --limit 50 \
  --json number,title,url,mergedAt,body
```

Read PR bodies for user-visible changes. Release notes should explain what a
user can do or what is fixed, not just repeat commit titles.

## Version Bump

Run the bundled bump script from the release worktree root:

```bash
python3 plugins/moraine-dev/skills/release/scripts/bump-version.py "$VERSION"
```

Then inspect the diff. Expected version-only files are normally:

- `Cargo.lock`
- `bindings/python/moraine_conversations/Cargo.lock` path-dependency entries
- release-managed `apps/*/Cargo.toml`
- release-managed `crates/*/Cargo.toml`
- `.github/workflows/release-moraine.yml` example tag, if it still contains
  the old tag
- `plugins/moraine/.claude-plugin/plugin.json`
- `plugins/moraine/.codex-plugin/plugin.json`
- install docs only if they contain an explicit `MORAINE_INSTALL_VERSION`
  example for the old tag

Do not bump the package version in
`bindings/python/moraine_conversations/Cargo.toml`; it is a separate internal
Python extension package. Its lockfile must still track the release-managed
path dependencies.

## Validation

Always run:

```bash
git diff --check
cargo fmt --all -- --check
cargo test --workspace --locked
```

Use `$moraine-dev:moraine-sandbox-qa` in Codex or `/moraine-sandbox-qa` in Kiro
when the release includes ingest, MCP, monitor, ClickHouse schema,
source-format, or stack-behavior changes since the previous tag. If that
workflow is unavailable, follow the dev sandbox commands required by
`AGENTS.md`: capture the sandbox id with `--quiet`, run focused checks inside
it, and tear it down before reporting completion.

Typical sandbox checks:

```bash
id="$(scripts/dev/sandbox/moraine-sandbox up --quiet)"
scripts/dev/sandbox/moraine-sandbox status "$id"
# Run cargo/test commands inside the sandbox per AGENTS.md, then:
scripts/dev/sandbox/moraine-sandbox down "$id"
```

If `moraine-mcp-core` or the MCP tool surface changed, run the strongest
available MCP smoke test. Prefer `scripts/dev/sandbox/agent-smoke-e2e` when its
API key prerequisites are present; otherwise run focused MCP crate tests and
the project smoke tests that are available.

## Release PR

Create a focused release commit:

```bash
git add Cargo.lock apps crates .github README.md docs
git commit -m "chore(release): cut $TAG"
git push -u origin "$branch"
```

Open a PR to `main` titled `chore(release): cut $TAG`. The PR body must include:

- what version was bumped,
- a concise user-facing summary of changes since `prev_tag`,
- validation commands and outcomes,
- operational impact: tag push will run `release-moraine` and publish PyPI.

Wait for required checks. Merge only when checks pass. Use the repo's normal
merge style, then fetch `origin/main` and verify the merge commit:

```bash
gh pr view <number> --json state,mergedAt,mergeCommit,url
git fetch origin main --tags
git log --oneline -1 origin/main
```

Stop before tagging if the PR is not merged into `main`.

## Tag And Workflow

Tag the merge commit on `origin/main` with an annotated tag:

```bash
merge_sha="$(gh pr view <number> --json mergeCommit -q .mergeCommit.oid)"
git tag -a "$TAG" "$merge_sha" -m "$TAG"
git push origin "$TAG"
```

Then wait for the tag-triggered workflow:

```bash
run_id="$(gh run list --workflow release-moraine.yml --event push \
  --branch "$TAG" --limit 1 --json databaseId -q '.[0].databaseId')"
gh run watch "$run_id" --exit-status
gh run view "$run_id" --json status,conclusion,url,name,event,headBranch,headSha
```

The tag-triggered `publish-pypi` job publishes `moraine-cli` to PyPI
automatically. Manual dispatches default to `skip` and are not the normal
release path.

## GitHub Release Notes

Edit the GitHub release body only after the workflow has completed. Matrix jobs
can rewrite generated notes while they upload assets.

Use the house format from `.claude/skills/release-notes/SKILL.md`, adapted to
the release size:

- Patch release: concise headline, what was fixed, upgrade note only if needed,
  changelog at the bottom.
- Feature release: install block, "What's new", "Under the hood", platform
  support only if it changed, upgrade notes if needed, changelog at the bottom.
- Always dedupe generated changelog blocks. Keep exactly one `Full Changelog`
  compare link.

Write for a general Moraine user. Prefer "Moraine now..." and "You can..." over
implementation-first phrasing. Link PRs the first time they are mentioned.

Publish with:

```bash
gh release edit "$TAG" --notes-file /tmp/moraine-release-notes.md
```

Verify:

```bash
gh release view "$TAG" --json tagName,name,url,body,assets,publishedAt,targetCommitish
```

Expect six GitHub release assets: three platform bundles and three checksum
files, unless the workflow has intentionally changed.

## PyPI Verification

Verify the version-specific endpoint, the simple index, aggregate metadata, and
an install smoke:

```bash
curl -fsSL -H 'Cache-Control: no-cache' \
  "https://pypi.org/pypi/moraine-cli/$VERSION/json" | \
  python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["info"]["version"], len(d["urls"])); print("\n".join(sorted(f["filename"] for f in d["urls"])))'

curl -fsSL -H 'Cache-Control: no-cache' https://pypi.org/simple/moraine-cli/ |
  rg "moraine_cli-$VERSION"

curl -fsSL -H 'Cache-Control: no-cache' https://pypi.org/pypi/moraine-cli/json |
  python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["info"]["version"]); print(len(d["releases"].get("'"$VERSION"'", [])))'

uvx --refresh-package moraine-cli --from "moraine-cli==$VERSION" moraine --version
```

Expected PyPI files are three wheels plus the stub sdist. PyPI metadata can lag
for a short time; retry with no-cache headers before declaring failure.

## Stop Conditions

Stop and report clearly if:

- target tag or PyPI version already exists,
- `gh` is unavailable or unauthenticated,
- version bump diff touches unexpected files,
- validation fails,
- the release PR cannot be merged,
- the tag workflow fails,
- the GitHub release is missing assets,
- PyPI does not publish the expected files after reasonable retries.

If a tag has already been pushed, do not delete or recreate it without explicit
user instruction.

## Final Report

End with the concrete public evidence:

- merged release PR URL,
- tag and tagged commit,
- release workflow run URL and conclusion,
- GitHub release URL and asset count,
- PyPI version URL and file count,
- `uvx` smoke output,
- the final general-audience release notes section or a concise excerpt plus
  the release link.
