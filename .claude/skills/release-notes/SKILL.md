---
name: release-notes
description: Rewrite a moraine GitHub release body in the house format — one-sentence pitch, install block, usage-focused "what's new" sections with PR links and optional screenshots, a platform-support table, upgrade notes, and a single deduped auto-generated changelog at the bottom. Use when the user says "/release-notes v0.4.2", "polish the v0.4.2 release notes", or asks to prepare release notes for a tag. NOT auto-invoked by the model.
disable-model-invocation: true
---

# release-notes

You are rewriting a GitHub release body in moraine's house format. The input
is a tag (e.g. `v0.4.2`); the output is an updated release body published via
`gh release edit <tag> --notes-file <path>`.

Your goal is **usage-focused, not commit-focused**. A reader skimming the
release page should understand in 30 seconds: how do I install this, what new
thing can I do with it, and am I supposed to do anything when I upgrade?

## Usage

`/release-notes v0.4.2` — rewrite the v0.4.2 release body.

If the tag doesn't exist on GitHub yet, stop and explain. Do not create a
release; the release is cut by `.github/workflows/release-moraine.yml` on tag
push.

## Process

### 1. Gather context

```bash
# Current (auto-generated) body. It's usually duplicated 3-6× because the
# release matrix runs softprops/action-gh-release from each target job, each
# time with generate_release_notes: true. You will dedupe in step 3.
gh release view "$TAG" --json body -q .body > /tmp/release-body.current.md

# Previous release tag, for the compare link and upgrade section.
prev_tag="$(gh release list --limit 20 --json tagName,isDraft,isPrerelease -q \
  '[.[] | select(.isDraft==false and .isPrerelease==false and .tagName!=env.TAG) | .tagName] | .[0]')"

# Full list of PRs merged since the previous tag, for context.
gh pr list --state merged --search "merged:>=<prev-tag-date>" --json number,title,url
# or simpler — trust the auto-generated list and just read it.
```

Also read:

- `scripts/build-python-wheels.py` — keep the "Platform support" table in
  sync with `TARGET_TO_WHEEL_PLATFORM`.
- Any PR body that introduces a user-visible feature (via `gh pr view <n>
  --json body -q .body`). Prefer the PR's own summary over inventing one.

### 2. Decide the shape of this release

Before writing, classify the release:

- **Landmark release** — new install path, big UI change, or a breaking
  change. Deserves a full-body treatment with install section, screenshots,
  upgrade notes. v0.4.2 is the canonical example.
- **Patch release** — bug fixes only. Skip "🚀 Install", "🐧 Platform
  support", and "⬆️ Upgrading". The "🛠 Under the hood" section becomes the
  whole body. Still dedupe the auto-generated changelog.
- **Minor release between the two** — include "🆕 What's new" but drop
  sections that didn't change. Don't pad.

Don't force a section that has nothing to say.

### 3. Dedupe the changelog

Every matrix job appended its own `## What's Changed ... **Full Changelog**`
block. Keep exactly one. The simplest path: open the current body in an
editor, delete all but the first block, move it to the bottom under a
`## Changelog` heading.

Check the dedupe with:

```bash
grep -c '^## What.s Changed' /tmp/release-body.current.md   # should be 1 after dedupe
grep -c 'Full Changelog'     /tmp/release-body.current.md   # should be 1
```

### 4. Write the new sections

Template (copy/paste, then fill in):

```markdown
# moraine <TAG>

<one-sentence pitch — what is the headline story of this release?>

## 🚀 Install

```bash
uv tool install moraine-cli
moraine up
```

<one short paragraph expanding on the install command, plus the upgrade
recipe for existing users if relevant>

## 🆕 What's new

### <feature title> ([#<PR>](<PR url>))

<2-4 sentences describing the user-visible outcome, in usage-first language.
Start with what a user does or sees, not with what you changed.>

<optional: side-by-side screenshot table; see "Screenshots" below>

<repeat per major feature; 2-4 subsections is ideal, 5+ feels padded>

## 🛠 Under the hood

- [#<PR>](<url>) — one line, user-facing angle if any
- ...

## 🐧 Platform support

| Platform | Wheel | Notes |
|---|---|---|
| Linux x86_64 | `manylinux_2_28_x86_64` | glibc 2.28+ (Debian 12+, Ubuntu 20.04+, RHEL 9+, AL2023) |
| Linux aarch64 | `manylinux_2_28_aarch64` | same floor |
| macOS Apple Silicon | `macosx_11_0_arm64` | macOS 11.0+ |
| macOS Intel | — | not published; use `scripts/install.sh` |
| Windows | — | not supported yet |

(Pull the tags from `scripts/build-python-wheels.py::TARGET_TO_WHEEL_PLATFORM`.
If the platform table hasn't changed since last release, skip this section.)

## ⬆️ Upgrading from <prev-tag>

```bash
moraine down
uv tool upgrade moraine-cli   # or: uv tool install --reinstall moraine-cli
moraine up && moraine status
```

<only include this section if there's something the user has to do. If it's
just a bug-fix release, skip it.>

---

## Changelog

<the single deduped auto-generated list from step 3 goes here>

**Full Changelog**: https://github.com/eric-tramel/moraine/compare/<prev-tag>...<TAG>
```

### 5. Tone + style rules

- **Usage-first sentences.** "You can now see tool-call latencies in the
  monitor" → good. "Adds a new flamegraph component" → bad.
- **Link every PR** the first time you mention a feature: `([#247](url))`.
- **No hedging.** Drop "essentially", "basically", "just". Say what it does.
- **Quote sparingly.** Never copy more than a few lines from a PR body
  verbatim; always rewrite for the release audience.
- **Emojis anchor the major sections only** (🚀 🆕 🛠 🐧 ⬆️). Don't sprinkle
  them through prose.
- **Call out breaking changes in the section they happen in**, with the
  word "removed" or "requires" — not buried in the changelog.
- **Honest platform support.** If Windows isn't supported, the table says
  "not supported yet", not "coming soon".
- **Don't promise canary-week timing.** The runbook in
  `docs/operations/pypi-release.md` covers that; the release notes are for
  the release itself.

### 6. Screenshots (optional, for UX-impacting releases)

Skip this section for patch releases. Include for any release where the
monitor UI visibly changed or install/CLI flows have a step the user can
see. Screenshots are especially worth the effort for the "landmark release"
class above.

**Preconditions:**

- `moraine up` is running on the host (check `curl -sf http://127.0.0.1:8080/
  >/dev/null && echo OK`). If not, stop and ask the user to start it.
- `web/monitor/node_modules/playwright` exists (part of the repo's dev
  install). If not, run `cd web/monitor && bun install --frozen-lockfile`.

**Capture:**

```bash
node .claude/skills/release-notes/scripts/shoot-monitor.mjs \
    "<session title substring>" /tmp/moraine-shots
```

The script drives headless chromium, waits for the Live Analytics charts +
sessions list to render, clicks the first session card whose visible title
contains `<session title substring>` (case-insensitive), and writes three
PNGs:

- `01-sessions-landing.png` — the Sessions surface with Live Analytics
- `02-session-detail.png` — transcript pane for the clicked session
- `03-session-flamegraph.png` — flamegraph tab for the same session

Light theme is Playwright's default; use it (it reads better in release
notes than dark chrome against the session rows).

**Sensitivity review — mandatory before uploading:**

1. Read every visible session title in `01-sessions-landing.png`. Session
   titles are **the first user prompt of that session, verbatim.** They
   often mention internal project names, ticket numbers, or colleague names.
2. For `02-session-detail.png` and `03-session-flamegraph.png`, look for:
   - Absolute file paths containing `/Users/<name>/` — reveals username +
     dir layout
   - Internal project names or acronyms in user prompts or tool args
   - Third-party ticket IDs (Jira, Linear, etc.)
   - Copy/pasted secrets (tokens, keys) in tool-call args — rare but
     possible
3. **Show the user the screenshots and flag specific concerns before
   uploading.** The user decides whether to ship them, crop them, or pick a
   different session. Don't self-approve.

**Pick a session whose content is already public** (referenced by number in a
public GitHub issue/PR, or directly about moraine's open-source
development). Sessions about unrelated work are tempting because they're
visually busy, but they're usually riskier.

**Upload + embed:**

```bash
# Rename for a stable, human-legible asset name on the release page.
cp /tmp/moraine-shots/02-session-detail.png \
   /tmp/moraine-shots/moraine-monitor-sessions-transcript.png
cp /tmp/moraine-shots/03-session-flamegraph.png \
   /tmp/moraine-shots/moraine-monitor-sessions-flamegraph.png

gh release upload "$TAG" \
  /tmp/moraine-shots/moraine-monitor-sessions-transcript.png \
  /tmp/moraine-shots/moraine-monitor-sessions-flamegraph.png \
  --clobber
```

Embed side-by-side inside the relevant `### <feature>` section of the body.
Tables render cleaner than raw HTML in GitHub markdown and each image can
link to its own full-size asset for click-to-zoom:

```markdown
| Transcript view | Flamegraph view |
|---|---|
| [![Session transcript](https://github.com/eric-tramel/moraine/releases/download/<TAG>/moraine-monitor-sessions-transcript.png)](https://github.com/eric-tramel/moraine/releases/download/<TAG>/moraine-monitor-sessions-transcript.png) | [![Session flamegraph](https://github.com/eric-tramel/moraine/releases/download/<TAG>/moraine-monitor-sessions-flamegraph.png)](https://github.com/eric-tramel/moraine/releases/download/<TAG>/moraine-monitor-sessions-flamegraph.png) |
```

Release-asset URLs are stable — they won't rot when you clean up issue
attachments. They render inline because GitHub serves them with the right
`Content-Type`.

### 7. Publish

```bash
gh release edit "$TAG" --notes-file /tmp/release-body.new.md
```

Then verify:

```bash
# Count should drop to 1 of each
gh release view "$TAG" --json body -q .body | grep -c '^## What.s Changed'
gh release view "$TAG" --json body -q .body | grep -c 'Full Changelog'

# Total body length should be ~5-8k (v0.4.2 settled at 7.1k). If it's
# over 10k, you probably didn't dedupe; under 2k, you probably skipped
# sections the release needed.
gh release view "$TAG" --json body -q .body | wc -c
```

Show the user the final URL (`https://github.com/eric-tramel/moraine/releases/tag/<TAG>`)
and the body length. They'll spot-check.

## Reference: the v0.4.2 release

Use <https://github.com/eric-tramel/moraine/releases/tag/v0.4.2> as the
canonical example of this format. It was cut with this skill's predecessor
workflow and is the specimen the format was distilled from.
