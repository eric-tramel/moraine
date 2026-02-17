#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: maintenance/run_multiagent_repo_audit.sh [options]

Runs a 4-stage Codex maintenance workflow:
1) Sharded repository review with many gpt-5.3-codex-spark agents.
2) Dedup/compile of shard findings with gpt-5.3-codex.
3) One gpt-5.3-codex-spark session per deduped finding to create GitHub issues.
4) One gpt-5.3-codex xhigh session to link related issues via blocked/blocks/relates.

Options:
  --shards N                  Number of review shards (default: 10)
  --review-parallel N         Max concurrent review agents (default: 5)
  --issue-parallel N          Max concurrent issue-creator agents (default: 3)
  --run-dir PATH              Artifact directory (default: /tmp/moraine-maintenance-<timestamp>)
  --max-files N               Limit number of source files (default: 0 = all)
  --sandbox-mode MODE         Codex sandbox mode:
                                bypass (default), read-only, workspace-write, danger-full-access
  --review-model MODEL        Review model (default: gpt-5.3-codex-spark)
  --dedupe-model MODEL        Dedupe model (default: gpt-5.3-codex)
  --issue-model MODEL         Issue creation model (default: gpt-5.3-codex-spark)
  --relationship-model MODEL  Relationship linker model (default: gpt-5.3-codex)
  --relationship-effort LVL   Relationship linker effort:
                                minimal|low|medium|high|xhigh (default: xhigh)
  --dry-run                   Do not create GitHub issues (still generates issue prompts)
  --yes                       Skip confirmation prompt before creating issues
  -h, --help                  Show this help text
EOF
}

log() {
  printf '[%s] %s\n' "$(date +'%Y-%m-%d %H:%M:%S')" "$*"
}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

wait_for_slot() {
  local max_jobs="$1"
  while [ "$(jobs -pr | wc -l | tr -d ' ')" -ge "$max_jobs" ]; do
    sleep 1
  done
}

SHARDS=10
REVIEW_PARALLEL=5
ISSUE_PARALLEL=3
SANDBOX_MODE="bypass"
REVIEW_MODEL="gpt-5.3-codex-spark"
DEDUPE_MODEL="gpt-5.3-codex"
ISSUE_MODEL="gpt-5.3-codex-spark"
RELATIONSHIP_MODEL="gpt-5.3-codex"
RELATIONSHIP_EFFORT="xhigh"
RUN_DIR="/tmp/moraine-maintenance-$(date +'%Y%m%d-%H%M%S')"
MAX_FILES=0
DRY_RUN=0
AUTO_YES=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --shards)
      SHARDS="${2:-}"; shift 2 ;;
    --review-parallel)
      REVIEW_PARALLEL="${2:-}"; shift 2 ;;
    --issue-parallel)
      ISSUE_PARALLEL="${2:-}"; shift 2 ;;
    --run-dir)
      RUN_DIR="${2:-}"; shift 2 ;;
    --max-files)
      MAX_FILES="${2:-}"; shift 2 ;;
    --sandbox-mode)
      SANDBOX_MODE="${2:-}"; shift 2 ;;
    --review-model)
      REVIEW_MODEL="${2:-}"; shift 2 ;;
    --dedupe-model)
      DEDUPE_MODEL="${2:-}"; shift 2 ;;
    --issue-model)
      ISSUE_MODEL="${2:-}"; shift 2 ;;
    --relationship-model)
      RELATIONSHIP_MODEL="${2:-}"; shift 2 ;;
    --relationship-effort)
      RELATIONSHIP_EFFORT="${2:-}"; shift 2 ;;
    --dry-run)
      DRY_RUN=1; shift ;;
    --yes)
      AUTO_YES=1; shift ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      die "Unknown argument: $1" ;;
  esac
done

[[ "$SHARDS" =~ ^[1-9][0-9]*$ ]] || die "--shards must be a positive integer"
[[ "$REVIEW_PARALLEL" =~ ^[1-9][0-9]*$ ]] || die "--review-parallel must be a positive integer"
[[ "$ISSUE_PARALLEL" =~ ^[1-9][0-9]*$ ]] || die "--issue-parallel must be a positive integer"
[[ "$MAX_FILES" =~ ^[0-9]+$ ]] || die "--max-files must be 0 or a positive integer"
[[ "$RELATIONSHIP_EFFORT" =~ ^(minimal|low|medium|high|xhigh)$ ]] || die "--relationship-effort must be one of: minimal, low, medium, high, xhigh"

require_cmd git
require_cmd rg
require_cmd codex
require_cmd awk
require_cmd sed
require_cmd grep

ROOT_DIR="$(git rev-parse --show-toplevel 2>/dev/null || true)"
[ -n "$ROOT_DIR" ] || die "Run this script inside a git repository"
cd "$ROOT_DIR"

case "$SANDBOX_MODE" in
  bypass)
    SANDBOX_ARGS=(--dangerously-bypass-approvals-and-sandbox)
    ;;
  read-only|workspace-write|danger-full-access)
    SANDBOX_ARGS=(--sandbox "$SANDBOX_MODE")
    ;;
  *)
    die "Invalid --sandbox-mode: $SANDBOX_MODE"
    ;;
esac

mkdir -p "$RUN_DIR"/{shards,prompts,raw_issues,logs,issue_prompts,issue_logs,relationship}

log "Collecting source files..."
git ls-files \
  'apps/**' 'crates/**' 'web/**' 'sql/**' 'scripts/**' 'bin/**' \
  'Cargo.toml' 'Cargo.lock' '*.toml' \
  | rg -v '(^site/|/target/|node_modules/|\.min\.|\.png$|\.jpg$|\.jpeg$|\.gif$|\.svg$|\.ico$|\.woff2?$|\.ttf$|\.map$)' \
  | rg '\.(rs|ts|tsx|js|cjs|mjs|svelte|css|html|sql|sh|py|toml)$|(^Cargo\.toml$)|(^Cargo\.lock$)' \
  | sort -u > "$RUN_DIR/all_source_files.txt"

if [ "$MAX_FILES" -gt 0 ]; then
  head -n "$MAX_FILES" "$RUN_DIR/all_source_files.txt" > "$RUN_DIR/all_source_files.limited.txt"
  mv "$RUN_DIR/all_source_files.limited.txt" "$RUN_DIR/all_source_files.txt"
fi

TOTAL_FILES="$(wc -l < "$RUN_DIR/all_source_files.txt" | tr -d ' ')"
[ "$TOTAL_FILES" -gt 0 ] || die "No source files found for sharding"
log "Found $TOTAL_FILES source files."

log "Building $SHARDS review shards..."
awk -v n="$SHARDS" -v out="$RUN_DIR/shards" \
  '{f=sprintf("%s/shard_%02d.txt", out, (NR-1)%n); print > f}' \
  "$RUN_DIR/all_source_files.txt"

SHARD_FILES=()
while IFS= read -r shard_path; do
  SHARD_FILES+=("$shard_path")
done < <(find "$RUN_DIR/shards" -type f -name 'shard_*.txt' | sort)
[ "${#SHARD_FILES[@]}" -gt 0 ] || die "Shard generation failed"
log "Generated ${#SHARD_FILES[@]} non-empty shard files."

for shard_file in "${SHARD_FILES[@]}"; do
  shard_id="$(basename "$shard_file" .txt)"
  prompt_file="$RUN_DIR/prompts/${shard_id}.txt"
  {
    echo "You are a repository code-audit subagent (${shard_id})."
    echo "Review ONLY the files listed below."
    echo "Constraints:"
    echo "- Use only local filesystem reads from this repository."
    echo "- Do not use web search or remote URLs."
    echo "Output format (strict):"
    echo "- Output ONLY markdown list lines, no headings or prose."
    echo "- One issue per line: '- [P0|P1|P2] path:line - issue summary (why it matters); suggested direction'."
    echo "- Priority meanings: P0=critical, P1=high, P2=medium/low."
    echo "- If no issues are found, output exactly: '- [NONE] no material issues found'."
    echo
    echo "Files to review:"
    cat "$shard_file"
  } > "$prompt_file"
done

log "Launching shard review agents with model '$REVIEW_MODEL'..."
for shard_file in "${SHARD_FILES[@]}"; do
  shard_id="$(basename "$shard_file" .txt)"
  prompt_file="$RUN_DIR/prompts/${shard_id}.txt"
  out_file="$RUN_DIR/raw_issues/${shard_id}.md"
  log_file="$RUN_DIR/logs/${shard_id}.log"

  (
    if ! codex exec \
      --ephemeral \
      -m "$REVIEW_MODEL" \
      "${SANDBOX_ARGS[@]}" \
      --output-last-message "$out_file" \
      - < "$prompt_file" > "$log_file" 2>&1; then
      echo "- [P0] audit-runner:1 - ${shard_id} failed; inspect ${log_file}" > "$out_file"
    fi
  ) &

  wait_for_slot "$REVIEW_PARALLEL"
done
wait

for shard_file in "${SHARD_FILES[@]}"; do
  shard_id="$(basename "$shard_file" .txt)"
  out_file="$RUN_DIR/raw_issues/${shard_id}.md"
  if [ ! -s "$out_file" ]; then
    echo "- [P0] audit-runner:1 - ${shard_id} produced empty output; inspect logs" > "$out_file"
  fi
done

log "Compiling/deduplicating shard findings with '$DEDUPE_MODEL'..."
DEDUPE_PROMPT="$RUN_DIR/prompts/dedupe.txt"
{
  cat <<'EOF'
You are a repository audit findings compiler.

Task:
- Merge and deduplicate the raw shard findings below.
- Keep only concrete code findings.
- Remove exact and semantic duplicates.

Output format (strict):
- Output ONLY markdown issue lines in this exact format:
  - [P0|P1|P2] path:line - issue summary (why it matters); suggested direction
- Priority meanings: P0=critical, P1=high, P2=medium/low.
- Do NOT output [NONE] lines.
- If no findings remain, output exactly:
  - [NONE] no material issues found
EOF
  echo
  echo "Raw shard findings:"
  for f in "$RUN_DIR"/raw_issues/*.md; do
    echo
    echo "## $(basename "$f")"
    cat "$f"
  done
} > "$DEDUPE_PROMPT"

if ! codex exec \
  --ephemeral \
  -m "$DEDUPE_MODEL" \
  "${SANDBOX_ARGS[@]}" \
  --output-last-message "$RUN_DIR/REPORT_DEDUPED.md" \
  - < "$DEDUPE_PROMPT" > "$RUN_DIR/logs/dedupe.log" 2>&1; then
  die "Dedupe pass failed; inspect $RUN_DIR/logs/dedupe.log"
fi

mkdir -p "$ROOT_DIR/maintenance"
cp "$RUN_DIR/REPORT_DEDUPED.md" "$ROOT_DIR/maintenance/REPORT.md"

grep -E '^- \[(P0|P1|P2|CRITICAL|HIGH|MEDIUM|LOW)\] ' "$RUN_DIR/REPORT_DEDUPED.md" > "$RUN_DIR/final_issue_lines.raw.txt" || true
sed -E \
  -e 's/^- \[CRITICAL\]/- [P0]/' \
  -e 's/^- \[HIGH\]/- [P1]/' \
  -e 's/^- \[(MEDIUM|LOW)\]/- [P2]/' \
  "$RUN_DIR/final_issue_lines.raw.txt" > "$RUN_DIR/final_issue_lines.txt"
ISSUE_COUNT="$(wc -l < "$RUN_DIR/final_issue_lines.txt" | tr -d ' ')"
log "Deduped findings: $ISSUE_COUNT"

if [ "$ISSUE_COUNT" -eq 0 ]; then
  log "No findings to open as issues. Final report: maintenance/REPORT.md"
  exit 0
fi

if [ "$DRY_RUN" -eq 1 ]; then
  log "Dry run enabled; skipping GitHub issue creation."
  log "Run artifacts: $RUN_DIR"
  exit 0
fi

require_cmd gh
if ! gh auth status >/dev/null 2>&1; then
  die "GitHub CLI is not authenticated. Run: gh auth login"
fi

log "Capturing current GitHub issue style and labels..."
ISSUE_STYLE_FILE="$RUN_DIR/issue_style_reference.md"
ISSUE_LABELS_FILE="$RUN_DIR/issue_labels_reference.md"

{
  cat <<'EOF'
Current issue conventions:
- Priority uses P0/P1/P2 labels and matching title prefixes.
- Title format starts with [P0], [P1], or [P2].
- Body sections (in order):
  - ## Problem
  - ## Suggested Direction
  - ## Source
  - ## Original Item
EOF
  echo
  echo "Recent open issue examples:"
  if ! gh issue list --state open --limit 12 --json number,title,labels --jq \
    '.[] | "- #\(.number) \(.title)\n  labels: \(.labels | map(.name) | join(", "))"' ; then
    echo "- (unable to fetch open issue examples)"
  fi
} > "$ISSUE_STYLE_FILE"

{
  echo "Use only labels that already exist in this repository."
  echo
  echo "Available priority/domain labels:"
  if ! gh label list --limit 200 --json name,description --jq \
    '.[] | select(.name == "bug" or .name == "documentation" or .name == "P0" or .name == "P1" or .name == "P2" or (.name | startswith("area/"))) | "- \(.name): \(.description // "no description")"' ; then
    echo "- (unable to fetch labels)"
  fi
} > "$ISSUE_LABELS_FILE"

if [ "$AUTO_YES" -ne 1 ]; then
  printf 'Create %s GitHub issues now? [y/N]: ' "$ISSUE_COUNT"
  read -r answer
  case "$answer" in
    y|Y|yes|YES) ;;
    *)
      log "Aborted before issue creation. Deduped report is at maintenance/REPORT.md"
      exit 0
      ;;
  esac
fi

log "Launching per-issue creator agents with model '$ISSUE_MODEL'..."
i=0
while IFS= read -r issue_line; do
  i=$((i + 1))
  issue_id="$(printf 'issue_%03d' "$i")"
  issue_prompt="$RUN_DIR/issue_prompts/${issue_id}.txt"
  issue_out="$RUN_DIR/issue_logs/${issue_id}.md"
  issue_log="$RUN_DIR/issue_logs/${issue_id}.log"

  {
    echo "You are creating exactly one GitHub issue in this repository."
    echo "Finding:"
    echo "$issue_line"
    echo
    echo "Current issue style reference:"
    cat "$ISSUE_STYLE_FILE"
    echo
    echo "Current label reference:"
    cat "$ISSUE_LABELS_FILE"
    cat <<'EOF'

Requirements:
- Use `gh issue create` to create exactly one issue for this finding.
- Parse and preserve the finding priority token: `[P0]`, `[P1]`, or `[P2]`.
- Title must begin with the same priority token, e.g. `[P1] ...`.
- Keep title concise and specific, with path references in backticks when available.
- Body sections must be exactly and only:
  - `## Problem`
  - `## Suggested Direction`
  - `## Source`
  - `## Original Item`
- In `## Source`, include:
  - `- \`maintenance/REPORT.md\``
  - `- Generated from consolidated backlog`
- In `## Original Item`, include the original finding line verbatim as inline code.
- Labels:
  - Add the matching priority label (`P0`, `P1`, or `P2`).
  - Add `bug` for code/runtime findings, or `documentation` for docs-only findings.
  - Add one or more relevant `area/*` labels when there is a clear path-based match.
  - Use only labels that already exist in this repository.
- If issue creation succeeds, output exactly:
  - [CREATED] <issue_url>
- If issue creation fails, output exactly:
  - [FAILED] <reason>
EOF
  } > "$issue_prompt"

  (
    if ! codex exec \
      --ephemeral \
      -m "$ISSUE_MODEL" \
      "${SANDBOX_ARGS[@]}" \
      --output-last-message "$issue_out" \
      - < "$issue_prompt" > "$issue_log" 2>&1; then
      echo "- [FAILED] codex runner failure for ${issue_id}; inspect ${issue_log}" > "$issue_out"
    fi
  ) &

  wait_for_slot "$ISSUE_PARALLEL"
done < "$RUN_DIR/final_issue_lines.txt"
wait

cat "$RUN_DIR"/issue_logs/*.md > "$RUN_DIR/ISSUES_CREATED.md"
cp "$RUN_DIR/ISSUES_CREATED.md" "$ROOT_DIR/maintenance/ISSUES_CREATED.md"

CREATED_COUNT="$(grep -c '^- \[CREATED\] ' "$RUN_DIR/ISSUES_CREATED.md" || true)"
FAILED_COUNT="$(grep -c '^- \[FAILED\] ' "$RUN_DIR/ISSUES_CREATED.md" || true)"

grep -Eo 'https://github.com/[^[:space:]]+/issues/[0-9]+' "$RUN_DIR/ISSUES_CREATED.md" | sort -u > "$RUN_DIR/created_issue_urls.txt" || true
sed -E 's#.*/issues/([0-9]+).*#\1#' "$RUN_DIR/created_issue_urls.txt" | sort -n > "$RUN_DIR/created_issue_numbers.txt"
RELATIONSHIP_CANDIDATE_COUNT="$(wc -l < "$RUN_DIR/created_issue_numbers.txt" | tr -d ' ')"
RELATIONSHIP_REPORT="$RUN_DIR/ISSUE_RELATIONSHIPS.md"

if [ "$RELATIONSHIP_CANDIDATE_COUNT" -lt 2 ]; then
  echo "- [SKIPPED] relationship pass requires at least 2 created issues" > "$RELATIONSHIP_REPORT"
else
  RELATIONSHIP_CONTEXT_FILE="$RUN_DIR/relationship/issues_context.md"
  : > "$RELATIONSHIP_CONTEXT_FILE"
  while IFS= read -r issue_number; do
    [ -n "$issue_number" ] || continue
    if ! gh issue view "$issue_number" --json number,title,url,labels,body --jq \
      '"### Issue #\(.number)\nURL: \(.url)\nTitle: \(.title)\nLabels: \(.labels | map(.name) | join(", "))\nBody:\n\(.body)\n"' >> "$RELATIONSHIP_CONTEXT_FILE"; then
      printf '### Issue #%s\nUnable to fetch issue details.\n\n' "$issue_number" >> "$RELATIONSHIP_CONTEXT_FILE"
    fi
    echo >> "$RELATIONSHIP_CONTEXT_FILE"
  done < "$RUN_DIR/created_issue_numbers.txt"

  RELATIONSHIP_PROMPT="$RUN_DIR/prompts/relationship_linker.txt"
  {
    echo "You are the final GitHub issue relationship linker for this run."
    echo "Only consider relationships among the listed created issues."
    echo
    echo "Created issue URLs:"
    cat "$RUN_DIR/created_issue_urls.txt"
    echo
    echo "Created issue details snapshot:"
    cat "$RELATIONSHIP_CONTEXT_FILE"
    cat <<'EOF'

Task:
- Identify clear issue-to-issue relationships among these created issues.
- Allowed relationship labels are exactly:
  - blocked by
  - blocks
  - relates
- Use GitHub CLI (`gh issue view`, `gh issue edit`) to verify and apply updates.
- Limit edits to the listed created issues only.
- For each edited issue body, preserve existing content and add or update this top-level section:
  ## Relationships
  - blocked by #<issue_number>
  - blocks #<issue_number>
  - relates #<issue_number>
- Avoid duplicate relationship bullets.
- If you add a `blocks` relationship on one issue, ensure the counterpart issue has the inverse `blocked by`.
- If no relationship is justified, do not edit issues.

Final response format (strict):
- One line per relationship that was added:
  - [LINKED] #<issue_number> <blocked by|blocks|relates> #<issue_number>
- If no updates were made, output exactly:
  - [NONE] no relationship updates
- If the run fails, output exactly:
  - [FAILED] <reason>
EOF
  } > "$RELATIONSHIP_PROMPT"

  log "Launching final relationship linker with model '$RELATIONSHIP_MODEL' (effort '$RELATIONSHIP_EFFORT')..."
  if ! codex exec \
    --ephemeral \
    -m "$RELATIONSHIP_MODEL" \
    -c "model_reasoning_effort=\"$RELATIONSHIP_EFFORT\"" \
    "${SANDBOX_ARGS[@]}" \
    --output-last-message "$RELATIONSHIP_REPORT" \
    - < "$RELATIONSHIP_PROMPT" > "$RUN_DIR/logs/relationship.log" 2>&1; then
    echo "- [FAILED] relationship linker run failed; inspect $RUN_DIR/logs/relationship.log" > "$RELATIONSHIP_REPORT"
  fi
fi

cp "$RELATIONSHIP_REPORT" "$ROOT_DIR/maintenance/ISSUE_RELATIONSHIPS.md"
REL_LINKED_COUNT="$(grep -c '^- \[LINKED\] ' "$RELATIONSHIP_REPORT" || true)"
REL_FAILED_COUNT="$(grep -c '^- \[FAILED\] ' "$RELATIONSHIP_REPORT" || true)"

log "Issue creation complete: created=$CREATED_COUNT failed=$FAILED_COUNT"
log "Relationship linker summary: linked=$REL_LINKED_COUNT failed=$REL_FAILED_COUNT"
log "Report: maintenance/REPORT.md"
log "Issue results: maintenance/ISSUES_CREATED.md"
log "Relationship results: maintenance/ISSUE_RELATIONSHIPS.md"
log "Run artifacts: $RUN_DIR"
