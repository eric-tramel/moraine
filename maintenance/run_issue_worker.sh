#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: maintenance/run_issue_worker.sh [options]

Select one highest-priority open GitHub issue that matches a comma-separated
label union, claim it as in-progress, spawn a fresh worktree, and launch a
Codex execution to implement/fix and open a PR.

Options:
  --tag-union "a,b,c"         Required. Comma-separated label union.
  --status-label LABEL        In-progress lock label (default: status/in-progress)
  --model MODEL               Codex model (default: gpt-5.3-codex)
  --effort LEVEL              Reasoning effort (default: xhigh)
  --base BRANCH               Base branch for worktree/PR (default: main)
  --worktrees-dir PATH        Worktree root relative to repo (default: .worktrees)
  --issue-limit N             Number of open issues to fetch (default: 200)
  --dry-run                   Select issue and print outcome only
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

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

slugify() {
  local raw="$1"
  local slug
  slug="$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/-/g; s/^-+//; s/-+$//; s/-+/-/g')"
  printf '%s' "$slug"
}

fail_run() {
  local step="$1"
  local message="$2"

  if [ "$CLAIMED" -eq 1 ] && [ -n "${ISSUE_NUMBER:-}" ]; then
    gh issue comment --repo "$REPO" "$ISSUE_NUMBER" --body "$(cat <<EOF
Automation run \`$RUN_ID\` failed at step \`$step\`.

Reason: $message
Branch: \`${BRANCH_NAME:-not-created}\`
Worktree: \`${WORKTREE_PATH:-not-created}\`
EOF
)" >/dev/null 2>&1 || true
  fi

  die "$message"
}

TAG_UNION=""
STATUS_LABEL="status/in-progress"
MODEL="gpt-5.3-codex"
EFFORT="xhigh"
BASE_BRANCH="main"
WORKTREES_DIR=".worktrees"
ISSUE_LIMIT=200
DRY_RUN=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --tag-union)
      TAG_UNION="${2:-}"; shift 2 ;;
    --status-label)
      STATUS_LABEL="${2:-}"; shift 2 ;;
    --model)
      MODEL="${2:-}"; shift 2 ;;
    --effort)
      EFFORT="${2:-}"; shift 2 ;;
    --base)
      BASE_BRANCH="${2:-}"; shift 2 ;;
    --worktrees-dir)
      WORKTREES_DIR="${2:-}"; shift 2 ;;
    --issue-limit)
      ISSUE_LIMIT="${2:-}"; shift 2 ;;
    --dry-run)
      DRY_RUN=1; shift ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      die "Unknown argument: $1" ;;
  esac
done

[ -n "$TAG_UNION" ] || die "--tag-union is required"
[[ "$ISSUE_LIMIT" =~ ^[1-9][0-9]*$ ]] || die "--issue-limit must be a positive integer"
[[ "$EFFORT" =~ ^(minimal|low|medium|high|xhigh)$ ]] || die "--effort must be one of: minimal, low, medium, high, xhigh"

require_cmd git
require_cmd gh
require_cmd codex
require_cmd jq
require_cmd rg
require_cmd sed
require_cmd awk

ROOT_DIR="$(git rev-parse --show-toplevel 2>/dev/null || true)"
[ -n "$ROOT_DIR" ] || die "Run this script inside a git repository"
cd "$ROOT_DIR"

if ! gh auth status >/dev/null 2>&1; then
  die "GitHub CLI is not authenticated. Run: gh auth login"
fi

REPO="$(gh repo view --json nameWithOwner --jq .nameWithOwner)"
RUN_ID="$(date -u +'%Y%m%dT%H%M%SZ')-$$"
RUN_DIR="$(mktemp -d "/tmp/cortex-issue-worker-${RUN_ID}.XXXXXX")"
cleanup() {
  rm -rf "$RUN_DIR"
}
trap cleanup EXIT

CLAIMED=0
ISSUE_NUMBER=""
ISSUE_URL=""
BRANCH_NAME=""
WORKTREE_PATH=""

IFS=',' read -r -a RAW_TAGS <<< "$TAG_UNION"
TAGS=()
for raw_tag in "${RAW_TAGS[@]}"; do
  cleaned_tag="$(trim "$raw_tag")"
  if [ -n "$cleaned_tag" ]; then
    TAGS+=("$cleaned_tag")
  fi
done
[ "${#TAGS[@]}" -gt 0 ] || die "--tag-union did not contain any valid labels"
printf '%s\n' "${TAGS[@]}" > "$RUN_DIR/tag_union.txt"
TAGS_JSON="$(printf '%s\n' "${TAGS[@]}" | jq -R . | jq -s .)"

log "Fetching open issues from $REPO..."
gh issue list --repo "$REPO" --state open --limit "$ISSUE_LIMIT" \
  --json number,title,url,labels,updatedAt > "$RUN_DIR/issues_raw.json"

jq --arg status_label "$STATUS_LABEL" --argjson tags "$TAGS_JSON" '
  map({
    number,
    title,
    url,
    updatedAt,
    labels: (.labels | map(.name))
  })
  | map(.matches_union = ([.labels[]] | any(. as $l | $tags | index($l))))
  | map(select(.matches_union))
  | map(select((.labels | index($status_label)) | not))
  | map(.priority_rank = (
      if (.labels | index("P0")) then 0
      elif (.labels | index("P1")) then 1
      elif (.labels | index("P2")) then 2
      else 3
      end
    ))
  | sort_by(.priority_rank, .number)
' "$RUN_DIR/issues_raw.json" > "$RUN_DIR/issues_candidates.json"

CANDIDATE_COUNT="$(jq 'length' "$RUN_DIR/issues_candidates.json")"
if [ "$CANDIDATE_COUNT" -eq 0 ]; then
  die "No matching open issues found for union: ${TAGS[*]}"
fi

jq '.[0]' "$RUN_DIR/issues_candidates.json" > "$RUN_DIR/issue_selected.json"
ISSUE_NUMBER="$(jq -r '.number' "$RUN_DIR/issue_selected.json")"
ISSUE_TITLE="$(jq -r '.title' "$RUN_DIR/issue_selected.json")"
ISSUE_URL="$(jq -r '.url' "$RUN_DIR/issue_selected.json")"
ISSUE_PRIORITY="$(jq -r '.priority_rank' "$RUN_DIR/issue_selected.json")"

log "Selected issue #$ISSUE_NUMBER (priority rank: $ISSUE_PRIORITY): $ISSUE_TITLE"

if [ "$DRY_RUN" -eq 1 ]; then
  log "Dry-run selected issue: $ISSUE_URL"
  exit 0
fi

require_cmd cargo

log "Ensuring status label '$STATUS_LABEL' exists..."
if ! gh label list --repo "$REPO" --limit 200 --json name --jq '.[].name' | grep -Fxq "$STATUS_LABEL"; then
  gh label create "$STATUS_LABEL" --repo "$REPO" --color "0e8a16" \
    --description "Issue is actively being worked by automation" >/dev/null || true
fi
if ! gh label list --repo "$REPO" --limit 200 --json name --jq '.[].name' | grep -Fxq "$STATUS_LABEL"; then
  fail_run "ensure_status_label" "Unable to create or find label '$STATUS_LABEL'."
fi

gh issue view --repo "$REPO" "$ISSUE_NUMBER" --json labels > "$RUN_DIR/issue_preclaim.json"
if jq -e --arg status "$STATUS_LABEL" '.labels | map(.name) | index($status) != null' \
  "$RUN_DIR/issue_preclaim.json" >/dev/null; then
  fail_run "preclaim_check" "Issue #$ISSUE_NUMBER was claimed concurrently before locking."
fi

log "Claiming issue #$ISSUE_NUMBER with '$STATUS_LABEL'..."
if ! gh issue edit --repo "$REPO" "$ISSUE_NUMBER" --add-label "$STATUS_LABEL" >/dev/null; then
  fail_run "claim_issue" "Failed to add '$STATUS_LABEL' to issue #$ISSUE_NUMBER."
fi
CLAIMED=1

CLAIM_COMMENT="$(cat <<EOF
Automation worker run \`$RUN_ID\` claimed this issue.

- Tag union: \`$(IFS=,; printf '%s' "${TAGS[*]}")\`
- Selected priority: rank \`$ISSUE_PRIORITY\` (P0=0, P1=1, P2=2, unlabeled=3)
- Status label: \`$STATUS_LABEL\`
EOF
)"
gh issue comment --repo "$REPO" "$ISSUE_NUMBER" --body "$CLAIM_COMMENT" >/dev/null || true

gh issue view --repo "$REPO" "$ISSUE_NUMBER" --json labels > "$RUN_DIR/issue_postclaim.json"
if ! jq -e --arg status "$STATUS_LABEL" '.labels | map(.name) | index($status) != null' \
  "$RUN_DIR/issue_postclaim.json" >/dev/null; then
  fail_run "verify_claim" "Issue #$ISSUE_NUMBER does not show '$STATUS_LABEL' after claim."
fi

ISSUE_SLUG="$(slugify "$ISSUE_TITLE")"
[ -n "$ISSUE_SLUG" ] || ISSUE_SLUG="issue"
ISSUE_SLUG="${ISSUE_SLUG:0:48}"
RUN_SUFFIX="${RUN_ID//[^0-9a-zA-Z]/}"
RUN_SUFFIX="${RUN_SUFFIX:0:8}"

BRANCH_BASE="issue/${ISSUE_NUMBER}-${ISSUE_SLUG}"
BRANCH_NAME="$BRANCH_BASE"

if git show-ref --verify --quiet "refs/heads/$BRANCH_NAME" || \
  [ -n "$(git ls-remote --heads origin "$BRANCH_NAME")" ]; then
  BRANCH_NAME="${BRANCH_BASE}-${RUN_SUFFIX}"
fi

WORKTREE_REL="${WORKTREES_DIR%/}/issue-${ISSUE_NUMBER}-${ISSUE_SLUG}"
WORKTREE_PATH="$ROOT_DIR/$WORKTREE_REL"
if [ -e "$WORKTREE_PATH" ]; then
  WORKTREE_PATH="${WORKTREE_PATH}-${RUN_SUFFIX}"
fi
mkdir -p "$(dirname "$WORKTREE_PATH")"

log "Creating worktree at $WORKTREE_PATH on branch $BRANCH_NAME..."
if ! git fetch origin "$BASE_BRANCH" >/dev/null 2>&1; then
  fail_run "fetch_base" "Failed to fetch origin/$BASE_BRANCH."
fi
if ! git worktree add -b "$BRANCH_NAME" "$WORKTREE_PATH" "origin/$BASE_BRANCH" >/dev/null; then
  fail_run "create_worktree" "Failed to create worktree for branch '$BRANCH_NAME'."
fi

gh issue view --repo "$REPO" "$ISSUE_NUMBER" --json number,title,body,url,labels > "$RUN_DIR/issue_details.json"

PROMPT_FILE="$RUN_DIR/prompt.txt"
cat > "$PROMPT_FILE" <<EOF
You are an autonomous coding agent working in a dedicated git worktree.

Repository: $REPO
Issue number: #$ISSUE_NUMBER
Issue URL: $ISSUE_URL
Target branch: $BRANCH_NAME
Base branch: $BASE_BRANCH
Worktree path: $WORKTREE_PATH
Run id: $RUN_ID

Issue details (JSON):
$(cat "$RUN_DIR/issue_details.json")

Execution requirements:
1. Read AGENTS.md and follow repository constraints.
2. Implement a complete fix for issue #$ISSUE_NUMBER in this worktree.
3. Run and pass this mandatory gate exactly:
   cargo test --workspace --locked
4. Ensure the worktree is clean (all intended changes committed).
5. Create one focused commit with an imperative, scoped subject.
6. Push branch:
   git push -u origin $BRANCH_NAME
7. Create a PR to $BASE_BRANCH with:
   - clear summary and validation steps
   - "Closes #$ISSUE_NUMBER" in the body
8. Comment on issue #$ISSUE_NUMBER with the PR URL.

Output format in final response (strict, lines only):
RESULT: <success|failure>
PR_URL: <url-or-none>
COMMIT_SHA: <sha-or-none>
TEST_COMMAND: cargo test --workspace --locked
TEST_RESULT: <pass|fail|not-run>
NOTES: <one-line summary>
EOF

log "Launching Codex run with model '$MODEL' (effort '$EFFORT')..."
if ! codex exec \
  --ephemeral \
  --full-auto \
  --sandbox workspace-write \
  -m "$MODEL" \
  -c "model_reasoning_effort=\"$EFFORT\"" \
  --cd "$WORKTREE_PATH" \
  --output-last-message "$RUN_DIR/final.md" \
  - < "$PROMPT_FILE" > "$RUN_DIR/codex.log" 2>&1; then
  fail_run "codex_exec" "Codex execution failed."
fi

log "Running verification gate locally: cargo test --workspace --locked..."
if ! (cd "$WORKTREE_PATH" && cargo test --workspace --locked > "$RUN_DIR/post_tests.log" 2>&1); then
  fail_run "post_run_tests" "Post-run verification failed."
fi

AHEAD_COUNT="$(git -C "$WORKTREE_PATH" rev-list --count "origin/$BASE_BRANCH..HEAD" || echo 0)"
if [ "${AHEAD_COUNT:-0}" -lt 1 ]; then
  fail_run "commit_check" "No commits ahead of origin/$BASE_BRANCH were created."
fi

if [ -n "$(git -C "$WORKTREE_PATH" status --porcelain)" ]; then
  fail_run "clean_tree_check" "Worktree has uncommitted changes after Codex run."
fi

if [ -z "$(git ls-remote --heads origin "$BRANCH_NAME")" ]; then
  fail_run "push_check" "Remote branch origin/$BRANCH_NAME not found."
fi

PR_URL="$(gh pr list --repo "$REPO" --head "$BRANCH_NAME" --state open --json url --limit 1 --jq '.[0].url // empty')"
if [ -z "$PR_URL" ]; then
  fail_run "pr_check" "No open PR found for branch '$BRANCH_NAME'."
fi

SUCCESS_COMMENT="$(cat <<EOF
Automation worker run \`$RUN_ID\` completed.

- PR: $PR_URL
- Branch: \`$BRANCH_NAME\`
- Validation gate: \`cargo test --workspace --locked\`
EOF
)"
gh issue comment --repo "$REPO" "$ISSUE_NUMBER" --body "$SUCCESS_COMMENT" >/dev/null || true

log "Completed successfully."
log "Issue: $ISSUE_URL"
log "PR: $PR_URL"
