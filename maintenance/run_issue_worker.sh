#!/usr/bin/env bash
set -euo pipefail

NO_ISSUES_EXIT_CODE=20
LAUNCH_STAGGER_SECONDS=20
WORKER_POLL_SECONDS=2

usage() {
  cat <<'USAGE'
Usage: maintenance/run_issue_worker.sh [options]

Select one highest-priority open GitHub issue, claim it as in-progress, spawn
one or more fresh worktrees, and launch Codex execution(s) to implement/fix and
open PR(s).

If --tag-union is provided, selection is filtered to unblocked issues matching
that label union; otherwise selection considers all unblocked open issues.

Options:
  --tag-union "a,b,c"         Optional. Comma-separated label union.
  --status-label LABEL        In-progress lock label (default: status/in-progress)
  --model MODEL               Codex model (default: gpt-5.3-codex)
  --effort LEVEL              Reasoning effort (default: xhigh)
  --base BRANCH               Base branch for worktree/PR (default: main)
  --worktrees-dir PATH        Worktree root relative to repo (default: .worktrees)
  --issue-limit N             Number of open issues to fetch (default: 200)
  --parallel N                Worker concurrency target (default: 1)
  --continuous                Keep launching new workers until no issues remain
  --dry-run                   Select issue and print outcome only
  -h, --help                  Show this help text
USAGE
}

log() {
  local context=""
  if [ -n "${LOG_CONTEXT:-}" ]; then
    context=" [$LOG_CONTEXT]"
  fi
  printf '[%s]%s %s\n' "$(date +'%Y-%m-%d %H:%M:%S')" "$context" "$*"
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

count_commits() {
  local repo_path="$1"
  local revision_range="$2"
  local count=""

  if ! count="$(git -C "$repo_path" rev-list --count "$revision_range" 2>/dev/null)"; then
    printf '0'
    return 0
  fi
  if [[ "$count" =~ ^[0-9]+$ ]]; then
    printf '%s' "$count"
    return 0
  fi
  printf '0'
}

extract_codex_field() {
  local field="$1"
  local file="$2"

  [ -f "$file" ] || return 1
  awk -v key="$field" 'index($0, key ": ") == 1 { print substr($0, length(key) + 3); exit }' "$file"
}

normalize_optional_value() {
  local value
  local lower

  value="$(trim "${1:-}")"
  lower="$(printf '%s' "$value" | tr '[:upper:]' '[:lower:]')"

  case "$lower" in
    ""|"none"|"null"|"n/a"|"na")
      printf ''
      ;;
    *)
      printf '%s' "$value"
      ;;
  esac
}

resolve_pr_info_json() {
  local branch_name="$1"
  local attempts="${2:-5}"
  local delay_seconds="${3:-2}"
  local attempt=1
  local info_json='[]'
  local pr_url=""

  while [ "$attempt" -le "$attempts" ]; do
    info_json="$(gh pr list --repo "$REPO" --head "$branch_name" --state open --json url,headRefOid --limit 1 2>/dev/null || printf '[]')"
    pr_url="$(jq -r '.[0].url // empty' <<< "$info_json")"
    if [ -n "$pr_url" ]; then
      printf '%s' "$info_json"
      return 0
    fi
    if [ "$attempt" -lt "$attempts" ]; then
      sleep "$delay_seconds"
    fi
    attempt=$((attempt + 1))
  done

  printf '%s' "$info_json"
  return 1
}

infer_worker_status_from_log() {
  local worker_log="$1"

  if [ ! -f "$worker_log" ]; then
    return 1
  fi

  if rg -Fq "Completed successfully." "$worker_log"; then
    printf '0'
    return 0
  fi
  if rg -Fq "No eligible issues found for filter" "$worker_log"; then
    printf '%s' "$NO_ISSUES_EXIT_CODE"
    return 0
  fi
  if rg -Fq "ERROR:" "$worker_log"; then
    printf '1'
    return 0
  fi

  return 1
}

remove_active_worker_at_index() {
  local index="$1"
  local nounset_was_set=0

  case $- in
    *u*)
      nounset_was_set=1
      set +u
      ;;
  esac

  unset "ACTIVE_PIDS[$index]"
  unset "ACTIVE_IDS[$index]"
  unset "ACTIVE_LOGS[$index]"

  ACTIVE_PIDS=("${ACTIVE_PIDS[@]}")
  ACTIVE_IDS=("${ACTIVE_IDS[@]}")
  ACTIVE_LOGS=("${ACTIVE_LOGS[@]}")

  if [ "$nounset_was_set" -eq 1 ]; then
    set -u
  fi
}

worker_cleanup() {
  if [ -n "${RUN_DIR:-}" ] && [ -d "$RUN_DIR" ]; then
    rm -rf "$RUN_DIR"
  fi
}

fail_run() {
  local step="$1"
  local message="$2"

  if [ "$CLAIMED" -eq 1 ] && [ -n "${ISSUE_NUMBER:-}" ]; then
    gh issue comment --repo "$REPO" "$ISSUE_NUMBER" --body "$(cat <<COMMENT
Automation run \`$RUN_ID\` failed at step \`$step\`.

Reason: $message
Branch: \`${BRANCH_NAME:-not-created}\`
Worktree: \`${WORKTREE_PATH:-not-created}\`
COMMENT
)" >/dev/null 2>&1 || true
  fi

  die "$message"
}

prepare_tag_filter() {
  RAW_TAGS=()
  if [ -n "$TAG_UNION" ]; then
    IFS=',' read -r -a RAW_TAGS <<< "$TAG_UNION"
  fi

  TAGS=()
  for raw_tag in "${RAW_TAGS[@]-}"; do
    cleaned_tag="$(trim "$raw_tag")"
    if [ -n "$cleaned_tag" ]; then
      TAGS+=("$cleaned_tag")
    fi
  done

  TAG_FILTER_DESC="all unblocked open issues (no tag filter)"
  if [ -n "$TAG_UNION" ] && [ "${#TAGS[@]}" -eq 0 ]; then
    die "--tag-union was provided but did not contain any valid labels"
  fi

  if [ "${#TAGS[@]}" -gt 0 ]; then
    TAGS_JSON="$(printf '%s\n' "${TAGS[@]}" | jq -R . | jq -s .)"
    TAG_FILTER_DESC="$(IFS=,; printf '%s' "${TAGS[*]}")"
  else
    TAGS_JSON='[]'
  fi
}

ensure_status_label_exists() {
  log "Ensuring status label '$STATUS_LABEL' exists..."
  if ! gh label list --repo "$REPO" --limit 200 --json name --jq '.[].name' | grep -Fxq "$STATUS_LABEL"; then
    gh label create "$STATUS_LABEL" --repo "$REPO" --color "0e8a16" \
      --description "Issue is actively being worked by automation" >/dev/null || true
  fi
  if ! gh label list --repo "$REPO" --limit 200 --json name --jq '.[].name' | grep -Fxq "$STATUS_LABEL"; then
    die "Unable to create or find label '$STATUS_LABEL'."
  fi
}

fetch_open_issues_with_dependencies() {
  local run_dir="$1"
  local cursor="null"
  local has_next="true"
  local fetched=0
  local page_size=0
  local page_count=0
  local fetched_this_page=0

  : > "$run_dir/issues_pages.jsonl"

  while [ "$fetched" -lt "$ISSUE_LIMIT" ] && [ "$has_next" = "true" ]; do
    page_size=$((ISSUE_LIMIT - fetched))
    if [ "$page_size" -gt 100 ]; then
      page_size=100
    fi

    gh api graphql \
      -f query='query($owner:String!,$name:String!,$limit:Int!,$cursor:String){ repository(owner:$owner,name:$name){ issues(first:$limit, states:OPEN, orderBy:{field:CREATED_AT,direction:ASC}, after:$cursor){ nodes { number title url updatedAt labels(first:50){nodes{name}} issueDependenciesSummary { blockedBy } } pageInfo { hasNextPage endCursor } } } }' \
      -F owner="$REPO_OWNER" \
      -F name="$REPO_NAME" \
      -F limit="$page_size" \
      -F cursor="$cursor" > "$run_dir/issues_page_${page_count}.json"

    jq -c '.data.repository.issues.nodes[]' "$run_dir/issues_page_${page_count}.json" >> "$run_dir/issues_pages.jsonl"
    fetched_this_page="$(jq '.data.repository.issues.nodes | length' "$run_dir/issues_page_${page_count}.json")"
    fetched=$((fetched + fetched_this_page))
    has_next="$(jq -r '.data.repository.issues.pageInfo.hasNextPage' "$run_dir/issues_page_${page_count}.json")"
    cursor="$(jq -r '.data.repository.issues.pageInfo.endCursor // "null"' "$run_dir/issues_page_${page_count}.json")"
    page_count=$((page_count + 1))

    if [ "$fetched_this_page" -eq 0 ] || [ "$has_next" != "true" ] || [ "$cursor" = "null" ]; then
      break
    fi
  done

  if [ -s "$run_dir/issues_pages.jsonl" ]; then
    jq -s '
      map({
        number,
        title,
        url,
        updatedAt,
        blockedByCount: (.issueDependenciesSummary.blockedBy // 0),
        labels: (.labels.nodes | map(.name))
      })
    ' "$run_dir/issues_pages.jsonl" > "$run_dir/issues_raw.json"
  else
    printf '[]\n' > "$run_dir/issues_raw.json"
  fi
}

issue_blocked_by_count() {
  local issue_number="$1"

  gh api graphql \
    -f query='query($owner:String!,$name:String!,$number:Int!){ repository(owner:$owner,name:$name){ issue(number:$number){ issueDependenciesSummary { blockedBy } } } }' \
    -F owner="$REPO_OWNER" \
    -F name="$REPO_NAME" \
    -F number="$issue_number" \
    --jq '.data.repository.issue.issueDependenciesSummary.blockedBy // 0'
}

select_issue_candidate() {
  local run_dir="$1"

  log "Fetching open issues from $REPO with dependency metadata..."
  fetch_open_issues_with_dependencies "$run_dir"

  jq --arg status_label "$STATUS_LABEL" --argjson tags "$TAGS_JSON" '
    map({
      number,
      title,
      url,
      updatedAt,
      blockedByCount,
      labels
    })
    | map(.matches_union = (
        if ($tags | length) == 0 then true
        else ([.labels[]] | any(. as $l | $tags | index($l)))
        end
      ))
    | map(select(.matches_union))
    | map(select((.labels | index($status_label)) | not))
    | map(select((.blockedByCount // 0) == 0))
    | map(.priority_rank = (
        if (.labels | index("P0")) then 0
        elif (.labels | index("P1")) then 1
        elif (.labels | index("P2")) then 2
        else 3
        end
      ))
    | sort_by(.priority_rank, .number)
  ' "$run_dir/issues_raw.json" > "$run_dir/issues_candidates.json"

  if [ "$(jq 'length' "$run_dir/issues_candidates.json")" -eq 0 ]; then
    return "$NO_ISSUES_EXIT_CODE"
  fi

  jq '.[0]' "$run_dir/issues_candidates.json" > "$run_dir/issue_selected.json"
  return 0
}

run_issue_cycle() {
  local launch_id="$1"
  local select_status=0

  LOG_CONTEXT="launch-$launch_id"
  RUN_ID="${ORCH_RUN_ID}-l${launch_id}"
  RUN_DIR="$(mktemp -d "/tmp/cortex-issue-worker-${RUN_ID}.XXXXXX")"
  trap worker_cleanup EXIT

  CLAIMED=0
  ISSUE_NUMBER=""
  ISSUE_URL=""
  BRANCH_NAME=""
  WORKTREE_PATH=""
  PR_URL=""

  if [ "${#TAGS[@]}" -gt 0 ]; then
    printf '%s\n' "${TAGS[@]}" > "$RUN_DIR/tag_union.txt"
  else
    : > "$RUN_DIR/tag_union.txt"
  fi

  select_issue_candidate "$RUN_DIR" || select_status=$?
  if [ "$select_status" -ne 0 ]; then
    if [ "$select_status" -eq "$NO_ISSUES_EXIT_CODE" ]; then
      log "No eligible issues found for filter '$TAG_FILTER_DESC'."
      return "$NO_ISSUES_EXIT_CODE"
    fi
    fail_run "issue_selection" "Failed selecting issue candidates."
  fi

  ISSUE_NUMBER="$(jq -r '.number' "$RUN_DIR/issue_selected.json")"
  ISSUE_TITLE="$(jq -r '.title' "$RUN_DIR/issue_selected.json")"
  ISSUE_URL="$(jq -r '.url' "$RUN_DIR/issue_selected.json")"
  ISSUE_PRIORITY="$(jq -r '.priority_rank' "$RUN_DIR/issue_selected.json")"

  log "Selected issue #$ISSUE_NUMBER (priority rank: $ISSUE_PRIORITY, filter: $TAG_FILTER_DESC): $ISSUE_TITLE"

  gh issue view --repo "$REPO" "$ISSUE_NUMBER" --json labels > "$RUN_DIR/issue_preclaim.json"
  if jq -e --arg status "$STATUS_LABEL" '.labels | map(.name) | index($status) != null' \
    "$RUN_DIR/issue_preclaim.json" >/dev/null; then
    fail_run "preclaim_check" "Issue #$ISSUE_NUMBER was claimed concurrently before locking."
  fi
  if ! BLOCKED_BY_COUNT="$(issue_blocked_by_count "$ISSUE_NUMBER")"; then
    fail_run "preclaim_check" "Failed to verify dependency state for issue #$ISSUE_NUMBER."
  fi
  if ! [[ "$BLOCKED_BY_COUNT" =~ ^[0-9]+$ ]]; then
    fail_run "preclaim_check" "Unexpected blocked-by count '$BLOCKED_BY_COUNT' for issue #$ISSUE_NUMBER."
  fi
  if [ "$BLOCKED_BY_COUNT" -gt 0 ]; then
    fail_run "preclaim_check" "Issue #$ISSUE_NUMBER is currently blocked by $BLOCKED_BY_COUNT issue(s)."
  fi

  log "Claiming issue #$ISSUE_NUMBER with '$STATUS_LABEL'..."
  if ! gh issue edit --repo "$REPO" "$ISSUE_NUMBER" --add-label "$STATUS_LABEL" >/dev/null; then
    fail_run "claim_issue" "Failed to add '$STATUS_LABEL' to issue #$ISSUE_NUMBER."
  fi
  CLAIMED=1

  CLAIM_COMMENT="$(cat <<COMMENT
Automation worker run \`$RUN_ID\` claimed this issue.

- Tag filter: \`$TAG_FILTER_DESC\`
- Selected priority: rank \`$ISSUE_PRIORITY\` (P0=0, P1=1, P2=2, unlabeled=3)
- Status label: \`$STATUS_LABEL\`
COMMENT
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
  RUN_SUFFIX="${RUN_SUFFIX:0:12}"

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
  cat > "$PROMPT_FILE" <<PROMPT
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
PROMPT

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

  if [ -n "$(git -C "$WORKTREE_PATH" status --porcelain)" ]; then
    fail_run "clean_tree_check" "Worktree has uncommitted changes after Codex run."
  fi

  if [ -z "$(git ls-remote --heads origin "$BRANCH_NAME")" ]; then
    fail_run "push_check" "Remote branch origin/$BRANCH_NAME not found."
  fi

  if ! git -C "$WORKTREE_PATH" fetch origin "$BASE_BRANCH" >/dev/null 2>&1; then
    log "Warning: failed to refresh origin/$BASE_BRANCH before commit_check."
  fi
  if ! git -C "$WORKTREE_PATH" fetch origin "$BRANCH_NAME" >/dev/null 2>&1; then
    log "Warning: failed to refresh origin/$BRANCH_NAME before commit_check."
  fi

  FINAL_PR_URL="$(normalize_optional_value "$(extract_codex_field "PR_URL" "$RUN_DIR/final.md" 2>/dev/null || true)")"
  FINAL_COMMIT_SHA="$(normalize_optional_value "$(extract_codex_field "COMMIT_SHA" "$RUN_DIR/final.md" 2>/dev/null || true)")"

  PR_INFO_JSON="$(resolve_pr_info_json "$BRANCH_NAME" 5 2 || true)"
  PR_URL="$(jq -r '.[0].url // empty' <<< "$PR_INFO_JSON")"
  PR_HEAD_SHA="$(jq -r '.[0].headRefOid // empty' <<< "$PR_INFO_JSON")"
  if [ -z "$PR_URL" ] && [ -n "$FINAL_PR_URL" ]; then
    PR_URL="$FINAL_PR_URL"
    log "PR discovery fallback: using PR URL from Codex final output: $PR_URL"
  fi
  if [ -z "$PR_URL" ]; then
    fail_run "pr_check" "No open PR found for branch '$BRANCH_NAME' (and no PR_URL in final output)."
  fi

  PR_COMMIT_COUNT=""
  if PR_VIEW_JSON="$(gh pr view --repo "$REPO" "$PR_URL" --json headRefOid,commits 2>/dev/null)"; then
    PR_VIEW_HEAD_SHA="$(jq -r '.headRefOid // empty' <<< "$PR_VIEW_JSON" 2>/dev/null || true)"
    PR_VIEW_COMMIT_COUNT="$(jq -r '.commits | length' <<< "$PR_VIEW_JSON" 2>/dev/null || true)"
    if [ -n "$PR_VIEW_HEAD_SHA" ]; then
      PR_HEAD_SHA="$PR_VIEW_HEAD_SHA"
    fi
    if [[ "$PR_VIEW_COMMIT_COUNT" =~ ^[0-9]+$ ]]; then
      PR_COMMIT_COUNT="$PR_VIEW_COMMIT_COUNT"
    fi
  fi

  LOCAL_AHEAD_COUNT="$(count_commits "$WORKTREE_PATH" "origin/$BASE_BRANCH..HEAD")"
  REMOTE_AHEAD_COUNT="$(count_commits "$WORKTREE_PATH" "origin/$BASE_BRANCH..origin/$BRANCH_NAME")"
  PR_HEAD_IS_VALID=0
  if [[ "$PR_HEAD_SHA" =~ ^[0-9a-fA-F]{40}$ ]]; then
    PR_HEAD_IS_VALID=1
  fi
  FINAL_COMMIT_IS_VALID=0
  if [[ "$FINAL_COMMIT_SHA" =~ ^[0-9a-fA-F]{40}$ ]]; then
    FINAL_COMMIT_IS_VALID=1
  fi

  if [ -n "$PR_COMMIT_COUNT" ] && [ "$PR_COMMIT_COUNT" -gt 0 ]; then
    log "Commit check: PR reports $PR_COMMIT_COUNT commit(s)."
  elif [ "${LOCAL_AHEAD_COUNT:-0}" -lt 1 ] && \
       [ "${REMOTE_AHEAD_COUNT:-0}" -lt 1 ] && \
       [ "$PR_HEAD_IS_VALID" -ne 1 ] && \
       [ "$FINAL_COMMIT_IS_VALID" -ne 1 ]; then
    fail_run "commit_check" "No commit evidence found (local ahead: ${LOCAL_AHEAD_COUNT:-0}, remote ahead: ${REMOTE_AHEAD_COUNT:-0}, pr head sha: ${PR_HEAD_SHA:-none}, final commit sha: ${FINAL_COMMIT_SHA:-none}, pr commit count: ${PR_COMMIT_COUNT:-unknown})."
  else
    log "Commit check fallback: using alternate evidence (local ahead: ${LOCAL_AHEAD_COUNT:-0}, remote ahead: ${REMOTE_AHEAD_COUNT:-0}, pr head sha: ${PR_HEAD_SHA:-none}, final commit sha: ${FINAL_COMMIT_SHA:-none}, pr commit count: ${PR_COMMIT_COUNT:-unknown})."
  fi

  SUCCESS_COMMENT="$(cat <<COMMENT
Automation worker run \`$RUN_ID\` completed.

- PR: $PR_URL
- Branch: \`$BRANCH_NAME\`
- Validation gate: \`cargo test --workspace --locked\`
COMMENT
)"
  gh issue comment --repo "$REPO" "$ISSUE_NUMBER" --body "$SUCCESS_COMMENT" >/dev/null || true

  log "Completed successfully."
  log "Issue: $ISSUE_URL"
  log "PR: $PR_URL"
  return 0
}

run_dry_run_selection() {
  local dry_run_id
  local dry_run_dir
  local select_status=0
  dry_run_id="${ORCH_RUN_ID}-dryrun"
  dry_run_dir="$(mktemp -d "/tmp/cortex-issue-worker-${dry_run_id}.XXXXXX")"

  select_issue_candidate "$dry_run_dir" || select_status=$?
  if [ "$select_status" -ne 0 ]; then
    rm -rf "$dry_run_dir"
    if [ "$select_status" -eq "$NO_ISSUES_EXIT_CODE" ]; then
      if [ "${#TAGS[@]}" -gt 0 ]; then
        die "No matching unblocked open issues found for union: $TAG_FILTER_DESC"
      fi
      die "No unblocked open issues available outside '$STATUS_LABEL'."
    fi
    die "Failed selecting issue candidates."
  fi

  local issue_number
  local issue_url
  local issue_title
  local issue_priority
  issue_number="$(jq -r '.number' "$dry_run_dir/issue_selected.json")"
  issue_url="$(jq -r '.url' "$dry_run_dir/issue_selected.json")"
  issue_title="$(jq -r '.title' "$dry_run_dir/issue_selected.json")"
  issue_priority="$(jq -r '.priority_rank' "$dry_run_dir/issue_selected.json")"
  rm -rf "$dry_run_dir"

  log "Selected issue #$issue_number (priority rank: $issue_priority, filter: $TAG_FILTER_DESC): $issue_title"
  log "Dry-run selected issue: $issue_url"
}

maybe_wait_for_stagger() {
  local now
  local elapsed
  local wait_seconds

  if [ "$LAST_LAUNCH_EPOCH" -eq 0 ]; then
    return 0
  fi

  now="$(date +%s)"
  elapsed=$((now - LAST_LAUNCH_EPOCH))
  if [ "$elapsed" -lt "$LAUNCH_STAGGER_SECONDS" ]; then
    wait_seconds=$((LAUNCH_STAGGER_SECONDS - elapsed))
    if [ "$wait_seconds" -gt 0 ]; then
      log "Waiting ${wait_seconds}s before next worker launch (stagger ${LAUNCH_STAGGER_SECONDS}s)."
      sleep "$wait_seconds"
    fi
  fi
}

launch_worker() {
  local launch_id="$1"
  local worker_log="$ORCH_RUN_DIR/launch-${launch_id}.log"
  local pid=""

  maybe_wait_for_stagger
  (
    run_issue_cycle "$launch_id"
  ) > "$worker_log" 2>&1 &
  pid="$!"

  if ! [[ "$pid" =~ ^[0-9]+$ ]]; then
    die "Failed to capture PID for worker $launch_id."
  fi

  ACTIVE_PIDS+=("$pid")
  ACTIVE_IDS+=("$launch_id")
  ACTIVE_LOGS+=("$worker_log")
  TOTAL_LAUNCHED=$((TOTAL_LAUNCHED + 1))
  LAST_LAUNCH_EPOCH="$(date +%s)"
  log "Launched worker $launch_id (pid $pid)."
}

handle_worker_completion() {
  local launch_id="$1"
  local status="$2"
  local worker_log="$3"

  if [ "$status" -eq 0 ]; then
    COMPLETED_SUCCESS=$((COMPLETED_SUCCESS + 1))
    log "Worker $launch_id completed successfully."
    return 0
  fi

  if [ "$status" -eq "$NO_ISSUES_EXIT_CODE" ]; then
    COMPLETED_NO_ISSUES=$((COMPLETED_NO_ISSUES + 1))
    NO_ISSUES_SIGNAL=1
    STOP_LAUNCHING=1
    log "Worker $launch_id reported no eligible issues for filter '$TAG_FILTER_DESC'."
    return 0
  fi

  COMPLETED_FAILED=$((COMPLETED_FAILED + 1))
  ANY_FAILURE=1
  STOP_LAUNCHING=1
  log "Worker $launch_id failed with exit code $status. Log: $worker_log"
  if [ -f "$worker_log" ]; then
    tail -n 60 "$worker_log" 2>/dev/null | sed 's/^/[worker-log] /' || true
  fi
}

reap_finished_workers() {
  local finished=0
  local i=0

  while [ "$i" -lt "${#ACTIVE_PIDS[@]}" ]; do
    local pid="${ACTIVE_PIDS[$i]-}"
    local launch_id="${ACTIVE_IDS[$i]-unknown-$i}"
    local worker_log="${ACTIVE_LOGS[$i]-}"
    local status=0
    local inferred_status=""

    if [ -z "$worker_log" ]; then
      worker_log="$ORCH_RUN_DIR/launch-${launch_id}.log"
    fi
    if ! [[ "$pid" =~ ^[0-9]+$ ]]; then
      log "Warning: worker $launch_id has invalid tracked PID '${pid:-<empty>}' (index $i)."
      status=1
      if inferred_status="$(infer_worker_status_from_log "$worker_log")"; then
        status="$inferred_status"
        log "Using inferred status $status for worker $launch_id from log."
      fi
      COMPLETED_TOTAL=$((COMPLETED_TOTAL + 1))
      handle_worker_completion "$launch_id" "$status" "$worker_log"
      remove_active_worker_at_index "$i"
      finished=1
      continue
    fi

    if kill -0 "$pid" >/dev/null 2>&1; then
      i=$((i + 1))
      continue
    fi

    if wait "$pid" >/dev/null 2>&1; then
      status=0
    else
      status=$?
    fi
    if [ "$status" -eq 127 ]; then
      if inferred_status="$(infer_worker_status_from_log "$worker_log")"; then
        status="$inferred_status"
        log "Warning: wait lost PID $pid for worker $launch_id; inferred status $status from log."
      else
        status=1
        log "Warning: wait lost PID $pid for worker $launch_id with no inferable status; treating as failure."
      fi
    fi

    COMPLETED_TOTAL=$((COMPLETED_TOTAL + 1))
    handle_worker_completion "$launch_id" "$status" "$worker_log"

    remove_active_worker_at_index "$i"
    finished=1
  done

  if [ "$finished" -eq 1 ]; then
    return 0
  fi
  return 1
}

run_parallel_batch() {
  local next_launch=1

  while [ "$next_launch" -le "$PARALLEL" ]; do
    launch_worker "$next_launch"
    next_launch=$((next_launch + 1))
  done

  while [ "${#ACTIVE_PIDS[@]}" -gt 0 ]; do
    if ! reap_finished_workers; then
      sleep "$WORKER_POLL_SECONDS"
    fi
  done
}

run_continuous_pool() {
  local next_launch=1

  while [ "${#ACTIVE_PIDS[@]}" -lt "$PARALLEL" ] && [ "$STOP_LAUNCHING" -eq 0 ]; do
    launch_worker "$next_launch"
    next_launch=$((next_launch + 1))
  done

  while :; do
    if [ "${#ACTIVE_PIDS[@]}" -eq 0 ]; then
      break
    fi

    if ! reap_finished_workers; then
      sleep "$WORKER_POLL_SECONDS"
      continue
    fi

    while [ "${#ACTIVE_PIDS[@]}" -lt "$PARALLEL" ] && [ "$STOP_LAUNCHING" -eq 0 ]; do
      launch_worker "$next_launch"
      next_launch=$((next_launch + 1))
    done
  done
}

main_cleanup() {
  if [ -n "${ORCH_RUN_DIR:-}" ] && [ -d "$ORCH_RUN_DIR" ]; then
    rm -rf "$ORCH_RUN_DIR"
  fi
}

TAG_UNION=""
STATUS_LABEL="status/in-progress"
MODEL="gpt-5.3-codex"
EFFORT="xhigh"
BASE_BRANCH="main"
WORKTREES_DIR=".worktrees"
ISSUE_LIMIT=200
PARALLEL=1
CONTINUOUS=0
DRY_RUN=0
LOG_CONTEXT=""

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
    --parallel)
      PARALLEL="${2:-}"; shift 2 ;;
    --continuous)
      CONTINUOUS=1; shift ;;
    --dry-run)
      DRY_RUN=1; shift ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      die "Unknown argument: $1" ;;
  esac
done

[[ "$ISSUE_LIMIT" =~ ^[1-9][0-9]*$ ]] || die "--issue-limit must be a positive integer"
[[ "$PARALLEL" =~ ^[1-9][0-9]*$ ]] || die "--parallel must be a positive integer"
[[ "$EFFORT" =~ ^(minimal|low|medium|high|xhigh)$ ]] || die "--effort must be one of: minimal, low, medium, high, xhigh"

if [ "$DRY_RUN" -eq 1 ] && { [ "$PARALLEL" -ne 1 ] || [ "$CONTINUOUS" -eq 1 ]; }; then
  log "Dry-run mode ignores --parallel and --continuous."
fi

require_cmd git
require_cmd gh
require_cmd codex
require_cmd jq
require_cmd rg
require_cmd sed
require_cmd awk
if [ "$DRY_RUN" -eq 0 ]; then
  require_cmd cargo
fi

ROOT_DIR="$(git rev-parse --show-toplevel 2>/dev/null || true)"
[ -n "$ROOT_DIR" ] || die "Run this script inside a git repository"
cd "$ROOT_DIR"

if ! gh auth status >/dev/null 2>&1; then
  die "GitHub CLI is not authenticated. Run: gh auth login"
fi

REPO="$(gh repo view --json nameWithOwner --jq .nameWithOwner)"
REPO_OWNER="${REPO%%/*}"
REPO_NAME="${REPO#*/}"
[ -n "$REPO_OWNER" ] || die "Unable to parse repository owner from '$REPO'."
[ -n "$REPO_NAME" ] || die "Unable to parse repository name from '$REPO'."
[ "$REPO_OWNER" != "$REPO_NAME" ] || die "Expected owner/name format for repo, got '$REPO'."
ORCH_RUN_ID="$(date -u +'%Y%m%dT%H%M%SZ')-$$"
ORCH_RUN_DIR="$(mktemp -d "/tmp/cortex-issue-worker-orch-${ORCH_RUN_ID}.XXXXXX")"
trap main_cleanup EXIT

prepare_tag_filter

if [ "$DRY_RUN" -eq 1 ]; then
  run_dry_run_selection
  exit 0
fi

ensure_status_label_exists

ACTIVE_PIDS=()
ACTIVE_IDS=()
ACTIVE_LOGS=()
TOTAL_LAUNCHED=0
COMPLETED_TOTAL=0
COMPLETED_SUCCESS=0
COMPLETED_NO_ISSUES=0
COMPLETED_FAILED=0
ANY_FAILURE=0
NO_ISSUES_SIGNAL=0
LAST_LAUNCH_EPOCH=0
STOP_LAUNCHING=0

if [ "$CONTINUOUS" -eq 1 ]; then
  log "Starting continuous issue worker pool (parallel=$PARALLEL, stagger=${LAUNCH_STAGGER_SECONDS}s, filter=$TAG_FILTER_DESC)."
  run_continuous_pool
else
  log "Starting one-shot issue worker batch (parallel=$PARALLEL, stagger=${LAUNCH_STAGGER_SECONDS}s, filter=$TAG_FILTER_DESC)."
  run_parallel_batch
fi

log "Worker summary: launched=$TOTAL_LAUNCHED completed=$COMPLETED_TOTAL success=$COMPLETED_SUCCESS no_issues=$COMPLETED_NO_ISSUES failed=$COMPLETED_FAILED"
if [ "$ANY_FAILURE" -ne 0 ]; then
  die "One or more worker launches failed."
fi

if [ "$CONTINUOUS" -eq 1 ] && [ "$COMPLETED_SUCCESS" -eq 0 ] && [ "$COMPLETED_NO_ISSUES" -gt 0 ]; then
  log "Continuous mode completed: no eligible issues remained for filter '$TAG_FILTER_DESC'."
fi

log "All worker launches completed successfully."
