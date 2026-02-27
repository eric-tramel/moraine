#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: maintenance/run_merge_train.sh [options]

Run a manual merge-train for GitHub PRs on a base branch.

Flow:
1) Select queued open PRs (label-based by default).
2) Validate each PR (draft/review/checks/mergeability).
3) Merge ready PRs in-order.
4) For behind/conflicting PRs, optionally launch a Codex repair session in a fresh worktree.

Options:
  --base BRANCH             Base branch to merge into (default: main)
  --queue-label LABEL       Queue label filter (default: merge-queue)
  --all-open                Ignore queue label and consider all open PRs for --base
  --status-label LABEL      Lock label for active repair attempts (default: status/merge-train)
  --pr-limit N              Max PRs to fetch from GitHub (default: 200)
  --max-merges N            Stop after N successful merges (default: 0 = unlimited)
  --merge-method METHOD     squash|merge|rebase (default: squash)
  --require-approval        Require reviewDecision=APPROVED before merge
  --allow-admin-merge       Pass --admin to gh pr merge
  --delete-branch           Delete head branch after successful merge
  --no-repair               Disable Codex repair runs for blocked PRs
  --continue-after-repair   Continue processing more PRs after a repair attempt
                            (default: stop after first repair attempt)
  --model MODEL             Codex repair model (default: gpt-5.3-codex)
  --effort LEVEL            Codex repair effort (default: high)
  --repair-gate CMD         Command to validate repaired branch
                            (default: cargo test --workspace --locked)
  --worktrees-dir PATH      Worktree root (default: .worktrees)
  --run-dir PATH            Run artifact directory (default: /tmp/moraine-merge-train-<timestamp>)
  --cleanup-worktrees       Remove successful repair worktrees after completion
  --dry-run                 Print planned actions only; no merge/repair actions
  --yes                     Skip confirmation prompt
  -h, --help                Show this help text
USAGE
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

normalize_merge_method() {
  case "$1" in
    squash|merge|rebase)
      printf '%s' "$1"
      ;;
    *)
      die "--merge-method must be one of: squash, merge, rebase"
      ;;
  esac
}

append_report() {
  local line="$1"
  printf '%s\n' "$line" >> "$REPORT_FILE"
}

ensure_label_exists() {
  local label_name="$1"
  [ -n "$label_name" ] || return 0

  if gh label list --repo "$REPO" --limit 500 --json name --jq '.[].name' | grep -Fxq "$label_name"; then
    return 0
  fi

  gh label create "$label_name" --repo "$REPO" --color "0e8a16" \
    --description "Automation lock label for merge-train repair work" >/dev/null || true

  if ! gh label list --repo "$REPO" --limit 500 --json name --jq '.[].name' | grep -Fxq "$label_name"; then
    die "Unable to create or find label '$label_name'."
  fi
}

fetch_candidates() {
  local args=(
    pr list
    --repo "$REPO"
    --state open
    --base "$BASE_BRANCH"
    --limit "$PR_LIMIT"
    --json number,title,url,isDraft,labels,reviewDecision,mergeStateStatus,headRefName,baseRefName,isCrossRepository,createdAt
  )

  if [ -n "$QUEUE_LABEL" ]; then
    args+=(--label "$QUEUE_LABEL")
  fi

  gh "${args[@]}" > "$RUN_DIR/candidates_raw.json"

  jq --arg status_label "$STATUS_LABEL" '
    map(
      . + {
        label_names: (.labels | map(.name))
      }
    )
    | if ($status_label | length) > 0
      then map(select((.label_names | index($status_label)) | not))
      else .
      end
    | sort_by(.number)
  ' "$RUN_DIR/candidates_raw.json" > "$RUN_DIR/candidates.json"
}

evaluate_checks() {
  local pr_number="$1"
  local checks_json='[]'
  local checks_exit=0

  CHECK_STATE="pass"
  CHECK_SUMMARY=""

  set +e
  checks_json="$(gh pr checks "$pr_number" --repo "$REPO" --required --json name,state,bucket,link,event 2>/dev/null)"
  checks_exit=$?
  set -e

  if [ -z "${checks_json:-}" ]; then
    checks_json='[]'
  fi
  if ! jq -e . >/dev/null 2>&1 <<< "$checks_json"; then
    checks_json='[]'
  fi

  printf '%s\n' "$checks_json" > "$RUN_DIR/pr-${pr_number}-checks.json"

  local total_count pending_count fail_count
  total_count="$(jq 'length' <<< "$checks_json")"
  pending_count="$(jq '[.[] | select(.bucket == "pending")] | length' <<< "$checks_json")"
  fail_count="$(jq '[.[] | select(.bucket == "fail" or .bucket == "cancel")] | length' <<< "$checks_json")"

  if [ "$checks_exit" -ne 0 ] && [ "$checks_exit" -ne 8 ] && [ "$total_count" -eq 0 ]; then
    CHECK_STATE="error"
    CHECK_SUMMARY="required-check query failed (exit=$checks_exit)"
    return 0
  fi

  if [ "$fail_count" -gt 0 ]; then
    CHECK_STATE="fail"
    CHECK_SUMMARY="$fail_count required check(s) failing/cancelled"
    return 0
  fi
  if [ "$pending_count" -gt 0 ]; then
    CHECK_STATE="pending"
    CHECK_SUMMARY="$pending_count required check(s) pending"
    return 0
  fi

  CHECK_STATE="pass"
  if [ "$total_count" -eq 0 ]; then
    CHECK_SUMMARY="no required checks reported"
  else
    CHECK_SUMMARY="$total_count required check(s) passing"
  fi
}

run_codex_repair() {
  local pr_number="$1"
  local pr_title="$2"
  local pr_url="$3"
  local head_ref="$4"
  local head_sha_before="$5"
  local is_cross_repo="$6"

  if [ "$is_cross_repo" = "true" ]; then
    log "PR #$pr_number is from a fork; skipping Codex repair."
    append_report "- #$pr_number [SKIP-REPAIR] $pr_url - fork PR (cross-repository branch)"
    return 2
  fi

  local claim_label_added=0
  if [ -n "$STATUS_LABEL" ]; then
    gh pr edit "$pr_number" --repo "$REPO" --add-label "$STATUS_LABEL" >/dev/null
    claim_label_added=1
  fi

  local pr_slug worktree_path worktree_rel local_branch run_token
  pr_slug="$(slugify "$pr_title")"
  [ -n "$pr_slug" ] || pr_slug="pr-${pr_number}"
  pr_slug="${pr_slug:0:40}"
  run_token="$(date -u +'%Y%m%d%H%M%S')"
  worktree_rel="${WORKTREES_DIR%/}/pr-${pr_number}-${pr_slug}-repair-${run_token}"
  worktree_path="$ROOT_DIR/$worktree_rel"
  local_branch="merge-train/pr-${pr_number}-repair-${run_token}"

  mkdir -p "$(dirname "$worktree_path")"

  log "PR #$pr_number: creating repair worktree at $worktree_path..."
  if ! git fetch origin "$BASE_BRANCH" "$head_ref" >/dev/null 2>&1; then
    if [ "$claim_label_added" -eq 1 ]; then
      gh pr edit "$pr_number" --repo "$REPO" --remove-label "$STATUS_LABEL" >/dev/null 2>&1 || true
    fi
    append_report "- #$pr_number [REPAIR-FAILED] $pr_url - git fetch origin $BASE_BRANCH $head_ref failed"
    return 1
  fi

  if ! git worktree add -B "$local_branch" "$worktree_path" "origin/$head_ref" >/dev/null 2>&1; then
    if [ "$claim_label_added" -eq 1 ]; then
      gh pr edit "$pr_number" --repo "$REPO" --remove-label "$STATUS_LABEL" >/dev/null 2>&1 || true
    fi
    append_report "- #$pr_number [REPAIR-FAILED] $pr_url - git worktree add failed"
    return 1
  fi

  local pr_run_dir prompt_file codex_log final_file gate_log
  pr_run_dir="$RUN_DIR/pr-${pr_number}"
  mkdir -p "$pr_run_dir"
  prompt_file="$pr_run_dir/repair_prompt.txt"
  codex_log="$pr_run_dir/repair_codex.log"
  final_file="$pr_run_dir/repair_final.md"
  gate_log="$pr_run_dir/repair_gate.log"

  cat > "$prompt_file" <<PROMPT
You are an autonomous coding agent repairing a pull request branch for a manual merge train.

Repository: $REPO
PR number: #$pr_number
PR URL: $pr_url
PR title: $pr_title
Base branch: $BASE_BRANCH
PR head branch: $head_ref
Worktree path: $worktree_path

Goal:
Make PR #$pr_number merge-train ready by integrating latest origin/$BASE_BRANCH into the PR head branch while preserving PR intent.

Required steps:
1. Read and follow AGENTS.md.
2. Update from remote:
   git fetch origin $BASE_BRANCH $head_ref
3. Integrate origin/$BASE_BRANCH into the current branch:
   - Prefer a rebase.
   - If rebase is risky, use a regular merge.
4. Resolve conflicts, keeping behavior correct.
5. Run validation gate exactly:
   $REPAIR_GATE
6. Ensure git status is clean.
7. Push updated branch to the existing PR head:
   git push origin HEAD:$head_ref
8. Comment on PR #$pr_number with a short summary and gate result.

Output format (strict, one line per field):
RESULT: <success|failure>
COMMIT_SHA: <sha-or-none>
TEST_RESULT: <pass|fail|not-run>
NOTES: <one-line summary>
PROMPT

  log "PR #$pr_number: launching Codex repair session..."
  if ! codex exec \
    --ephemeral \
    --full-auto \
    --sandbox workspace-write \
    -m "$MODEL" \
    -c "model_reasoning_effort=\"$EFFORT\"" \
    --cd "$worktree_path" \
    --output-last-message "$final_file" \
    - < "$prompt_file" > "$codex_log" 2>&1; then
    if [ "$claim_label_added" -eq 1 ]; then
      gh pr edit "$pr_number" --repo "$REPO" --remove-label "$STATUS_LABEL" >/dev/null 2>&1 || true
    fi
    append_report "- #$pr_number [REPAIR-FAILED] $pr_url - Codex execution failed (see $codex_log)"
    return 1
  fi

  log "PR #$pr_number: running post-repair gate locally..."
  if ! (cd "$worktree_path" && bash -lc "$REPAIR_GATE" > "$gate_log" 2>&1); then
    if [ "$claim_label_added" -eq 1 ]; then
      gh pr edit "$pr_number" --repo "$REPO" --remove-label "$STATUS_LABEL" >/dev/null 2>&1 || true
    fi
    append_report "- #$pr_number [REPAIR-FAILED] $pr_url - post-repair gate failed (see $gate_log)"
    return 1
  fi

  if [ -n "$(git -C "$worktree_path" status --porcelain)" ]; then
    if [ "$claim_label_added" -eq 1 ]; then
      gh pr edit "$pr_number" --repo "$REPO" --remove-label "$STATUS_LABEL" >/dev/null 2>&1 || true
    fi
    append_report "- #$pr_number [REPAIR-FAILED] $pr_url - worktree has uncommitted changes after repair"
    return 1
  fi

  local head_sha_after
  head_sha_after="$(gh pr view "$pr_number" --repo "$REPO" --json headRefOid --jq '.headRefOid // ""')"
  if [ -z "$head_sha_after" ]; then
    if [ "$claim_label_added" -eq 1 ]; then
      gh pr edit "$pr_number" --repo "$REPO" --remove-label "$STATUS_LABEL" >/dev/null 2>&1 || true
    fi
    append_report "- #$pr_number [REPAIR-FAILED] $pr_url - unable to read updated PR head SHA"
    return 1
  fi

  if [ "$claim_label_added" -eq 1 ]; then
    gh pr edit "$pr_number" --repo "$REPO" --remove-label "$STATUS_LABEL" >/dev/null 2>&1 || true
  fi

  if [ "$head_sha_after" = "$head_sha_before" ]; then
    append_report "- #$pr_number [REPAIRED-NOCHANGE] $pr_url - Codex ran but head SHA unchanged ($head_sha_after)"
  else
    append_report "- #$pr_number [REPAIRED] $pr_url - head SHA $head_sha_before -> $head_sha_after"
  fi

  if [ "$CLEANUP_WORKTREES" -eq 1 ]; then
    git worktree remove --force "$worktree_path" >/dev/null 2>&1 || true
  fi
  return 0
}

BASE_BRANCH="main"
QUEUE_LABEL="merge-queue"
STATUS_LABEL="status/merge-train"
PR_LIMIT=200
MAX_MERGES=0
MERGE_METHOD="squash"
REQUIRE_APPROVAL=0
ALLOW_ADMIN_MERGE=0
DELETE_BRANCH=0
ENABLE_REPAIR=1
STOP_AFTER_REPAIR=1
MODEL="gpt-5.3-codex"
EFFORT="high"
REPAIR_GATE="cargo test --workspace --locked"
WORKTREES_DIR=".worktrees"
RUN_DIR="/tmp/moraine-merge-train-$(date +'%Y%m%d-%H%M%S')"
CLEANUP_WORKTREES=0
DRY_RUN=0
AUTO_YES=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --base)
      BASE_BRANCH="${2:-}"; shift 2 ;;
    --queue-label)
      QUEUE_LABEL="${2:-}"; shift 2 ;;
    --all-open)
      QUEUE_LABEL=""; shift ;;
    --status-label)
      STATUS_LABEL="${2:-}"; shift 2 ;;
    --pr-limit)
      PR_LIMIT="${2:-}"; shift 2 ;;
    --max-merges)
      MAX_MERGES="${2:-}"; shift 2 ;;
    --merge-method)
      MERGE_METHOD="$(normalize_merge_method "${2:-}")"; shift 2 ;;
    --require-approval)
      REQUIRE_APPROVAL=1; shift ;;
    --allow-admin-merge)
      ALLOW_ADMIN_MERGE=1; shift ;;
    --delete-branch)
      DELETE_BRANCH=1; shift ;;
    --no-repair)
      ENABLE_REPAIR=0; shift ;;
    --continue-after-repair)
      STOP_AFTER_REPAIR=0; shift ;;
    --model)
      MODEL="${2:-}"; shift 2 ;;
    --effort)
      EFFORT="${2:-}"; shift 2 ;;
    --repair-gate)
      REPAIR_GATE="${2:-}"; shift 2 ;;
    --worktrees-dir)
      WORKTREES_DIR="${2:-}"; shift 2 ;;
    --run-dir)
      RUN_DIR="${2:-}"; shift 2 ;;
    --cleanup-worktrees)
      CLEANUP_WORKTREES=1; shift ;;
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

BASE_BRANCH="$(trim "$BASE_BRANCH")"
QUEUE_LABEL="$(trim "$QUEUE_LABEL")"
STATUS_LABEL="$(trim "$STATUS_LABEL")"
WORKTREES_DIR="$(trim "$WORKTREES_DIR")"
REPAIR_GATE="$(trim "$REPAIR_GATE")"

[[ "$PR_LIMIT" =~ ^[1-9][0-9]*$ ]] || die "--pr-limit must be a positive integer"
[[ "$MAX_MERGES" =~ ^[0-9]+$ ]] || die "--max-merges must be 0 or a positive integer"
[[ "$EFFORT" =~ ^(minimal|low|medium|high|xhigh)$ ]] || die "--effort must be one of: minimal, low, medium, high, xhigh"
[ -n "$BASE_BRANCH" ] || die "--base cannot be empty"
[ -n "$WORKTREES_DIR" ] || die "--worktrees-dir cannot be empty"
[ -n "$REPAIR_GATE" ] || die "--repair-gate cannot be empty"

require_cmd git
require_cmd gh
require_cmd jq
require_cmd sed
require_cmd bash
if [ "$ENABLE_REPAIR" -eq 1 ] && [ "$DRY_RUN" -eq 0 ]; then
  require_cmd codex
fi

ROOT_DIR="$(git rev-parse --show-toplevel 2>/dev/null || true)"
[ -n "$ROOT_DIR" ] || die "Run this script inside a git repository"
cd "$ROOT_DIR"

if ! gh auth status >/dev/null 2>&1; then
  die "GitHub CLI is not authenticated. Run: gh auth login"
fi

REPO="$(gh repo view --json nameWithOwner --jq .nameWithOwner)"
[ -n "$REPO" ] || die "Unable to resolve repository name."

mkdir -p "$RUN_DIR"
REPORT_FILE="$RUN_DIR/report.md"
{
  echo "# Merge Train Report"
  echo
  echo "- Repo: $REPO"
  echo "- Base branch: $BASE_BRANCH"
  if [ -n "$QUEUE_LABEL" ]; then
    echo "- Queue label: $QUEUE_LABEL"
  else
    echo "- Queue label: (none, all open PRs)"
  fi
  echo "- Dry run: $DRY_RUN"
  if [ "$STOP_AFTER_REPAIR" -eq 1 ]; then
    echo "- Stop after repair: yes"
  else
    echo "- Stop after repair: no"
  fi
  echo "- Started: $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
  echo
} > "$REPORT_FILE"

if [ "$ENABLE_REPAIR" -eq 1 ] && [ "$DRY_RUN" -eq 0 ] && [ -n "$STATUS_LABEL" ]; then
  ensure_label_exists "$STATUS_LABEL"
fi

fetch_candidates
CANDIDATE_COUNT="$(jq 'length' "$RUN_DIR/candidates.json")"
log "Fetched $CANDIDATE_COUNT candidate PR(s)."

if [ "$CANDIDATE_COUNT" -eq 0 ]; then
  append_report "No candidate PRs found."
  cp "$REPORT_FILE" "$ROOT_DIR/maintenance/MERGE_TRAIN_REPORT.md"
  log "No candidate PRs found. Report written to maintenance/MERGE_TRAIN_REPORT.md"
  exit 0
fi

if [ "$AUTO_YES" -eq 0 ] && [ "$DRY_RUN" -eq 0 ]; then
  log "About to process $CANDIDATE_COUNT PR(s) in merge train order."
  read -r -p "Proceed? [y/N] " answer
  case "${answer:-}" in
    y|Y|yes|YES)
      ;;
    *)
      die "Aborted by user."
      ;;
  esac
fi

MERGED_COUNT=0
REPAIRED_COUNT=0
SKIPPED_COUNT=0
FAILED_COUNT=0
HALT_REASON=""

while IFS= read -r pr_number; do
  if [ "$MAX_MERGES" -gt 0 ] && [ "$MERGED_COUNT" -ge "$MAX_MERGES" ]; then
    log "Reached --max-merges limit ($MAX_MERGES); stopping."
    break
  fi

  pr_json="$(gh pr view "$pr_number" --repo "$REPO" --json number,title,url,isDraft,mergeable,mergeStateStatus,reviewDecision,headRefName,headRefOid,baseRefName,isCrossRepository)"
  pr_title="$(jq -r '.title' <<< "$pr_json")"
  pr_url="$(jq -r '.url' <<< "$pr_json")"
  pr_draft="$(jq -r '.isDraft' <<< "$pr_json")"
  pr_review="$(jq -r '.reviewDecision // ""' <<< "$pr_json")"
  pr_mergeable="$(jq -r '.mergeable // ""' <<< "$pr_json")"
  pr_merge_state="$(jq -r '.mergeStateStatus // ""' <<< "$pr_json")"
  pr_head_ref="$(jq -r '.headRefName' <<< "$pr_json")"
  pr_head_sha="$(jq -r '.headRefOid // ""' <<< "$pr_json")"
  pr_cross_repo="$(jq -r '.isCrossRepository' <<< "$pr_json")"

  log "Evaluating PR #$pr_number: $pr_title"

  if [ "$pr_draft" = "true" ]; then
    SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
    append_report "- #$pr_number [SKIP] $pr_url - draft PR"
    continue
  fi

  if [ "$REQUIRE_APPROVAL" -eq 1 ] && [ "$pr_review" != "APPROVED" ]; then
    SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
    append_report "- #$pr_number [SKIP] $pr_url - reviewDecision=$pr_review (approval required)"
    continue
  fi

  evaluate_checks "$pr_number"
  case "$CHECK_STATE" in
    pass)
      ;;
    pending)
      SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
      append_report "- #$pr_number [SKIP] $pr_url - $CHECK_SUMMARY"
      continue
      ;;
    fail)
      SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
      append_report "- #$pr_number [SKIP] $pr_url - $CHECK_SUMMARY"
      continue
      ;;
    error)
      SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
      append_report "- #$pr_number [SKIP] $pr_url - $CHECK_SUMMARY"
      continue
      ;;
    *)
      SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
      append_report "- #$pr_number [SKIP] $pr_url - unknown check state '$CHECK_STATE'"
      continue
      ;;
  esac

  needs_repair=0
  case "$pr_mergeable" in
    CONFLICTING)
      needs_repair=1
      ;;
    MERGEABLE)
      case "$pr_merge_state" in
        DIRTY|BEHIND)
          needs_repair=1
          ;;
      esac
      ;;
    UNKNOWN)
      SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
      append_report "- #$pr_number [SKIP] $pr_url - mergeability unknown"
      continue
      ;;
    *)
      needs_repair=1
      ;;
  esac

  if [ "$needs_repair" -eq 1 ]; then
    if [ "$ENABLE_REPAIR" -eq 0 ]; then
      SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
      append_report "- #$pr_number [SKIP] $pr_url - needs repair (mergeable=$pr_mergeable state=$pr_merge_state)"
      HALT_REASON="queue blocked at PR #$pr_number (needs repair; repair disabled)"
      log "Stopping: $HALT_REASON"
      break
    fi

    if [ "$DRY_RUN" -eq 1 ]; then
      REPAIRED_COUNT=$((REPAIRED_COUNT + 1))
      append_report "- #$pr_number [DRY-RUN-REPAIR] $pr_url - would run Codex repair (mergeable=$pr_mergeable state=$pr_merge_state)"
      if [ "$STOP_AFTER_REPAIR" -eq 1 ]; then
        HALT_REASON="stopped after first repair attempt in dry-run mode at PR #$pr_number"
        log "Stopping: $HALT_REASON"
        break
      fi
      continue
    fi

    repair_status=0
    if run_codex_repair "$pr_number" "$pr_title" "$pr_url" "$pr_head_ref" "$pr_head_sha" "$pr_cross_repo"; then
      REPAIRED_COUNT=$((REPAIRED_COUNT + 1))
      if [ "$STOP_AFTER_REPAIR" -eq 1 ]; then
        HALT_REASON="stopped after repairing PR #$pr_number; rerun after checks complete to merge"
        log "Stopping: $HALT_REASON"
        break
      fi
    else
      repair_status=$?
      if [ "$repair_status" -eq 2 ]; then
        SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
      else
        FAILED_COUNT=$((FAILED_COUNT + 1))
        if [ "$STOP_AFTER_REPAIR" -eq 1 ]; then
          HALT_REASON="stopped after repair failure on PR #$pr_number"
          log "Stopping: $HALT_REASON"
          break
        fi
      fi
    fi
    continue
  fi

  if [ "$DRY_RUN" -eq 1 ]; then
    MERGED_COUNT=$((MERGED_COUNT + 1))
    append_report "- #$pr_number [DRY-RUN-MERGE] $pr_url - would merge via $MERGE_METHOD"
    continue
  fi

  merge_log="$RUN_DIR/pr-${pr_number}-merge.log"
  merge_args=(pr merge "$pr_number" --repo "$REPO")
  case "$MERGE_METHOD" in
    squash)
      merge_args+=(--squash)
      ;;
    merge)
      merge_args+=(--merge)
      ;;
    rebase)
      merge_args+=(--rebase)
      ;;
  esac
  if [ "$ALLOW_ADMIN_MERGE" -eq 1 ]; then
    merge_args+=(--admin)
  fi
  if [ "$DELETE_BRANCH" -eq 1 ]; then
    merge_args+=(--delete-branch)
  fi

  log "Merging PR #$pr_number via '$MERGE_METHOD'..."
  if gh "${merge_args[@]}" > "$merge_log" 2>&1; then
    MERGED_COUNT=$((MERGED_COUNT + 1))
    append_report "- #$pr_number [MERGED] $pr_url - method=$MERGE_METHOD"
  else
    FAILED_COUNT=$((FAILED_COUNT + 1))
    append_report "- #$pr_number [MERGE-FAILED] $pr_url - see $merge_log"
  fi
done < <(jq -r '.[].number' "$RUN_DIR/candidates.json")

{
  echo
  echo "## Summary"
  echo
  echo "- merged: $MERGED_COUNT"
  echo "- repaired: $REPAIRED_COUNT"
  echo "- skipped: $SKIPPED_COUNT"
  echo "- failed: $FAILED_COUNT"
  if [ -n "$HALT_REASON" ]; then
    echo "- halted: $HALT_REASON"
  fi
  echo
  echo "- Finished: $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
} >> "$REPORT_FILE"

cp "$REPORT_FILE" "$ROOT_DIR/maintenance/MERGE_TRAIN_REPORT.md"

log "Merge-train run complete."
log "Summary: merged=$MERGED_COUNT repaired=$REPAIRED_COUNT skipped=$SKIPPED_COUNT failed=$FAILED_COUNT"
log "Run artifacts: $RUN_DIR"
log "Report copied to maintenance/MERGE_TRAIN_REPORT.md"
