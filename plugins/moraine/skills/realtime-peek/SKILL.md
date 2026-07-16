---
name: realtime-peek
description: Use Moraine realtime session data to inspect active agent work across harnesses when the user asks what is happening now or what another agent is doing.
---

# Realtime Peek

Use this skill when the user asks about current or very recent agent activity, including other harnesses, sibling agents, subagents, or your own active session.

## Workflow

1. Choose a recent explicit time window with timezone. For "now" questions, start with the last one to two hours and widen only if needed.
2. Call `list_sessions` for that window, sorted newest first.
3. Prefer sessions that are still updating, have incomplete final turns, or match the harness, branch, issue, file, or agent named by the user.
4. Open relevant sessions or turns with id-only `open` first. If the compact
   summary is insufficient, request a bounded page with `id` plus `limit` and
   continue with `{ "cursor": next_cursor }` only as far as needed. Open an
   individual event ID when exact content is required.
5. If the user asks about a specific file, call `file_attention` and then open the returned event, turn, or session handles.

## What To Report

Summarize active work by source or harness, session title or ID, branch or working directory when visible, last update time, visible tool or terminal activity, and the current apparent state. Keep the summary compact and separate observed facts from inference.

If a record is still active, say that it is a live view and may change after the query. If the latest turn is incomplete, avoid treating it as a final decision.

Do not replay sensitive terminal or tool output into the current chat. Summarize what happened, redact secret-looking values such as tokens, keys, cookies, and credentials, and avoid quoting private data unless it is necessary for the user's requested debugging task.

## Search Follow-Up

Use `search_sessions` with keyword queries when the recent session list is too broad or when the user gives a content clue such as an issue number, command, error, file path, or branch name. After finding a candidate, narrow with `within_id` rather than pulling whole sessions into context.
