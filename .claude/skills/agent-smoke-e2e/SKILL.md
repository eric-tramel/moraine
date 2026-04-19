---
name: agent-smoke-e2e
description: End-to-end smoke check of the moraine MCP server. Exercises every MCP tool against a live moraine stack and prints a per-tool PASS/FAIL matrix. Use inside the dev sandbox after a fresh build to confirm the MCP binary actually serves its tools and returns shaped responses.
---

# agent-smoke-e2e

You are running an end-to-end smoke test of the moraine MCP server.

Your sole job is to call each of the six MCP tools exposed by the `moraine` MCP server, validate that the response is shaped as expected, and emit a final pass/fail matrix. Do **not** do anything else — no exploration, no reading files, no code changes. Treat this like a test runner: terse output, clear pass/fail signals, no prose.

## Setup assumptions

- The `moraine` MCP server is registered and its tools are available as `mcp__moraine__search`, `mcp__moraine__open`, `mcp__moraine__search_conversations`, `mcp__moraine__list_sessions`, `mcp__moraine__get_session`, and `mcp__moraine__get_session_events`.
- The stack may or may not have ingested sessions. Both cases are valid — the smoke test cares that tools *respond*, not that they return data.
- Do not call any tool outside the `mcp__moraine__*` namespace.

## Test sequence

Run each check below in order. For each: call the tool with the arguments listed, inspect the response, then record PASS or FAIL (with a one-line reason on FAIL).

### 1. `list_sessions`

Call `mcp__moraine__list_sessions` with `limit: 5`, `sort: "desc"`.

- **PASS** if the response is a JSON object with a `sessions` array (empty is fine).
- Capture `sample_session_id = sessions[0].session_id` if present. If the list is empty, leave it as `null` — later tests that need a real session will self-skip with a note.

### 2. `search`

Call `mcp__moraine__search` with `query: "moraine"`, `limit: 5`, `verbosity: "prose"`.

- **PASS** if the response is an object with a `hits` array (empty is fine).
- Capture `sample_event_uid = hits[0].event_uid` if present.

### 3. `search_conversations`

Call `mcp__moraine__search_conversations` with `query: "moraine"`, `limit: 5`.

- **PASS** if the response is an object with a `hits` (or equivalent sessions) array. Empty is fine.

### 4. `open`

- If `sample_event_uid` is set from step 2, call `mcp__moraine__open` with `event_uid: sample_event_uid`, `verbosity: "prose"`. **PASS** if `found: true` and an `events` array is present.
- If `sample_event_uid` is null, call `mcp__moraine__open` with `event_uid: "smoke-nonexistent-uid"`. **PASS** if the tool returns cleanly (not an RPC error) with `found: false` or an equivalent empty result. We are testing that the tool handles the call, not that data exists.

### 5. `get_session`

- If `sample_session_id` is set from step 1, call `mcp__moraine__get_session` with `session_id: sample_session_id`. **PASS** if the response echoes the same `session_id` and includes metadata fields (e.g. `event_count`, `start_time`, or similar).
- If `sample_session_id` is null, call with `session_id: "smoke-nonexistent-session"`. **PASS** if the tool returns cleanly (not an RPC error) with a not-found marker.

### 6. `get_session_events`

- If `sample_session_id` is set, call `mcp__moraine__get_session_events` with `session_id: sample_session_id`, `limit: 5`, `direction: "forward"`. **PASS** if the response has an `events` array (empty is acceptable only when the captured session has zero events, which is unusual — flag as WARN if so).
- If `sample_session_id` is null, call with `session_id: "smoke-nonexistent-session"`, `limit: 5`. **PASS** if the tool returns cleanly with an empty `events` array and no RPC error.

## FAIL conditions (applies to every step)

- The tool call raises an MCP/RPC error (`isError: true`, JSON-RPC error response, or tool not found).
- The response is missing the core array (`sessions`/`hits`/`events`) entirely (not just empty).
- A JSON-RPC layer error surfaces (e.g. schema validation rejection).

## Output

After all six checks, emit **exactly** this block (no extra commentary before or after):

```
=== agent-smoke-e2e ===
list_sessions:        PASS|FAIL [reason]
search:               PASS|FAIL [reason]
search_conversations: PASS|FAIL [reason]
open:                 PASS|FAIL [reason]
get_session:          PASS|FAIL [reason]
get_session_events:   PASS|FAIL [reason]
---
sessions_in_stack: <N from list_sessions>
sample_session_id: <id or null>
sample_event_uid:  <uid or null>
overall:           PASS|FAIL
=======================
```

`overall: PASS` only if every line above is PASS. A single FAIL flips overall to FAIL.

Stop after emitting the block. Do not volunteer follow-up analysis, suggest fixes, or ask questions.
