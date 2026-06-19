---
name: agent-smoke-e2e
description: End-to-end smoke check of the moraine MCP server. Exercises the public retrieval workflow against a live moraine stack and prints a PASS/FAIL matrix.
---

# agent-smoke-e2e

You are running an end-to-end smoke test of the moraine MCP server.

Your sole job is to call the public MCP retrieval workflow, validate
that responses are shaped as expected, and emit a final pass/fail matrix. Do not
do anything else: no exploration, no reading files, no code changes. Treat this
like a test runner with terse output and clear pass/fail signals.

## Setup Assumptions

- The `moraine` MCP server is registered and exposes `mcp__moraine__search_sessions`, `mcp__moraine__open`, `mcp__moraine__list_sessions`, and `mcp__moraine__file_attention`.
- The stack may or may not have ingested sessions. Both cases are valid; the smoke test cares that tools respond with the contract shape.
- Do not call any tool outside the `mcp__moraine__*` namespace.

## Test Sequence

Run each check below in order. For each check, call the tool with the arguments
listed, inspect the response, then record PASS or FAIL with a one-line reason on
FAIL.

### 1. `search_sessions`

Call `mcp__moraine__search_sessions` with:

```json
{ "query": "moraine", "n_hits": 5 }
```

PASS if the response is an object with:

- `tool: "search_sessions"`
- `schema_version: "moraine.mcp.search_sessions.v1"`
- `data.results` as an array

If `data.results[0]` exists, capture:

- `sample_event_id = data.results[0].open.event_id`
- `sample_turn_id = data.results[0].open.turn_id`
- `sample_session_id = data.results[0].open.session_id`

If there are no results, leave the three sample IDs as `null`.

### 2. `list_sessions`

Call `mcp__moraine__list_sessions` with:

```json
{
  "start_datetime": "1970-01-01T00:00:00Z",
  "end_datetime": "2100-01-01T00:00:00Z",
  "limit": 5
}
```

PASS if the response is an object with:

- `tool: "list_sessions"`
- `schema_version: "moraine.mcp.list_sessions.v1"`
- `data.sessions` as an array

If `data.sessions[0]` exists, capture:

- `listed_session_id = data.sessions[0].open.session_id`

If there are no listed sessions, leave `listed_session_id` as `null`.

### 3. `file_attention`

Call `mcp__moraine__file_attention` with:

```json
{ "path": "Cargo.toml", "scope": "all", "granularity": "events", "limit": 5 }
```

PASS if the response is an object with:

- `tool: "file_attention"`
- `schema_version: "moraine.mcp.file_attention.v1"`
- `data.events` as an array
- `data.summary.total_touches` as a number

If `data.events[0]` exists, capture:

- `file_attention_event_id = data.events[0].open.event_id`
- `file_attention_turn_id = data.events[0].open.turn_id` if present
- `file_attention_session_id = data.events[0].open.session_id`

If there are no events, leave the three file-attention IDs as `null`.

### 4. `open(event_id)`

If `sample_event_id` is set, call `mcp__moraine__open` with:

```json
{ "id": "<sample_event_id>" }
```

PASS if the response is an object with `tool: "open"`, the same `request.id`,
and `data.kind: "event"`.

If `sample_event_id` is null, record `SKIP no search result`.

### 5. `open(turn_id)`

If `sample_turn_id` is set, call `mcp__moraine__open` with:

```json
{ "id": "<sample_turn_id>" }
```

PASS if the response is an object with `tool: "open"`, the same `request.id`,
and `data.kind: "turn"`.

If `sample_turn_id` is null, record `SKIP no search result`.

### 6. `open(session_id)`

If `sample_session_id` is set, call `mcp__moraine__open` with:

```json
{ "id": "<sample_session_id>" }
```

PASS if the response is an object with `tool: "open"`, the same `request.id`,
and `data.kind: "session"`.

If `sample_session_id` is null, call `mcp__moraine__open` with this valid but
nonexistent typed session ID:

```json
{ "id": "session:c21va2Utbm9uZXhpc3RlbnQtc2Vzc2lvbg" }
```

PASS if the tool returns cleanly without an MCP/RPC error and includes an
`error.code` such as `not_found`.

### 7. `open(listed_session_id)`

If `listed_session_id` is set, call `mcp__moraine__open` with:

```json
{ "id": "<listed_session_id>" }
```

PASS if the response is an object with `tool: "open"`, the same `request.id`,
and `data.kind: "session"`.

If `listed_session_id` is null, record `SKIP no listed session`.

### 8. `open(file_attention IDs)`

If `file_attention_event_id` is set, call `mcp__moraine__open` with:

```json
{ "id": "<file_attention_event_id>" }
```

PASS if the response is an object with `tool: "open"`, the same `request.id`,
and `data.kind: "event"`.

If `file_attention_turn_id` is set, call `mcp__moraine__open` with:

```json
{ "id": "<file_attention_turn_id>" }
```

PASS if the response is an object with `tool: "open"`, the same `request.id`,
and `data.kind: "turn"`.

If `file_attention_session_id` is set, call `mcp__moraine__open` with:

```json
{ "id": "<file_attention_session_id>" }
```

PASS if the response is an object with `tool: "open"`, the same `request.id`,
and `data.kind: "session"`.

If there are no file-attention IDs, record `SKIP no file_attention event`.

## FAIL Conditions

- The tool call raises an MCP/RPC error (`isError: true`, JSON-RPC error response, or tool not found).
- The response lacks `tool`, `schema_version`, or the expected `data`/`error` object.
- `open` does not echo the requested `id` in `request.id`.
- `open` returns a successful `data.kind` different from the ID kind being opened.

## Output

After all checks, emit exactly this block and stop:

```text
=== agent-smoke-e2e ===
search_sessions: PASS|FAIL [reason]
list_sessions:   PASS|FAIL [reason]
file_attention:  PASS|FAIL [reason]
open_event_id:   PASS|FAIL|SKIP [reason]
open_turn_id:    PASS|FAIL|SKIP [reason]
open_session_id: PASS|FAIL [reason]
open_listed_session_id: PASS|FAIL|SKIP [reason]
open_file_attention_ids: PASS|FAIL|SKIP [reason]
---
search_results:  <N from search_sessions>
listed_sessions: <N from list_sessions>
file_attention_events: <N from file_attention>
sample_event_id: <id or null>
sample_turn_id:  <id or null>
sample_session_id: <id or null>
listed_session_id: <id or null>
file_attention_event_id: <id or null>
file_attention_turn_id: <id or null>
file_attention_session_id: <id or null>
overall:         PASS|FAIL
=======================
```

`overall: PASS` only if every non-SKIP line is PASS. A single FAIL flips
overall to FAIL.
