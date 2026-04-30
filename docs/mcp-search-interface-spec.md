# MCP Search Interface Specification

This document specifies the desired behavior for Moraine's two primary MCP
retrieval tools:

- `search_sessions`
- `open`

The scope of this document is the external interface contract: accepted inputs,
output shapes, response behavior, errors, performance targets, and success
criteria. It intentionally does not specify storage layout, indexing strategy,
query planning, or implementation internals.

## Status

This is a product and interface specification for the next Moraine MCP search
surface. It should be treated as the behavioral target for implementation.

## Terminology

The interface exposes session history as a hierarchy:

```text
Session
  Turn
    Event
```

Definitions:

- A **session** is one agent conversation or trace file.
- A **turn** is one user-driven interaction within a session.
- An **event** is one recorded item inside a turn, such as user input, assistant
  output, reasoning, tool call, tool response, compaction, or runtime status.
- A **terminal event** is an event that ends a turn and requires new external
  input before the session can continue.
- A **searchable document** is one event eligible for retrieval by
  `search_sessions`.

## Design Principles

- Search returns lookup handles, not full conversations.
- `open` is the only expansion primitive.
- Every returned ID should be usable in a later `open` call.
- Full payloads should appear only when opening an event.
- Session and turn responses should summarize and provide traversal IDs.
- Defaults should favor useful content over noisy internal events.
- Error responses should be explicit and machine-readable.

## Common Interface Rules

### ID Format

MCP IDs are opaque strings. Callers must not parse them for meaning.

The specification examples use typed illustrative IDs:

```text
session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8
turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4
event:evt_01J9Q3Q2C4TD9K7F8M1N5R6P2A
```

Implementations must return stable IDs for the lifetime of the indexed session
data. An ID returned by `search_sessions` or `open` must be accepted by `open`
unless the underlying session data has been deleted or the index has been
rebuilt with incompatible IDs.

### Time Format

All timestamps must be RFC 3339 strings in UTC.

Example:

```text
2026-04-29T18:42:31.125Z
```

### Event Types

The MCP interface should expose a normalized event type vocabulary:

```text
user_input
assistant_response
reasoning
tool_call
tool_response
compaction
system
runtime
unknown
```

Rules:

- Event type names are lowercase snake_case.
- Provider-specific event names should be normalized into this vocabulary.
- Events that cannot be confidently classified should use `unknown`.
- `unknown` events may be opened, but are not searched by default.

### Completion Status

Completion must be reported at session and turn levels.

Session completion:

- `completed: true` means the latest known turn has reached a terminal event.
- `completed: false` means the latest known turn has not reached a terminal
  event, or Moraine cannot prove that it has.

Turn completion:

- `completed: true` means the turn has a terminal event.
- `completed: false` means no terminal event is known.

Terminal event types include:

- final assistant response
- cancellation
- user interrupt
- crash
- runtime exit
- other stop condition

### Response Envelope

Successful tool responses must use a top-level JSON object with this common
shape:

```json
{
  "schema_version": "moraine.mcp.<tool>.v1",
  "tool": "<tool>",
  "request": {},
  "data": {},
  "warnings": [],
  "performance": {
    "elapsed_ms": 42,
    "sla_target_ms": 750,
    "met_sla": true
  }
}
```

Field rules:

- `schema_version` identifies the response schema.
- `tool` is the MCP tool name.
- `request` contains the canonicalized request after defaults are applied.
- `data` contains the tool-specific payload.
- `warnings` is always present and is an array.
- `performance.elapsed_ms` is the measured end-to-end tool handling time.
- `performance.sla_target_ms` is the target that applied to this request.
- `performance.met_sla` indicates whether the request met its target.

### Error Envelope

Rejected requests and failed lookups must return a machine-readable error.
These are in-band tool results, not MCP/JSON-RPC transport failures; the MCP
tool result wrapper should therefore be returned cleanly with the error object
in `structuredContent`.

```json
{
  "schema_version": "moraine.mcp.error.v1",
  "tool": "<tool>",
  "request": {},
  "error": {
    "code": "invalid_request",
    "message": "query must be a non-empty string",
    "details": {
      "field": "query"
    }
  },
  "warnings": [],
  "performance": {
    "elapsed_ms": 3,
    "sla_target_ms": 750,
    "met_sla": true
  }
}
```

Required error codes:

```text
invalid_request
invalid_id
not_found
unsupported_event_type
deadline_exceeded
internal_error
```

Error behavior:

- Invalid inputs must not be silently coerced, except where this specification
  explicitly defines normalization.
- Missing IDs must return `not_found`.
- Malformed IDs must return `invalid_id`.
- Unknown event type filters must return `unsupported_event_type`.
- Runtime failures must return `internal_error` with a concise message.

## Tool: `search_sessions`

### Purpose

`search_sessions` finds relevant events across Moraine history and returns
compact handles into the session graph.

It answers:

```text
Where should I look?
```

It does not return full event payloads, full turns, or full sessions.

### Request Schema

```ts
search_sessions({
  query: string,
  within_id?: string | null,
  event_types?: string[] | null,
  n_hits?: number | null
})
```

### Accepted Inputs

`query`:

- Required.
- Must be a string.
- Must contain at least one non-whitespace character after trimming.
- Maximum length: 4096 characters.
- The original query text should be preserved in the canonical request except
  for trimming leading and trailing whitespace.

`within_id`:

- Optional.
- May be omitted or `null`.
- When omitted or `null`, search covers all indexed sessions visible to the
  Moraine configuration.
- May be a session ID returned by Moraine.
- May be a turn ID returned by Moraine.
- Must not be an event ID in v1.

`event_types`:

- Optional.
- May be omitted or `null`.
- When omitted or `null`, defaults to:

```json
["user_input", "assistant_response", "tool_response"]
```

- May contain any supported event type except `unknown`.
- Must contain at least one event type after normalization.
- Duplicate event types should be de-duplicated in the canonical request.
- Event type order in the canonical request should follow Moraine's normalized
  event type order, not caller order.

`n_hits`:

- Optional.
- May be omitted or `null`.
- Default: `10`.
- Minimum: `1`.
- Maximum: `50`.
- Must be an integer.

### Search Scope Behavior

When `within_id` is omitted:

- Search all indexed sessions.
- Return hits from any matching session.

When `within_id` is a session ID:

- Search only events whose `session.id` matches that session.
- If the session exists but has no matching events, return an empty result set.
- If the session ID does not exist, return `not_found`.

When `within_id` is a turn ID:

- Search only events whose `turn.id` matches that turn.
- If the turn exists but has no matching events, return an empty result set.
- If the turn ID does not exist, return `not_found`.

When `within_id` is an event ID:

- Return `invalid_request`.
- Message: `within_id accepts session and turn IDs, not event IDs`.

### Event Type Filter Behavior

Default behavior searches:

```text
user_input
assistant_response
tool_response
```

This default intentionally omits:

```text
reasoning
tool_call
compaction
system
runtime
unknown
```

Callers may explicitly include supported omitted types when they need them.

Examples:

```json
{
  "query": "cargo clippy failure",
  "event_types": ["reasoning", "tool_call", "tool_response"]
}
```

```json
{
  "query": "sandbox monitor health endpoint",
  "within_id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8"
}
```

### Ranking Behavior

Results must be ordered by:

1. Relevance score descending.
2. Event timestamp descending for ties.
3. Event ID ascending for remaining ties.

The score must be a number between `0.0` and `1.0`.

Scores are only required to be comparable within a single response. Callers
should not compare scores across different queries.

### Successful Response Shape

```json
{
  "schema_version": "moraine.mcp.search_sessions.v1",
  "tool": "search_sessions",
  "request": {
    "query": "clickhouse schema migration failure",
    "within_id": null,
    "event_types": ["user_input", "assistant_response", "tool_response"],
    "n_hits": 10
  },
  "data": {
    "result_count": 2,
    "limit": 10,
    "truncated": false,
    "results": []
  },
  "warnings": [],
  "performance": {
    "elapsed_ms": 64,
    "sla_target_ms": 750,
    "met_sla": true
  }
}
```

`data.result_count`:

- Number of hits returned.
- Must be less than or equal to `data.limit`.

`data.limit`:

- The canonical `n_hits` value.

`data.truncated`:

- `true` when more matches may exist beyond `data.limit`.
- `false` when the response includes all matches known to the query execution.

`data.results`:

- Ordered array of search hits.

### Search Hit Shape

Each hit must have this shape:

```json
{
  "rank": 1,
  "score": 0.82,
  "id": "event:evt_01J9Q3Q2C4TD9K7F8M1N5R6P2A",
  "event": {
    "id": "event:evt_01J9Q3Q2C4TD9K7F8M1N5R6P2A",
    "type": "user_input",
    "timestamp": "2026-04-29T18:42:31.125Z",
    "ordinal": 1,
    "terminal": false
  },
  "turn": {
    "id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
    "ordinal": 7,
    "completed": true,
    "event_count": 12
  },
  "session": {
    "id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8",
    "title": "Fix ClickHouse schema migration failure",
    "source": "codex",
    "started_at": "2026-04-29T18:41:55.000Z",
    "updated_at": "2026-04-29T19:03:12.442Z",
    "completed": true
  },
  "snippet": {
    "text": "The ClickHouse schema migration is failing after adding the event ordinal column...",
    "truncated": true
  },
  "open": {
    "event_id": "event:evt_01J9Q3Q2C4TD9K7F8M1N5R6P2A",
    "turn_id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
    "session_id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8"
  }
}
```

Field rules:

- `rank` is one-based.
- `id` is the same as `event.id`.
- `event.ordinal` is one-based within the parent turn.
- `turn.ordinal` is one-based within the parent session.
- `snippet.text` should be short enough to scan and must not contain a full
  payload when the event content is large.
- `open` repeats the IDs callers are expected to use next.

### Example: Default Global Search

Request:

```json
{
  "query": "ClickHouse schema migration failure"
}
```

Response:

```json
{
  "schema_version": "moraine.mcp.search_sessions.v1",
  "tool": "search_sessions",
  "request": {
    "query": "ClickHouse schema migration failure",
    "within_id": null,
    "event_types": ["user_input", "assistant_response", "tool_response"],
    "n_hits": 10
  },
  "data": {
    "result_count": 2,
    "limit": 10,
    "truncated": false,
    "results": [
      {
        "rank": 1,
        "score": 0.91,
        "id": "event:evt_01J9Q3Q2C4TD9K7F8M1N5R6P2A",
        "event": {
          "id": "event:evt_01J9Q3Q2C4TD9K7F8M1N5R6P2A",
          "type": "user_input",
          "timestamp": "2026-04-29T18:42:31.125Z",
          "ordinal": 1,
          "terminal": false
        },
        "turn": {
          "id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
          "ordinal": 7,
          "completed": true,
          "event_count": 12
        },
        "session": {
          "id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8",
          "title": "Fix ClickHouse schema migration failure",
          "source": "codex",
          "started_at": "2026-04-29T18:41:55.000Z",
          "updated_at": "2026-04-29T19:03:12.442Z",
          "completed": true
        },
        "snippet": {
          "text": "The ClickHouse schema migration is failing after adding the event ordinal column...",
          "truncated": true
        },
        "open": {
          "event_id": "event:evt_01J9Q3Q2C4TD9K7F8M1N5R6P2A",
          "turn_id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
          "session_id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8"
        }
      },
      {
        "rank": 2,
        "score": 0.78,
        "id": "event:evt_01J9Q4A91M7S4V3BK2Y5N6X8D9",
        "event": {
          "id": "event:evt_01J9Q4A91M7S4V3BK2Y5N6X8D9",
          "type": "assistant_response",
          "timestamp": "2026-04-29T19:02:48.030Z",
          "ordinal": 12,
          "terminal": true
        },
        "turn": {
          "id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
          "ordinal": 7,
          "completed": true,
          "event_count": 12
        },
        "session": {
          "id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8",
          "title": "Fix ClickHouse schema migration failure",
          "source": "codex",
          "started_at": "2026-04-29T18:41:55.000Z",
          "updated_at": "2026-04-29T19:03:12.442Z",
          "completed": true
        },
        "snippet": {
          "text": "Updated the migration ordering and verified the ClickHouse schema applies cleanly...",
          "truncated": true
        },
        "open": {
          "event_id": "event:evt_01J9Q4A91M7S4V3BK2Y5N6X8D9",
          "turn_id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
          "session_id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8"
        }
      }
    ]
  },
  "warnings": [],
  "performance": {
    "elapsed_ms": 64,
    "sla_target_ms": 750,
    "met_sla": true
  }
}
```

### Example: Search Within A Turn

Request:

```json
{
  "query": "cargo test failure",
  "within_id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
  "event_types": ["tool_response"],
  "n_hits": 3
}
```

Response:

```json
{
  "schema_version": "moraine.mcp.search_sessions.v1",
  "tool": "search_sessions",
  "request": {
    "query": "cargo test failure",
    "within_id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
    "event_types": ["tool_response"],
    "n_hits": 3
  },
  "data": {
    "result_count": 1,
    "limit": 3,
    "truncated": false,
    "results": [
      {
        "rank": 1,
        "score": 0.86,
        "id": "event:evt_01J9Q45J7G6KN92PV4RB8M2N0H",
        "event": {
          "id": "event:evt_01J9Q45J7G6KN92PV4RB8M2N0H",
          "type": "tool_response",
          "timestamp": "2026-04-29T18:56:02.810Z",
          "ordinal": 9,
          "terminal": false
        },
        "turn": {
          "id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
          "ordinal": 7,
          "completed": true,
          "event_count": 12
        },
        "session": {
          "id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8",
          "title": "Fix ClickHouse schema migration failure",
          "source": "codex",
          "started_at": "2026-04-29T18:41:55.000Z",
          "updated_at": "2026-04-29T19:03:12.442Z",
          "completed": true
        },
        "snippet": {
          "text": "cargo test --workspace --locked failed in moraine-clickhouse-client...",
          "truncated": true
        },
        "open": {
          "event_id": "event:evt_01J9Q45J7G6KN92PV4RB8M2N0H",
          "turn_id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
          "session_id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8"
        }
      }
    ]
  },
  "warnings": [],
  "performance": {
    "elapsed_ms": 18,
    "sla_target_ms": 300,
    "met_sla": true
  }
}
```

### Example: No Hits

Request:

```json
{
  "query": "nonexistent exact phrase for this index",
  "n_hits": 5
}
```

Response:

```json
{
  "schema_version": "moraine.mcp.search_sessions.v1",
  "tool": "search_sessions",
  "request": {
    "query": "nonexistent exact phrase for this index",
    "within_id": null,
    "event_types": ["user_input", "assistant_response", "tool_response"],
    "n_hits": 5
  },
  "data": {
    "result_count": 0,
    "limit": 5,
    "truncated": false,
    "results": []
  },
  "warnings": [],
  "performance": {
    "elapsed_ms": 21,
    "sla_target_ms": 750,
    "met_sla": true
  }
}
```

### Search Error Examples

Blank query:

```json
{
  "schema_version": "moraine.mcp.error.v1",
  "tool": "search_sessions",
  "request": {
    "query": "   "
  },
  "error": {
    "code": "invalid_request",
    "message": "query must be a non-empty string",
    "details": {
      "field": "query"
    }
  },
  "warnings": [],
  "performance": {
    "elapsed_ms": 2,
    "sla_target_ms": 750,
    "met_sla": true
  }
}
```

Unsupported event type:

```json
{
  "schema_version": "moraine.mcp.error.v1",
  "tool": "search_sessions",
  "request": {
    "query": "migration",
    "event_types": ["user_input", "debug_trace"]
  },
  "error": {
    "code": "unsupported_event_type",
    "message": "unsupported event type: debug_trace",
    "details": {
      "field": "event_types",
      "supported": [
        "user_input",
        "assistant_response",
        "reasoning",
        "tool_call",
        "tool_response",
        "compaction",
        "system",
        "runtime"
      ]
    }
  },
  "warnings": [],
  "performance": {
    "elapsed_ms": 2,
    "sla_target_ms": 750,
    "met_sla": true
  }
}
```

## Tool: `open`

### Purpose

`open` expands a stable Moraine ID into structured context.

It answers:

```text
Show me more about this thing.
```

The same tool opens sessions, turns, and events. The response type depends on
the ID kind.

### Request Schema

```ts
open({
  id: string
})
```

### Accepted Inputs

`id`:

- Required.
- Must be a string.
- Must be a valid Moraine MCP ID returned by `search_sessions` or `open`.
- May refer to a session, turn, or event.

Invalid inputs:

- Missing `id` returns `invalid_request`.
- Empty or whitespace-only `id` returns `invalid_request`.
- Malformed IDs return `invalid_id`.
- Well-formed but unknown IDs return `not_found`.

### Successful Response Shape

All successful `open` responses use this envelope:

```json
{
  "schema_version": "moraine.mcp.open.v1",
  "tool": "open",
  "request": {
    "id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8"
  },
  "data": {
    "kind": "session"
  },
  "warnings": [],
  "performance": {
    "elapsed_ms": 32,
    "sla_target_ms": 500,
    "met_sla": true
  }
}
```

`data.kind` must be one of:

```text
session
turn
event
```

### Opening A Session

Opening a session returns session metadata plus a compact list of turns.

It must not return full event payloads.

Session response shape:

```json
{
  "kind": "session",
  "session": {
    "id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8",
    "title": "Fix ClickHouse schema migration failure",
    "source": "codex",
    "started_at": "2026-04-29T18:41:55.000Z",
    "updated_at": "2026-04-29T19:03:12.442Z",
    "completed": true,
    "turn_count": 2,
    "event_count": 18
  },
  "turns": [],
  "traversal": {
    "previous_session_id": null,
    "next_session_id": null
  }
}
```

Turn summary shape:

```json
{
  "id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
  "ordinal": 1,
  "completed": true,
  "terminal_event_id": "event:evt_01J9Q4A91M7S4V3BK2Y5N6X8D9",
  "event_count": 12,
  "started_at": "2026-04-29T18:42:31.125Z",
  "updated_at": "2026-04-29T19:02:48.030Z",
  "user_input": {
    "event_id": "event:evt_01J9Q3Q2C4TD9K7F8M1N5R6P2A",
    "text": "The ClickHouse schema migration is failing after adding the event ordinal column...",
    "truncated": true
  },
  "final_response": {
    "event_id": "event:evt_01J9Q4A91M7S4V3BK2Y5N6X8D9",
    "text": "Updated the migration ordering and verified the ClickHouse schema applies cleanly...",
    "truncated": true
  },
  "tools_called": ["exec_command", "apply_patch"],
  "event_types": ["user_input", "tool_call", "tool_response", "assistant_response"],
  "open": {
    "turn_id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
    "terminal_event_id": "event:evt_01J9Q4A91M7S4V3BK2Y5N6X8D9"
  }
}
```

Rules:

- `turns` are ordered by `ordinal` ascending.
- `turns` must include every known turn in the session.
- `user_input` may be `null` if no user input event is known.
- `final_response` may be `null` if the turn is incomplete or ended without a
  final assistant response.
- `tools_called` must contain unique tool names in first-seen order.
- `event_types` must contain unique event types in first-seen order.
- `terminal_event_id` may be `null` when `completed` is `false`.

Example:

```json
{
  "schema_version": "moraine.mcp.open.v1",
  "tool": "open",
  "request": {
    "id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8"
  },
  "data": {
    "kind": "session",
    "session": {
      "id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8",
      "title": "Fix ClickHouse schema migration failure",
      "source": "codex",
      "started_at": "2026-04-29T18:41:55.000Z",
      "updated_at": "2026-04-29T19:03:12.442Z",
      "completed": true,
      "turn_count": 2,
      "event_count": 18
    },
    "turns": [
      {
        "id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
        "ordinal": 1,
        "completed": true,
        "terminal_event_id": "event:evt_01J9Q4A91M7S4V3BK2Y5N6X8D9",
        "event_count": 12,
        "started_at": "2026-04-29T18:42:31.125Z",
        "updated_at": "2026-04-29T19:02:48.030Z",
        "user_input": {
          "event_id": "event:evt_01J9Q3Q2C4TD9K7F8M1N5R6P2A",
          "text": "The ClickHouse schema migration is failing after adding the event ordinal column...",
          "truncated": true
        },
        "final_response": {
          "event_id": "event:evt_01J9Q4A91M7S4V3BK2Y5N6X8D9",
          "text": "Updated the migration ordering and verified the ClickHouse schema applies cleanly...",
          "truncated": true
        },
        "tools_called": ["exec_command", "apply_patch"],
        "event_types": ["user_input", "tool_call", "tool_response", "assistant_response"],
        "open": {
          "turn_id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
          "terminal_event_id": "event:evt_01J9Q4A91M7S4V3BK2Y5N6X8D9"
        }
      },
      {
        "id": "turn:turn_01J9Q4P93SP6B2D5M8K7V1H4NC",
        "ordinal": 2,
        "completed": true,
        "terminal_event_id": "event:evt_01J9Q4T0N8D6W3F2HK5P7M9VX1",
        "event_count": 6,
        "started_at": "2026-04-29T19:02:59.510Z",
        "updated_at": "2026-04-29T19:03:12.442Z",
        "user_input": {
          "event_id": "event:evt_01J9Q4P93SP6B2D5M8K7V1H4NC",
          "text": "Can you summarize the validation?",
          "truncated": false
        },
        "final_response": {
          "event_id": "event:evt_01J9Q4T0N8D6W3F2HK5P7M9VX1",
          "text": "Validation passed with make docs-build and cargo test for the affected crate.",
          "truncated": false
        },
        "tools_called": [],
        "event_types": ["user_input", "assistant_response"],
        "open": {
          "turn_id": "turn:turn_01J9Q4P93SP6B2D5M8K7V1H4NC",
          "terminal_event_id": "event:evt_01J9Q4T0N8D6W3F2HK5P7M9VX1"
        }
      }
    ],
    "traversal": {
      "previous_session_id": null,
      "next_session_id": null
    }
  },
  "warnings": [],
  "performance": {
    "elapsed_ms": 32,
    "sla_target_ms": 500,
    "met_sla": true
  }
}
```

### Opening A Turn

Opening a turn returns a compact view of that turn plus ordered event handles.

It must not return full payloads for every event.

Turn response shape:

```json
{
  "kind": "turn",
  "turn": {
    "id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
    "session_id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8",
    "ordinal": 1,
    "completed": true,
    "terminal_event_id": "event:evt_01J9Q4A91M7S4V3BK2Y5N6X8D9",
    "event_count": 12,
    "started_at": "2026-04-29T18:42:31.125Z",
    "updated_at": "2026-04-29T19:02:48.030Z"
  },
  "session": {
    "id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8",
    "title": "Fix ClickHouse schema migration failure",
    "source": "codex"
  },
  "summary": {
    "user_input": {},
    "final_response": {},
    "tools_called": [],
    "event_types": []
  },
  "events": [],
  "traversal": {}
}
```

Event summary shape inside `events`:

```json
{
  "id": "event:evt_01J9Q45J7G6KN92PV4RB8M2N0H",
  "ordinal": 9,
  "type": "tool_response",
  "timestamp": "2026-04-29T18:56:02.810Z",
  "terminal": false,
  "tool_name": "exec_command",
  "model": null,
  "summary": "cargo test --workspace --locked failed in moraine-clickhouse-client...",
  "truncated": true
}
```

Rules:

- `events` are ordered by `ordinal` ascending.
- `events` must include every known event in the turn.
- Event summaries are compact. Full event content is available through
  `open(event_id)`.
- `summary.user_input` may be `null`.
- `summary.final_response` may be `null`.
- `summary.tools_called` must contain unique tool names in first-seen order.
- `summary.event_types` must contain unique event types in first-seen order.

Complete turn example:

```json
{
  "schema_version": "moraine.mcp.open.v1",
  "tool": "open",
  "request": {
    "id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4"
  },
  "data": {
    "kind": "turn",
    "turn": {
      "id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
      "session_id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8",
      "ordinal": 1,
      "completed": true,
      "terminal_event_id": "event:evt_01J9Q4A91M7S4V3BK2Y5N6X8D9",
      "event_count": 4,
      "started_at": "2026-04-29T18:42:31.125Z",
      "updated_at": "2026-04-29T19:02:48.030Z"
    },
    "session": {
      "id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8",
      "title": "Fix ClickHouse schema migration failure",
      "source": "codex"
    },
    "summary": {
      "user_input": {
        "event_id": "event:evt_01J9Q3Q2C4TD9K7F8M1N5R6P2A",
        "text": "The ClickHouse schema migration is failing after adding the event ordinal column...",
        "truncated": true
      },
      "final_response": {
        "event_id": "event:evt_01J9Q4A91M7S4V3BK2Y5N6X8D9",
        "text": "Updated the migration ordering and verified the ClickHouse schema applies cleanly...",
        "truncated": true
      },
      "tools_called": ["exec_command"],
      "event_types": ["user_input", "tool_call", "tool_response", "assistant_response"]
    },
    "events": [
      {
        "id": "event:evt_01J9Q3Q2C4TD9K7F8M1N5R6P2A",
        "ordinal": 1,
        "type": "user_input",
        "timestamp": "2026-04-29T18:42:31.125Z",
        "terminal": false,
        "tool_name": null,
        "model": null,
        "summary": "The ClickHouse schema migration is failing after adding the event ordinal column...",
        "truncated": true
      },
      {
        "id": "event:evt_01J9Q43T4HB1W7V69N2CM8K5D0",
        "ordinal": 2,
        "type": "tool_call",
        "timestamp": "2026-04-29T18:55:59.001Z",
        "terminal": false,
        "tool_name": "exec_command",
        "model": null,
        "summary": "cargo test --workspace --locked",
        "truncated": false
      },
      {
        "id": "event:evt_01J9Q45J7G6KN92PV4RB8M2N0H",
        "ordinal": 3,
        "type": "tool_response",
        "timestamp": "2026-04-29T18:56:02.810Z",
        "terminal": false,
        "tool_name": "exec_command",
        "model": null,
        "summary": "cargo test --workspace --locked failed in moraine-clickhouse-client...",
        "truncated": true
      },
      {
        "id": "event:evt_01J9Q4A91M7S4V3BK2Y5N6X8D9",
        "ordinal": 4,
        "type": "assistant_response",
        "timestamp": "2026-04-29T19:02:48.030Z",
        "terminal": true,
        "tool_name": null,
        "model": "gpt-5",
        "summary": "Updated the migration ordering and verified the ClickHouse schema applies cleanly...",
        "truncated": true
      }
    ],
    "traversal": {
      "session_id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8",
      "previous_turn_id": null,
      "next_turn_id": "turn:turn_01J9Q4P93SP6B2D5M8K7V1H4NC",
      "first_event_id": "event:evt_01J9Q3Q2C4TD9K7F8M1N5R6P2A",
      "last_event_id": "event:evt_01J9Q4A91M7S4V3BK2Y5N6X8D9"
    }
  },
  "warnings": [],
  "performance": {
    "elapsed_ms": 19,
    "sla_target_ms": 300,
    "met_sla": true
  }
}
```

Incomplete turn example:

```json
{
  "schema_version": "moraine.mcp.open.v1",
  "tool": "open",
  "request": {
    "id": "turn:turn_01J9R1A7X2C6D5E4F3G2H1J9K8"
  },
  "data": {
    "kind": "turn",
    "turn": {
      "id": "turn:turn_01J9R1A7X2C6D5E4F3G2H1J9K8",
      "session_id": "session:ses_01J9R19PV0A2B3C4D5E6F7G8H9",
      "ordinal": 4,
      "completed": false,
      "terminal_event_id": null,
      "event_count": 2,
      "started_at": "2026-04-29T20:12:00.000Z",
      "updated_at": "2026-04-29T20:12:08.000Z"
    },
    "session": {
      "id": "session:ses_01J9R19PV0A2B3C4D5E6F7G8H9",
      "title": "Investigate monitor startup",
      "source": "codex"
    },
    "summary": {
      "user_input": {
        "event_id": "event:evt_01J9R1B1CZ0K6P9Q8R7S6T5V4W",
        "text": "Check whether the monitor starts after the latest config change.",
        "truncated": false
      },
      "final_response": null,
      "tools_called": ["exec_command"],
      "event_types": ["user_input", "tool_call"]
    },
    "events": [
      {
        "id": "event:evt_01J9R1B1CZ0K6P9Q8R7S6T5V4W",
        "ordinal": 1,
        "type": "user_input",
        "timestamp": "2026-04-29T20:12:00.000Z",
        "terminal": false,
        "tool_name": null,
        "model": null,
        "summary": "Check whether the monitor starts after the latest config change.",
        "truncated": false
      },
      {
        "id": "event:evt_01J9R1B9N8M7L6K5J4H3G2F1E0",
        "ordinal": 2,
        "type": "tool_call",
        "timestamp": "2026-04-29T20:12:08.000Z",
        "terminal": false,
        "tool_name": "exec_command",
        "model": null,
        "summary": "bin/moraine status",
        "truncated": false
      }
    ],
    "traversal": {
      "session_id": "session:ses_01J9R19PV0A2B3C4D5E6F7G8H9",
      "previous_turn_id": "turn:turn_01J9R18Y8W7V6T5S4R3Q2P1N0M",
      "next_turn_id": null,
      "first_event_id": "event:evt_01J9R1B1CZ0K6P9Q8R7S6T5V4W",
      "last_event_id": "event:evt_01J9R1B9N8M7L6K5J4H3G2F1E0"
    }
  },
  "warnings": [],
  "performance": {
    "elapsed_ms": 16,
    "sla_target_ms": 300,
    "met_sla": true
  }
}
```

### Opening An Event

Opening an event returns full event metadata and full event content.

Event response shape:

```json
{
  "kind": "event",
  "event": {
    "id": "event:evt_01J9Q45J7G6KN92PV4RB8M2N0H",
    "session_id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8",
    "turn_id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
    "ordinal": 3,
    "type": "tool_response",
    "timestamp": "2026-04-29T18:56:02.810Z",
    "terminal": false,
    "model": null,
    "originating_model": "gpt-5",
    "tool_name": "exec_command"
  },
  "content": {},
  "session": {},
  "turn": {},
  "traversal": {}
}
```

Common content fields:

```json
{
  "format": "text",
  "text": "full event text",
  "truncated": false
}
```

Rules:

- `content.truncated` must be `false` for normal event opens.
- `content.text` must contain the full available event text for text-like
  events.
- Structured event payloads may include additional fields, but `content.text`
  should provide a human-readable representation when available.
- Tool calls should include tool name and arguments.
- Tool responses should include tool name, exit status when available, and full
  output when available.
- Reasoning events should be openable when captured and permitted by the local
  data source.

User input event example:

```json
{
  "schema_version": "moraine.mcp.open.v1",
  "tool": "open",
  "request": {
    "id": "event:evt_01J9Q3Q2C4TD9K7F8M1N5R6P2A"
  },
  "data": {
    "kind": "event",
    "event": {
      "id": "event:evt_01J9Q3Q2C4TD9K7F8M1N5R6P2A",
      "session_id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8",
      "turn_id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
      "ordinal": 1,
      "type": "user_input",
      "timestamp": "2026-04-29T18:42:31.125Z",
      "terminal": false,
      "model": null,
      "originating_model": null,
      "tool_name": null
    },
    "content": {
      "format": "text",
      "text": "The ClickHouse schema migration is failing after adding the event ordinal column. Please find the issue and fix it.",
      "truncated": false
    },
    "session": {
      "id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8",
      "title": "Fix ClickHouse schema migration failure",
      "source": "codex"
    },
    "turn": {
      "id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
      "ordinal": 1,
      "completed": true
    },
    "traversal": {
      "session_id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8",
      "turn_id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
      "previous_event_id": null,
      "next_event_id": "event:evt_01J9Q43T4HB1W7V69N2CM8K5D0",
      "previous_turn_id": null,
      "next_turn_id": "turn:turn_01J9Q4P93SP6B2D5M8K7V1H4NC"
    }
  },
  "warnings": [],
  "performance": {
    "elapsed_ms": 9,
    "sla_target_ms": 200,
    "met_sla": true
  }
}
```

Tool call event example:

```json
{
  "schema_version": "moraine.mcp.open.v1",
  "tool": "open",
  "request": {
    "id": "event:evt_01J9Q43T4HB1W7V69N2CM8K5D0"
  },
  "data": {
    "kind": "event",
    "event": {
      "id": "event:evt_01J9Q43T4HB1W7V69N2CM8K5D0",
      "session_id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8",
      "turn_id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
      "ordinal": 2,
      "type": "tool_call",
      "timestamp": "2026-04-29T18:55:59.001Z",
      "terminal": false,
      "model": null,
      "originating_model": "gpt-5",
      "tool_name": "exec_command"
    },
    "content": {
      "format": "tool_call",
      "tool_name": "exec_command",
      "arguments": {
        "cmd": "cargo test --workspace --locked",
        "workdir": "/Users/eric/src/moraine"
      },
      "text": "exec_command(cmd=\"cargo test --workspace --locked\", workdir=\"/Users/eric/src/moraine\")",
      "truncated": false
    },
    "session": {
      "id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8",
      "title": "Fix ClickHouse schema migration failure",
      "source": "codex"
    },
    "turn": {
      "id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
      "ordinal": 1,
      "completed": true
    },
    "traversal": {
      "session_id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8",
      "turn_id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
      "previous_event_id": "event:evt_01J9Q3Q2C4TD9K7F8M1N5R6P2A",
      "next_event_id": "event:evt_01J9Q45J7G6KN92PV4RB8M2N0H",
      "previous_turn_id": null,
      "next_turn_id": "turn:turn_01J9Q4P93SP6B2D5M8K7V1H4NC"
    }
  },
  "warnings": [],
  "performance": {
    "elapsed_ms": 8,
    "sla_target_ms": 200,
    "met_sla": true
  }
}
```

Tool response event example:

```json
{
  "schema_version": "moraine.mcp.open.v1",
  "tool": "open",
  "request": {
    "id": "event:evt_01J9Q45J7G6KN92PV4RB8M2N0H"
  },
  "data": {
    "kind": "event",
    "event": {
      "id": "event:evt_01J9Q45J7G6KN92PV4RB8M2N0H",
      "session_id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8",
      "turn_id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
      "ordinal": 3,
      "type": "tool_response",
      "timestamp": "2026-04-29T18:56:02.810Z",
      "terminal": false,
      "model": null,
      "originating_model": "gpt-5",
      "tool_name": "exec_command"
    },
    "content": {
      "format": "tool_response",
      "tool_name": "exec_command",
      "exit_code": 101,
      "text": "cargo test --workspace --locked failed in moraine-clickhouse-client\n\nfailures:\n  migrations_apply_in_order\n",
      "truncated": false
    },
    "session": {
      "id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8",
      "title": "Fix ClickHouse schema migration failure",
      "source": "codex"
    },
    "turn": {
      "id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
      "ordinal": 1,
      "completed": true
    },
    "traversal": {
      "session_id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8",
      "turn_id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
      "previous_event_id": "event:evt_01J9Q43T4HB1W7V69N2CM8K5D0",
      "next_event_id": "event:evt_01J9Q4A91M7S4V3BK2Y5N6X8D9",
      "previous_turn_id": null,
      "next_turn_id": "turn:turn_01J9Q4P93SP6B2D5M8K7V1H4NC"
    }
  },
  "warnings": [],
  "performance": {
    "elapsed_ms": 10,
    "sla_target_ms": 200,
    "met_sla": true
  }
}
```

Assistant response event example:

```json
{
  "schema_version": "moraine.mcp.open.v1",
  "tool": "open",
  "request": {
    "id": "event:evt_01J9Q4A91M7S4V3BK2Y5N6X8D9"
  },
  "data": {
    "kind": "event",
    "event": {
      "id": "event:evt_01J9Q4A91M7S4V3BK2Y5N6X8D9",
      "session_id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8",
      "turn_id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
      "ordinal": 4,
      "type": "assistant_response",
      "timestamp": "2026-04-29T19:02:48.030Z",
      "terminal": true,
      "model": "gpt-5",
      "originating_model": "gpt-5",
      "tool_name": null
    },
    "content": {
      "format": "text",
      "text": "Updated the migration ordering and verified the ClickHouse schema applies cleanly. Validation: cargo test --workspace --locked passed.",
      "truncated": false
    },
    "session": {
      "id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8",
      "title": "Fix ClickHouse schema migration failure",
      "source": "codex"
    },
    "turn": {
      "id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
      "ordinal": 1,
      "completed": true
    },
    "traversal": {
      "session_id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8",
      "turn_id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
      "previous_event_id": "event:evt_01J9Q45J7G6KN92PV4RB8M2N0H",
      "next_event_id": null,
      "previous_turn_id": null,
      "next_turn_id": "turn:turn_01J9Q4P93SP6B2D5M8K7V1H4NC"
    }
  },
  "warnings": [],
  "performance": {
    "elapsed_ms": 9,
    "sla_target_ms": 200,
    "met_sla": true
  }
}
```

### Open Error Examples

Malformed ID:

```json
{
  "schema_version": "moraine.mcp.error.v1",
  "tool": "open",
  "request": {
    "id": "not-a-valid-id"
  },
  "error": {
    "code": "invalid_id",
    "message": "id is not a valid Moraine MCP ID",
    "details": {
      "field": "id"
    }
  },
  "warnings": [],
  "performance": {
    "elapsed_ms": 1,
    "sla_target_ms": 200,
    "met_sla": true
  }
}
```

Missing object:

```json
{
  "schema_version": "moraine.mcp.error.v1",
  "tool": "open",
  "request": {
    "id": "event:evt_01J9DOESNOTEXIST000000000000"
  },
  "error": {
    "code": "not_found",
    "message": "event not found",
    "details": {
      "id": "event:evt_01J9DOESNOTEXIST000000000000"
    }
  },
  "warnings": [],
  "performance": {
    "elapsed_ms": 4,
    "sla_target_ms": 200,
    "met_sla": true
  }
}
```

## Input Permutation Matrix

### `search_sessions`

| Input combination | Expected behavior |
|---|---|
| `query` only | Search all sessions with default event types and default `n_hits`. |
| `query` + `n_hits` | Search all sessions with default event types and requested hit count. |
| `query` + `event_types` | Search all sessions limited to those event types. |
| `query` + `within_id=session` | Search only that session with default event types. |
| `query` + `within_id=turn` | Search only that turn with default event types. |
| `query` + `within_id=session` + `event_types` | Search only that session and only those event types. |
| `query` + `within_id=turn` + `event_types` | Search only that turn and only those event types. |
| `query` + `within_id=event` | Return `invalid_request`. |
| blank `query` | Return `invalid_request`. |
| unknown `within_id` | Return `not_found` if ID is well-formed. |
| malformed `within_id` | Return `invalid_id`. |
| empty `event_types` | Return `invalid_request`. |
| unsupported `event_types` value | Return `unsupported_event_type`. |
| `n_hits < 1` | Return `invalid_request`. |
| `n_hits > 50` | Return `invalid_request`. |
| non-integer `n_hits` | Return `invalid_request`. |

### `open`

| Input combination | Expected behavior |
|---|---|
| `id=session` | Return session metadata and compact turn list. |
| `id=turn` | Return turn metadata, compact event list, and traversal references. |
| `id=event` | Return event metadata, full content, and traversal references. |
| missing `id` | Return `invalid_request`. |
| blank `id` | Return `invalid_request`. |
| malformed `id` | Return `invalid_id`. |
| well-formed unknown `id` | Return `not_found`. |

## Traversal Contract

The interface succeeds only if callers can move through history without
inventing IDs or issuing unrelated searches.

Required traversal paths:

- Search hit to event: `search_sessions(...).data.results[].open.event_id`
- Search hit to turn: `search_sessions(...).data.results[].open.turn_id`
- Search hit to session: `search_sessions(...).data.results[].open.session_id`
- Event to parent turn: `open(event).data.traversal.turn_id`
- Event to parent session: `open(event).data.traversal.session_id`
- Event to adjacent event: `open(event).data.traversal.previous_event_id` and
  `next_event_id`
- Event to adjacent turn: `open(event).data.traversal.previous_turn_id` and
  `next_turn_id`
- Turn to parent session: `open(turn).data.traversal.session_id`
- Turn to adjacent turn: `open(turn).data.traversal.previous_turn_id` and
  `next_turn_id`
- Turn to first and last event: `open(turn).data.traversal.first_event_id` and
  `last_event_id`
- Session to contained turns: `open(session).data.turns[].id`

Null traversal references are valid at boundaries.

## Performance SLA

These targets define what success looks like for local MCP use. They are
measured from MCP tool invocation receipt to completed tool response
serialization.

### Definitions

- `N` is the number of searchable documents visible to the request.
- One searchable document is one event eligible for `search_sessions`.
- Warm path means Moraine is already running and the relevant data has been
  ingested.
- Cold process startup is outside this SLA.
- Extremely large event payload serialization is measured separately for
  `open(event)`.

### `search_sessions` Targets

For default event types and `n_hits <= 10`:

| Searchable documents | P50 | P95 | P99 |
|---:|---:|---:|---:|
| 100k | <= 250 ms | <= 750 ms | <= 1500 ms |
| 500k | <= 500 ms | <= 1500 ms | <= 3000 ms |
| 1M | <= 800 ms | <= 2500 ms | <= 5000 ms |

For constrained search with `within_id=turn`:

| Scope | P50 | P95 | P99 |
|---|---:|---:|---:|
| Any single turn with <= 500 events | <= 50 ms | <= 300 ms | <= 750 ms |

For constrained search with `within_id=session`:

| Scope | P50 | P95 | P99 |
|---|---:|---:|---:|
| Any single session with <= 250 turns | <= 100 ms | <= 500 ms | <= 1000 ms |

Deadline:

- `search_sessions` should return a response or `deadline_exceeded` within
  5 seconds for warm-path requests up to 1M searchable documents.

### `open` Targets

| Request | P50 | P95 | P99 |
|---|---:|---:|---:|
| `open(event)` with payload <= 64 KiB | <= 25 ms | <= 200 ms | <= 500 ms |
| `open(turn)` with <= 100 events | <= 50 ms | <= 300 ms | <= 750 ms |
| `open(session)` with <= 100 turns | <= 100 ms | <= 500 ms | <= 1000 ms |
| `open(session)` with <= 1000 turns | <= 250 ms | <= 1500 ms | <= 3000 ms |

Large payload note:

- `open(event)` must return full event content.
- For event payloads over 64 KiB, latency may scale with serialized payload
  size.
- Large payload responses should still report `performance.elapsed_ms` and
  `performance.met_sla` against the applicable target.

## Success Criteria

### `search_sessions`

An implementation is successful when:

- Valid requests return the specified response envelope.
- Invalid requests return the specified error envelope.
- Default search covers `user_input`, `assistant_response`, and
  `tool_response`.
- Reasoning, tool calls, compactions, system events, and runtime events are
  excluded by default.
- Explicit event type filters are honored exactly.
- Session and turn scoped search never returns hits outside the requested
  scope.
- `n_hits` is honored exactly up to the maximum.
- Results are ranked, stable, and include normalized scores.
- Every hit includes event, turn, and session IDs that can be opened.
- Snippets are compact and never substitute for full event content.
- Empty result sets return success with `results: []`.
- Performance meets the `search_sessions` SLA for the target corpus sizes.

### `open`

An implementation is successful when:

- `open` accepts every ID returned by `search_sessions`.
- Session IDs return session metadata and all known turn summaries.
- Turn IDs return turn metadata, compact summaries, all known event summaries,
  and traversal references.
- Event IDs return full event metadata and full event content.
- Completion and terminal status are correct at session and turn levels.
- Parent references are correct for every opened object.
- Previous and next traversal references are correct or `null` at boundaries.
- Tool names are surfaced for tool call and tool response events.
- Incomplete turns are represented without inventing final responses.
- Missing or malformed IDs produce the specified errors.
- Performance meets the `open` SLA for the target object sizes.

### End-To-End Discovery

The combined interface is successful when an agent can reliably perform this
workflow:

1. Call `search_sessions` with a vague natural-language query.
2. Select a hit and call `open` on its event ID.
3. Call `open` on the parent turn ID to inspect surrounding events.
4. Call `open` on the parent session ID to inspect the broader session.
5. Traverse adjacent events or turns using IDs returned by `open`.

This workflow should not require any additional MCP tools.

## Explicit Non-Goals

This specification does not define:

- database schema
- index format
- query algorithm
- ranking algorithm
- caching strategy
- migration plan from existing MCP tools
- monitor UI behavior
- authorization or multi-user access control
