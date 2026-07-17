# MCP Search Interface Specification

This document specifies the desired behavior for Moraine's MCP retrieval tools:

- `search_sessions`
- `open`
- `list_sessions`
- `file_attention`

The scope of this document is the external interface contract: accepted inputs,
output shapes, response behavior, errors, performance reporting, and success
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
data. An ID returned by `search_sessions`, `list_sessions`, `file_attention`,
or `open` must be accepted by `open` unless the underlying session data has been
deleted or the index has been rebuilt with incompatible IDs.

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
    "elapsed_ms": 42
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
    "elapsed_ms": 3
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
- A provided `harness` or `source` containing only whitespace must return
  `invalid_request` with `error.details.field` naming that filter.
- Missing IDs must return `not_found`.
- Malformed IDs must return `invalid_id`.
- Unknown event type filters must return `unsupported_event_type`.
- Runtime failures must return `internal_error` with a concise message.
- While the search read model is publishing an active-ingest update,
  `search_sessions` returns `internal_error` with
  `error.details.reason = "read_model_refresh"`, `retryable = true`, and a
  positive `retry_after_ms`; clients should wait for that interval and retry the
  same request.

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
  harness?: string | null,
  source?: string | null,
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
["user_input", "assistant_response"]
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

`harness` and `source`:

- Optional and independently nullable.
- When present, each must contain at least one non-whitespace character after
  trimming; blank values return `invalid_request`.
- Each is an exact, case-sensitive filter. Supported `harness` values are
  `codex`, `claude-code`, `cursor`, `hermes`, `kimi-cli`, `opencode`, and
  `pi-coding-agent`.
- `source` matches a configured ingest source name. The default configured
  values are `claude`, `codex`, `cursor`, `cursor-sqlite`, `hermes`, `kimi-cli`,
  `omp`, `opencode`, and `pi`; each server's MCP tool instructions list its
  actual configured source names.
- When both are present, both predicates must match. Use `source` to distinguish
  `pi` and `omp`, which share the `pi-coding-agent` harness.

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
```

This default intentionally omits:

```text
reasoning
tool_call
tool_response
compaction
system
runtime
unknown
```

Callers may explicitly include supported omitted types when they need them.
Raw tool evidence requires `tool_call` or `tool_response` in `event_types`.
Open a returned turn or session handle to inspect the full context around a
search hit.

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
    "event_types": ["user_input", "assistant_response"],
    "harness": null,
    "source": null,
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
    "elapsed_ms": 64
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
    "harness": "codex",
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
- `session.harness` is the normalized harness; `session.source` is the
  configured ingest source.
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
    "event_types": ["user_input", "assistant_response"],
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
    "elapsed_ms": 64
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
    "elapsed_ms": 18
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
    "event_types": ["user_input", "assistant_response"],
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
    "elapsed_ms": 21
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
    "elapsed_ms": 2
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
    "elapsed_ms": 2
  }
}
```

## Tool: `list_sessions`

### Purpose

`list_sessions` lists sessions that overlap a caller-supplied datetime range.
Use it for metadata browsing by time; use `search_sessions` for content search
and `open` to inspect a selected session.

### Request Schema

```json
{
  "start_datetime": "2026-04-30T09:00:00-04:00",
  "end_datetime": "2026-04-30T13:00:00-04:00",
  "limit": 20,
  "cursor": null,
  "mode": null,
  "harness": null,
  "source": null,
  "sort": "desc"
}
```

Rules:

- `start_datetime` is inclusive and `end_datetime` is exclusive.
- Datetimes must be RFC 3339 / ISO 8601 strings with an explicit timezone
  offset or `Z`.
- Sessions match when `updated_at >= start_datetime` and
  `started_at < end_datetime`.
- `mode`, when present, is one of `web_search`, `mcp_internal`,
  `tool_calling`, or `chat`.
- `harness` and `source` are optional exact, case-sensitive filters with the
  same semantics as `search_sessions`. Blank provided values return
  `invalid_request`; when both are present, both must match.
- `sort` is `desc` or `asc`, ordered by session `updated_at` and session ID.

### Response Shape

Successful responses use `moraine.mcp.list_sessions.v1` and return compact
session metadata only:

```json
{
  "schema_version": "moraine.mcp.list_sessions.v1",
  "tool": "list_sessions",
  "request": {},
  "data": {
    "result_count": 1,
    "limit": 20,
    "truncated": false,
    "sessions": [
      {
        "rank": 1,
        "id": "session:c2Vzcy0x",
        "session": {
          "id": "session:c2Vzcy0x",
          "title": "Build failure triage",
          "source": "codex",
          "harness": "codex",
          "started_at": "2026-04-30T13:00:00.000Z",
          "updated_at": "2026-04-30T13:10:00.000Z",
          "completed": true,
          "turn_count": 3,
          "event_count": 17,
          "mode": "tool_calling",
          "session_slug": "build-failure",
          "session_summary": "Build failure triage."
        },
        "open": {
          "session_id": "session:c2Vzcy0x"
        }
      }
    ],
    "next_cursor": null
  },
  "warnings": [],
  "performance": {
    "elapsed_ms": 42
  }
}
```

`list_sessions` must not return event snippets, transcript text, or event
payloads. To inspect a listed session, pass `open.session_id` to `open`.
The returned `session.harness` and `session.source` identify the normalized
harness and configured ingest source that the corresponding filters match.

## Tool: `file_attention`

### Purpose

`file_attention` lists captured tool calls that touched a file, across the main
checkout, sibling worktrees, and agent-isolation worktrees.

It answers:

```text
Which sessions touched this file, and where do I drill in?
```

### Request Schema

```json
{
  "path": "crates/moraine-mcp-core/src/file_attention_v1.rs",
  "scope": "project",
  "granularity": "sessions",
  "start_datetime": null,
  "end_datetime": null,
  "tool": null,
  "harness": null,
  "source": null,
  "mutations_only": false,
  "limit": 25
}
```

Rules:

- `path` is required and must name a file path string. Leading/trailing
  whitespace, `file://` URIs, and directory-style trailing slashes are invalid.
- Absolute paths are reduced to a project-relative tail using the nearest Git
  boundary or exact containment beneath a non-Git launch directory. Relative
  paths are resolved from the client's MCP launch directory, including through
  a central-server route, so missing files still retain launch-project
  provenance. Git common-directory metadata unifies linked worktrees; without
  Git metadata, the canonical launch directory is the identity and different
  launch subdirectories remain separate. `.moraine.toml` independently selects
  a backend and is not required. Compound shell text and multi-path
  captures are not interpreted as one path or root; unprovable roots remain
  `unknown`.
- `scope` is `project` or `all`. `project` independently restricts normalized
  and legacy fallback lookup to the launch project's canonical Git-common-
  directory or exact working-directory identity and fails closed if neither can
  be established. `all` deliberately drops request-level project narrowing. A
  configured `--project-only` server scope remains a hard floor, so returned IDs
  remain accepted by `open`.
- Registered pre-digest roots are migrated into a durable project mapping, and
  future normalized roots populate it automatically. Retained older rows with
  blank identity may be attributed only when exactly one top-level scalar
  structured path agrees with the recorded cwd and that cwd is itself a current
  or durable project root. A
  root pruned before that
  mapping was installed lacks stored Git identity and cannot be attributed
  safely; `project` excludes it rather than widening and emits an upgrade
  limitation warning.
- `granularity` is `sessions` or `events`.
- Datetime bounds are optional, inclusive at `start_datetime` and exclusive at
  `end_datetime`. Bounds must be RFC 3339 strings with explicit timezone and at
  most millisecond precision.
- `tool` filters by tool name case-insensitively. `mutations_only` excludes
  common pure-read tools.
- `harness` and `source` are optional exact, case-sensitive filters with the
  same semantics as `search_sessions`. Blank provided values return
  `invalid_request`; when both are present, both must match.
- The default limit is `min(50, mcp.max_results)` and the maximum is
  server-configured.

### Response Shape

Successful responses use `moraine.mcp.file_attention.v1` and return a summary,
root buckets, and either session rollups or a flat event timeline:

```json
{
  "schema_version": "moraine.mcp.file_attention.v1",
  "tool": "file_attention",
  "request": {},
  "data": {
    "path": "crates/moraine-mcp-core/src/file_attention_v1.rs",
    "tail": "crates/moraine-mcp-core/src/file_attention_v1.rs",
    "tail_is_absolute": false,
    "stripped_root": "/Users/me/src/moraine",
    "scope": "project",
    "granularity": "events",
    "summary": {
      "total_touches": 6,
      "distinct_sessions": 3,
      "distinct_roots": 2,
      "distinct_known_roots": 2,
      "unknown_root_touches": 0,
      "first_touch": "2026-06-10T12:00:00.000Z",
      "last_touch": "2026-06-15T09:30:00.000Z",
      "ambiguous": true,
      "scan_truncated": false
    },
    "roots": [
      { "root": "/Users/me/src/moraine", "touch_count": 4, "session_count": 2 },
      { "root": "/Users/me/src/moraine/worktrees/feat", "touch_count": 2, "session_count": 1 }
    ],
    "result_count": 1,
    "limit": 25,
    "truncated": false,
    "events": [
      {
        "rank": 1,
        "id": "event:...",
        "event": {
          "id": "event:...",
          "session_id": "session:...",
          "harness": "codex",
          "source": "codex",
          "timestamp": "2026-06-15T09:30:00.000Z",
          "tool_name": "Edit",
          "phase": "request",
          "turn": 11,
          "match_kind": "path_suffix",
          "worktree_root": "/Users/me/src/moraine",
          "action_preview": "{\"file_path\":\"crates/...\"}"
        },
        "open": {
          "event_id": "event:...",
          "session_id": "session:...",
          "turn_id": "turn:..."
        }
      }
    ]
  },
  "warnings": [],
  "performance": {
    "elapsed_ms": 90
  }
}
```
Event rows and session rollups expose `harness` and `source` for the normalized
harness and configured ingest source that the corresponding filters match.

`event_id` and `session_id` must be present on displayed rows. `turn_id` is
present when the touch joins to the conversation trace. `truncated` is true when
either displayed rows are hidden by `limit` or the scan cap is hit. Unknown roots
are counted and warned because they can make a single known root ambiguous.

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

open({
  id: string,
  limit: number
})

open({
  cursor: string
})
```

### Accepted Inputs

`id`:

- Required unless `cursor` is provided.
- Must be a string.
- Must be a valid Moraine MCP ID returned by `search_sessions`,
  `list_sessions`, or `open`.
- May refer to a session, turn, or event.

`limit`:

- Optional and valid only with a session or turn `id`.
- Must be from 1 through the configured MCP result maximum.
- Starts forward expansion of compact child summaries. Omit it for a
  summary-only session or turn response.

`cursor`:

- Optional opaque continuation returned as `next_cursor` by an expanded open.
- Must be provided by itself. It carries the original target, page size,
  ordering anchor, and read-model snapshot.
- A stale cursor returns `invalid_request` and tells the caller to reopen the
  typed target.

Invalid inputs:

- Missing both `id` and `cursor` returns `invalid_request`.
- Empty or whitespace-only `id` returns `invalid_request`.
- Malformed IDs return `invalid_id`.
- Well-formed but unknown IDs return `not_found`.
- `id` with `cursor`, `limit` with `cursor`, `limit` without `id`, pagination
  arguments for an event ID, and unknown fields return `invalid_request`.

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
    "elapsed_ms": 32
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

Opening a session returns metadata and traversal handles. Its id-only default
is summary-only; `id + limit` or a continuation cursor returns one compact page
of turns.

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
  "next_cursor": null,
  "traversal": {
    "previous_session_id": null,
    "next_session_id": null,
    "first_turn_id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
    "last_turn_id": "turn:turn_01J9Q4P93SP6B2D5M8K7V1H4NC"
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

- Id-only session opens return `turns: []` and `next_cursor: null`.
- Expanded `turns` are ordered by `ordinal` ascending and contain at most the
  requested page size.
- Following every `next_cursor` until null returns every known turn exactly
  once from the pinned snapshot.
- `user_input` may be `null` if no user input event is known.
- `final_response` may be `null` if the turn is incomplete or ended without a
  final assistant response.
- `tools_called` must contain unique tool names in first-seen order.
- `event_types` must contain unique known event types in first-seen order;
  `unknown` is omitted from this compact list but remains valid on individual
  event summaries.
- `terminal_event_id` may be `null` when `completed` is `false`.

Example:

```json
{
  "schema_version": "moraine.mcp.open.v1",
  "tool": "open",
  "request": {
    "id": "session:ses_01J9Q3N7W6F9A8K2M4V5R6T7Y8",
    "limit": 2
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
    "next_cursor": null,
    "traversal": {
      "previous_session_id": null,
      "next_session_id": null,
      "first_turn_id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
      "last_turn_id": "turn:turn_01J9Q4P93SP6B2D5M8K7V1H4NC"
    }
  },
  "warnings": [],
  "performance": {
    "elapsed_ms": 32
  }
}
```

### Opening A Turn

Opening a turn returns a compact summary and traversal handles. Its id-only
default is summary-only; `id + limit` or a continuation cursor returns one page
of ordered event handles.

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
  "next_cursor": null,
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

- Id-only turn opens return `events: []` and `next_cursor: null`.
- Expanded `events` are ordered by absolute turn-local `ordinal` ascending and
  contain at most the requested page size.
- Following every `next_cursor` until null returns every known compact event
  summary exactly once from the pinned snapshot.
- Event summaries are compact. Full event content is available through
  `open(event_id)`.
- Encrypted reasoning payloads use the summary placeholder
  `[encrypted reasoning omitted]`; opening the event directly still returns its
  full opaque payload.
- `summary.user_input` may be `null`.
- `summary.final_response` may be `null`.
- `summary.tools_called` must contain unique tool names in first-seen order.
- `summary.event_types` must contain unique event types in first-seen order.

Expanded complete-turn example:

```json
{
  "schema_version": "moraine.mcp.open.v1",
  "tool": "open",
  "request": {
    "id": "turn:turn_01J9Q3P4V8BN7XM9G2K6Q1W3E4",
    "limit": 4
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
    "next_cursor": null,
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
    "elapsed_ms": 19
  }
}
```

Expanded incomplete-turn example:

```json
{
  "schema_version": "moraine.mcp.open.v1",
  "tool": "open",
  "request": {
    "id": "turn:turn_01J9R1A7X2C6D5E4F3G2H1J9K8",
    "limit": 2
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
    "next_cursor": null,
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
    "elapsed_ms": 16
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
    "elapsed_ms": 9
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
    "elapsed_ms": 8
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
    "elapsed_ms": 10
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
    "elapsed_ms": 9
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
    "elapsed_ms": 1
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
    "elapsed_ms": 4
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
| `query` + `harness` | Return only events from the exact normalized harness. |
| `query` + `source` | Return only events from the exact configured ingest source. |
| `query` + `harness` + `source` | AND both exact filters. |
| blank `harness` or `source` | Return `invalid_request`. |
| `query` + `within_id=event` | Return `invalid_request`. |
| blank `query` | Return `invalid_request`. |
| unknown `within_id` | Return `not_found` if ID is well-formed. |
| malformed `within_id` | Return `invalid_id`. |
| empty `event_types` | Return `invalid_request`. |
| unsupported `event_types` value | Return `unsupported_event_type`. |
| `n_hits < 1` | Return `invalid_request`. |
| `n_hits > 50` | Return `invalid_request`. |
| non-integer `n_hits` | Return `invalid_request`. |

### `list_sessions`

| Input combination | Expected behavior |
|---|---|
| `start_datetime` + `end_datetime` | List sessions overlapping the datetime range with defaults. |
| range + `limit` | Return at most `limit` sessions. |
| range + `cursor` | Return the next deterministic page for the same filter and sort. |
| range + `mode` | Return only sessions with that mode. |
| range + `sort=asc` | Return oldest matching sessions first by `updated_at`, then ID. |
| range + `harness` | Return only sessions from the exact normalized harness. |
| range + `source` | Return only sessions from the exact configured ingest source. |
| range + `harness` + `source` | AND both exact filters. |
| blank `harness` or `source` | Return `invalid_request`. |
| missing datetime | Return `invalid_request`. |
| datetime without timezone | Return `invalid_request`. |
| `end_datetime <= start_datetime` | Return `invalid_request`. |
| unknown field | Return `invalid_request`. |
| invalid `mode` or `sort` | Return `invalid_request`. |
| cursor with changed filter or sort | Return `invalid_request`. |

### `file_attention`

| Input combination | Expected behavior |
|---|---|
| `path` + `harness` | Return only touches from the exact normalized harness. |
| `path` + `source` | Return only touches from the exact configured ingest source. |
| `path` + `harness` + `source` | AND both exact filters. |
| blank `harness` or `source` | Return `invalid_request`. |

### `open`

| Input combination | Expected behavior |
|---|---|
| `id=session` | Return session metadata/traversal with an empty turn list. |
| `id=session, limit=N` | Return the first bounded compact turn page. |
| `id=turn` | Return turn summary/traversal with an empty event list. |
| `id=turn, limit=N` | Return the first bounded compact event-summary page. |
| `cursor=...` | Continue the original session/turn page with its embedded limit. |
| `id=event` | Return event metadata, full content, and traversal references. |
| missing both `id` and `cursor` | Return `invalid_request`. |
| blank `id` | Return `invalid_request`. |
| malformed `id` | Return `invalid_id`. |
| well-formed unknown `id` | Return `not_found`. |
| `id` or `limit` combined with `cursor` | Return `invalid_request`. |
| pagination argument with `id=event` | Return `invalid_request`. |
| stale or wrong-kind cursor | Return `invalid_request` with reopen guidance. |

## Traversal Contract

The interface succeeds only if callers can move through history without
inventing IDs or issuing unrelated searches.

Required traversal paths:

- Search hit to event: `search_sessions(...).data.results[].open.event_id`
- Search hit to turn: `search_sessions(...).data.results[].open.turn_id`
- Search hit to session: `search_sessions(...).data.results[].open.session_id`
- Listed session to session: `list_sessions(...).data.sessions[].open.session_id`
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
- Session to first and last turn:
  `open(session).data.traversal.first_turn_id` and `last_turn_id`
- Session/turn to every child: start with `open(id, limit)` and follow
  `data.next_cursor` with cursor-only `open` calls until null.

Null traversal references are valid at boundaries.

## Performance Reporting

Tool responses report the observed end-to-end handling time in
`performance.elapsed_ms`. Moraine does not advertise a fixed latency target or
classify individual responses as meeting an SLA: query cost varies with the
amount of stored session data, request scope, event payload size, hardware, and
concurrent work.

The `list_sessions` and `file_attention` implementations retain bounded
execution deadlines for resource safety. A deadline failure returns the
`deadline_exceeded` error code; these safety limits are not latency guarantees.
`open(event)` always returns full event content, so its elapsed time can scale
with the serialized payload size.

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
- Exact `harness` and `source` filters are honored independently and ANDed when
  combined; every hit exposes the matching normalized harness and configured
  source.
- Session and turn scoped search never returns hits outside the requested
  scope.
- `n_hits` is honored exactly up to the maximum.
- Results are ranked, stable, and include normalized scores.
- Every hit includes event, turn, and session IDs that can be opened.
- Snippets are compact and never substitute for full event content.
- Empty result sets return success with `results: []`.

### `list_sessions`

An implementation is successful when:

- Valid requests return sessions overlapping the requested datetime range.
- Boundary behavior is inclusive at `start_datetime` and exclusive at
  `end_datetime`.
- Exact `harness` and `source` filters are honored independently and ANDed when
  combined; every listed session exposes the matching normalized harness and
  configured source.
- Results are sorted deterministically and cursor-paginated.
- Each session includes a typed session ID accepted by `open`.
- The response contains compact metadata and no event snippets, event payloads,
  or transcript text.
- Invalid ranges, unknown fields, bad cursors, invalid modes, and invalid sort
  values produce the specified errors.

### `file_attention`

An implementation is successful when:

- Valid requests return the specified response envelope.
- Exact `harness` and `source` filters are honored independently and ANDed when
  combined; event rows and session rollups expose the matching values.
- Invalid paths, unknown fields, bad enum values, bad datetime precision, and
  invalid ranges produce structured errors.
- The same logical file touched in the main checkout, a sibling worktree, and
  an agent-isolation worktree is unified by repo-relative tail.
- `file_attention` calls do not report themselves as file touches.
- Returned `event_id`, `session_id`, and present `turn_id` handles are accepted
  by `open`.
- Timestamps and datetime filtering use the same timestamp source as
  `open(event)`.
- Missing trace joins produce `null` timestamps and no `turn_id`, never epoch
  sentinel timestamps.
- Known and unknown worktree roots are surfaced so ambiguous tails are visible.
- Nested/array structured path values and shell path-like operands are covered
  without treating remote URLs or prose mentions as local file touches.
- Display truncation and scan-cap truncation are visible to generic clients via
  `data.truncated`.

### `open`

An implementation is successful when:

- `open` accepts every ID returned by `search_sessions` and `list_sessions`.
- Id-only session opens return bounded metadata, counts, and first/last-turn
  traversal references without embedding turn summaries.
- Id-only turn opens return bounded metadata, compact user/final summaries,
  tool and event-type summaries, counts, and traversal references without
  embedding event summaries.
- Session and turn expansion accepts a configured bounded `limit`; following
  opaque `next_cursor` values until null returns every child exactly once in
  stable forward order, even when event-order values tie.
- Continuation cursors are bound to the target, page size, keyset anchor, and
  snapshot; malformed, mismatched, and stale cursors produce structured
  errors that tell the caller to reopen the typed ID.
- Event IDs return full event metadata and full event content.
- Completion and terminal status are correct at session and turn levels.
- Parent references are correct for every opened object.
- Previous and next traversal references are correct or `null` at boundaries.
- Tool names are surfaced for tool call and tool response events.
- Incomplete turns are represented without inventing final responses.
- Missing or malformed IDs produce the specified errors.

### End-To-End Discovery

The combined interface is successful when an agent can reliably perform this
workflow:

1. Call `search_sessions` with a vague natural-language query.
2. Select a hit and call `open` on its event ID.
3. Call id-only `open` on the parent turn ID for compact conversational
   orientation, then expand a bounded event page only if needed.
4. Call id-only `open` on the parent session ID for broader orientation, then
   expand bounded turn pages only if needed.
5. Traverse adjacent events or turns using IDs returned by `open`, or follow
   `next_cursor` values when deliberate sequential expansion is required.

For time-window discovery, the agent can call `list_sessions`, select a
returned `open.session_id`, and then call `open`.

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
