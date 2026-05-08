# MCP Interface

This page explains Moraine's MCP interface as an agent user should understand
it. For the exhaustive contract, examples, and SLA targets, see the
[MCP Search Interface Specification](../mcp-search-interface-spec.md).

## How MCP Tools Appear

MCP clients discover tools by calling `tools/list` and invoke them through
`tools/call`. The MCP tool definition includes a name, description,
`inputSchema`, optional `outputSchema`, and annotations; tool results may include
plain text `content` and machine-readable `structuredContent`. See the
[MCP tools specification](https://modelcontextprotocol.io/specification/2025-06-18/server/tools)
for the protocol-level model.

Moraine advertises one read-only server, `moraine-mcp`, with three tools:

| Tool | Use it for |
| --- | --- |
| `search_sessions` | Content search over indexed agent events. |
| `open` | Expanding an event, turn, or session ID into structured context. |
| `list_sessions` | Time-window browsing over sessions and active work. |

All three tools return a short text summary for clients that display text, and
the same result as JSON in `structuredContent` for clients that can inspect
structured tool output.

## Record Model

Moraine normalizes every harness into three levels:

| Level | Meaning |
| --- | --- |
| Session | One agent conversation or trace file. |
| Turn | One user-to-agent cycle within a session. |
| Event | One normalized record, such as user input, assistant response, tool call, tool response, reasoning, compaction, system, or runtime event. |

Search returns events because events are the smallest useful evidence unit. An
event points upward to its turn and session, so agents can start with a precise
hit and expand only as needed.

IDs are opaque typed strings:

```text
session:...
turn:...
event:...
```

Do not parse these IDs. Pass them back to `open` exactly as returned.

## `search_sessions`

`search_sessions` finds relevant events across Moraine history.

Input:

```json
{
  "query": "mcp open tool oneof top-level schema",
  "within_id": null,
  "event_types": ["user_input", "assistant_response", "tool_response"],
  "n_hits": 10
}
```

Fields:

| Field | Meaning |
| --- | --- |
| `query` | Required keyword query. Empty strings are rejected. |
| `within_id` | Optional `session:...` or `turn:...` ID to scope the search. Event IDs are not valid scopes. |
| `event_types` | Optional filter. Searchable event types are `user_input`, `assistant_response`, `reasoning`, `tool_call`, `tool_response`, `compaction`, `system`, and `runtime`. |
| `n_hits` | Optional result limit from 1 to 50. Default is 10. |

The default event type filter is `user_input`, `assistant_response`, and
`tool_response`. That default is intentionally practical: it searches what the
user asked, what the assistant concluded, and what tools returned, while leaving
lower-signal operational records out until you request them.

Output data:

| Field | Meaning |
| --- | --- |
| `result_count` | Number of hits returned. |
| `limit` | Applied limit. |
| `truncated` | Whether more matching records may exist. |
| `results[]` | Event-ranked hits with rank, score, event metadata, turn metadata, session metadata, snippet, and `open` handles. |

Each result includes:

```json
{
  "rank": 1,
  "score": 12.34,
  "event": { "id": "event:...", "type": "tool_response" },
  "turn": { "id": "turn:...", "ordinal": 7 },
  "session": { "id": "session:...", "title": "..." },
  "snippet": { "text": "...", "truncated": false },
  "open": {
    "event_id": "event:...",
    "turn_id": "turn:...",
    "session_id": "session:..."
  }
}
```

The snippet is a pointer, not the full record. Open the event when the answer
depends on exact wording, command output, payload JSON, or tool arguments.

## `open`

`open` expands an ID returned by `search_sessions`, `list_sessions`, or another
`open` response.

Input:

```json
{ "id": "event:..." }
```

What comes back depends on the ID kind:

| ID kind | Returned context |
| --- | --- |
| `event` | Full event content, payload details when available, parent turn/session summary, and traversal IDs. |
| `turn` | Turn metadata, compact user/final-response summaries, tool names, event summaries, and previous/next turn IDs. |
| `session` | Session metadata and compact summaries for each turn. |

Use event open for evidence, turn open for local context, and session open for
orientation across the whole conversation.

## `list_sessions`

`list_sessions` browses sessions by time window. Use it when the clue is
temporal, such as "today", "last night", or "active sessions".

Input:

```json
{
  "start_datetime": "2026-05-08T09:00:00-04:00",
  "end_datetime": "2026-05-08T12:00:00-04:00",
  "limit": 20,
  "cursor": null,
  "mode": null,
  "sort": "desc"
}
```

`start_datetime` and `end_datetime` are required and must include an explicit
timezone. `mode` can filter session mode: `web_search`, `mcp_internal`,
`tool_calling`, or `chat`. `next_cursor` lets clients continue the same listing.

Output data includes compact session records:

```json
{
  "rank": 1,
  "id": "session:...",
  "session": {
    "title": "...",
    "source": "codex",
    "started_at": "2026-05-08T13:00:00.000Z",
    "updated_at": "2026-05-08T13:45:00.000Z",
    "turn_count": 12,
    "event_count": 87,
    "mode": "tool_calling"
  },
  "open": { "session_id": "session:..." }
}
```

`list_sessions` intentionally does not return transcript text. Open a listed
session if you need to inspect its turns.

## Response Envelope

Successful structured responses share this shape:

```json
{
  "schema_version": "moraine.mcp.search_sessions.v1",
  "tool": "search_sessions",
  "request": {},
  "data": {},
  "warnings": [],
  "performance": {
    "elapsed_ms": 12,
    "sla_target_ms": 750,
    "met_sla": true
  }
}
```

Tool-level errors return `schema_version: "moraine.mcp.error.v1"` with an
`error` object. Common error codes are `invalid_request`, `invalid_id`,
`not_found`, `unsupported_event_type`, `deadline_exceeded`, and
`internal_error`.

## How Agents Should Read Results

The agent should treat Moraine records as a navigable evidence graph:

- A search hit says "this event probably matters."
- `open(event)` says "this is exactly what happened at that point."
- `open(turn)` says "this is the immediate conversational context."
- `open(session)` says "this is the full session map."
- Traversal IDs let the agent move to neighboring events or turns without
  re-running a broad search.

This is why the recommended flow is narrow-to-wide. It gives the model enough
context to answer accurately without flooding its current context window with
whole transcripts.
