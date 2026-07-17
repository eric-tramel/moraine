# MCP Interface

This page explains Moraine's MCP interface as an agent user should understand
it. For the exhaustive contract and examples, see the
[MCP Search Interface Specification](../mcp-search-interface-spec.md).

## How MCP Tools Appear

MCP clients discover tools by calling `tools/list` and invoke them through
`tools/call`. The MCP tool definition includes a name, description,
`inputSchema`, optional `outputSchema`, and annotations; tool results may include
plain text `content` and machine-readable `structuredContent`. See the
[MCP tools specification](https://modelcontextprotocol.io/specification/2025-06-18/server/tools)
for the protocol-level model.

Moraine advertises one read-only server, `moraine-mcp`, with four tools:

| Tool | Use it for |
| --- | --- |
| `search_sessions` | Content search over indexed agent events. |
| `open` | Expanding an event, turn, or session ID into structured context. |
| `list_sessions` | Time-window browsing over sessions and active work. |
| `file_attention` | Every session that touched a file, across every worktree. |

All four tools return a short text summary for clients that display text, and
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
  "event_types": ["user_input", "assistant_response"],
  "harness": null,
  "source": null,
  "n_hits": 10
}
```

Fields:

| Field | Meaning |
| --- | --- |
| `query` | Required keyword query. Empty strings are rejected. |
| `within_id` | Optional `session:...` or `turn:...` ID to scope the search. Event IDs are not valid scopes. |
| `event_types` | Optional filter. Searchable event types are `user_input`, `assistant_response`, `reasoning`, `tool_call`, `tool_response`, `compaction`, `system`, and `runtime`. |
| `harness` | Optional exact, case-sensitive normalized harness filter. Supported values are `codex`, `claude-code`, `cursor`, `hermes`, `kimi-cli`, `opencode`, and `pi-coding-agent`. |
| `source` | Optional exact, case-sensitive ingest source filter. The default configured values are `claude`, macOS-only `claude-cowork`, `codex`, `cursor`, `cursor-sqlite`, `hermes`, `kimi-cli`, `omp`, `opencode`, and `pi`; each server's MCP tool instructions list its actual configured source names. |
| `n_hits` | Optional result limit from 1 to 50. Default is 10. |

The default event type filter is `user_input` and `assistant_response`. This
deterministic, message-first default searches what the user asked and what the
assistant concluded without returning raw tool evidence. Request `tool_call`
or `tool_response` explicitly through `event_types` when that evidence is
needed. To inspect the full context around a hit, pass its returned turn or
session handle to `open`.

Use `source` when the configured source is the distinction that matters. For
example, the `pi` and `omp` sources both use the `pi-coding-agent` harness, so
`source: "omp"` selects only OMP sessions while `harness: "pi-coding-agent"`
selects both.

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
  "event": { "id": "event:...", "type": "assistant_response" },
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

`open` reads an ID returned by `search_sessions`, `list_sessions`, or another
`open` response. Session and turn reads are summary-first so a large history
does not enter the model context unless the agent asks for it.

There are three call shapes:

```json
{ "id": "turn:..." }
{ "id": "turn:...", "limit": 20 }
{ "cursor": "opaque-next-cursor" }
```

- `id` alone returns session/turn metadata, compact user input and final
  response, tools and event types, counts, and traversal handles. Its `turns`
  or `events` array is empty and `next_cursor` is null.
- `id` plus `limit` starts bounded forward expansion. `limit` is from 1 to the
  server's configured maximum.
- `{ "cursor": next_cursor }` continues the same target with the original page
  size. Treat the cursor as opaque. If an active session changes between pages,
  reopen the typed ID and start again.

Follow `next_cursor` until it is null to recover every compact turn or event
summary. Then open an individual `event:` ID when exact wording, full tool
arguments/output, or payload JSON is needed. There is intentionally no
unbounded one-call transcript mode.
Encrypted reasoning payloads appear as `[encrypted reasoning omitted]` in
compact event summaries; open the event directly only when its opaque payload
is needed.

What comes back depends on the ID kind:

| ID kind | Returned context |
| --- | --- |
| `event` | Full event content, payload details when available, parent turn/session summary, and traversal IDs. |
| `turn` | Summary and traversal by default; a bounded page of compact event handles when `limit` or `cursor` is used. |
| `session` | Metadata and first/last-turn traversal by default; a bounded page of compact turn summaries when expanded. |

Use event open for exact evidence, id-only turn/session open for orientation,
and expand only when the summary is insufficient.

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
  "harness": null,
  "source": null,
  "sort": "desc"
}
```

`start_datetime` and `end_datetime` are required and must include an explicit
timezone. `mode` can filter session mode: `web_search`, `mcp_internal`,
`tool_calling`, or `chat`. `harness` and `source` use the same exact,
case-sensitive semantics as `search_sessions`. `next_cursor` lets clients
continue the same listing; changing any filter invalidates that cursor.

Output data includes compact session records:

```json
{
  "rank": 1,
  "id": "session:...",
  "session": {
    "title": "...",
    "harness": "codex",
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

## `file_attention`

`file_attention` answers "show me every session that touched this file, and let
me drill into what was done, when." Given a path, it returns the full
agent-attention history of that file — edits, reads, and aborted attempts —
across *every* worktree of the project: the main checkout, sibling worktrees,
and agent-isolation worktrees, including work that never landed in git. Unlike
`git blame`, it shows the debugging session that only read the file and the edit
that was tried and reverted. Matching is by the project-relative path *tail*, which
is byte-identical across worktree roots, so the roots unify by construction.

Input:

```json
{
  "path": "crates/moraine-conversations/src/clickhouse_repo.rs",
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

`path` is required. Absolute paths are reduced to a project-relative tail using
the nearest Git boundary or, for a non-Git project, exact containment beneath
the client's launch directory. Relative paths are resolved from that launch
directory, including when the client is routed through the central MCP server,
so deleted or not-yet-created files retain launch-project provenance. Git
checkouts share one identity across linked worktrees. Without Git metadata, the
canonical launch directory is the identity and sessions launched from different
subdirectories remain separate. `.moraine.toml` selects a backend independently
and is not required for identity. Boundary whitespace, `file://` URIs, and directory-style trailing
slashes are rejected rather than silently mapped to a different file. Compound
shell text and multi-path captures are never interpreted as one path or root;
unprovable roots remain `unknown`.

`scope` is `project` (default) or `all`. `project` restricts both normalized and
legacy fallback lookup to the launch project's canonical Git-common-directory
or exact working-directory identity independently of `--project-only`, and
fails closed when neither identity can be established. `all` deliberately drops that request-level project
narrowing. A configured `--project-only` server scope remains a hard floor so
returned IDs stay openable. Registered pre-digest roots are migrated to a
durable project mapping, and future normalized roots populate that mapping
automatically. Retained older rows with blank normalized identity can be
recovered only when one top-level scalar structured path agrees exactly with
the recorded working directory and that directory is itself a current or
durable root for this project. A
root pruned before this mapping was installed has no stored Git
identity and cannot be attributed safely; project scope excludes it rather than
widening across projects, and the response warns about this one-time upgrade
limitation. `granularity` is `sessions` (default, one rollup per session) or
`events` (the flat touch-by-touch timeline). `tool` filters by tool name,
`harness` and `source` apply the same exact filters as the other retrieval
tools, and `mutations_only` excludes common pure-read tools. The default limit
is `min(50, mcp.max_results)` and the maximum is server-configured.

Output data carries a summary, the distinct worktree roots the tail matched
(so over-match is visible, never silently merged), and either per-session
rollups or an event timeline. Each item exposes typed `session:` / `event:` IDs
with `open` handles:

```json
{
  "tail": "crates/.../clickhouse_repo.rs",
  "summary": {
    "total_touches": 9,
    "distinct_sessions": 4,
    "distinct_roots": 2,
    "distinct_known_roots": 2,
    "unknown_root_touches": 0,
    "first_touch": "2026-06-10T12:00:00.000Z",
    "last_touch": "2026-06-15T09:30:00.000Z",
    "ambiguous": true,
    "scan_truncated": false
  },
  "path": "crates/moraine-conversations/src/clickhouse_repo.rs",
  "scope": "project",
  "granularity": "sessions",
  "limit": 25,
  "truncated": false,
  "roots": [
    { "root": "/Users/me/src/moraine", "touch_count": 7, "session_count": 3 },
    { "root": "/Users/me/src/moraine/worktrees/feat", "touch_count": 2, "session_count": 1 }
  ],
  "sessions": [
    {
      "rank": 1,
      "id": "session:...",
      "session": { "harness": "claude-code", "touch_count": 5, "tools": ["Edit", "Read"] },
      "open": { "session_id": "session:...", "event_id": "event:...", "turn_id": "turn:..." }
    }
  ]
}
```

`file_attention` only *locates* touches; "what was done, when" is `open` on a
returned `event:` / `turn:` / `session:` ID, which already returns the full edit,
diff, and surrounding reasoning. `turn_id` is present when the touch joins to
the conversation trace; `event_id` and `session_id` are always present on
displayed rows. A tail with too few path segments (a bare basename) is
inherently ambiguous and returns a warning alongside the surfaced roots. Unknown
roots are counted and warned because they can make an otherwise single known
root ambiguous.

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
    "elapsed_ms": 12
  }
}
```

Tool-level errors return `schema_version: "moraine.mcp.error.v1"` with an
`error` object. Common error codes are `invalid_request`, `invalid_id`,
`not_found`, `unsupported_event_type`, `deadline_exceeded`, and
`internal_error`.

While the search read model is publishing an active-ingest update,
`search_sessions` returns `internal_error` with
`error.details.reason = "read_model_refresh"`, `retryable = true`, and a
positive `retry_after_ms`. Wait for that interval, then retry the same request.

## How Agents Should Read Results

The agent should treat Moraine records as a navigable evidence graph:

- A search hit says "this event probably matters."
- `open(event)` says "this is exactly what happened at that point."
- Id-only `open(turn)` says "this is a bounded map of the immediate
  conversational context."
- Id-only `open(session)` says "this is a bounded map of the session."
- Bounded expansion pages say "these are the next compact child handles";
  follow their opaque cursor only when more context is needed.
- Traversal IDs let the agent move to neighboring events or turns without
  re-running a broad search.

This is why the recommended flow is narrow-to-wide. It gives the model enough
context to answer accurately without flooding its current context window with
whole transcripts.
