# Moraine MCP Agent Interface

## Contract

Issue #290 narrows the agent-facing MCP surface to three session-first tools:

- `search_session_data`: answer questions about prior session content.
- `open_session`: expand one known session when search snippets are not enough.
- `list_sessions`: browse session metadata without searching content.

The boundary is intentionally small. Agents should search sessions first, open only targeted sessions, and use metadata listing only for time/mode browsing. The MCP server remains a stateless stdio JSON-RPC process over ClickHouse-backed conversation tables; ingestion, schema maintenance, and index construction stay outside this service. The Rust MCP tool list publishes these three tools first, with explicit `inputSchema`, `outputSchema`, and read-only annotations, while lower-level compatibility tools remain available but de-emphasized. [src: crates/moraine-mcp-core/src/lib.rs:L827-L1180]

## Tool Boundaries

### `search_session_data`

Primary entry point for content questions. Use it for decisions, fixes, errors, logs, codenames, config values, file paths, migrations, status codes, and "what did we decide?" prompts.

Important arguments:

- `query` is natural language plus any exact tokens from the user request.
- `limit` defaults to `5`, caps at `20`, and is also bounded by the configured MCP maximum.
- `matching_events_limit` defaults to `5` and caps at `8`; raise it only for multi-evidence questions.
- `event_scope` defaults to `auto`; `auto` searches compact user-facing content but includes raw tool output for raw/error/log/stack/status/tool-output queries.
- `recency_policy` defaults to `auto`; `auto` prefers current, final, latest, corrected evidence and down-ranks stale, draft, provisional, superseded, or deprecated material unless the user asks for history.
- `from_unix_ms`, `to_unix_ms`, `mode`, and `session_id` narrow the search by time window, computed session mode, or one known session.
- `include_context` defaults to `false`; set it only when snippets need nearby turns.

Default behavior returns session-ranked evidence, not event-ranked transcript dumps. The implementation combines conversation search, session-metadata search, per-session matching event snippets, summary evidence, optional bounded context, and recency adjustments. Internal MCP traces are suppressed by default so the model sees remembered work, not retrieval plumbing. [src: crates/moraine-mcp-core/src/lib.rs:L1593-L1920]

### `open_session`

Expansion tool after `search_session_data` returns a `session_id`. Use it for transcript context, surrounding turns, or additional evidence when search snippets are insufficient. Do not call it just because search returned a hit with enough evidence to answer.

Important arguments:

- `session_id` is required.
- `query` optionally requests query-relevant windows first.
- `around_event_uid` optionally requests bounded before/after context around one evidence event.
- `event_scope` controls transcript breadth. `auto` uses query cues for tool-output windows, `messages` restricts chronological pages to messages/reasoning, and `all` broadens to normal event kinds.
- `limit` defaults to `20` and caps at `50`; `cursor` paginates chronological pages.
- `before` and `after` default to `3` and cap at `10` for event-centered windows.
- `include_payload_json` should be explicit when exact raw payload JSON is needed.
- `include_system_events` defaults to `false`.

Without `query` or `around_event_uid`, `open_session` returns a chronological page with metadata and a cursor. With `query`, it searches within the session and returns relevant windows first. With `around_event_uid`, it returns bounded before/after context and verifies the event belongs to the requested session. [src: crates/moraine-mcp-core/src/lib.rs:L1921-L2119]

### `list_sessions`

Metadata-only browsing. Use it for newest/latest sessions by time, date windows, mode filters, pagination, and session inventory. Do not use it to answer questions about what happened inside a session.

Important arguments:

- `limit` defaults to the configured MCP maximum when omitted and is validated against server bounds.
- `from_unix_ms` and `to_unix_ms` filter by session end time; `from_unix_ms` must be less than `to_unix_ms`.
- `mode` filters the computed session mode: `web_search`, `mcp_internal`, `tool_calling`, or `chat`.
- `sort` defaults to `desc`, so the newest session end time comes first.
- `cursor` continues a deterministic page for the same filter and sort.

Responses include compact rows: `session_id`, mode, start/end times, event and turn counts, `session_slug`, `session_summary`, and a next-call hint to `open_session`. [src: crates/moraine-mcp-core/src/lib.rs:L2121-L2186]

## Response Shape

Default tool results should be answer-first hybrid:

- `content[0].text` gives a short answer or salience summary the model can use immediately.
- `structuredContent` carries compact parseable evidence with stable IDs.
- `isError=false` marks successful tool results.
- `isError=true` returns a concise text error; transport and parameter errors still use JSON-RPC error envelopes where appropriate.

`search_session_data` structured results should include `query`, requested and effective `event_scope`, `recency_policy`, `recency_applied`, `hits`, and per-hit `rank`, `session_id`, `score`, time bounds, `session_summary`, compact `evidence`, optional `context`, and a next-call hint such as `open_session(session_id="...")`. Evidence entries should carry stable `event_uid`, `event_order`, kind/role, snippet text, and raw output snippets only when the effective scope includes them.

`open_session` structured results should include `found`, `session_id`, session metadata, effective scope, page/window metadata, `events`, and `next_cursor` when more transcript data exists.

`list_sessions` structured results should include the effective filters, `sort`, `sessions`, and `next_cursor`.

The published MCP metadata includes `outputSchema` for these structured payloads. For the three agent-facing tools, default `verbosity=prose` returns hybrid text plus `structuredContent`; `verbosity=full` returns pretty JSON text plus the same structured payload. Legacy tools may still use prose-only default responses. [src: crates/moraine-mcp-core/src/lib.rs:L886-L966, crates/moraine-mcp-core/src/lib.rs:L1146-L1178, crates/moraine-mcp-core/src/lib.rs:L1248-L1330]

Example:

```json
{
  "content": [
    {
      "type": "text",
      "text": "Top session s-20260424-iris-final records the final Iris batch policy: IRIS_BATCH_SIZE=96 and IRIS_FLUSH_MS=250."
    }
  ],
  "structuredContent": {
    "query": "Iris batch size current setting",
    "event_scope": "auto",
    "recency_policy": "auto",
    "hits": [
      {
        "rank": 1,
        "session_id": "s-20260424-iris-final",
        "score": 11.94,
        "last_event_time": "2026-04-24 14:34:00",
        "session_summary": "Iris final current batch policy changed to IRIS_BATCH_SIZE=96 with IRIS_FLUSH_MS=250.",
        "evidence": [
          {
            "event_uid": "evt-iris-final-03",
            "event_order": 3,
            "kind": "message/message",
            "snippet": "Final current Iris batch size policy: set IRIS_BATCH_SIZE=96 and IRIS_FLUSH_MS=250."
          }
        ],
        "next": "open_session(session_id=\"s-20260424-iris-final\")"
      }
    ]
  },
  "isError": false
}
```

## Recency And Raw Output

Recency is semantic, not just timestamp sorting. When the query asks for current, latest, final, corrected, or active state, the search contract should prefer evidence that marks a final/corrected state and demote stale drafts, provisional notes, deprecated values, and superseded answers. When the user asks for history, old or superseded evidence can rank normally.

Raw output is automatic under `event_scope=auto`. Queries containing cues such as raw, exact error, logs, stack trace, traceback, status, command output, HTTP status, panic, exception, or tool result should include the relevant tool/result payload snippets without requiring the agent to set a broader scope manually. Outside those cases, default snippets should stay compact and user-facing.

## Legacy And De-Emphasized Tools

The current server still publishes broader implementation tools: `search`, `open`, `search_conversations`, `get_session`, and `get_session_events`. For issue #290, agents should treat them as legacy, compatibility, or internal-building-block surfaces.

- `search` is event-ranked and useful for low-level debugging, but it is too narrow as the default agent retrieval tool.
- `search_conversations` is the implementation basis for `search_session_data`, but the agent-facing name and result contract should be clearer and answer-first.
- `open` is overloaded between event windows and session opening; `open_session` should make the session expansion path explicit.
- `get_session` and `get_session_events` are lower-level metadata/event APIs. Prefer `open_session` unless a caller truly needs raw paginated event rows.

Keep full transcript context opt-in. The successful default path for most content questions is one `search_session_data` call with enough evidence to answer, followed by `open_session` only when snippets are insufficient.
