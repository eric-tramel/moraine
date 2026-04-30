# MCP Search Interface Implementation Plan

This document turns the `search_sessions` / `open` specification into an
implementation sequence for the current Moraine code base. It is based on a
parallel review of the MCP layer, conversation/search repository, schema/data
model, and existing test coverage.

The goal is targeted harmonization around the new interface, not a rewrite of
Moraine.

## Current State

The MCP binary is thin. `apps/moraine-mcp` loads configuration and delegates to
`moraine-mcp-core`.

The real MCP implementation currently lives in one large file:

```text
crates/moraine-mcp-core/src/lib.rs
```

It owns:

- JSON-RPC stdio handling
- `initialize`
- `tools/list`
- `tools/call`
- tool request structs
- response wrappers
- prose formatting
- MCP unit tests

The current listed MCP retrieval surface is larger than the desired spec:

```text
search_session_data
open_session
search
open
search_conversations
list_sessions
get_session
get_session_events
```

The current `open` name conflicts with the new spec. Today it accepts
`event_uid` or `session_id`; the spec requires `open({ id })` where the ID can
refer to a session, turn, or event.

The conversation/search layer already has useful primitives:

- event search: `search_events`
- session metadata: `get_session_metadata`
- session listing: `list_conversations`
- turn listing: `list_turns`
- turn loading: `get_turn(session_id, turn_seq)`
- event opening: `open_event(event_uid)`
- session event paging: `list_session_events`

The schema is event-centric. `moraine.events` stores normalized events,
`v_conversation_trace` derives `event_order` and `turn_seq`, and search tables
index event text.

## Implementation Strategy

Implement the new MCP contract first as a thin v1 adapter over existing
repository capabilities. Add only the repository support needed to make the
contract correct and efficient. Defer deeper schema work unless it is needed for
correctness, performance, or clean removal of legacy behavior.

Use this order:

1. Add MCP contract primitives.
2. Add repository support for the missing lookups and scoped search.
3. Implement `search_sessions`.
4. Replace `open` with the spec `open({ id })`.
5. Update tests and smoke checks.
6. Remove or hide stale legacy tools.
7. Add schema/search cleanup only where the first implementation proves it is
   needed.

## Phase 1: MCP Contract Primitives

Add the shared MCP v1 contract layer in `moraine-mcp-core`.

Recommended code shape:

```text
crates/moraine-mcp-core/src/lib.rs
```

or, preferably, split the monolith into focused modules:

```text
crates/moraine-mcp-core/src/contract.rs
crates/moraine-mcp-core/src/search_sessions.rs
crates/moraine-mcp-core/src/open.rs
crates/moraine-mcp-core/src/format.rs
```

Add:

- `SearchSessionsArgs`
- `OpenV1Args`
- `McpId`
- `McpEntityKind`
- `McpEventType`
- `ToolEnvelope`
- `ToolErrorEnvelope`
- `Performance`
- validation helpers
- timestamp normalization helpers

### ID Codec

Define typed opaque IDs at the MCP boundary:

```text
session:<encoded session id>
turn:<encoded session id>:<turn seq>
event:<encoded event uid>
```

The concrete encoding should be URL-safe and reversible by Moraine. Callers
must not parse these IDs, but this lets the first implementation avoid a new
turn table while still producing stable turn handles for indexed data.

Rules:

- session IDs decode to existing `session_id`
- event IDs decode to existing `event_uid`
- turn IDs decode to `(session_id, turn_seq)`
- malformed IDs produce `invalid_id`
- well-formed IDs that do not resolve produce `not_found`

This avoids a schema migration for initial typed IDs. A persisted `turn_uid`
can be added later if turn identity needs to survive changes to turn-boundary
logic.

### Event Type Mapping

Add an MCP event type mapper over existing event fields:

```text
(event_class, payload_type, actor_role) -> McpEventType
```

Initial mapping:

| MCP type | Current event shape |
|---|---|
| `user_input` | message-like event with `actor_role = user` |
| `assistant_response` | message-like event with `actor_role = assistant` |
| `reasoning` | `event_class = reasoning` or reasoning payload types |
| `tool_call` | `event_class = tool_call` or tool-use payload types |
| `tool_response` | `event_class = tool_result` or tool-result payload types |
| `compaction` | compacted / summary / compaction payload types |
| `system` | `actor_role = system` or system/progress/file-history events |
| `runtime` | task lifecycle, cancellation, crash, interrupt, queue/runtime events |
| `unknown` | fallback |

Do not rename database event kinds in this phase. Normalize only at the MCP
boundary and in repository result models meant for MCP.

### Envelopes

Every new tool response should return structured content with the spec
envelope:

```text
schema_version
tool
request
data
warnings
performance
```

Errors should also be structured:

```text
schema_version = moraine.mcp.error.v1
tool
request
error.code
error.message
error.details
warnings
performance
```

The MCP `content` text can be a concise rendering of the same payload, but
`structuredContent` should always be present for `search_sessions` and the new
`open`.

## Phase 2: Repository Support

The current repository is close, but the spec needs a few missing capabilities.

Add targeted domain/query types in:

```text
crates/moraine-conversations/src/domain.rs
crates/moraine-conversations/src/repo.rs
crates/moraine-conversations/src/clickhouse_repo.rs
```

Recommended new repository operations:

```text
search_mcp_events(query)
get_mcp_session(session_id)
get_mcp_turn(session_id, turn_seq)
get_mcp_event(event_uid)
```

These can internally reuse existing SQL and helper methods, but they should
return data shaped for the spec instead of forcing `moraine-mcp-core` to make
many small queries and infer too much.

### `search_mcp_events`

Needed behavior:

- global search
- session-scoped search
- turn-scoped search
- normalized MCP event type filters
- default event types:
  - `user_input`
  - `assistant_response`
  - `tool_response`
- `n_hits` with `limit + 1` fetching for `truncated`
- event timestamp
- session metadata
- turn ordinal
- event ordinal within turn
- terminal flag when derivable
- source/harness metadata
- tool/model metadata

The existing `search_events` can cover global and session-scoped search, but it
cannot express turn scope or role-specific MCP event types precisely enough.
Add a new MCP-specific query path instead of contorting the legacy API.

For turn-scoped search, start with a bounded turn event set:

1. Decode `turn:<...>` into `(session_id, turn_seq)`.
2. Resolve that turn and its event UIDs from `v_conversation_trace`.
3. Search only within those event UIDs, or add an equivalent query filter.

This avoids adding turn sequence columns to the search index before we know the
performance profile.

### `get_mcp_session`

Needed behavior:

- resolve session metadata
- list all known turns
- summarize each turn
- include session title/source where available
- compute session completion from latest turn completion
- avoid full event payloads

The current `list_turns` gives counts and times, but not user prompt, final
response, tools called, event type list, or terminal event. Add one repository
path that returns compact turn summaries for a session without creating an
N+1-query pattern for large sessions.

### `get_mcp_turn`

Needed behavior:

- load one turn by `(session_id, turn_seq)`
- return compact ordered event summaries
- derive user input and final response summaries
- list tools called in first-seen order
- list normalized event types in first-seen order
- compute completion and terminal event
- include previous/next turn IDs
- include first/last event IDs

The current `get_turn(session_id, turn_seq)` is a good starting point. It needs
additional derivation and traversal helpers.

### `get_mcp_event`

Needed behavior:

- open one event by `event_uid`
- return full available event text and payload
- return normalized metadata
- include parent session/turn summary
- include previous/next event IDs
- include previous/next turn IDs

The current `open_event(event_uid)` opens bounded context and currently MCP
formatting truncates event text. The spec event-open path should retrieve the
target event without truncating the available normalized content.

If full raw source fidelity is required beyond `payload_json`, add a follow-up
raw event lookup from `moraine.raw_events`.

## Phase 3: Implement `search_sessions`

Add the new MCP tool and publish it in `tools/list`.

Request:

```json
{
  "query": "string",
  "within_id": "optional session or turn id",
  "event_types": ["optional list"],
  "n_hits": 10
}
```

Implementation steps:

1. Parse arguments into `SearchSessionsArgs`.
2. Trim and validate `query`.
3. Validate `query` max length.
4. Canonicalize `within_id`.
5. Reject `within_id` when it is an event ID.
6. Canonicalize `event_types`.
7. Apply default event types when omitted.
8. Validate `n_hits`, default `10`, max `50`.
9. Call `search_mcp_events`.
10. Normalize scores into `0.0..1.0` within the response.
11. Build the spec search hit shape.
12. Include `open.event_id`, `open.turn_id`, and `open.session_id`.
13. Return the success envelope.
14. Map validation and repository failures into spec error envelopes.

The output must be event-ranked, not session-ranked. This is the main behavior
change from `search_session_data`.

## Phase 4: Replace `open` With Spec `open({ id })`

The new `open` should dispatch by typed ID:

```text
session -> session compact map
turn    -> compact turn context
event   -> full event content
```

Implementation steps:

1. Replace the public `OpenArgs` schema with `{ id: string }`.
2. Parse and validate `McpId`.
3. Route to `open_session_v1`, `open_turn_v1`, or `open_event_v1`.
4. Return the spec envelope for all successes.
5. Return spec error envelopes for bad IDs or missing objects.

### Session Open

Use `get_mcp_session`.

Return:

- `kind: "session"`
- session metadata
- all known turn summaries
- completion status
- source/title
- traversal references where available

Do not return full event payloads.

### Turn Open

Use `get_mcp_turn`.

Return:

- `kind: "turn"`
- turn metadata
- parent session reference
- compact ordered event summaries
- tools called
- event types
- user input summary
- final response summary
- previous/next turn IDs
- first/last event IDs

Do not return full payloads for every event.

### Event Open

Use `get_mcp_event`.

Return:

- `kind: "event"`
- full event metadata
- full available event content
- parent session reference
- parent turn reference
- previous/next event IDs
- previous/next turn IDs

This is the only open path that should return full event content.

## Phase 5: Completion And Traversal Semantics

Completion is not currently modeled directly. Add a deterministic derivation
layer before adding persisted schema.

Recommended first-pass terminal classification:

- terminal assistant response:
  - assistant message that is the last substantive event in a turn
- cancellation / interrupt / aborted turn:
  - payload types or statuses such as `turn_aborted`
- task complete:
  - `task_complete` / runtime completion events
- crash / error:
  - runtime or operation failure status when present

Rules:

- A turn is complete when it has a terminal event.
- A session is complete when its latest turn is complete.
- Incomplete turns must keep `final_response: null`.
- Terminal event IDs must be typed event IDs.

Traversal can be derived from:

- `session_id`
- `turn_seq`
- `event_order`
- `event_uid`

Add helpers to resolve:

- previous/next event within a session
- previous/next turn within a session
- first/last event in a turn
- parent session/turn references

## Phase 6: Tests And Validation

Update tests before removing legacy tools.

### MCP Unit Tests

Add or update tests in:

```text
crates/moraine-mcp-core/src/lib.rs
```

Cover:

- `tools/list` exposes `search_sessions` and spec `open`
- request defaults and canonicalization
- `n_hits` bounds `1..50`
- blank query
- overlong query
- unsupported event types
- duplicate event type normalization
- malformed IDs
- unknown IDs
- `within_id=event` rejection
- success envelope shape
- error envelope shape
- structured content is always present
- score normalization
- typed ID round trips

### Repository Tests

Add tests in:

```text
crates/moraine-conversations/tests/repository_integration.rs
```

Cover:

- global `search_mcp_events`
- session-scoped `search_mcp_events`
- turn-scoped `search_mcp_events`
- event type filters including user vs assistant messages
- `limit + 1` truncation
- event ordinal within turn
- turn summary derivation
- session summary derivation
- event open with full available content
- previous/next traversal IDs
- RFC 3339 UTC timestamps

### Smoke Tests

Update:

```text
scripts/ci/mcp_smoke.py
.claude/skills/agent-smoke-e2e/SKILL.md
```

The smoke path should be:

```text
search_sessions(query)
open(event_id from first hit)
open(turn_id from first hit)
open(session_id from first hit)
```

### Validation Commands

Fast local checks:

```bash
cargo fmt --all -- --check
cargo test -p moraine-mcp-core --locked
cargo test -p moraine-conversations --locked
cargo test --workspace --locked
```

For changes touching ingest, MCP, monitor, or schema, run the dev sandbox and
tear it down afterward:

```bash
id=$(scripts/dev/sandbox/moraine-sandbox up --quiet)
scripts/dev/sandbox/moraine-sandbox shell "$id"
scripts/dev/sandbox/moraine-sandbox down "$id"
```

Functional gate:

```bash
bash scripts/ci/e2e-stack.sh
```

## Phase 7: Cleanup And Harmonization

Once `search_sessions` and spec `open` pass contract and smoke tests, remove or
hide stale surface area.

Recommended cleanup sequence:

1. Make `search_sessions` and spec `open` the only listed retrieval tools.
2. Update docs and CI to stop referencing legacy retrieval tools.
3. Remove or hide:
   - `search_session_data`
   - `open_session`
   - legacy `search`
   - legacy `search_conversations`
   - `list_sessions`
   - `get_session`
   - `get_session_events`
4. Remove dead app scaffolding modules if still unused:
   - `apps/moraine-mcp/src/tools/open.rs`
   - `apps/moraine-mcp/src/tools/search.rs`
   - `apps/moraine-mcp/src/format/prose.rs`
   - `apps/moraine-mcp/src/sql/builders.rs`
   - `apps/moraine-mcp/src/sql/logging.rs`
   - `apps/moraine-mcp/src/tokenize.rs`
5. Remove the corresponding `mod` declarations from
   `apps/moraine-mcp/src/main.rs`.

Do not implement new work under legacy `rust/` or `moraine-monitor/` paths.

## Schema And Search Cleanup Decision Points

The first implementation should not require a large schema rewrite. However,
there are targeted schema/search changes that may be justified.

### Likely Not Required For First Pass

- new session table
- new turn table
- replacing BM25
- rebuilding ingest around the MCP spec
- moving MCP logic out of `moraine-mcp-core` entirely

### Likely Useful If Performance Or Clarity Requires It

Add a new view or projection for MCP traversal:

```text
v_mcp_trace
```

Possible fields:

- `session_id`
- `event_uid`
- `event_order`
- `turn_seq`
- `event_ordinal_in_turn`
- normalized MCP event type
- terminal flag
- model
- tool name
- event timestamp formatted consistently

This can keep query code cleaner without changing the base `events` table.

### Consider Persisted Schema Only If Needed

Persisted columns or tables should be considered if the derived approach cannot
meet the SLA:

- persisted `turn_uid`
- persisted `event_ordinal_in_turn`
- persisted normalized MCP event type
- persisted terminal status
- search index fields for turn scope

Adding these would require an ordered migration and backfill/rebuild of search
tables and materialized views.

## Risks

Typed IDs are the first blocker. Event and session IDs exist, but turn identity
is currently derived from `turn_seq`. Use an opaque reversible turn ID first,
then persist turn IDs later only if needed.

Turn-scoped search is the main search-layer gap. Avoid rewriting the index for
the first pass; search within a bounded turn event set and measure.

Completion semantics are derived. They need tests across Codex, Claude, Kimi,
and Hermes fixtures before they should be treated as authoritative.

Legacy tool removal can break smoke tests and existing users. Update tests and
docs first, then remove the stale surface deliberately.

Full event content depends on what Moraine has stored. The first implementation
should return full available normalized content. If raw-source fidelity is
required, add an explicit raw event lookup as a follow-up.

## Definition Of Done

The implementation is complete when:

- `tools/list` exposes `search_sessions` and spec `open`.
- `search_sessions` returns spec envelopes and event-ranked hits.
- `open` accepts one typed `id` and opens sessions, turns, and events.
- Every search hit ID round-trips through `open`.
- Session, turn, and event traversal IDs are correct.
- Default event type behavior matches the spec.
- Validation and errors match the spec.
- Response performance metadata is present.
- MCP and repository tests cover the input permutation matrix.
- `scripts/ci/mcp_smoke.py` validates the new discovery workflow.
- Legacy retrieval tools are removed from the listed MCP surface or explicitly
  marked as temporary compatibility paths.
