# MCP Search Interface Plan

This note captures the proposed direction for Moraine's MCP search interface:
keep the tool surface small, make search return stable lookup handles, and use a
single `open` tool for progressive traversal through session data.

## Goals

- Provide most retrieval utility through two main MCP tools:
  `search_sessions` and `open`.
- Model session history as a navigable hierarchy of sessions, turns, and
  events.
- Let agents start with lightweight search hits, then expand only the context
  they need.
- Preserve access to full-fidelity data without requiring large payloads in the
  common case.
- Make every returned result useful as an entry point into adjacent context.

## Data Model

Moraine should expose indexed history as a three-level hierarchy:

```text
Session
  Turn
    Event
```

A session contains `K` turns. A turn begins with a user input and proceeds
through zero or more internal events until a terminal event is reached.

A typical turn looks like:

```text
user_input
reasoning, optional
tool_call, optional
tool_response, optional
...
assistant_response
```

Reasoning, tool calls, and tool responses can repeat many times within a turn.
Some turns can therefore be very long, so the interface should avoid returning a
full turn unless explicitly requested.

## Completion And Terminal Events

A terminal event is any event that requires new external input before the
session can continue. Examples include:

- final assistant response
- cancellation
- user interrupt
- crash or failed runtime exit
- other stop conditions

Completion should be represented at multiple levels:

- A session is complete when its latest turn is complete.
- A turn is complete when it has a terminal event.
- An event can be terminal when it ends the current turn.

This gives callers enough structure to distinguish ongoing work, interrupted
work, and completed turns.

## Entity Metadata

Each entity should have a stable ID and enough metadata for traversal.

Session metadata:

```text
session_id
title or inferred name
turn_count
completed
started_at
updated_at
source or harness
```

Turn metadata:

```text
turn_id
session_id
event_count
completed
terminal_event_id
started_at
updated_at
previous_turn_id
next_turn_id
```

Event metadata:

```text
event_id
turn_id
session_id
event_type
timestamp
model, if applicable
tool_name, if applicable
originating_model, if applicable
previous_event_id
next_event_id
```

Core event types should include:

- `user_input`
- `reasoning`
- `tool_call`
- `tool_response`
- `assistant_response`
- `compaction`
- `system` or runtime events

The exact set can grow, but the interface should keep event type names stable
once exposed.

## Tool Surface

The interface should center on two tools:

```text
search_sessions
open
```

`search_sessions` answers: "Where should I look?"

`open` answers: "Show me more about this thing."

Keeping these operations separate makes the search path predictable. Search
returns handles and compact snippets. Open expands those handles into structured
context.

## `search_sessions`

Purpose: find relevant entry points into historical session data.

Proposed request shape:

```ts
search_sessions({
  query: string,
  within_id?: string,
  event_types?: string[],
  n_hits?: number
})
```

Parameters:

- `query`: semantic or text query.
- `within_id`: optional session ID or turn ID used to constrain the search.
- `event_types`: optional event type filter.
- `n_hits`: optional result count.

Default event types:

```text
user_input
assistant_response
tool_response
```

The default should skip reasoning and tool calls because they are often noisy.
Those event classes should remain searchable when explicitly requested.

Search results should optimize for lookup information, not full context. A hit
should include:

```text
rank
score
session_id
turn_id
event_id
event_type
timestamp
snippet
session_title or inferred name
turn position
event position
parent and neighbor references when cheap
```

Example result:

```text
[1] score: 0.82
session_id: ses_...
turn_id: turn_...
event_id: evt_...
event_type: user_input
snippet: "...clickhouse schema migration failure..."
```

The key contract is that every hit gives the caller enough information to call
`open` on the event, turn, or session.

## `open`

Purpose: expand a stable ID into structured context.

Proposed request shape:

```ts
open({
  id: string
})
```

The ID may refer to a session, turn, or event. The response shape should depend
on the entity type.

### Opening A Session

Opening a session should return a compact table of turns, not every event.

Include:

- session metadata
- list of turn IDs
- inferred session title, if available
- turn completion status
- turn event counts
- truncated user prompt for each turn
- truncated final response for each turn
- previous and next session references if available

This gives the caller a map of the session without flooding the context window.

### Opening A Turn

Opening a turn should return a compressed view of the turn.

Include:

- turn metadata
- parent session reference
- previous and next turn references
- user message, truncated if long
- final response, truncated if long
- list of tools called
- list of event IDs in order
- present event types
- terminal event reference, if complete

This is the main local-context view. It should be enough to understand what the
turn attempted, what tools were involved, and which exact event to open next.

### Opening An Event

Opening an event should return full event content.

Include:

- full event metadata
- full event payload or content
- parent turn reference
- parent session reference
- previous and next event references
- previous and next turn references when useful

For example, opening an event should show the actual user message, assistant
response, tool call, tool response, compaction, or runtime event represented by
that ID.

## Progressive Discovery Workflow

The intended workflow is broad-to-specific:

```text
search_sessions(query="clickhouse schema migration failure")
open(event_id)
open(turn_id)
open(session_id)
```

A caller can then traverse nearby context:

```text
open(previous_event_id)
open(next_event_id)
open(previous_turn_id)
open(next_turn_id)
```

This lets an agent move from a vague memory to the exact historical context:

1. Search for relevant content.
2. Inspect a promising event.
3. Expand to the containing turn.
4. Expand to the containing session.
5. Walk neighboring events or turns as needed.

## Traversal Properties

Every response should expose enough IDs to keep moving without another global
search:

- event to parent turn
- event to parent session
- event to previous or next event
- turn to parent session
- turn to previous or next turn
- session to contained turns

This makes session history feel like a graph that can be explored from any
entry point.

## Why This Is A Strong Interface

This design separates retrieval from exploration.

Search remains small, relevance-oriented, and cheap to inspect. It returns the
places worth looking, not entire conversations.

Open becomes the single expansion primitive. It works uniformly across sessions,
turns, and events, so callers do not need separate tools for `get_session`,
`get_turn`, `get_event`, `list_events`, or `list_turns`.

The result is strong progressive disclosure:

- Start with compact ranked hits.
- Open exactly one object.
- Climb to broader context only when needed.
- Traverse laterally through adjacent events or turns.
- Pull full content only at the event level.

This is especially important for agent use, where the caller often needs to
recover the reasoning behind an old decision without loading an entire session
into context.

## Implementation Implications

- IDs need to be stable and typed enough for `open` to route correctly.
- Search indexing should prioritize content-bearing events by default.
- Reasoning and tool calls should be indexed but opt-in for normal search.
- Snippet generation should preserve enough surrounding text to identify why a
  hit matched.
- Turn summaries should be derived from user input, final response, tool list,
  terminal status, and event counts.
- Responses should include traversal IDs consistently, even when content is
  truncated.
- The event-level open path should preserve full-fidelity payloads for exact
  audit and reconstruction.

## Open Questions

- Should `within_id` accept only sessions and turns, or should it also accept an
  event ID and search around that event's local neighborhood?
- Should `open` support optional expansion controls later, such as
  `max_chars`, `include_payloads`, or `event_window`?
- Should event type names align directly with database values, or should MCP
  expose a stable normalized vocabulary over provider-specific event classes?
- Should session titles be persisted, inferred at query time, or both?
