-- 020_purge_empty_session_claude_code.sql
--
-- One-time cleanup for issue #386. Before the ingest exclusion landed, the
-- recursive `~/.claude/projects/**/*.jsonl` glob slurped Claude Code `Workflow`
-- orchestration journals (`subagents/workflows/<wf>/journal.jsonl`). Those
-- records carry no `sessionId`, so they normalized to events with an empty
-- `session_id` (harness = 'claude-code'), which break `list_sessions` for any
-- time range overlapping them and pollute search.
--
-- This migration deletes those orphan rows — and the search fan-out derived
-- from them — from every table that keys on `session_id` (+ `harness`). The MV
-- targets (search_documents, search_postings, search_conversation_terms) are
-- physical tables populated at ingest, so deleting the base `events` rows does
-- NOT remove them; each is purged explicitly. (search_interaction_log keys on
-- event_uid only and has no writer in the codebase, so it holds no junk and is
-- not touched.)
--
-- Predicate is scoped to the junk only: `session_id = '' AND harness =
-- 'claude-code'` (the canonical harness value post-012). No legitimate row
-- has an empty session_id, so `search_conversation_terms` — a SummingMergeTree
-- aggregate with no harness column — is purged on `session_id = ''` alone.
--
-- Each `ALTER TABLE ... DELETE` runs with `SETTINGS mutations_sync = 1` so the
-- statement only returns once the mutation has actually completed on the
-- server: the migration is recorded as applied only after the junk is gone,
-- and a mutation that fails surfaces as a migration error rather than being
-- silently lost. The deletes are idempotent — the whole file re-runs if any
-- statement fails, and a re-run matches zero rows once clean. Mutations operate
-- on physical rows across all parts, so ReplacingMergeTree version columns and
-- merge state are irrelevant here.

-- Base event tables.
ALTER TABLE moraine.events DELETE WHERE session_id = '' AND harness = 'claude-code' SETTINGS mutations_sync = 1;
ALTER TABLE moraine.raw_events DELETE WHERE session_id = '' AND harness = 'claude-code' SETTINGS mutations_sync = 1;
ALTER TABLE moraine.event_links DELETE WHERE session_id = '' AND harness = 'claude-code' SETTINGS mutations_sync = 1;
ALTER TABLE moraine.tool_io DELETE WHERE session_id = '' AND harness = 'claude-code' SETTINGS mutations_sync = 1;

-- Search fan-out (materialized-view target tables).
ALTER TABLE moraine.search_documents DELETE WHERE session_id = '' AND harness = 'claude-code' SETTINGS mutations_sync = 1;
ALTER TABLE moraine.search_postings DELETE WHERE session_id = '' AND harness = 'claude-code' SETTINGS mutations_sync = 1;

-- Conversation-term aggregates have no harness column; an empty session_id can
-- only originate from this junk, so scope on session_id alone.
ALTER TABLE moraine.search_conversation_terms DELETE WHERE session_id = '' SETTINGS mutations_sync = 1;

-- Search query telemetry (defensive; only populated if junk was ever returned
-- as a search hit).
ALTER TABLE moraine.search_hit_log DELETE WHERE session_id = '' AND harness = 'claude-code' SETTINGS mutations_sync = 1;
