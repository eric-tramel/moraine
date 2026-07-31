-- Issue #603 WI-07 — the semantic half of the read-index reclaim probe, as a
-- `clickhouse local` fixture. Run after `fixture_read_index_ddl.sql` against
-- the same `--path`; the recipe lives in that file's header.
--
-- NOTHING IN `cargo test` RUNS THIS.
-- `the_read_index_fixture_files_are_this_builds_statements_verbatim` compares
-- generated strings against the checked-in files under `expected/`; it starts
-- no ClickHouse. The `reclaim-generation` live gate (plan §7.2) is unwritten
-- in this branch. This fixture is what was actually executed, by hand, and it
-- is checked in so the next reader can execute it too.
--
-- Expected probe output (executed 2026-07-31, expected/read_index.sql):
--   source_host source_name source_file  source_generation nav loc dir
--   h           codex       /live.jsonl  5                 3   2   2
--   and NOTHING else:
--     /live.jsonl 6    — the current head (R2 keeps it)
--     /fresh.jsonl 2   — retired, but its superseding head was published one
--                        minute ago; the horizon protects the freshly
--                        superseded generation a just-pinned reader (or an
--                        operator about to roll back) can still need (R3)
--     /unpub.jsonl 2   — in the indexes but never published: exactly what a
--                        publication in flight looks like (children first,
--                        head last), so R1 excludes it structurally
--     /nohead.jsonl 1  — a file with no published head at all (R2's INNER
--                        JOIN, fail-closed)
--     /empty.jsonl 1   — retired in history but the indexes hold no rows for
--                        it: not a unit (R4), or it would be re-claimed on
--                        every tick forever
--
-- Expected delete-script output (executed 2026-07-31,
-- expected/read_index_delete.sql):
--   reader-visible (head-joined) census, IDENTICAL at 'before', 'mid-unit'
--   (after the navigation delete, before the locator/directory deletes) and
--   'after' — the proof that a reader pinned to the published heads cannot
--   straddle a partially-reclaimed generation:
--     directory  4
--     locator    5
--     navigation 5
--   physical survivor census:
--     /empty.jsonl  2  1 1 1
--     /fresh.jsonl  2  1 1 1
--     /fresh.jsonl  3  1 1 1
--     /live.jsonl   5  gone from all three tables (absent from the census)
--     /live.jsonl   6  2 2 1
--     /nohead.jsonl 1  1 1 1
--     /unpub.jsonl  1  1 1 1
--     /unpub.jsonl  2  1 1 1
--
-- The mutation ledger these cases witness is in the PR description; each row
-- was produced by editing the probe or predicate text and re-running the
-- recipe.

-- ===================== publication truth ==================================
-- /live.jsonl: gen 5 was published, then gen 6 superseded it 30 days ago.
-- /fresh.jsonl: gen 2 was published, then gen 3 superseded it ONE MINUTE ago.
-- /unpub.jsonl: gen 1 is the head, published 30 days ago; gen 2 exists only
--               in the indexes (a publication in flight / crashed prepare).
-- /empty.jsonl: gen 1 retired by gen 2 thirty days ago, but the indexes hold
--               no gen-1 rows.
-- /nohead.jsonl: never published at all.
INSERT INTO moraine.v_published_source_generation_history VALUES
  ('h', 'codex', '/live.jsonl', 5),
  ('h', 'codex', '/live.jsonl', 6),
  ('h', 'codex', '/fresh.jsonl', 2),
  ('h', 'codex', '/fresh.jsonl', 3),
  ('h', 'codex', '/unpub.jsonl', 1),
  ('h', 'codex', '/empty.jsonl', 1),
  ('h', 'codex', '/empty.jsonl', 2);

INSERT INTO moraine.v_current_published_source_generations VALUES
  ('h', 'codex', '/live.jsonl', 6, 600, now64(3) - toIntervalDay(30)),
  ('h', 'codex', '/fresh.jsonl', 3, 700, now64(3) - toIntervalMinute(1)),
  ('h', 'codex', '/unpub.jsonl', 1, 500, now64(3) - toIntervalDay(30)),
  ('h', 'codex', '/empty.jsonl', 2, 800, now64(3) - toIntervalDay(30));

-- ===================== read-index rows ====================================
-- /live.jsonl gen 5 carries the double-attribution shape (#608): uid `d1` is
-- one physical line attributed to BOTH sessions S-A and S-B — two navigation
-- rows, two directory rows, ONE locator row (the uid excludes the session, so
-- the locator collapses to one arbitrary winner). The generation-tuple delete
-- removes all of them together; nothing is predicated on `session_id` (H2).
INSERT INTO moraine.mcp_event_navigation
  (session_id, sort_time, source_host, source_file, source_generation, source_offset, source_line_no, event_uid, event_version, source_name) VALUES
  ('S-A', now64(3) - toIntervalDay(40), 'h', '/live.jsonl', 5, 10, 1, 'd1', 100, 'codex'),
  ('S-B', now64(3) - toIntervalDay(40), 'h', '/live.jsonl', 5, 10, 1, 'd1', 100, 'codex'),
  ('S-A', now64(3) - toIntervalDay(40), 'h', '/live.jsonl', 5, 20, 2, 'u2', 100, 'codex'),
  ('S-A', now64(3) - toIntervalDay(29), 'h', '/live.jsonl', 6, 10, 1, 'v1', 200, 'codex'),
  ('S-A', now64(3) - toIntervalDay(29), 'h', '/live.jsonl', 6, 20, 2, 'v2', 200, 'codex'),
  ('S-F', now64(3) - toIntervalDay(5), 'h', '/fresh.jsonl', 2, 10, 1, 'f2', 300, 'codex'),
  ('S-F', now64(3) - toIntervalMinute(1), 'h', '/fresh.jsonl', 3, 10, 1, 'f3', 400, 'codex'),
  ('S-U', now64(3) - toIntervalDay(29), 'h', '/unpub.jsonl', 1, 10, 1, 'p1', 500, 'codex'),
  ('S-U', now64(3), 'h', '/unpub.jsonl', 2, 10, 1, 'p2', 600, 'codex'),
  ('S-N', now64(3) - toIntervalDay(40), 'h', '/nohead.jsonl', 1, 10, 1, 'n1', 700, 'codex'),
  ('S-E', now64(3) - toIntervalDay(29), 'h', '/empty.jsonl', 2, 10, 1, 'e2', 800, 'codex');

INSERT INTO moraine.mcp_event_locator
  (event_uid, source_host, event_version, session_id, source_name, source_file, source_generation) VALUES
  ('d1', 'h', 100, 'S-A', 'codex', '/live.jsonl', 5),
  ('u2', 'h', 100, 'S-A', 'codex', '/live.jsonl', 5),
  ('v1', 'h', 200, 'S-A', 'codex', '/live.jsonl', 6),
  ('v2', 'h', 200, 'S-A', 'codex', '/live.jsonl', 6),
  ('f2', 'h', 300, 'S-F', 'codex', '/fresh.jsonl', 2),
  ('f3', 'h', 400, 'S-F', 'codex', '/fresh.jsonl', 3),
  ('p1', 'h', 500, 'S-U', 'codex', '/unpub.jsonl', 1),
  ('p2', 'h', 600, 'S-U', 'codex', '/unpub.jsonl', 2),
  ('n1', 'h', 700, 'S-N', 'codex', '/nohead.jsonl', 1),
  ('e2', 'h', 800, 'S-E', 'codex', '/empty.jsonl', 2);

INSERT INTO moraine.mcp_session_directory
  (session_id, source_host, source_name, source_file, source_generation, harness) VALUES
  ('S-A', 'h', 'codex', '/live.jsonl', 5, 'codex'),
  ('S-B', 'h', 'codex', '/live.jsonl', 5, 'codex'),
  ('S-A', 'h', 'codex', '/live.jsonl', 6, 'codex'),
  ('S-F', 'h', 'codex', '/fresh.jsonl', 2, 'codex'),
  ('S-F', 'h', 'codex', '/fresh.jsonl', 3, 'codex'),
  ('S-U', 'h', 'codex', '/unpub.jsonl', 1, 'codex'),
  ('S-U', 'h', 'codex', '/unpub.jsonl', 2, 'codex'),
  ('S-N', 'h', 'codex', '/nohead.jsonl', 1, 'codex'),
  ('S-E', 'h', 'codex', '/empty.jsonl', 2, 'codex');
