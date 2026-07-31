-- Issue #603 WI-09 — the dual-attributed-uid case (plan §7.2, G-DUALUID's
-- shape as a `clickhouse local` stand-in). Run after
-- `fixture_canonical_ddl.sql` against the same `--path`; step 4 of the recipe
-- in that file's header.
--
-- NOTHING IN `cargo test` RUNS THIS — the same contract as
-- `fixture_canonical.sql`: the golden test compares generated statements
-- against `expected/canonical_dualuid_delete.sql`; this fixture is what was
-- actually executed, by hand, and it is checked in so the next reader can
-- execute it too.
--
-- ## The shape
--
-- `event_uid` material is `file|generation|line|offset|fingerprint|suffix`
-- (`sources/shared.rs`) — it EXCLUDES `source_name`. Source names and their
-- file globs are free-form config, so two overlapping `[sources]`
-- definitions on one host can reach the same physical line: one uid, TWO
-- attributions under two source names. Here uid `dU1` exists under
--   (h, codex,    /dup.jsonl, 5)  — RETIRED: codex's head is generation 6;
--   (h, codexalt, /dup.jsonl, 5)  — LIVE: codexalt's head IS generation 5.
-- A delete for the retired `codex` unit that binds only `(source_host,
-- event_uid)` (tool_io/event_links) or only `source_host` + `doc_id`
-- (search_postings) takes the LIVE `codexalt` rows with it — which is why
-- every derived predicate binds the row's own `source_name` as well.
-- Measured on the reference host 2026-07-31: uniqExact(host, uid) equals
-- uniqExact(host, name, uid) — no dual-attributed uid exists there today, so
-- the hazard is config-reachable rather than present; this fixture is where
-- its semantics are proven.
--
-- Expected probe output (executed 2026-07-31, expected/canonical.sql):
--   source_host source_name source_file  gen  event raw error document
--   h           codex       /dup.jsonl   5    1     1   0     1
--   and NOTHING else: codexalt's generation 5 is its own current head (R2)
--   and codex's generation 6 is codex's head.
--
-- Expected censuses (executed 2026-07-31,
-- expected/canonical_dualuid_delete.sql run directly after this file):
--   pinned-reader census, IDENTICAL at 'before', 'mid-unit' and 'after':
--     documents   2   (dU1 through its codexalt attribution — the FINAL
--                      survivor of the shared (dU1, h) sort key is the
--                      higher-doc_version codexalt row — and dU2)
--     event_links 1   (the codexalt row, source_event_version 700)
--     events      2   (dU2 under codex/6; dU1 under codexalt/5)
--     tool_io     1   (the codexalt row, source_event_version 700)
--   live per-term df, IDENTICAL at all three points:
--     delta 1  (dU1's codexalt posting)     epsilon 1  (dU2)
--   physical survivor census ('after'):
--     every `@codex` generation-5 row is gone from all seven tables; every
--     `@codexalt` row and codex's generation-6 rows survive — including the
--     codexalt posting, tool_io and event_links rows that share their uid
--     with the deleted unit. (The one surviving `uid:dU@codex` posting tag
--     is dU2's, the generation-6 `epsilon` posting: the tag prefix cannot
--     tell dU1 from dU2.)
--
-- The mutation rows this fixture witnesses (dropping the `source_name` bind
-- from each of the three satellite predicates, separately) are in the PR
-- description's executed mutation ledger.

-- ===================== publication truth ==================================
INSERT INTO moraine.v_current_published_source_generations VALUES
  ('h', 'codex',    '/dup.jsonl', 6, 70, now64(3) - INTERVAL 30 DAY),
  ('h', 'codexalt', '/dup.jsonl', 5, 71, now64(3) - INTERVAL 30 DAY);

INSERT INTO moraine.v_published_source_generation_history VALUES
  ('h', 'codex',    '/dup.jsonl', 5, 65, now64(3) - INTERVAL 60 DAY),
  ('h', 'codex',    '/dup.jsonl', 6, 70, now64(3) - INTERVAL 30 DAY),
  ('h', 'codexalt', '/dup.jsonl', 5, 71, now64(3) - INTERVAL 30 DAY);

-- ===================== events =============================================
-- dU1 is the dual-attributed uid: one physical line, two source names, two
-- events rows (their sort keys differ in session and name, so FINAL keeps
-- both). dU2 exists only under codex's live generation 6.
INSERT INTO moraine.events VALUES
  ('2026-06-10 10:00:00.000', 'dU1', 'sX', 'h', 'codex',    '/dup.jsonl', 5, 1, 1, '2026-06-01 09:00:00.000', 500),
  ('2026-06-15 11:00:00.000', 'dU2', 'sX', 'h', 'codex',    '/dup.jsonl', 6, 1, 1, '2026-06-01 09:00:00.000', 601),
  ('2026-06-10 12:00:00.000', 'dU1', 'sY', 'h', 'codexalt', '/dup.jsonl', 5, 1, 1, '2026-06-01 09:00:00.000', 700);

-- ===================== raw_events (bucket 2) ==============================
INSERT INTO moraine.raw_events VALUES
  ('2026-06-10 10:00:00.000', 'h', 'codex',    '/dup.jsonl', 5, 1, 1, 'dU1'),
  ('2026-06-10 12:00:00.000', 'h', 'codexalt', '/dup.jsonl', 5, 1, 1, 'dU1');

-- ===================== search_documents ===================================
-- Separate INSERT blocks: the two dU1 attributions arrive in separate ingest
-- flushes, so both physical rows exist until a background merge collapses
-- them onto the higher doc_version (700, codexalt) — the durable state the
-- delete runs against, and the reader-visible state under FINAL either way.
INSERT INTO moraine.search_documents VALUES
  (500, '2026-06-10 10:00:00.000', 'dU1', 'sX', 'h', 'codex',    '/dup.jsonl', 5, 'rd1', 3);
INSERT INTO moraine.search_documents VALUES
  (700, '2026-06-10 12:00:00.000', 'dU1', 'sY', 'h', 'codexalt', '/dup.jsonl', 5, 'rd1', 3);
INSERT INTO moraine.search_documents VALUES
  (601, '2026-06-15 11:00:00.000', 'dU2', 'sX', 'h', 'codex',    '/dup.jsonl', 6, 'rd2', 2);

-- ===================== search_postings ====================================
-- The two `delta` postings share the (term, doc_id, source_host) sort key;
-- only the posting-side source_name bind separates the retired codex row
-- from the live codexalt row a name-blind delete would sweep.
INSERT INTO moraine.search_postings VALUES
  (500, 'delta',   'dU1', 'sX', 'h', 'codex',    '/dup.jsonl', 5, 'rd1', 3, 1);
INSERT INTO moraine.search_postings VALUES
  (700, 'delta',   'dU1', 'sY', 'h', 'codexalt', '/dup.jsonl', 5, 'rd1', 3, 1);
INSERT INTO moraine.search_postings VALUES
  (601, 'epsilon', 'dU2', 'sX', 'h', 'codex',    '/dup.jsonl', 6, 'rd2', 2, 1);

-- ===================== tool_io / event_links (uid-keyed, H3) ==============
-- Both tables hold one row per attribution of dU1: same (host, uid), two
-- names, two source_event_versions binding each row to its own parent event.
INSERT INTO moraine.tool_io VALUES
  ('2026-06-10 10:00:00.000', 'sX', 'c1', 'dU1', 'h', 'codex',    500, 500),
  ('2026-06-10 12:00:00.000', 'sY', 'c2', 'dU1', 'h', 'codexalt', 700, 700);

INSERT INTO moraine.event_links VALUES
  ('2026-06-10 10:00:00.000', 'sX', 'dU1', 'follows', 'dU2', 'h', 'codex',    500, 500),
  ('2026-06-10 12:00:00.000', 'sY', 'dU1', 'follows', 'dU9', 'h', 'codexalt', 700, 700);
