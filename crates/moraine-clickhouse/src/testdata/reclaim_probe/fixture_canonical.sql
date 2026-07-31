-- Issue #603 WI-09 — the semantic half of the canonical-generation reclaim
-- probe and deletes, as a `clickhouse local` fixture. Run after
-- `fixture_canonical_ddl.sql` against the same `--path`; the recipe lives in
-- that file's header.
--
-- NOTHING IN `cargo test` RUNS THIS.
-- `the_canonical_fixture_files_are_this_builds_statements_verbatim` compares
-- generated strings against the checked-in files under `expected/`; it starts
-- no ClickHouse. The `reclaim-generation` live gate (plan §7.2) is unwritten
-- in this branch. This fixture is what was actually executed, by hand, and it
-- is checked in so the next reader can execute it too.
--
-- Expected probe output (executed 2026-07-31, expected/canonical.sql):
--   source_host source_name source_file  gen  event raw error document
--   h           codex       /live.jsonl  5    3     2   1     2
--   (document_rows is 2, not 3: the two g5u2 document rows share one
--   (event_uid, source_host) sort key, and the ReplacingMergeTree collapses
--   them at insert — hazard H2's "one physical row serves both attributions",
--   observed rather than asserted. The three g5u2 EVENT rows survive because
--   `session_id` leads that table's sort key.)
--   and NOTHING else:
--     h  /live.jsonl 6   — the current head (R2 keeps it)
--     h  /fresh.jsonl 2  — retired, but its superseding head was published
--                          one minute ago; the 7-day horizon protects it (R3)
--     h  /nohead.jsonl 1 — never published at all (R1/R2, fail-closed)
--     h  /empty.jsonl 1  — retired in history but no table holds rows: not a
--                          unit (R4)
--     h2 /live.jsonl 5   — ANOTHER HOST's copy of the same file at the same
--                          generation, sharing the same uids (the uid excludes
--                          the host): h2's head, live, and never a candidate
--     '' /pre032.jsonl 1 — a pre-032-era head; its posting carries the
--                          back-filled ''/0 source columns (hazard H1) under
--                          a LIVE document and must survive everything below
--
-- Expected censuses (executed 2026-07-31, expected/canonical_delete.sql run
-- directly after this file — the uninterrupted unit):
--   pinned-reader census, IDENTICAL at 'before', 'mid-unit' (after the four
--   satellite deletes, before any bucket-1/2 delete) and 'after':
--     documents   5      (g6u1, f3u1, p1u1, h2:g5u1, h2:g5u2; the live
--                         g6u2 tombstone is doc_len=0 and never visible)
--     event_links 1      (h:g6u1)
--     events      6      (g6u1 ONCE — two physical rows in two monthly
--                         partitions, one under FINAL (sql/019) — g6u2,
--                         f3u1, p1u1, h2:g5u1, h2:g5u2)
--     tool_io     2      (h:g6u1, h2:g5u1)
--   live per-term df, IDENTICAL at all three points:
--     alpha 2  (h:g6u1 + h2:g5u1)   beta 1 (h2:g5u2)
--     gamma 1  (f3u1)               preterm 1 (the H1 witness)
--   physical survivor census ('after'):
--     /live.jsonl#5 rows survive ONLY for h2 (events 2, raw 1, docs 2);
--     h's generation 5 is gone from all seven tables; every other
--     generation's physical rows are intact, including the two-row FINAL
--     pair of /live.jsonl#6 and the ''-host /pre032.jsonl posting.
--
-- Expected interrupted-run censuses (executed 2026-07-31,
-- expected/canonical_interrupt.sql after this file, then
-- expected/canonical_delete.sql as the re-drive):
--   'interrupted' pinned-reader census and df: identical to 'before' — the
--   crash window leaves readers untouched;
--   'interrupted' physical census: h's gen-5 SATELLITES gone
--   (search_postings/search_documents/tool_io/event_links), its
--   events/raw_events/ingest_errors rows INTACT — derived data missing over
--   intact canonical data, the re-derivable half-state;
--   the re-drive then reports the same 'before'/'mid-unit'/'after' censuses
--   as the uninterrupted run (its satellite deletes remove zero additional
--   rows) and the same final physical census.
--
-- The mutation ledger these cases witness is in the PR description; each row
-- was produced by editing the probe or predicate text and re-running the
-- recipe.

-- ===================== publication truth ==================================
-- Heads: /live.jsonl@h is at gen 6 (published 30 d ago; gen 5 retired past
-- the 7-day fixture horizon). /fresh.jsonl@h is at gen 3 (published ONE
-- MINUTE ago; gen 2 inside the horizon). /empty.jsonl@h is at gen 2 (gen 1
-- retired, no rows anywhere). /live.jsonl@h2 is at gen 5 — the same file and
-- generation h retired, still live on the other host. /pre032.jsonl@'' is a
-- pre-032-era head whose rows carry the back-filled '' host.
INSERT INTO moraine.v_current_published_source_generations VALUES
  ('h',  'codex', '/live.jsonl',   6, 60, now64(3) - INTERVAL 30 DAY),
  ('h',  'codex', '/fresh.jsonl',  3, 61, now64(3) - INTERVAL 1 MINUTE),
  ('h',  'codex', '/empty.jsonl',  2, 62, now64(3) - INTERVAL 30 DAY),
  ('h2', 'codex', '/live.jsonl',   5, 63, now64(3) - INTERVAL 30 DAY),
  ('',   'codex', '/pre032.jsonl', 1, 64, now64(3) - INTERVAL 30 DAY);

INSERT INTO moraine.v_published_source_generation_history VALUES
  ('h',  'codex', '/live.jsonl',   5, 50, now64(3) - INTERVAL 60 DAY),
  ('h',  'codex', '/live.jsonl',   6, 60, now64(3) - INTERVAL 30 DAY),
  ('h',  'codex', '/fresh.jsonl',  2, 51, now64(3) - INTERVAL 60 DAY),
  ('h',  'codex', '/fresh.jsonl',  3, 61, now64(3) - INTERVAL 1 MINUTE),
  ('h',  'codex', '/empty.jsonl',  1, 52, now64(3) - INTERVAL 60 DAY),
  ('h',  'codex', '/empty.jsonl',  2, 62, now64(3) - INTERVAL 30 DAY),
  ('h2', 'codex', '/live.jsonl',   5, 63, now64(3) - INTERVAL 30 DAY),
  ('',   'codex', '/pre032.jsonl', 1, 64, now64(3) - INTERVAL 30 DAY);

-- ===================== events =============================================
-- Fixture uids are prefixed g<generation>u… so the uid-keyed satellites stay
-- attributable in the physical census after their parent rows are gone (H3).
--
-- h /live.jsonl gen 5 (THE UNIT): three physical rows over two uids — g5u2
-- is DOUBLE-ATTRIBUTED (sessions sA and sB share one physical line; the uid
-- excludes the session, hazard H2's shape).
-- h /live.jsonl gen 6 (live head): g6u1 was re-ingested — same sort key, a
-- fresh ingested_at in a LATER MONTH and a higher event_version — so it is
-- two physical rows in two partitions and ONE row under FINAL, the sql/019
-- cross-partition dedup the reader census must not double-count.
-- h2 /live.jsonl gen 5: the SAME uids as h's retired generation, live.
INSERT INTO moraine.events VALUES
  ('2026-06-10 10:00:00.000', 'g5u1', 'sA', 'h',  'codex', '/live.jsonl',   5, 1, 1, '2026-06-01 09:00:00.000', 500),
  ('2026-06-10 10:00:01.000', 'g5u2', 'sA', 'h',  'codex', '/live.jsonl',   5, 2, 2, '2026-06-01 09:00:01.000', 501),
  ('2026-06-10 10:00:02.000', 'g5u2', 'sB', 'h',  'codex', '/live.jsonl',   5, 2, 2, '2026-06-01 09:00:01.000', 502),
  ('2026-06-15 11:00:00.000', 'g6u1', 'sA', 'h',  'codex', '/live.jsonl',   6, 1, 1, '2026-06-01 09:00:00.000', 601),
  ('2026-07-20 11:00:00.000', 'g6u1', 'sA', 'h',  'codex', '/live.jsonl',   6, 1, 1, '2026-06-01 09:00:00.000', 602),
  ('2026-07-20 11:00:01.000', 'g6u2', 'sB', 'h',  'codex', '/live.jsonl',   6, 2, 2, '2026-06-01 09:00:01.000', 603),
  ('2026-07-20 12:00:00.000', 'f2u1', 'sF', 'h',  'codex', '/fresh.jsonl',  2, 1, 1, '2026-07-01 08:00:00.000', 201),
  ('2026-07-20 12:00:01.000', 'f3u1', 'sF', 'h',  'codex', '/fresh.jsonl',  3, 1, 1, '2026-07-01 08:00:00.000', 301),
  ('2026-07-20 13:00:00.000', 'n1u1', 'sN', 'h',  'codex', '/nohead.jsonl', 1, 1, 1, '2026-07-01 07:00:00.000', 101),
  ('2026-06-10 14:00:00.000', 'g5u1', 'sA2', 'h2', 'codex', '/live.jsonl',  5, 1, 1, '2026-06-01 09:00:00.000', 520),
  ('2026-06-10 14:00:01.000', 'g5u2', 'sA2', 'h2', 'codex', '/live.jsonl',  5, 2, 2, '2026-06-01 09:00:01.000', 521),
  ('2026-06-10 15:00:00.000', 'p1u1', 'sP', '',   'codex', '/pre032.jsonl', 1, 1, 1, '2026-06-01 06:00:00.000', 11);

-- ===================== raw_events / ingest_errors (bucket 2) ==============
INSERT INTO moraine.raw_events VALUES
  ('2026-06-10 10:00:00.000', 'h',  'codex', '/live.jsonl', 5, 1, 1, 'g5u1'),
  ('2026-06-10 10:00:01.000', 'h',  'codex', '/live.jsonl', 5, 2, 2, 'g5u2'),
  ('2026-07-20 11:00:00.000', 'h',  'codex', '/live.jsonl', 6, 1, 1, 'g6u1'),
  ('2026-06-10 14:00:00.000', 'h2', 'codex', '/live.jsonl', 5, 1, 1, 'g5u1');

INSERT INTO moraine.ingest_errors VALUES
  ('2026-06-10 10:00:03.000', 'h', 'codex', '/live.jsonl', 5, 3, 3);

-- ===================== search_documents ===================================
-- ORDER BY (event_uid, source_host): h's two g5u2 rows (one per attributing
-- session) share one sort key and collapse under FINAL — one physical row
-- serves both attributions (H2). g6u1 carries its June/July doc_version pair
-- (the FINAL collapse again). g6u2 is a LIVE ZERO-LENGTH TOMBSTONE: never
-- visible to the reader census (doc_len > 0), physically present throughout.
INSERT INTO moraine.search_documents VALUES
  (500, '2026-06-10 10:00:00.000', 'g5u1', 'sA',  'h',  'codex', '/live.jsonl',   5, 'r51', 3),
  (501, '2026-06-10 10:00:01.000', 'g5u2', 'sA',  'h',  'codex', '/live.jsonl',   5, 'r52', 4),
  (502, '2026-06-10 10:00:02.000', 'g5u2', 'sB',  'h',  'codex', '/live.jsonl',   5, 'r52', 4),
  (601, '2026-06-15 11:00:00.000', 'g6u1', 'sA',  'h',  'codex', '/live.jsonl',   6, 'r61', 5),
  (602, '2026-07-20 11:00:00.000', 'g6u1', 'sA',  'h',  'codex', '/live.jsonl',   6, 'r61', 5),
  (603, '2026-07-20 11:00:01.000', 'g6u2', 'sB',  'h',  'codex', '/live.jsonl',   6, 'r62', 0),
  (201, '2026-07-20 12:00:00.000', 'f2u1', 'sF',  'h',  'codex', '/fresh.jsonl',  2, 'rf2', 2),
  (301, '2026-07-20 12:00:01.000', 'f3u1', 'sF',  'h',  'codex', '/fresh.jsonl',  3, 'rf3', 2),
  (101, '2026-07-20 13:00:00.000', 'n1u1', 'sN',  'h',  'codex', '/nohead.jsonl', 1, 'rn1', 2),
  (520, '2026-06-10 14:00:00.000', 'g5u1', 'sA2', 'h2', 'codex', '/live.jsonl',   5, 'r51', 3),
  (521, '2026-06-10 14:00:01.000', 'g5u2', 'sA2', 'h2', 'codex', '/live.jsonl',   5, 'r52', 4),
  (11,  '2026-06-10 15:00:00.000', 'p1u1', 'sP',  '',   'codex', '/pre032.jsonl', 1, 'pre', 3);

-- ===================== search_postings ====================================
-- The /pre032.jsonl posting is HAZARD H1 MADE FLESH: its own source_file is
-- '' and source_generation is 0 — the back-filled type defaults 83.6% of the
-- reference host's postings carry — while the document it serves is LIVE.
-- Any predicate consulting the posting's own generation columns sweeps it;
-- the shipped document-joined predicate cannot reach it.
-- h2's postings carry the SAME doc_ids as h's retired generation: only the
-- posting-side source_host bind separates them (the shared-backend hazard).
INSERT INTO moraine.search_postings VALUES
  (500, 'alpha',   'g5u1', 'sA',  'h',  'codex', '/live.jsonl', 5, 'r51', 3, 1),
  (502, 'beta',    'g5u2', 'sB',  'h',  'codex', '/live.jsonl', 5, 'r52', 4, 2),
  (602, 'alpha',   'g6u1', 'sA',  'h',  'codex', '/live.jsonl', 6, 'r61', 5, 1),
  (201, 'gamma',   'f2u1', 'sF',  'h',  'codex', '/fresh.jsonl', 2, 'rf2', 2, 1),
  (301, 'gamma',   'f3u1', 'sF',  'h',  'codex', '/fresh.jsonl', 3, 'rf3', 2, 1),
  (520, 'alpha',   'g5u1', 'sA2', 'h2', 'codex', '/live.jsonl', 5, 'r51', 3, 1),
  (521, 'beta',    'g5u2', 'sA2', 'h2', 'codex', '/live.jsonl', 5, 'r52', 4, 2),
  (11,  'preterm', 'p1u1', 'sP',  '',   'codex', '',            0, 'pre', 3, 1);

-- ===================== tool_io / event_links (uid-keyed, H3) ==============
INSERT INTO moraine.tool_io VALUES
  ('2026-06-10 10:00:00.000', 'sA',  'c1', 'g5u1', 'h',  'codex', 500, 500),
  ('2026-06-10 10:00:02.000', 'sB',  'c2', 'g5u2', 'h',  'codex', 502, 502),
  ('2026-07-20 11:00:00.000', 'sA',  'c3', 'g6u1', 'h',  'codex', 602, 602),
  ('2026-06-10 14:00:00.000', 'sA2', 'c1', 'g5u1', 'h2', 'codex', 520, 520);

INSERT INTO moraine.event_links VALUES
  ('2026-06-10 10:00:00.000', 'sA', 'g5u1', 'follows', 'g5u2', 'h', 'codex', 500, 500),
  ('2026-07-20 11:00:00.000', 'sA', 'g6u1', 'follows', 'g6u2', 'h', 'codex', 602, 602);
