# One frozen golden of the retired v1 `mcp_open_*` projector

`projected_publication_header.sql` is a **historical record, not a live
fixture.** Nothing `include_str!`s it and no test executes it.

Issue #603 WI-10 deleted the v1 projector (`mcp_open_projection.rs`) and
migration 041 dropped the `mcp_open_*` family it wrote. The snapshot test that
used to pin this file against the projector's generated SQL went with it, so
these bytes are inert: they record what the projector emitted at the moment it
was retired, and nothing keeps them in step with anything, because there is no
longer anything for them to be in step with.

## Why this one file survives

Four places derive a value or an expectation from it by citation, and without
the file those would be unfalsifiable magic values. Every citation of it —
here and in those four files — spells the file name in full rather than a bare
`:LINE`, because `storage_class::tests::a_cross_file_line_citation_resolves_to_what_it_claims`
resolves citations by file name: a bare line number is a citation nothing can
check, and this file's lines are frozen precisely so that they can be.

- `moraine-conversations/src/clickhouse_repo/canonical_open.rs` —
  `open_title_and_summary` and `list_title_and_summary`, production code rather
  than gates, deriving no pinned expectation of their own, reproduce the
  projector's OPEN and LIST title/summary chains. Only the OPEN one is cited by
  line: `open_title_and_summary`'s docstring carries
  `projected_publication_header.sql:120,122`, and it is the only line-numbered
  citation this file has. `list_title_and_summary`'s docstring names the golden
  by path with no line numbers at all.
- `moraine-conversations/tests/live_clickhouse/session_list_parity.rs` — the
  `list-parity` gate's `EXPECTED_LIST_METADATA` is transcribed from
  `projected_publication_header.sql:123-124`, and is what stops a
  both-arms-wrong drift from passing the page-size diff. This is the one
  **pinned expectation** derived from the golden. It is also the only place
  outside this README that cites the LIST chain by line — the tree's other two
  `projected_publication_header.sql:123-124` citations are the one above and
  this one.
- `moraine-clickhouse/src/canonical_derivations.rs` —
  `MAX_PROJECTED_TEXT_SUMMARY_CHARS` (65 536) is the v1 hydration cap the v2
  reader inherits; `projected_publication_header.sql:45`'s
  `leftUTF8(text_content, 65536)` is the surviving record of it.
- `sql/037_search_ranking_metadata.sql` — its supersession note points the
  DIGEST DOMAIN argument's evidence here, at the `canonical` CTE that shows the
  projector copied `events.text_content` verbatim. The migration's original
  citation was `mcp_open_projection.rs`, which WI-10 deleted.

If every citation is ever replaced by an oracle that does not depend on the
projector's output, delete this directory with them.

## Why the other seven goldens are gone

WI-10's review round 3 ran a per-file citation census. `batch_projected_events.sql`,
`batch_projected_turns.sql`, `event_type.sql`, `projection_ctes_child.sql`,
`projection_ctes_reval.sql`, `single_projected_events.sql` and
`single_projected_turns.sql` — 47,165 bytes — had **zero** citations from any
source file, test, doc or migration. They were being kept under a justification
that was true of exactly one of the eight. In a PR whose purpose is deleting
what nothing needs, they went.

## The four deleted `reclaim_probe` fixtures

The same round-3 census removed `reclaim_probe/fixture.sql` and
`reclaim_probe/expected/{delete,orphan,retired}.sql`. Those were the hand-run
fixture for the `mcp_open` reclaim probe — the executor WI-10 deletes and the
tables migration 041 drops — and they had zero surviving citations for the same
reason: there is nothing left to run them against. The `reclaim_probe`
fixtures for the two LIVE scopes (`read_index` and `canonical`) are untouched
and still cited by `reclaim_read_index.rs` and `reclaim_canonical.rs`.
