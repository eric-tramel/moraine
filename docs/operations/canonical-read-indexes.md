# Canonical read indexes and the `open` reader

Moraine's `open` tool family (`open(session|turn|event)`) reads through a
page-aware reader backed by content-free **canonical read indexes** (issue
#598). The full-content `mcp_open_*` projection it replaced is retired: issue
#603 WI-10 removed the v1 reader and projector, and migration 041 drops the
projection tables outright once a store has cut over. This page is the
operator reference for the canonical reader: how to inspect its readiness and
how to repair it.

## Architecture overview

The legacy `open` path served a full-content **projection** (`mcp_open_*`) that
was rebuilt for every touched session after each ingest flush — turning a
fixed-size append into work proportional to the whole prior session. The
canonical reader retires that steady-state rebuild: canonical `events` rows are
the content authority, and `open` reconstructs only the requested session (or
turn, or event) on demand, one keyset page at a time.

Two ideas make that cheap:

- **Content-free read indexes (migration 036).** Three small index tables carry
  only scalars — sort keys, kinds, identifiers, versions, and precomputed
  boolean flags — never text, payloads, summaries, or turn bodies. They are
  maintained incrementally by join-free materialized views on every `events`
  insert block, so index maintenance is `O(new events)` and never rereads prior
  history. `mcp_event_navigation` provides the session-ordered paging key,
  `mcp_event_locator` provides an exact per-event seek, and
  `mcp_session_directory` provides per-session discovery scalars.
- **A page-aware reader.** `open` filters to the requested session and a pinned
  live publication revision, reads narrow ordering/classification columns first,
  derives event order, turns, summaries, and neighbors over that bounded set,
  and only then hydrates the wide `text_content` / `payload_json` columns for the
  events on the returned page. A fixed-size append never scans or rewrites prior
  session content, and continuations reuse a cursor carry so a quiescent next
  page touches only a directory point-read.

The deterministic navigation sort key (`record_ts` parsed, or an epoch sentinel
when it does not parse — never the unstable `ingested_at`) means a re-inserted
or re-normalized event collapses to the same index row instead of producing a
ghost, so cursor continuations can validate an anchor exactly. The full design
rationale lives with issue #598; this page is the operator surface.

## What migration 036 installs

`sql/036_canonical_read_indexes.sql` adds three content-free index tables and
their maintaining materialized views, plus a small state table:

| Object | Role |
|---|---|
| `mcp_session_directory` | scalar discovery hints per session/source generation |
| `mcp_event_locator` | versioned exact-event seek for `open(event)` |
| `mcp_event_navigation` | content-free session-order index for paging |
| `mcp_read_index_state` | coverage cursor + per-consumer readiness + audit record |

The materialized views maintain the indexes from every new `events` insert
block. A one-shot **backfill** sweeps the pre-existing corpus into the same
tables, runs an overlap audit, and publishes readiness. The backfill runs
automatically at the end of every `moraine up` / `moraine db migrate`, is
crash-resumable from a durable per-page cursor, and is idempotent with the live
views.

Readiness lives in `mcp_read_index_state` under three keys:

- `core_indexes` — the coverage sweep cursor and its `ready` flag. `ready = 1`
  means the sweep completed **and** the overlap audit passed.
- `core_audit` — the persisted overlap-audit outcome (pass/fail plus counts).
- `open_v2` — the one-way `open` cutover flag consumers read. `cursor` records
  how it was published: `auto-local` (the Local auto-gate) or `operator-promote`
  (the explicit command below).

## Staged cutover status

The canonical reader is landing one consumer at a time, not as a single
cross-surface switch. As of this build:

- **`open(session|turn|event)` is cut over.** The `open` tool family reads
  canonical rows through the page-aware reader — the only reader since issue
  #603 WI-10. While a fresh store's first sweep has not yet published
  `open_v2` readiness, `open` fails with a typed error naming the sweep.
- **Session discovery is cut over** (issue #599). MCP `list_sessions` and the
  monitor `/api/v1/sessions` feed are now one shared repository operation that
  selects candidates from `mcp_session_directory` and hydrates only the
  candidate page from `mcp_event_navigation` plus the metadata-bearing `events`
  rows. It reads no `mcp_open_*` relation and no `v_session_summary` /
  `v_conversation_trace` / `v_turn_summary` view.
  - **Discovery by CONTENT shares the same hydration.** The monitor's
    `/api/v1/sessions/search` route picks candidates with issue #597's bounded
    postings ranking and then runs the identical hydration and fold, so the
    project-scope re-check, the exact `harness`/`source` re-check, the tombstone
    rule, and the title/summary precedence are single-sourced across both
    discovery surfaces. Having no keyset page of its own, it also issues one
    `mcp_session_directory` point-range read over the ids it ranked, so the
    `updated_at` it reports is the same `max(max_observed_event_time)` the feed
    orders, pages and renders by rather than a second aggregate of its own —
    which is what lets both surfaces derive the same `status` for one session at
    one instant. It branches on the readiness gate below exactly as the feed
    does: a not-ready store refuses typed rather than answering with an empty
    result set that would read as "the whole corpus was searched".
  - **Readiness gate.** The shared operation serves the directory path only
    when the `open_v2` key reads ready — the same key `open` consults. With
    the projected-header fallback retired (issue #603 WI-10), an unpublished
    store refuses the page with a typed error instead of answering from an
    incomplete index. The negative is never cached, so the flip needs no
    restart; the positive is latched per process.
  - **Continuation tokens are path-tagged.** A cursor minted by one path is
    refused by the other with a cursor mismatch rather than silently resuming,
    because the two paths anchor on values that a readiness flip can move
    apart. A client that sees a mismatch restarts its feed from page 1.
  - The projected-header fallback those tokens used to guard against is gone
    (its tables with it, migration 041); the path tag survives so a token
    minted before the retirement is refused with the same cursor mismatch.
  - **Directory scan cost is linear in sessions, not in event bytes.**
    `mcp_session_directory` leads its sort key with `session_id`, so a
    time-windowed candidate page cannot be granule-pruned and the candidate
    pass groups every directory row. Directory rows grow with
    `sessions × source-files` and are narrow scalars, so adding transcript
    content to existing sessions adds none. If the `list-bench` gate ever
    measures the candidate pass reading more than 3× the directory's row count,
    or its wall time exceeds 500 ms at 100k sessions, that is the trigger to
    add a time-leading projection in a follow-up migration — not something to
    tune here.
- **Search reads the bounded canonical engine.** Ranking runs over
  `search_postings` joined to the live locator, and hydration reads the
  navigation index plus bounded canonical `events` rows.
- **The v1 projector is retired** (issue #603 WI-10). The projector, its
  dirty-session materialized view, the compatibility publication bridge, the
  startup backfill, and the `mcp_open` reclaim scopes are gone from the
  binary, and migration 041 drops the `mcp_open_*` tables themselves. The
  drop is gated: on a store that has not cut over, the migration defers with
  a named reason until the canonical sweep publishes `open_v2` readiness (the
  migrate/`up` sequence retries it in the same startup), so the projection
  bytes are only released once nothing can need them. The migration's
  preflight note reports the compressed column bytes the drop returned
  (`sum(data_compressed_bytes)` over the family's active `system.parts`).

The cutover is validated by three live gates —
`scripts/dev/sandbox/run-live-test list-parity`, `… list-query-log`, and
`… list-bench`. See [Testing and benchmarking](../development/testing.md).

## Append-to-visible latency contract

For an active file-backed session on the reference host, a committed append
becomes visible through the canonical `open` reader within **2 seconds at p95**,
measured from ingest acknowledgement (the durable `events` insert) to the first
valid `open` that reflects the appended event. This is the realtime contract the
canonical reader must hold; the projector rebuild it replaced could stall this
path under load.

The gate is validated by the pre-wired live-test modes `append-to-visible`
(`insert-ack → first-valid-open` p95 ≤ 2 s) and `fsync-to-open-valid`
(end-to-end `fsync → first-valid-open`, reported alongside). Structured
retry/reopen responses during an insert-only append fence count as
not-yet-visible samples, never failures. Polled SQLite/NAC/Cursor sources retain
their documented polling latency until their delta-scan follow-up lands. See
[Testing and benchmarking](../development/testing.md) for how to run these
modes.

## The reader selector: `[mcp] open_reader` (retired to one reader)

```toml
[mcp]
# "auto" (default) | "v2" (synonym) | "v1" (accepted-and-noted, retired)
# open_reader = "auto"   # leave unset
```

The value is validated at config load; an unknown value is rejected with a
friendly error. Since issue #603 WI-10 retired the v1 projection there is one
reader, so the selector resolves as follows:

- **`auto`** (default) and **`v2`** are synonyms: serve the canonical reader
  once `open_v2.ready == 1`; fail with a typed error naming the sweep
  otherwise. Once readiness is observed in a process it stays observed
  (monotonic; no mid-run demotion).
- **`v1`** — the former kill-switch — is still **accepted** so an existing
  `moraine.toml` cannot brick the load on upgrade, but it selects nothing:
  reads are served by the canonical reader, and `moraine status` /
  `moraine db doctor` render a retirement note until the key is removed.

## Inspecting readiness

`moraine db core-index status` prints the full picture:

```
$ moraine db core-index status
Canonical Read Indexes
  core indexes ready: yes
  open v2 ready: yes (auto-local)
  backfill cursor age: 3h12m
  overlap audit: pass (sessions=128, events=32768, nav_missing=0, loc_missing=0, dir_missing=0, cardinality_delta=0)
  open reader: configured=auto, effective=v2
```

The same fields are additive JSON under `--output json`, and are surfaced by:

- `moraine db doctor` — appends the core-index lines to the doctor report
  (`--output json` nests them under `core_index` beside the existing `doctor`
  object; the `doctor` shape is unchanged).
- `moraine status` — a concise `core indexes: … | open reader: …` line in the
  Database panel, plus a prominent note whenever the retired `v1` selector is
  still configured or the indexes are not ready.

Core-index readiness is a normal transient state (like publication replaying);
it does **not** fail the doctor exit code.

> **Status/doctor read live state; serving processes do not.** Each backend or
> MCP process samples `open_v2` readiness once at construction and keeps that
> answer for its lifetime. After an in-place `moraine db migrate` publishes
> readiness under a running backend daemon, `status`/`doctor` report the
> canonical reader as ready while the daemon keeps refusing `open` reads with
> the typed unready error until it is restarted (`moraine down && moraine
> up`). Doctor output describes the state a *newly started* process would
> adopt, not necessarily what a long-running daemon is doing right now.
>
> **Session discovery is the one exception.** Its readiness gate caches only a
> ready answer, never a not-ready one, so a long-running daemon adopts the
> directory path on the first `list_sessions` or `/api/v1/sessions` request
> after readiness is published — no restart, and no window in which the monitor
> session-detail route is pinned to `503`. While readiness is 0 each such
> request pays one point-read of `mcp_read_index_state`.

> **Monitor `/api/v1/health`** is intended to expose the same readiness fields.
> Wiring that endpoint requires an additive `StoreHealth` field in the
> repository crate (`moraine-conversations`); see the deviation note at the end
> of this page.

## Operator commands

### `moraine db core-index status`

Read-only. Prints readiness, backfill cursor age (decoded from the state row's
snowflake generation), the overlap-audit outcome, and the active/effective
open-reader mode. Safe to run any time; returns "unavailable" if ClickHouse is
unreachable or migration 036 has not been applied.

### `moraine db core-index rebuild`

Resets the `open_v2` / `core_audit` / `core_indexes` readiness rows to
`ready = 0` (in that order — readiness is revoked before anything destructive
runs), truncates the three index tables, then re-runs the backfill from
scratch (the same engine `moraine db migrate` uses, with one Migration-class
query envelope per page). Use it when the indexes are suspected stale or
corrupt, or after a schema change that invalidated them.

**Rebuild is not transparent to already-running readers.** Every process reads
`open_v2` readiness once at backend construction and caches it for its
lifetime (that monotonic cache is what makes the v2 flip one-way). Revoking
readiness therefore only reaches processes started afterwards: a backend
daemon or stdio MCP process that already cached v2 keeps resolving v2 against
the truncated indexes for the whole re-sweep — serving "not found" and
silently truncated sessions — and indefinitely if the rebuild fails midway.
Before (or immediately after) starting a rebuild, restart the running stack:

```
moraine down && moraine up
```

Processes started while `open_v2.ready` is 0 refuse `open`/`list` reads with
a typed error and adopt the canonical reader at their next start after the
rebuild republishes readiness. The `rebuild` command prints this warning
before touching anything — mid-rebuild reads are refusals, not stale answers,
so schedule the rebuild accordingly.

On a Shared backend, run the rebuild and then re-`promote`; reader processes
serve typed refusals until the promotion republishes readiness.

A rebuild also honors the promote ceremony: when the pre-rebuild `open_v2` row
was published with `operator-promote` provenance, the rebuild withholds the
auto-publication (recording a `withheld-non-local` marker) and the explicit
`moraine db core-index promote --force` remains the only way to re-publish —
the rebuild never silently re-flips a promoted backend as `auto-local`.

### `moraine db core-index promote`

Publishes `open_v2.ready = 1` with `operator-promote` provenance. This is the
**explicit, non-Local path**: the Local auto-gate deliberately withholds
`open_v2` for Shared/multi-writer backends, so a shared backend only switches to
v2 when an operator promotes it. It is also how you re-publish `open_v2` after a
`rebuild`.

Promotion is refused unless `core_indexes.ready == 1` and the overlap audit
passed — the reader is never switched onto an unaudited index. Because promotion
switches **every** reader of the backend onto the canonical reader, it requires
an explicit confirmation:

```
$ moraine db core-index promote
Canonical Read Indexes: Promote
  Promotion publishes open_v2.ready=1 for this backend.
  Every `open` consumer of this backend must be v2-capable (this build or newer);
  a downlevel reader will fail after promotion.
  Re-run with --force to confirm and publish.

$ moraine db core-index promote --force
Canonical Read Indexes: Promote
  Published open_v2.ready=1 (provenance: operator-promote).
```

Verify every reader process of a shared backend is on a v2-capable build before
promoting. Promotion is idempotent — re-running on an already-promoted backend
reports "no change" and succeeds.

## Reclaiming superseded generations

The three index tables store rows for **every** source generation ever
ingested — the maintaining MVs are publication-blind by design, and gating
happens at read time via the pinned published-head join. A replacement replay
of a source therefore leaves the old generation's rows in place forever,
growing the indexes without bound in the number of replays.

Issue #603's `read_index_generation` reclaim scope bounds that. It collects
rows whose `(source_host, source_name, source_file, source_generation)`:

1. appears in publication **history** (it was once published — a durable
   liveness decision exists), and
2. is no longer the file's **current head** (a later publication displaced
   it), and
3. was displaced longer ago than `retention.derived_horizon_hours` (24 h by
   default).

It runs from `moraine db reclaim run --scope read_index_generation --confirm`,
and from the unattended maintenance tick when
`retention.storage_reclaim_maintenance = true` (see
[configuration](../configuration.md#retention)).

**Why this is invisible to readers.** Every reader of these tables filters
every row through the head set reconstructed at its request's pinned
publication revision, and that pin is captured from current state at request
start and lives for one request. A generation displaced from the head more
than a horizon ago is selectable by no in-flight or future request — which is
also why a reader can never observe a *partially* reclaimed generation: the
three per-table deletes of one unit are not atomic, but every row they remove
is already invisible at every reachable pin.

**Rows for generations that were never published are not collected.** The MVs
write index rows when the `events` insert lands, *before* the publication
head is written, so "in the indexes but not in publication history" is
exactly what a publication in flight looks like — and also what a crashed
pre-#602 ingest left behind. The scope refuses to touch either; a
`core-index rebuild` removes the crashed residue along with everything else.

**The rollback caveat — the one operational hazard to know.** Re-publishing a
generation that was already reclaimed (an operator rollback past the horizon)
makes readers select a generation whose index rows are gone: sessions served
from it will read as absent or truncated *with no error*. The canonical
`events` rows are untouched by this scope, so recovery is always available
and always the same:

```
moraine db core-index rebuild
```

A rollback executed **within** the horizon needs nothing — the horizon exists
precisely so that a freshly displaced generation is never claimed. If you
roll a source's publication back to an earlier generation more than a horizon
after it was displaced, run the rebuild afterwards.

## Rollback

The v1 reader and its projection are retired (issue #603 WI-10), so there is
no kill-switch back to it. What remains:

- **Repair in place (preferred):** every canonical-reader defect is
  recoverable with `moraine db core-index rebuild` — the indexes are
  content-free derivations of canonical `events`, and the rebuild re-sweeps,
  re-audits, and republishes readiness.
- **Binary downgrade:** a pre-retirement binary can be restored ONLY on a
  store where migration 041 has not applied (the projection tables still
  exist); the downlevel projector then reconciles `mcp_open_*` and serves v1
  reads as before. Once 041 has dropped the family there is no in-place
  downgrade path — the projection would have to be rebuilt by the downlevel
  binary's own backfill against canonical `events`, which is a full
  re-projection, not a rollback. Take a `moraine export events --format
  jsonl` before any deliberate downgrade experiment, and remove `open_reader`
  from `moraine.toml` first if it is set: binaries older than the #598 PR
  window reject unknown `[mcp]` keys at config load.

## DB-level reset recipe (CLI unavailable)

`moraine db core-index rebuild` performs exactly the statements below. Run them
directly against ClickHouse only when the CLI is unavailable (replace `moraine`
with your database name). This resets readiness and empties the indexes; a
subsequent `moraine up` / `moraine db migrate` re-runs the backfill and
republishes.

```sql
-- Revoke readiness FIRST (ReplacingMergeTree(generation): a fresh snowflake
-- makes each zero win). Statements are not transactional, so this ordering is
-- the crash-safety guarantee: an interruption at any point leaves the state
-- reporting not-ready over intact-or-empty tables, never ready-but-empty.
INSERT INTO moraine.mcp_read_index_state (state_key, ready, generation, cursor)
VALUES ('open_v2',      0, generateSnowflakeID(), ''),
       ('core_audit',   0, generateSnowflakeID(), ''),
       ('core_indexes', 0, generateSnowflakeID(), '');

TRUNCATE TABLE IF EXISTS moraine.mcp_session_directory;
TRUNCATE TABLE IF EXISTS moraine.mcp_event_locator;
TRUNCATE TABLE IF EXISTS moraine.mcp_event_navigation;
```

To publish `open_v2` by hand (the DB-level equivalent of `promote --force`,
after confirming `core_indexes.ready = 1` and a passing audit):

```sql
INSERT INTO moraine.mcp_read_index_state (state_key, ready, generation, cursor)
VALUES ('open_v2', 1, generateSnowflakeID(), 'operator-promote');
```

## Deviation note (monitor surfacing)

The monitor `/api/v1/health` (and `/api/v1/status`) core-index block is not yet
wired. The monitor reads store health exclusively through the
`ConversationRepository` trait (`StoreHealth`, in the `moraine-conversations`
crate); surfacing readiness there requires an additive `StoreHealth` field
populated by `read_store_health_impl` (reusing the exported
`ClickHouseClient::{canonical_read_indexes_ready, open_v2_reader_ready,
read_index_state, core_index_audit_outcome}` accessors), plus the matching JSON
block in the monitor handlers. That change lands with the repository-reader work
and is intentionally out of this change's crate scope.
