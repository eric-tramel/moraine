# Canonical read indexes and the `open` reader

Moraine is migrating the `open` tool family (`open(session|turn|event)`) off the
full-content `mcp_open_*` projection and onto a page-aware reader backed by
content-free **canonical read indexes** (issue #598). This page is the operator
reference for that reader: how to select it, how to inspect its readiness, and
how to repair or roll it back.

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

- **`open(session|turn|event)` is cut over.** When `open_reader` resolves to v2
  (published and Local, or forced), the `open` tool family reads canonical rows
  through the page-aware reader. This is a one-way flip per process.
- **Session discovery is cut over** (issue #599). MCP `list_sessions` and the
  monitor `/api/v1/sessions` feed are now one shared repository operation that
  selects candidates from `mcp_session_directory` and hydrates only the
  candidate page from `mcp_event_navigation` plus the metadata-bearing `events`
  rows. It reads no `mcp_open_*` relation and no `v_session_summary` /
  `v_conversation_trace` / `v_turn_summary` view.
  - **Readiness gate.** The shared operation selects the directory path only
    when the `open_v2` key reads ready — the same key `open` consults — and
    otherwise serves the pre-#599 `mcp_open_publication_headers` page. The
    negative is never cached, so the flip needs no restart; the positive is
    latched per process.
  - **Continuation tokens are path-tagged.** A cursor minted by one path is
    refused by the other with a cursor mismatch rather than silently resuming,
    because the two paths anchor on values that a readiness flip can move
    apart. A client that sees a mismatch restarts its feed from page 1.
  - **`mcp_open_publication_headers` is still written and read** by that
    fallback, so a store whose backfill has not published readiness keeps
    working unchanged. The fallback is deliberately short-lived; it is removed
    once the #599 live gates are green in CI.
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
- **Search still reads v1** (issue #597). Search ranking and result hydration
  continue to use the projected read model.
- **The v1 projector, its dirty-session materialized view, the janitor, and the
  publication bridge keep running** as the compatibility reconciler. They keep
  `mcp_open_*` current for the consumers still on v1 — and that is exactly what
  makes a binary downgrade a safe full rollback for this whole change (see
  [Rollback](#rollback)).

Only after search (#597) has cut over, and after the #599 readiness-gated
fallback has been removed, does a later, separate retirement change stop the
projector and dirty writes and drop the compatibility tables. Until then, expect
steady-state reads and writes against `mcp_open_*` from search, from the
projector itself, and from any backend that has not published readiness; they
are not a sign the canonical reader is inactive.

The cutover is validated by three live gates —
`scripts/dev/sandbox/run-live-test list-parity`, `… list-query-log`, and
`… list-bench`. See [Testing and benchmarking](../development/testing.md).

## Append-to-visible latency contract

For an active file-backed session on the reference host, a committed append
becomes visible through the canonical `open` reader within **2 seconds at p95**,
measured from ingest acknowledgement (the durable `events` insert) to the first
valid `open` that reflects the appended event. This is the realtime contract the
canonical reader must hold; the projector rebuild it replaces could stall this
path under load.

The gate is validated by the pre-wired live-test modes `append-to-visible`
(`insert-ack → first-valid-open` p95 ≤ 2 s) and `fsync-to-open-valid`
(end-to-end `fsync → first-valid-open`, reported alongside). Structured
retry/reopen responses during an insert-only append fence count as
not-yet-visible samples, never failures. Polled SQLite/NAC/Cursor sources retain
their documented polling latency until their delta-scan follow-up lands. See
[Testing and benchmarking](../development/testing.md) for how to run these
modes.

## Selecting the reader: `[mcp] open_reader`

```toml
[mcp]
# "auto" (default) | "v1" | "v2"
open_reader = "auto"
```

The value is validated at config load; an unknown value is rejected with a
friendly error. The selector resolves as follows (a config override always beats
the process-cached readiness at process start):

- **`auto`** (default): use the canonical v2 reader **iff** `open_v2.ready == 1`
  **and** the backend is the default single-owner Local backend; otherwise stay
  on the legacy v1 projected reader. Once v2 is selected in a process it stays
  selected (monotonic; no mid-run demotion).
- **`v1`**: force the legacy v1 reader regardless of published readiness. This is
  the **non-silent kill-switch** — `moraine status` and `moraine db doctor`
  display that a config override is in effect. It takes effect at process start
  and is the immediate operational escape hatch.
- **`v2`**: force the canonical v2 reader (for testing, or a promoted Shared
  backend). When the indexes are not ready the reader fails with a typed error
  rather than silently falling back to v1.

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
  Database panel, plus a prominent note whenever a config override or a
  forced-v2-not-ready misconfiguration is in effect.

Core-index readiness is a normal transient state (like publication replaying);
it does **not** fail the doctor exit code.

> **Status/doctor read live state; serving processes do not.** Each backend or
> MCP process samples `open_v2` readiness once at construction and keeps that
> answer for its lifetime. After an in-place `moraine db migrate` publishes
> readiness under a running backend daemon, `status`/`doctor` report the v2
> reader as active while the daemon keeps dispatching v1 until it is restarted
> (`moraine down && moraine up`). This divergence is safe in direction (v1
> serves correctly) but means doctor output describes the state a *newly
> started* process would adopt, not necessarily what a long-running daemon is
> doing right now.
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

Processes started while `open_v2.ready` is 0 resolve `auto` to v1 and adopt v2
at their next start after the rebuild republishes readiness. The `rebuild`
command prints this warning before touching anything.

On a Shared backend, additionally set `open_reader = "v1"` on all reader
processes first (a restart alone re-probes readiness, and mid-rebuild that
yields v1 anyway — the explicit kill-switch makes it deterministic), run the
rebuild, then re-`promote`.

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

## Rollback

- **Kill-switch (fastest):** set `open_reader = "v1"` and restart the reader
  process(es). This is a non-silent override surfaced in status/doctor and takes
  effect immediately at process start, ahead of any cached readiness.
- **Binary downgrade (full rollback):** downgrading the Moraine binary is a
  supported rollback for the entire #598 PR window. The v1 projector, dirty
  materialized view, janitor, and publication bridge keep running as a
  compatibility reconciler, so `mcp_open_*` stays current and a downlevel binary
  reads it exactly as before. (This holds until the separate step-6 retirement
  PR removes the v1 writers.)

  **Remove `open_reader` from `moraine.toml` before downgrading.** Downlevel
  binaries reject unknown `[mcp]` keys at config load, so a config still
  carrying `open_reader` (for example after applying the kill-switch above)
  makes every service — `up`, the backend daemon, ingest, MCP — refuse to
  start on the old binary. Delete the line, then downgrade.

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
