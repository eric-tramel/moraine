# Canonical read indexes and the `open` reader

Moraine is migrating the `open` tool family (`open(session|turn|event)`) off the
full-content `mcp_open_*` projection and onto a page-aware reader backed by
content-free **canonical read indexes** (issue #598). This page is the operator
reference for that reader: how to select it, how to inspect its readiness, and
how to repair or roll it back.

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

Truncates the three index tables, resets the `core_indexes` / `core_audit` /
`open_v2` readiness rows to `ready = 0`, then re-runs the backfill from scratch
(the same engine `moraine db migrate` uses, with one Migration-class query
envelope per page). Use it when the indexes are suspected stale or corrupt, or
after a schema change that invalidated them.

Rebuild is safe while serving traffic **on the default Local backend**: readers
resolving `auto` fall back to v1 while `open_v2.ready` is 0, and flip back to v2
only after the rebuild republishes readiness. On a Shared backend, set
`open_reader = "v1"` on all reader processes first (they will not otherwise
demote once they have cached v2), run the rebuild, then re-`promote`.

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

## DB-level reset recipe (CLI unavailable)

`moraine db core-index rebuild` performs exactly the statements below. Run them
directly against ClickHouse only when the CLI is unavailable (replace `moraine`
with your database name). This resets readiness and empties the indexes; a
subsequent `moraine up` / `moraine db migrate` re-runs the backfill and
republishes.

```sql
TRUNCATE TABLE IF EXISTS moraine.mcp_session_directory;
TRUNCATE TABLE IF EXISTS moraine.mcp_event_locator;
TRUNCATE TABLE IF EXISTS moraine.mcp_event_navigation;

-- ReplacingMergeTree(generation): a fresh snowflake makes each zero win.
INSERT INTO moraine.mcp_read_index_state (state_key, ready, generation, cursor)
VALUES ('core_indexes', 0, generateSnowflakeID(), ''),
       ('core_audit',   0, generateSnowflakeID(), ''),
       ('open_v2',      0, generateSnowflakeID(), '');
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
