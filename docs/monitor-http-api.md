# Monitor HTTP API v1

Moraine exposes a read-only HTTP API for the monitor dashboard and other local
monitor clients. The canonical base path is `/api/v1`. API versioning applies to
the HTTP surface only; it does not change the MCP transport or MCP tool schemas.

The examples below use relative paths because the bundled dashboard calls the
API on the same origin that served the dashboard.

## Route Matrix

All canonical routes are `GET` routes and successful responses use JSON.

| Route | Query | Purpose |
| --- | --- | --- |
| `/api/v1/capabilities` | none | Describe this server build, its observed schema migration level, and available HTTP feature groups. |
| `/api/v1/health` | none | Probe ClickHouse health and report compact publication-readiness, query-budget, and ingest-heartbeat summaries. |
| `/api/v1/status` | none | Return the diagnostic ClickHouse, publication, database, table, connection, and ingestor snapshot used by the status dashboard, plus `known_harnesses`. |
| `/api/v1/analytics` | `range` | Return token, turn, and concurrent-session time series for a supported window. |
| `/api/v1/tables` | none | List tables with engine, temporary-table marker, and estimated row count. |
| `/api/v1/tables/:table` | `limit` | Return the named table's schema and a bounded row preview. `:table` is a path parameter. |
| `/api/v1/web-searches` | `limit` | Return a bounded list of normalized web-search activity. |
| `/api/v1/sessions` | `since`, `limit`, `cursor`, `harness`, `source`, `mode`, `sort` | Return one keyset page of session **summaries**. Never transcripts. |
| `/api/v1/sessions/:id/page` | `limit`, `cursor` | Return one bounded page of a session's turns. `:id` is a path parameter. |

The session collection carries summaries only. It has no `turns` key and no
message content, so its response size does not grow with transcript size. A
client that needs a session's turns loads them from
`/api/v1/sessions/:id/page`, one page at a time. Both routes read the same
repository operations the MCP `list_sessions` and `open` tools use, so the two
surfaces report the same session facts.

`/api/v1/sessions/:id` itself is still not a route: the detail path is
`/api/v1/sessions/:id/page`.

`/api/v1/status` carries `known_harnesses`: the vocabulary the session feed's
`harness` filter accepts. It is served because `harness` narrows a keyset-paged
feed server-side, so a client that derived the menu from the sessions it happens
to have loaded could never select a harness absent from the loaded pages.

`/api/v1/sessions/:id/page` requires this backend to have published its
canonical read indexes — the same readiness the MCP `open` tool and the session
feed's own read path gate on. Until then the route returns `503` with
`code: "canonical_reader_unavailable"` rather than reading indexes whose
backfill or overlap audit has not completed. Session summaries stay available
throughout; only the transcript detail is withheld.

Successful analytics, web-search, and session responses carry
`"read_model":"live"`: their rows are authorized against published source
generations. Table listing and bounded table preview carry
`"read_model":"audit"` because they intentionally inspect physical relations,
including retained inactive generations. Table preview is not a current-data
API.

## Capabilities Contract

`GET /api/v1/capabilities` returns HTTP `200` with this contract:

```json
{
  "ok": true,
  "server_version": "0.0.0",
  "schema_migration_level": "017",
  "features": {
    "analytics": true,
    "sessions": true,
    "table_inspection": true,
    "web_searches": true
  }
}
```

The example version values are illustrative. Field semantics are:

- `ok` is always `true` for a successfully generated capabilities document.
- `server_version` is the version of the running Moraine server build. It is
  not the ClickHouse version.
- `schema_migration_level` is the latest applied Moraine migration identifier
  the server can observe. Migration identifiers are opaque strings; clients
  must not parse them as integers. It is `null` when no applied migration level
  is available, including when the database or migration ledger is absent or
  cannot be read. That `null` is capability metadata, not by itself an HTTP
  error.
- `features.analytics` means the analytics collection route is implemented.
- `features.sessions` means the session collection is implemented. It does not
  separately advertise the session page route; a build that serves the
  collection serves the page route too.
- `features.table_inspection` means table listing and bounded table preview are
  implemented.
- `features.web_searches` means the normalized web-search collection is
  implemented.

Feature values advertise route support, not backend health or the presence of
rows. A feature can be `true` while a particular read returns an empty result
or a backend error.

The `features` object is additive. Clients must test the keys they understand
and ignore unrecognized feature keys. Adding another feature key does not, on
its own, require a new API version. The top-level fields and the four feature
keys shown above are the required v1 contract.

## Query Parameters and Bounds

Limits are parsed as unsigned decimal integers and then clamped. A value below
the lower bound, including `0`, becomes the lower bound; a value above the
upper bound becomes the upper bound. A value that cannot be parsed as an
unsigned integer is a malformed query and returns HTTP `400`.

| Route | Parameter | Default | Accepted or effective values |
| --- | --- | --- | --- |
| `/api/v1/analytics` | `range` | `24h` | `15m`, `1h`, `6h`, `24h`, `7d`, or `30d` |
| `/api/v1/sessions` | `since` | `30d` | `1h`, `6h`, `24h`, `7d`, `30d`, `90d`, or `all` |
| `/api/v1/sessions` | `limit` | `50` | clamped to `1..=200`, then to the backend's configured `[mcp] max_results` |
| `/api/v1/sessions` | `sort` | `desc` | `desc` or `asc` |
| `/api/v1/sessions` | `mode` | none | `chat`, `tool_calling`, `mcp_internal`, or `web_search` |
| `/api/v1/sessions` | `harness`, `source` | none | any value; trimmed, and blank-after-trim means "no filter" |
| `/api/v1/sessions` | `cursor` | none | an opaque `next_cursor` from a previous page |
| `/api/v1/sessions/:id/page` | `limit` | `50` | clamped to `1..=200` |
| `/api/v1/sessions/:id/page` | `cursor` | none | an opaque `next_cursor` from a previous page of the **same** session |
| `/api/v1/web-searches` | `limit` | `100` | clamped to `1..=1000` |
| `/api/v1/tables/:table` | `limit` | `25` | clamped to `1..=500` |

An unknown `range` does not produce an error; it resolves to `24h`. An unknown
`since` similarly resolves to `30d`. Responses report or embody the resolved
window, so clients should treat the supported values above as the request
contract rather than relying on fallback behavior.

`/api/v1/sessions` reports the window it resolved as
`"window": {"start": <integer>, "end": <integer>}`. Both values are milliseconds
since the Unix epoch, `start` inclusive and `end` exclusive; `since=all` reports
`start: 0`. The window is the time filter the page was actually computed under,
which on a continuation is not the one the current clock would produce — see
Session Paging.

`sort` and `mode` do **not** fall back. An unrecognized value returns HTTP
`400`, because silently dropping a narrowing filter would answer a narrow
question with a wider result set. A `cursor` that is present but blank is also
`400` with `code: "invalid_cursor"`.

The session `limit` is clamped twice. The route's own bound is `1..=200`; the
backend then applies its configured `[mcp] max_results`. The response's `limit`
field reports the second, effective value — the number of sessions the page was
actually allowed to contain.

### Session Paging

`/api/v1/sessions` returns one keyset page. `next_cursor` is an opaque token
and `has_more` is exactly `next_cursor != null`. There is deliberately no total
count: computing one is corpus-sized work that this route exists to avoid.

Clients must treat `sessions: []` with `has_more: true` as "keep paging", not
as "no results". It is a legal response: the server bounds how many candidate
sessions it hydrates per request, and a selective filter can eliminate all of
them while more remain.

**The cursor pins the window.** `since` names a lookback from *now*, so the
window a page is computed under moves between requests. A `next_cursor` carries
the window its page was computed under and the continuation replays it verbatim,
which is what makes a freshly minted cursor redeemable at all. Consequences a
client must plan for:

- A continuation reports the same `window` as the page that minted it, not a
  window recomputed from the current clock. Sessions whose activity falls after
  `window.end` appear only after the client restarts from page 1.
- `since` must stay the same across a paging run. Presenting a cursor under a
  different `since` returns HTTP `400` with `code: "invalid_cursor"` rather than
  quietly serving the pinned window; changing the lookback means restarting the
  feed from page 1.
- The remaining filters (`harness`, `source`, `mode`, `sort`) must also stay the
  same. The backend refuses a cursor presented under different ones, with the
  same `400` / `invalid_cursor` pair.
- The pinned window is validated, not trusted. It must be a valid half-open
  range and must be the exact window the token's own `since` names, so an edited
  token cannot widen the query beyond the lookback it still claims. A window
  that fails either check is `400` with `code: "invalid_cursor"`, the same as
  any other refused token — never a `503`.

The cursor is a value anchor, not a snapshot. A session that receives events
while a client pages moves ahead of the anchor and will not be seen again until
the client restarts from page 1; unrelated activity does not invalidate the
cursor. Every cursor the server refuses — malformed, oversized, carrying an
edited window, minted under different filter inputs, or minted by a different
server read path — returns HTTP `400` with `code: "invalid_cursor"`. That is
recoverable by restarting the feed, and it is reported as an error rather than
as a short page, so a stale cursor can never present as a gap in the results.

`/api/v1/sessions/:id/page` pages a single session's turns the same way, with
its own `next_cursor` / `has_more`. Its token is bound to the session it was
minted for and is rejected with `400` and `code: "invalid_cursor"` on any other
session. Both routes' tokens are at most 4096 characters: they encode a keyset
anchor and fixed-shape validation state, and any optional carried state that
would exceed the bound is dropped before the token is minted. Token size
therefore does not grow with session, transcript, or page size. The bound is
enforced when minting as well as when parsing, so a token a client receives is
always one the server will accept back; a token longer than the bound is refused
without being parsed. The session page route can also
answer `{"ok": true, "read_model": "live", "reopen": true}` with no `session`
key: the pinned view of that session changed underneath the cursor (a source
replay flipped its generations) and the client must reload it from page 1.

The table path parameter must match the strict ASCII identifier pattern
`[A-Za-z_][A-Za-z0-9_]*`. A rejected identifier returns HTTP `400`. A
syntactically valid name that cannot be read from the configured database is a
backend read failure, not a `404` resource response.

## Successful Empty and Nullable Values

Collection routes use empty arrays when a successful query has no matching
rows. They do not use `null` to mean an empty collection.

`null` marks unavailable or unobserved optional diagnostics:

- `capabilities.schema_migration_level` is `null` under the conditions described
  in the capabilities contract.
- `/api/v1/status` can return HTTP `200` and `ok: true` while
  `clickhouse.healthy` is `false`. In that diagnostic response, unavailable
  `clickhouse.version` or `clickhouse.ping_ms` values are `null` and
  `clickhouse.error` explains the probe result. If the database does not exist,
  its table list is empty.
- Health and status connection diagnostics use `connections.total: null` with
  a sibling `connections.error` when connection metrics are unavailable. A
  connection-metrics error alone does not make an otherwise successful health
  probe fail.
- Health and status include a `publication` object. `available` distinguishes a
  successful readiness query from a missing/incompatible control schema;
  `healthy` is false when ownership is ambiguous, a generation or append
  preparation is blocked, or writers conflict. Counts for replaying
  generations, transient append preparations, and mirror catch-up are progress
  facts and do not by themselves make publication unhealthy. `issues` contains
  operator-facing detail. Publication degradation
  does not silently expose candidate generations: live reads remain authorized
  by the prior published heads. A publication-probe failure is reported inside
  an otherwise live HTTP `200` health response; `moraine db doctor` treats a
  missing or unhealthy publication diagnosis as needing attention.
- Health and status include an additive `query_budgets` object with
  process-lifetime counters from the daemon's mandatory query envelopes:
  `requests`, `statements`, `deadline_exceeded`, `resource_exhausted`, and
  `unenveloped_statements`. The counts cover both request boundaries the
  backend daemon hosts in one process — MCP tool calls and monitor HTTP
  requests — and reset when the backend restarts. Nonzero `deadline_exceeded`
  or `resource_exhausted` totals mean requests were refused or killed against
  the configured `[query_budgets]`; `moraine status` prints the same counters
  as a one-line status note when they are nonzero. `unenveloped_statements`
  is always `0`: the transport refuses statements issued outside a query
  envelope, so a nonzero value indicates a regression in the fail-closed
  transport. Health failure responses
  keep the block, since budget exhaustion is exactly when it matters.
- Ingest establishes its durable publication host identity before connecting
  writers. Creation with named backends emits a migration warning explaining
  that legacy `HOSTNAME`/`USER` keys are not adopted. An unreadable, corrupt,
  or insecure `publication-host-id` file prevents ingest startup rather than
  falling back to an environment-derived identity. Nonempty legacy publisher
  keys remain separate and may require deliberate backend cleanup; hostless
  ambiguous shared rows appear in the `publication` diagnosis above.
- When no ingest heartbeat is available, `ingestor.present` and
  `ingestor.alive` are `false`, while `ingestor.latest` and
  `ingestor.age_seconds` are `null`.

Clients must distinguish a diagnostic value inside an HTTP `200` response from
an endpoint failure represented by a non-2xx status.

## Errors and Status Codes

API handlers use JSON errors with at least this envelope:

```json
{
  "ok": false,
  "error": "human-readable message"
}
```

An endpoint can add diagnostic fields to that minimum envelope. For example, a
health failure also reports its configured database information and connection
diagnostics. Error message text is for operators and can include backend
details; clients should branch on the HTTP status and `code`, not match
arbitrary message text.

A failure that the server can classify also carries an additive machine-readable
`code`: `deadline_exceeded`, `resource_exhausted`, `invalid_cursor`, or
`canonical_reader_unavailable`. The field is absent when the failure has no
classification, so clients must tolerate its absence.

| Status | Meaning |
| --- | --- |
| `200 OK` | The request completed. Inspect diagnostic fields such as `clickhouse.healthy`; `200` does not mean every component is healthy. |
| `400 Bad Request` | The query could not be deserialized, a table identifier was rejected, a filter value was not recognized, or a continuation cursor was refused. Every refused cursor carries `code: "invalid_cursor"`, whether the route or the backend refused it. A rejected table identifier uses `{"ok":false,"error":"invalid table name"}`. Framework-generated malformed-query responses are not guaranteed to use the application JSON envelope. |
| `403 Forbidden` | Static-file path traversal or a path that resolves outside the configured static root. The JSON error is `{"ok":false,"error":"forbidden"}`. |
| `404 Not Found` | No requested static file exists, or a requested session is unknown or outside this backend's configured scope. The JSON error is `{"ok":false,"error":"not found"}` or `{"ok":false,"error":"session not found"}`. |
| `405 Method Not Allowed` | The path exists but does not support the requested HTTP method. A JSON error envelope is not guaranteed. |
| `429 Too Many Requests` | Concurrent-read admission overflow, or a repository read that exhausted a non-time query budget. Carries `code: "resource_exhausted"`. |
| `500 Internal Server Error` | The static root cannot be resolved or a selected static file cannot be read. |
| `503 Service Unavailable` | A required repository or ClickHouse read failed with no budget classification, or `/api/v1/sessions/:id/page` was called on a backend that has not published its canonical read indexes — the latter carries `code: "canonical_reader_unavailable"`. Handler-generated responses use the JSON error envelope. |
| `504 Gateway Timeout` | A repository read exceeded its query-budget deadline. Carries `code: "deadline_exceeded"`. |

`/api/v1/health` returns `503` when the required store-health read, ping, or
version probe fails. Most collection read failures also return `503`.
`/api/v1/status` is deliberately diagnostic: it can describe a missing or
unhealthy database with `200`; it returns `503` when a required table-summary
read fails after the database was observed to exist.

## Removed Legacy Aliases

The pre-`/api/v1` dashboard routes (`/api/health`, `/api/status`,
`/api/analytics`, `/api/tables`, `/api/tables/:table`, `/api/web-searches`,
`/api/sessions`) were direct aliases for a one-release compatibility window.
That window opened when `/api/v1` shipped in `v0.7.0` and closed with `v0.7.1`;
the aliases are removed.

An unversioned `/api/*` path is now an unknown path. It falls through to static
handling and returns the JSON `404` described below. `/api/v1` is the only API
base path.

## Static Assets

API routes take precedence over static-file handling. Every other `GET` request
is resolved under the configured monitor static directory:

- `/` serves `index.html`.
- A path naming a directory serves that directory's `index.html`.
- A path naming a file serves that file with a MIME type inferred from its
  extension.
- Missing paths return the JSON `404` described above. There is no implicit
  single-page-app rewrite of an unknown path back to the root `index.html`.
- Paths containing `..`, and paths whose canonical location escapes the static
  root, return the JSON `403` described above.

The server validates at startup that the selected static path is a directory
and contains `index.html`. `--static-dir` selects custom built assets; without
an explicit override, packaged assets or the source-tree `web/monitor/dist`
build are used according to the server's static-directory resolution rules.

Because unmatched paths enter static-file handling, an unknown path under
`/api/v1` is not evidence that an API resource exists. Clients must use the
routes in the matrix above rather than inferring paths from them; in particular
`/api/v1/sessions/:id` is not a route, while `/api/v1/sessions/:id/page` is.

## MCP Protocol Is Unchanged

The versioned HTTP API does not replace or tunnel MCP. Agent harnesses still
launch `moraine run mcp` over stdio, and its existing local central-server proxy
continues to use the configured Unix socket and existing socket protocol. MCP
tool names, request/response schemas, and fallback behavior are unchanged by
this HTTP API version.

There are no MCP tool endpoints under `/api/v1`, and HTTP clients cannot select
a named backend through this API. This API version also does not introduce HTTP
authentication or a remote-deployment contract.
