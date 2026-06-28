# Analytics Export

Moraine can stream normalized event rows from the default ClickHouse backend for
offline trace mining:

```bash
moraine export events \
  --format jsonl \
  --since 2026-06-01T00:00:00Z \
  --until 2026-06-15T00:00:00Z \
  --harness codex \
  --project-id agent-stuff \
  --columns session_id,event_uid,event_ts,turn_seq,event_kind,payload_type,actor_kind,tool_name,tool_phase,tool_error,text_content,payload_json \
  --include-sensitive
```

V1 exports events only. Sessions, turns, tool I/O, raw events, CSV, named
backend selection, MCP batch open, HTTP export, and monitor UI export are not in
scope.

## Row Format

`moraine export events` requires `--format jsonl`. Stdout contains JSONL data
rows only: one JSON object per line, UTF-8 encoded. Completion metadata is never
written to stdout, so output can be piped into tools such as `jq`, `head`, or
offline mining jobs.

The global `--output` flag is for normal command rendering. Export commands
reject explicit `--output plain`, `--output rich`, or `--output json`; use
`--format jsonl` for export rows.

## Schema

Print the static, config-free public schema with:

```bash
moraine schema analytics --json
```

The v1 data schema is `moraine.analytics.events.v1`. Default event columns are:

```text
session_id,event_uid,event_ts,turn_seq,event_order,harness,source_name,event_kind,payload_type,actor_kind,tool_name,tool_phase,tool_error,model,project_id
```

`--columns all` means all non-sensitive public v1 columns. It is not physical
`SELECT *`.

## Sensitive Columns

The default export omits sensitive previews, full text, payloads, token usage
JSON, source references, and path-like fields. These columns require
`--include-sensitive` when named in `--columns`:

```text
source_file,source_ref,repo_rel_path,worktree_root,cwd,text_preview,text_content,payload_json,token_usage_json
```

Metadata is out of band, but it can still include the filter values supplied on
the command line, including path filters. Treat stderr metadata as
trace-adjacent data.

## Filters

Without `--all`, at least one filter is required. String filters are exact and
case-sensitive. Repeat a flag to OR values within that field; different fields
are ANDed together. Comma-separated value lists are rejected in v1.

Supported filters:

```text
--since <rfc3339>
--until <rfc3339>
--session-id <id>
--harness <name>
--source-name <name>
--project-id <id>
--cwd-prefix <path>
--worktree-root <path>
--repo-rel-path <path>
--event-kind <kind>
--payload-type <type>
--actor-kind <kind>
--model-name <name>
--tool-name <name>
--tool-error-only
```

`--since` is inclusive and `--until` is exclusive. Both use Moraine's canonical
event time: `record_ts` parsed as RFC3339 when possible, otherwise
`ingested_at`. Public `event_ts` values are UTC RFC3339 strings with exactly
millisecond precision, for example `2026-06-01T12:34:56.789Z`.

## Metadata

Completion metadata is one JSON object written to stderr:

```json
{"schema_version":"moraine.analytics.export_metadata.v1","data_schema_version":"moraine.analytics.events.v1","export_kind":"events","backend":"default","query_id":"...","columns":["session_id","event_uid"],"filters":{"harness":["codex"]},"limit":1000,"row_count":1000,"truncated":true,"elapsed_ms":1234,"sensitive_columns_requested":[]}
```

Stream, preflight, and query failures exit nonzero and do not emit completion
metadata. If stdout is closed by a downstream consumer, such as `head`, Moraine
stops streaming, suppresses completion metadata, and exits successfully.

## Limits And Schema Checks

`--limit <n>` writes at most `n` rows. Moraine asks ClickHouse for `n + 1` rows
and drops the sentinel row, so `truncated` metadata is exact.

Exports run against the default `[clickhouse]` backend only. Before streaming,
Moraine checks the backend migration ledger and refuses to export if the server
schema is behind or ahead of this build. Run `moraine db migrate` for an older
default backend, or upgrade Moraine if the server is newer.
