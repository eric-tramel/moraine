# Ingest Sources

Moraine normalizes each supported trace wire format through a source adapter in
`crates/moraine-ingest-core/src/sources/`. The public ingest entry point remains
`normalize::normalize_record`; source adapters own the per-harness behavior that
used to live behind the hardcoded dispatcher in `normalize.rs`.

## Source Contract

Each adapter implements `IngestSource` from `sources/mod.rs`:

- `harness()` returns the stable value written to the ClickHouse `harness`
  column.
- `default_inference_provider()` returns the static provider when the source has
  one. Sources such as Hermes that encode the provider per record return `None`
  and fill it in from `source_metadata()`.
- `preflight()` skips or rewrites non-event records before generic raw/error row
  handling. Kimi CLI uses this to skip its metadata header.
- `record_ts()` extracts the timestamp string consumed by the generic timestamp
  parser. Kimi CLI converts Unix-seconds wire timestamps here.
- `top_type()` returns the source-level event discriminator used inside the
  adapter.
- `session_id()` resolves the stable session id from the record, source file,
  prior session hint, top type, and base raw UID.
- `normalize()` emits event, link, and tool rows. The generic wrapper writes raw
  rows, timestamp error rows, context fields, and model hints.

Adapters return `NormalizedPartials`; the framework handles the rest of the
`NormalizedRecord`.

## Shared Helpers

Use `sources/shared.rs` for common row construction and invariants:

- scalar conversion helpers such as `to_str`, `to_u32`, and `to_u8_bool`
- timestamp helpers and session id inference
- canonical event, payload, and link type enforcement
- `RecordContext`, `base_event_obj`, `build_link_row`, and `build_tool_row`
- token accounting helpers such as `TokenBuckets`, `generation_token_buckets`,
  and `openai_generation_token_buckets`

Source modules should prefer these helpers over duplicating row-building logic.
Rows emitted from an adapter should already be in the shared event/link/tool
domain before returning to `normalize_record`.

## Adding A Source

1. Add `crates/moraine-ingest-core/src/sources/<name>.rs`.
2. Implement `IngestSource` for a zero-sized adapter type.
3. Register the adapter in `sources::registry()`.
4. Add the harness name to `moraine_config::KNOWN_INGEST_HARNESSES`.
5. Add fixtures under `fixtures/<name>/` and tests under
   `crates/moraine-ingest-core/tests/`.
6. Document any source-specific config examples in `docs/configuration.md`.

The config crate is intentionally lower-level than ingest-core, so it cannot
call the ingest registry without a dependency cycle. The ingest-core registry
test checks that its registered harnesses match the config validation list.

## Fixture Expectations

New fixture tests should call `normalize_record` through the public wrapper, not
adapter internals. Assert both normalized rows and the generic rows that the
framework owns:

- `raw_row.harness`, `raw_row.inference_provider`, `raw_row.top_type`, and
  `raw_row.session_id`
- event row `event_kind`, `payload_type`, `actor_kind`, `model`, timestamp, and
  token fields
- link row `linked_event_uid` versus `linked_external_id`
- tool row request/response phases, tool ids, previews, and error flags
- `session_hint` and `model_hint` continuity across sequential records

For token usage records, assert both legacy aggregate columns and
`token_usage_buckets`. Bucket totals should not exceed their aggregate input or
output token columns unless the upstream wire format explicitly reports a native
unit outside token accounting.
