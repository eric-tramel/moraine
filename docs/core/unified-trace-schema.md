# Unified Trace Schema Mapping

This page maps raw trace fields into the unified `moraine.events` table so you can move directly from source data to canonical columns when querying ClickHouse. Codex, Claude Code, Kimi CLI, and Hermes normalize into the same table, with format-specific notes after the provider comparison table. The table rows below follow the `moraine.events` schema order. [src: sql/001_schema.sql:L23-L77, crates/moraine-ingest-core/src/normalize.rs]

## Field Mapping Table

| Unified field (`moraine.events`) | Codex trace mapping | Claude Code trace mapping |
| --- | --- | --- |
| `ingested_at` | Not from trace. Set by ClickHouse default `now64(3)` at insert time. | Not from trace. Set by ClickHouse default `now64(3)` at insert time. |
| `event_uid` | Derived SHA-256 from source file coordinates plus raw fingerprint and suffix (`raw`, or `compacted:{idx}` for expanded compacted items). | Derived SHA-256 from source file coordinates plus raw fingerprint and suffix (`raw`, or `claude:block:{idx}` for content blocks). |
| `session_id` | `session_meta.payload.id` when present; otherwise session hint or UUID parsed from source filename stem. | `sessionId`; fallback is session hint or UUID parsed from source filename stem. |
| `session_date` | Derived from source path date (`.../sessions/YYYY/MM/DD/...`) else from parsed `timestamp` date. | Derived from source path date (`.../projects/YYYY/MM/DD/...`) else from parsed `timestamp` date. |
| `source_name` | Not from trace. Ingest source config label. | Not from trace. Ingest source config label. |
| `harness` | Not from trace. Ingest source harness (CLI/agent that wrote the trace), set to `codex`. | Not from trace. Ingest source harness (CLI/agent that wrote the trace), set to `claude-code`. |
| `inference_provider` | Not from trace. Populated by the normalizer based on `harness`: `codex` → `openai`. Future cloud-prefixed values like `bedrock/anthropic` or `azure/openai` are supported by the column grammar but not yet emitted by Codex/Claude Code ingestors. | Not from trace. Populated by the normalizer based on `harness`: `claude-code` → `anthropic`. Same grammar extensibility applies. |
| `source_file` | Not from trace. Absolute/relative file path being ingested. | Not from trace. Absolute/relative file path being ingested. |
| `source_inode` | Not from trace. File inode captured by watcher/reconcile loop. | Not from trace. File inode captured by watcher/reconcile loop. |
| `source_generation` | Not from trace. Generation counter for truncation/rotation handling. | Not from trace. Generation counter for truncation/rotation handling. |
| `source_line_no` | Not from trace. Physical JSONL line number in source file. | Not from trace. Physical JSONL line number in source file. |
| `source_offset` | Not from trace. Byte offset in source file. | Not from trace. Byte offset in source file. |
| `source_ref` | Derived as `{source_file}:{source_generation}:{source_line_no}`. | Derived as `{source_file}:{source_generation}:{source_line_no}`. |
| `record_ts` | Top-level `timestamp` string. | Top-level `timestamp` string. |
| `event_ts` | Parsed UTC timestamp from `timestamp` (fallback is `1970-01-01 00:00:00.000` UTC when parse fails; ingest also emits `timestamp_parse_error`). | Parsed UTC timestamp from `timestamp` (fallback is `1970-01-01 00:00:00.000` UTC when parse fails; ingest also emits `timestamp_parse_error`). |
| `event_kind` | Derived from top-level `type` and `payload.type` (for example: `response_item.function_call` -> `tool_call`, `response_item.function_call_output` -> `tool_result`, `response_item.message` -> `message`). | Derived from top-level `type` and `message.content[].type` (for example: `thinking` -> `reasoning`, `tool_use` -> `tool_call`, `tool_result` -> `tool_result`). |
| `actor_kind` | Derived by payload class: user/assistant/tool/system depending on record shape (for example `payload.role` for messages, `tool` for tool outputs). | Derived by top-level role plus block type (`assistant`/`user`, and `tool` for `tool_result` blocks). |
| `payload_type` | Usually `payload.type` for modern envelopes, plus fixed values for legacy/top-level branches (`message`, `function_call`, `turn_context`, etc.). | Usually block `message.content[].type` for assistant/user records; for non-message records uses `data.type` (`progress`) or `subtype` (`system`) or top-level type fallback. |
| `op_kind` | Set for `web_search_call` from `payload.action.type`; otherwise empty. | Set for non-assistant/user records from derived payload type; otherwise empty. |
| `op_status` | From `payload.phase` or `payload.status` depending on payload type (message/tool/event status); otherwise empty. | From top-level `status` for non-assistant/user records; otherwise usually empty. |
| `request_id` | `payload.turn_id` for `turn_context` and `event_msg`; otherwise empty. | Top-level `requestId` for stamped rows. |
| `trace_id` | Not populated (empty string). | Mirrors top-level `requestId`. |
| `turn_index` | `payload.turn_id` coerced to `UInt32` on `turn_context`; otherwise `0`. | Not populated (stays `0`). |
| `item_id` | `payload.id` for `session_meta`, `response_item.message`, `response_item.reasoning`; or `payload.turn_id` for turn/event rows. | Top-level `uuid` (stamped onto generated rows). |
| `tool_call_id` | `payload.call_id` (or top-level `call_id` in legacy Codex records) for tool call/result rows. | `message.content[].id` for `tool_use`; `message.content[].tool_use_id` for `tool_result`. |
| `parent_tool_call_id` | Not populated in Codex event rows (empty string). | Top-level `parentToolUseID` for assistant/user content-block rows. |
| `origin_event_id` | Derived only for expanded compacted children: set to parent compacted event UID. | Top-level `sourceToolAssistantUUID`. |
| `origin_tool_call_id` | Not populated (empty string). | Top-level `sourceToolUseID`. |
| `tool_name` | `payload.name` for function/custom tool calls; constant `web_search` for web-search calls. | `message.content[].name` for `tool_use` blocks. |
| `tool_phase` | Set to `payload.status` for `web_search_call`; otherwise empty. | Top-level `stop_reason` on assistant/user content-block rows. |
| `tool_error` | Not populated in event rows (defaults to `0`). | `message.content[].is_error` converted to `0/1` on `tool_result` blocks. |
| `agent_run_id` | Not populated (empty string). | Top-level `agentId`. |
| `agent_label` | Not populated (empty string). | Top-level `agentName`. |
| `coord_group_id` | Not populated (empty string). | Not populated (empty string). |
| `coord_group_label` | Not populated (empty string). | Top-level `teamName`. |
| `is_substream` | Not populated (defaults to `0`). | Top-level `isSidechain` converted to `0/1`. |
| `model` | Canonicalized from `payload.model` or token-count `rate_limits` fields (`limit_name`, `limit_id`), with model-hint fallback; alias `codex` maps to `gpt-5.3-codex-xhigh`. | Canonicalized from `message.model` and stamped on generated rows. |
| `input_tokens` | `payload.info.last_token_usage.input_tokens` when `payload.type == token_count`; otherwise `0`. | `message.usage.input_tokens`. |
| `output_tokens` | `payload.info.last_token_usage.output_tokens` when `payload.type == token_count`; otherwise `0`. | `message.usage.output_tokens`. |
| `cache_read_tokens` | `payload.info.last_token_usage.cached_input_tokens` (or `cache_read_input_tokens`) for token-count rows; otherwise `0`. | `message.usage.cache_read_input_tokens`. |
| `cache_write_tokens` | `payload.info.last_token_usage.cache_creation_input_tokens` (or `cache_write_input_tokens`) for token-count rows; otherwise `0`. | `message.usage.cache_creation_input_tokens`. |
| `latency_ms` | Not populated (defaults to `0`). | Top-level `durationMs` for non-assistant/user rows. |
| `retry_count` | Not populated (defaults to `0`). | Top-level `retryAttempt` for non-assistant/user rows. |
| `service_tier` | `payload.rate_limits.plan_type` for token-count rows; otherwise empty. | `message.usage.service_tier`. |
| `content_types` | From `payload.content[].type` (message branches) when present; otherwise empty array. | For blocks: explicit single-type arrays (`thinking`, `tool_use`, `tool_result`, or block type). For non-block message rows: extracted from `message.content[].type`. |
| `has_reasoning` | Set to `1` for explicit reasoning branches (`response_item.reasoning`, top-level `reasoning`, and `event_msg` with `agent_reasoning`); otherwise `0`. | Set to `1` for `thinking` blocks; otherwise `0`. |
| `text_content` | Derived via recursive text extraction over relevant payload branches (message content, tool input/output, summaries, etc.), truncated to limit. | Derived via recursive text extraction over content blocks or full record payload, truncated to limit. |
| `text_preview` | Derived from `text_content` and truncated to preview length. | Derived from `text_content` and truncated to preview length. |
| `payload_json` | Compact JSON string of provider payload branch (`payload`, top-level record, or compacted item depending on branch). | Compact JSON string of content block or full record depending on branch. |
| `token_usage_json` | Compact JSON of full `payload` for `event_msg` token-count rows; otherwise empty string. | Not populated (empty string). |
| `event_version` | Not from trace. Generated from current UNIX epoch milliseconds at normalization time. | Not from trace. Generated from current UNIX epoch milliseconds at normalization time. |

Field defaults and provider-specific overrides come from `base_event_obj`, `normalize_codex_event`, `normalize_claude_event`, and `normalize_record`. [src: crates/moraine-ingest-core/src/normalize.rs:L78-L104, crates/moraine-ingest-core/src/normalize.rs:L172-L230, crates/moraine-ingest-core/src/normalize.rs:L259-L379, crates/moraine-ingest-core/src/normalize.rs:L440-L997, crates/moraine-ingest-core/src/normalize.rs:L999-L1320, crates/moraine-ingest-core/src/normalize.rs:L1322-L1415]

## Hermes ShareGPT Mapping

Hermes Agent trajectories are stored as ShareGPT-compatible JSONL where one line contains the full rollout plus metadata such as `timestamp`, `model`, `completed`, `partial`, and `conversations`. Moraine maps that shape into canonical rows with these rules:

- One raw JSONL line becomes one synthetic session. Because the format has no native session ID, Moraine derives `session_id` as `hermes:{raw_event_uid}`.
- The top-level rollout metadata becomes a canonical `session_meta` row. `op_status` reflects `completed`/`partial`.
- Hermes encodes the LLM vendor in the record's `model` field as `vendor/model` (e.g. `anthropic/claude-sonnet-4.6`). The normalizer splits on the first `/`: the left side becomes `inference_provider` on every emitted row, and the right side is stored verbatim as `model`. Values with no slash keep `inference_provider` empty and store the whole string as `model`.
- `conversations[].from == "system"` becomes `event_kind=system`, `actor_kind=system`.
- `conversations[].from == "human"` becomes `event_kind=message`, `actor_kind=user`.
- `conversations[].from == "gpt"` is segmented: plain text becomes `message`, `<think>...</think>` becomes `reasoning` with `payload_type=thinking`, and `<tool_call>...</tool_call>` becomes `tool_call` with `payload_type=tool_use`.
- `conversations[].from == "tool"` is segmented on `<tool_response>...</tool_response>` and normalized into `tool_result` rows plus `tool_io` response rows.
- When Hermes emits only one top-level timestamp for the entire rollout, Moraine preserves event order by assigning microsecond offsets per generated canonical event. This keeps `v_conversation_trace` chronological within the rollout while preserving the original raw JSON in `raw_events`.

## Hermes Live-Session Mapping

Live Hermes CLI / gateway sessions land at `~/.hermes/sessions/session_<ts>_<id>.json` — one file per conversation, rewritten in place via atomic rename on every turn. Sources with `format = "session_json"` dispatch to the live-session processor, which treats the file's `messages[]` array as the source of truth and uses the checkpoint's `last_line_no` cursor to emit only the messages that appeared since the last flush.

- The synthetic session record carries `session_id = "hermes:<content-session_id>"`, where `<content-session_id>` is the agent's own id (e.g. `hermes:20260418_142200_live01`). This id is stable across every save, so all events from the same conversation share one session.
- Event identity pins `source_inode = 0` and `source_generation = 1` regardless of on-disk inode churn from atomic renames. `source_line_no = message_index + 1` and `source_offset = 0`. `event_uid`s stay stable across reads, so re-emitted rows dedupe via the `events` `ReplacingMergeTree(event_version)`.
- The inference provider is pre-prepended to the bare `model` field from `base_url` — `https://api.anthropic.com` → `anthropic`, `https://api.openai.com` → `openai`, etc. — so the Hermes vendor/model split produces the same canonical split used for trajectories.
- The first time a session file is seen, the processor emits one `event_kind=session_meta` row carrying `model`, `platform`, `system_prompt`, and `tools[]`. Subsequent re-reads of the same file do not re-emit the meta row unless the processor's checkpoint has been reset.
- OpenAI chat-completions messages map one-for-one:
  - `role == "user"` → one `event_kind=message` row with `actor_kind=user`.
  - `role == "assistant"` → optional `event_kind=reasoning` (when `reasoning` is non-empty, `payload_type=thinking`, `has_reasoning=1`) followed by optional `event_kind=message` (when `content` is non-empty, `payload_type=agent_message`, `op_status=<finish_reason>`) followed by one `event_kind=tool_call` per entry in `tool_calls[]` (function name from `function.name`, arguments parsed from `function.arguments`, `tool_call_id` from `id`).
  - `role == "tool"` → one `event_kind=tool_result` row correlated to the originating call via `tool_call_id`, plus a matching `tool_io` response row.
  - `role == "system"` → one `event_kind=system` row.
- All rows emitted from one message share `turn_index = message_index + 1`, giving each conversation turn a monotonically increasing sequence for `v_conversation_trace`. Within a single message, sub-events are time-offset by microsecond so the reasoning row sorts before the tool_call row that follows it.

## Kimi CLI Mapping

Kimi CLI uses `~/.kimi/sessions/**/wire.jsonl`. Wire records with `message.type=TurnBegin` or `SteerInput` become `message` / `user_message` rows. `ContentPart` with `type=text` becomes an assistant message, while `type=think` becomes a `reasoning` / `thinking` row. `ToolCall` and `ToolResult` map to `tool_call` and `tool_result` plus `tool_io` rows keyed by the Kimi tool call id. `StatusUpdate.token_usage` maps input, output, and cache token counters. Session ids are derived from the parent session directory and namespaced as `kimi-cli:<session-id>`. The leading `{"type":"metadata","protocol_version":...}` header line is skipped during normalization — it's a per-file protocol marker, not a session event.

The sibling `context.jsonl` file is not ingested: it's a materialized-turn view that Kimi writes from the same wire events, so anything in it is already captured via `wire.jsonl` (except the system prompt, which Moraine does not persist today).
