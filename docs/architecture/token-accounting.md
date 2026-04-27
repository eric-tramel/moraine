# Harmonized Token Accounting

Moraine stores provider token reports in two layers:

- compatibility scalar columns: `input_tokens`, `output_tokens`, `cache_read_tokens`, and `cache_write_tokens`
- canonical accounting columns: `endpoint_kind`, `token_usage_buckets`, and `token_usage_native_units`

Consumers should prefer the canonical columns. The scalar columns remain for older queries and operational continuity.

## Endpoint Kinds

`endpoint_kind` identifies the billing shape of the call:

| Kind | Meaning |
| --- | --- |
| `generation` | Chat, completion, response, or agent turn generation. |
| `embedding` | Text or multimodal embedding endpoint. |
| `rerank` | Reranking endpoint. |
| `moderation` | Moderation or classification endpoint. |
| `image_generation` | Image generation or image editing endpoint. |
| `audio_generation` | Audio generation or transcription endpoint. |
| `other` | Known usage that does not fit the current vocabulary. |

All current Codex, Claude Code, Hermes, and Kimi CLI rows are tagged `generation`.

## Canonical Buckets

`token_usage_buckets` is a `Map(String, UInt64)`. Every normalized row includes every known key with `0` when the provider did not emit that bucket.

| Bucket | Semantics |
| --- | --- |
| `input_text` | Uncached text input tokens. |
| `output_text` | Text output tokens. |
| `input_cache_read` | Input tokens served from provider cache. |
| `input_cache_write` | Input tokens written to provider cache. |
| `input_image` | Provider-declared input image token equivalent. |
| `output_image` | Provider-declared output image token equivalent. |
| `input_audio` | Provider-declared input audio token equivalent. |
| `output_audio` | Provider-declared output audio token equivalent. |
| `reasoning` | Thinking/internal/reasoning tokens reported separately. |
| `server_tool_use` | Provider-side tool-use units reported in usage payloads. |
| `embedding_input_text` | Text input token count for embedding endpoints. |
| `embedding_input_image` | Image token equivalent for embedding endpoints. |
| `other` | Forward-compatible bucket for recognized but unmapped usage. |

Bucket semantics are additive. Cache buckets are not subsets of `input_text`; they are separate billable input dimensions. For providers such as OpenAI that report `cached_tokens` as a subset of `input_tokens`, Moraine subtracts cache and modality details to derive uncached `input_text`. For providers such as Anthropic and Kimi that report uncached text and cache counters separately, Moraine stores those counters directly.

The total provider-declared token-equivalent spend for a row is the sum of `token_usage_buckets` values. Native units are not part of that sum.

## Native Units

`token_usage_native_units` is a `Map(String, Float64)` for billed dimensions that are not naturally tokens:

| Native unit | Semantics |
| --- | --- |
| `input_image_pixels` | Input image pixels, for providers that bill images by pixel. |
| `output_image_pixels` | Output image pixels. |
| `input_audio_seconds` | Input audio duration in seconds. |
| `output_audio_seconds` | Output audio duration in seconds. |
| `input_images` | Count of input images. |
| `output_images` | Count of output images. |

When a provider also declares a token equivalent, store that value in `token_usage_buckets` and the native unit here. Voyage-style multimodal embedding usage, for example, can carry `embedding_input_text`, `embedding_input_image`, and `input_image_pixels` together.

## Backfill And Compatibility

Migration `014_harmonized_token_accounting.sql` adds the canonical columns and materializes defaults for historical rows from the existing scalar counters. Historical rows keep the raw provider payload in `token_usage_json`; if a source preserved richer modality details there, operators can run a provider-specific mutation later without re-ingesting raw trace files.

Monitor analytics, MCP session-event responses, and the search document projection all carry `endpoint_kind` and the canonical maps so downstream tools can separate modality, cache status, endpoint kind, and model without parsing `token_usage_json`.
