use super::pi_family::{normalize_pi_family_record, pi_family_source_metadata, PiFamilyPolicy};
use super::shared::{
    build_external_link_row, infer_session_id_from_file, to_str, to_u64, RecordContext,
};
use super::{IngestSource, NormalizedPartials, SourceMetadata, SourceRecordContext};
use serde_json::{json, Value};

pub(crate) static PRIME_AGENT: PrimeAgent = PrimeAgent;

pub(crate) const ROOT_SOURCE_NAME: &str = "prime-agent";
pub(crate) const SUBAGENT_SOURCE_NAME: &str = "prime-agent-subagents";

pub(crate) struct PrimeAgent;

impl IngestSource for PrimeAgent {
    fn harness(&self) -> &'static str {
        "prime-agent"
    }

    fn default_inference_provider(&self) -> Option<&'static str> {
        None
    }

    fn source_metadata(&self, record: &Value) -> SourceMetadata {
        let top_type = to_str(record.get("type"));
        if prime_record_can_delegate(record, &top_type) {
            pi_family_source_metadata(record)
        } else {
            SourceMetadata::new("")
        }
    }

    fn session_id(&self, record: &Value, ctx: &SourceRecordContext<'_>) -> String {
        if ctx.top_type == "session" {
            let session_id = to_str(record.get("id"));
            if !session_id.is_empty() {
                return session_id;
            }
        }

        if ctx.session_hint.is_empty() {
            infer_session_id_from_file(ctx.source_file)
        } else {
            ctx.session_hint.to_string()
        }
    }

    fn jsonl_carries_cwd(&self) -> bool {
        true
    }

    fn cwd(&self, record: &Value) -> String {
        to_str(record.get("cwd"))
    }

    fn normalize(
        &self,
        record: &Value,
        ctx: &RecordContext<'_>,
        top_type: &str,
        base_uid: &str,
        model_hint: &str,
    ) -> NormalizedPartials {
        if !prime_record_can_delegate(record, top_type) {
            return NormalizedPartials::default();
        }

        let mut partials = normalize_pi_family_record(
            record,
            ctx,
            top_type,
            base_uid,
            model_hint,
            PiFamilyPolicy::PRIME,
        );

        if ctx.source_name != SUBAGENT_SOURCE_NAME {
            return partials;
        }

        for event in &mut partials.event_rows {
            if let Some(event) = event.as_object_mut() {
                event.insert("is_substream".to_string(), json!(1));
            }
        }

        if top_type == "session" && to_u64(record.get("rlmDepth")) > 0 {
            let parent_session_id =
                canonical_uuid_from_jsonl_path(&to_str(record.get("parentSession")));
            if let Some(parent_session_id) = parent_session_id {
                if let Some(event_uid) = partials
                    .event_rows
                    .first()
                    .and_then(|event| event.get("event_uid"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
                {
                    partials.push_link(build_external_link_row(
                        ctx,
                        &event_uid,
                        &parent_session_id,
                        "subagent_parent",
                        "{}",
                    ));
                }
            }
        }

        partials
    }
}

pub(crate) fn canonical_uuid_from_jsonl_path(path: &str) -> Option<String> {
    let path = std::path::Path::new(path);
    if path.extension().and_then(|extension| extension.to_str()) != Some("jsonl") {
        return None;
    }
    let stem = path.file_stem()?.to_str()?;
    let session_id = uuid::Uuid::parse_str(stem).ok()?;
    let canonical = session_id.hyphenated().to_string();
    (canonical == stem).then_some(canonical)
}

fn prime_record_can_delegate(record: &Value, top_type: &str) -> bool {
    match top_type {
        "custom_message" => record.get("display").and_then(Value::as_bool) != Some(false),
        "message" => prime_message_is_normalizable(record.get("message")),
        _ => true,
    }
}

fn prime_message_is_normalizable(message: Option<&Value>) -> bool {
    let Some(message) = message else {
        return false;
    };
    match to_str(message.get("role")).as_str() {
        "user" | "toolResult" => {
            content_blocks_are_known(message.get("content"), &["text", "image"])
        }
        "assistant" => {
            content_blocks_are_known(message.get("content"), &["text", "thinking", "toolCall"])
        }
        "bashExecution" | "branchSummary" | "compactionSummary" => true,
        _ => false,
    }
}

fn content_blocks_are_known(content: Option<&Value>, allowed: &[&str]) -> bool {
    match content {
        None | Some(Value::Null | Value::String(_)) => true,
        Some(Value::Array(blocks)) => blocks.iter().all(|block| {
            let block_type = to_str(block.get("type"));
            !block_type.is_empty() && allowed.contains(&block_type.as_str())
        }),
        Some(Value::Object(_)) => {
            let block_type = to_str(content.and_then(|value| value.get("type")));
            !block_type.is_empty() && allowed.contains(&block_type.as_str())
        }
        Some(_) => false,
    }
}
