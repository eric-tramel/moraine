use super::pi_family::{normalize_pi_family_record, pi_family_source_metadata, PiFamilyPolicy};
use super::shared::{infer_session_id_from_file, to_str, RecordContext};
use super::{IngestSource, NormalizedPartials, SourceMetadata, SourceRecordContext};
use serde_json::Value;

pub(crate) static PI_CODING_AGENT: PiCodingAgent = PiCodingAgent;

pub(crate) struct PiCodingAgent;

impl IngestSource for PiCodingAgent {
    fn harness(&self) -> &'static str {
        "pi-coding-agent"
    }

    fn default_inference_provider(&self) -> Option<&'static str> {
        None
    }

    fn source_metadata(&self, record: &Value) -> SourceMetadata {
        pi_family_source_metadata(record)
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
        // Only the `session` header record carries the working directory;
        // later records inherit it via the normalizer's session-level fallback.
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
        normalize_pi_family_record(
            record,
            ctx,
            top_type,
            base_uid,
            model_hint,
            PiFamilyPolicy::PI,
        )
    }
}
